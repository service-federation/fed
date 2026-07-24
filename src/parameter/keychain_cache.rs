//! Operating-system credential-store backend for the team-vault cache.
//!
//! Each value is a separate credential so a project with many or unusually
//! large secrets does not depend on one monolithic platform entry. Accounts
//! are opaque hashes: secret names and project identifiers are not exposed in
//! credential-store listings. A manifest records the names fed owns so a cache
//! rewrite can remove entries deleted from the project configuration.

use crate::error::{Error, Result};
use crate::parameter::secret::CacheEntry;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet};
use std::path::Path;

const SERVICE: &str = "com.service-federation.fed.vault-cache";

#[derive(Debug, Serialize, Deserialize)]
struct StoredEntry {
    value: String,
    fetched_at: Option<u64>,
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct Manifest {
    names: Vec<String>,
}

#[derive(Debug)]
pub(crate) struct KeychainCache {
    project_id: String,
}

impl KeychainCache {
    pub(crate) fn for_work_dir(work_dir: &Path) -> Result<Self> {
        let link = crate::cloud::load_link(work_dir).ok_or_else(|| {
            Error::Validation(
                "secret_cache: keychain requires a linked project in .fed/cloud.yaml".into(),
            )
        })?;
        let cloud_url = crate::cloud::load_credentials()
            .map(|credentials| credentials.url)
            .unwrap_or_else(|| crate::cloud::DEFAULT_URL.to_string());
        Ok(Self {
            project_id: digest(&format!("{cloud_url}\0{}\0{}", link.org, link.project)),
        })
    }

    pub(crate) fn load(
        &self,
        declared_names: &HashSet<String>,
    ) -> Result<(HashMap<String, String>, HashMap<String, u64>)> {
        let mut values = HashMap::new();
        let mut stamps = HashMap::new();
        for name in declared_names {
            let Some(stored) = self.read_secret(name)? else {
                continue;
            };
            if let Some(stamp) = stored.fetched_at {
                stamps.insert(name.clone(), stamp);
            }
            values.insert(name.clone(), stored.value);
        }
        Ok((values, stamps))
    }

    pub(crate) fn replace(&self, entries: &HashMap<String, CacheEntry>) -> Result<()> {
        let old_manifest = self.read_manifest()?;
        let new_names: HashSet<&str> = entries.keys().map(String::as_str).collect();

        // Write values before the manifest. If a platform write fails, the old
        // manifest remains authoritative for cleanup on the next successful run.
        for (name, entry) in entries {
            let stored = StoredEntry {
                value: entry.value.clone(),
                fetched_at: entry.fetched_at,
            };
            let payload = serde_json::to_vec(&stored).map_err(|e| {
                Error::Validation(format!("serializing keychain vault cache entry: {e}"))
            })?;
            credential(&self.secret_account(name))?
                .set_secret(&payload)
                .map_err(|e| keychain_error("write", e))?;
        }

        for stale in old_manifest
            .names
            .iter()
            .filter(|name| !new_names.contains(name.as_str()))
        {
            delete_if_present(&credential(&self.secret_account(stale))?)?;
        }

        let mut names: Vec<String> = entries.keys().cloned().collect();
        names.sort();
        let payload = serde_json::to_vec(&Manifest { names }).map_err(|e| {
            Error::Validation(format!("serializing keychain vault cache manifest: {e}"))
        })?;
        credential(&self.manifest_account())?
            .set_secret(&payload)
            .map_err(|e| keychain_error("write manifest to", e))
    }

    fn read_secret(&self, name: &str) -> Result<Option<StoredEntry>> {
        let Some(payload) = read_if_present(&credential(&self.secret_account(name))?)? else {
            return Ok(None);
        };
        serde_json::from_slice(&payload).map(Some).map_err(|e| {
            Error::Validation(format!(
                "team-vault keychain entry is corrupt for project secret {name}: {e}"
            ))
        })
    }

    fn read_manifest(&self) -> Result<Manifest> {
        let Some(payload) = read_if_present(&credential(&self.manifest_account())?)? else {
            return Ok(Manifest::default());
        };
        serde_json::from_slice(&payload)
            .map_err(|e| Error::Validation(format!("team-vault keychain manifest is corrupt: {e}")))
    }

    fn manifest_account(&self) -> String {
        format!("{}:manifest", self.project_id)
    }

    fn secret_account(&self, name: &str) -> String {
        format!("{}:{}", self.project_id, digest(name))
    }
}

fn digest(value: &str) -> String {
    format!("{:x}", Sha256::digest(value.as_bytes()))
}

#[cfg(any(target_os = "macos", target_os = "linux", target_os = "windows"))]
fn credential(account: &str) -> Result<keyring::Entry> {
    keyring::Entry::new(SERVICE, account).map_err(|e| keychain_error("open", e))
}

#[cfg(not(any(target_os = "macos", target_os = "linux", target_os = "windows")))]
fn credential(_account: &str) -> Result<()> {
    Err(Error::Validation(
        "secret_cache: keychain is unsupported on this operating system".into(),
    ))
}

#[cfg(any(target_os = "macos", target_os = "linux", target_os = "windows"))]
fn read_if_present(entry: &keyring::Entry) -> Result<Option<Vec<u8>>> {
    match entry.get_secret() {
        Ok(value) => Ok(Some(value)),
        Err(keyring::Error::NoEntry) => Ok(None),
        Err(e) => Err(keychain_error("read from", e)),
    }
}

#[cfg(any(target_os = "macos", target_os = "linux", target_os = "windows"))]
fn delete_if_present(entry: &keyring::Entry) -> Result<()> {
    match entry.delete_credential() {
        Ok(()) | Err(keyring::Error::NoEntry) => Ok(()),
        Err(e) => Err(keychain_error("delete from", e)),
    }
}

#[cfg(any(target_os = "macos", target_os = "linux", target_os = "windows"))]
fn keychain_error(action: &str, error: keyring::Error) -> Error {
    Error::Validation(format!(
        "cannot {action} the operating-system keychain vault cache: {error}"
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn account_identifiers_are_fixed_length_and_hide_names() {
        let cache = KeychainCache {
            project_id: digest("https://example.test\0acme\0web"),
        };
        let account = cache.secret_account("DATABASE_PASSWORD");
        assert_eq!(account.len(), 129);
        assert!(!account.contains("DATABASE_PASSWORD"));
        assert_eq!(account, cache.secret_account("DATABASE_PASSWORD"));
        assert_ne!(account, cache.secret_account("STRIPE_SECRET"));
    }

    #[test]
    fn stored_entry_round_trips_multiline_binary_like_text() {
        let entry = StoredEntry {
            value: "line one\nline two\0tail".into(),
            fetched_at: Some(42),
        };
        let encoded = serde_json::to_vec(&entry).unwrap();
        let decoded: StoredEntry = serde_json::from_slice(&encoded).unwrap();
        assert_eq!(decoded.value, entry.value);
        assert_eq!(decoded.fetched_at, Some(42));
    }
}
