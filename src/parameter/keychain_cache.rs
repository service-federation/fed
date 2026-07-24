//! Operating-system credential-store backend for the team-vault cache.
//!
//! macOS Keychain and Linux Secret Service use one item per project. In
//! particular, macOS access control is item-scoped: one item avoids a password
//! prompt for every individual secret after installing a new fed binary.
//! Windows Credential Manager has a much smaller per-credential payload limit,
//! so Windows keeps one item per secret plus a manifest for cleanup. Accounts
//! are opaque hashes: secret names and project identifiers are not exposed in
//! credential-store listings.

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

#[cfg(not(target_os = "windows"))]
#[derive(Debug, Default, Serialize, Deserialize)]
struct StoredProject {
    entries: HashMap<String, StoredEntry>,
}

#[cfg(target_os = "windows")]
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
        #[cfg(not(target_os = "windows"))]
        return self.load_project(declared_names);

        #[cfg(target_os = "windows")]
        {
            self.load_entries(declared_names)
        }
    }

    #[cfg(not(target_os = "windows"))]
    fn load_project(
        &self,
        declared_names: &HashSet<String>,
    ) -> Result<(HashMap<String, String>, HashMap<String, u64>)> {
        let Some(payload) = read_if_present(&credential(&self.project_account())?)? else {
            return Ok((HashMap::new(), HashMap::new()));
        };
        let stored: StoredProject = serde_json::from_slice(&payload)
            .map_err(|e| Error::Validation(format!("team-vault keychain cache is corrupt: {e}")))?;
        let mut values = HashMap::new();
        let mut stamps = HashMap::new();
        for (name, entry) in stored.entries {
            if !declared_names.contains(&name) {
                continue;
            }
            if let Some(stamp) = entry.fetched_at {
                stamps.insert(name.clone(), stamp);
            }
            values.insert(name, entry.value);
        }
        Ok((values, stamps))
    }

    #[cfg(target_os = "windows")]
    fn load_entries(
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
        #[cfg(not(target_os = "windows"))]
        return self.replace_project(entries);

        #[cfg(target_os = "windows")]
        {
            self.replace_entries(entries)
        }
    }

    #[cfg(not(target_os = "windows"))]
    fn replace_project(&self, entries: &HashMap<String, CacheEntry>) -> Result<()> {
        let credential = credential(&self.project_account())?;
        if entries.is_empty() {
            return delete_if_present(&credential);
        }
        let stored = StoredProject {
            entries: entries
                .iter()
                .map(|(name, entry)| {
                    (
                        name.clone(),
                        StoredEntry {
                            value: entry.value.clone(),
                            fetched_at: entry.fetched_at,
                        },
                    )
                })
                .collect(),
        };
        let payload = serde_json::to_vec(&stored)
            .map_err(|e| Error::Validation(format!("serializing keychain vault cache: {e}")))?;
        credential
            .set_secret(&payload)
            .map_err(|e| keychain_error("write to", e))
    }

    #[cfg(target_os = "windows")]
    fn replace_entries(&self, entries: &HashMap<String, CacheEntry>) -> Result<()> {
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

    #[cfg(target_os = "windows")]
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

    #[cfg(target_os = "windows")]
    fn read_manifest(&self) -> Result<Manifest> {
        let Some(payload) = read_if_present(&credential(&self.manifest_account())?)? else {
            return Ok(Manifest::default());
        };
        serde_json::from_slice(&payload)
            .map_err(|e| Error::Validation(format!("team-vault keychain manifest is corrupt: {e}")))
    }

    #[cfg(target_os = "windows")]
    fn manifest_account(&self) -> String {
        format!("{}:manifest", self.project_id)
    }

    #[cfg(target_os = "windows")]
    fn secret_account(&self, name: &str) -> String {
        format!("{}:{}", self.project_id, digest(name))
    }

    fn project_account(&self) -> String {
        format!("{}:cache", self.project_id)
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
        let account = cache.project_account();
        assert_eq!(account.len(), 70);
        assert!(!account.contains("DATABASE_PASSWORD"));
        assert_eq!(account, cache.project_account());
        assert_ne!(digest("DATABASE_PASSWORD"), digest("STRIPE_SECRET"));
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
