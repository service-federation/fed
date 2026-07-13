//! The per-checkout `.fed/` directory.
//!
//! `.fed/` holds fed's internal per-checkout state: the lock database
//! (`lock.db`), logs, the vault secrets cache (`secrets.cache.env`), generated
//! secrets (`secrets.generated.env`), and the committed cloud link
//! (`cloud.yaml`).
//!
//! fed self-manages a `.fed/.gitignore` (the `.terraform`-style trick): it
//! ignores everything inside `.fed/` except `cloud.yaml` (which teammates
//! should commit) and the `.gitignore` itself (so git applies the same rules
//! on every clone). Users never have to hand-edit their root `.gitignore`
//! for fed's state files.

use crate::error::{Error, Result};
use std::path::{Path, PathBuf};

/// Name of the per-checkout state directory.
pub const FED_DIR: &str = ".fed";

/// Vault secrets cache, relative to the work dir. Internal state — always
/// lives here regardless of configuration.
pub const SECRETS_CACHE_REL: &str = ".fed/secrets.cache.env";

/// Default location for generated secrets, relative to the work dir.
/// Used when the (deprecated) `generated_secrets_file` key is not set.
pub const GENERATED_SECRETS_REL: &str = ".fed/secrets.generated.env";

/// Contents of the self-managed `.fed/.gitignore`.
const GITIGNORE_CONTENT: &str = "*\n!cloud.yaml\n!.gitignore\n";

/// Absolute path of the `.fed/` directory for a work dir.
pub fn fed_dir(work_dir: &Path) -> PathBuf {
    work_dir.join(FED_DIR)
}

/// Absolute path of the vault secrets cache for a work dir.
pub fn secrets_cache_path(work_dir: &Path) -> PathBuf {
    work_dir.join(SECRETS_CACHE_REL)
}

/// Absolute path of the default generated-secrets file for a work dir.
pub fn default_generated_secrets_path(work_dir: &Path) -> PathBuf {
    work_dir.join(GENERATED_SECRETS_REL)
}

/// Ensure `.fed/` exists and carries its self-ignoring `.gitignore`.
///
/// Creates the directory if needed and writes `.fed/.gitignore` only when the
/// file does not exist yet — a user-edited `.gitignore` is never clobbered.
pub fn ensure_fed_dir(work_dir: &Path) -> Result<PathBuf> {
    let dir = fed_dir(work_dir);
    std::fs::create_dir_all(&dir)
        .map_err(|e| Error::Filesystem(format!("creating {}: {}", dir.display(), e)))?;
    let gitignore = dir.join(".gitignore");
    // create_new is atomic: a concurrent fed (or a user-edited file appearing
    // between check and write) can never be clobbered.
    match std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&gitignore)
    {
        Ok(mut f) => {
            use std::io::Write;
            f.write_all(GITIGNORE_CONTENT.as_bytes()).map_err(|e| {
                Error::Filesystem(format!("writing {}: {}", gitignore.display(), e))
            })?;
        }
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {}
        Err(e) => {
            return Err(Error::Filesystem(format!(
                "creating {}: {}",
                gitignore.display(),
                e
            )))
        }
    }
    Ok(dir)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn creates_dir_and_gitignore() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = ensure_fed_dir(tmp.path()).unwrap();
        assert!(dir.is_dir());
        let content = std::fs::read_to_string(dir.join(".gitignore")).unwrap();
        assert_eq!(content, "*\n!cloud.yaml\n!.gitignore\n");
    }

    #[test]
    fn does_not_clobber_user_edited_gitignore() {
        let tmp = tempfile::tempdir().unwrap();
        ensure_fed_dir(tmp.path()).unwrap();
        let gi = tmp.path().join(".fed/.gitignore");
        std::fs::write(&gi, "# custom\n*\n!cloud.yaml\n!extra.yaml\n!.gitignore\n").unwrap();
        ensure_fed_dir(tmp.path()).unwrap();
        let content = std::fs::read_to_string(&gi).unwrap();
        assert!(content.contains("!extra.yaml"), "user edits must survive");
    }

    #[test]
    fn gitignore_makes_state_ignored_but_cloud_yaml_committable() {
        let tmp = tempfile::tempdir().unwrap();
        let repo = git2::Repository::init(tmp.path()).unwrap();
        ensure_fed_dir(tmp.path()).unwrap();
        std::fs::write(tmp.path().join(".fed/lock.db"), "x").unwrap();
        std::fs::write(tmp.path().join(".fed/secrets.cache.env"), "x").unwrap();
        std::fs::write(tmp.path().join(".fed/cloud.yaml"), "org: a\n").unwrap();

        assert!(repo.is_path_ignored(".fed/lock.db").unwrap());
        assert!(repo.is_path_ignored(".fed/secrets.cache.env").unwrap());
        assert!(repo.is_path_ignored(".fed/secrets.generated.env").unwrap());
        assert!(!repo.is_path_ignored(".fed/cloud.yaml").unwrap());
        assert!(!repo.is_path_ignored(".fed/.gitignore").unwrap());
    }
}
