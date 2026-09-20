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

/// Location for generated secrets, relative to the work dir.
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
            )));
        }
    }
    Ok(dir)
}

/// Longest path a `sockaddr_un` can hold on this platform, excluding the
/// terminating NUL: 103 bytes on macOS, 107 on Linux.
#[cfg(unix)]
pub fn max_socket_path_len() -> usize {
    std::mem::size_of::<nix::libc::sockaddr_un>()
        - std::mem::offset_of!(nix::libc::sockaddr_un, sun_path)
        - 1
}

/// Where the attach host for `service` listens.
///
/// `.fed/attach/<service>.sock` keeps the socket with the rest of the
/// checkout's state, but a unix socket path has to fit in `sun_path`, and a
/// realistic workspace path plus `.fed/attach/` often does not (measured: a
/// 135-character workspace produces a 161-byte socket path against macOS's
/// 103-byte limit). When it does not fit, fall back to a short path under
/// `$TMPDIR` keyed by a hash of the work dir. `fed start` calls this once and
/// stores the result in the service's state row, because macOS gives a
/// different `$TMPDIR` per user and launch context, so a host and a client
/// that each computed the path could disagree.
#[cfg(unix)]
pub fn attach_socket_path(work_dir: &Path, service: &str) -> PathBuf {
    attach_socket_path_under(work_dir, service, &std::env::temp_dir())
}

#[cfg(unix)]
fn attach_socket_path_under(work_dir: &Path, service: &str, temp_dir: &Path) -> PathBuf {
    let file = format!("{}.sock", service_file_stem(service));
    let in_checkout = fed_dir(work_dir).join("attach").join(&file);
    if in_checkout.as_os_str().len() <= max_socket_path_len() {
        return in_checkout;
    }
    let directory = format!("fed-{}", crate::service::hash_work_dir(work_dir));
    let in_temp = temp_dir.join(&directory).join(&file);
    if in_temp.as_os_str().len() <= max_socket_path_len() {
        return in_temp;
    }
    // TMPDIR itself can be longer than sun_path. The uid keeps this last
    // resort separate between users even for a shared workspace path.
    let uid = unsafe { nix::libc::geteuid() };
    PathBuf::from("/tmp")
        .join(format!("{directory}-{uid}"))
        .join(file)
}

/// A short, stable filename component for service-owned files.
///
/// Names that need escaping are hashed so distinct service names cannot
/// collide through sanitization or escape their containing directory.
#[cfg(unix)]
pub fn service_file_stem(service: &str) -> String {
    if service.len() <= 24
        && !service.is_empty()
        && service
            .bytes()
            .all(|c| c.is_ascii_alphanumeric() || c == b'-' || c == b'_')
    {
        service.to_owned()
    } else {
        format!("~{:016x}", socket_name_hash(service.as_bytes()))
    }
}

#[cfg(unix)]
fn socket_name_hash(bytes: &[u8]) -> u64 {
    bytes.iter().fold(0xcbf29ce484222325u64, |hash, byte| {
        (hash ^ u64::from(*byte)).wrapping_mul(0x100000001b3)
    })
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

    #[cfg(unix)]
    #[test]
    fn a_short_workspace_keeps_the_socket_in_the_checkout() {
        let path = attach_socket_path(Path::new("/w"), "echo-svc");
        assert_eq!(path, PathBuf::from("/w/.fed/attach/echo-svc.sock"));
    }

    #[cfg(unix)]
    #[test]
    fn a_deep_workspace_moves_the_socket_under_tmpdir() {
        let deep = PathBuf::from("/w").join("x".repeat(max_socket_path_len()));
        let path = attach_socket_path(&deep, "echo-svc");
        assert!(
            path.starts_with(std::env::temp_dir()),
            "expected a $TMPDIR fallback, got {}",
            path.display()
        );
        assert!(path.as_os_str().len() <= max_socket_path_len());
    }

    #[cfg(unix)]
    #[test]
    fn a_socket_file_name_cannot_escape_its_directory() {
        let path = attach_socket_path(Path::new("/w"), "../../etc/x");
        assert_eq!(path.parent(), Some(Path::new("/w/.fed/attach")));
        assert_ne!(path, attach_socket_path(Path::new("/w"), "______etc_x"));
    }

    #[cfg(unix)]
    #[test]
    fn long_names_and_temp_directories_still_fit_and_bind() {
        use std::os::unix::net::UnixListener;
        let workspace = tempfile::tempdir().unwrap();
        let long_temp = PathBuf::from("/tmp").join("x".repeat(200));
        let deep = workspace.path().join("nested".repeat(30));
        let path = attach_socket_path_under(&deep, &"é".repeat(200), &long_temp);
        assert!(path.as_os_str().len() <= max_socket_path_len());
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        let listener = UnixListener::bind(&path).expect("fallback socket should bind");
        drop(listener);
        std::fs::remove_file(&path).unwrap();
        std::fs::remove_dir(path.parent().unwrap()).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn names_that_need_escaping_do_not_alias_safe_names() {
        let workspace = Path::new("/w");
        assert_ne!(
            attach_socket_path(workspace, "a/b"),
            attach_socket_path(workspace, "a_b")
        );
        assert_ne!(
            attach_socket_path(workspace, "a.b"),
            attach_socket_path(workspace, "a/b")
        );
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
