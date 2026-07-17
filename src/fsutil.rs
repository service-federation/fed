//! Atomic, owner-only (0600) file replacement.
//!
//! One helper shared by every writer that must land a sensitive file (the vault
//! cache, the login credentials) without ever leaving it more permissive than
//! 0600 — not even transiently on first creation under a permissive umask — and
//! without leaving a truncated file behind on a crash. The bytes go to a temp
//! file in the *same* directory (created 0600 from the very first open), are
//! flushed, then atomically renamed over the destination. Renaming within the
//! directory keeps the operation on one filesystem (so the rename is atomic) and
//! replaces the destination in place rather than following a symlink an attacker
//! might have planted there.

use crate::error::{Error, Result};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

/// Process-local counter mixed into temp file names so two writers in the same
/// process (same PID) never pick the same name.
static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Create a fresh temp sibling of `file_name` in `dir`, opened `O_EXCL` (never
/// following or truncating a pre-existing file/symlink at that path) and 0600
/// from the first open. Retries on name collision with a fresh random suffix.
///
/// The name mixes PID, a process-local counter, and random bits so it is
/// collision-resistant *and* unpredictable — an attacker can't pre-plant a
/// symlink at a guessable path, and `O_EXCL` refuses it even if they did.
fn create_temp_sibling(dir: &Path, file_name: &str) -> Result<(std::fs::File, PathBuf)> {
    let pid = std::process::id();
    for _ in 0..32 {
        let counter = TEMP_COUNTER.fetch_add(1, Ordering::Relaxed);
        let rand: u64 = rand::random();
        let tmp_path = dir.join(format!(".{file_name}.tmp.{pid}.{counter}.{rand:016x}"));

        let mut opts = std::fs::OpenOptions::new();
        // create_new = O_EXCL|O_CREAT: fails if anything already exists at the
        // path (regular file OR symlink), so a planted symlink is never followed
        // or truncated. No truncate(true) — O_EXCL guarantees a brand-new file.
        opts.create_new(true).write(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            opts.mode(0o600);
        }
        match opts.open(&tmp_path) {
            Ok(file) => return Ok((file, tmp_path)),
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(e) => {
                return Err(Error::Filesystem(format!(
                    "Cannot write '{}': {}",
                    tmp_path.display(),
                    e
                )))
            }
        }
    }
    Err(Error::Filesystem(format!(
        "Cannot create a unique temp file for '{}' after 32 attempts",
        file_name
    )))
}

/// Best-effort fsync of a directory so a rename into it is durable across a
/// power loss (the rename's metadata change is only guaranteed on disk once the
/// containing directory is synced). Unix-only; ignored on other platforms.
#[cfg(unix)]
fn fsync_dir(dir: &Path) {
    if let Ok(handle) = std::fs::File::open(dir) {
        let _ = handle.sync_all();
    }
}

/// Write `contents` to `path`, atomically and readable only by the owner (0600
/// on Unix).
///
/// When `sync` is true the temp file's data is flushed to disk (`sync_all`)
/// before the rename — durability across a power loss at the cost of one fsync.
/// Callers holding a must-not-be-silently-lost secret (a bearer token) pass
/// true; ephemeral state that can be re-derived can pass false.
///
/// On non-Unix platforms there is no `mode`, so the temp file is created with
/// the default permissions and the atomicity/crash-safety guarantees still hold;
/// the 0600 guarantee is Unix-only.
pub fn write_owner_only_atomic(path: &Path, contents: &[u8], sync: bool) -> Result<()> {
    use std::io::Write;

    let dir = path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .map(Path::to_path_buf)
        .unwrap_or_else(|| Path::new(".").to_path_buf());
    let file_name = path
        .file_name()
        .and_then(|n| n.to_str())
        .ok_or_else(|| Error::Filesystem(format!("'{}' has no file name", path.display())))?;
    // Temp sibling in the same directory: same filesystem (so the rename is
    // atomic) and the rename replaces the destination in place rather than
    // following a symlink planted there. The name is collision-resistant
    // (pid + counter + random) and the file is opened O_EXCL, so a planted
    // symlink is never followed or truncated and two same-process writers can't
    // clobber a shared temp file.
    let (mut file, tmp_path) = create_temp_sibling(&dir, file_name)?;

    let write_result = (|| -> Result<()> {
        // Belt-and-braces: if the freshly opened temp file is somehow more
        // permissive than 0600, tighten it through the handle before any bytes
        // land. On Unix `mode(0o600)` already guarantees this; this also covers
        // an odd platform default.
        tighten_to_owner_only(&file, &tmp_path)?;

        file.write_all(contents)
            .map_err(|e| Error::Filesystem(format!("Write error: {}", e)))?;
        if sync {
            file.sync_all()
                .map_err(|e| Error::Filesystem(format!("Sync error: {}", e)))?;
        }
        // Close before the rename so every byte is in the file the rename
        // publishes (std::fs::File is unbuffered, but the close makes the
        // ordering explicit and drops the handle).
        drop(file);
        std::fs::rename(&tmp_path, path).map_err(|e| {
            Error::Filesystem(format!("Cannot replace '{}': {}", path.display(), e))
        })?;
        // Durability: the rename's directory-entry change is only guaranteed on
        // disk once the containing directory is fsynced. Best-effort (Unix only)
        // so the documented crash-safety claim holds without failing the write
        // on filesystems that reject a directory fsync.
        #[cfg(unix)]
        if sync {
            fsync_dir(&dir);
        }
        Ok(())
    })();
    if write_result.is_err() {
        // Best-effort cleanup of the temp file; the original destination is
        // untouched because the rename never happened.
        let _ = std::fs::remove_file(&tmp_path);
    }
    write_result
}

/// Tighten an already-open file to owner-only (0600) permissions via its handle
/// (`fchmod` — no path race), but only when it is currently more permissive.
///
/// Operating on the handle rather than the pathname avoids following a symlink
/// or racing a path swap between the check and the change.
pub(crate) fn tighten_to_owner_only(file: &std::fs::File, path: &Path) -> Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let meta = file
            .metadata()
            .map_err(|e| Error::Filesystem(format!("Cannot stat '{}': {}", path.display(), e)))?;
        if meta.permissions().mode() & 0o177 != 0 {
            file.set_permissions(std::fs::Permissions::from_mode(0o600))
                .map_err(|e| {
                    Error::Filesystem(format!("Cannot chmod '{}': {}", path.display(), e))
                })?;
        }
    }
    #[cfg(not(unix))]
    let _ = (file, path);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(unix)]
    fn mode_of(path: &Path) -> u32 {
        use std::os::unix::fs::PermissionsExt;
        std::fs::metadata(path).unwrap().permissions().mode() & 0o777
    }

    #[test]
    fn initial_write_is_owner_only_and_never_broader() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("credentials");
        write_owner_only_atomic(&path, b"token: abc\n", true).unwrap();
        assert_eq!(std::fs::read_to_string(&path).unwrap(), "token: abc\n");
        #[cfg(unix)]
        {
            // The destination inherits the temp file's mode across the rename, so
            // checking the destination checks the mode the file was opened with.
            // It must be exactly 0600 — never any group/other bits.
            let mode = mode_of(&path);
            assert_eq!(mode, 0o600, "mode was {:o}, must be 0600", mode);
            assert_eq!(mode & 0o077, 0, "no group/other bits may be set");
        }
    }

    #[test]
    fn overwrite_keeps_owner_only() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("credentials");
        write_owner_only_atomic(&path, b"first\n", true).unwrap();
        write_owner_only_atomic(&path, b"second\n", true).unwrap();
        assert_eq!(std::fs::read_to_string(&path).unwrap(), "second\n");
        #[cfg(unix)]
        assert_eq!(mode_of(&path), 0o600);
    }

    /// Probe whether the process can still create files in a mode-0500
    /// directory (true when running as root, which bypasses directory write
    /// permissions). Used to skip permission-based injection under root instead
    /// of asserting a failure that can't happen there. Restores 0700 on exit.
    #[cfg(unix)]
    fn dir_write_blocked_for_us(dir: &Path) -> bool {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(dir, std::fs::Permissions::from_mode(0o500)).unwrap();
        let probe = dir.join(".probe");
        let blocked = std::fs::File::create(&probe).is_err();
        let _ = std::fs::remove_file(&probe);
        if !blocked {
            std::fs::set_permissions(dir, std::fs::Permissions::from_mode(0o700)).unwrap();
        }
        blocked
    }

    #[cfg(unix)]
    #[test]
    fn failed_write_preserves_previous_file() {
        // Inject a temp-open failure by making the destination directory
        // read-only, so the writer can't create its (now randomly-named) temp
        // sibling. A failed write must never touch the destination — the rename
        // that would replace it never happens. Skipped under root, which
        // bypasses directory permissions.
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("credentials");
        write_owner_only_atomic(&path, b"good-token\n", true).unwrap();

        if !dir_write_blocked_for_us(dir.path()) {
            return; // running as root — the injection can't work; skip.
        }

        let result = write_owner_only_atomic(&path, b"new-token\n", true);
        // Restore write perms so tempdir cleanup succeeds regardless of outcome.
        std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o700)).unwrap();

        assert!(result.is_err(), "creating the temp file must fail");
        assert_eq!(
            std::fs::read_to_string(&path).unwrap(),
            "good-token\n",
            "the previous valid file must be left intact"
        );
    }

    /// A symlink planted at the destination path must NOT be followed: the
    /// atomic rename replaces the symlink itself, so an attacker-controlled
    /// "victim" file the link points at is never truncated or overwritten, and
    /// the destination ends up a real, owner-only file with our bytes.
    #[cfg(unix)]
    #[test]
    fn symlink_at_destination_is_not_followed() {
        use std::os::unix::fs::symlink;
        let dir = tempfile::tempdir().unwrap();
        let victim = dir.path().join("victim");
        std::fs::write(&victim, "precious\n").unwrap();

        let path = dir.path().join("credentials");
        symlink(&victim, &path).unwrap();

        write_owner_only_atomic(&path, b"new-token\n", true).unwrap();

        assert_eq!(
            std::fs::read_to_string(&victim).unwrap(),
            "precious\n",
            "the symlink target must be left untouched"
        );
        // The destination is now a regular file (not a symlink) with our bytes.
        assert!(!std::fs::symlink_metadata(&path)
            .unwrap()
            .file_type()
            .is_symlink());
        assert_eq!(std::fs::read_to_string(&path).unwrap(), "new-token\n");
        assert_eq!(mode_of(&path), 0o600);
    }

    /// Two temp siblings created back-to-back in the same process must get
    /// distinct, owner-only paths — the pid+counter+random suffix rules out the
    /// old "filename+pid" collision where two same-process writers shared a temp
    /// file.
    #[cfg(unix)]
    #[test]
    fn temp_siblings_are_unique_and_owner_only() {
        let dir = tempfile::tempdir().unwrap();
        let (f1, p1) = create_temp_sibling(dir.path(), "credentials").unwrap();
        let (f2, p2) = create_temp_sibling(dir.path(), "credentials").unwrap();
        assert_ne!(p1, p2, "temp names must not collide within a process");
        drop(f1);
        drop(f2);
        assert_eq!(mode_of(&p1), 0o600);
        assert_eq!(mode_of(&p2), 0o600);

        // O_EXCL: opening create_new over an existing temp path must be refused,
        // never truncating or following it.
        let err = std::fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&p1)
            .unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::AlreadyExists);
    }

    #[test]
    fn reload_round_trips_bytes() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("credentials");
        let contents = "url: https://example\ntoken: xyz\n";
        write_owner_only_atomic(&path, contents.as_bytes(), true).unwrap();
        assert_eq!(std::fs::read_to_string(&path).unwrap(), contents);
    }

    /// Loading an over-permissive file tightens it to 0600 through the held
    /// handle (fchmod — no path race), the same guarantee the credential load
    /// path relies on.
    #[cfg(unix)]
    #[test]
    fn tighten_handle_narrows_overpermissive_file() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("credentials");
        std::fs::write(&path, "token\n").unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).unwrap();
        assert_eq!(mode_of(&path), 0o644);

        let file = std::fs::File::open(&path).unwrap();
        tighten_to_owner_only(&file, &path).unwrap();
        assert_eq!(
            mode_of(&path),
            0o600,
            "over-permissive file must be tightened via the handle"
        );
    }
}
