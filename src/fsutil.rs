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
use std::path::Path;

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
    // following a symlink planted there.
    let tmp_path = dir.join(format!(".{}.tmp.{}", file_name, std::process::id()));

    let write_result = (|| -> Result<()> {
        let mut opts = std::fs::OpenOptions::new();
        opts.create(true).truncate(true).write(true);
        // 0600 from the first open — there is never a window where the temp file
        // (and so the secret it will hold) is group- or world-readable, even
        // under a permissive umask.
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            opts.mode(0o600);
        }
        let mut file = opts.open(&tmp_path).map_err(|e| {
            Error::Filesystem(format!("Cannot write '{}': {}", tmp_path.display(), e))
        })?;

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

/// Best-effort: tighten an existing file at `path` to 0600 if it is more
/// permissive. Used on load paths (e.g. reading credentials written by an older
/// fed that chmodded after the fact) where a chmod failure must not abort the
/// read. Errors are deliberately ignored.
pub(crate) fn tighten_path_to_owner_only(path: &Path) {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if let Ok(meta) = std::fs::metadata(path) {
            if meta.permissions().mode() & 0o177 != 0 {
                let _ = std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600));
            }
        }
    }
    #[cfg(not(unix))]
    let _ = path;
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

    #[test]
    fn failed_write_preserves_previous_file() {
        // Inject a write failure deterministically and without depending on the
        // running uid: pre-create a *directory* exactly where the writer will
        // place its temp file, so opening the temp file fails. The pid is the
        // current process's, so the test knows the exact temp path.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("credentials");
        write_owner_only_atomic(&path, b"good-token\n", true).unwrap();

        let file_name = path.file_name().unwrap().to_str().unwrap();
        let tmp_path = dir
            .path()
            .join(format!(".{}.tmp.{}", file_name, std::process::id()));
        std::fs::create_dir(&tmp_path).unwrap();

        let result = write_owner_only_atomic(&path, b"new-token\n", true);
        assert!(result.is_err(), "opening the temp path must fail");
        assert_eq!(
            std::fs::read_to_string(&path).unwrap(),
            "good-token\n",
            "the previous valid file must be left intact"
        );
    }

    #[test]
    fn reload_round_trips_bytes() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("credentials");
        let contents = "url: https://example\ntoken: xyz\n";
        write_owner_only_atomic(&path, contents.as_bytes(), true).unwrap();
        assert_eq!(std::fs::read_to_string(&path).unwrap(), contents);
    }

    #[test]
    fn tighten_path_narrows_overpermissive_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("credentials");
        std::fs::write(&path, "token\n").unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).unwrap();
            assert_eq!(mode_of(&path), 0o644);
            tighten_path_to_owner_only(&path);
            assert_eq!(
                mode_of(&path),
                0o600,
                "over-permissive file must be tightened"
            );
        }
        #[cfg(not(unix))]
        tighten_path_to_owner_only(&path);
    }
}
