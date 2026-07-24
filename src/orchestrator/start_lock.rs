//! Cross-process serialization for `fed start`.
//!
//! Parameter resolution happens before per-service registration. Without a
//! scope-wide guard, two CLI processes can resolve different port sets and
//! then win different service registrations, producing one stack assembled
//! from incompatible configurations. This lock covers initialization and the
//! finite startup phase; watch mode releases it before entering its long-lived
//! event loop.

use crate::error::{Error, Result};
use fs2::FileExt;
use std::fs::OpenOptions;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::Duration;

const START_LOCK_FILE_NAME: &str = "start.lock";
const RETRY_INTERVAL: Duration = Duration::from_millis(50);

/// Exclusive guard for one workspace's parameter-resolution/startup phase.
///
/// The operating system releases the lock automatically if the holder exits,
/// including after SIGKILL. The PID text is diagnostic only; lock ownership is
/// determined entirely by `flock`.
pub struct StartLock {
    file: std::fs::File,
    path: PathBuf,
}

impl StartLock {
    /// Wait until no other `fed start` owns this workspace's startup phase.
    pub async fn acquire(work_dir: &Path) -> Result<Self> {
        let fed_dir = crate::fed_dir::ensure_fed_dir(work_dir)?;
        let path = fed_dir.join(START_LOCK_FILE_NAME);
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)
            .map_err(|e| Error::Filesystem(format!("Failed to open start lock: {}", e)))?;

        let contended = fs2::lock_contended_error();
        let mut announced_wait = false;
        loop {
            match FileExt::try_lock_exclusive(&file) {
                Ok(()) => break,
                Err(e)
                    if e.kind() == contended.kind()
                        || e.raw_os_error() == contended.raw_os_error() =>
                {
                    if !announced_wait {
                        tracing::info!(
                            "Another fed start is resolving this workspace; waiting for it to finish"
                        );
                        announced_wait = true;
                    }
                    tokio::time::sleep(RETRY_INTERVAL).await;
                }
                Err(e) => {
                    return Err(Error::Filesystem(format!(
                        "Failed to acquire start lock '{}': {}",
                        path.display(),
                        e
                    )));
                }
            }
        }

        file.set_len(0).map_err(|e| {
            Error::Filesystem(format!(
                "Failed to update start lock '{}': {}",
                path.display(),
                e
            ))
        })?;
        writeln!(file, "{} fed-start", std::process::id()).map_err(|e| {
            Error::Filesystem(format!(
                "Failed to update start lock '{}': {}",
                path.display(),
                e
            ))
        })?;

        Ok(Self { file, path })
    }

    /// Path to the lock file, primarily for diagnostics and tests.
    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for StartLock {
    fn drop(&mut self) {
        let _ = FileExt::unlock(&self.file);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn second_start_waits_until_the_first_releases() {
        let temp_dir = tempfile::tempdir().unwrap();
        let first = StartLock::acquire(temp_dir.path()).await.unwrap();
        assert_eq!(first.path(), temp_dir.path().join(".fed/start.lock"));

        let work_dir = temp_dir.path().to_path_buf();
        let mut second = tokio::spawn(async move { StartLock::acquire(&work_dir).await.unwrap() });

        assert!(
            tokio::time::timeout(Duration::from_millis(150), &mut second)
                .await
                .is_err(),
            "a second start must remain blocked while the first owns the lock"
        );

        drop(first);
        tokio::time::timeout(Duration::from_secs(2), second)
            .await
            .expect("second start should acquire after release")
            .expect("second acquire task should not panic");
    }
}
