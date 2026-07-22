//! `.fed/supervisor.lock` — single-instance enforcement and liveness
//! detection for the `fed supervise` daemon (`07-supervisor.md` Design
//! §1/§7).
//!
//! Modeled on the existing `.fed/.lock` advisory-lock pattern
//! (`crate::state::sqlite::mod`'s `try_acquire_lock`), but held by a
//! different, long-lived OS process (the supervisor daemon) rather than by
//! any `SqliteStateTracker` — the supervisor never touches `.fed/.lock` at
//! all (see [`crate::state::SqliteStateTracker::new_for_supervisor`]).
//!
//! Unlike `.fed/.lock`'s PID-and-process-name check (`is_fed_process`),
//! liveness here is derived directly from `flock()` semantics: a
//! non-blocking exclusive lock attempt either succeeds (nobody holds it —
//! any PID text left over from a dead process is stale and harmless) or
//! fails (a live process genuinely holds the lock right now, on this exact
//! file). This sidesteps PID-recycling ambiguity entirely rather than
//! reasoning about it after the fact, and doesn't require exposing
//! `is_fed_process` (private to the `state` module) across a module
//! boundary.

use crate::error::{Error, Result};
use fs2::FileExt;
use std::fs::OpenOptions;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::Duration;

const SUPERVISOR_LOCK_FILE_NAME: &str = "supervisor.lock";

/// Held for the supervisor daemon's entire lifetime, released on `Drop`
/// (mirrors `.fed/.lock`'s own release-on-drop discipline). The OS also
/// releases the underlying `flock()` automatically when the process exits
/// (including `kill -9`), which is what makes "stale lock recovery" free:
/// the next [`try_acquire`] call simply succeeds once the old holder is
/// gone, no explicit staleness check required.
pub struct SupervisorLock {
    file: std::fs::File,
    path: PathBuf,
}

impl SupervisorLock {
    /// Path to the lock file this guard holds.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Explicitly release the lock. Equivalent to dropping the guard —
    /// provided for call sites where an explicit release reads more
    /// clearly than an implicit end-of-scope drop.
    pub fn release(self) {
        drop(self);
    }
}

impl Drop for SupervisorLock {
    fn drop(&mut self) {
        let _ = FileExt::unlock(&self.file);
    }
}

/// Try to acquire `.fed/supervisor.lock` exclusively.
///
/// Returns `Err` if a live supervisor already holds it — the caller (`fed
/// supervise`'s entry point) should treat this as "another instance won
/// the race" and exit quietly, not as a hard failure; this is the expected
/// outcome of two concurrent `fed start`/`fed restart` invocations both
/// deciding to spawn a supervisor at the same time.
pub fn try_acquire(work_dir: &Path) -> Result<SupervisorLock> {
    let fed_dir = crate::fed_dir::ensure_fed_dir(work_dir)?;
    let lock_path = fed_dir.join(SUPERVISOR_LOCK_FILE_NAME);

    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(&lock_path)
        .map_err(|e| Error::Filesystem(format!("Failed to open supervisor lock file: {}", e)))?;

    match FileExt::try_lock_exclusive(&file) {
        Ok(()) => {
            let _ = file.set_len(0);
            let _ = writeln!(file, "{} fed-supervisor", std::process::id());
            Ok(SupervisorLock {
                file,
                path: lock_path,
            })
        }
        Err(e) => Err(Error::Validation(format!(
            "another fed supervisor already holds {:?}: {}",
            lock_path, e
        ))),
    }
}

/// If a live supervisor currently holds `.fed/supervisor.lock`, return its
/// PID. Never blocks and never disturbs a genuine holder: attempts a
/// non-blocking exclusive lock first — if that succeeds, nobody holds the
/// lock (release immediately and report `None`); if it fails, something is
/// genuinely holding it right now, so the PID text in the file (written by
/// that same live holder in [`try_acquire`]) is trustworthy.
///
/// Returns `None` if the lock file doesn't exist yet (no supervisor has
/// ever run here) or its PID marker can't be parsed.
pub fn live_supervisor_pid(work_dir: &Path) -> Option<u32> {
    let lock_path = work_dir.join(".fed").join(SUPERVISOR_LOCK_FILE_NAME);

    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&lock_path)
        .ok()?;

    if FileExt::try_lock_exclusive(&file).is_ok() {
        // Nobody was holding it — release what we just took and report none.
        let _ = FileExt::unlock(&file);
        return None;
    }

    let contents = std::fs::read_to_string(&lock_path).ok()?;
    let pid_str = contents.split_whitespace().next()?;
    pid_str.parse::<u32>().ok()
}

/// SIGTERM a live supervisor (grace period, then SIGKILL), matching the
/// existing pattern used to stop a detached service process
/// (`ProcessService::stop`, `src/service/process.rs:539-583`): signal the
/// process group first (the supervisor is spawned with `process_group(0)`,
/// per Design §5), fall back to the bare PID, wait up to `grace_period`
/// polling for the lock to become acquirable (i.e. the supervisor's own
/// `stop_monitoring_only` released it and the process exited), then escalate
/// to SIGKILL if it's still holding on.
///
/// Returns `true` if there was nothing to stop, or the supervisor is
/// confirmed stopped by the time this returns. Returns `false` only if a
/// SIGKILL was sent and the lock still could not be reacquired afterward
/// (should not happen in practice, but this is a best-effort teardown, not
/// a guarantee).
#[cfg(unix)]
pub async fn signal_stop_and_wait(work_dir: &Path, grace_period: Duration) -> bool {
    use nix::sys::signal::{self, Signal};
    use nix::unistd::{Pid, getpgid};

    let Some(pid) = live_supervisor_pid(work_dir) else {
        return true;
    };

    let nix_pid = Pid::from_raw(pid as i32);
    let pgid = getpgid(Some(nix_pid))
        .ok()
        .filter(|&pg| pg != Pid::from_raw(1));

    let term_ok = if let Some(pg) = pgid {
        signal::killpg(pg, Signal::SIGTERM)
            .or_else(|_| signal::kill(nix_pid, Signal::SIGTERM))
            .is_ok()
    } else {
        signal::kill(nix_pid, Signal::SIGTERM).is_ok()
    };

    if !term_ok {
        // Already gone (ESRCH) or genuinely unsignalable — either way,
        // nothing more we can do here.
        return live_supervisor_pid(work_dir).is_none();
    }

    let deadline = tokio::time::Instant::now() + grace_period;
    while tokio::time::Instant::now() < deadline {
        if live_supervisor_pid(work_dir).is_none() {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    if live_supervisor_pid(work_dir).is_none() {
        return true;
    }

    // Still alive after the grace period — escalate.
    if let Some(pg) = pgid {
        let _ =
            signal::killpg(pg, Signal::SIGKILL).or_else(|_| signal::kill(nix_pid, Signal::SIGKILL));
    } else {
        let _ = signal::kill(nix_pid, Signal::SIGKILL);
    }

    tokio::time::sleep(Duration::from_millis(300)).await;
    live_supervisor_pid(work_dir).is_none()
}

#[cfg(not(unix))]
pub async fn signal_stop_and_wait(_work_dir: &Path, _grace_period: Duration) -> bool {
    true
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_try_acquire_then_live_supervisor_pid_sees_it() {
        let temp_dir = TempDir::new().unwrap();
        let lock = try_acquire(temp_dir.path()).expect("first acquire should succeed");

        let seen = live_supervisor_pid(temp_dir.path());
        assert_eq!(
            seen,
            Some(std::process::id()),
            "a live holder's PID must be readable while the lock is held"
        );

        drop(lock);

        assert_eq!(
            live_supervisor_pid(temp_dir.path()),
            None,
            "after the guard drops (releasing the flock), no live supervisor should be reported"
        );
    }

    #[test]
    fn test_try_acquire_fails_while_already_held() {
        let temp_dir = TempDir::new().unwrap();
        let _lock = try_acquire(temp_dir.path()).expect("first acquire should succeed");

        let second = try_acquire(temp_dir.path());
        assert!(
            second.is_err(),
            "a second acquire attempt must fail while the first guard is alive"
        );
    }

    #[test]
    fn test_stale_lock_recovers_automatically_after_release() {
        let temp_dir = TempDir::new().unwrap();
        {
            let _lock = try_acquire(temp_dir.path()).expect("first acquire should succeed");
        } // dropped here, releasing the flock

        let second = try_acquire(temp_dir.path());
        assert!(
            second.is_ok(),
            "a fresh acquire must succeed once the previous guard has dropped \
             (stale-lock recovery falls out of flock's own process-exit semantics)"
        );
    }

    #[test]
    fn test_live_supervisor_pid_none_when_never_acquired() {
        let temp_dir = TempDir::new().unwrap();
        assert_eq!(live_supervisor_pid(temp_dir.path()), None);
    }
}
