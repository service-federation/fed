//! Regression tests for the persisted `desired_state` column
//! (`07-supervisor.md` Design §1, Phase 1: the state foundation only — no
//! supervisor consumes this column yet).
//!
//! These drive the real `fed` binary across separate process invocations to
//! verify the actual cross-process contract: registration marks a service's
//! intent as `running`, and every stop path writes `stopped` *before* the
//! kill signal lands — not after, and not only once the process has fully
//! exited. A future restart-policy supervisor (a separate OS process) will
//! consult exactly this column instead of any in-process manager object.

use std::fs;
use std::path::Path;
use std::process::Command;
use std::time::{Duration, Instant};

fn fed_binary() -> String {
    env!("CARGO_BIN_EXE_fed").to_string()
}

fn create_test_config(temp_dir: &tempfile::TempDir, content: &str) -> std::path::PathBuf {
    let config_path = temp_dir.path().join("fed.yaml");
    fs::write(&config_path, content).expect("Failed to write test config");
    config_path
}

fn overwrite_config(config_path: &Path, content: &str) {
    fs::write(config_path, content).expect("Failed to overwrite test config");
}

fn run_fed(config_path: &Path, workdir: &Path, args: &[&str]) -> std::process::Output {
    Command::new(fed_binary())
        .arg("-c")
        .arg(config_path)
        .arg("-w")
        .arg(workdir)
        .args(args)
        .env("FED_NON_INTERACTIVE", "1")
        .output()
        .expect("Failed to run fed")
}

fn spawn_fed(config_path: &Path, workdir: &Path, args: &[&str]) -> std::process::Child {
    Command::new(fed_binary())
        .arg("-c")
        .arg(config_path)
        .arg("-w")
        .arg(workdir)
        .args(args)
        .env("FED_NON_INTERACTIVE", "1")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .expect("Failed to spawn fed")
}

fn combined(output: &std::process::Output) -> String {
    format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    )
}

fn open_lock_db(workdir: &Path) -> Option<rusqlite::Connection> {
    let db_path = workdir.join(".fed/lock.db");
    rusqlite::Connection::open(db_path).ok()
}

/// Read the raw `desired_state` column for a service, or `None` if the row
/// doesn't exist (e.g. it's already been unregistered).
fn get_desired_state(workdir: &Path, service: &str) -> Option<String> {
    let conn = open_lock_db(workdir)?;
    conn.query_row(
        "SELECT desired_state FROM services WHERE id = ?1",
        rusqlite::params![service],
        |row| row.get::<_, String>(0),
    )
    .ok()
}

fn get_service_pid(workdir: &Path, service: &str) -> Option<u32> {
    let conn = open_lock_db(workdir)?;
    conn.query_row(
        "SELECT pid FROM services WHERE id = ?1",
        rusqlite::params![service],
        |row| row.get::<_, Option<u32>>(0),
    )
    .ok()
    .flatten()
}

fn is_pid_alive(pid: u32) -> bool {
    Command::new("kill")
        .args(["-0", &pid.to_string()])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

fn wait_for_pid_exit(pid: u32, timeout: Duration) -> bool {
    let start = Instant::now();
    while start.elapsed() < timeout {
        if !is_pid_alive(pid) {
            return true;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    !is_pid_alive(pid)
}

fn wait_for_child_exit(child: &mut std::process::Child, timeout: Duration) -> bool {
    let start = Instant::now();
    while start.elapsed() < timeout {
        if let Ok(Some(_)) = child.try_wait() {
            return true;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    matches!(child.try_wait(), Ok(Some(_)))
}

/// A service that ignores SIGTERM outright (`trap '' TERM`), so it only ever
/// dies via SIGKILL after the (short, here) `grace_period` elapses. This
/// gives a wide, deterministic window to observe `desired_state` mid-stop
/// without racing exact timing — the process is *guaranteed* to still be
/// alive for the whole grace period, not just "probably still alive for a
/// moment". `trap '' TERM` sets SIGTERM to be ignored (`SIG_IGN`), which
/// survives both `fork()` (child inherits it) and `exec()` (ignored, as
/// opposed to caught, dispositions are preserved) — so this holds regardless
/// of which process ends up as the tracked PID.
const STUBBORN_CONFIG: &str = r#"
services:
  stubborn:
    process: |
      trap '' TERM
      sleep 300
    grace_period: 1s
"#;

const TWO_STUBBORN_CONFIG: &str = r#"
services:
  stubborn-a:
    process: |
      trap '' TERM
      sleep 300
    grace_period: 1s
  stubborn-b:
    process: |
      trap '' TERM
      sleep 300
    grace_period: 1s
"#;

const SIMPLE_CONFIG: &str = r#"
services:
  quick:
    process: |
      sleep 300
"#;

const INVALID_CONFIG: &str = r#"
services:
  stubborn:
    process: [this is not valid yaml
"#;

/// Registration marks intent as `running` — the read half of the round trip.
#[test]
fn test_start_marks_desired_state_running() {
    let temp_dir = tempfile::tempdir().unwrap();
    let workdir = temp_dir.path();
    let config_path = create_test_config(&temp_dir, SIMPLE_CONFIG);

    let start = run_fed(&config_path, workdir, &["start", "quick"]);
    assert!(start.status.success(), "start failed: {}", combined(&start));

    let desired = get_desired_state(workdir, "quick");
    assert_eq!(
        desired.as_deref(),
        Some("running"),
        "freshly registered service should be desired_state='running'"
    );

    let pid = get_service_pid(workdir, "quick").expect("expected a tracked pid");
    let _ = run_fed(&config_path, workdir, &["stop"]);
    wait_for_pid_exit(pid, Duration::from_secs(8));
}

/// The critical semantic test: `desired_state` flips to 'stopped' *before*
/// the kill signal takes effect, not merely by the time `fed stop` returns.
/// Without the write-before-kill ordering fix, this column would still read
/// 'running' while the process is visibly still alive and being torn down.
#[test]
fn test_stop_writes_stopped_before_process_exits() {
    let temp_dir = tempfile::tempdir().unwrap();
    let workdir = temp_dir.path();
    let config_path = create_test_config(&temp_dir, STUBBORN_CONFIG);

    let start = run_fed(&config_path, workdir, &["start", "stubborn"]);
    assert!(start.status.success(), "start failed: {}", combined(&start));

    let pid = get_service_pid(workdir, "stubborn").expect("expected a tracked pid");
    assert!(is_pid_alive(pid), "expected stubborn pid {} alive", pid);
    assert_eq!(
        get_desired_state(workdir, "stubborn").as_deref(),
        Some("running")
    );

    // Don't wait for `fed stop` to return — it blocks until the process
    // actually exits (SIGTERM is ignored, so this takes the full grace
    // period plus SIGKILL, ~1-1.5s here). Poll the DB while it's in flight.
    let mut stop_child = spawn_fed(&config_path, workdir, &["stop", "stubborn"]);

    let mut observed_stopped_while_alive = false;
    let deadline = Instant::now() + Duration::from_secs(3);
    while Instant::now() < deadline {
        let desired = get_desired_state(workdir, "stubborn");
        if desired.as_deref() == Some("stopped") {
            // Caught it — and the tracked process must still be alive right
            // now, proving the write landed before (or without waiting for)
            // the kill to actually take effect.
            if is_pid_alive(pid) {
                observed_stopped_while_alive = true;
            }
            break;
        }
        std::thread::sleep(Duration::from_millis(20));
    }

    assert!(
        observed_stopped_while_alive,
        "expected to observe desired_state='stopped' while pid {} was still alive \
         (write-before-kill ordering regression)",
        pid
    );

    assert!(
        wait_for_child_exit(&mut stop_child, Duration::from_secs(10)),
        "fed stop did not finish in time"
    );
    assert!(
        wait_for_pid_exit(pid, Duration::from_secs(2)),
        "stubborn pid {} should be dead once fed stop returns",
        pid
    );

    // A normal successful stop unregisters (deletes) the row entirely — the
    // column having been 'stopped' is what mattered, not that a row lingers.
    assert!(
        get_desired_state(workdir, "stubborn").is_none(),
        "row should be unregistered after a successful stop"
    );
}

/// Whole-project `fed stop` (no service names) quiesces every service's
/// desired_state in a single upfront batch before killing anything — not
/// interleaved per-service. Verified by observing both rows flip to
/// 'stopped' together, before either process has necessarily exited.
#[test]
fn test_whole_project_stop_batches_desired_state_before_kills() {
    let temp_dir = tempfile::tempdir().unwrap();
    let workdir = temp_dir.path();
    let config_path = create_test_config(&temp_dir, TWO_STUBBORN_CONFIG);

    let start = run_fed(
        &config_path,
        workdir,
        &["start", "stubborn-a", "stubborn-b"],
    );
    assert!(start.status.success(), "start failed: {}", combined(&start));

    let pid_a = get_service_pid(workdir, "stubborn-a").expect("expected pid for a");
    let pid_b = get_service_pid(workdir, "stubborn-b").expect("expected pid for b");
    assert!(is_pid_alive(pid_a) && is_pid_alive(pid_b));

    let mut stop_child = spawn_fed(&config_path, workdir, &["stop"]);

    // Poll soon after spawning: the batch write happens before `stop_all()`
    // is even called, so both rows should show 'stopped' well before either
    // process has necessarily received (let alone finished reacting to) its
    // kill signal.
    let mut both_stopped_early = false;
    let deadline = Instant::now() + Duration::from_secs(3);
    while Instant::now() < deadline {
        let a = get_desired_state(workdir, "stubborn-a");
        let b = get_desired_state(workdir, "stubborn-b");
        if a.as_deref() == Some("stopped") && b.as_deref() == Some("stopped") {
            both_stopped_early = true;
            break;
        }
        std::thread::sleep(Duration::from_millis(20));
    }

    assert!(
        both_stopped_early,
        "expected both services' desired_state to flip to 'stopped' together \
         (single upfront batch, not interleaved per-service writes)"
    );

    assert!(
        wait_for_child_exit(&mut stop_child, Duration::from_secs(12)),
        "fed stop did not finish in time"
    );
    wait_for_pid_exit(pid_a, Duration::from_secs(2));
    wait_for_pid_exit(pid_b, Duration::from_secs(2));
}

/// The config-can't-load fallback (`run_stop_from_state`) must write
/// `desired_state='stopped'` before killing too — this is the gap the
/// original design review flagged: without it, a `fed stop` run against a
/// broken config would never touch the intent column at all.
#[test]
fn test_stop_from_state_fallback_writes_stopped_before_kill() {
    let temp_dir = tempfile::tempdir().unwrap();
    let workdir = temp_dir.path();
    let config_path = create_test_config(&temp_dir, STUBBORN_CONFIG);

    let start = run_fed(&config_path, workdir, &["start", "stubborn"]);
    assert!(start.status.success(), "start failed: {}", combined(&start));

    let pid = get_service_pid(workdir, "stubborn").expect("expected a tracked pid");
    assert!(is_pid_alive(pid));

    // Break the config so `fed stop` is forced onto the state-only fallback.
    overwrite_config(&config_path, INVALID_CONFIG);

    let mut stop_child = spawn_fed(&config_path, workdir, &["stop"]);

    let mut observed_stopped_while_alive = false;
    let deadline = Instant::now() + Duration::from_secs(3);
    while Instant::now() < deadline {
        if get_desired_state(workdir, "stubborn").as_deref() == Some("stopped") {
            if is_pid_alive(pid) {
                observed_stopped_while_alive = true;
            }
            break;
        }
        std::thread::sleep(Duration::from_millis(20));
    }

    assert!(
        observed_stopped_while_alive,
        "expected the state-only fallback to write desired_state='stopped' \
         before pid {} exited",
        pid
    );

    assert!(
        wait_for_child_exit(&mut stop_child, Duration::from_secs(10)),
        "fed stop (fallback) did not finish in time"
    );
    wait_for_pid_exit(pid, Duration::from_secs(2));
}

/// After a stop-then-start cycle, the newly registered row's desired_state
/// is 'running' again — the read half of the round trip, on the far side of
/// a real stop.
#[test]
fn test_restart_after_stop_resets_desired_state_to_running() {
    let temp_dir = tempfile::tempdir().unwrap();
    let workdir = temp_dir.path();
    let config_path = create_test_config(&temp_dir, SIMPLE_CONFIG);

    let start = run_fed(&config_path, workdir, &["start", "quick"]);
    assert!(start.status.success(), "start failed: {}", combined(&start));

    let stop = run_fed(&config_path, workdir, &["stop", "quick"]);
    assert!(stop.status.success(), "stop failed: {}", combined(&stop));
    assert!(
        get_desired_state(workdir, "quick").is_none(),
        "row should be gone after a successful stop"
    );

    let restart = run_fed(&config_path, workdir, &["start", "quick"]);
    assert!(
        restart.status.success(),
        "restart failed: {}",
        combined(&restart)
    );

    assert_eq!(
        get_desired_state(workdir, "quick").as_deref(),
        Some("running"),
        "restarted service should be desired_state='running' again"
    );

    let pid = get_service_pid(workdir, "quick").expect("expected a tracked pid");
    let _ = run_fed(&config_path, workdir, &["stop"]);
    wait_for_pid_exit(pid, Duration::from_secs(8));
}
