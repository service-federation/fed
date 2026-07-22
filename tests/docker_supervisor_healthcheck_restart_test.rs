//! Integration test for `07-supervisor.md` Design §3's central Docker
//! claim, Phase 4: Docker's native `--restart` policy only reacts to the
//! container's own process exiting — it has no visibility into fed's
//! separate `healthcheck:` command feature. A Docker service with
//! `restart: always` (which gets the native `--restart unless-stopped`
//! flag) whose *healthcheck command* starts failing while its main process
//! keeps running must still be restarted — by fed's own monitoring, not
//! Docker's native policy, which structurally cannot fire here since the
//! container's process never exits on its own.
//!
//! Drives the real `fed` binary end-to-end (plain `fed start`, which spawns
//! the `fed supervise` daemon for a `restart: always` service — Phase 3),
//! the same cross-process contract used by `tests/supervisor_daemon_test.rs`.

use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{Duration, Instant};

fn is_docker_available() -> bool {
    std::process::Command::new("docker")
        .arg("version")
        .output()
        .map(|output| output.status.success())
        .unwrap_or(false)
}

macro_rules! require_docker {
    () => {
        if !is_docker_available() {
            eprintln!("Skipping test: Docker not available");
            return;
        }
    };
}

fn fed_binary() -> String {
    env!("CARGO_BIN_EXE_fed").to_string()
}

fn create_test_config(temp_dir: &tempfile::TempDir, content: &str) -> PathBuf {
    let config_path = temp_dir.path().join("fed.yaml");
    fs::write(&config_path, content).expect("Failed to write test config");
    config_path
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

fn combined(output: &std::process::Output) -> String {
    format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    )
}

fn open_lock_db(workdir: &Path) -> Option<rusqlite::Connection> {
    rusqlite::Connection::open(workdir.join(".fed/lock.db")).ok()
}

fn restart_count(workdir: &Path, service: &str) -> u64 {
    let Some(conn) = open_lock_db(workdir) else {
        return 0;
    };
    conn.query_row(
        "SELECT restart_count FROM services WHERE id = ?1",
        rusqlite::params![service],
        |row| row.get::<_, u32>(0),
    )
    .map(|c| c as u64)
    .unwrap_or(0)
}

fn container_id(workdir: &Path, service: &str) -> Option<String> {
    let conn = open_lock_db(workdir)?;
    conn.query_row(
        "SELECT container_id FROM services WHERE id = ?1",
        rusqlite::params![service],
        |row| row.get::<_, Option<String>>(0),
    )
    .ok()
    .flatten()
}

fn wait_for_restart_count_above(
    workdir: &Path,
    service: &str,
    above: u64,
    timeout: Duration,
) -> u64 {
    let start = Instant::now();
    loop {
        let count = restart_count(workdir, service);
        if count > above {
            return count;
        }
        assert!(
            start.elapsed() < timeout,
            "restart_count for '{}' did not exceed {} within {:?} (last seen: {})",
            service,
            above,
            timeout,
            count
        );
        std::thread::sleep(Duration::from_millis(500));
    }
}

fn docker_container_running(id: &str) -> bool {
    Command::new("docker")
        .args(["inspect", "--format={{.State.Running}}", id])
        .output()
        .ok()
        .filter(|o| o.status.success())
        .map(|o| String::from_utf8_lossy(&o.stdout).trim() == "true")
        .unwrap_or(false)
}

/// `sleep 300` never exits on its own — Docker's native `--restart
/// unless-stopped` (which this service also gets, since it's `restart:
/// always`) has no trigger to fire on. `healthcheck: "exit 1"` always
/// fails, so only fed's own healthcheck-driven restart can recover it.
const FLAKY_HEALTHCHECK_CONFIG: &str = r#"
services:
  flaky-health:
    image: alpine:latest
    command: ["sleep", "300"]
    healthcheck: "exit 1"
    restart: always
"#;

#[test]
#[cfg_attr(not(feature = "docker-tests"), ignore)] // Requires Docker
fn test_healthcheck_failure_restart_handled_by_fed_with_container_alive() {
    require_docker!();

    let temp_dir = tempfile::tempdir().unwrap();
    let workdir = temp_dir.path();
    let config_path = create_test_config(&temp_dir, FLAKY_HEALTHCHECK_CONFIG);

    let start = run_fed(&config_path, workdir, &["start", "flaky-health"]);
    assert!(start.status.success(), "start failed: {}", combined(&start));

    // Give the container time to actually come up before polling restarts.
    std::thread::sleep(Duration::from_secs(2));
    let first_container_id = container_id(workdir, "flaky-health");

    // Health checks tick every ~5s; the always-failing healthcheck should
    // trip a restart well within a couple of minutes. This is the direct
    // regression test for Design §3's "healthcheck-while-alive" gap: a
    // Docker-native-only implementation would never restart this
    // container, since its process never exits.
    let restarts =
        wait_for_restart_count_above(workdir, "flaky-health", 0, Duration::from_secs(120));
    assert!(
        restarts >= 1,
        "fed must have restarted the service due to the failing healthcheck"
    );

    // The restart must have gone through fed's own stop+start (a new
    // container), not Docker silently doing nothing — assert the
    // container id actually changed, i.e. fed really did recreate it, not
    // just increment a counter with no real action.
    let second_container_id = container_id(workdir, "flaky-health");
    if let (Some(first), Some(second)) = (&first_container_id, &second_container_id) {
        assert_ne!(
            first, second,
            "a fed-driven restart recreates the Docker container (stop+start), so the \
             container id must change across the restart"
        );
    }

    // And the (new) container must currently be alive — fed's restart
    // brought it back up, it didn't just leave it dead.
    if let Some(id) = &second_container_id {
        assert!(
            docker_container_running(id),
            "the container fed restarted into must be running"
        );
    }

    let _ = run_fed(&config_path, workdir, &["stop"]);
}
