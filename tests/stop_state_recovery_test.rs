use std::fs;
use std::path::Path;
use std::process::Command;
use std::time::{Duration, Instant};

fn create_test_config(
    temp_dir: &tempfile::TempDir,
    filename: &str,
    content: &str,
) -> std::path::PathBuf {
    let config_path = temp_dir.path().join(filename);
    fs::write(&config_path, content).expect("Failed to write test config");
    config_path
}

fn overwrite_config(config_path: &Path, content: &str) {
    fs::write(config_path, content).expect("Failed to overwrite test config");
}

fn fed_binary() -> String {
    env!("CARGO_BIN_EXE_fed").to_string()
}

fn run_fed(args: &[&str]) -> std::process::Output {
    Command::new(fed_binary())
        .args(args)
        .env("FED_NON_INTERACTIVE", "1")
        .output()
        .expect("Failed to run fed")
}

fn get_service_pid(workdir: &Path, service: &str) -> Option<u32> {
    let db_path = workdir.join(".fed/lock.db");
    let conn = rusqlite::Connection::open(db_path).ok()?;
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

const ONE_SERVICE_CONFIG: &str = r#"
services:
  alpha:
    process: |
      echo "alpha started"
      sleep 300
"#;

const DIFFERENT_VALID_CONFIG: &str = r#"
services:
  beta:
    process: echo "beta"
"#;

const TWO_SERVICE_CONFIG: &str = r#"
services:
  alpha:
    process: |
      echo "alpha started"
      sleep 300
  beta:
    process: |
      echo "beta started"
      sleep 300
"#;

const INVALID_CONFIG: &str = r#"
services:
  alpha:
    process: [this is not valid yaml
"#;

fn child_spawner_config(child_pid_file: &Path) -> String {
    format!(
        r#"
services:
  spawner:
    process: |
      sleep 300 &
      CHILD=$!
      echo $CHILD > {pid_file}
      wait
"#,
        pid_file = child_pid_file.display()
    )
}

#[test]
fn test_stop_stops_services_not_in_current_config() {
    let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
    let workdir = temp_dir.path();
    let config_path = create_test_config(&temp_dir, "config.yaml", ONE_SERVICE_CONFIG);

    let start = run_fed(&[
        "-c",
        config_path.to_str().unwrap(),
        "-w",
        workdir.to_str().unwrap(),
        "start",
        "alpha",
    ]);
    assert!(
        start.status.success(),
        "fed start failed: stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&start.stdout),
        String::from_utf8_lossy(&start.stderr),
    );

    std::thread::sleep(Duration::from_secs(1));
    let pid = get_service_pid(workdir, "alpha").expect("Expected alpha pid in lock db");
    assert!(is_pid_alive(pid), "Expected alpha PID {} alive", pid);

    // Simulate config change: alpha removed/renamed, but still running in state
    overwrite_config(&config_path, DIFFERENT_VALID_CONFIG);

    let stop = run_fed(&[
        "-c",
        config_path.to_str().unwrap(),
        "-w",
        workdir.to_str().unwrap(),
        "stop",
    ]);
    assert!(
        stop.status.success(),
        "fed stop failed: stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&stop.stdout),
        String::from_utf8_lossy(&stop.stderr),
    );

    assert!(
        wait_for_pid_exit(pid, Duration::from_secs(8)),
        "Expected alpha PID {} to exit after stop",
        pid
    );
}

#[test]
fn test_stop_from_state_does_not_clear_unstopped_services() {
    let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
    let workdir = temp_dir.path();
    let config_path = create_test_config(&temp_dir, "config.yaml", TWO_SERVICE_CONFIG);

    let start = run_fed(&[
        "-c",
        config_path.to_str().unwrap(),
        "-w",
        workdir.to_str().unwrap(),
        "start",
        "alpha",
        "beta",
    ]);
    assert!(
        start.status.success(),
        "fed start failed: stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&start.stdout),
        String::from_utf8_lossy(&start.stderr),
    );

    std::thread::sleep(Duration::from_secs(1));
    let alpha_pid = get_service_pid(workdir, "alpha").expect("Expected alpha pid");
    let beta_pid = get_service_pid(workdir, "beta").expect("Expected beta pid");
    assert!(is_pid_alive(alpha_pid), "Expected alpha PID alive");
    assert!(is_pid_alive(beta_pid), "Expected beta PID alive");

    // Make config invalid so `fed stop` falls back to state-tracker-only stop.
    overwrite_config(&config_path, INVALID_CONFIG);

    let stop_alpha = run_fed(&[
        "-c",
        config_path.to_str().unwrap(),
        "-w",
        workdir.to_str().unwrap(),
        "stop",
        "alpha",
    ]);
    assert!(
        stop_alpha.status.success(),
        "fed stop alpha failed: stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&stop_alpha.stdout),
        String::from_utf8_lossy(&stop_alpha.stderr),
    );

    assert!(
        wait_for_pid_exit(alpha_pid, Duration::from_secs(8)),
        "Expected alpha PID {} to exit after stop alpha",
        alpha_pid
    );
    assert!(is_pid_alive(beta_pid), "Expected beta to remain running");

    // Stopping all (still with invalid config) should stop beta too.
    let stop_all = run_fed(&[
        "-c",
        config_path.to_str().unwrap(),
        "-w",
        workdir.to_str().unwrap(),
        "stop",
    ]);
    assert!(
        stop_all.status.success(),
        "fed stop all failed: stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&stop_all.stdout),
        String::from_utf8_lossy(&stop_all.stderr),
    );

    assert!(
        wait_for_pid_exit(beta_pid, Duration::from_secs(8)),
        "Expected beta PID {} to exit after stop all",
        beta_pid
    );
}

/// When `fed stop` falls back to the state-tracker path (invalid config),
/// it must kill child processes, not just the tracked wrapper PID.
#[test]
fn test_stop_from_state_kills_child_processes() {
    let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
    let workdir = temp_dir.path();
    let child_pid_file = workdir.join("child.pid");
    let config_content = child_spawner_config(&child_pid_file);
    let config_path = create_test_config(&temp_dir, "config.yaml", &config_content);

    let start = run_fed(&[
        "-c",
        config_path.to_str().unwrap(),
        "-w",
        workdir.to_str().unwrap(),
        "start",
        "spawner",
    ]);
    assert!(
        start.status.success(),
        "fed start failed: stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&start.stdout),
        String::from_utf8_lossy(&start.stderr),
    );

    // Wait for the child PID file to be written
    let deadline = Instant::now() + Duration::from_secs(5);
    while !child_pid_file.exists() && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(100));
    }
    assert!(child_pid_file.exists(), "Child PID file was never written");

    let tracked_pid = get_service_pid(workdir, "spawner").expect("Expected spawner pid in lock db");
    let child_pid: u32 = fs::read_to_string(&child_pid_file)
        .expect("Failed to read child pid file")
        .trim()
        .parse()
        .expect("Failed to parse child pid");

    assert!(
        is_pid_alive(tracked_pid),
        "Expected tracked PID {} alive",
        tracked_pid
    );
    assert!(
        is_pid_alive(child_pid),
        "Expected child PID {} alive",
        child_pid
    );

    // Make config invalid to force state-based stop path
    overwrite_config(&config_path, INVALID_CONFIG);

    let stop = run_fed(&[
        "-c",
        config_path.to_str().unwrap(),
        "-w",
        workdir.to_str().unwrap(),
        "stop",
    ]);
    assert!(
        stop.status.success(),
        "fed stop failed: stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&stop.stdout),
        String::from_utf8_lossy(&stop.stderr),
    );

    assert!(
        wait_for_pid_exit(tracked_pid, Duration::from_secs(8)),
        "Expected tracked PID {} to exit after stop",
        tracked_pid,
    );
    assert!(
        wait_for_pid_exit(child_pid, Duration::from_secs(8)),
        "Expected child PID {} to exit after stop (orphaned child survived)",
        child_pid,
    );
}

/// When `fed stop` uses the normal orchestrator path (valid config),
/// `ProcessService::stop()` already kills process groups via `killpg`.
#[test]
fn test_orchestrator_stop_kills_child_processes() {
    let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
    let workdir = temp_dir.path();
    let child_pid_file = workdir.join("child.pid");
    let config_content = child_spawner_config(&child_pid_file);
    let config_path = create_test_config(&temp_dir, "config.yaml", &config_content);

    let start = run_fed(&[
        "-c",
        config_path.to_str().unwrap(),
        "-w",
        workdir.to_str().unwrap(),
        "start",
        "spawner",
    ]);
    assert!(
        start.status.success(),
        "fed start failed: stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&start.stdout),
        String::from_utf8_lossy(&start.stderr),
    );

    // Wait for the child PID file to be written
    let deadline = Instant::now() + Duration::from_secs(5);
    while !child_pid_file.exists() && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(100));
    }
    assert!(child_pid_file.exists(), "Child PID file was never written");

    let tracked_pid = get_service_pid(workdir, "spawner").expect("Expected spawner pid in lock db");
    let child_pid: u32 = fs::read_to_string(&child_pid_file)
        .expect("Failed to read child pid file")
        .trim()
        .parse()
        .expect("Failed to parse child pid");

    assert!(
        is_pid_alive(tracked_pid),
        "Expected tracked PID {} alive",
        tracked_pid
    );
    assert!(
        is_pid_alive(child_pid),
        "Expected child PID {} alive",
        child_pid
    );

    // Config stays valid — this uses the orchestrator (killpg) path
    let stop = run_fed(&[
        "-c",
        config_path.to_str().unwrap(),
        "-w",
        workdir.to_str().unwrap(),
        "stop",
    ]);
    assert!(
        stop.status.success(),
        "fed stop failed: stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&stop.stdout),
        String::from_utf8_lossy(&stop.stderr),
    );

    assert!(
        wait_for_pid_exit(tracked_pid, Duration::from_secs(8)),
        "Expected tracked PID {} to exit after stop",
        tracked_pid,
    );
    assert!(
        wait_for_pid_exit(child_pid, Duration::from_secs(8)),
        "Expected child PID {} to exit after stop (child survived killpg)",
        child_pid,
    );
}

/// Killing a package-manager-style launcher must not make its surviving
/// descendants invisible. A fresh start should use the persisted process
/// group to clean the old tree before registering a replacement.
#[test]
fn test_start_recovers_when_tracked_launcher_is_killed_but_child_survives() {
    let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
    let workdir = temp_dir.path();
    let child_pid_file = workdir.join("child.pid");
    let config_content = child_spawner_config(&child_pid_file);
    let config_path = create_test_config(&temp_dir, "config.yaml", &config_content);

    let first_start = run_fed(&[
        "-c",
        config_path.to_str().unwrap(),
        "-w",
        workdir.to_str().unwrap(),
        "start",
        "spawner",
    ]);
    assert!(
        first_start.status.success(),
        "initial fed start failed: stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&first_start.stdout),
        String::from_utf8_lossy(&first_start.stderr),
    );

    let deadline = Instant::now() + Duration::from_secs(5);
    while !child_pid_file.exists() && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(100));
    }
    assert!(child_pid_file.exists(), "Child PID file was never written");

    let tracked_pid = get_service_pid(workdir, "spawner").expect("tracked launcher PID");
    let orphaned_child_pid: u32 = fs::read_to_string(&child_pid_file)
        .unwrap()
        .trim()
        .parse()
        .unwrap();
    let process_group_id: u32 = rusqlite::Connection::open(workdir.join(".fed/lock.db"))
        .unwrap()
        .query_row(
            "SELECT process_group_id FROM services WHERE id = 'spawner'",
            [],
            |row| row.get(0),
        )
        .expect("persisted process group ID");
    assert!(process_group_id > 1);

    let killed = Command::new("kill")
        .args(["-9", &tracked_pid.to_string()])
        .status()
        .expect("kill tracked launcher");
    assert!(killed.success());
    assert!(
        wait_for_pid_exit(tracked_pid, Duration::from_secs(5)),
        "tracked launcher {} did not exit",
        tracked_pid
    );
    assert!(
        is_pid_alive(orphaned_child_pid),
        "the repro requires child {} to survive its launcher",
        orphaned_child_pid
    );

    let second_start = run_fed(&[
        "-c",
        config_path.to_str().unwrap(),
        "-w",
        workdir.to_str().unwrap(),
        "start",
        "spawner",
    ]);
    assert!(
        second_start.status.success(),
        "recovery start failed: stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&second_start.stdout),
        String::from_utf8_lossy(&second_start.stderr),
    );
    assert!(
        wait_for_pid_exit(orphaned_child_pid, Duration::from_secs(5)),
        "orphaned child {} from process group {} survived recovery",
        orphaned_child_pid,
        process_group_id
    );

    let replacement_pid =
        get_service_pid(workdir, "spawner").expect("replacement launcher should be tracked");
    assert_ne!(replacement_pid, tracked_pid);
    assert!(is_pid_alive(replacement_pid));

    let stop = run_fed(&[
        "-c",
        config_path.to_str().unwrap(),
        "-w",
        workdir.to_str().unwrap(),
        "stop",
    ]);
    assert!(
        stop.status.success(),
        "cleanup stop failed: stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&stop.stdout),
        String::from_utf8_lossy(&stop.stderr),
    );
}
