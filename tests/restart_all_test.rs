use std::fs;
use std::path::PathBuf;
use std::process::Command;
use std::time::Duration;
use tempfile::TempDir;

fn create_restart_test_config() -> (TempDir, PathBuf) {
    let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
    let config_path = temp_dir.path().join("test-config.yaml");

    let config_content = r#"
services:
  fast-service:
    process: |
      echo "Fast service starting"
      sleep 300

  slow-service:
    process: |
      echo "Slow service starting"
      sleep 300
"#;

    fs::write(&config_path, config_content).expect("Failed to write test config");
    (temp_dir, config_path)
}

fn fed_binary() -> String {
    env!("CARGO_BIN_EXE_fed").to_string()
}

#[test]
fn test_restart_all_without_args() {
    let (temp_dir, config_path) = create_restart_test_config();
    let workdir = temp_dir.path().to_str().unwrap();

    // Start all services
    let start_output = Command::new(fed_binary())
        .args(["-c", config_path.to_str().unwrap(), "-w", workdir, "start"])
        .output()
        .expect("Failed to start services");

    assert!(
        start_output.status.success(),
        "Start should succeed: {}",
        String::from_utf8_lossy(&start_output.stderr)
    );

    std::thread::sleep(Duration::from_secs(3));

    // Restart all services without args (this is the key test)
    let restart_output = Command::new(fed_binary())
        .args([
            "-c",
            config_path.to_str().unwrap(),
            "-w",
            workdir,
            "restart",
        ])
        .output()
        .expect("Failed to restart all services");

    println!(
        "Restart output:\n{}",
        String::from_utf8_lossy(&restart_output.stdout)
    );
    println!(
        "Restart stderr:\n{}",
        String::from_utf8_lossy(&restart_output.stderr)
    );

    assert!(
        restart_output.status.success(),
        "Restart all should succeed"
    );

    // Verify it mentions restarting "all services"
    let restart_text = String::from_utf8_lossy(&restart_output.stdout);
    assert!(
        restart_text.contains("all services") || restart_text.contains("dependency-aware"),
        "Should indicate restarting all services: {}",
        restart_text
    );

    std::thread::sleep(Duration::from_secs(3));

    // Verify services are running after restart
    let status_after = Command::new(fed_binary())
        .args(["-c", config_path.to_str().unwrap(), "-w", workdir, "status"])
        .output()
        .expect("Failed to get status");

    let status_after_text = String::from_utf8_lossy(&status_after.stdout);
    println!("Status after restart:\n{}", status_after_text);

    assert!(
        status_after_text.contains("fast-service"),
        "fast-service should be running after restart"
    );
    assert!(
        status_after_text.contains("slow-service"),
        "slow-service should be running after restart"
    );

    // Cleanup
    Command::new(fed_binary())
        .args(["-c", config_path.to_str().unwrap(), "-w", workdir, "stop"])
        .output()
        .expect("Failed to stop");
}

#[test]
fn test_restart_specific_service_still_works() {
    let (temp_dir, config_path) = create_restart_test_config();
    let workdir = temp_dir.path().to_str().unwrap();

    // Start all services
    Command::new(fed_binary())
        .args(["-c", config_path.to_str().unwrap(), "-w", workdir, "start"])
        .output()
        .expect("Failed to start services");

    std::thread::sleep(Duration::from_secs(2));

    // Restart specific service (old behavior should still work)
    let restart_output = Command::new(fed_binary())
        .args([
            "-c",
            config_path.to_str().unwrap(),
            "-w",
            workdir,
            "restart",
            "fast-service",
        ])
        .output()
        .expect("Failed to restart specific service");

    assert!(
        restart_output.status.success(),
        "Restart specific service should succeed"
    );

    let restart_text = String::from_utf8_lossy(&restart_output.stdout);
    assert!(
        restart_text.contains("fast-service"),
        "Should restart the specified service"
    );

    // Verify service still running
    let status_output = Command::new(fed_binary())
        .args(["-c", config_path.to_str().unwrap(), "-w", workdir, "status"])
        .output()
        .expect("Failed to get status");

    let status = String::from_utf8_lossy(&status_output.stdout);
    assert!(
        status.contains("fast-service"),
        "fast-service should still be running"
    );
    // Untargeted services must be untouched by a selective restart.
    assert!(
        status.contains("slow-service"),
        "slow-service should be untouched after restarting fast-service: {}",
        status
    );

    // Cleanup
    Command::new(fed_binary())
        .args(["-c", config_path.to_str().unwrap(), "-w", workdir, "stop"])
        .output()
        .expect("Failed to stop");
}

/// Two-service config: `dependent` depends on `dependency`.
fn create_chain_config() -> (TempDir, PathBuf) {
    let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
    let config_path = temp_dir.path().join("test-config.yaml");

    let config_content = r#"
services:
  dependency:
    process: sleep 300

  dependent:
    process: sleep 300
    depends_on:
      - dependency
"#;

    fs::write(&config_path, config_content).expect("Failed to write test config");
    (temp_dir, config_path)
}

/// Build a 3-service chain: A <- B <- C (B depends on A, C depends on B).
fn create_three_level_chain_config() -> (TempDir, PathBuf) {
    let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
    let config_path = temp_dir.path().join("test-config.yaml");

    let config_content = r#"
services:
  svc-a:
    process: sleep 300

  svc-b:
    process: sleep 300
    depends_on:
      - svc-a

  svc-c:
    process: sleep 300
    depends_on:
      - svc-b
"#;

    fs::write(&config_path, config_content).expect("Failed to write test config");
    (temp_dir, config_path)
}

fn count_running(status: &str) -> usize {
    status
        .lines()
        .filter(|line| line.contains("running") || line.contains("healthy"))
        .count()
}

/// `fed restart <svc>` must leave any dependent that was running when the
/// restart began running afterwards. Previously the `stop` cascade took
/// dependents down and `start` never brought them back.
#[test]
fn test_restart_brings_back_direct_dependent() {
    let (temp_dir, config_path) = create_chain_config();
    let workdir = temp_dir.path().to_str().unwrap();

    // Pass the dependent explicitly so `fed start` has an entrypoint;
    // `dependency` comes up automatically as its transitive dep.
    let start_output = Command::new(fed_binary())
        .args([
            "-c",
            config_path.to_str().unwrap(),
            "-w",
            workdir,
            "start",
            "dependent",
        ])
        .output()
        .expect("Failed to start services");
    assert!(
        start_output.status.success(),
        "Start should succeed: {}",
        String::from_utf8_lossy(&start_output.stderr)
    );

    std::thread::sleep(Duration::from_secs(2));

    let before = Command::new(fed_binary())
        .args(["-c", config_path.to_str().unwrap(), "-w", workdir, "status"])
        .output()
        .expect("status");
    let before_text = String::from_utf8_lossy(&before.stdout);
    assert_eq!(
        count_running(&before_text),
        2,
        "expected 2 running before restart; got status:\n{}",
        before_text
    );

    let restart_output = Command::new(fed_binary())
        .args([
            "-c",
            config_path.to_str().unwrap(),
            "-w",
            workdir,
            "restart",
            "dependency",
        ])
        .output()
        .expect("Failed to restart");
    assert!(
        restart_output.status.success(),
        "Restart should succeed: {}",
        String::from_utf8_lossy(&restart_output.stderr)
    );

    std::thread::sleep(Duration::from_secs(2));

    let after = Command::new(fed_binary())
        .args(["-c", config_path.to_str().unwrap(), "-w", workdir, "status"])
        .output()
        .expect("status");
    let after_text = String::from_utf8_lossy(&after.stdout);
    assert_eq!(
        count_running(&after_text),
        2,
        "both services should be Running after restart; got:\n{}",
        after_text
    );

    Command::new(fed_binary())
        .args(["-c", config_path.to_str().unwrap(), "-w", workdir, "stop"])
        .output()
        .expect("stop");
}

/// Three-level dependency chain. Restarting the root must bring back ALL
/// transitive dependents, not just direct ones.
#[test]
fn test_restart_brings_back_transitive_dependents() {
    let (temp_dir, config_path) = create_three_level_chain_config();
    let workdir = temp_dir.path().to_str().unwrap();

    let start_output = Command::new(fed_binary())
        .args([
            "-c",
            config_path.to_str().unwrap(),
            "-w",
            workdir,
            "start",
            "svc-c",
        ])
        .output()
        .expect("Failed to start services");
    assert!(
        start_output.status.success(),
        "Start should succeed: stdout={}, stderr={}",
        String::from_utf8_lossy(&start_output.stdout),
        String::from_utf8_lossy(&start_output.stderr)
    );

    std::thread::sleep(Duration::from_secs(3));

    let restart_output = Command::new(fed_binary())
        .args([
            "-c",
            config_path.to_str().unwrap(),
            "-w",
            workdir,
            "restart",
            "svc-a",
        ])
        .output()
        .expect("Failed to restart");
    assert!(
        restart_output.status.success(),
        "Restart should succeed: {}",
        String::from_utf8_lossy(&restart_output.stderr)
    );

    std::thread::sleep(Duration::from_secs(3));

    let after = Command::new(fed_binary())
        .args(["-c", config_path.to_str().unwrap(), "-w", workdir, "status"])
        .output()
        .expect("status");
    let after_text = String::from_utf8_lossy(&after.stdout);
    assert_eq!(
        count_running(&after_text),
        3,
        "all three services should be Running after restart svc-a; got:\n{}",
        after_text
    );

    Command::new(fed_binary())
        .args(["-c", config_path.to_str().unwrap(), "-w", workdir, "stop"])
        .output()
        .expect("stop");
}

/// If a dependent was already stopped when restart began, it must stay
/// stopped — restart restores running state, it does not resurrect.
#[test]
fn test_restart_with_dependent_already_stopped_does_not_revive_it() {
    let (temp_dir, config_path) = create_chain_config();
    let workdir = temp_dir.path().to_str().unwrap();

    let start_output = Command::new(fed_binary())
        .args([
            "-c",
            config_path.to_str().unwrap(),
            "-w",
            workdir,
            "start",
            "dependent",
        ])
        .output()
        .expect("Failed to start services");
    assert!(
        start_output.status.success(),
        "Start should succeed: stdout={}, stderr={}",
        String::from_utf8_lossy(&start_output.stdout),
        String::from_utf8_lossy(&start_output.stderr)
    );
    std::thread::sleep(Duration::from_secs(2));

    // Stop the dependent first; it has no dependents of its own, so this
    // takes down only the named service.
    Command::new(fed_binary())
        .args([
            "-c",
            config_path.to_str().unwrap(),
            "-w",
            workdir,
            "stop",
            "dependent",
        ])
        .output()
        .expect("stop dependent");
    std::thread::sleep(Duration::from_secs(1));

    let restart_output = Command::new(fed_binary())
        .args([
            "-c",
            config_path.to_str().unwrap(),
            "-w",
            workdir,
            "restart",
            "dependency",
        ])
        .output()
        .expect("restart dependency");
    assert!(
        restart_output.status.success(),
        "Restart should succeed: {}",
        String::from_utf8_lossy(&restart_output.stderr)
    );
    std::thread::sleep(Duration::from_secs(2));

    let after = Command::new(fed_binary())
        .args(["-c", config_path.to_str().unwrap(), "-w", workdir, "status"])
        .output()
        .expect("status");
    let after_text = String::from_utf8_lossy(&after.stdout);

    // Only `dependency` should be Running. `dependent` should stay stopped.
    assert_eq!(
        count_running(&after_text),
        1,
        "only dependency should be Running; dependent should stay stopped. Got:\n{}",
        after_text
    );

    Command::new(fed_binary())
        .args(["-c", config_path.to_str().unwrap(), "-w", workdir, "stop"])
        .output()
        .expect("stop");
}

#[test]
fn test_restart_all_empty_initially() {
    let (temp_dir, config_path) = create_restart_test_config();
    let workdir = temp_dir.path().to_str().unwrap();

    // Restart all when nothing is running should still work
    let restart_output = Command::new(fed_binary())
        .args([
            "-c",
            config_path.to_str().unwrap(),
            "-w",
            workdir,
            "restart",
        ])
        .output()
        .expect("Failed to restart all");

    // Should succeed and start all services
    assert!(
        restart_output.status.success(),
        "Restart all should succeed: {}",
        String::from_utf8_lossy(&restart_output.stderr)
    );

    std::thread::sleep(Duration::from_secs(3));

    // Verify services started
    let status_output = Command::new(fed_binary())
        .args(["-c", config_path.to_str().unwrap(), "-w", workdir, "status"])
        .output()
        .expect("Failed to get status");

    let status = String::from_utf8_lossy(&status_output.stdout);
    assert!(
        status.contains("fast-service") && status.contains("slow-service"),
        "services should be in status after restart all: {}",
        status
    );

    // Cleanup
    Command::new(fed_binary())
        .args(["-c", config_path.to_str().unwrap(), "-w", workdir, "stop"])
        .output()
        .expect("Failed to stop");
}

#[test]
fn test_restart_all_with_dependencies() {
    let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
    let config_path = temp_dir.path().join("test-config.yaml");

    // Config with service dependencies
    let config_content = r#"
services:
  dependency:
    process: sleep 300

  dependent:
    process: sleep 300
    depends_on:
      - dependency
"#;

    fs::write(&config_path, config_content).expect("Failed to write test config");
    let workdir = temp_dir.path().to_str().unwrap();

    // Start all services
    let start_output = Command::new(fed_binary())
        .args(["-c", config_path.to_str().unwrap(), "-w", workdir, "start"])
        .output()
        .expect("Failed to start services");

    assert!(
        start_output.status.success(),
        "Start should succeed: {}",
        String::from_utf8_lossy(&start_output.stderr)
    );

    std::thread::sleep(Duration::from_secs(2));

    // Restart all - should respect dependency order
    let restart_output = Command::new(fed_binary())
        .args([
            "-c",
            config_path.to_str().unwrap(),
            "-w",
            workdir,
            "restart",
        ])
        .output()
        .expect("Failed to restart all");

    assert!(
        restart_output.status.success(),
        "Restart all with dependencies should succeed"
    );

    std::thread::sleep(Duration::from_secs(2));

    // Verify both services are running
    let status_output = Command::new(fed_binary())
        .args(["-c", config_path.to_str().unwrap(), "-w", workdir, "status"])
        .output()
        .expect("Failed to get status");

    let status = String::from_utf8_lossy(&status_output.stdout);
    assert!(
        status.contains("dependency"),
        "dependency should be running"
    );
    assert!(status.contains("dependent"), "dependent should be running");

    // Cleanup
    Command::new(fed_binary())
        .args(["-c", config_path.to_str().unwrap(), "-w", workdir, "stop"])
        .output()
        .expect("Failed to stop");
}

#[test]
fn test_restart_all_preserves_services() {
    let (temp_dir, config_path) = create_restart_test_config();
    let workdir = temp_dir.path().to_str().unwrap();

    // Start services
    let start_output = Command::new(fed_binary())
        .args(["-c", config_path.to_str().unwrap(), "-w", workdir, "start"])
        .output()
        .expect("Failed to start services");

    assert!(
        start_output.status.success(),
        "Start should succeed: {}",
        String::from_utf8_lossy(&start_output.stderr)
    );

    std::thread::sleep(Duration::from_secs(3));

    // Restart all
    let restart_output = Command::new(fed_binary())
        .args([
            "-c",
            config_path.to_str().unwrap(),
            "-w",
            workdir,
            "restart",
        ])
        .output()
        .expect("Failed to restart all");

    assert!(
        restart_output.status.success(),
        "Restart all should succeed: {}",
        String::from_utf8_lossy(&restart_output.stderr)
    );

    std::thread::sleep(Duration::from_secs(3));

    // Verify services still exist and can be stopped
    let stop_output = Command::new(fed_binary())
        .args(["-c", config_path.to_str().unwrap(), "-w", workdir, "stop"])
        .output()
        .expect("Failed to stop");

    assert!(stop_output.status.success(), "Stop should succeed");

    // Verify stopped
    let status_output = Command::new(fed_binary())
        .args(["-c", config_path.to_str().unwrap(), "-w", workdir, "status"])
        .output()
        .expect("Failed to get status");

    let status = String::from_utf8_lossy(&status_output.stdout);
    // After stop, services should either not appear or show as stopped
    let has_running = status.contains("running") || status.contains("healthy");
    assert!(!has_running, "No services should be running after stop");
}
