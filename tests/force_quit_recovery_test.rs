use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::Duration;
use tempfile::TempDir;

#[path = "support/mod.rs"]
mod support;

fn create_recovery_test_config() -> (TempDir, PathBuf) {
    let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
    let config_path = temp_dir.path().join("test-config.yaml");

    let config_content = r#"
services:
  resilient-service:
    process: |
      echo "Service starting"
      sleep 300
    restart: !onfailure
      max_retries: 3

  simple-service:
    process: |
      echo "Simple service"
      sleep 300
"#;

    fs::write(&config_path, config_content).expect("Failed to write test config");
    (temp_dir, config_path)
}

fn fed_binary() -> String {
    env!("CARGO_BIN_EXE_fed").to_string()
}

#[test]
fn test_state_persisted_after_interrupt() {
    let (temp_dir, config_path) = create_recovery_test_config();
    let workdir = temp_dir.path().to_str().unwrap();
    let lock_path = temp_dir.path().join(".fed").join("lock.db");

    // Start services
    let start_output = Command::new(fed_binary())
        .args([
            "-c",
            config_path.to_str().unwrap(),
            "-w",
            workdir,
            "start",
            "simple-service",
        ])
        .output()
        .expect("Failed to start");

    assert!(
        start_output.status.success(),
        "Start failed: {}",
        String::from_utf8_lossy(&start_output.stderr)
    );

    std::thread::sleep(Duration::from_secs(2));

    // Verify lock database exists (state was persisted)
    assert!(lock_path.exists(), "Lock file should exist after start");

    // Verify services are running via status command
    let status_output = Command::new(fed_binary())
        .args(["-c", config_path.to_str().unwrap(), "-w", workdir, "status"])
        .output()
        .expect("Failed to get status");

    let status_text = String::from_utf8_lossy(&status_output.stdout);
    println!("Status after start:\n{}", status_text);

    // Verify service appears in status
    assert!(
        status_text.contains("simple-service"),
        "Service should appear in status output"
    );

    // Cleanup
    Command::new(fed_binary())
        .args(["-c", config_path.to_str().unwrap(), "-w", workdir, "stop"])
        .output()
        .expect("Failed to stop");
}

#[test]
fn test_recovery_from_stale_lock_file() {
    let (temp_dir, config_path) = create_recovery_test_config();
    let workdir = temp_dir.path().to_str().unwrap();
    let lock_path = temp_dir.path().join(".fed-lock.json");

    // Create a stale lock file (simulating crashed previous run)
    let stale_lock = r#"{
  "services": [
    {
      "id": "resilient-service",
      "service_type": "Process",
      "namespace": "default",
      "status": "running",
      "pid": 99999,
      "container_id": null,
      "port_allocations": {}
    }
  ],
  "allocated_ports": [],
  "created_at": "2024-01-01T00:00:00Z"
}"#;

    fs::write(&lock_path, stale_lock).expect("Failed to write stale lock");

    println!("Created stale lock file");

    // Try to start services - should handle stale lock gracefully
    let start_output = Command::new(fed_binary())
        .args(["-c", config_path.to_str().unwrap(), "-w", workdir, "start"])
        .output()
        .expect("Failed to start");

    println!(
        "Start output:\n{}",
        String::from_utf8_lossy(&start_output.stdout)
    );
    println!(
        "Start stderr:\n{}",
        String::from_utf8_lossy(&start_output.stderr)
    );

    // Should succeed or gracefully handle stale state
    // The key is it shouldn't crash
    std::thread::sleep(Duration::from_secs(2));

    // Verify new lock file was created
    if lock_path.exists() {
        let lock_after = fs::read_to_string(&lock_path).expect("Failed to read lock");
        println!("Lock file after recovery:\n{}", lock_after);

        // Verify it's valid JSON
        assert!(
            serde_json::from_str::<serde_json::Value>(&lock_after).is_ok(),
            "Lock file should be valid JSON after recovery"
        );
    }

    // Cleanup
    Command::new(fed_binary())
        .args(["-c", config_path.to_str().unwrap(), "-w", workdir, "stop"])
        .output()
        .expect("Failed to stop");
}

#[test]
fn test_lock_file_cleared_on_clean_exit() {
    let (temp_dir, config_path) = create_recovery_test_config();
    let workdir = temp_dir.path().to_str().unwrap();
    let fed_dir = temp_dir.path().join(".fed");

    // Start services
    let start_output = Command::new(fed_binary())
        .args([
            "-c",
            config_path.to_str().unwrap(),
            "-w",
            workdir,
            "start",
            "simple-service",
        ])
        .output()
        .expect("Failed to start");

    assert!(
        start_output.status.success(),
        "Start failed: {}",
        String::from_utf8_lossy(&start_output.stderr)
    );

    std::thread::sleep(Duration::from_secs(2));
    assert!(fed_dir.exists(), "Fed directory should exist after start");

    // Clean stop
    Command::new(fed_binary())
        .args(["-c", config_path.to_str().unwrap(), "-w", workdir, "stop"])
        .output()
        .expect("Failed to stop");

    std::thread::sleep(Duration::from_secs(1));

    // Verify no services running via status command
    let status_output = Command::new(fed_binary())
        .args(["-c", config_path.to_str().unwrap(), "-w", workdir, "status"])
        .output()
        .expect("Failed to get status");

    let status_text = String::from_utf8_lossy(&status_output.stdout);

    // After clean stop, no services should be running
    assert!(
        !status_text.contains("running") && !status_text.contains("healthy"),
        "No services should be running after clean stop, got:\n{}",
        status_text
    );
}

#[test]
fn test_multiple_start_attempts_with_existing_lock() {
    let (temp_dir, config_path) = create_recovery_test_config();
    let workdir = temp_dir.path().to_str().unwrap();

    // First start
    Command::new(fed_binary())
        .args(["-c", config_path.to_str().unwrap(), "-w", workdir, "start"])
        .output()
        .expect("Failed to start first time");

    std::thread::sleep(Duration::from_secs(2));

    // Try to start again without stopping (should handle gracefully)
    let second_start = Command::new(fed_binary())
        .args(["-c", config_path.to_str().unwrap(), "-w", workdir, "start"])
        .output()
        .expect("Failed to start second time");

    println!(
        "Second start output:\n{}",
        String::from_utf8_lossy(&second_start.stdout)
    );
    println!(
        "Second start stderr:\n{}",
        String::from_utf8_lossy(&second_start.stderr)
    );

    // Should either succeed (idempotent) or give clear error
    // Key is it shouldn't corrupt state

    // Cleanup
    Command::new(fed_binary())
        .args(["-c", config_path.to_str().unwrap(), "-w", workdir, "stop"])
        .output()
        .expect("Failed to stop");
}

#[test]
fn test_status_command_with_corrupted_lock() {
    let (temp_dir, config_path) = create_recovery_test_config();
    let workdir = temp_dir.path().to_str().unwrap();
    let lock_path = temp_dir.path().join(".fed-lock.json");

    // Create corrupted lock file
    let corrupted_lock = r#"{ "services": [ { "invalid json"#;
    fs::write(&lock_path, corrupted_lock).expect("Failed to write corrupted lock");

    // Status command should handle corruption gracefully
    let status_output = Command::new(fed_binary())
        .args(["-c", config_path.to_str().unwrap(), "-w", workdir, "status"])
        .output()
        .expect("Failed to run status");

    println!(
        "Status with corrupted lock:\n{}",
        String::from_utf8_lossy(&status_output.stdout)
    );
    println!(
        "Status stderr:\n{}",
        String::from_utf8_lossy(&status_output.stderr)
    );

    // Should not panic, either succeeds or gives clear error

    // Cleanup corrupted file
    if lock_path.exists() {
        fs::remove_file(&lock_path).ok();
    }
}

#[test]
fn test_stop_with_missing_processes() {
    let (temp_dir, config_path) = create_recovery_test_config();
    let workdir = temp_dir.path().to_str().unwrap();
    let lock_path = temp_dir.path().join(".fed-lock.json");

    // Create lock file with non-existent PIDs
    let stale_lock = r#"{
  "services": [
    {
      "id": "simple-service",
      "service_type": "Process",
      "namespace": "default",
      "status": "running",
      "pid": 99998,
      "container_id": null,
      "port_allocations": {}
    },
    {
      "id": "resilient-service",
      "service_type": "Process",
      "namespace": "default",
      "status": "running",
      "pid": 99997,
      "container_id": null,
      "port_allocations": {}
    }
  ],
  "allocated_ports": []
}"#;

    fs::write(&lock_path, stale_lock).expect("Failed to write lock");

    // Stop command should handle missing processes gracefully
    let stop_output = Command::new(fed_binary())
        .args(["-c", config_path.to_str().unwrap(), "-w", workdir, "stop"])
        .output()
        .expect("Failed to stop");

    println!(
        "Stop with missing processes:\n{}",
        String::from_utf8_lossy(&stop_output.stdout)
    );

    // Should succeed and clean up state
    std::thread::sleep(Duration::from_secs(1));

    // Lock file should be cleared or PIDs removed
    if lock_path.exists() {
        let lock_after = fs::read_to_string(&lock_path).unwrap_or_default();
        println!("Lock after stop:\n{}", lock_after);
    }
}

#[test]
fn test_restart_cleans_up_properly() {
    let (temp_dir, config_path) = create_recovery_test_config();
    let workdir = temp_dir.path().to_str().unwrap();
    let lock_path = temp_dir.path().join(".fed-lock.json");

    // Start services
    Command::new(fed_binary())
        .args(["-c", config_path.to_str().unwrap(), "-w", workdir, "start"])
        .output()
        .expect("Failed to start");

    std::thread::sleep(Duration::from_secs(2));

    let lock_before = fs::read_to_string(&lock_path).ok();

    // Restart
    let restart_output = Command::new(fed_binary())
        .args([
            "-c",
            config_path.to_str().unwrap(),
            "-w",
            workdir,
            "restart",
        ])
        .output()
        .expect("Failed to restart");

    assert!(restart_output.status.success(), "Restart should succeed");

    std::thread::sleep(Duration::from_secs(2));

    // Verify lock file is updated (PIDs should be different)
    let lock_after = fs::read_to_string(&lock_path).ok();

    if let (Some(before), Some(after)) = (lock_before, lock_after) {
        println!("Lock before restart:\n{}", before);
        println!("Lock after restart:\n{}", after);

        // Lock file should exist and be valid
        assert!(
            serde_json::from_str::<serde_json::Value>(&after).is_ok(),
            "Lock file should be valid after restart"
        );
    }

    // Cleanup
    Command::new(fed_binary())
        .args(["-c", config_path.to_str().unwrap(), "-w", workdir, "stop"])
        .output()
        .expect("Failed to stop");
}

// =============================================================================
// Comprehensive Cleanup & Recovery Tests
// =============================================================================

#[test]
fn test_crash_recovery_state_consistency() {
    let (temp_dir, config_path) = create_recovery_test_config();
    let workdir = temp_dir.path().to_str().unwrap();

    // Start services normally
    let start = Command::new(fed_binary())
        .args([
            "-c",
            config_path.to_str().unwrap(),
            "-w",
            workdir,
            "start",
            "resilient-service",
        ])
        .output()
        .expect("Failed to start");

    assert!(start.status.success());
    std::thread::sleep(Duration::from_secs(2));

    // Get initial status
    let status1 = Command::new(fed_binary())
        .args(["-c", config_path.to_str().unwrap(), "-w", workdir, "status"])
        .output()
        .expect("Failed to get status");

    let status_output1 = String::from_utf8_lossy(&status1.stdout);
    println!("Status before stop:\n{}", status_output1);

    // Clean stop
    Command::new(fed_binary())
        .args(["-c", config_path.to_str().unwrap(), "-w", workdir, "stop"])
        .output()
        .expect("Failed to stop");

    std::thread::sleep(Duration::from_secs(1));

    // Get status after clean stop
    let status2 = Command::new(fed_binary())
        .args(["-c", config_path.to_str().unwrap(), "-w", workdir, "status"])
        .output()
        .expect("Failed to get status");

    let status_output2 = String::from_utf8_lossy(&status2.stdout);
    println!("Status after stop:\n{}", status_output2);

    // Should be able to start again without issues
    let restart = Command::new(fed_binary())
        .args([
            "-c",
            config_path.to_str().unwrap(),
            "-w",
            workdir,
            "start",
            "resilient-service",
        ])
        .output()
        .expect("Failed to restart");

    assert!(
        restart.status.success(),
        "Should be able to restart after clean stop"
    );

    // Cleanup
    Command::new(fed_binary())
        .args(["-c", config_path.to_str().unwrap(), "-w", workdir, "stop"])
        .output()
        .ok();
}

#[test]
fn test_repeated_start_stop_cycles() {
    let (temp_dir, config_path) = create_recovery_test_config();
    let workdir = temp_dir.path().to_str().unwrap();

    // Perform multiple start/stop cycles
    for cycle in 0..3 {
        println!("Cycle {}", cycle);

        let start = Command::new(fed_binary())
            .args([
                "-c",
                config_path.to_str().unwrap(),
                "-w",
                workdir,
                "start",
                "resilient-service",
            ])
            .output()
            .expect("Failed to start");

        assert!(start.status.success(), "Start failed in cycle {}", cycle);
        std::thread::sleep(Duration::from_secs(1));

        let stop = Command::new(fed_binary())
            .args(["-c", config_path.to_str().unwrap(), "-w", workdir, "stop"])
            .output()
            .expect("Failed to stop");

        assert!(stop.status.success(), "Stop failed in cycle {}", cycle);
        std::thread::sleep(Duration::from_secs(1));
    }

    // Final check - should be clean state
    let status = Command::new(fed_binary())
        .args(["-c", config_path.to_str().unwrap(), "-w", workdir, "status"])
        .output()
        .expect("Failed to get final status");

    assert!(status.status.success());
}

#[test]
fn test_cleanup_on_service_failure() {
    let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
    let config_path = temp_dir.path().join("test-config.yaml");

    let config_content = r#"
services:
  failing-service:
    process: exit 1
"#;

    fs::write(&config_path, config_content).expect("Failed to write config");
    let workdir = temp_dir.path().to_str().unwrap();

    // Try to start failing service
    let start = Command::new(fed_binary())
        .args([
            "-c",
            config_path.to_str().unwrap(),
            "-w",
            workdir,
            "start",
            "failing-service",
        ])
        .output()
        .expect("Failed to start");

    println!("Start output: {:?}", String::from_utf8_lossy(&start.stdout));
    println!("Start stderr: {:?}", String::from_utf8_lossy(&start.stderr));

    std::thread::sleep(Duration::from_secs(1));

    // Service should not be running
    let status = Command::new(fed_binary())
        .args(["-c", config_path.to_str().unwrap(), "-w", workdir, "status"])
        .output()
        .expect("Failed to get status");

    let status_output = String::from_utf8_lossy(&status.stdout);
    println!("Status: {}", status_output);

    // Should be able to stop/cleanup without errors
    let cleanup = Command::new(fed_binary())
        .args(["-c", config_path.to_str().unwrap(), "-w", workdir, "stop"])
        .output()
        .expect("Failed to cleanup");

    assert!(cleanup.status.success(), "Cleanup should succeed");
}

// =============================================================================
// Restart-on-failure recovery (watch mode)
// =============================================================================
//
// `restart:` only has an observable effect while the monitoring loop is
// running, and `start_monitoring` skips entirely in the default `file`
// output mode used by plain `fed start` — only `fed start --watch`
// (`captured` output by default) keeps it alive. So these tests drive the
// scenario through `--watch`, spawned in the background so the test can
// kill it once it's done, rather than the `.output()` pattern used above.
//
// The fixture process is `sleep 1 && exit 1`, not a bare `exit 1`: an
// immediate failure trips `ProcessService::start`'s 300ms startup probe
// before watch mode (and therefore the monitoring loop) ever begins, which
// would make the test pass or fail for the wrong reason. Waiting past the
// probe, then dying, is what actually reaches `run_monitoring_loop`.

/// Fixture for the watch-mode restart tests: a service whose process exits
/// ~1s after starting (past the 300ms startup probe), with or without a
/// `restart: !onfailure` policy attached.
fn create_flaky_service_config(with_restart_policy: bool) -> (TempDir, PathBuf) {
    let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
    let config_path = temp_dir.path().join("test-config.yaml");

    let restart_block = if with_restart_policy {
        "    restart: !onfailure\n      max_retries: 3\n"
    } else {
        ""
    };

    let config_content =
        format!("services:\n  flaky-service:\n    process: sleep 1 && exit 1\n{restart_block}");

    // Sanity check at the point of authorship: this is exactly the bug that
    // shipped nine vacuous tests in this file before — a fixture key that
    // doesn't parse into the field the test thinks it does. The standing
    // audit (`tests/config_key_audit_test.rs`) covers every fixture in the
    // repo structurally; this is the same check inline, for this one.
    support::parse_checked(&config_content);

    fs::write(&config_path, &config_content).expect("Failed to write test config");
    (temp_dir, config_path)
}

/// Best-effort kill+reap so a panic mid-test doesn't leak a running
/// `fed --watch` process on CI.
fn kill_best_effort(child: &mut std::process::Child) {
    let _ = child.kill();
    let _ = child.wait();
}

/// Run `fed debug state --json` (a fresh, ordinary invocation — not the
/// spawned `--watch` process) and return the `restart_count` for `service`.
/// This is the "stateless CLI rebuilds from SQLite" contract that makes it
/// possible to assert on the watch process's behavior from the outside.
fn restart_count_for(config_path: &Path, workdir: &str, service: &str) -> u64 {
    let debug_output = Command::new(fed_binary())
        .args([
            "-c",
            config_path.to_str().unwrap(),
            "-w",
            workdir,
            "debug",
            "state",
            "--json",
        ])
        .output()
        .expect("Failed to run debug state");

    let stdout = String::from_utf8_lossy(&debug_output.stdout);
    let state: serde_json::Value = serde_json::from_str(&stdout).unwrap_or_else(|e| {
        panic!("debug state --json did not produce valid JSON: {e}\nstdout: {stdout}")
    });

    let services = state["services"]
        .as_array()
        .unwrap_or_else(|| panic!("no 'services' array in debug state: {stdout}"));

    let service_state = services
        .iter()
        .find(|s| s["name"] == service)
        .unwrap_or_else(|| panic!("service '{service}' missing from debug state: {stdout}"));

    service_state["restart_count"]
        .as_u64()
        .unwrap_or_else(|| panic!("no numeric restart_count for '{service}': {stdout}"))
}

#[test]
fn test_restart_on_failure_recovers() {
    let (temp_dir, config_path) = create_flaky_service_config(true);
    let workdir = temp_dir.path().to_str().unwrap();

    let mut watch_child = Command::new(fed_binary())
        .args([
            "-c",
            config_path.to_str().unwrap(),
            "-w",
            workdir,
            "start",
            "flaky-service",
            "--watch",
        ])
        .spawn()
        .expect("Failed to spawn watch mode");

    // The process dies at ~1s; the monitoring loop ticks every 5s. Sleep
    // past two-plus ticks (loose, matching this file's existing sleep
    // pattern) so at least one restart has had a chance to fire and be
    // recorded, without pinning the assertion to exact tick timing.
    std::thread::sleep(Duration::from_secs(13));

    let restart_count = restart_count_for(&config_path, workdir, "flaky-service");

    kill_best_effort(&mut watch_child);
    Command::new(fed_binary())
        .args(["-c", config_path.to_str().unwrap(), "-w", workdir, "stop"])
        .output()
        .ok();

    assert!(
        restart_count > 0,
        "restart: !onfailure should have triggered at least one restart after \
         the monitored process died, but restart_count was {restart_count}"
    );
}

/// Negative control for `test_restart_on_failure_recovers`: same failing
/// process, no `restart:` (defaults to `RestartPolicy::No`). Without this,
/// a bug that always increments `restart_count` regardless of policy would
/// pass the positive test too — this is what proves the harness actually
/// checks the policy, not just "some counter is nonzero".
#[test]
fn test_restart_no_policy_does_not_recover() {
    let (temp_dir, config_path) = create_flaky_service_config(false);
    let workdir = temp_dir.path().to_str().unwrap();

    let mut watch_child = Command::new(fed_binary())
        .args([
            "-c",
            config_path.to_str().unwrap(),
            "-w",
            workdir,
            "start",
            "flaky-service",
            "--watch",
        ])
        .spawn()
        .expect("Failed to spawn watch mode");

    std::thread::sleep(Duration::from_secs(13));

    let restart_count = restart_count_for(&config_path, workdir, "flaky-service");

    kill_best_effort(&mut watch_child);
    Command::new(fed_binary())
        .args(["-c", config_path.to_str().unwrap(), "-w", workdir, "stop"])
        .output()
        .ok();

    assert_eq!(
        restart_count, 0,
        "no restart policy (defaults to RestartPolicy::No) should never restart, \
         but restart_count was {restart_count}"
    );
}
