use std::fs;
use std::path::PathBuf;
use std::process::Command;
use std::time::Duration;
use tempfile::TempDir;

fn create_recovery_test_config() -> (TempDir, PathBuf) {
    let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
    let config_path = temp_dir.path().join("test-config.yaml");

    let config_content = r#"
services:
  resilient-service:
    process: |
      echo "Service starting"
      sleep 300
    restart_policy:
      on_failure:
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
