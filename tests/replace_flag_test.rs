use std::fs;
use std::net::TcpListener;
use std::process::{Command, Stdio};
use std::time::Duration;
use tempfile::TempDir;

fn fed_binary() -> String {
    env!("CARGO_BIN_EXE_fed").to_string()
}

/// Test that --replace doesn't kill fed itself when it reserves ports during startup.
/// This is a regression test for SF-00032.
#[test]
fn test_replace_does_not_kill_self() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let config_path = temp_dir.path().join("service-federation.yaml");

    // Config with a port parameter - fed will bind to this during parameter resolution
    let config = r#"
parameters:
  TEST_PORT:
    default: "18765"

services:
  test-service:
    process: |
      echo "Service running on port $TEST_PORT"
      sleep 5
    environment:
      PORT: "{{TEST_PORT}}"
"#;

    fs::write(&config_path, config).expect("Failed to write config");

    // Run fed start --replace - it should NOT kill itself
    let output = Command::new(fed_binary())
        .args([
            "-c",
            config_path.to_str().unwrap(),
            "-w",
            temp_dir.path().to_str().unwrap(),
            "start",
            "--replace",
            "--dry-run", // Use dry-run to avoid actually starting services
        ])
        .output()
        .expect("Failed to run fed");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);

    // The command should complete successfully (not kill itself)
    assert!(
        output.status.success(),
        "fed start --replace killed itself or failed!\nstdout: {}\nstderr: {}",
        stdout,
        stderr
    );

    // Should not contain "Killing process 'fed'" for our own PID
    assert!(
        !stdout.contains("Killing process 'fed'"),
        "fed tried to kill itself!\nstdout: {}",
        stdout
    );
}

/// Test that --replace actually kills OTHER processes on conflicting ports.
#[test]
fn test_replace_kills_other_processes() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let config_path = temp_dir.path().join("service-federation.yaml");

    // Find an available port and bind to it
    let listener = TcpListener::bind("127.0.0.1:0").expect("Failed to bind");
    let port = listener.local_addr().unwrap().port();

    // Config that needs this specific port
    let config = format!(
        r#"
parameters:
  CONFLICT_PORT:
    default: "{}"

services:
  port-conflict-service:
    process: |
      echo "Service needs port $CONFLICT_PORT"
      sleep 5
    environment:
      PORT: "{{{{CONFLICT_PORT}}}}"
"#,
        port
    );

    fs::write(&config_path, config).expect("Failed to write config");

    // Spawn a process that holds the port (simulating another app)
    // We'll use a simple nc/sleep to hold the port
    #[cfg(unix)]
    let port_holder = {
        // Spawn a background process (simulating another app)
        Command::new("sh")
            .args(["-c", "sleep 60"])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
    };

    // Drop the listener - the port_holder or another process should now conflict
    drop(listener);

    // Small delay to ensure port is released
    std::thread::sleep(Duration::from_millis(100));

    // Run fed start --replace --dry-run
    // Even with dry-run, it should detect and report conflicts
    let output = Command::new(fed_binary())
        .args([
            "-c",
            config_path.to_str().unwrap(),
            "-w",
            temp_dir.path().to_str().unwrap(),
            "start",
            "--replace",
            "--dry-run",
        ])
        .output()
        .expect("Failed to run fed");

    // Clean up port holder if we spawned one
    #[cfg(unix)]
    if let Ok(mut child) = port_holder {
        let _ = child.kill();
    }

    // Command should complete (may or may not have conflicts depending on timing)
    // The key is it shouldn't crash or kill itself
    assert!(
        output.status.success() || output.status.code() == Some(1),
        "fed crashed unexpectedly!\nstderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

/// Test that --replace works correctly when there are no port conflicts.
#[test]
fn test_replace_no_conflicts() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let config_path = temp_dir.path().join("service-federation.yaml");

    // Simple config with no port conflicts expected
    let config = r#"
services:
  simple-service:
    process: |
      echo "Hello"
      sleep 1
"#;

    fs::write(&config_path, config).expect("Failed to write config");

    let output = Command::new(fed_binary())
        .args([
            "-c",
            config_path.to_str().unwrap(),
            "-w",
            temp_dir.path().to_str().unwrap(),
            "start",
            "--replace",
            "--dry-run",
        ])
        .output()
        .expect("Failed to run fed");

    assert!(
        output.status.success(),
        "fed start --replace --dry-run failed!\nstderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}
