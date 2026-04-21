//! Tests for the --dry-run flag on the start command.
//!
//! These tests verify that:
//! 1. Dry run shows expected output (services, start order, etc.)
//! 2. Dry run does NOT actually start any services
//! 3. Dry run detects port conflicts
//! 4. Dry run masks sensitive environment variables
//! 5. Dry run does NOT print resolved parameters (they can contain secrets)

use std::fs;
use std::net::TcpListener;
use std::process::Command;
use tempfile::TempDir;

fn fed_binary() -> String {
    env!("CARGO_BIN_EXE_fed").to_string()
}

fn create_test_config(content: &str) -> (TempDir, std::path::PathBuf) {
    let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
    let config_path = temp_dir.path().join("service-federation.yaml");
    fs::write(&config_path, content).expect("Failed to write config");
    (temp_dir, config_path)
}

#[test]
fn test_dry_run_shows_services_and_start_order() {
    let config = r#"
parameters:
  API_PORT:
    type: port
    default: 3000
  DB_PORT:
    type: port
    default: 5432

services:
  database:
    process: "echo database"

  api:
    process: "echo api"
    depends_on:
      - database

  frontend:
    process: "echo frontend"
    depends_on:
      - api

entrypoint: frontend
"#;

    let (temp_dir, config_path) = create_test_config(config);
    let workdir = temp_dir.path().to_str().unwrap();

    let output = Command::new(fed_binary())
        .args([
            "-c",
            config_path.to_str().unwrap(),
            "-w",
            workdir,
            "start",
            "--dry-run",
        ])
        .output()
        .expect("Failed to run dry-run");

    let stdout = String::from_utf8_lossy(&output.stdout);
    println!("Dry run output:\n{}", stdout);

    assert!(output.status.success(), "Dry run should succeed");

    // Check that it shows dry run header
    assert!(
        stdout.contains("Dry Run Mode"),
        "Should show dry run header"
    );

    // Check that it shows services to start
    assert!(
        stdout.contains("Services to start:"),
        "Should show services to start section"
    );
    assert!(stdout.contains("frontend"), "Should mention frontend");

    // Check start order - dependencies should come before dependents
    assert!(stdout.contains("Start order:"), "Should show start order");

    // Resolved parameter values must not leak to stdout — they can contain secrets.
    assert!(
        !stdout.contains("Resolved parameters:"),
        "Must not print resolved parameters (can contain secrets)"
    );

    // Check validation summary
    assert!(
        stdout.contains("Validation Summary"),
        "Should show validation summary"
    );

    // Check that it tells user to remove --dry-run to actually start
    assert!(
        stdout.contains("Run without --dry-run"),
        "Should tell user how to actually start"
    );
}

#[test]
fn test_dry_run_does_not_start_services() {
    let config = r#"
services:
  marker-service:
    process: "touch /tmp/dry-run-test-marker && sleep 300"

entrypoint: marker-service
"#;

    // Clean up any leftover marker file
    let marker_path = "/tmp/dry-run-test-marker";
    let _ = fs::remove_file(marker_path);

    let (temp_dir, config_path) = create_test_config(config);
    let workdir = temp_dir.path().to_str().unwrap();

    let output = Command::new(fed_binary())
        .args([
            "-c",
            config_path.to_str().unwrap(),
            "-w",
            workdir,
            "start",
            "--dry-run",
        ])
        .output()
        .expect("Failed to run dry-run");

    assert!(output.status.success(), "Dry run should succeed");

    // Give it a moment just in case
    std::thread::sleep(std::time::Duration::from_millis(500));

    // The marker file should NOT exist because dry-run doesn't start services
    assert!(
        !std::path::Path::new(marker_path).exists(),
        "Dry run should NOT actually start services - marker file should not exist"
    );
}

#[test]
fn test_dry_run_detects_port_conflicts() {
    // Bind to a random port to create a conflict
    let listener = TcpListener::bind("127.0.0.1:0").expect("Failed to bind");
    let port = listener.local_addr().unwrap().port();

    // Use type: port so the resolver detects the conflict and auto-resolves.
    // The dry-run then checks the default port and reports [CONFLICT].
    let config = format!(
        r#"
parameters:
  CONFLICT_PORT:
    type: port
    default: {}

services:
  conflicting-service:
    process: "echo service"
    environment:
      PORT: "${{{{CONFLICT_PORT}}}}"

entrypoint: conflicting-service
"#,
        port
    );

    let (temp_dir, config_path) = create_test_config(&config);
    let workdir = temp_dir.path().to_str().unwrap();

    let output = Command::new(fed_binary())
        .args([
            "-c",
            config_path.to_str().unwrap(),
            "-w",
            workdir,
            "start",
            "--dry-run",
        ])
        .output()
        .expect("Failed to run dry-run");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let port_str = port.to_string();
    println!(
        "Dry run output with conflict (port {}):\n{}",
        port_str, stdout
    );

    assert!(output.status.success(), "Dry run should succeed");

    // Check that port conflict is detected on the default port
    assert!(
        stdout.contains("[CONFLICT]"),
        "Should detect port conflict on default port. Output: {}",
        stdout
    );

    // The conflicting port number should appear in the output
    assert!(
        stdout.contains(&port_str),
        "Should mention the conflicting port {}. Output: {}",
        port_str,
        stdout
    );

    // Drop the listener to release the port
    drop(listener);
}

#[test]
fn test_dry_run_masks_sensitive_env_vars() {
    let config = r#"
services:
  secure-service:
    process: "echo service"
    environment:
      DATABASE_URL: "postgres://localhost/db"
      API_SECRET: "super-secret-key"
      AUTH_TOKEN: "bearer-token-12345"
      PASSWORD: "my-password"
      NORMAL_VAR: "visible-value"

entrypoint: secure-service
"#;

    let (temp_dir, config_path) = create_test_config(config);
    let workdir = temp_dir.path().to_str().unwrap();

    let output = Command::new(fed_binary())
        .args([
            "-c",
            config_path.to_str().unwrap(),
            "-w",
            workdir,
            "start",
            "--dry-run",
        ])
        .output()
        .expect("Failed to run dry-run");

    let stdout = String::from_utf8_lossy(&output.stdout);
    println!("Dry run output with env vars:\n{}", stdout);

    assert!(output.status.success(), "Dry run should succeed");

    // Sensitive values should be masked
    assert!(
        !stdout.contains("super-secret-key"),
        "API_SECRET value should be masked"
    );
    assert!(
        !stdout.contains("bearer-token-12345"),
        "AUTH_TOKEN value should be masked"
    );
    assert!(
        !stdout.contains("my-password"),
        "PASSWORD value should be masked"
    );

    // But the key names should still appear
    assert!(
        stdout.contains("API_SECRET"),
        "API_SECRET key should appear"
    );
    assert!(
        stdout.contains("AUTH_TOKEN"),
        "AUTH_TOKEN key should appear"
    );
    assert!(stdout.contains("PASSWORD"), "PASSWORD key should appear");

    // Non-sensitive values should be visible
    assert!(
        stdout.contains("visible-value"),
        "Non-sensitive values should be visible"
    );

    // Masked values should show ***
    assert!(stdout.contains("***"), "Masked values should show ***");
}

#[test]
fn test_dry_run_shows_resource_limits() {
    let config = r#"
services:
  limited-service:
    process: "echo service"
    resources:
      memory: "512m"
      cpus: "0.5"
      nofile: 1024

entrypoint: limited-service
"#;

    let (temp_dir, config_path) = create_test_config(config);
    let workdir = temp_dir.path().to_str().unwrap();

    let output = Command::new(fed_binary())
        .args([
            "-c",
            config_path.to_str().unwrap(),
            "-w",
            workdir,
            "start",
            "--dry-run",
        ])
        .output()
        .expect("Failed to run dry-run");

    let stdout = String::from_utf8_lossy(&output.stdout);
    println!("Dry run output with resource limits:\n{}", stdout);

    assert!(output.status.success(), "Dry run should succeed");

    // Check resource limits section
    assert!(
        stdout.contains("Resource limits:"),
        "Should show resource limits section"
    );
    assert!(stdout.contains("512m"), "Should show memory limit");
    assert!(stdout.contains("0.5"), "Should show CPU limit");
    assert!(stdout.contains("1024"), "Should show nofile limit");
}

#[test]
fn test_dry_run_shows_health_check_config() {
    let config = r#"
parameters:
  API_PORT:
    type: port
    default: 8080

services:
  health-checked-service:
    process: "echo service"
    healthcheck:
      httpGet: "http://localhost:${API_PORT}/health"
      timeout: "30s"

entrypoint: health-checked-service
"#;

    let (temp_dir, config_path) = create_test_config(config);
    let workdir = temp_dir.path().to_str().unwrap();

    let output = Command::new(fed_binary())
        .args([
            "-c",
            config_path.to_str().unwrap(),
            "-w",
            workdir,
            "start",
            "--dry-run",
        ])
        .output()
        .expect("Failed to run dry-run");

    let stdout = String::from_utf8_lossy(&output.stdout);
    println!("Dry run output with health check:\n{}", stdout);

    assert!(output.status.success(), "Dry run should succeed");

    // Check health check is shown
    assert!(
        stdout.contains("healthcheck:"),
        "Should show health check configuration"
    );
    assert!(
        stdout.contains("HTTP GET"),
        "Should show HTTP health check type"
    );
    assert!(stdout.contains("timeout"), "Should show timeout");
}

#[test]
fn test_dry_run_with_dependencies_shows_all_services() {
    let config = r#"
services:
  base:
    process: "echo base"

  middle:
    process: "echo middle"
    depends_on:
      - base

  top:
    process: "echo top"
    depends_on:
      - middle
"#;

    let (temp_dir, config_path) = create_test_config(config);
    let workdir = temp_dir.path().to_str().unwrap();

    // Start only "top" but dry run should show all dependencies
    let output = Command::new(fed_binary())
        .args([
            "-c",
            config_path.to_str().unwrap(),
            "-w",
            workdir,
            "start",
            "--dry-run",
            "top",
        ])
        .output()
        .expect("Failed to run dry-run");

    let stdout = String::from_utf8_lossy(&output.stdout);
    println!("Dry run output with dependencies:\n{}", stdout);

    assert!(output.status.success(), "Dry run should succeed");

    // All three services should be shown in start order
    assert!(stdout.contains("base"), "Should show base service");
    assert!(stdout.contains("middle"), "Should show middle service");
    assert!(stdout.contains("top"), "Should show top service");

    // Dependencies should be mentioned
    assert!(
        stdout.contains("depends on:"),
        "Should show dependency information"
    );
}

#[test]
fn test_dry_run_shows_service_types() {
    let config = r#"
services:
  process-service:
    process: "echo process"

  docker-service:
    image: "nginx:latest"
"#;

    let (temp_dir, config_path) = create_test_config(config);
    let workdir = temp_dir.path().to_str().unwrap();

    let output = Command::new(fed_binary())
        .args([
            "-c",
            config_path.to_str().unwrap(),
            "-w",
            workdir,
            "start",
            "--dry-run",
            "process-service",
            "docker-service",
        ])
        .output()
        .expect("Failed to run dry-run");

    let stdout = String::from_utf8_lossy(&output.stdout);
    println!("Dry run output with service types:\n{}", stdout);

    assert!(output.status.success(), "Dry run should succeed");

    // Should show service types
    assert!(stdout.contains("Process"), "Should show Process type");
    assert!(stdout.contains("Docker"), "Should show Docker type");
}

#[test]
fn test_dry_run_no_port_conflicts_message() {
    let config = r#"
parameters:
  FREE_PORT:
    type: port
    default: 0

services:
  simple-service:
    process: "echo service"

entrypoint: simple-service
"#;

    let (temp_dir, config_path) = create_test_config(config);
    let workdir = temp_dir.path().to_str().unwrap();

    let output = Command::new(fed_binary())
        .args([
            "-c",
            config_path.to_str().unwrap(),
            "-w",
            workdir,
            "start",
            "--dry-run",
        ])
        .output()
        .expect("Failed to run dry-run");

    let stdout = String::from_utf8_lossy(&output.stdout);
    println!("Dry run output:\n{}", stdout);

    assert!(output.status.success(), "Dry run should succeed");

    // When there are no ports or no conflicts
    assert!(
        stdout.contains("Port conflicts: None") || stdout.contains("No port parameters"),
        "Should indicate no port conflicts"
    );
}
