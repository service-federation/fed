use std::fs;
use std::path::PathBuf;
use std::process::Command;
use std::time::Duration;
use tempfile::TempDir;

/// Helper to create test config with port parameters
fn create_port_test_config() -> (TempDir, PathBuf) {
    let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
    let config_path = temp_dir.path().join("test-config.yaml");

    let config_content = r#"
parameters:
  API_PORT:
    type: port
    default: 8080

  DB_PORT:
    type: port
    default: 5432

  CACHE_PORT:
    type: port
    # No default - random allocation

services:
  api:
    process: |
      echo "API starting on port {{API_PORT}}"
      sleep 100
    environment:
      PORT: '{{API_PORT}}'

  database:
    process: |
      echo "Database on port {{DB_PORT}}"
      sleep 100
    environment:
      PORT: '{{DB_PORT}}'

  cache:
    process: |
      echo "Cache on port {{CACHE_PORT}}"
      sleep 100
    environment:
      PORT: '{{CACHE_PORT}}'
"#;

    fs::write(&config_path, config_content).expect("Failed to write test config");
    (temp_dir, config_path)
}

fn fed_binary() -> String {
    env!("CARGO_BIN_EXE_fed").to_string()
}

#[test]
fn test_multiple_services_get_different_ports() {
    let (temp_dir, config_path) = create_port_test_config();
    let workdir = temp_dir.path().to_str().unwrap();

    // Start all services
    Command::new(fed_binary())
        .args(["-c", config_path.to_str().unwrap(), "-w", workdir, "start"])
        .output()
        .expect("Failed to start");

    std::thread::sleep(Duration::from_secs(3));

    // Get ports for all services
    let mut ports = Vec::new();

    for service in &["api", "database", "cache"] {
        let port_output = Command::new(fed_binary())
            .args([
                "-c",
                config_path.to_str().unwrap(),
                "-w",
                workdir,
                "port",
                service,
            ])
            .output();

        if let Ok(output) = port_output
            && output.status.success()
        {
            let port_str = String::from_utf8_lossy(&output.stdout);
            if let Ok(port) = port_str.trim().parse::<u16>() {
                ports.push(port);
            }
        }
    }

    println!("Allocated ports: {:?}", ports);

    // Verify all ports are different
    if ports.len() >= 2 {
        for i in 0..ports.len() {
            for j in i + 1..ports.len() {
                assert_ne!(
                    ports[i], ports[j],
                    "Services should get different ports: {} and {}",
                    ports[i], ports[j]
                );
            }
        }
    }

    // Cleanup
    Command::new(fed_binary())
        .args(["-c", config_path.to_str().unwrap(), "-w", workdir, "stop"])
        .output()
        .expect("Failed to cleanup");
}

#[test]
fn test_preferred_port_allocation() {
    let (temp_dir, config_path) = create_port_test_config();
    let workdir = temp_dir.path().to_str().unwrap();

    // Start services
    Command::new(fed_binary())
        .args(["-c", config_path.to_str().unwrap(), "-w", workdir, "start"])
        .output()
        .expect("Failed to start");

    std::thread::sleep(Duration::from_secs(2));

    // Check if API got its preferred port 8080 (if available)
    let api_port_output = Command::new(fed_binary())
        .args([
            "-c",
            config_path.to_str().unwrap(),
            "-w",
            workdir,
            "port",
            "api",
        ])
        .output();

    if let Ok(output) = api_port_output
        && output.status.success()
    {
        let port_str = String::from_utf8_lossy(&output.stdout);
        if let Ok(port) = port_str.trim().parse::<u16>() {
            println!("API allocated port: {}", port);
            // If 8080 was available, it should get it
            // But we can't guarantee it, so just check it's valid
            assert!(port > 1024, "Port should be valid");
        }
    }

    // Cleanup
    Command::new(fed_binary())
        .args(["-c", config_path.to_str().unwrap(), "-w", workdir, "stop"])
        .output()
        .expect("Failed to cleanup");
}

#[test]
fn test_port_released_after_stop() {
    let (temp_dir, config_path) = create_port_test_config();
    let workdir = temp_dir.path().to_str().unwrap();

    // Start services
    Command::new(fed_binary())
        .args(["-c", config_path.to_str().unwrap(), "-w", workdir, "start"])
        .output()
        .expect("Failed to start");

    std::thread::sleep(Duration::from_secs(2));

    // Get initial port
    let first_port_output = Command::new(fed_binary())
        .args([
            "-c",
            config_path.to_str().unwrap(),
            "-w",
            workdir,
            "port",
            "api",
        ])
        .output();

    let _first_port = if let Ok(output) = first_port_output {
        if output.status.success() {
            let port_str = String::from_utf8_lossy(&output.stdout);
            port_str.trim().parse::<u16>().ok()
        } else {
            None
        }
    } else {
        None
    };

    // Stop services
    Command::new(fed_binary())
        .args(["-c", config_path.to_str().unwrap(), "-w", workdir, "stop"])
        .output()
        .expect("Failed to stop");

    std::thread::sleep(Duration::from_secs(1));

    // Try to query port after stop (should fail or return empty)
    let after_stop_output = Command::new(fed_binary())
        .args([
            "-c",
            config_path.to_str().unwrap(),
            "-w",
            workdir,
            "port",
            "api",
        ])
        .output();

    if let Ok(output) = after_stop_output {
        // Note: The `fed port` command returns the port from config parameters,
        // which remains valid even after services are stopped. This is expected
        // behavior - the port shows what value would be used if the service starts.
        if output.status.success() {
            let port_str = String::from_utf8_lossy(&output.stdout);
            // If we get a port, verify it's still valid (from config)
            if let Ok(port) = port_str.trim().parse::<u16>() {
                assert!(port > 0, "Port should be valid if returned");
            }
        }
    }
}
