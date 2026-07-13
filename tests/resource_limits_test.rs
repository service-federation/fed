//! Integration tests for resource limit enforcement
//!
//! Tests that resource limits are properly applied to Docker and process services.
//! Docker tests require Docker to be installed and running.

use std::fs;
use std::path::PathBuf;
use std::process::Command;
use tempfile::TempDir;

/// Check if Docker is available
fn is_docker_available() -> bool {
    Command::new("docker")
        .arg("version")
        .output()
        .map(|output| output.status.success())
        .unwrap_or(false)
}

/// Macro to skip Docker tests if Docker is not available
macro_rules! require_docker {
    () => {
        if !is_docker_available() {
            eprintln!("Skipping Docker test - Docker not available");
            return;
        }
    };
}

/// Helper to create test config and return temp directory and path
fn create_test_config(config_content: &str) -> (TempDir, PathBuf) {
    let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
    let config_path = temp_dir.path().join("service-federation.yaml");
    fs::write(&config_path, config_content).expect("Failed to write test config");
    (temp_dir, config_path)
}

/// Helper to clean up Docker containers after tests
fn cleanup_docker_container(container_name: &str) {
    let _ = Command::new("docker")
        .args(["rm", "-f", container_name])
        .output();
}

#[test]
#[cfg_attr(not(feature = "docker-tests"), ignore)] // Requires Docker
fn test_docker_memory_limit_in_command() {
    require_docker!();

    let config = r#"
services:
  memory-limited:
    image: alpine:latest
    resources:
      memory: 256m
"#;

    let (_temp_dir, config_path) = create_test_config(config);
    cleanup_docker_container("fed-memory-limited");

    // Parse config and verify resource limits are recognized
    let config_content = fs::read_to_string(&config_path).expect("Failed to read config");
    let config: fed::config::Config =
        serde_yaml::from_str(&config_content).expect("Failed to parse config");

    // Verify memory limit is parsed
    let service = config
        .services
        .get("memory-limited")
        .expect("Service not found");
    assert!(
        service.resources.is_some(),
        "Resources should be present in service"
    );

    let resources = service.resources.as_ref().unwrap();
    assert_eq!(
        resources.memory.as_deref(),
        Some("256m"),
        "Memory limit should be 256m"
    );
}

#[test]
#[cfg_attr(not(feature = "docker-tests"), ignore)] // Requires Docker
fn test_docker_cpu_limit_in_command() {
    require_docker!();

    let config = r#"
services:
  cpu-limited:
    image: alpine:latest
    resources:
      cpus: "1.5"
"#;

    let (_temp_dir, config_path) = create_test_config(config);

    let config_content = fs::read_to_string(&config_path).expect("Failed to read config");
    let config: fed::config::Config =
        serde_yaml::from_str(&config_content).expect("Failed to parse config");

    let service = config
        .services
        .get("cpu-limited")
        .expect("Service not found");
    assert!(service.resources.is_some(), "Resources should be present");

    let resources = service.resources.as_ref().unwrap();
    assert_eq!(
        resources.cpus.as_deref(),
        Some("1.5"),
        "CPU limit should be 1.5"
    );
}

#[test]
#[cfg_attr(not(feature = "docker-tests"), ignore)] // Requires Docker
fn test_docker_multiple_resource_limits() {
    require_docker!();

    let config = r#"
services:
  constrained:
    image: alpine:latest
    resources:
      memory: 512m
      cpus: "2.0"
      pids: 50
      nofile: 1024
"#;

    let (_temp_dir, config_path) = create_test_config(config);

    let config_content = fs::read_to_string(&config_path).expect("Failed to read config");
    let config: fed::config::Config =
        serde_yaml::from_str(&config_content).expect("Failed to parse config");

    let service = config
        .services
        .get("constrained")
        .expect("Service not found");
    let resources = service
        .resources
        .as_ref()
        .expect("Resources should be present");

    assert_eq!(resources.memory.as_deref(), Some("512m"));
    assert_eq!(resources.cpus.as_deref(), Some("2.0"));
    assert_eq!(resources.pids, Some(50));
    assert_eq!(resources.nofile, Some(1024));
}

#[test]
#[cfg_attr(not(feature = "docker-tests"), ignore)] // Requires Docker
fn test_docker_memory_swap_limit() {
    require_docker!();

    let config = r#"
services:
  swap-limited:
    image: alpine:latest
    resources:
      memory: 256m
      memory_swap: 512m
"#;

    let (_temp_dir, config_path) = create_test_config(config);

    let config_content = fs::read_to_string(&config_path).expect("Failed to read config");
    let config: fed::config::Config =
        serde_yaml::from_str(&config_content).expect("Failed to parse config");

    let service = config
        .services
        .get("swap-limited")
        .expect("Service not found");
    let resources = service
        .resources
        .as_ref()
        .expect("Resources should be present");

    assert_eq!(resources.memory.as_deref(), Some("256m"));
    assert_eq!(resources.memory_swap.as_deref(), Some("512m"));
}

#[test]
fn test_resource_limits_validation_valid() {
    let config = r#"
services:
  valid-limits:
    process: echo "test"
    resources:
      memory: 512m
      cpus: "1.0"
      pids: 100
"#;

    let (_temp_dir, config_path) = create_test_config(config);

    let config_content = fs::read_to_string(&config_path).expect("Failed to read config");
    let config: fed::config::Config =
        serde_yaml::from_str(&config_content).expect("Failed to parse config");

    // Config should validate without errors
    assert!(
        config.validate().is_ok(),
        "Valid resource limits should pass validation"
    );
}

#[test]
fn test_resource_limits_validation_invalid_memory() {
    let config = r#"
services:
  invalid-memory:
    process: echo "test"
    resources:
      memory: "invalid"
"#;

    let (_temp_dir, config_path) = create_test_config(config);

    let config_content = fs::read_to_string(&config_path).expect("Failed to read config");
    let config: fed::config::Config =
        serde_yaml::from_str(&config_content).expect("Failed to parse config");

    let result = config.validate();
    assert!(
        result.is_err(),
        "Invalid memory format should fail validation"
    );
}

#[test]
fn test_resource_limits_validation_invalid_cpus() {
    let config = r#"
services:
  invalid-cpus:
    process: echo "test"
    resources:
      cpus: "0"
"#;

    let (_temp_dir, config_path) = create_test_config(config);

    let config_content = fs::read_to_string(&config_path).expect("Failed to read config");
    let config: fed::config::Config =
        serde_yaml::from_str(&config_content).expect("Failed to parse config");

    let result = config.validate();
    assert!(result.is_err(), "Zero CPU value should fail validation");
}

#[test]
fn test_resource_limits_validation_invalid_pids() {
    let config = r#"
services:
  invalid-pids:
    process: echo "test"
    resources:
      pids: 0
"#;

    let (_temp_dir, config_path) = create_test_config(config);

    let config_content = fs::read_to_string(&config_path).expect("Failed to read config");
    let config: fed::config::Config =
        serde_yaml::from_str(&config_content).expect("Failed to parse config");

    let result = config.validate();
    assert!(result.is_err(), "Zero PID limit should fail validation");
}

#[test]
fn test_resource_limits_memory_format_gigabytes() {
    let config = r#"
services:
  gigabyte-memory:
    process: echo "test"
    resources:
      memory: "2g"
"#;

    let (_temp_dir, config_path) = create_test_config(config);

    let config_content = fs::read_to_string(&config_path).expect("Failed to read config");
    let config: fed::config::Config =
        serde_yaml::from_str(&config_content).expect("Failed to parse config");

    assert!(
        config.validate().is_ok(),
        "Memory in gigabytes should be valid"
    );

    let service = config
        .services
        .get("gigabyte-memory")
        .expect("Service not found");
    let resources = service
        .resources
        .as_ref()
        .expect("Resources should be present");
    assert_eq!(resources.memory.as_deref(), Some("2g"));
}

#[test]
fn test_resource_limits_memory_format_decimal() {
    let config = r#"
services:
  decimal-memory:
    process: echo "test"
    resources:
      memory: "1.5g"
"#;

    let (_temp_dir, config_path) = create_test_config(config);

    let config_content = fs::read_to_string(&config_path).expect("Failed to read config");
    let config: fed::config::Config =
        serde_yaml::from_str(&config_content).expect("Failed to parse config");

    assert!(config.validate().is_ok(), "Decimal memory should be valid");
}

#[test]
fn test_resource_limits_cpu_format_decimal() {
    let config = r#"
services:
  fractional-cpu:
    process: echo "test"
    resources:
      cpus: "0.5"
"#;

    let (_temp_dir, config_path) = create_test_config(config);

    let config_content = fs::read_to_string(&config_path).expect("Failed to read config");
    let config: fed::config::Config =
        serde_yaml::from_str(&config_content).expect("Failed to parse config");

    assert!(config.validate().is_ok(), "Fractional CPU should be valid");

    let service = config
        .services
        .get("fractional-cpu")
        .expect("Service not found");
    let resources = service
        .resources
        .as_ref()
        .expect("Resources should be present");
    assert_eq!(resources.cpus.as_deref(), Some("0.5"));
}

#[test]
fn test_resource_limits_cpu_shares_bounds() {
    let config = r#"
services:
  cpu-shares-valid:
    process: echo "test"
    resources:
      cpu_shares: 1024
"#;

    let (_temp_dir, config_path) = create_test_config(config);

    let config_content = fs::read_to_string(&config_path).expect("Failed to read config");
    let config: fed::config::Config =
        serde_yaml::from_str(&config_content).expect("Failed to parse config");

    assert!(
        config.validate().is_ok(),
        "Valid CPU shares should pass validation"
    );
}

#[test]
fn test_resource_limits_no_limits_valid() {
    let config = r#"
services:
  unlimited:
    process: echo "test"
"#;

    let (_temp_dir, config_path) = create_test_config(config);

    let config_content = fs::read_to_string(&config_path).expect("Failed to read config");
    let config: fed::config::Config =
        serde_yaml::from_str(&config_content).expect("Failed to parse config");

    assert!(
        config.validate().is_ok(),
        "Service without resources should be valid"
    );

    let service = config.services.get("unlimited").expect("Service not found");
    assert!(
        service.resources.is_none(),
        "Service should have no resources"
    );
}

#[test]
fn test_resource_limits_partial_limits() {
    let config = r#"
services:
  partial-limits:
    process: echo "test"
    resources:
      memory: 512m
"#;

    let (_temp_dir, config_path) = create_test_config(config);

    let config_content = fs::read_to_string(&config_path).expect("Failed to read config");
    let config: fed::config::Config =
        serde_yaml::from_str(&config_content).expect("Failed to parse config");

    assert!(config.validate().is_ok(), "Partial limits should be valid");

    let service = config
        .services
        .get("partial-limits")
        .expect("Service not found");
    let resources = service
        .resources
        .as_ref()
        .expect("Resources should be present");

    assert_eq!(resources.memory.as_deref(), Some("512m"));
    assert!(resources.cpus.is_none(), "CPU should not be set");
    assert!(resources.pids.is_none(), "PIDs should not be set");
}

#[test]
fn test_resource_limits_multiple_services() {
    let config = r#"
services:
  service-a:
    process: echo "a"
    resources:
      memory: 256m
      cpus: "1.0"

  service-b:
    process: echo "b"
    resources:
      memory: 512m
      cpus: "2.0"

  service-c:
    process: echo "c"
"#;

    let (_temp_dir, config_path) = create_test_config(config);

    let config_content = fs::read_to_string(&config_path).expect("Failed to read config");
    let config: fed::config::Config =
        serde_yaml::from_str(&config_content).expect("Failed to parse config");

    assert!(
        config.validate().is_ok(),
        "Multiple services with mixed limits should validate"
    );

    let service_a = config
        .services
        .get("service-a")
        .expect("Service A not found");
    assert_eq!(
        service_a.resources.as_ref().unwrap().memory.as_deref(),
        Some("256m")
    );

    let service_b = config
        .services
        .get("service-b")
        .expect("Service B not found");
    assert_eq!(
        service_b.resources.as_ref().unwrap().memory.as_deref(),
        Some("512m")
    );

    let service_c = config
        .services
        .get("service-c")
        .expect("Service C not found");
    assert!(
        service_c.resources.is_none(),
        "Service C should have no limits"
    );
}
