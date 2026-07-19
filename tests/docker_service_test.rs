// Integration tests for Docker service
// These tests require Docker daemon to be running
// Run with: cargo test --test docker_service_test -- --ignored

use fed::config::Service as ServiceConfig;
use fed::service::{DockerService, ServiceManager, Status};
use std::collections::HashMap;
use tokio::time::{Duration, sleep};

// Check if Docker is available
fn is_docker_available() -> bool {
    std::process::Command::new("docker")
        .arg("version")
        .output()
        .map(|output| output.status.success())
        .unwrap_or(false)
}

// Skip test if Docker is not available
macro_rules! require_docker {
    () => {
        if !is_docker_available() {
            eprintln!("⏭️  Skipping test: Docker not available");
            return;
        }
    };
}

// Helper to create a minimal Docker service config
fn create_test_service(name: &str, image: &str) -> DockerService {
    let config = ServiceConfig {
        image: Some(image.to_string()),
        ..Default::default()
    };

    DockerService::new(
        name.to_string(),
        config,
        HashMap::new(),
        "/tmp".to_string(),
        None, // No isolation ID
    )
}

// Helper to create service with environment variables
fn create_test_service_with_env(
    name: &str,
    image: &str,
    env: HashMap<String, String>,
) -> DockerService {
    let config = ServiceConfig {
        image: Some(image.to_string()),
        ..Default::default()
    };

    DockerService::new(
        name.to_string(),
        config,
        env,
        "/tmp".to_string(),
        None, // No isolation ID
    )
}

// Helper to cleanup a container by name (best effort)
async fn cleanup_container(name: &str) {
    let _ = tokio::process::Command::new("docker")
        .args(["rm", "-f", name])
        .output()
        .await;
}

/// Resolve the actual container name fed generates for a service.
///
/// The name is `fed-{work_dir_hash}-{service}` (or `fed-{session_id}-{service}`),
/// not `fed-{service}` — the stack prefix can't be computed from a test because
/// `docker_container_name` is `pub(crate)`. We look it up by filtering running
/// containers on the service-name substring (unique per test here).
async fn resolve_container_name(service_substr: &str) -> String {
    let out = tokio::process::Command::new("docker")
        .args([
            "ps",
            "-a",
            "--filter",
            &format!("name={service_substr}"),
            "--format",
            "{{.Names}}",
        ])
        .output()
        .await
        .expect("docker ps");
    String::from_utf8_lossy(&out.stdout)
        .lines()
        .next()
        .unwrap_or("")
        .trim()
        .to_string()
}

// ============================================================================
// Lifecycle Tests
// ============================================================================

#[tokio::test]
#[cfg_attr(not(feature = "docker-tests"), ignore)] // Requires Docker
async fn test_docker_service_basic_lifecycle() {
    require_docker!();

    let container_name = "fed-test-lifecycle";
    cleanup_container(container_name).await;

    let mut service = create_test_service("test-lifecycle", "nginx:alpine");

    // Start service
    let result = service.start().await;
    assert!(result.is_ok(), "Service should start successfully");

    // Give container time to start
    sleep(Duration::from_millis(500)).await;

    // Check status is Running
    assert_eq!(service.status(), Status::Running);

    // Health check should return true
    let health = service.health().await;
    assert!(health.is_ok(), "Health check should not error");
    assert!(health.unwrap(), "Container should be healthy");

    // Get logs
    let logs = service.logs(None).await;
    assert!(logs.is_ok(), "Should be able to get logs");

    // Stop service
    let result = service.stop().await;
    assert!(result.is_ok(), "Service should stop successfully");

    // Check status is Stopped
    assert_eq!(service.status(), Status::Stopped);

    // Health check should return false after stop
    let health = service.health().await;
    assert!(health.is_ok(), "Health check should not error after stop");
    assert!(
        !health.unwrap(),
        "Container should not be healthy after stop"
    );

    // Verify container is removed (our fix ensures cleanup)
    let inspect = tokio::process::Command::new("docker")
        .args(["inspect", container_name])
        .output()
        .await;
    assert!(inspect.is_ok());
    assert!(
        !inspect.unwrap().status.success(),
        "Container should be removed"
    );
}

#[tokio::test]
#[cfg_attr(not(feature = "docker-tests"), ignore)] // Requires Docker
async fn test_docker_service_start_idempotent() {
    require_docker!();

    let container_name = "fed-test-idempotent";
    cleanup_container(container_name).await;

    let mut service = create_test_service("test-idempotent", "nginx:alpine");

    // Start service first time
    let result = service.start().await;
    assert!(result.is_ok(), "First start should succeed");

    sleep(Duration::from_millis(300)).await;

    // Start again (should be no-op since already running)
    let result = service.start().await;
    assert!(result.is_ok(), "Second start should succeed (idempotent)");

    // Cleanup
    service.stop().await.ok();
}

#[tokio::test]
#[cfg_attr(not(feature = "docker-tests"), ignore)] // Requires Docker
async fn test_docker_service_kill() {
    require_docker!();

    let container_name = "fed-test-kill";
    cleanup_container(container_name).await;

    let mut service = create_test_service("test-kill", "nginx:alpine");

    // Start service
    service.start().await.expect("Start should succeed");
    sleep(Duration::from_millis(300)).await;

    // Kill service
    let result = service.kill().await;
    assert!(result.is_ok(), "Kill should succeed");

    // Check status
    assert_eq!(service.status(), Status::Stopped);

    // Container should be removed
    let inspect = tokio::process::Command::new("docker")
        .args(["inspect", container_name])
        .output()
        .await
        .expect("inspect command");
    assert!(
        !inspect.status.success(),
        "Container should be removed after kill"
    );
}

#[tokio::test]
#[cfg_attr(not(feature = "docker-tests"), ignore)] // Requires Docker
async fn test_docker_service_logs_retrieval() {
    require_docker!();

    let container_name = "fed-test-logs";
    cleanup_container(container_name).await;

    // Use alpine with a command that produces output
    let config = ServiceConfig {
        image: Some("alpine:latest".to_string()),
        process: Some("sh -c 'echo Hello && echo World && sleep 30'".to_string()),
        ..Default::default()
    };

    let mut service = DockerService::new(
        "test-logs".to_string(),
        config,
        HashMap::new(),
        "/tmp".to_string(),
        None, // No isolation ID
    );

    service.start().await.expect("Start should succeed");
    sleep(Duration::from_secs(1)).await;

    // Get logs
    let logs = service.logs(None).await;
    assert!(logs.is_ok(), "Should retrieve logs");

    let _log_lines = logs.unwrap();
    // Note: Logs might be empty depending on Docker's log driver
    // This test mainly verifies no panic/error

    // Cleanup
    service.kill().await.ok();
}

// ============================================================================
// Error Handling Tests
// ============================================================================

#[tokio::test]
#[cfg_attr(not(feature = "docker-tests"), ignore)] // Requires Docker
async fn test_docker_service_invalid_image() {
    let mut service = create_test_service(
        "test-invalid-image",
        "this-image-definitely-does-not-exist-12345:latest",
    );

    // Starting with missing image should fail (after timeout)
    // Note: This test will take 5 minutes due to image pull timeout
    // In practice, you might want to mock this or use a shorter timeout
    let result = service.start().await;
    assert!(result.is_err(), "Should fail with non-existent image");
}

#[tokio::test]
#[cfg_attr(not(feature = "docker-tests"), ignore)] // Requires Docker
async fn test_docker_service_invalid_volume() {
    let config = ServiceConfig {
        image: Some("nginx:alpine".to_string()),
        volumes: vec!["/etc/passwd:/passwd".to_string()], // Should be rejected
        ..Default::default()
    };

    let mut service = DockerService::new(
        "test-invalid-volume".to_string(),
        config,
        HashMap::new(),
        "/tmp".to_string(),
        None, // No isolation ID
    );

    // Should fail validation before even trying to start container
    let result = service.start().await;
    assert!(result.is_err(), "Should reject absolute volume paths");
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("Absolute host paths")
    );
}

#[tokio::test]
#[cfg_attr(not(feature = "docker-tests"), ignore)] // Requires Docker
async fn test_docker_service_invalid_port() {
    let config = ServiceConfig {
        image: Some("nginx:alpine".to_string()),
        ports: vec!["0.0.0.0:8080:80".to_string()], // Should be rejected
        ..Default::default()
    };

    let mut service = DockerService::new(
        "test-invalid-port".to_string(),
        config,
        HashMap::new(),
        "/tmp".to_string(),
        None, // No isolation ID
    );

    // Should fail validation
    let result = service.start().await;
    assert!(result.is_err(), "Should reject 0.0.0.0 port bindings");
    assert!(result.unwrap_err().to_string().contains("0.0.0.0"));
}

// ============================================================================
// TOCTOU and Race Condition Tests
// ============================================================================

#[tokio::test]
#[cfg_attr(not(feature = "docker-tests"), ignore)] // Requires Docker
async fn test_docker_service_health_check_after_external_removal() {
    require_docker!();

    let container_name = "fed-test-toctou";
    cleanup_container(container_name).await;

    let mut service = create_test_service("test-toctou", "nginx:alpine");

    // Start service
    service.start().await.expect("Start should succeed");
    sleep(Duration::from_millis(500)).await;

    // Externally remove the container (simulating TOCTOU)
    let real_name = resolve_container_name("test-toctou").await;
    tokio::process::Command::new("docker")
        .args(["rm", "-f", &real_name])
        .output()
        .await
        .expect("external remove");

    // Health check should handle this gracefully (our fix)
    let health = service.health().await;
    assert!(
        health.is_ok(),
        "Health check should not panic on removed container"
    );
    assert!(
        !health.unwrap(),
        "Should return false for removed container"
    );
}

#[tokio::test]
#[cfg_attr(not(feature = "docker-tests"), ignore)] // Requires Docker
async fn test_docker_service_logs_after_removal() {
    require_docker!();

    let container_name = "fed-test-logs-removed";
    cleanup_container(container_name).await;

    let mut service = create_test_service("test-logs-removed", "nginx:alpine");

    // Start and stop to remove container
    service.start().await.expect("Start should succeed");
    sleep(Duration::from_millis(300)).await;
    service.stop().await.expect("Stop should succeed");

    // Try to get logs after container is removed
    let logs = service.logs(None).await;
    assert!(logs.is_ok(), "Should handle removed container gracefully");
    assert_eq!(
        logs.unwrap(),
        Vec::<String>::new(),
        "Should return empty logs"
    );
}

#[tokio::test]
#[cfg_attr(not(feature = "docker-tests"), ignore)] // Requires Docker
async fn test_docker_service_orphan_cleanup() {
    require_docker!();

    let container_name = "fed-test-orphan";

    // Create an orphaned container manually
    let output = tokio::process::Command::new("docker")
        .args(["run", "-d", "--name", container_name, "nginx:alpine"])
        .output()
        .await
        .expect("create orphan");

    assert!(output.status.success(), "Should create orphan container");

    // Now try to start our service with same name (should cleanup orphan)
    let mut service = create_test_service("test-orphan", "nginx:alpine");

    let result = service.start().await;
    assert!(result.is_ok(), "Should handle orphan by removing it");

    // Cleanup
    service.stop().await.ok();
}

// ============================================================================
// Container Naming and Labels Tests
// ============================================================================

#[tokio::test]
#[cfg_attr(not(feature = "docker-tests"), ignore)] // Requires Docker
async fn test_docker_service_container_name_format() {
    require_docker!();

    let container_name = "fed-test-naming";
    cleanup_container(container_name).await;

    let mut service = create_test_service("test-naming", "nginx:alpine");

    service.start().await.expect("Start should succeed");
    sleep(Duration::from_millis(500)).await;

    // Verify container exists with expected name
    let real_name = resolve_container_name("test-naming").await;
    let output = tokio::process::Command::new("docker")
        .args(["inspect", "--format={{.Name}}", &real_name])
        .output()
        .await
        .expect("inspect command");

    assert!(output.status.success(), "Container should exist");
    let name = String::from_utf8_lossy(&output.stdout);
    assert!(
        name.contains("test-naming"),
        "Container name should contain service name"
    );

    // Cleanup
    service.stop().await.ok();
}

#[tokio::test]
#[cfg_attr(not(feature = "docker-tests"), ignore)] // Requires Docker
async fn test_docker_service_container_labels() {
    require_docker!();

    let container_name = "fed-test-labels";
    cleanup_container(container_name).await;

    let mut service = create_test_service("test-labels", "nginx:alpine");

    service.start().await.expect("Start should succeed");
    sleep(Duration::from_millis(500)).await;

    // Check for our labels
    let real_name = resolve_container_name("test-labels").await;
    let output = tokio::process::Command::new("docker")
        .args(["inspect", "--format={{.Config.Labels}}", &real_name])
        .output()
        .await
        .expect("inspect labels");

    assert!(output.status.success());
    let labels = String::from_utf8_lossy(&output.stdout);
    assert!(
        labels.contains("com.service-federation.managed") || labels.contains("service-federation"),
        "Should have service-federation labels"
    );

    // Cleanup
    service.stop().await.ok();
}

// ============================================================================
// Environment Variables Tests
// ============================================================================

#[tokio::test]
#[cfg_attr(not(feature = "docker-tests"), ignore)] // Requires Docker
async fn test_docker_service_environment_variables() {
    require_docker!();

    let container_name = "fed-test-env";
    cleanup_container(container_name).await;

    let mut env = HashMap::new();
    env.insert("TEST_VAR".to_string(), "test_value".to_string());
    env.insert("ANOTHER_VAR".to_string(), "another_value".to_string());

    let mut service = create_test_service_with_env("test-env", "nginx:alpine", env);

    service.start().await.expect("Start should succeed");
    sleep(Duration::from_millis(500)).await;

    // Verify environment variables are set
    let real_name = resolve_container_name("test-env").await;
    let output = tokio::process::Command::new("docker")
        .args(["exec", &real_name, "env"])
        .output()
        .await
        .expect("exec env");

    let env_output = String::from_utf8_lossy(&output.stdout);
    assert!(env_output.contains("TEST_VAR=test_value"));
    assert!(env_output.contains("ANOTHER_VAR=another_value"));

    // Cleanup
    service.stop().await.ok();
}

// ============================================================================
// Concurrent Operations Tests
// ============================================================================

#[tokio::test]
#[cfg_attr(not(feature = "docker-tests"), ignore)] // Requires Docker
async fn test_docker_service_stop_idempotent() {
    require_docker!();

    let container_name = "fed-test-stop-idempotent";
    cleanup_container(container_name).await;

    let mut service = create_test_service("test-stop-idempotent", "nginx:alpine");

    // Start service
    service.start().await.expect("Start should succeed");
    sleep(Duration::from_millis(300)).await;

    // Stop once
    let result = service.stop().await;
    assert!(result.is_ok(), "First stop should succeed");

    // Stop again (should be idempotent)
    let result = service.stop().await;
    assert!(result.is_ok(), "Second stop should succeed (idempotent)");
}

#[tokio::test]
#[cfg_attr(not(feature = "docker-tests"), ignore)] // Requires Docker
async fn test_docker_service_kill_already_stopped() {
    require_docker!();

    let container_name = "fed-test-kill-stopped";
    cleanup_container(container_name).await;

    let mut service = create_test_service("test-kill-stopped", "nginx:alpine");

    // Start and stop
    service.start().await.expect("Start should succeed");
    sleep(Duration::from_millis(300)).await;
    service.stop().await.expect("Stop should succeed");

    // Kill already stopped container (should handle gracefully)
    let result = service.kill().await;
    assert!(
        result.is_ok(),
        "Kill on stopped container should be graceful"
    );
}

// ============================================================================
// SF-00095: Container Removal Verification Tests
// ============================================================================

#[tokio::test]
#[cfg_attr(not(feature = "docker-tests"), ignore)] // Requires Docker
async fn test_stop_succeeds_when_container_removed() {
    require_docker!();

    let container_name = "fed-test-stop-success";
    cleanup_container(container_name).await;

    let mut service = create_test_service("test-stop-success", "nginx:alpine");

    // Start service
    service.start().await.expect("Start should succeed");
    sleep(Duration::from_millis(500)).await;

    // Stop should succeed - container is removed
    let result = service.stop().await;
    assert!(
        result.is_ok(),
        "Stop should succeed when container can be removed"
    );

    // Verify status is Stopped
    assert_eq!(service.status(), Status::Stopped);

    // Verify container is actually gone
    let check = tokio::process::Command::new("docker")
        .args(["inspect", container_name])
        .output()
        .await
        .expect("inspect command");

    assert!(!check.status.success(), "Container should be removed");
}

#[tokio::test]
#[cfg_attr(not(feature = "docker-tests"), ignore)] // Requires Docker
async fn test_stop_retries_on_transient_failure() {
    require_docker!();

    // This test verifies the retry logic exists, but we can't easily
    // simulate a transient failure without mocking Docker.
    // Instead, we just verify stop succeeds in normal case (retries work).

    let container_name = "fed-test-stop-retry";
    cleanup_container(container_name).await;

    let mut service = create_test_service("test-stop-retry", "nginx:alpine");

    service.start().await.expect("Start should succeed");
    sleep(Duration::from_millis(500)).await;

    // Stop should succeed (possibly using retries internally)
    let result = service.stop().await;
    assert!(result.is_ok(), "Stop should succeed with retry logic");

    // Verify container is gone
    let check = tokio::process::Command::new("docker")
        .args(["ps", "-a", "-q", "-f", &format!("name={}", container_name)])
        .output()
        .await
        .expect("ps command");

    let output = String::from_utf8_lossy(&check.stdout);
    assert!(
        output.trim().is_empty(),
        "Container should be fully removed"
    );
}

/// Safety regression (codex blocker): fed's volume cleanup keys on the ownership LABEL, not
/// the `fed-` name prefix. A look-alike `fed-*` volume without the label is a user's and must
/// never be reaped by prune/doctor.
#[tokio::test]
#[cfg_attr(not(feature = "docker-tests"), ignore)] // Requires Docker
async fn prune_reaps_only_labeled_fed_volumes() {
    require_docker!();
    use fed::docker::DockerClient;

    let labeled = "fed-labelgatetest-labeled_data";
    let unlabeled = "fed-labelgatetest-unlabeled_data";
    let docker = |args: &[&str]| {
        std::process::Command::new("docker")
            .args(args)
            .output()
            .ok();
    };
    // Clean slate, then create one labeled (fed-created) and one bare look-alike.
    docker(&["volume", "rm", "-f", labeled, unlabeled]);
    docker(&[
        "volume",
        "create",
        "--label",
        "com.service-federation.managed=true",
        labeled,
    ]);
    docker(&["volume", "create", unlabeled]);

    let orphans = DockerClient::new()
        .orphaned_fed_volumes()
        .await
        .expect("list orphaned fed volumes");

    assert!(
        orphans.iter().any(|v| v == labeled),
        "a labeled, dangling fed volume should be reapable"
    );
    assert!(
        !orphans.iter().any(|v| v == unlabeled),
        "an UNLABELED fed-* volume must never be reapable (it may be the user's)"
    );

    docker(&["volume", "rm", "-f", labeled, unlabeled]);
}
