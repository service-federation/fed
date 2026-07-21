use fed::{Orchestrator, Parser};
use std::path::Path;
use std::time::Duration;
use tokio::time::sleep;

/// Absolute path to the shared compose fixture.
///
/// These tests construct the orchestrator with a temp dir as its work_dir, and
/// a service's `composeFile` is resolved against that work_dir. A repo-relative
/// path would therefore be looked up under the temp dir and not found, so the
/// fixtures use this absolute path (built from the crate root at compile time).
const COMPOSE_FIXTURE: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/tests/fixtures/compose/test-services.yml"
);

/// Serialize these tests: they share one compose fixture and therefore ONE
/// compose project (the project name is derived from the compose file path),
/// and stopping any service runs `compose down` on the whole project. Run in
/// parallel, tests tear down each other's containers mid-test.
static COMPOSE_PROJECT_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// Compute the compose project name from a compose file path (same logic as DockerComposeService).
fn compose_project_name(compose_file: &Path) -> String {
    let canonical =
        std::fs::canonicalize(compose_file).unwrap_or_else(|_| compose_file.to_path_buf());
    let bytes = canonical.as_os_str().as_encoded_bytes();
    // FNV-1a 32-bit hash (matches service::fnv1a_32)
    const FNV_OFFSET: u32 = 2_166_136_261;
    const FNV_PRIME: u32 = 16_777_619;
    let mut hash = FNV_OFFSET;
    for &byte in bytes {
        hash ^= byte as u32;
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    format!("fed-{:04x}", hash & 0xFFFF)
}

/// Helper to clean up compose projects after tests
async fn cleanup_compose_project_by_path(compose_file: &Path) {
    let project_name = compose_project_name(compose_file);

    let _ = tokio::process::Command::new("docker")
        .args([
            "compose",
            "-p",
            &project_name,
            "down",
            "-v",
            "--remove-orphans",
            "-t",
            "1",
        ])
        .output()
        .await;

    let _ = tokio::process::Command::new("docker-compose")
        .args([
            "-p",
            &project_name,
            "down",
            "-v",
            "--remove-orphans",
            "-t",
            "1",
        ])
        .output()
        .await;

    sleep(Duration::from_millis(500)).await;
}

/// Helper to check if docker is available
async fn docker_available() -> bool {
    tokio::process::Command::new("docker")
        .args(["version"])
        .output()
        .await
        .map(|o| o.status.success())
        .unwrap_or(false)
}

/// Helper to check if docker compose is available
async fn docker_compose_available() -> bool {
    // Try v2
    let v2 = tokio::process::Command::new("docker")
        .args(["compose", "version"])
        .output()
        .await
        .map(|o| o.status.success())
        .unwrap_or(false);

    if v2 {
        return true;
    }

    // Try v1
    tokio::process::Command::new("docker-compose")
        .args(["--version"])
        .output()
        .await
        .map(|o| o.status.success())
        .unwrap_or(false)
}

#[tokio::test]
#[cfg_attr(not(feature = "docker-tests"), ignore)] // Requires Docker (CI: cargo test --features docker-tests)
async fn test_compose_file_not_found() {
    let _project_guard = COMPOSE_PROJECT_LOCK.lock().await;
    let yaml = r#"
services:
  test:
    composeFile: ./nonexistent.yml
    composeService: nginx
"#;

    let parser = Parser::new();
    let config = parser
        .parse_config(&yaml.replace("tests/fixtures/compose/test-services.yml", COMPOSE_FIXTURE))
        .expect("Failed to parse");

    let orch_temp_dir = tempfile::tempdir().unwrap();
    let mut orchestrator = Orchestrator::new(config, orch_temp_dir.path().to_path_buf())
        .await
        .unwrap();
    orchestrator.set_auto_resolve_conflicts(true);
    let result = orchestrator.initialize().await;

    // Should fail because compose file doesn't exist
    assert!(result.is_err());
    let err_msg = result.unwrap_err().to_string();
    assert!(err_msg.contains("does not exist") || err_msg.contains("Compose file"));
}

#[tokio::test]
#[cfg_attr(not(feature = "docker-tests"), ignore)] // Requires Docker (CI: cargo test --features docker-tests)
async fn test_compose_service_not_found() {
    let _project_guard = COMPOSE_PROJECT_LOCK.lock().await;
    if !docker_available().await || !docker_compose_available().await {
        eprintln!("Skipping test - Docker or Docker Compose not available");
        return;
    }

    let yaml = r#"
services:
  test:
    composeFile: tests/fixtures/compose/test-services.yml
    composeService: nonexistent_service
"#;

    let parser = Parser::new();
    let config = parser
        .parse_config(&yaml.replace("tests/fixtures/compose/test-services.yml", COMPOSE_FIXTURE))
        .expect("Failed to parse");

    let orch_temp_dir = tempfile::tempdir().unwrap();
    let mut orchestrator = Orchestrator::new(config, orch_temp_dir.path().to_path_buf())
        .await
        .unwrap();
    orchestrator.set_auto_resolve_conflicts(true);
    orchestrator.initialize().await.expect("Init failed");

    let result = orchestrator.start("test").await;

    // Should fail because service doesn't exist in compose file
    assert!(result.is_err());
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("not found") || err_msg.contains("no such service"),
        "Expected 'not found' error, got: {}",
        err_msg
    );
}

#[tokio::test]
#[cfg_attr(not(feature = "docker-tests"), ignore)] // Requires Docker (CI: cargo test --features docker-tests)
async fn test_compose_basic_lifecycle() {
    let _project_guard = COMPOSE_PROJECT_LOCK.lock().await;
    if !docker_available().await || !docker_compose_available().await {
        eprintln!("Skipping test - Docker or Docker Compose not available");
        return;
    }

    let compose_file = Path::new(COMPOSE_FIXTURE);
    cleanup_compose_project_by_path(compose_file).await;

    let yaml = r#"
services:
  nginx:
    composeFile: tests/fixtures/compose/test-services.yml
    composeService: nginx
"#;

    let parser = Parser::new();
    let config = parser
        .parse_config(&yaml.replace("tests/fixtures/compose/test-services.yml", COMPOSE_FIXTURE))
        .expect("Failed to parse");

    let orch_temp_dir = tempfile::tempdir().unwrap();
    let mut orchestrator = Orchestrator::new(config, orch_temp_dir.path().to_path_buf())
        .await
        .unwrap();
    orchestrator.set_auto_resolve_conflicts(true);
    orchestrator.initialize().await.expect("Init failed");

    // Start service
    orchestrator.start("nginx").await.expect("Start failed");

    // Give it time to start
    sleep(Duration::from_secs(2)).await;

    // Check status - should be running
    let status = orchestrator
        .get_service("nginx")
        .await
        .expect("Get status failed");
    assert!(
        status.to_string().contains("running") || status.to_string().contains("healthy"),
        "Service should be running, got: {}",
        status
    );

    // Stop service
    orchestrator.stop("nginx").await.expect("Stop failed");

    // Check status - should be stopped
    let status = orchestrator
        .get_service("nginx")
        .await
        .expect("Get status failed");
    assert_eq!(status.to_string(), "stopped");

    cleanup_compose_project_by_path(compose_file).await;
}

#[tokio::test]
#[cfg_attr(not(feature = "docker-tests"), ignore)] // Requires Docker (CI: cargo test --features docker-tests)
async fn test_compose_environment_override() {
    let _project_guard = COMPOSE_PROJECT_LOCK.lock().await;
    if !docker_available().await || !docker_compose_available().await {
        eprintln!("Skipping test - Docker or Docker Compose not available");
        return;
    }

    let compose_file = Path::new(COMPOSE_FIXTURE);
    cleanup_compose_project_by_path(compose_file).await;

    let yaml = r#"
parameters:
  TEST_VALUE:
    default: overridden_value

services:
  busybox:
    composeFile: tests/fixtures/compose/test-services.yml
    composeService: busybox
    environment:
      TEST_VAR: '{{TEST_VALUE}}'
"#;

    let parser = Parser::new();
    let config = parser
        .parse_config(&yaml.replace("tests/fixtures/compose/test-services.yml", COMPOSE_FIXTURE))
        .expect("Failed to parse");

    let orch_temp_dir = tempfile::tempdir().unwrap();
    let mut orchestrator = Orchestrator::new(config, orch_temp_dir.path().to_path_buf())
        .await
        .unwrap();
    orchestrator.set_auto_resolve_conflicts(true);
    orchestrator.initialize().await.expect("Init failed");

    orchestrator.start("busybox").await.expect("Start failed");
    sleep(Duration::from_secs(2)).await;

    // Get logs to verify environment override
    let logs = orchestrator
        .get_logs("busybox", Some(50))
        .await
        .expect("Get logs failed");
    let logs_text = logs.join("\n");

    // Should contain overridden value, not default
    assert!(
        logs_text.contains("overridden_value"),
        "Logs should contain overridden value, got: {}",
        logs_text
    );

    orchestrator.stop("busybox").await.expect("Stop failed");
    cleanup_compose_project_by_path(compose_file).await;
}

#[tokio::test]
#[cfg_attr(not(feature = "docker-tests"), ignore)] // Requires Docker (CI: cargo test --features docker-tests)
async fn test_compose_with_process_dependency() {
    let _project_guard = COMPOSE_PROJECT_LOCK.lock().await;
    if !docker_available().await || !docker_compose_available().await {
        eprintln!("Skipping test - Docker or Docker Compose not available");
        return;
    }

    let compose_file = Path::new(COMPOSE_FIXTURE);
    cleanup_compose_project_by_path(compose_file).await;

    let yaml = r#"
services:
  redis:
    composeFile: tests/fixtures/compose/test-services.yml
    composeService: redis

  app:
    process: |
      echo "Connecting to redis..."
      sleep 2
      echo "App running"
      sleep 30
    depends_on:
      - redis
"#;

    let parser = Parser::new();
    let config = parser
        .parse_config(&yaml.replace("tests/fixtures/compose/test-services.yml", COMPOSE_FIXTURE))
        .expect("Failed to parse");

    let orch_temp_dir = tempfile::tempdir().unwrap();
    let mut orchestrator = Orchestrator::new(config, orch_temp_dir.path().to_path_buf())
        .await
        .unwrap();
    orchestrator.set_auto_resolve_conflicts(true);
    orchestrator.initialize().await.expect("Init failed");

    // Start app - should start redis first
    orchestrator.start("app").await.expect("Start failed");
    sleep(Duration::from_secs(3)).await;

    // Both should be running
    let redis_status = orchestrator
        .get_service("redis")
        .await
        .expect("Get redis status failed");
    let app_status = orchestrator
        .get_service("app")
        .await
        .expect("Get app status failed");

    assert!(
        redis_status.to_string().contains("running")
            || redis_status.to_string().contains("healthy"),
        "Redis should be running"
    );
    assert!(
        app_status.to_string().contains("running") || app_status.to_string().contains("healthy"),
        "App should be running"
    );

    orchestrator.stop_all().await.expect("Stop all failed");
    cleanup_compose_project_by_path(compose_file).await;
}

#[tokio::test]
#[cfg_attr(not(feature = "docker-tests"), ignore)] // Requires Docker (CI: cargo test --features docker-tests)
async fn test_compose_project_isolation() {
    let _project_guard = COMPOSE_PROJECT_LOCK.lock().await;
    if !docker_available().await || !docker_compose_available().await {
        eprintln!("Skipping test - Docker or Docker Compose not available");
        return;
    }

    // Create two separate compose files in temp locations
    let temp_dir1 = tempfile::tempdir().unwrap();
    let temp_dir2 = tempfile::tempdir().unwrap();

    let compose1_path = temp_dir1.path().join("docker-compose.yml");
    let compose2_path = temp_dir2.path().join("docker-compose.yml");

    let compose_content = r#"
version: '3.8'
services:
  test:
    image: nginx:alpine
    ports:
      - "0:80"
"#;

    std::fs::write(&compose1_path, compose_content).unwrap();
    std::fs::write(&compose2_path, compose_content).unwrap();

    let yaml1 = format!(
        r#"
services:
  nginx1:
    composeFile: {}
    composeService: test
"#,
        compose1_path.display()
    );

    let yaml2 = format!(
        r#"
services:
  nginx2:
    composeFile: {}
    composeService: test
"#,
        compose2_path.display()
    );

    let parser = Parser::new();

    let config1 = parser.parse_config(&yaml1).expect("Failed to parse 1");
    let orch1_temp = tempfile::tempdir().unwrap();
    let mut orch1 = Orchestrator::new(config1, orch1_temp.path().to_path_buf())
        .await
        .unwrap();
    orch1.set_auto_resolve_conflicts(true);
    orch1.initialize().await.expect("Init 1 failed");
    orch1.start("nginx1").await.expect("Start 1 failed");

    // Different compose file path → different project name → isolated
    let config2 = parser.parse_config(&yaml2).expect("Failed to parse 2");
    let orch2_temp = tempfile::tempdir().unwrap();
    let mut orch2 = Orchestrator::new(config2, orch2_temp.path().to_path_buf())
        .await
        .unwrap();
    orch2.set_auto_resolve_conflicts(true);
    orch2.initialize().await.expect("Init 2 failed");
    orch2.start("nginx2").await.expect("Start 2 failed");

    sleep(Duration::from_secs(2)).await;

    // Both should be running independently
    let status1 = orch1
        .get_service("nginx1")
        .await
        .expect("Get status 1 failed");
    let status2 = orch2
        .get_service("nginx2")
        .await
        .expect("Get status 2 failed");

    assert!(status1.to_string().contains("running") || status1.to_string().contains("healthy"));
    assert!(status2.to_string().contains("running") || status2.to_string().contains("healthy"));

    // Cleanup both orchestrators
    orch1.stop_all().await.expect("Stop 1 failed");
    orch2.stop_all().await.expect("Stop 2 failed");

    // Clean up both projects
    cleanup_compose_project_by_path(&compose1_path).await;
    cleanup_compose_project_by_path(&compose2_path).await;
}

#[tokio::test]
#[cfg_attr(not(feature = "docker-tests"), ignore)] // Requires Docker (CI: cargo test --features docker-tests)
async fn test_compose_idempotent_start() {
    let _project_guard = COMPOSE_PROJECT_LOCK.lock().await;
    if !docker_available().await || !docker_compose_available().await {
        eprintln!("Skipping test - Docker or Docker Compose not available");
        return;
    }

    let compose_file = Path::new(COMPOSE_FIXTURE);
    cleanup_compose_project_by_path(compose_file).await;

    let yaml = r#"
services:
  nginx:
    composeFile: tests/fixtures/compose/test-services.yml
    composeService: nginx
"#;

    let parser = Parser::new();
    let config = parser
        .parse_config(&yaml.replace("tests/fixtures/compose/test-services.yml", COMPOSE_FIXTURE))
        .expect("Failed to parse");

    let orch_temp_dir = tempfile::tempdir().unwrap();
    let mut orchestrator = Orchestrator::new(config, orch_temp_dir.path().to_path_buf())
        .await
        .unwrap();
    orchestrator.set_auto_resolve_conflicts(true);
    orchestrator.initialize().await.expect("Init failed");

    // Start once
    orchestrator
        .start("nginx")
        .await
        .expect("First start failed");
    sleep(Duration::from_secs(2)).await;

    // Start again - should be idempotent
    let result = orchestrator.start("nginx").await;
    assert!(result.is_ok(), "Second start should succeed");

    orchestrator.stop("nginx").await.expect("Stop failed");
    cleanup_compose_project_by_path(compose_file).await;
}

#[tokio::test]
#[cfg_attr(not(feature = "docker-tests"), ignore)] // Requires Docker (CI: cargo test --features docker-tests)
async fn test_compose_health_check() {
    let _project_guard = COMPOSE_PROJECT_LOCK.lock().await;
    if !docker_available().await || !docker_compose_available().await {
        eprintln!("Skipping test - Docker or Docker Compose not available");
        return;
    }

    let compose_file = Path::new(COMPOSE_FIXTURE);
    cleanup_compose_project_by_path(compose_file).await;

    let yaml = r#"
services:
  nginx:
    composeFile: tests/fixtures/compose/test-services.yml
    composeService: nginx
"#;

    let parser = Parser::new();
    let config = parser
        .parse_config(&yaml.replace("tests/fixtures/compose/test-services.yml", COMPOSE_FIXTURE))
        .expect("Failed to parse");

    let orch_temp_dir = tempfile::tempdir().unwrap();
    let mut orchestrator = Orchestrator::new(config, orch_temp_dir.path().to_path_buf())
        .await
        .unwrap();
    orchestrator.set_auto_resolve_conflicts(true);
    orchestrator.initialize().await.expect("Init failed");

    orchestrator.start("nginx").await.expect("Start failed");

    // Wait for container to be fully up (compose has its own healthcheck)
    sleep(Duration::from_secs(3)).await;

    let status = orchestrator
        .get_service("nginx")
        .await
        .expect("Get status failed");

    // Should be running (compose services use compose's own healthcheck)
    assert!(
        status.to_string().contains("running") || status.to_string().contains("healthy"),
        "Service should be running or healthy, got: {}",
        status
    );

    orchestrator.stop("nginx").await.expect("Stop failed");
    cleanup_compose_project_by_path(compose_file).await;
}

#[tokio::test]
#[cfg_attr(not(feature = "docker-tests"), ignore)] // Requires Docker (CI: cargo test --features docker-tests)
async fn test_compose_port_conflict() {
    let _project_guard = COMPOSE_PROJECT_LOCK.lock().await;
    if !docker_available().await || !docker_compose_available().await {
        eprintln!("Skipping test - Docker or Docker Compose not available");
        return;
    }

    let compose_file = Path::new(COMPOSE_FIXTURE);
    cleanup_compose_project_by_path(compose_file).await;

    // Start a service on port 18080
    let yaml1 = r#"
services:
  nginx1:
    composeFile: tests/fixtures/compose/test-services.yml
    composeService: nginx
"#;

    let parser = Parser::new();
    let config1 = parser
        .parse_config(&yaml1.replace("tests/fixtures/compose/test-services.yml", COMPOSE_FIXTURE))
        .expect("Failed to parse");

    let orch1_temp = tempfile::tempdir().unwrap();
    let mut orch1 = Orchestrator::new(config1, orch1_temp.path().to_path_buf())
        .await
        .unwrap();
    orch1.set_auto_resolve_conflicts(true);
    orch1.initialize().await.expect("Init failed");
    orch1.start("nginx1").await.expect("Start 1 failed");

    sleep(Duration::from_secs(2)).await;

    // Try to start another service on the same port
    let config2 = parser
        .parse_config(&yaml1.replace("tests/fixtures/compose/test-services.yml", COMPOSE_FIXTURE))
        .expect("Failed to parse");
    let orch2_temp = tempfile::tempdir().unwrap();
    let mut orch2 = Orchestrator::new(config2, orch2_temp.path().to_path_buf())
        .await
        .unwrap();
    orch2.set_auto_resolve_conflicts(true);
    orch2.initialize().await.expect("Init 2 failed");

    let result = orch2.start("nginx1").await;

    // Should fail due to port conflict (or succeed if idempotent)
    // The behavior depends on docker compose version
    if let Err(e) = result {
        let err_msg = e.to_string();
        assert!(
            err_msg.contains("port") || err_msg.contains("already") || err_msg.contains("conflict"),
            "Expected port conflict error, got: {}",
            err_msg
        );
    }

    orch1.stop_all().await.expect("Stop failed");
    cleanup_compose_project_by_path(compose_file).await;
}

#[tokio::test]
#[cfg_attr(not(feature = "docker-tests"), ignore)] // Requires Docker, tests cleanup behavior
async fn test_compose_process_cleanup() {
    let _project_guard = COMPOSE_PROJECT_LOCK.lock().await;
    if !docker_available().await || !docker_compose_available().await {
        eprintln!("Skipping test - Docker or Docker Compose not available");
        return;
    }

    let temp_dir = tempfile::tempdir().unwrap();
    let compose_path = temp_dir.path().join("docker-compose.yml");

    // Create a simple compose file with random ports
    let compose_content = r#"
version: '3.8'
services:
  test-nginx:
    image: nginx:alpine
    ports:
      - "0:80"
  test-redis:
    image: redis:7-alpine
    ports:
      - "0:6379"
"#;

    std::fs::write(&compose_path, compose_content).unwrap();

    let yaml = format!(
        r#"
services:
  compose-process:
    cwd: {}
    process: docker compose up
"#,
        temp_dir.path().display()
    );

    let parser = Parser::new();
    let config = parser.parse_config(&yaml).expect("Failed to parse");

    let orch_temp_dir = tempfile::tempdir().unwrap();
    let mut orchestrator = Orchestrator::new(config, orch_temp_dir.path().to_path_buf())
        .await
        .unwrap();
    orchestrator.set_auto_resolve_conflicts(true);
    orchestrator.initialize().await.expect("Init failed");
    orchestrator
        .start("compose-process")
        .await
        .expect("Start failed");

    // Wait for containers to start
    sleep(Duration::from_secs(5)).await;

    // Verify containers are running
    let ps_output = tokio::process::Command::new("docker")
        .args([
            "compose",
            "-f",
            compose_path.to_str().unwrap(),
            "ps",
            "--format",
            "json",
        ])
        .output()
        .await
        .expect("Failed to run docker compose ps");

    let ps_before = String::from_utf8_lossy(&ps_output.stdout);
    assert!(
        ps_before.contains("running") || ps_before.contains("Up"),
        "Containers should be running"
    );

    // Stop the service
    orchestrator
        .stop("compose-process")
        .await
        .expect("Stop failed");

    // Wait a bit for cleanup
    sleep(Duration::from_secs(3)).await;

    // Verify containers are stopped (should work now with process group handling)
    let ps_output = tokio::process::Command::new("docker")
        .args([
            "compose",
            "-f",
            compose_path.to_str().unwrap(),
            "ps",
            "--format",
            "json",
        ])
        .output()
        .await
        .expect("Failed to run docker compose ps");

    let ps_after = String::from_utf8_lossy(&ps_output.stdout);

    // With the fix, containers should be stopped
    assert!(
        !ps_after.contains("running") && !ps_after.contains("Up"),
        "Containers should be stopped but are still running: {}",
        ps_after
    );

    // Cleanup
    let _ = tokio::process::Command::new("docker")
        .args([
            "compose",
            "-f",
            compose_path.to_str().unwrap(),
            "down",
            "-v",
        ])
        .output()
        .await;
}
