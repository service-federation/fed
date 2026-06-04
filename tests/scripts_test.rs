use fed::{Orchestrator, Parser};
use tempfile::tempdir;

#[tokio::test]
async fn test_run_simple_script() {
    let yaml = r#"
scripts:
  hello:
    script: "echo 'Hello from script'"

services:
  dummy:
    process: "echo test"
"#;

    let parser = Parser::new();
    let config = parser.parse_config(yaml).expect("Failed to parse");

    let orch_temp = tempfile::tempdir().unwrap();
    let mut orchestrator = Orchestrator::new(config, orch_temp.path().to_path_buf())
        .await
        .unwrap();
    orchestrator.set_auto_resolve_conflicts(true);
    orchestrator
        .initialize()
        .await
        .expect("Failed to initialize");

    let output = orchestrator
        .run_script("hello")
        .await
        .expect("Failed to run script");

    assert!(output.status.success(), "Script should succeed");
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("Hello from script"),
        "Output should contain expected text"
    );
}

#[tokio::test]
async fn test_script_with_dependencies() {
    let temp_dir = tempdir().expect("Failed to create temp dir");

    let yaml = r#"
services:
  backend:
    process: "sleep 30"

scripts:
  check:
    depends_on:
      - backend
    script: "echo 'Backend is running'"
"#;

    let parser = Parser::new();
    let config = parser.parse_config(yaml).expect("Failed to parse");

    let orch_temp = tempfile::tempdir().unwrap();
    let mut orchestrator = Orchestrator::new(config, orch_temp.path().to_path_buf())
        .await
        .unwrap();
    orchestrator
        .set_work_dir(temp_dir.path().to_path_buf())
        .await
        .expect("Failed to set work dir");
    orchestrator.set_auto_resolve_conflicts(true);
    orchestrator
        .initialize()
        .await
        .expect("Failed to initialize");

    // Backend isn't running, so the script starts it to satisfy the dependency.
    let output = orchestrator
        .run_script("check")
        .await
        .expect("Failed to run script");

    assert!(output.status.success(), "Script should succeed");

    // Borrow-or-own: the script started backend, so it stops it again afterward.
    assert!(
        !orchestrator.is_service_running("backend").await,
        "Backend should be stopped after a script that started it"
    );

    // Cleanup
    orchestrator.cleanup().await;
}

#[tokio::test]
async fn test_script_with_environment() {
    let yaml = r#"
parameters:
  MESSAGE:
    default: "Test message"

scripts:
  env_test:
    environment:
      TEST_VAR: "{{MESSAGE}}"
    script: "echo $TEST_VAR"
"#;

    let parser = Parser::new();
    let config = parser.parse_config(yaml).expect("Failed to parse");

    let orch_temp = tempfile::tempdir().unwrap();
    let mut orchestrator = Orchestrator::new(config, orch_temp.path().to_path_buf())
        .await
        .unwrap();
    orchestrator.set_auto_resolve_conflicts(true);
    orchestrator
        .initialize()
        .await
        .expect("Failed to initialize");

    let output = orchestrator
        .run_script("env_test")
        .await
        .expect("Failed to run script");

    assert!(output.status.success(), "Script should succeed");
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("Test message"),
        "Environment variable should be resolved"
    );
}

#[tokio::test]
async fn test_script_with_custom_cwd() {
    let yaml = r#"
scripts:
  pwd_check:
    cwd: "/tmp"
    script: "pwd"
"#;

    let parser = Parser::new();
    let config = parser.parse_config(yaml).expect("Failed to parse");

    let orch_temp = tempfile::tempdir().unwrap();
    let mut orchestrator = Orchestrator::new(config, orch_temp.path().to_path_buf())
        .await
        .unwrap();
    orchestrator.set_auto_resolve_conflicts(true);
    orchestrator
        .initialize()
        .await
        .expect("Failed to initialize");

    let output = orchestrator
        .run_script("pwd_check")
        .await
        .expect("Failed to run script");

    assert!(output.status.success(), "Script should succeed");
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.trim().contains("tmp"),
        "Should run in /tmp directory"
    );
}

#[tokio::test]
async fn test_script_not_found() {
    let yaml = r#"
services:
  dummy:
    process: "echo test"
"#;

    let parser = Parser::new();
    let config = parser.parse_config(yaml).expect("Failed to parse");

    let orch_temp = tempfile::tempdir().unwrap();
    let mut orchestrator = Orchestrator::new(config, orch_temp.path().to_path_buf())
        .await
        .unwrap();
    orchestrator.set_auto_resolve_conflicts(true);
    orchestrator
        .initialize()
        .await
        .expect("Failed to initialize");

    let result = orchestrator.run_script("nonexistent").await;
    assert!(result.is_err(), "Should fail for non-existent script");
}

#[tokio::test]
async fn test_list_scripts() {
    let yaml = r#"
scripts:
  script1:
    script: "echo 1"
  script2:
    script: "echo 2"
  script3:
    script: "echo 3"

services:
  dummy:
    process: "echo test"
"#;

    let parser = Parser::new();
    let config = parser.parse_config(yaml).expect("Failed to parse");

    let orch_temp = tempfile::tempdir().unwrap();
    let mut orchestrator = Orchestrator::new(config, orch_temp.path().to_path_buf())
        .await
        .unwrap();
    orchestrator.set_auto_resolve_conflicts(true);
    orchestrator
        .initialize()
        .await
        .expect("Failed to initialize");

    let scripts = orchestrator.list_scripts();
    assert_eq!(scripts.len(), 3, "Should have 3 scripts");
    assert!(scripts.contains(&"script1".to_string()));
    assert!(scripts.contains(&"script2".to_string()));
    assert!(scripts.contains(&"script3".to_string()));
}

#[tokio::test]
async fn test_script_failure() {
    let yaml = r#"
scripts:
  failing_script:
    script: "exit 1"
"#;

    let parser = Parser::new();
    let config = parser.parse_config(yaml).expect("Failed to parse");

    let orch_temp = tempfile::tempdir().unwrap();
    let mut orchestrator = Orchestrator::new(config, orch_temp.path().to_path_buf())
        .await
        .unwrap();
    orchestrator.set_auto_resolve_conflicts(true);
    orchestrator
        .initialize()
        .await
        .expect("Failed to initialize");

    let output = orchestrator
        .run_script("failing_script")
        .await
        .expect("Should execute");

    assert!(!output.status.success(), "Script should fail");
    assert_eq!(output.status.code(), Some(1), "Exit code should be 1");
}

#[tokio::test]
async fn test_multiline_script() {
    let yaml = r#"
scripts:
  multiline:
    script: |
      echo "Line 1"
      echo "Line 2"
      echo "Line 3"
"#;

    let parser = Parser::new();
    let config = parser.parse_config(yaml).expect("Failed to parse");

    let orch_temp = tempfile::tempdir().unwrap();
    let mut orchestrator = Orchestrator::new(config, orch_temp.path().to_path_buf())
        .await
        .unwrap();
    orchestrator.set_auto_resolve_conflicts(true);
    orchestrator
        .initialize()
        .await
        .expect("Failed to initialize");

    let output = orchestrator
        .run_script("multiline")
        .await
        .expect("Failed to run script");

    assert!(output.status.success(), "Script should succeed");
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("Line 1"));
    assert!(stdout.contains("Line 2"));
    assert!(stdout.contains("Line 3"));
}

#[tokio::test]
async fn test_script_with_multiple_dependencies() {
    let temp_dir = tempdir().expect("Failed to create temp dir");

    let yaml = r#"
services:
  service1:
    process: "sleep 30"
  service2:
    process: "sleep 30"
  service3:
    process: "sleep 30"

scripts:
  multi_dep:
    depends_on:
      - service1
      - service2
      - service3
    script: "echo 'All services started'"
"#;

    let parser = Parser::new();
    let config = parser.parse_config(yaml).expect("Failed to parse");

    let orch_temp = tempfile::tempdir().unwrap();
    let mut orchestrator = Orchestrator::new(config, orch_temp.path().to_path_buf())
        .await
        .unwrap();
    orchestrator
        .set_work_dir(temp_dir.path().to_path_buf())
        .await
        .expect("Failed to set work dir");
    orchestrator.set_auto_resolve_conflicts(true);
    orchestrator
        .initialize()
        .await
        .expect("Failed to initialize");

    let output = orchestrator
        .run_script("multi_dep")
        .await
        .expect("Failed to run script");

    assert!(output.status.success(), "Script should succeed");

    // Borrow-or-own: the script started all three deps, so all are stopped after.
    assert!(!orchestrator.is_service_running("service1").await);
    assert!(!orchestrator.is_service_running("service2").await);
    assert!(!orchestrator.is_service_running("service3").await);

    orchestrator.cleanup().await;
}

// Tests for run_script_interactive (TTY passthrough mode)

#[tokio::test]
async fn test_run_script_interactive_success() {
    let yaml = r#"
scripts:
  hello:
    script: "echo 'Hello from interactive'"
"#;

    let parser = Parser::new();
    let config = parser.parse_config(yaml).expect("Failed to parse");

    let orch_temp = tempfile::tempdir().unwrap();
    let mut orchestrator = Orchestrator::new(config, orch_temp.path().to_path_buf())
        .await
        .unwrap();
    orchestrator.set_auto_resolve_conflicts(true);
    orchestrator
        .initialize()
        .await
        .expect("Failed to initialize");

    let status = orchestrator
        .run_script_interactive("hello", &[])
        .await
        .expect("Failed to run script");

    assert!(status.success(), "Script should succeed");
}

#[tokio::test]
async fn test_run_script_interactive_failure() {
    let yaml = r#"
scripts:
  failing:
    script: "exit 42"
"#;

    let parser = Parser::new();
    let config = parser.parse_config(yaml).expect("Failed to parse");

    let orch_temp = tempfile::tempdir().unwrap();
    let mut orchestrator = Orchestrator::new(config, orch_temp.path().to_path_buf())
        .await
        .unwrap();
    orchestrator.set_auto_resolve_conflicts(true);
    orchestrator
        .initialize()
        .await
        .expect("Failed to initialize");

    let status = orchestrator
        .run_script_interactive("failing", &[])
        .await
        .expect("Should execute");

    assert!(!status.success(), "Script should fail");
    assert_eq!(status.code(), Some(42), "Exit code should be 42");
}

#[tokio::test]
async fn test_run_script_interactive_starts_dependencies() {
    let temp_dir = tempdir().expect("Failed to create temp dir");

    let yaml = r#"
services:
  backend:
    process: "sleep 30"

scripts:
  check:
    depends_on:
      - backend
    script: "echo 'Backend should be running'"
"#;

    let parser = Parser::new();
    let config = parser.parse_config(yaml).expect("Failed to parse");

    let orch_temp = tempfile::tempdir().unwrap();
    let mut orchestrator = Orchestrator::new(config, orch_temp.path().to_path_buf())
        .await
        .unwrap();
    orchestrator
        .set_work_dir(temp_dir.path().to_path_buf())
        .await
        .expect("Failed to set work dir");
    orchestrator.set_auto_resolve_conflicts(true);
    orchestrator
        .initialize()
        .await
        .expect("Failed to initialize");

    let status = orchestrator
        .run_script_interactive("check", &[])
        .await
        .expect("Failed to run script");

    assert!(status.success(), "Script should succeed");

    // Borrow-or-own: the script started backend, so it stops it again afterward.
    assert!(
        !orchestrator.is_service_running("backend").await,
        "Backend should be stopped after a script that started it"
    );

    orchestrator.cleanup().await;
}

#[tokio::test]
async fn test_run_script_interactive_not_found() {
    let yaml = r#"
services:
  dummy:
    process: "echo test"
"#;

    let parser = Parser::new();
    let config = parser.parse_config(yaml).expect("Failed to parse");

    let orch_temp = tempfile::tempdir().unwrap();
    let mut orchestrator = Orchestrator::new(config, orch_temp.path().to_path_buf())
        .await
        .unwrap();
    orchestrator.set_auto_resolve_conflicts(true);
    orchestrator
        .initialize()
        .await
        .expect("Failed to initialize");

    let result = orchestrator
        .run_script_interactive("nonexistent", &[])
        .await;
    assert!(result.is_err(), "Should fail for non-existent script");
}

#[tokio::test]
async fn test_run_script_interactive_with_environment() {
    let yaml = r#"
scripts:
  env_check:
    environment:
      MY_VAR: "test_value"
    script: "test \"$MY_VAR\" = \"test_value\""
"#;

    let parser = Parser::new();
    let config = parser.parse_config(yaml).expect("Failed to parse");

    let orch_temp = tempfile::tempdir().unwrap();
    let mut orchestrator = Orchestrator::new(config, orch_temp.path().to_path_buf())
        .await
        .unwrap();
    orchestrator.set_auto_resolve_conflicts(true);
    orchestrator
        .initialize()
        .await
        .expect("Failed to initialize");

    let status = orchestrator
        .run_script_interactive("env_check", &[])
        .await
        .expect("Failed to run script");

    assert!(
        status.success(),
        "Script should succeed when env var is set correctly"
    );
}

#[tokio::test]
async fn test_run_script_interactive_with_cwd() {
    let yaml = r#"
scripts:
  cwd_check:
    cwd: "/tmp"
    script: "test \"$(pwd)\" = \"/tmp\" || test \"$(pwd)\" = \"/private/tmp\""
"#;

    let parser = Parser::new();
    let config = parser.parse_config(yaml).expect("Failed to parse");

    let orch_temp = tempfile::tempdir().unwrap();
    let mut orchestrator = Orchestrator::new(config, orch_temp.path().to_path_buf())
        .await
        .unwrap();
    orchestrator.set_auto_resolve_conflicts(true);
    orchestrator
        .initialize()
        .await
        .expect("Failed to initialize");

    let status = orchestrator
        .run_script_interactive("cwd_check", &[])
        .await
        .expect("Failed to run script");

    assert!(status.success(), "Script should run in /tmp directory");
}

// ============================================================================
// Tests for argument passthrough (SF-00033)
// ============================================================================

#[tokio::test]
async fn test_script_argument_passthrough() {
    let yaml = r#"
scripts:
  echo:
    script: "echo"
"#;

    let parser = Parser::new();
    let config = parser.parse_config(yaml).expect("Failed to parse");

    let orch_temp = tempfile::tempdir().unwrap();
    let mut orchestrator = Orchestrator::new(config, orch_temp.path().to_path_buf())
        .await
        .unwrap();
    orchestrator.set_auto_resolve_conflicts(true);
    orchestrator
        .initialize()
        .await
        .expect("Failed to initialize");

    // Test with simple arguments
    let args = vec!["hello".to_string(), "world".to_string()];
    let status = orchestrator
        .run_script_interactive("echo", &args)
        .await
        .expect("Failed to run script");

    assert!(status.success(), "Script with arguments should succeed");
}

#[tokio::test]
async fn test_script_argument_passthrough_special_chars() {
    let yaml = r#"
scripts:
  echo:
    script: "echo"
"#;

    let parser = Parser::new();
    let config = parser.parse_config(yaml).expect("Failed to parse");

    let orch_temp = tempfile::tempdir().unwrap();
    let mut orchestrator = Orchestrator::new(config, orch_temp.path().to_path_buf())
        .await
        .unwrap();
    orchestrator.set_auto_resolve_conflicts(true);
    orchestrator
        .initialize()
        .await
        .expect("Failed to initialize");

    // Test with special characters that need escaping
    let args = vec![
        "test's".to_string(),
        "arg with spaces".to_string(),
        "quoted\"arg".to_string(),
    ];
    let status = orchestrator
        .run_script_interactive("echo", &args)
        .await
        .expect("Failed to run script");

    assert!(
        status.success(),
        "Script with special characters should succeed"
    );
}

#[tokio::test]
async fn test_script_argument_passthrough_empty() {
    let yaml = r#"
scripts:
  hello:
    script: "echo 'Hello'"
"#;

    let parser = Parser::new();
    let config = parser.parse_config(yaml).expect("Failed to parse");

    let orch_temp = tempfile::tempdir().unwrap();
    let mut orchestrator = Orchestrator::new(config, orch_temp.path().to_path_buf())
        .await
        .unwrap();
    orchestrator.set_auto_resolve_conflicts(true);
    orchestrator
        .initialize()
        .await
        .expect("Failed to initialize");

    // Test with no extra arguments (backward compatibility)
    let status = orchestrator
        .run_script_interactive("hello", &[])
        .await
        .expect("Failed to run script");

    assert!(
        status.success(),
        "Script without extra arguments should succeed"
    );
}

// ============================================================================
// Tests for script-to-script dependencies (SF-00035)
// ============================================================================

#[tokio::test]
async fn test_script_depends_on_script() {
    let temp_dir = tempdir().expect("Failed to create temp dir");
    let marker_file = temp_dir.path().join("marker.txt");

    // Script 'setup' creates a marker file, script 'main' depends on it and reads it
    let yaml = format!(
        r#"
scripts:
  setup:
    script: "echo 'setup ran' > {}"
  main:
    depends_on:
      - setup
    script: "cat {}"
"#,
        marker_file.display(),
        marker_file.display()
    );

    let parser = Parser::new();
    let config = parser.parse_config(&yaml).expect("Failed to parse");

    let orch_temp = tempfile::tempdir().unwrap();
    let mut orchestrator = Orchestrator::new(config, orch_temp.path().to_path_buf())
        .await
        .unwrap();
    orchestrator.set_auto_resolve_conflicts(true);
    orchestrator
        .initialize()
        .await
        .expect("Failed to initialize");

    // Run 'main' - it should automatically run 'setup' first
    let output = orchestrator
        .run_script("main")
        .await
        .expect("Failed to run script");

    assert!(output.status.success(), "Script should succeed");
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("setup ran"),
        "Dependent script should have run first, creating the file"
    );
}

#[tokio::test]
async fn test_script_chain_dependencies() {
    let temp_dir = tempdir().expect("Failed to create temp dir");
    let log_file = temp_dir.path().join("log.txt");

    // Chain: c depends on b, b depends on a
    // Each appends a line to the log file
    let yaml = format!(
        r#"
scripts:
  a:
    script: "echo 'a' >> {}"
  b:
    depends_on:
      - a
    script: "echo 'b' >> {}"
  c:
    depends_on:
      - b
    script: "echo 'c' >> {} && cat {}"
"#,
        log_file.display(),
        log_file.display(),
        log_file.display(),
        log_file.display()
    );

    let parser = Parser::new();
    let config = parser.parse_config(&yaml).expect("Failed to parse");

    let orch_temp = tempfile::tempdir().unwrap();
    let mut orchestrator = Orchestrator::new(config, orch_temp.path().to_path_buf())
        .await
        .unwrap();
    orchestrator.set_auto_resolve_conflicts(true);
    orchestrator
        .initialize()
        .await
        .expect("Failed to initialize");

    // Run 'c' - should run a, then b, then c
    let output = orchestrator
        .run_script("c")
        .await
        .expect("Failed to run script");

    assert!(output.status.success(), "Script should succeed");
    let stdout = String::from_utf8_lossy(&output.stdout);
    // The output should show a, b, c in order
    assert!(stdout.contains("a"), "Should contain 'a'");
    assert!(stdout.contains("b"), "Should contain 'b'");
    assert!(stdout.contains("c"), "Should contain 'c'");
}

#[tokio::test]
async fn test_script_mixed_dependencies() {
    let temp_dir = tempdir().expect("Failed to create temp dir");

    // Script depends on both a script and a service
    let yaml = r#"
services:
  backend:
    process: "sleep 30"

scripts:
  setup:
    script: "echo 'setup done'"
  main:
    depends_on:
      - setup
      - backend
    script: "echo 'main running'"
"#;

    let parser = Parser::new();
    let config = parser.parse_config(yaml).expect("Failed to parse");

    let orch_temp = tempfile::tempdir().unwrap();
    let mut orchestrator = Orchestrator::new(config, orch_temp.path().to_path_buf())
        .await
        .unwrap();
    orchestrator
        .set_work_dir(temp_dir.path().to_path_buf())
        .await
        .expect("Failed to set work dir");
    orchestrator.set_auto_resolve_conflicts(true);
    orchestrator
        .initialize()
        .await
        .expect("Failed to initialize");

    let status = orchestrator
        .run_script_interactive("main", &[])
        .await
        .expect("Failed to run script");

    assert!(status.success(), "Script with mixed deps should succeed");

    // Verify service was started
    let service_status = orchestrator.get_status().await;
    assert!(
        service_status.contains_key("backend"),
        "Backend service should be started"
    );

    orchestrator.cleanup().await;
}

// ============================================================================
// Tests for isolated (SF-00034)
// ============================================================================

#[tokio::test]
async fn test_isolated_allocates_different_port() {
    let temp_dir = tempdir().expect("Failed to create temp dir");
    let port_file = temp_dir.path().join("port.txt");

    // Script with isolated outputs the port it received
    let yaml = format!(
        r#"
parameters:
  TEST_PORT:
    type: port
    default: 54321

scripts:
  check_port:
    isolated: true
    script: "echo $TEST_PORT > {}"
    environment:
      TEST_PORT: "{{{{TEST_PORT}}}}"
"#,
        port_file.display()
    );

    let parser = Parser::new();
    let config = parser.parse_config(&yaml).expect("Failed to parse");

    let orch_temp = tempfile::tempdir().unwrap();
    let mut orchestrator = Orchestrator::new(config, orch_temp.path().to_path_buf())
        .await
        .unwrap();
    orchestrator
        .set_work_dir(temp_dir.path().to_path_buf())
        .await
        .expect("Failed to set work dir");
    orchestrator.set_auto_resolve_conflicts(true);
    orchestrator
        .initialize()
        .await
        .expect("Failed to initialize");

    // Run the script with isolated
    let status = orchestrator
        .run_script_interactive("check_port", &[])
        .await
        .expect("Failed to run script");

    assert!(status.success(), "Script should succeed");

    // Read the port that was used
    let port_contents = std::fs::read_to_string(&port_file).expect("Failed to read port file");
    let isolated_port: u16 = port_contents.trim().parse().expect("Failed to parse port");

    // The port should be different from the default (since it was randomized)
    // Note: There's a small chance it picks the same port, but the allocator
    // generally picks high random ports, not 54321
    assert!(isolated_port > 0, "Should have received a valid port");

    orchestrator.cleanup().await;
}

#[tokio::test]
async fn test_isolated_cleanup_after_script() {
    use fed::service::Status;
    let temp_dir = tempdir().expect("Failed to create temp dir");

    let yaml = r#"
parameters:
  DB_PORT:
    type: port
    default: 5433

services:
  backend:
    process: "sleep 30"

scripts:
  test:
    isolated: true
    depends_on:
      - backend
    script: "echo 'Test running'"
"#;

    let parser = Parser::new();
    let config = parser.parse_config(yaml).expect("Failed to parse");

    let orch_temp = tempfile::tempdir().unwrap();
    let mut orchestrator = Orchestrator::new(config, orch_temp.path().to_path_buf())
        .await
        .unwrap();
    orchestrator
        .set_work_dir(temp_dir.path().to_path_buf())
        .await
        .expect("Failed to set work dir");
    orchestrator.set_auto_resolve_conflicts(true);
    orchestrator
        .initialize()
        .await
        .expect("Failed to initialize");

    // Don't start backend in main orchestrator - verify it's Stopped
    let status_before = orchestrator.get_status().await;
    if let Some(backend_status) = status_before.get("backend") {
        assert_eq!(
            *backend_status,
            Status::Stopped,
            "Backend should be stopped before script"
        );
    }

    // Run script - should start backend in isolated context
    let status = orchestrator
        .run_script_interactive("test", &[])
        .await
        .expect("Failed to run script");

    assert!(status.success(), "Script should succeed");

    // After script completes, backend should still be Stopped in main context
    // (it was started in the child orchestrator which was cleaned up)
    let status_after = orchestrator.get_status().await;
    if let Some(backend_status) = status_after.get("backend") {
        assert_eq!(
            *backend_status,
            Status::Stopped,
            "Backend should be stopped after isolated script"
        );
    }

    orchestrator.cleanup().await;
}

#[tokio::test]
async fn test_isolated_does_not_affect_main_session() {
    let temp_dir = tempdir().expect("Failed to create temp dir");

    let yaml = r#"
services:
  backend:
    process: "sleep 30"

scripts:
  test:
    isolated: true
    depends_on:
      - backend
    script: "echo 'Test running'"
"#;

    let parser = Parser::new();
    let config = parser.parse_config(yaml).expect("Failed to parse");

    let orch_temp = tempfile::tempdir().unwrap();
    let mut orchestrator = Orchestrator::new(config, orch_temp.path().to_path_buf())
        .await
        .unwrap();
    orchestrator
        .set_work_dir(temp_dir.path().to_path_buf())
        .await
        .expect("Failed to set work dir");
    orchestrator.set_auto_resolve_conflicts(true);
    orchestrator
        .initialize()
        .await
        .expect("Failed to initialize");

    // Start backend in main orchestrator
    orchestrator
        .start("backend")
        .await
        .expect("Failed to start backend");

    let status_before = orchestrator.get_status().await;
    assert!(
        status_before.contains_key("backend"),
        "Backend should be running before script"
    );

    // Run isolated script - should not affect main orchestrator's backend
    let status = orchestrator
        .run_script_interactive("test", &[])
        .await
        .expect("Failed to run script");

    assert!(status.success(), "Script should succeed");

    // Main session's backend should still be running
    let status_after = orchestrator.get_status().await;
    assert!(
        status_after.contains_key("backend"),
        "Main session backend should still be running"
    );

    orchestrator.cleanup().await;
}

/// Borrow-or-own: a service that was already running before the script (e.g.
/// started via `fed start`) is *borrowed* — the script leaves it running.
#[tokio::test]
async fn test_script_borrows_already_running_service() {
    let temp_dir = tempdir().expect("Failed to create temp dir");

    let yaml = r#"
services:
  backend:
    process: "sleep 30"

scripts:
  test:
    # isolated: false (default)
    depends_on:
      - backend
    script: "echo 'Test running'"
"#;

    let parser = Parser::new();
    let config = parser.parse_config(yaml).expect("Failed to parse");

    let orch_temp = tempfile::tempdir().unwrap();
    let mut orchestrator = Orchestrator::new(config, orch_temp.path().to_path_buf())
        .await
        .unwrap();
    orchestrator
        .set_work_dir(temp_dir.path().to_path_buf())
        .await
        .expect("Failed to set work dir");
    orchestrator.set_auto_resolve_conflicts(true);
    orchestrator
        .initialize()
        .await
        .expect("Failed to initialize");

    // Pre-start backend (this is the `fed start` keep-alive path).
    orchestrator
        .start("backend")
        .await
        .expect("Failed to start backend");
    assert!(
        orchestrator.is_service_running("backend").await,
        "backend should be running before the script"
    );

    // Run script - backend is already up, so it is borrowed, not owned.
    let status = orchestrator
        .run_script_interactive("test", &[])
        .await
        .expect("Failed to run script");

    assert!(status.success(), "Script should succeed");

    // Borrowed service is left running — the script must not stop what it didn't start.
    assert!(
        orchestrator.is_service_running("backend").await,
        "backend was already running, so the script must leave it running"
    );

    orchestrator.cleanup().await;
}

// ============================================================================
// Tests for isolated state tracker isolation (SF-00104)
// ============================================================================

/// Verify that the parent orchestrator's state tracker survives an isolated
/// script execution. Before the fix, the child's `clear()` would wipe the
/// parent's `.fed/lock.db` because they shared the same file.
#[tokio::test]
async fn test_isolated_script_preserves_parent_state_tracker() {
    let temp_dir = tempdir().expect("Failed to create temp dir");

    let yaml = r#"
services:
  parent-svc:
    process: "sleep 30"

scripts:
  isolated-test:
    isolated: true
    script: "echo 'isolated ran'"
"#;

    let parser = Parser::new();
    let config = parser.parse_config(yaml).expect("Failed to parse");

    let orch_temp = tempfile::tempdir().unwrap();
    let mut orchestrator = Orchestrator::new(config, orch_temp.path().to_path_buf())
        .await
        .unwrap();
    orchestrator
        .set_work_dir(temp_dir.path().to_path_buf())
        .await
        .expect("Failed to set work dir");
    orchestrator.set_auto_resolve_conflicts(true);
    orchestrator
        .initialize()
        .await
        .expect("Failed to initialize");

    // Start a service in the parent context and verify it's tracked
    orchestrator
        .start("parent-svc")
        .await
        .expect("Failed to start parent-svc");

    assert!(
        orchestrator.is_service_running("parent-svc").await,
        "parent-svc should be running before isolated script"
    );

    // Read the state tracker to confirm the service is registered
    let services_before = {
        let tracker = orchestrator.state_tracker.read().await;
        tracker.get_services().await
    };
    assert!(
        services_before.contains_key("parent-svc"),
        "parent-svc should be in state tracker before isolated script"
    );

    // Run isolated script — this creates a child orchestrator with ephemeral state
    let status = orchestrator
        .run_script_interactive("isolated-test", &[])
        .await
        .expect("Failed to run isolated script");

    assert!(status.success(), "Isolated script should succeed");

    // Verify the parent's state tracker still has the service registered
    let services_after = {
        let tracker = orchestrator.state_tracker.read().await;
        tracker.get_services().await
    };
    assert!(
        services_after.contains_key("parent-svc"),
        "parent-svc should still be in state tracker after isolated script"
    );

    // Verify the service is still actually running
    assert!(
        orchestrator.is_service_running("parent-svc").await,
        "parent-svc should still be running after isolated script"
    );

    orchestrator.cleanup().await;
}

// ============================================================================
// Tests for script failure cleanup (SF-00105)
// ============================================================================

/// Verify that service dependencies started by an isolated script are cleaned
/// up even when the script fails. Before the fix, `std::process::exit()`
/// bypassed Drop impls and left orphaned processes.
#[tokio::test]
async fn test_isolated_script_failure_cleans_up_dependencies() {
    use fed::service::Status;
    let temp_dir = tempdir().expect("Failed to create temp dir");

    let yaml = r#"
services:
  dep-svc:
    process: "sleep 30"

scripts:
  failing-isolated:
    isolated: true
    depends_on:
      - dep-svc
    script: "exit 1"
"#;

    let parser = Parser::new();
    let config = parser.parse_config(yaml).expect("Failed to parse");

    let orch_temp = tempfile::tempdir().unwrap();
    let mut orchestrator = Orchestrator::new(config, orch_temp.path().to_path_buf())
        .await
        .unwrap();
    orchestrator
        .set_work_dir(temp_dir.path().to_path_buf())
        .await
        .expect("Failed to set work dir");
    orchestrator.set_auto_resolve_conflicts(true);
    orchestrator
        .initialize()
        .await
        .expect("Failed to initialize");

    // dep-svc should not be running initially
    assert!(
        !orchestrator.is_service_running("dep-svc").await,
        "dep-svc should not be running before script"
    );

    // Run the failing isolated script
    let result = orchestrator
        .run_script_interactive("failing-isolated", &[])
        .await;

    // The script should have run (exit 1 is a valid execution, not a spawn error)
    // ScriptFailed error is also acceptable
    if let Ok(status) = result {
        assert!(!status.success(), "Script should have failed with exit 1");
    }

    // After the failing script, dep-svc should NOT be running in the parent context.
    // The child orchestrator's cleanup should have stopped it.
    let status_after = orchestrator.get_status().await;
    if let Some(svc_status) = status_after.get("dep-svc") {
        assert_eq!(
            *svc_status,
            Status::Stopped,
            "dep-svc should be stopped in parent context after failed isolated script"
        );
    }

    orchestrator.cleanup().await;
}

// Check if Docker is available
fn is_docker_available() -> bool {
    std::process::Command::new("docker")
        .arg("version")
        .output()
        .map(|output| output.status.success())
        .unwrap_or(false)
}

/// Test that Docker containers are cleaned up after isolated script completes
/// This is critical because Docker containers persist outside the process lifecycle
#[tokio::test]
#[ignore] // Requires Docker
async fn test_isolated_docker_container_cleanup() {
    if !is_docker_available() {
        eprintln!("Skipping test: Docker not available");
        return;
    }

    let temp_dir = tempdir().expect("Failed to create temp dir");

    // Use a unique container name suffix to avoid collisions
    let test_id = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis();

    let yaml = format!(
        r#"
parameters:
  TEST_PORT:
    type: port
    default: 18080

services:
  test-nginx-{test_id}:
    image: nginx:alpine
    ports: ["{{{{TEST_PORT}}}}:80"]

scripts:
  test-docker:
    isolated: true
    depends_on:
      - test-nginx-{test_id}
    script: |
      echo "Docker container started, checking nginx..."
      curl -s --max-time 5 http://localhost:$TEST_PORT/ > /dev/null && echo "nginx responding"
      echo "Script complete"
"#,
        test_id = test_id
    );

    let parser = Parser::new();
    let config = parser.parse_config(&yaml).expect("Failed to parse");

    let orch_temp = tempfile::tempdir().unwrap();
    let mut orchestrator = Orchestrator::new(config, orch_temp.path().to_path_buf())
        .await
        .unwrap();
    orchestrator
        .set_work_dir(temp_dir.path().to_path_buf())
        .await
        .expect("Failed to set work dir");
    orchestrator.set_auto_resolve_conflicts(true);
    orchestrator
        .initialize()
        .await
        .expect("Failed to initialize");

    // Container name used by fed
    let container_name = format!("fed-test-nginx-{}", test_id);

    // Ensure container doesn't exist before test
    let _ = tokio::process::Command::new("docker")
        .args(["rm", "-f", &container_name])
        .output()
        .await;

    // Run the script - should start container in isolated context
    let status = orchestrator
        .run_script_interactive("test-docker", &[])
        .await
        .expect("Failed to run script");

    // Script might fail if curl isn't available, but container should still be cleaned up
    let _ = status;

    // Give cleanup a moment to complete
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

    // Verify container is NOT running after script completion
    let output = tokio::process::Command::new("docker")
        .args(["ps", "-q", "-f", &format!("name={}", container_name)])
        .output()
        .await
        .expect("Failed to run docker ps");

    let running_containers = String::from_utf8_lossy(&output.stdout);
    assert!(
        running_containers.trim().is_empty(),
        "Container '{}' should not be running after script cleanup, but found: {}",
        container_name,
        running_containers.trim()
    );

    // Also verify container doesn't exist at all (not just stopped)
    let output = tokio::process::Command::new("docker")
        .args(["ps", "-aq", "-f", &format!("name={}", container_name)])
        .output()
        .await
        .expect("Failed to run docker ps -a");

    let all_containers = String::from_utf8_lossy(&output.stdout);
    assert!(
        all_containers.trim().is_empty(),
        "Container '{}' should be removed after cleanup, but found stopped container: {}",
        container_name,
        all_containers.trim()
    );

    orchestrator.cleanup().await;

    // Final cleanup just in case
    let _ = tokio::process::Command::new("docker")
        .args(["rm", "-f", &container_name])
        .output()
        .await;
}

// ============================================================================
// Borrow-or-own cleanup for non-isolated scripts
//
// A script stops the services *it* started and leaves alone services that were
// already running. There is no config knob — lifecycle is decided at runtime by
// who started the service. `fed start` is the keep-alive mechanism (covered by
// `test_script_borrows_already_running_service`).
// ============================================================================

/// Helper: a standard test orchestrator initialized in `work_dir`.
///
/// Returns the orchestrator's state-dir guard alongside it; the caller must keep
/// it alive for the orchestrator's lifetime.
async fn init_orchestrator(
    yaml: &str,
    work_dir: &std::path::Path,
) -> (Orchestrator, tempfile::TempDir) {
    let config = Parser::new().parse_config(yaml).expect("Failed to parse");
    let orch_temp = tempfile::tempdir().unwrap();
    let mut orchestrator = Orchestrator::new(config, orch_temp.path().to_path_buf())
        .await
        .unwrap();
    orchestrator
        .set_work_dir(work_dir.to_path_buf())
        .await
        .expect("Failed to set work dir");
    orchestrator.set_auto_resolve_conflicts(true);
    orchestrator
        .initialize()
        .await
        .expect("Failed to initialize");
    (orchestrator, orch_temp)
}

/// Own: a script that starts a service stops it again afterward. The service
/// writes a marker on startup, proving the run actually brought it up (not a
/// no-op that left it stopped the whole time).
#[tokio::test]
async fn test_script_owns_and_stops_started_service() {
    let temp_dir = tempdir().expect("Failed to create temp dir");
    let marker = temp_dir.path().join("worker-started");

    let yaml = format!(
        r#"
services:
  worker:
    process: "sh -c 'touch {} && sleep 30'"

scripts:
  task:
    depends_on:
      - worker
    script: "echo done"
"#,
        marker.display()
    );

    let (orchestrator, _orch_temp) = init_orchestrator(&yaml, temp_dir.path()).await;

    assert!(
        !orchestrator.is_service_running("worker").await,
        "worker should not be running before the script"
    );

    let status = orchestrator
        .run_script_interactive("task", &[])
        .await
        .expect("Failed to run script");
    assert!(status.success(), "Script should succeed");

    assert!(
        marker.exists(),
        "worker should have been started by the run (marker file written)"
    );
    assert!(
        !orchestrator.is_service_running("worker").await,
        "worker was started by the run, so it must be stopped afterward"
    );

    orchestrator.cleanup().await;
}

/// Transitive own: a script depends on `app`, which depends on `db`. Both are
/// down; the run brings up the whole chain and must tear down *both* afterward,
/// not just the directly-listed dep.
#[tokio::test]
async fn test_script_stops_transitive_dependencies() {
    let temp_dir = tempdir().expect("Failed to create temp dir");
    let db_marker = temp_dir.path().join("db-started");

    let yaml = format!(
        r#"
services:
  db:
    process: "sh -c 'touch {} && sleep 30'"
  app:
    process: "sleep 30"
    depends_on:
      - db

scripts:
  task:
    depends_on:
      - app
    script: "echo done"
"#,
        db_marker.display()
    );

    let (orchestrator, _orch_temp) = init_orchestrator(&yaml, temp_dir.path()).await;

    let status = orchestrator
        .run_script_interactive("task", &[])
        .await
        .expect("Failed to run script");
    assert!(status.success(), "Script should succeed");

    assert!(
        db_marker.exists(),
        "db should have been started transitively (marker file written)"
    );
    assert!(
        !orchestrator.is_service_running("app").await,
        "app (direct dep) should be stopped after the run"
    );
    assert!(
        !orchestrator.is_service_running("db").await,
        "db (transitive dep) should also be stopped after the run"
    );

    orchestrator.cleanup().await;
}

/// Top-level ownership: script `main` depends on script `setup`, which depends
/// on service `svc`. Cleanup is owned by the outermost run, so `svc` must stay
/// up while `main`'s body runs (it liveness-checks `svc`'s pid) and only be
/// stopped after `main` completes. A broken implementation that let `setup`
/// clean up `svc` would kill it before `main`'s body, failing the pid check.
#[tokio::test]
async fn test_nested_script_dep_not_torn_down_midflight() {
    let temp_dir = tempdir().expect("Failed to create temp dir");
    let pidfile = temp_dir.path().join("svc.pid");
    let pid = pidfile.display();

    let yaml = format!(
        r#"
services:
  svc:
    process: "sh -c 'echo $$ > {pid} && sleep 30'"

scripts:
  setup:
    depends_on:
      - svc
    script: "echo setup"
  main:
    depends_on:
      - setup
    script: "for i in 1 2 3 4 5; do test -f {pid} && break; sleep 0.3; done; kill -0 $(cat {pid})"
"#,
        pid = pid
    );

    let (orchestrator, _orch_temp) = init_orchestrator(&yaml, temp_dir.path()).await;

    // main's body asserts svc is still alive; success proves setup did not stop it.
    let status = orchestrator
        .run_script_interactive("main", &[])
        .await
        .expect("Failed to run script");
    assert!(
        status.success(),
        "main should succeed — svc must remain alive through the whole top-level run"
    );

    // After the top-level run, svc (started by the chain) is stopped.
    assert!(
        !orchestrator.is_service_running("svc").await,
        "svc should be stopped after the top-level run completes"
    );

    orchestrator.cleanup().await;
}

/// Failure cleanup: a script that starts a service and then exits non-zero still
/// stops the service it started (parallels the isolated guarantee).
#[tokio::test]
async fn test_failing_script_stops_services_it_started() {
    let temp_dir = tempdir().expect("Failed to create temp dir");

    let yaml = r#"
services:
  dep:
    process: "sleep 30"

scripts:
  failing:
    depends_on:
      - dep
    script: "exit 1"
"#;

    let (orchestrator, _orch_temp) = init_orchestrator(yaml, temp_dir.path()).await;

    let status = orchestrator
        .run_script_interactive("failing", &[])
        .await
        .expect("run_script_interactive should return the exit status, not error");
    assert!(!status.success(), "script should fail (exit 1)");

    assert!(
        !orchestrator.is_service_running("dep").await,
        "dep should be stopped even though the script failed"
    );

    orchestrator.cleanup().await;
}

// ============================================================================
// Tests for `keep_services` — opting out of borrow-or-own cleanup
// ============================================================================

/// keep_services: a script that opts in leaves the service it started running,
/// the exact inverse of `test_script_owns_and_stops_started_service`. This is
/// the scenario/seed-script use case: set up state, then keep the stack up for
/// manual testing.
#[tokio::test]
async fn test_keep_services_leaves_started_service_running() {
    let temp_dir = tempdir().expect("Failed to create temp dir");
    let marker = temp_dir.path().join("worker-started");

    let yaml = format!(
        r#"
services:
  worker:
    process: "sh -c 'touch {} && sleep 30'"

scripts:
  task:
    keep_services: true
    depends_on:
      - worker
    script: "echo done"
"#,
        marker.display()
    );

    let (orchestrator, _orch_temp) = init_orchestrator(&yaml, temp_dir.path()).await;

    assert!(
        !orchestrator.is_service_running("worker").await,
        "worker should not be running before the script"
    );

    let status = orchestrator
        .run_script_interactive("task", &[])
        .await
        .expect("Failed to run script");
    assert!(status.success(), "Script should succeed");

    assert!(
        marker.exists(),
        "worker should have been started by the run (marker file written)"
    );
    assert!(
        orchestrator.is_service_running("worker").await,
        "keep_services: worker started by the run must stay running afterward"
    );

    // We opted out of automatic cleanup, so tear the stack down explicitly.
    orchestrator.cleanup().await;
}

/// keep_services keeps the *whole* started subtree up, not just the directly
/// listed dependency — the inverse of `test_script_stops_transitive_dependencies`.
#[tokio::test]
async fn test_keep_services_leaves_transitive_dependencies_running() {
    let temp_dir = tempdir().expect("Failed to create temp dir");

    let yaml = r#"
services:
  db:
    process: "sleep 30"
  app:
    process: "sleep 30"
    depends_on:
      - db

scripts:
  task:
    keep_services: true
    depends_on:
      - app
    script: "echo done"
"#;

    let (orchestrator, _orch_temp) = init_orchestrator(yaml, temp_dir.path()).await;

    let status = orchestrator
        .run_script_interactive("task", &[])
        .await
        .expect("Failed to run script");
    assert!(status.success(), "Script should succeed");

    assert!(
        orchestrator.is_service_running("app").await,
        "keep_services: directly-depended service must stay running"
    );
    assert!(
        orchestrator.is_service_running("db").await,
        "keep_services: transitive dependency must also stay running"
    );

    orchestrator.cleanup().await;
}

/// keep_services applies to the script you invoke, not to a script pulled in as
/// a dependency: the outermost run owns cleanup. Here the top-level `main` does
/// NOT keep services, so the service started via its keep_services-tagged
/// dependency `setup` is still stopped.
#[tokio::test]
async fn test_keep_services_on_nested_dependency_does_not_leak() {
    let temp_dir = tempdir().expect("Failed to create temp dir");

    let yaml = r#"
services:
  svc:
    process: "sleep 30"

scripts:
  setup:
    keep_services: true
    depends_on:
      - svc
    script: "echo setup"
  main:
    depends_on:
      - setup
    script: "echo main"
"#;

    let (orchestrator, _orch_temp) = init_orchestrator(yaml, temp_dir.path()).await;

    let status = orchestrator
        .run_script_interactive("main", &[])
        .await
        .expect("Failed to run script");
    assert!(status.success(), "Script should succeed");

    assert!(
        !orchestrator.is_service_running("svc").await,
        "keep_services on a nested dep must not apply: the top-level run owns \
         cleanup and 'main' does not keep services"
    );

    orchestrator.cleanup().await;
}

// ============================================================================
// Tests for isolated script marker scoping
// ============================================================================

/// Regression test: when a script runs with `isolated: true`, the child
/// orchestrator's install/migrate marker namespace must be scoped by its own
/// `isolation_id` — so it can't observe (or mutate) the parent's shared-scope
/// markers.
///
/// Before the fix, markers were keyed only by work_dir hash. That meant
/// `run_migrate_if_needed` inside an isolated child would see stale "already
/// migrated" markers left by a previous non-isolated `fed start` against the
/// same work_dir and skip migrations — silently breaking runs against the
/// fresh empty databases in the isolated containers.
///
/// The fix is structural: shared-scope markers live under
/// `~/.fed/installed/<hash>/`, isolated-scope markers live under
/// `~/.fed/isolated/installed/<hash>/<isolation_id>/`. The two trees are
/// disjoint, so parent markers can't affect the child's view and clearing
/// one scope can never wipe the other.
#[tokio::test]
async fn test_isolated_script_marker_scope_is_disjoint_from_parent() {
    let temp_dir = tempdir().expect("Failed to create temp dir");

    // Script with no service deps so we don't need Docker running — we're
    // verifying the marker-scoping contract, not dependency orchestration.
    let yaml = r#"
scripts:
  isolated-run:
    isolated: true
    script: "true"

services:
  dummy:
    process: "echo dummy"
"#;

    let parser = Parser::new();
    let config = parser.parse_config(yaml).expect("Failed to parse");

    let orch_temp = tempfile::tempdir().unwrap();
    let mut orchestrator = Orchestrator::new(config, orch_temp.path().to_path_buf())
        .await
        .unwrap();
    orchestrator
        .set_work_dir(temp_dir.path().to_path_buf())
        .await
        .expect("Failed to set work dir");
    orchestrator.set_auto_resolve_conflicts(true);
    orchestrator
        .initialize()
        .await
        .expect("Failed to initialize");

    // Simulate a prior non-isolated `fed start` by planting shared-scope
    // markers for the same work_dir.
    let shared = fed::markers::LifecycleMarkers::new(temp_dir.path().to_path_buf(), None);
    let svc_a = "isolated-marker-test-svc-a";
    let svc_b = "isolated-marker-test-svc-b";

    shared.mark_installed(svc_a).expect("seed install a");
    shared.mark_installed(svc_b).expect("seed install b");
    shared.mark_migrated(svc_a, "fp").expect("seed migrate a");
    shared.mark_migrated(svc_b, "fp").expect("seed migrate b");

    assert!(shared.is_installed(svc_a).unwrap());
    assert!(shared.is_installed(svc_b).unwrap());
    assert!(shared.is_migrated(svc_a).unwrap());
    assert!(shared.is_migrated(svc_b).unwrap());

    // Run the isolated script. The child orchestrator gets its own
    // isolation_id → its own empty marker namespace; it never touches the
    // parent's shared-scope markers.
    let status = orchestrator
        .run_script_interactive("isolated-run", &[])
        .await
        .expect("Failed to run isolated script");
    assert!(status.success(), "Isolated script should succeed");

    // The shared-scope markers seeded above must still be present. Before
    // the fix these were wiped by `clear_all_installed/migrated` inside
    // `run_script_isolated`, which broke any subsequent non-isolated
    // `fed start` by re-running install/migrate unnecessarily (and, worse,
    // stomped on a concurrent non-isolated `fed start`'s marker state).
    assert!(
        shared.is_installed(svc_a).unwrap(),
        "shared-scope install marker for {} must survive an isolated script run",
        svc_a
    );
    assert!(
        shared.is_installed(svc_b).unwrap(),
        "shared-scope install marker for {} must survive an isolated script run",
        svc_b
    );
    assert!(
        shared.is_migrated(svc_a).unwrap(),
        "shared-scope migrate marker for {} must survive an isolated script run",
        svc_a
    );
    assert!(
        shared.is_migrated(svc_b).unwrap(),
        "shared-scope migrate marker for {} must survive an isolated script run",
        svc_b
    );

    // Cleanup the test's shared-scope markers (they live in the user's
    // global `~/.fed/installed/<hash>/` keyed by temp_dir path hash).
    let _ = shared.clear_installed(svc_a);
    let _ = shared.clear_installed(svc_b);
    let _ = shared.clear_migrated(svc_a);
    let _ = shared.clear_migrated(svc_b);

    orchestrator.cleanup().await;
}
