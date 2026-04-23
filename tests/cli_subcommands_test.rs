//! Comprehensive CLI subcommand tests
//!
//! This module provides smoke tests for all CLI subcommands to ensure
//! they parse arguments correctly and produce expected output.

use std::fs;
use std::process::Command;
use tempfile::TempDir;

fn fed_binary() -> String {
    env!("CARGO_BIN_EXE_fed").to_string()
}

fn create_test_config(temp_dir: &TempDir) -> String {
    let config_path = temp_dir.path().join("service-federation.yaml");
    let config = r#"
variables:
  TEST_PORT:
    default: "19876"

services:
  test-service:
    process: |
      echo "Test service running"
      sleep 1

  docker-service:
    image: alpine:latest
    command: ["echo", "hello"]

scripts:
  test-script:
    script: echo "Script executed"

install:
  - echo "Installing..."

clean:
  - echo "Cleaning..."
"#;
    fs::write(&config_path, config).expect("Failed to write config");
    config_path.to_str().unwrap().to_string()
}

// ============================================================================
// Help and version tests
// ============================================================================

#[test]
fn test_help_flag() {
    let output = Command::new(fed_binary())
        .arg("--help")
        .output()
        .expect("Failed to run fed");

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("Service Federation"));
    assert!(stdout.contains("Commands:"));
    assert!(stdout.contains("start"));
    assert!(stdout.contains("stop"));
}

#[test]
fn test_help_subcommand() {
    let output = Command::new(fed_binary())
        .arg("help")
        .output()
        .expect("Failed to run fed");

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("Commands:"));
}

// ============================================================================
// start subcommand
// ============================================================================

#[test]
fn test_start_help() {
    let output = Command::new(fed_binary())
        .args(["start", "--help"])
        .output()
        .expect("Failed to run fed");

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("Start services"));
}

#[test]
fn test_start_dry_run() {
    let temp_dir = TempDir::new().unwrap();
    let config_path = create_test_config(&temp_dir);

    let output = Command::new(fed_binary())
        .args([
            "-c",
            &config_path,
            "-w",
            temp_dir.path().to_str().unwrap(),
            "start",
            "--dry-run",
        ])
        .output()
        .expect("Failed to run fed");

    assert!(
        output.status.success(),
        "start --dry-run failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn test_start_missing_config() {
    let output = Command::new(fed_binary())
        .args(["-c", "/nonexistent/config.yaml", "start"])
        .output()
        .expect("Failed to run fed");

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("not found") || stderr.contains("No such file") || stderr.contains("error"),
        "Expected file not found error, got: {}",
        stderr
    );
}

// ============================================================================
// stop subcommand
// ============================================================================

#[test]
fn test_stop_help() {
    let output = Command::new(fed_binary())
        .args(["stop", "--help"])
        .output()
        .expect("Failed to run fed");

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("Stop services"));
}

#[test]
fn test_stop_no_services() {
    let temp_dir = TempDir::new().unwrap();
    let config_path = create_test_config(&temp_dir);

    let output = Command::new(fed_binary())
        .args([
            "-c",
            &config_path,
            "-w",
            temp_dir.path().to_str().unwrap(),
            "stop",
        ])
        .output()
        .expect("Failed to run fed");

    // Should succeed even with no running services
    assert!(
        output.status.success(),
        "stop failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

// ============================================================================
// restart subcommand
// ============================================================================

#[test]
fn test_restart_help() {
    let output = Command::new(fed_binary())
        .args(["restart", "--help"])
        .output()
        .expect("Failed to run fed");

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("Restart services"));
}

// ============================================================================
// status subcommand
// ============================================================================

#[test]
fn test_status_help() {
    let output = Command::new(fed_binary())
        .args(["status", "--help"])
        .output()
        .expect("Failed to run fed");

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("status") || stdout.contains("Show"));
}

#[test]
fn test_status_no_services() {
    let temp_dir = TempDir::new().unwrap();
    let config_path = create_test_config(&temp_dir);

    let output = Command::new(fed_binary())
        .args([
            "-c",
            &config_path,
            "-w",
            temp_dir.path().to_str().unwrap(),
            "status",
        ])
        .output()
        .expect("Failed to run fed");

    assert!(
        output.status.success(),
        "status failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn test_status_json_output() {
    let temp_dir = TempDir::new().unwrap();
    let config_path = create_test_config(&temp_dir);

    let output = Command::new(fed_binary())
        .args([
            "-c",
            &config_path,
            "-w",
            temp_dir.path().to_str().unwrap(),
            "status",
            "--json",
        ])
        .output()
        .expect("Failed to run fed");

    assert!(
        output.status.success(),
        "status --json failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8_lossy(&output.stdout);
    // Should be valid JSON (either empty array or object)
    assert!(
        stdout.trim().starts_with('[')
            || stdout.trim().starts_with('{')
            || stdout.trim().is_empty(),
        "Expected JSON output, got: {}",
        stdout
    );
}

// ============================================================================
// logs subcommand
// ============================================================================

#[test]
fn test_logs_help() {
    let output = Command::new(fed_binary())
        .args(["logs", "--help"])
        .output()
        .expect("Failed to run fed");

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("logs") || stdout.contains("Show"));
}

// ============================================================================
// run subcommand (scripts)
// ============================================================================

#[test]
fn test_run_help() {
    let output = Command::new(fed_binary())
        .args(["run", "--help"])
        .output()
        .expect("Failed to run fed");

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("Run") || stdout.contains("script"));
}

#[test]
fn test_run_missing_script() {
    let temp_dir = TempDir::new().unwrap();
    let config_path = create_test_config(&temp_dir);

    let output = Command::new(fed_binary())
        .args([
            "-c",
            &config_path,
            "-w",
            temp_dir.path().to_str().unwrap(),
            "run",
            "nonexistent-script",
        ])
        .output()
        .expect("Failed to run fed");

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("not found") || stderr.contains("Script") || stderr.contains("error"),
        "Expected script not found error, got: {}",
        stderr
    );
}

#[test]
fn test_run_with_args() {
    let temp_dir = TempDir::new().unwrap();

    // Create a config with a script that echoes its arguments via $@
    let config_content = r#"
scripts:
  echo-args:
    script: 'echo "ARGS:" "$@"'

services:
  dummy:
    process: "true"
"#;
    let config_path = temp_dir.path().join("service-federation.yaml");
    std::fs::write(&config_path, config_content).unwrap();

    let output = Command::new(fed_binary())
        .args([
            "-c",
            config_path.to_str().unwrap(),
            "-w",
            temp_dir.path().to_str().unwrap(),
            "run",
            "echo-args",
            "--",
            "hello",
            "world",
        ])
        .output()
        .expect("Failed to run fed");

    assert!(
        output.status.success(),
        "fed run with args should succeed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("hello") && stdout.contains("world"),
        "Output should contain passed arguments, got: {}",
        stdout
    );
}

#[test]
fn test_run_with_args_no_separator() {
    // Test that args work even without -- separator
    let temp_dir = TempDir::new().unwrap();

    let config_content = r#"
scripts:
  echo-args:
    script: 'echo "ARGS:" "$@"'

services:
  dummy:
    process: "true"
"#;
    let config_path = temp_dir.path().join("service-federation.yaml");
    std::fs::write(&config_path, config_content).unwrap();

    let output = Command::new(fed_binary())
        .args([
            "-c",
            config_path.to_str().unwrap(),
            "-w",
            temp_dir.path().to_str().unwrap(),
            "run",
            "echo-args",
            "arg1",
            "arg2",
        ])
        .output()
        .expect("Failed to run fed");

    assert!(
        output.status.success(),
        "fed run with args (no separator) should succeed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("arg1") && stdout.contains("arg2"),
        "Output should contain passed arguments, got: {}",
        stdout
    );
}

#[test]
fn test_run_auto_append_args() {
    // Test that args are auto-appended when script doesn't use $@
    let temp_dir = TempDir::new().unwrap();

    let config_content = r#"
scripts:
  echo-cmd:
    script: 'echo'

services:
  dummy:
    process: "true"
"#;
    let config_path = temp_dir.path().join("service-federation.yaml");
    std::fs::write(&config_path, config_content).unwrap();

    let output = Command::new(fed_binary())
        .args([
            "-c",
            config_path.to_str().unwrap(),
            "-w",
            temp_dir.path().to_str().unwrap(),
            "run",
            "echo-cmd",
            "--",
            "hello",
            "world",
        ])
        .output()
        .expect("Failed to run fed");

    assert!(
        output.status.success(),
        "fed run with auto-append should succeed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("hello") && stdout.contains("world"),
        "Output should contain auto-appended arguments, got: {}",
        stdout
    );
}

#[test]
fn test_run_auto_append_with_special_chars() {
    // Test that auto-appended args are properly escaped
    let temp_dir = TempDir::new().unwrap();

    let config_content = r#"
scripts:
  echo-cmd:
    script: 'echo'

services:
  dummy:
    process: "true"
"#;
    let config_path = temp_dir.path().join("service-federation.yaml");
    std::fs::write(&config_path, config_content).unwrap();

    let output = Command::new(fed_binary())
        .args([
            "-c",
            config_path.to_str().unwrap(),
            "-w",
            temp_dir.path().to_str().unwrap(),
            "run",
            "echo-cmd",
            "--",
            "hello world", // space
            "it's",        // quote
        ])
        .output()
        .expect("Failed to run fed");

    assert!(
        output.status.success(),
        "fed run with special chars should succeed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("hello world") && stdout.contains("it's"),
        "Output should contain properly escaped arguments, got: {}",
        stdout
    );
}

#[test]
fn test_run_positional_params_still_work() {
    // Test that $@ still works when explicitly used
    let temp_dir = TempDir::new().unwrap();

    let config_content = r#"
scripts:
  echo-wrapped:
    script: 'echo "WRAPPED:" "$@"'

services:
  dummy:
    process: "true"
"#;
    let config_path = temp_dir.path().join("service-federation.yaml");
    std::fs::write(&config_path, config_content).unwrap();

    let output = Command::new(fed_binary())
        .args([
            "-c",
            config_path.to_str().unwrap(),
            "-w",
            temp_dir.path().to_str().unwrap(),
            "run",
            "echo-wrapped",
            "--",
            "arg1",
            "arg2",
        ])
        .output()
        .expect("Failed to run fed");

    assert!(
        output.status.success(),
        "fed run with $@ should succeed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("WRAPPED:") && stdout.contains("arg1") && stdout.contains("arg2"),
        "Output should contain wrapped arguments, got: {}",
        stdout
    );
}

#[test]
fn test_run_dollar_star_detected() {
    // Test that $* is also detected as positional params
    let temp_dir = TempDir::new().unwrap();

    let config_content = r#"
scripts:
  echo-star:
    script: 'echo "STAR:" $*'

services:
  dummy:
    process: "true"
"#;
    let config_path = temp_dir.path().join("service-federation.yaml");
    std::fs::write(&config_path, config_content).unwrap();

    let output = Command::new(fed_binary())
        .args([
            "-c",
            config_path.to_str().unwrap(),
            "-w",
            temp_dir.path().to_str().unwrap(),
            "run",
            "echo-star",
            "--",
            "a",
            "b",
        ])
        .output()
        .expect("Failed to run fed");

    assert!(
        output.status.success(),
        "fed run with $* should succeed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("STAR:") && stdout.contains("a") && stdout.contains("b"),
        "Output should contain star arguments, got: {}",
        stdout
    );
}

#[test]
fn test_run_dollar_one_detected() {
    // Test that $1 is also detected as positional params
    let temp_dir = TempDir::new().unwrap();

    let config_content = r#"
scripts:
  echo-first:
    script: 'echo "FIRST:" $1'

services:
  dummy:
    process: "true"
"#;
    let config_path = temp_dir.path().join("service-federation.yaml");
    std::fs::write(&config_path, config_content).unwrap();

    let output = Command::new(fed_binary())
        .args([
            "-c",
            config_path.to_str().unwrap(),
            "-w",
            temp_dir.path().to_str().unwrap(),
            "run",
            "echo-first",
            "--",
            "first",
            "second",
        ])
        .output()
        .expect("Failed to run fed");

    assert!(
        output.status.success(),
        "fed run with $1 should succeed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("FIRST:") && stdout.contains("first"),
        "Output should contain first argument, got: {}",
        stdout
    );
}

// ============================================================================
// install subcommand
// ============================================================================

#[test]
fn test_install_help() {
    let output = Command::new(fed_binary())
        .args(["install", "--help"])
        .output()
        .expect("Failed to run fed");

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("install") || stdout.contains("Install"));
}

#[test]
fn test_install_runs() {
    let temp_dir = TempDir::new().unwrap();
    let config_path = create_test_config(&temp_dir);

    let output = Command::new(fed_binary())
        .args([
            "-c",
            &config_path,
            "-w",
            temp_dir.path().to_str().unwrap(),
            "install",
        ])
        .output()
        .expect("Failed to run fed");

    assert!(
        output.status.success(),
        "install failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("Installing") || stdout.contains("install"),
        "Expected install output, got: {}",
        stdout
    );
}

// ============================================================================
// clean subcommand
// ============================================================================

#[test]
fn test_clean_help() {
    let output = Command::new(fed_binary())
        .args(["clean", "--help"])
        .output()
        .expect("Failed to run fed");

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("clean") || stdout.contains("Clean"));
}

#[test]
fn test_clean_runs() {
    let temp_dir = TempDir::new().unwrap();
    let config_path = create_test_config(&temp_dir);

    let output = Command::new(fed_binary())
        .args([
            "-c",
            &config_path,
            "-w",
            temp_dir.path().to_str().unwrap(),
            "clean",
        ])
        .output()
        .expect("Failed to run fed");

    assert!(
        output.status.success(),
        "clean failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

// ============================================================================
// init subcommand
// ============================================================================

#[test]
fn test_init_help() {
    let output = Command::new(fed_binary())
        .args(["init", "--help"])
        .output()
        .expect("Failed to run fed");

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("init") || stdout.contains("Initialize"));
}

#[test]
fn test_init_creates_config() {
    let temp_dir = TempDir::new().unwrap();
    let config_path = temp_dir.path().join("service-federation.yaml");

    // Ensure no config exists
    assert!(!config_path.exists());

    // init uses --output/-o flag, not the global -c flag
    let output = Command::new(fed_binary())
        .args(["init", "-o", config_path.to_str().unwrap()])
        .output()
        .expect("Failed to run fed");

    assert!(
        output.status.success(),
        "init failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    // Config should now exist
    assert!(
        config_path.exists(),
        "init should create service-federation.yaml"
    );
}

#[test]
fn test_init_does_not_overwrite() {
    let temp_dir = TempDir::new().unwrap();
    let config_path = temp_dir.path().join("service-federation.yaml");

    // Create existing config
    fs::write(&config_path, "existing: config").unwrap();

    // init uses --output/-o flag, not the global -w flag
    let output = Command::new(fed_binary())
        .args(["init", "-o", config_path.to_str().unwrap()])
        .output()
        .expect("Failed to run fed");

    // Should fail or warn about existing config
    let stderr = String::from_utf8_lossy(&output.stderr);
    let stdout = String::from_utf8_lossy(&output.stdout);

    // Either fails or warns
    assert!(
        !output.status.success() || stderr.contains("exists") || stdout.contains("exists"),
        "init should not silently overwrite existing config"
    );

    // Original content should be preserved
    let content = fs::read_to_string(&config_path).unwrap();
    assert!(
        content.contains("existing"),
        "init should not overwrite existing config"
    );
}

// ============================================================================
// validate subcommand
// ============================================================================

#[test]
fn test_validate_help() {
    let output = Command::new(fed_binary())
        .args(["validate", "--help"])
        .output()
        .expect("Failed to run fed");

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("Validate") || stdout.contains("validate"));
}

#[test]
fn test_validate_valid_config() {
    let temp_dir = TempDir::new().unwrap();
    let config_path = create_test_config(&temp_dir);

    let output = Command::new(fed_binary())
        .args([
            "-c",
            &config_path,
            "-w",
            temp_dir.path().to_str().unwrap(),
            "validate",
        ])
        .output()
        .expect("Failed to run fed");

    assert!(
        output.status.success(),
        "validate failed on valid config: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn test_validate_invalid_config() {
    let temp_dir = TempDir::new().unwrap();
    let config_path = temp_dir.path().join("service-federation.yaml");

    // Write invalid YAML
    fs::write(&config_path, "invalid: yaml: content: [").unwrap();

    let output = Command::new(fed_binary())
        .args([
            "-c",
            config_path.to_str().unwrap(),
            "-w",
            temp_dir.path().to_str().unwrap(),
            "validate",
        ])
        .output()
        .expect("Failed to run fed");

    assert!(
        !output.status.success(),
        "validate should fail on invalid config"
    );
}

// ============================================================================
// completions subcommand
// ============================================================================

#[test]
fn test_completions_help() {
    let output = Command::new(fed_binary())
        .args(["completions", "--help"])
        .output()
        .expect("Failed to run fed");

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("completions") || stdout.contains("shell"));
}

#[test]
fn test_completions_bash() {
    let output = Command::new(fed_binary())
        .args(["completions", "bash"])
        .output()
        .expect("Failed to run fed");

    assert!(
        output.status.success(),
        "completions bash failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("complete") || stdout.contains("_fed"),
        "Expected bash completion script"
    );
}

#[test]
fn test_completions_zsh() {
    let output = Command::new(fed_binary())
        .args(["completions", "zsh"])
        .output()
        .expect("Failed to run fed");

    assert!(
        output.status.success(),
        "completions zsh failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("compdef") || stdout.contains("_fed") || stdout.contains("_arguments"),
        "Expected zsh completion script"
    );
}

#[test]
fn test_completions_fish() {
    let output = Command::new(fed_binary())
        .args(["completions", "fish"])
        .output()
        .expect("Failed to run fed");

    assert!(
        output.status.success(),
        "completions fish failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("complete") || stdout.contains("fed"),
        "Expected fish completion script"
    );
}

// ============================================================================
// doctor subcommand
// ============================================================================

#[test]
fn test_doctor_help() {
    let output = Command::new(fed_binary())
        .args(["doctor", "--help"])
        .output()
        .expect("Failed to run fed");

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("doctor") || stdout.contains("Check"));
}

#[test]
fn test_doctor_runs() {
    let output = Command::new(fed_binary())
        .args(["doctor"])
        .output()
        .expect("Failed to run fed");

    // Doctor should always succeed (it's diagnostic)
    assert!(
        output.status.success(),
        "doctor failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8_lossy(&output.stdout);
    // Should check for Docker at minimum
    assert!(
        stdout.contains("Docker")
            || stdout.contains("docker")
            || stdout.contains("✓")
            || stdout.contains("✗"),
        "Expected doctor to check system requirements, got: {}",
        stdout
    );
}

// ============================================================================
// top subcommand
// ============================================================================

#[test]
fn test_top_help() {
    let output = Command::new(fed_binary())
        .args(["top", "--help"])
        .output()
        .expect("Failed to run fed");

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("top") || stdout.contains("resource") || stdout.contains("usage"));
}

// Note: `top` is interactive and blocks, so we only test help
// The actual `top` functionality is tested in other integration tests

// ============================================================================
// debug subcommand
// ============================================================================

#[test]
fn test_debug_help() {
    let output = Command::new(fed_binary())
        .args(["debug", "--help"])
        .output()
        .expect("Failed to run fed");

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("debug") || stdout.contains("Debug"));
}

// ============================================================================
// tui subcommand
// ============================================================================

#[test]
fn test_tui_help() {
    let output = Command::new(fed_binary())
        .args(["tui", "--help"])
        .output()
        .expect("Failed to run fed");

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("tui") || stdout.contains("TUI") || stdout.contains("interactive"));
}

// Note: We can't easily test the interactive TUI without a PTY

// ============================================================================
// Global options tests
// ============================================================================

#[test]
fn test_config_flag() {
    let temp_dir = TempDir::new().unwrap();
    let config_path = create_test_config(&temp_dir);

    let output = Command::new(fed_binary())
        .args(["-c", &config_path, "validate"])
        .output()
        .expect("Failed to run fed");

    assert!(
        output.status.success(),
        "-c flag not working: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn test_workdir_flag() {
    let temp_dir = TempDir::new().unwrap();
    let config_path = create_test_config(&temp_dir);

    // -w sets working directory for execution, but still needs -c to find config
    let output = Command::new(fed_binary())
        .args([
            "-c",
            &config_path,
            "-w",
            temp_dir.path().to_str().unwrap(),
            "validate",
        ])
        .output()
        .expect("Failed to run fed");

    assert!(
        output.status.success(),
        "-w flag not working: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn test_env_flag() {
    let temp_dir = TempDir::new().unwrap();
    let config_path = create_test_config(&temp_dir);

    let output = Command::new(fed_binary())
        .args(["-c", &config_path, "-e", "production", "validate"])
        .output()
        .expect("Failed to run fed");

    assert!(
        output.status.success(),
        "-e flag not working: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn test_profile_flag() {
    let temp_dir = TempDir::new().unwrap();
    let config_path = create_test_config(&temp_dir);

    let output = Command::new(fed_binary())
        .args(["-c", &config_path, "-p", "dev", "validate"])
        .output()
        .expect("Failed to run fed");

    assert!(
        output.status.success(),
        "-p flag not working: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn test_multiple_profiles() {
    let temp_dir = TempDir::new().unwrap();
    let config_path = create_test_config(&temp_dir);

    let output = Command::new(fed_binary())
        .args(["-c", &config_path, "-p", "dev", "-p", "debug", "validate"])
        .output()
        .expect("Failed to run fed");

    assert!(
        output.status.success(),
        "multiple -p flags not working: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

// ============================================================================
// Error handling tests
// ============================================================================

#[test]
fn test_unknown_command() {
    let output = Command::new(fed_binary())
        .args(["nonexistent-command-xyz123"])
        .output()
        .expect("Failed to run fed");

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("error") || stderr.contains("Unknown") || stderr.contains("unrecognized"),
        "Expected error for unknown command, got: {}",
        stderr
    );
}

#[test]
fn test_invalid_flag() {
    let output = Command::new(fed_binary())
        .args(["--invalid-flag-12345"])
        .output()
        .expect("Failed to run fed");

    assert!(!output.status.success());
}

// ============================================================================
// `fed ports` command tests
// ============================================================================

fn create_port_test_config(temp_dir: &TempDir, api_port: u16, db_port: u16) -> String {
    let config_path = temp_dir.path().join("service-federation.yaml");
    let config = format!(
        r#"
parameters:
  TEST_API_PORT:
    type: port
    default: {api_port}
  TEST_DB_PORT:
    type: port
    default: {db_port}

services:
  api:
    process: "echo running on {{{{TEST_API_PORT}}}}"
    environment:
      PORT: "{{{{TEST_API_PORT}}}}"
  db:
    process: "echo running on {{{{TEST_DB_PORT}}}}"
    environment:
      PORT: "{{{{TEST_DB_PORT}}}}"

entrypoint: api
"#
    );
    fs::write(&config_path, config).expect("Failed to write config");
    config_path.to_str().unwrap().to_string()
}

fn parse_resolved_param(stdout: &str, param_name: &str) -> u16 {
    // Parses the resolved port for a parameter from dry-run's "Port availability:" section.
    // Handles all four shapes emitted by src/commands/start.rs::run_dry_run:
    //   [OK] Port N (NAME) is available
    //   [OK] Port N (NAME) randomly allocated
    //   [CONFLICT] Port N (NAME): ...
    //   [CONFLICT] Default port D (NAME) occupied by ... - resolved to N
    // In the last case the resolved value is after "resolved to"; in the others it's the
    // numeric token immediately before "(NAME)".
    let marker = format!("({})", param_name);
    for line in stdout.lines() {
        let Some(marker_idx) = line.find(&marker) else {
            continue;
        };
        if let Some(idx) = line.find("resolved to ") {
            let rest = &line[idx + "resolved to ".len()..];
            if let Some(word) = rest.split_whitespace().next() {
                if let Ok(port) = word.parse::<u16>() {
                    return port;
                }
            }
        }
        for word in line[..marker_idx].split_whitespace().rev() {
            if let Ok(port) = word.parse::<u16>() {
                return port;
            }
        }
    }
    panic!("Parameter {} not found in stdout:\n{}", param_name, stdout);
}

#[test]
fn test_ports_randomize_allocates_different_ports() {
    let temp_dir = TempDir::new().unwrap();
    let config_path = create_port_test_config(&temp_dir, 18080, 15432);

    // Occupy port 18080 so randomize is forced to allocate a different port
    let listener =
        std::net::TcpListener::bind(("127.0.0.1", 18080)).expect("Failed to bind port 18080");

    let output = Command::new(fed_binary())
        .args([
            "-c",
            &config_path,
            "-w",
            temp_dir.path().to_str().unwrap(),
            "ports",
            "randomize",
            "-f",
        ])
        .output()
        .expect("Failed to run fed");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "ports randomize failed.\nstdout: {}\nstderr: {}",
        stdout,
        stderr
    );

    // Verify via `fed ports list --json`
    let list_output = Command::new(fed_binary())
        .args([
            "-c",
            &config_path,
            "-w",
            temp_dir.path().to_str().unwrap(),
            "ports",
            "list",
            "--json",
        ])
        .output()
        .expect("Failed to run fed ports list");

    let list_stdout = String::from_utf8_lossy(&list_output.stdout);
    assert!(
        list_output.status.success(),
        "ports list --json failed.\nstdout: {}\nstderr: {}",
        list_stdout,
        String::from_utf8_lossy(&list_output.stderr)
    );

    let ports: std::collections::HashMap<String, u16> =
        serde_json::from_str(&list_stdout).expect("Failed to parse ports JSON");

    let api_port = ports
        .get("TEST_API_PORT")
        .expect("TEST_API_PORT not in ports");
    let db_port = ports
        .get("TEST_DB_PORT")
        .expect("TEST_DB_PORT not in ports");

    // Port 18080 is occupied, so randomize must allocate a different one
    assert_ne!(
        *api_port, 18080,
        "API port should differ from occupied default 18080"
    );
    assert_ne!(
        *db_port, 15432,
        "DB port should differ from default 15432 in randomize mode"
    );
    assert!(*api_port > 1024, "API port {} out of valid range", api_port);
    assert!(*db_port > 1024, "DB port {} out of valid range", db_port);
    assert_ne!(api_port, db_port, "Ports should differ from each other");

    drop(listener);
}

#[test]
fn test_ports_reset_clears_allocations() {
    let temp_dir = TempDir::new().unwrap();
    let config_path = create_port_test_config(&temp_dir, 18180, 15532);

    // First randomize
    let output = Command::new(fed_binary())
        .args([
            "-c",
            &config_path,
            "-w",
            temp_dir.path().to_str().unwrap(),
            "ports",
            "randomize",
            "-f",
        ])
        .output()
        .expect("Failed to run fed");
    assert!(output.status.success(), "ports randomize failed");

    // Then reset
    let output = Command::new(fed_binary())
        .args([
            "-c",
            &config_path,
            "-w",
            temp_dir.path().to_str().unwrap(),
            "ports",
            "reset",
            "-f",
        ])
        .output()
        .expect("Failed to run fed");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "ports reset failed.\nstdout: {}\nstderr: {}",
        stdout,
        stderr
    );

    // Verify ports are empty via list --json
    let list_output = Command::new(fed_binary())
        .args([
            "-c",
            &config_path,
            "-w",
            temp_dir.path().to_str().unwrap(),
            "ports",
            "list",
            "--json",
        ])
        .output()
        .expect("Failed to run fed ports list");

    let list_stdout = String::from_utf8_lossy(&list_output.stdout);
    let ports: std::collections::HashMap<String, u16> =
        serde_json::from_str(&list_stdout).expect("Failed to parse ports JSON");

    assert!(
        ports.is_empty(),
        "After reset, no ports should be allocated"
    );
}

#[test]
fn test_start_without_randomize_uses_defaults() {
    let temp_dir = TempDir::new().unwrap();
    let config_path = create_port_test_config(&temp_dir, 18280, 15632);

    let output = Command::new(fed_binary())
        .args([
            "-c",
            &config_path,
            "-w",
            temp_dir.path().to_str().unwrap(),
            "start",
            "--dry-run",
        ])
        .output()
        .expect("Failed to run fed");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "start --dry-run failed.\nstdout: {}\nstderr: {}",
        stdout,
        stderr
    );

    let api_port = parse_resolved_param(&stdout, "TEST_API_PORT");
    let db_port = parse_resolved_param(&stdout, "TEST_DB_PORT");

    assert_eq!(
        api_port, 18280,
        "Without port randomization, API port should be the default 18280"
    );
    assert_eq!(
        db_port, 15632,
        "Without port randomization, DB port should be the default 15632"
    );
}

#[test]
fn test_start_dry_run_does_not_persist_ports() {
    let temp_dir = TempDir::new().unwrap();
    let config_path = create_port_test_config(&temp_dir, 18281, 15633);

    let output = Command::new(fed_binary())
        .args([
            "-c",
            &config_path,
            "-w",
            temp_dir.path().to_str().unwrap(),
            "start",
            "--dry-run",
        ])
        .output()
        .expect("Failed to run fed");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "start --dry-run failed.\nstdout: {}\nstderr: {}",
        stdout,
        stderr
    );

    // Dry-run should not persist parameter->port mappings to SQLite state.
    let list_output = Command::new(fed_binary())
        .args([
            "-c",
            &config_path,
            "-w",
            temp_dir.path().to_str().unwrap(),
            "ports",
            "list",
            "--json",
        ])
        .output()
        .expect("Failed to run fed ports list");

    let list_stdout = String::from_utf8_lossy(&list_output.stdout);
    assert!(
        list_output.status.success(),
        "ports list --json failed.\nstdout: {}\nstderr: {}",
        list_stdout,
        String::from_utf8_lossy(&list_output.stderr)
    );

    let ports: std::collections::HashMap<String, u16> =
        serde_json::from_str(&list_stdout).expect("Failed to parse ports JSON");
    assert!(
        ports.is_empty(),
        "dry-run must not persist port allocations, found: {:?}",
        ports
    );
}

#[test]
fn test_start_randomize_flag_parses() {
    // Verify --randomize is a valid flag by running it with --dry-run on a config
    let temp_dir = TempDir::new().unwrap();
    let config_path = create_port_test_config(&temp_dir, 18480, 15832);

    let output = Command::new(fed_binary())
        .args([
            "-c",
            &config_path,
            "-w",
            temp_dir.path().to_str().unwrap(),
            "start",
            "--randomize",
            "--dry-run",
        ])
        .output()
        .expect("Failed to run fed");

    assert!(
        output.status.success(),
        "start --randomize --dry-run should parse and succeed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn test_start_randomize_dry_run_shows_non_default_ports() {
    let temp_dir = TempDir::new().unwrap();
    // Use very high defaults that are almost certainly free, so without
    // --randomize they'd be used as-is.
    let config_path = create_port_test_config(&temp_dir, 18380, 15732);

    let output = Command::new(fed_binary())
        .args([
            "-c",
            &config_path,
            "-w",
            temp_dir.path().to_str().unwrap(),
            "start",
            "--randomize",
            "--dry-run",
        ])
        .output()
        .expect("Failed to run fed");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "start --randomize --dry-run failed.\nstdout: {}\nstderr: {}",
        stdout,
        stderr
    );

    let api_port = parse_resolved_param(&stdout, "TEST_API_PORT");
    let db_port = parse_resolved_param(&stdout, "TEST_DB_PORT");

    // Randomize should allocate ports different from defaults
    assert_ne!(
        api_port, 18380,
        "With --randomize, API port should differ from default 18380"
    );
    assert_ne!(
        db_port, 15732,
        "With --randomize, DB port should differ from default 15732"
    );
    assert!(api_port > 1024, "API port {} out of valid range", api_port);
    assert!(db_port > 1024, "DB port {} out of valid range", db_port);
    assert_ne!(api_port, db_port, "Ports should differ from each other");
}

// ── Isolate command tests ───────────────────────────────────────────────

#[test]
fn test_isolate_enable_allocates_ports() {
    let temp_dir = TempDir::new().unwrap();
    let config_path = create_port_test_config(&temp_dir, 18580, 15932);

    // Occupy port 18580 so isolation is forced to allocate a different port
    let listener =
        std::net::TcpListener::bind(("127.0.0.1", 18580)).expect("Failed to bind port 18580");

    let output = Command::new(fed_binary())
        .args([
            "-c",
            &config_path,
            "-w",
            temp_dir.path().to_str().unwrap(),
            "isolate",
            "enable",
            "-f",
        ])
        .output()
        .expect("Failed to run fed");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "isolate enable failed.\nstdout: {}\nstderr: {}",
        stdout,
        stderr
    );

    // Verify via fed ports list --json
    let list_output = Command::new(fed_binary())
        .args([
            "-c",
            &config_path,
            "-w",
            temp_dir.path().to_str().unwrap(),
            "ports",
            "list",
            "--json",
        ])
        .output()
        .expect("Failed to run fed ports list");

    let list_stdout = String::from_utf8_lossy(&list_output.stdout);
    assert!(
        list_output.status.success(),
        "ports list --json failed.\nstdout: {}\nstderr: {}",
        list_stdout,
        String::from_utf8_lossy(&list_output.stderr)
    );

    let ports: std::collections::HashMap<String, u16> =
        serde_json::from_str(&list_stdout).expect("Failed to parse ports JSON");

    let api_port = ports
        .get("TEST_API_PORT")
        .expect("TEST_API_PORT not in ports");
    let db_port = ports
        .get("TEST_DB_PORT")
        .expect("TEST_DB_PORT not in ports");

    // Port 18580 is occupied, so isolation must allocate a different one
    assert_ne!(
        *api_port, 18580,
        "API port should differ from occupied default 18580"
    );
    assert!(*api_port > 1024, "API port {} out of valid range", api_port);
    assert!(*db_port > 1024, "DB port {} out of valid range", db_port);
    assert_ne!(api_port, db_port, "Ports should differ from each other");

    drop(listener);
}

#[test]
fn test_isolate_disable_clears_allocations() {
    let temp_dir = TempDir::new().unwrap();
    let config_path = create_port_test_config(&temp_dir, 18680, 16032);

    // First enable isolation
    let output = Command::new(fed_binary())
        .args([
            "-c",
            &config_path,
            "-w",
            temp_dir.path().to_str().unwrap(),
            "isolate",
            "enable",
            "-f",
        ])
        .output()
        .expect("Failed to run fed");
    assert!(output.status.success(), "isolate enable failed");

    // Then disable isolation
    let output = Command::new(fed_binary())
        .args([
            "-c",
            &config_path,
            "-w",
            temp_dir.path().to_str().unwrap(),
            "isolate",
            "disable",
            "-f",
        ])
        .output()
        .expect("Failed to run fed");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "isolate disable failed.\nstdout: {}\nstderr: {}",
        stdout,
        stderr
    );

    // Verify ports are empty via list --json
    let list_output = Command::new(fed_binary())
        .args([
            "-c",
            &config_path,
            "-w",
            temp_dir.path().to_str().unwrap(),
            "ports",
            "list",
            "--json",
        ])
        .output()
        .expect("Failed to run fed ports list");

    let list_stdout = String::from_utf8_lossy(&list_output.stdout);
    let ports: std::collections::HashMap<String, u16> =
        serde_json::from_str(&list_stdout).expect("Failed to parse ports JSON");

    assert!(
        ports.is_empty(),
        "After isolate disable, no ports should be allocated"
    );
}

#[test]
fn test_isolate_status_shows_isolation_info() {
    let temp_dir = TempDir::new().unwrap();
    let config_path = create_port_test_config(&temp_dir, 18780, 16132);

    // Enable isolation first
    let output = Command::new(fed_binary())
        .args([
            "-c",
            &config_path,
            "-w",
            temp_dir.path().to_str().unwrap(),
            "isolate",
            "enable",
            "-f",
        ])
        .output()
        .expect("Failed to run fed");
    assert!(
        output.status.success(),
        "isolate enable failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    // Check status
    let status_output = Command::new(fed_binary())
        .args([
            "-c",
            &config_path,
            "-w",
            temp_dir.path().to_str().unwrap(),
            "isolate",
            "status",
        ])
        .output()
        .expect("Failed to run fed isolate status");

    let stdout = String::from_utf8_lossy(&status_output.stdout);
    let stderr = String::from_utf8_lossy(&status_output.stderr);
    assert!(
        status_output.status.success(),
        "isolate status failed.\nstdout: {}\nstderr: {}",
        stdout,
        stderr
    );

    assert!(
        stdout.contains("enabled"),
        "Status output should contain 'enabled', got:\n{}",
        stdout
    );
    assert!(
        stdout.contains("iso-"),
        "Status output should contain an isolation ID (iso-...), got:\n{}",
        stdout
    );
}

#[test]
fn test_isolate_rotate_rerolls_ports() {
    let temp_dir = TempDir::new().unwrap();
    let config_path = create_port_test_config(&temp_dir, 18880, 16232);

    // Enable isolation first
    let enable_output = Command::new(fed_binary())
        .args([
            "-c",
            &config_path,
            "-w",
            temp_dir.path().to_str().unwrap(),
            "isolate",
            "enable",
            "-f",
        ])
        .output()
        .expect("Failed to run fed");
    assert!(
        enable_output.status.success(),
        "isolate enable failed: {}",
        String::from_utf8_lossy(&enable_output.stderr)
    );

    // Capture initial ports
    let list_output = Command::new(fed_binary())
        .args([
            "-c",
            &config_path,
            "-w",
            temp_dir.path().to_str().unwrap(),
            "ports",
            "list",
            "--json",
        ])
        .output()
        .expect("Failed to run fed ports list");
    let list_stdout = String::from_utf8_lossy(&list_output.stdout);
    let ports_before: std::collections::HashMap<String, u16> =
        serde_json::from_str(&list_stdout).expect("Failed to parse ports JSON");

    // Capture initial isolation ID from status output
    let status_before = Command::new(fed_binary())
        .args([
            "-c",
            &config_path,
            "-w",
            temp_dir.path().to_str().unwrap(),
            "isolate",
            "status",
        ])
        .output()
        .expect("Failed to run fed isolate status");
    let status_before_stdout = String::from_utf8_lossy(&status_before.stdout);

    // Rotate
    let rotate_output = Command::new(fed_binary())
        .args([
            "-c",
            &config_path,
            "-w",
            temp_dir.path().to_str().unwrap(),
            "isolate",
            "rotate",
            "-f",
        ])
        .output()
        .expect("Failed to run fed");

    let stdout = String::from_utf8_lossy(&rotate_output.stdout);
    let stderr = String::from_utf8_lossy(&rotate_output.stderr);
    assert!(
        rotate_output.status.success(),
        "isolate rotate failed.\nstdout: {}\nstderr: {}",
        stdout,
        stderr
    );

    // Capture new ports
    let list_output = Command::new(fed_binary())
        .args([
            "-c",
            &config_path,
            "-w",
            temp_dir.path().to_str().unwrap(),
            "ports",
            "list",
            "--json",
        ])
        .output()
        .expect("Failed to run fed ports list");
    let list_stdout = String::from_utf8_lossy(&list_output.stdout);
    let ports_after: std::collections::HashMap<String, u16> =
        serde_json::from_str(&list_stdout).expect("Failed to parse ports JSON");

    // Capture new isolation ID
    let status_after = Command::new(fed_binary())
        .args([
            "-c",
            &config_path,
            "-w",
            temp_dir.path().to_str().unwrap(),
            "isolate",
            "status",
        ])
        .output()
        .expect("Failed to run fed isolate status");
    let status_after_stdout = String::from_utf8_lossy(&status_after.stdout);

    // At least the isolation ID should change (ports may coincidentally match)
    assert_ne!(
        status_before_stdout.to_string(),
        status_after_stdout.to_string(),
        "Isolation ID should change after rotate"
    );

    // Verify ports are still allocated
    assert!(
        !ports_after.is_empty(),
        "Ports should still be allocated after rotate"
    );

    // Check at least one port or the ID changed
    let api_before = ports_before.get("TEST_API_PORT");
    let api_after = ports_after.get("TEST_API_PORT");
    let db_before = ports_before.get("TEST_DB_PORT");
    let db_after = ports_after.get("TEST_DB_PORT");

    let ports_changed = api_before != api_after || db_before != db_after;
    let id_changed = status_before_stdout != status_after_stdout;

    assert!(
        ports_changed || id_changed,
        "After rotate, at least ports or isolation ID should have changed.\n\
         Before: {:?}\nAfter: {:?}",
        ports_before,
        ports_after
    );
}

#[test]
fn test_start_isolate_flag_parses() {
    let temp_dir = TempDir::new().unwrap();
    let config_path = create_port_test_config(&temp_dir, 18980, 16332);

    let output = Command::new(fed_binary())
        .args([
            "-c",
            &config_path,
            "-w",
            temp_dir.path().to_str().unwrap(),
            "start",
            "--isolate",
            "--dry-run",
        ])
        .output()
        .expect("Failed to run fed");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "start --isolate --dry-run should parse and succeed.\nstdout: {}\nstderr: {}",
        stdout,
        stderr
    );

    let api_port = parse_resolved_param(&stdout, "TEST_API_PORT");
    let db_port = parse_resolved_param(&stdout, "TEST_DB_PORT");

    // --isolate should allocate ports different from defaults
    assert_ne!(
        api_port, 18980,
        "With --isolate, API port should differ from default 18980"
    );
    assert_ne!(
        db_port, 16332,
        "With --isolate, DB port should differ from default 16332"
    );
    assert!(api_port > 1024, "API port {} out of valid range", api_port);
    assert!(db_port > 1024, "DB port {} out of valid range", db_port);
    assert_ne!(api_port, db_port, "Ports should differ from each other");
}

#[test]
fn test_deprecated_randomize_still_works() {
    let temp_dir = TempDir::new().unwrap();
    let config_path = create_port_test_config(&temp_dir, 19080, 16432);

    // Occupy port 19080 so randomize is forced to allocate a different port
    let listener =
        std::net::TcpListener::bind(("127.0.0.1", 19080)).expect("Failed to bind port 19080");

    let output = Command::new(fed_binary())
        .args([
            "-c",
            &config_path,
            "-w",
            temp_dir.path().to_str().unwrap(),
            "ports",
            "randomize",
            "-f",
        ])
        .output()
        .expect("Failed to run fed");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "ports randomize failed.\nstdout: {}\nstderr: {}",
        stdout,
        stderr
    );

    // Verify deprecation warning on stderr
    assert!(
        stderr.contains("deprecated"),
        "stderr should contain deprecation warning, got:\n{}",
        stderr
    );

    // Verify ports were allocated
    let list_output = Command::new(fed_binary())
        .args([
            "-c",
            &config_path,
            "-w",
            temp_dir.path().to_str().unwrap(),
            "ports",
            "list",
            "--json",
        ])
        .output()
        .expect("Failed to run fed ports list");

    let list_stdout = String::from_utf8_lossy(&list_output.stdout);
    let ports: std::collections::HashMap<String, u16> =
        serde_json::from_str(&list_stdout).expect("Failed to parse ports JSON");

    let api_port = ports
        .get("TEST_API_PORT")
        .expect("TEST_API_PORT not in ports");
    assert_ne!(
        *api_port, 19080,
        "API port should differ from occupied default 19080"
    );
    assert!(*api_port > 1024, "API port {} out of valid range", api_port);

    drop(listener);
}

#[test]
fn test_deprecated_start_randomize_warns() {
    let temp_dir = TempDir::new().unwrap();
    let config_path = create_port_test_config(&temp_dir, 19180, 16532);

    let output = Command::new(fed_binary())
        .args([
            "-c",
            &config_path,
            "-w",
            temp_dir.path().to_str().unwrap(),
            "start",
            "--randomize",
            "--dry-run",
        ])
        .output()
        .expect("Failed to run fed");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "start --randomize --dry-run failed.\nstdout: {}\nstderr: {}",
        stdout,
        stderr
    );

    // Verify deprecation warning on stderr
    assert!(
        stderr.contains("deprecated"),
        "stderr should contain deprecation warning for --randomize, got:\n{}",
        stderr
    );
}
