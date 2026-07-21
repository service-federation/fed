//! Regression tests for the global `-e`/`--env` flag.
//!
//! The flag was accepted but never wired into parameter resolution before
//! v7.2.0, so every run silently resolved development values. These tests
//! drive the real binary to make sure the flag stays connected end to end.

use std::fs;
use std::process::Command;
use tempfile::TempDir;

fn fed_binary() -> String {
    env!("CARGO_BIN_EXE_fed").to_string()
}

fn write_config(temp_dir: &TempDir) -> String {
    let config_path = temp_dir.path().join("fed.yaml");
    let config = r#"
parameters:
  GREETING:
    default: from-default
    development: from-development
    staging: from-staging
    production: from-production

scripts:
  echo-greeting:
    environment:
      GREETING: '{{GREETING}}'
    script: echo "resolved=$GREETING"
"#;
    fs::write(&config_path, config).expect("Failed to write config");
    config_path.to_str().unwrap().to_string()
}

fn run_with_env(config: &str, env_flag: Option<&str>) -> std::process::Output {
    let mut cmd = Command::new(fed_binary());
    if let Some(env) = env_flag {
        cmd.arg("-e").arg(env);
    }
    cmd.arg("-c")
        .arg(config)
        .arg("run")
        .arg("echo-greeting")
        .output()
        .expect("Failed to run fed")
}

fn stdout_and_stderr(output: &std::process::Output) -> String {
    format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    )
}

#[test]
fn default_environment_is_development() {
    let temp_dir = TempDir::new().unwrap();
    let config = write_config(&temp_dir);
    let output = run_with_env(&config, None);
    assert!(output.status.success());
    assert!(stdout_and_stderr(&output).contains("resolved=from-development"));
}

#[test]
fn staging_flag_selects_staging_value() {
    let temp_dir = TempDir::new().unwrap();
    let config = write_config(&temp_dir);
    let output = run_with_env(&config, Some("staging"));
    assert!(output.status.success());
    assert!(stdout_and_stderr(&output).contains("resolved=from-staging"));
}

#[test]
fn production_flag_selects_production_value() {
    let temp_dir = TempDir::new().unwrap();
    let config = write_config(&temp_dir);
    let output = run_with_env(&config, Some("production"));
    assert!(output.status.success());
    assert!(stdout_and_stderr(&output).contains("resolved=from-production"));
}

#[test]
fn develop_is_an_alias_for_development() {
    let temp_dir = TempDir::new().unwrap();
    let config = write_config(&temp_dir);
    let output = run_with_env(&config, Some("develop"));
    assert!(output.status.success());
    assert!(stdout_and_stderr(&output).contains("resolved=from-development"));
}

#[test]
fn isolated_script_inherits_environment() {
    // The isolated:true path builds a child orchestrator; it must inherit the
    // parent's -e environment instead of resetting to development.
    let temp_dir = TempDir::new().unwrap();
    let config_path = temp_dir.path().join("fed.yaml");
    fs::write(
        &config_path,
        r#"
parameters:
  GREETING:
    default: from-default
    development: from-development
    staging: from-staging

scripts:
  echo-isolated:
    isolated: true
    environment:
      GREETING: '{{GREETING}}'
    script: echo "resolved=$GREETING"
"#,
    )
    .unwrap();
    let output = Command::new(fed_binary())
        .arg("-e")
        .arg("staging")
        .arg("-c")
        .arg(config_path.to_str().unwrap())
        .arg("run")
        .arg("echo-isolated")
        .output()
        .expect("Failed to run fed");
    assert!(output.status.success());
    assert!(stdout_and_stderr(&output).contains("resolved=from-staging"));
}

#[test]
fn bare_fed_ports_defaults_to_list() {
    // `fed ports` with no subcommand must behave like `fed ports list`.
    let temp_dir = TempDir::new().unwrap();
    let config = write_config(&temp_dir);
    let output = Command::new(fed_binary())
        .arg("-c")
        .arg(&config)
        .arg("-w")
        .arg(temp_dir.path())
        .arg("ports")
        .output()
        .expect("Failed to run fed");
    assert!(output.status.success());
    let combined = stdout_and_stderr(&output);
    assert!(
        combined.contains("No ports") || combined.contains("Port Allocations"),
        "unexpected output: {combined}"
    );
}

#[test]
fn invalid_environment_fails_with_hint() {
    let temp_dir = TempDir::new().unwrap();
    let config = write_config(&temp_dir);
    let output = run_with_env(&config, Some("bogus"));
    assert!(!output.status.success());
    let combined = stdout_and_stderr(&output);
    assert!(combined.contains("Invalid environment 'bogus'"));
    assert!(combined.contains("development, develop, staging, production"));
}
