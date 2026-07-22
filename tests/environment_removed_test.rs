//! Regression tests for the removal of the `-e`/`--env` environment axis in
//! fed 8.0 (see `08-environments-removal.md`).
//!
//! The flag (and `fed secrets ls --env`) are kept registered — hidden from
//! `--help`, but still parsing — so a stale invocation gets an explicit
//! migration error instead of clap's generic "unexpected argument" failure.
//! These tests drive the real binary to prove the rejection fires with the
//! right message and exit code, and that the common no-flag path is
//! unaffected.

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

scripts:
  echo-greeting:
    environment:
      GREETING: '{{GREETING}}'
    script: echo "resolved=$GREETING"
"#;
    fs::write(&config_path, config).expect("Failed to write config");
    config_path.to_str().unwrap().to_string()
}

fn stdout_and_stderr(output: &std::process::Output) -> String {
    format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    )
}

#[test]
fn env_flag_still_parses_but_errors() {
    let temp_dir = TempDir::new().unwrap();
    let config = write_config(&temp_dir);

    let output = Command::new(fed_binary())
        .args(["-c", &config, "-e", "staging", "validate"])
        .output()
        .expect("Failed to run fed");

    assert!(
        !output.status.success(),
        "-e must be rejected after fed 8.0"
    );
    let combined = stdout_and_stderr(&output);
    assert!(
        combined.contains("removed in fed 8.0"),
        "unexpected output: {combined}"
    );
    assert!(
        combined.contains("env_file"),
        "unexpected output: {combined}"
    );
}

#[test]
fn secrets_ls_env_flag_still_parses_but_errors() {
    let output = Command::new(fed_binary())
        .args(["secrets", "ls", "--env", "staging"])
        .output()
        .expect("Failed to run fed");

    assert!(
        !output.status.success(),
        "secrets ls --env must be rejected after fed 8.0"
    );
    let combined = stdout_and_stderr(&output);
    assert!(
        combined.contains("removed in fed 8.0"),
        "unexpected output: {combined}"
    );
    assert!(
        combined.contains("env_file"),
        "unexpected output: {combined}"
    );
}

#[test]
fn env_flag_absent_is_unaffected() {
    // Proves the hidden-flag rejection mechanism doesn't regress the common
    // no-flag path.
    let temp_dir = TempDir::new().unwrap();
    let config = write_config(&temp_dir);

    let output = Command::new(fed_binary())
        .args(["-c", &config, "validate"])
        .output()
        .expect("Failed to run fed");

    assert!(
        output.status.success(),
        "fed validate (no -e) must still succeed: {}",
        stdout_and_stderr(&output)
    );
}

#[test]
fn bare_fed_ports_defaults_to_list() {
    // `fed ports` with no subcommand must behave like `fed ports list`.
    // (Unrelated to the environment axis — kept here after
    // `environment_flag_test.rs` was retired, per
    // `08-environments-removal.md`.)
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
