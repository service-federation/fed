//! RB-1: end-to-end proof that a scoped script run does not hard-fail on an
//! unrelated service that references a missing manual secret, while an unscoped
//! `fed start` on the *same* config still fails.
//!
//! Reviewer's repro: a secret-free target script plus an unrelated service that
//! references `{{UNRELATED_SECRET}}` (a declared manual secret with no value).
//! Before the fix, initialization resolved every service in the project and the
//! scoped run exited 1 during init on a secret the script never touches.

use std::fs;
use std::process::Command;
use tempfile::TempDir;

fn fed_binary() -> String {
    env!("CARGO_BIN_EXE_fed").to_string()
}

const CONFIG: &str = r#"
parameters:
  UNRELATED_SECRET:
    type: secret
    source: manual
    description: "used only by the unrelated service"

services:
  unrelated:
    process: "echo serving {{UNRELATED_SECRET}}"

scripts:
  noop:
    script: "echo scoped-run-ok"
"#;

fn write_config(dir: &TempDir) -> String {
    let path = dir.path().join("service-federation.yaml");
    fs::write(&path, CONFIG).expect("write config");
    path.to_str().unwrap().to_string()
}

#[test]
fn scoped_script_run_ignores_unrelated_broken_service() {
    let dir = TempDir::new().unwrap();
    let config = write_config(&dir);

    // `fed run noop` is scoped to what `noop` references (nothing), so the
    // unrelated service's missing secret must not fail the run.
    let output = Command::new(fed_binary())
        .args(["-c", &config, "run", "noop"])
        .current_dir(dir.path())
        // Ensure no ambient cloud credentials turn this into a vault call.
        .env_remove("FED_TOKEN")
        .env_remove("FED_CLOUD_URL")
        .output()
        .expect("failed to run fed");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "scoped run must pass despite the unrelated broken service.\nstdout: {stdout}\nstderr: {stderr}"
    );
    assert!(
        stdout.contains("scoped-run-ok"),
        "the script should have executed.\nstdout: {stdout}\nstderr: {stderr}"
    );
}

#[test]
fn unscoped_start_still_fails_on_missing_secret() {
    let dir = TempDir::new().unwrap();
    let config = write_config(&dir);

    // `fed start` is unscoped: the missing required manual secret must still
    // fail. --dry-run resolves parameters/secrets without launching services.
    let output = Command::new(fed_binary())
        .args(["-c", &config, "start", "--dry-run"])
        .current_dir(dir.path())
        .env_remove("FED_TOKEN")
        .env_remove("FED_CLOUD_URL")
        .output()
        .expect("failed to run fed");

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        !output.status.success(),
        "unscoped start must still fail on the missing required secret"
    );
    assert!(
        stderr.contains("UNRELATED_SECRET") || stderr.contains("Missing secret"),
        "the failure should name the missing secret.\nstderr: {stderr}"
    );
}
