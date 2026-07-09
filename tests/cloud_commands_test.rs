//! Integration tests for `fed login/logout/whoami/link/secrets` surfaces
//! that don't need a network.

use std::process::Command;
use tempfile::TempDir;

fn fed_binary() -> &'static str {
    env!("CARGO_BIN_EXE_fed")
}

/// whoami without credentials says so and exits 0 (informational, not an error).
#[test]
fn test_whoami_signed_out() {
    let tmp = TempDir::new().unwrap();
    let output = Command::new(fed_binary())
        .args(["whoami"])
        .env("HOME", tmp.path()) // no ~/.fed/credentials
        .env_remove("FED_TOKEN")
        .output()
        .unwrap();
    assert!(output.status.success());
    let combined = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        combined.contains("fed login"),
        "should hint at fed login: {combined}"
    );
}

/// link with an explicit target writes .fed/cloud.yaml without needing auth.
#[test]
fn test_link_writes_cloud_yaml() {
    let tmp = TempDir::new().unwrap();
    let output = Command::new(fed_binary())
        .args([
            "--workdir",
            tmp.path().to_str().unwrap(),
            "link",
            "acme/web",
        ])
        .env("HOME", tmp.path())
        .env_remove("FED_TOKEN")
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let written = std::fs::read_to_string(tmp.path().join(".fed/cloud.yaml")).unwrap();
    assert!(written.contains("org: acme"));
    assert!(written.contains("project: web"));
}

/// link rejects malformed targets.
#[test]
fn test_link_rejects_bad_target() {
    let tmp = TempDir::new().unwrap();
    let output = Command::new(fed_binary())
        .args([
            "--workdir",
            tmp.path().to_str().unwrap(),
            "link",
            "not-a-path",
        ])
        .env("HOME", tmp.path())
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("org/project"));
}

/// secrets ls without login fails with the login hint.
#[test]
fn test_secrets_ls_requires_login() {
    let tmp = TempDir::new().unwrap();
    let output = Command::new(fed_binary())
        .args(["--workdir", tmp.path().to_str().unwrap(), "secrets", "ls"])
        .env("HOME", tmp.path())
        .env_remove("FED_TOKEN")
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("fed login"));
}

/// logout when not signed in is a no-op, not an error.
#[test]
fn test_logout_signed_out() {
    let tmp = TempDir::new().unwrap();
    let output = Command::new(fed_binary())
        .args(["logout"])
        .env("HOME", tmp.path())
        .env_remove("FED_TOKEN")
        .output()
        .unwrap();
    assert!(output.status.success());
}
