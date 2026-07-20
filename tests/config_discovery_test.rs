//! Config filename discovery tests.
//!
//! fed prefers `fed.yaml` and falls back to the legacy `service-federation.yaml`.
//! Both names also accept a `.yml` extension. When a directory contains both a
//! `fed.*` config and a legacy config, fed uses the `fed.*` one and prints a
//! one-line warning naming both files.

use std::fs;
use std::process::Command;
use tempfile::TempDir;

fn fed_binary() -> String {
    env!("CARGO_BIN_EXE_fed").to_string()
}

const MINIMAL_CONFIG: &str = r#"
services:
  app:
    process: echo "hello"
"#;

fn run_validate(dir: &std::path::Path) -> std::process::Output {
    Command::new(fed_binary())
        .arg("validate")
        .current_dir(dir)
        .output()
        .expect("Failed to run fed")
}

#[test]
fn test_discovers_fed_yaml() {
    let temp_dir = TempDir::new().unwrap();
    fs::write(temp_dir.path().join("fed.yaml"), MINIMAL_CONFIG).unwrap();

    let output = run_validate(temp_dir.path());
    assert!(
        output.status.success(),
        "validate failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("fed.yaml"),
        "expected fed.yaml in output, got: {}",
        stdout
    );
}

#[test]
fn test_discovers_legacy_service_federation_yaml() {
    let temp_dir = TempDir::new().unwrap();
    fs::write(
        temp_dir.path().join("service-federation.yaml"),
        MINIMAL_CONFIG,
    )
    .unwrap();

    let output = run_validate(temp_dir.path());
    assert!(
        output.status.success(),
        "validate failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("service-federation.yaml"),
        "expected service-federation.yaml in output, got: {}",
        stdout
    );
    // No warning when only the legacy name exists
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        !stderr.contains("warning: both"),
        "unexpected warning: {}",
        stderr
    );
}

#[test]
fn test_discovers_fed_yml_extension() {
    let temp_dir = TempDir::new().unwrap();
    fs::write(temp_dir.path().join("fed.yml"), MINIMAL_CONFIG).unwrap();

    let output = run_validate(temp_dir.path());
    assert!(
        output.status.success(),
        "validate failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("fed.yml"),
        "expected fed.yml in output, got: {}",
        stdout
    );
}

#[test]
fn test_prefers_fed_yaml_and_warns_when_both_exist() {
    let temp_dir = TempDir::new().unwrap();
    // Distinguishable configs: the legacy one is invalid, so validation only
    // succeeds if fed.yaml was chosen.
    fs::write(temp_dir.path().join("fed.yaml"), MINIMAL_CONFIG).unwrap();
    fs::write(
        temp_dir.path().join("service-federation.yaml"),
        "services:\n  broken:\n    process: echo hi\n    depends_on: [missing]\n",
    )
    .unwrap();

    let output = run_validate(temp_dir.path());
    assert!(
        output.status.success(),
        "validate should use fed.yaml (valid), stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("fed.yaml") && !stdout.contains("service-federation.yaml"),
        "expected fed.yaml to be validated, got: {}",
        stdout
    );

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("fed.yaml") && stderr.contains("service-federation.yaml"),
        "warning should name both files, got: {}",
        stderr
    );
    assert!(
        stderr.lines().any(|l| l.contains("warning")),
        "expected a one-line warning, got: {}",
        stderr
    );
}

#[test]
fn test_discovery_walks_parent_directories() {
    let temp_dir = TempDir::new().unwrap();
    fs::write(temp_dir.path().join("fed.yaml"), MINIMAL_CONFIG).unwrap();
    let sub_dir = temp_dir.path().join("nested").join("deeper");
    fs::create_dir_all(&sub_dir).unwrap();

    let output = run_validate(&sub_dir);
    assert!(
        output.status.success(),
        "validate should find fed.yaml in a parent directory, stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}
