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

/// RB: the reviewer's exact repro. A derived *parameter default* that
/// interpolates an out-of-scope missing manual secret used to fail project-wide
/// parameter resolution before service deferral ever ran ("Parameter
/// 'UNUSED_DERIVED' has unresolved template variables"). A scoped run must defer
/// it; an unscoped `fed start` must still fail.
const DERIVED_DEFAULT_CONFIG: &str = r#"
parameters:
  UNUSED_SECRET:
    type: secret
    source: manual
  UNUSED_DERIVED:
    default: "prefix-{{UNUSED_SECRET}}"

services:
  unrelated:
    process: "echo {{UNUSED_DERIVED}}"

scripts:
  target:
    script: "echo derived-default-ok"
"#;

fn write_derived_default_config(dir: &TempDir) -> String {
    let path = dir.path().join("service-federation.yaml");
    fs::write(&path, DERIVED_DEFAULT_CONFIG).expect("write config");
    path.to_str().unwrap().to_string()
}

#[test]
fn scoped_run_defers_derived_default_over_out_of_scope_secret() {
    let dir = TempDir::new().unwrap();
    let config = write_derived_default_config(&dir);

    let output = Command::new(fed_binary())
        .args(["-c", &config, "run", "target"])
        .current_dir(dir.path())
        .env_remove("FED_TOKEN")
        .env_remove("FED_CLOUD_URL")
        .output()
        .expect("failed to run fed");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "scoped run must not fail on a derived default over an out-of-scope secret.\nstdout: {stdout}\nstderr: {stderr}"
    );
    assert!(
        stdout.contains("derived-default-ok"),
        "the target script should have run.\nstdout: {stdout}\nstderr: {stderr}"
    );
}

#[test]
fn unscoped_start_still_fails_on_derived_default_secret() {
    let dir = TempDir::new().unwrap();
    let config = write_derived_default_config(&dir);

    let output = Command::new(fed_binary())
        .args(["-c", &config, "start", "--dry-run"])
        .current_dir(dir.path())
        .env_remove("FED_TOKEN")
        .env_remove("FED_CLOUD_URL")
        .output()
        .expect("failed to run fed");

    assert!(
        !output.status.success(),
        "unscoped start must still fail on the missing secret behind the derived default"
    );
}

/// RB (a): a non-secret `generate` parameter that references an out-of-scope
/// missing manual secret is deferred in a scoped run (its command not executed),
/// but still executed — and failing — in an unscoped run.
const GENERATE_CONFIG: &str = r#"
parameters:
  UNUSED_SECRET:
    type: secret
    source: manual
  UNUSED_GEN:
    generate: "sh -c 'test -n \"{{UNUSED_SECRET}}\" && printf %s derived || exit 3'"

services:
  unrelated:
    process: "echo {{UNUSED_GEN}}"

scripts:
  target:
    script: "echo generate-ok"
"#;

fn write_generate_config(dir: &TempDir) -> String {
    let path = dir.path().join("service-federation.yaml");
    fs::write(&path, GENERATE_CONFIG).expect("write config");
    path.to_str().unwrap().to_string()
}

#[test]
fn scoped_run_defers_generate_over_out_of_scope_secret() {
    let dir = TempDir::new().unwrap();
    let config = write_generate_config(&dir);

    let output = Command::new(fed_binary())
        .args(["-c", &config, "run", "target"])
        .current_dir(dir.path())
        .env_remove("FED_TOKEN")
        .env_remove("FED_CLOUD_URL")
        .output()
        .expect("failed to run fed");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "scoped run must defer the out-of-scope generate, not execute it.\nstdout: {stdout}\nstderr: {stderr}"
    );
    assert!(
        stdout.contains("generate-ok"),
        "the target script should have run.\nstdout: {stdout}\nstderr: {stderr}"
    );
}

#[test]
fn unscoped_start_still_fails_on_generate_secret() {
    let dir = TempDir::new().unwrap();
    let config = write_generate_config(&dir);

    let output = Command::new(fed_binary())
        .args(["-c", &config, "start", "--dry-run"])
        .current_dir(dir.path())
        .env_remove("FED_TOKEN")
        .env_remove("FED_CLOUD_URL")
        .output()
        .expect("failed to run fed");

    assert!(
        !output.status.success(),
        "unscoped start must still run the generate and fail on its missing secret"
    );
}

/// Strictness preserved: a derived default over an *in-scope* secret that is
/// missing must still fail the scoped run — only out-of-scope dependencies are
/// deferred. Here the target script itself references the derived value, pulling
/// its secret into scope, so the missing secret is fatal.
#[test]
fn scoped_run_still_fails_on_in_scope_derived_missing_secret() {
    let dir = TempDir::new().unwrap();
    let cfg = r#"
parameters:
  IN_SCOPE_SECRET:
    type: secret
    source: manual
  DERIVED:
    default: "prefix-{{IN_SCOPE_SECRET}}"

scripts:
  target:
    script: "echo {{DERIVED}}"
"#;
    let path = dir.path().join("service-federation.yaml");
    fs::write(&path, cfg).expect("write config");
    let config = path.to_str().unwrap().to_string();

    let output = Command::new(fed_binary())
        .args(["-c", &config, "run", "target"])
        .current_dir(dir.path())
        .env_remove("FED_TOKEN")
        .env_remove("FED_CLOUD_URL")
        .output()
        .expect("failed to run fed");

    assert!(
        !output.status.success(),
        "an in-scope secret behind a referenced derived value must still fail the run"
    );
}
