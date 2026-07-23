//! Regression tests for compose-service state across fed processes.
//!
//! Before v7.2.1, a compose service persisted neither a PID nor a container
//! id, so `mark_dead_services` declared it stale the moment a NEW fed process
//! looked at the state: `fed status` reported a running compose project as
//! stopped, and `fed stop` early-returned without running `compose down`,
//! leaving the containers up forever. These tests drive the real binary twice
//! (separate processes) to make sure that stays fixed.

use std::fs;
use std::process::Command;
use tempfile::TempDir;

fn fed_binary() -> String {
    env!("CARGO_BIN_EXE_fed").to_string()
}

fn write_project(temp_dir: &TempDir) {
    let compose = r#"
services:
  kv:
    image: redis:7-alpine
"#;
    fs::write(temp_dir.path().join("docker-compose.yml"), compose).unwrap();
    let fed_yaml = r#"
services:
  kv:
    compose_file: ./docker-compose.yml
    compose_service: kv
"#;
    fs::write(temp_dir.path().join("fed.yaml"), fed_yaml).unwrap();
}

fn fed(temp_dir: &TempDir, args: &[&str]) -> std::process::Output {
    Command::new(fed_binary())
        .arg("-w")
        .arg(temp_dir.path())
        .arg("-c")
        .arg(temp_dir.path().join("fed.yaml"))
        .args(args)
        .current_dir(temp_dir.path())
        .output()
        .expect("Failed to run fed")
}

fn combined(output: &std::process::Output) -> String {
    format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    )
}

#[test]
#[cfg_attr(not(feature = "docker-tests"), ignore)] // Requires Docker (CI: cargo test --features docker-tests)
fn compose_service_survives_process_boundary() {
    let temp_dir = TempDir::new().unwrap();
    write_project(&temp_dir);

    let start = fed(&temp_dir, &["start", "kv"]);
    assert!(start.status.success(), "start failed: {}", combined(&start));

    // A brand-new fed process must see the compose service as running, not
    // report the stale-swept default.
    let status = fed(&temp_dir, &["status"]);
    let status_text = combined(&status);
    assert!(
        status_text.contains("running"),
        "fresh-process status should report running: {status_text}"
    );

    // And a brand-new fed process must actually stop it (run `compose down`),
    // not early-return off in-memory Stopped state.
    let stop = fed(&temp_dir, &["stop"]);
    assert!(stop.status.success(), "stop failed: {}", combined(&stop));

    let after = fed(&temp_dir, &["status"]);
    let after_text = combined(&after);
    assert!(
        after_text.contains("stopped"),
        "post-stop status should report stopped: {after_text}"
    );

    // The container itself must be gone — this is the part that silently
    // failed before the fix.
    let ps = Command::new("docker")
        .args(["ps", "--format", "{{.Names}}"])
        .output()
        .expect("docker ps failed");
    let names = String::from_utf8_lossy(&ps.stdout).to_string();
    assert!(
        !names.contains("-kv-"),
        "compose container still running after fed stop: {names}"
    );
}
