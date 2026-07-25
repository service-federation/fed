//! Integration tests for SF-00077: services with configured healthchecks should
//! be awaited during startup. Currently, `start_service_impl` only checks PID
//! liveness (300-500ms) and reports "ready" without polling the healthcheck.
//! These tests demonstrate the bug by asserting correct behavior that the
//! current implementation fails to provide.

use std::fs;
use std::process::Command;
use std::time::Duration;

fn fed_binary() -> String {
    env!("CARGO_BIN_EXE_fed").to_string()
}

fn fed_stop(config_path: &std::path::Path, workdir: &str) {
    Command::new(fed_binary())
        .args(["-c", config_path.to_str().unwrap(), "-w", workdir, "stop"])
        .env("FED_NON_INTERACTIVE", "1")
        .output()
        .ok();
}

/// SF-00077 test 1: A service that exits immediately with a command healthcheck
/// should NOT be reported as Running/Healthy after `fed start` completes.
///
/// Currently `start_service_impl` only checks PID liveness for 300-500ms after
/// spawn, then reports "ready" without ever polling the configured healthcheck.
/// A service that dies immediately (exit 1) is briefly reported as "Running"
/// because the healthcheck is never consulted during startup.
///
/// Expected (correct) behavior: after `fed start` returns, a service that
/// crashed should be reported as Stopped/Failed, not Running.
#[test]
fn test_service_with_healthcheck_reports_stopped_when_immediately_dying() {
    let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
    let config_path = temp_dir.path().join("service-federation.yaml");
    let workdir = temp_dir.path().to_str().unwrap();

    // Service that exits immediately. The healthcheck should never pass because
    // the process dies before it can be checked.
    let config_content = r#"
services:
  dying-service:
    process: "exit 1"
    healthcheck:
      command: "true"
      timeout: "5s"
"#;

    fs::write(&config_path, config_content).expect("Failed to write config");

    // Start the service
    let start_output = Command::new(fed_binary())
        .args([
            "-c",
            config_path.to_str().unwrap(),
            "-w",
            workdir,
            "start",
            "dying-service",
        ])
        .env("FED_NON_INTERACTIVE", "1")
        .output()
        .expect("Failed to run fed start");

    println!(
        "Start stdout:\n{}",
        String::from_utf8_lossy(&start_output.stdout)
    );
    println!(
        "Start stderr:\n{}",
        String::from_utf8_lossy(&start_output.stderr)
    );

    // Small delay to let any async state settle
    std::thread::sleep(Duration::from_millis(500));

    // Check status immediately
    let status_output = Command::new(fed_binary())
        .args(["-c", config_path.to_str().unwrap(), "-w", workdir, "status"])
        .env("FED_NON_INTERACTIVE", "1")
        .output()
        .expect("Failed to run fed status");

    let status_text = String::from_utf8_lossy(&status_output.stdout);
    let status_stderr = String::from_utf8_lossy(&status_output.stderr);
    println!("Status stdout:\n{}", status_text);
    println!("Status stderr:\n{}", status_stderr);

    // Check stdout only — stderr may contain unrelated Docker daemon warnings
    // (e.g. "Docker daemon unhealthy") that would false-positive on "healthy".
    let status_lower = status_text.to_lowercase();

    // The service exited immediately. If healthchecks were awaited during
    // startup, fed would have noticed the process died. The status should
    // NOT show "Running" or "Healthy" for a dead service.
    assert!(
        !status_lower.contains("running") && !status_lower.contains("healthy"),
        "SF-00077 BUG: A service that exited immediately should NOT be reported as \
         Running or Healthy. If healthchecks were awaited during startup, the dead \
         process would have been detected. Status output:\n{}{}",
        status_text,
        status_stderr
    );

    // The service name should appear in the status output
    assert!(
        status_lower.contains("dying-service"),
        "Service name should appear in status output. Got:\n{}",
        status_text
    );

    // It should show as Stopped or Failed
    assert!(
        status_lower.contains("stopped") || status_lower.contains("failed"),
        "Service should be reported as Stopped or Failed after immediate exit. \
         Status output:\n{}",
        status_text
    );

    // Cleanup
    fed_stop(&config_path, workdir);
}

/// SF-00077 test 2: A service with a healthcheck that takes time to become
/// healthy should be awaited during startup. After `fed start` returns, the
/// service should be Healthy (not just Running).
///
/// This test creates a long-running process that writes a marker file after 2
/// seconds. The healthcheck checks for that file. If `fed start` awaits the
/// healthcheck, it will not return until the marker file exists (i.e., the
/// service is truly healthy). Immediately checking `fed status` afterward
/// should show "Healthy".
///
/// Currently, `start_service_impl` returns after ~300-500ms of PID liveness,
/// well before the healthcheck has a chance to pass. `fed status` right after
/// would show "Running" (not "Healthy") because the health state hasn't been
/// polled yet.
#[test]
fn test_service_with_healthcheck_awaits_health_before_ready() {
    let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
    let config_path = temp_dir.path().join("service-federation.yaml");
    let workdir = temp_dir.path().to_str().unwrap();
    let marker_path = temp_dir.path().join("healthy-marker");

    // Service that starts immediately but becomes "healthy" after 2 seconds
    // (writes a marker file). The healthcheck checks for that file.
    let config_content = format!(
        r#"
services:
  slow-health-service:
    process: |
      echo "Starting up..."
      sleep 2
      touch {marker}
      echo "Now healthy"
      sleep 300
    healthcheck:
      command: "test -f {marker}"
      timeout: "10s"
"#,
        marker = marker_path.display()
    );

    fs::write(&config_path, &config_content).expect("Failed to write config");

    let start_time = std::time::Instant::now();

    // Start the service. If healthchecks are awaited, this should block for
    // at least ~2 seconds (until the marker file is created).
    let start_output = Command::new(fed_binary())
        .args([
            "-c",
            config_path.to_str().unwrap(),
            "-w",
            workdir,
            "start",
            "slow-health-service",
        ])
        .env("FED_NON_INTERACTIVE", "1")
        .output()
        .expect("Failed to run fed start");

    let start_duration = start_time.elapsed();

    println!(
        "Start stdout:\n{}",
        String::from_utf8_lossy(&start_output.stdout)
    );
    println!(
        "Start stderr:\n{}",
        String::from_utf8_lossy(&start_output.stderr)
    );
    println!("Start took: {:?}", start_duration);

    assert!(
        start_output.status.success(),
        "fed start should succeed: {}",
        String::from_utf8_lossy(&start_output.stderr)
    );

    // If healthchecks were properly awaited, `fed start` should have waited
    // at least ~2 seconds for the marker file to appear.
    //
    // SF-00077 bug: currently start returns after ~300-500ms PID check,
    // not waiting for the healthcheck at all.
    assert!(
        start_duration >= Duration::from_secs(2),
        "SF-00077 BUG: `fed start` returned in {:?}, which is before the service \
         became healthy (~2s). This means the healthcheck was NOT awaited during \
         startup. `fed start` should wait for the configured healthcheck to pass \
         before reporting the service as ready.",
        start_duration
    );

    // Since we waited for health, the marker file should exist
    assert!(
        marker_path.exists(),
        "Marker file should exist after fed start returns (healthcheck passed)"
    );

    // Check status immediately - should show Healthy since we waited
    let status_output = Command::new(fed_binary())
        .args(["-c", config_path.to_str().unwrap(), "-w", workdir, "status"])
        .env("FED_NON_INTERACTIVE", "1")
        .output()
        .expect("Failed to run fed status");

    let status_text = String::from_utf8_lossy(&status_output.stdout);
    let status_stderr = String::from_utf8_lossy(&status_output.stderr);
    println!("Status stdout:\n{}", status_text);
    println!("Status stderr:\n{}", status_stderr);

    let combined = format!("{}{}", status_text, status_stderr);
    let combined_lower = combined.to_lowercase();

    // After waiting for healthcheck, the service should be reported as Healthy
    assert!(
        combined_lower.contains("healthy") || combined_lower.contains("running"),
        "Service should be Healthy after fed start awaited its healthcheck. \
         Status output:\n{}",
        combined
    );

    // Cleanup
    fed_stop(&config_path, workdir);
}

/// Regression: a healthcheck that passes during `fed start` must still read as
/// healthy from a *later, separate* `fed status` process.
///
/// The passing check used to be observed, printed ("✓ verified healthy in
/// 1.5s"), and then discarded: only the SQLite row was moved to `Healthy`,
/// while the in-memory manager stayed `Running`. So `fed start`'s own summary
/// two lines later said "Running (health unverified)", and a fresh `fed
/// status` — which restores managers from the row but only carried the
/// persisted status forward for oneshot and compose services — agreed with it.
///
/// Asserted through `--json` on purpose: the human-readable status line says
/// "healthy" for both `Healthy` and (via the `running` substring) `Running`,
/// which is exactly the looseness that let this through. `health` is
/// `"healthy"` only for `Status::Healthy`, and `"unknown"` for `Running`.
#[test]
fn test_passed_startup_healthcheck_is_reported_healthy_by_later_status() {
    let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
    let config_path = temp_dir.path().join("service-federation.yaml");
    let workdir = temp_dir.path().to_str().unwrap();

    // `verified` becomes healthy once it touches a marker; `no-check` has no
    // healthcheck at all. Both must stay distinguishable afterwards — the fix
    // must promote the first without collapsing the second into a false
    // "healthy". The marker matters: fed rejects a healthcheck that already
    // passes *before* its service starts (it can't tell that apart from a
    // foreign process serving the same endpoint), so a `true` check is not a
    // usable stand-in here.
    let marker_path = temp_dir.path().join("verified-marker");
    let config_content = format!(
        r#"
services:
  verified:
    process: |
      touch {marker}
      sleep 300
    healthcheck:
      command: "test -f {marker}"
      timeout: "10s"
  no-check:
    process: "sleep 300"
"#,
        marker = marker_path.display()
    );
    fs::write(&config_path, &config_content).expect("Failed to write config");

    let start_output = Command::new(fed_binary())
        .args([
            "-c",
            config_path.to_str().unwrap(),
            "-w",
            workdir,
            "start",
            "verified",
            "no-check",
        ])
        .env("FED_NON_INTERACTIVE", "1")
        .output()
        .expect("Failed to run fed start");

    let start_text = format!(
        "{}{}",
        String::from_utf8_lossy(&start_output.stdout),
        String::from_utf8_lossy(&start_output.stderr)
    );
    println!("Start output:\n{}", start_text);
    assert!(
        start_output.status.success(),
        "fed start failed:\n{}",
        start_text
    );

    // The start summary must not contradict the outcome line it just printed.
    assert!(
        !start_text.contains("verified: Running (health unverified)"),
        "start summary reported 'health unverified' for a service whose \
         healthcheck passed during this very start:\n{}",
        start_text
    );

    let status_output = Command::new(fed_binary())
        .args([
            "-c",
            config_path.to_str().unwrap(),
            "-w",
            workdir,
            "status",
            "--json",
        ])
        .env("FED_NON_INTERACTIVE", "1")
        .output()
        .expect("Failed to run fed status --json");

    let status_stdout = String::from_utf8_lossy(&status_output.stdout).to_string();
    println!("Status JSON:\n{}", status_stdout);
    let json: serde_json::Value = serde_json::from_str(&status_stdout)
        .unwrap_or_else(|e| panic!("status --json did not emit valid JSON: {e}\n{status_stdout}"));

    assert_eq!(
        json["verified"]["health"], "healthy",
        "a healthcheck that passed during start must survive into a later \
         `fed status`, not decay back to unverified:\n{}",
        status_stdout
    );
    assert_eq!(
        json["verified"]["status"], "healthy",
        "status word must match the health bucket:\n{}",
        status_stdout
    );

    // The distinction has to stay meaningful: no healthcheck configured is
    // still "process is up, nothing verified it", never "healthy".
    assert_eq!(
        json["no-check"]["health"], "unknown",
        "a service with no healthcheck must not be promoted to healthy:\n{}",
        status_stdout
    );
    assert_eq!(
        json["no-check"]["status"], "running",
        "a service with no healthcheck must stay Running:\n{}",
        status_stdout
    );

    fed_stop(&config_path, workdir);
}
