//! Integration tests for startup health-warning reporting.
//!
//! Semantics under test:
//! - A healthcheck timeout during `fed start` is non-fatal: the process keeps
//!   running, dependents proceed, and the command exits 0.
//! - But "All services started successfully!" is reserved for fully healthy
//!   starts. Timed-out healthchecks must surface as a structured warning
//!   summary ("Services started with N health warning(s)") and as
//!   "Running (healthcheck timed out)" in the post-start status list.
//! - `fed status` shows Running-with-a-configured-healthcheck as
//!   "(health unverified)"; JSON keeps `status: "running"` with
//!   `health: "unknown"`.

use std::fs;
use std::process::{Command, Output};

fn fed_binary() -> String {
    env!("CARGO_BIN_EXE_fed").to_string()
}

fn fed(config_path: &std::path::Path, workdir: &str, args: &[&str]) -> Output {
    Command::new(fed_binary())
        .args(["-c", config_path.to_str().unwrap(), "-w", workdir])
        .args(args)
        .env("FED_NON_INTERACTIVE", "1")
        .output()
        .expect("failed to run fed")
}

fn fed_stop(config_path: &std::path::Path, workdir: &str) {
    fed(config_path, workdir, &["stop"]);
}

struct StartRun {
    _temp_dir: tempfile::TempDir,
    config_path: std::path::PathBuf,
    workdir: String,
    stdout: String,
    stderr: String,
    exit_success: bool,
}

/// Stop services on drop so a failing assertion doesn't leak `sleep 300`
/// processes.
impl Drop for StartRun {
    fn drop(&mut self) {
        fed_stop(&self.config_path, &self.workdir);
    }
}

/// Write `config` to a temp dir and run `fed start <services...>`.
fn run_start(config: &str, services: &[&str]) -> StartRun {
    let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
    let config_path = temp_dir.path().join("service-federation.yaml");
    let workdir = temp_dir.path().to_str().unwrap().to_string();
    fs::write(&config_path, config).expect("Failed to write config");

    let mut args = vec!["start"];
    args.extend_from_slice(services);
    let output = fed(&config_path, &workdir, &args);

    let run = StartRun {
        config_path,
        workdir,
        stdout: String::from_utf8_lossy(&output.stdout).to_string(),
        stderr: String::from_utf8_lossy(&output.stderr).to_string(),
        exit_success: output.status.success(),
        _temp_dir: temp_dir,
    };
    println!("Start stdout:\n{}", run.stdout);
    println!("Start stderr:\n{}", run.stderr);
    run
}

/// Fully healthy start: the unconditional success line is printed and the
/// command exits 0 with no warning summary.
#[test]
fn healthy_start_prints_success_and_exits_zero() {
    // The healthcheck must fail before start (fed refuses healthchecks that
    // already pass — foreign-listener preflight) and pass shortly after: the
    // process writes a marker file the check looks for. Absolute marker path
    // because the healthcheck command's cwd is not the service's workdir.
    let marker_dir = tempfile::tempdir().expect("Failed to create marker dir");
    let marker = marker_dir.path().join("ok-marker");
    let run = run_start(
        &format!(
            r#"
services:
  ok-service:
    process: "touch {marker} && sleep 300"
    healthcheck:
      command: "test -f {marker}"
      timeout: "10s"
"#,
            marker = marker.display()
        ),
        &["ok-service"],
    );

    assert!(run.exit_success, "healthy start must exit 0");
    assert!(
        run.stdout.contains("All services started successfully!"),
        "fully healthy start must print the success line. stdout:\n{}",
        run.stdout
    );
    assert!(
        !run.stderr.contains("health warning"),
        "no warning summary on a healthy start. stderr:\n{}",
        run.stderr
    );
}

/// Healthcheck timeout with the process still alive: non-fatal (exit 0), but
/// the success line must NOT be printed — the warning summary and the
/// "Running (healthcheck timed out)" status line replace it.
#[test]
fn health_timeout_is_nonfatal_but_never_reports_full_success() {
    let run = run_start(
        r#"
services:
  never-healthy:
    process: "sleep 300"
    healthcheck:
      command: "false"
      timeout: "1s"
"#,
        &["never-healthy"],
    );

    assert!(
        run.exit_success,
        "a healthcheck timeout with a live process is non-fatal — exit 0. stderr:\n{}",
        run.stderr
    );
    assert!(
        !run.stdout.contains("All services started successfully!"),
        "the success line is reserved for fully healthy starts. stdout:\n{}",
        run.stdout
    );
    assert!(
        run.stderr
            .contains("Services started with 1 health warning"),
        "the summary must count health warnings. stderr:\n{}",
        run.stderr
    );
    assert!(
        run.stderr.contains("never-healthy"),
        "the warning summary must name the affected service. stderr:\n{}",
        run.stderr
    );
    assert!(
        run.stdout.contains("Running (healthcheck timed out)"),
        "post-start status must show the timed-out state, not bare Running. stdout:\n{}",
        run.stdout
    );
}

/// A dependent may proceed after its dependency's healthcheck times out
/// (non-fatal), but the warning must be preserved in the final summary.
#[test]
fn dependent_proceeds_after_nonfatal_timeout_and_warning_is_preserved() {
    let run = run_start(
        r#"
services:
  flaky-dep:
    process: "sleep 300"
    healthcheck:
      command: "false"
      timeout: "1s"
  app:
    process: "sleep 300"
    depends_on:
      - flaky-dep
"#,
        &["app"],
    );

    assert!(
        run.exit_success,
        "start must exit 0. stderr:\n{}",
        run.stderr
    );
    assert!(
        run.stdout.contains("healthcheck timed out"),
        "the dependency's progress line must show the timeout. stdout:\n{}",
        run.stdout
    );
    assert!(
        run.stderr
            .contains("Services started with 1 health warning"),
        "exactly the dependency's timeout is a warning. stderr:\n{}",
        run.stderr
    );

    // The dependent proceeded: its own status line (not just any line) must
    // show it running. It has no healthcheck, so it stays plain Running.
    let status = fed(&run.config_path, &run.workdir, &["status"]);
    let status_text = String::from_utf8_lossy(&status.stdout).to_string();
    println!("Status stdout:\n{}", status_text);
    let app_line = status_text
        .lines()
        .find(|line| line.contains(" app ") || line.trim_start().starts_with("app"))
        .unwrap_or_else(|| panic!("dependent must appear in status. stdout:\n{}", status_text));
    assert!(
        app_line.contains("running") || app_line.contains("healthy"),
        "dependent must have started after the non-fatal timeout. app line: {:?}\nstdout:\n{}",
        app_line,
        status_text
    );
}

/// `fed restart` (restart_all path) after a warning start: the timeout is
/// re-observed and reported, and the unconditional restart success line is
/// suppressed.
#[test]
fn restart_reports_health_warnings_and_suppresses_success_line() {
    let run = run_start(
        r#"
services:
  never-healthy:
    process: "sleep 300"
    healthcheck:
      command: "false"
      timeout: "1s"
"#,
        &["never-healthy"],
    );
    assert!(run.exit_success);

    let restart = fed(&run.config_path, &run.workdir, &["restart"]);
    let stdout = String::from_utf8_lossy(&restart.stdout).to_string();
    let stderr = String::from_utf8_lossy(&restart.stderr).to_string();
    println!("Restart stdout:\n{}", stdout);
    println!("Restart stderr:\n{}", stderr);

    assert!(
        restart.status.success(),
        "health timeouts stay non-fatal on restart. stderr:\n{}",
        stderr
    );
    assert!(
        !stdout.contains("restarted successfully"),
        "the restart success line is reserved for fully healthy restarts. stdout:\n{}",
        stdout
    );
    assert!(
        stderr.contains("Services restarted with 1 health warning"),
        "restart must report the re-observed timeout. stderr:\n{}",
        stderr
    );
}

/// Process death before the healthcheck passes is fatal: non-zero exit and no
/// success line.
#[test]
fn process_death_before_health_success_fails_start() {
    let run = run_start(
        r#"
services:
  crasher:
    process: "sleep 1 && exit 1"
    healthcheck:
      command: "false"
      timeout: "30s"
"#,
        &["crasher"],
    );

    assert!(
        !run.exit_success,
        "a process that dies before its healthcheck passes must fail fed start. stdout:\n{}\nstderr:\n{}",
        run.stdout, run.stderr
    );
    assert!(
        !run.stdout.contains("All services started successfully!"),
        "no success line on a failed start. stdout:\n{}",
        run.stdout
    );
}

/// Multiple timed-out healthchecks are all counted and listed.
#[test]
fn multiple_health_warnings_are_counted_and_listed() {
    let run = run_start(
        r#"
services:
  first-unhealthy:
    process: "sleep 300"
    healthcheck:
      command: "false"
      timeout: "1s"
  second-unhealthy:
    process: "sleep 300"
    healthcheck:
      command: "false"
      timeout: "1s"
"#,
        &["first-unhealthy", "second-unhealthy"],
    );

    assert!(
        run.exit_success,
        "timeouts are non-fatal. stderr:\n{}",
        run.stderr
    );
    assert!(
        run.stderr
            .contains("Services started with 2 health warning"),
        "both timeouts must be counted. stderr:\n{}",
        run.stderr
    );
    assert!(
        run.stderr.contains("first-unhealthy") && run.stderr.contains("second-unhealthy"),
        "both services must be listed. stderr:\n{}",
        run.stderr
    );
    assert!(
        !run.stdout.contains("All services started successfully!"),
        "no success line with warnings present. stdout:\n{}",
        run.stdout
    );
}

/// `fed status` after a timeout: human output annotates the unverified state;
/// JSON keeps the compatible `status: "running"` plus `health: "unknown"`.
#[test]
fn status_human_and_json_distinguish_unverified_running() {
    let run = run_start(
        r#"
services:
  never-healthy:
    process: "sleep 300"
    healthcheck:
      command: "false"
      timeout: "1s"
"#,
        &["never-healthy"],
    );
    assert!(run.exit_success);

    let status = fed(&run.config_path, &run.workdir, &["status"]);
    let human = String::from_utf8_lossy(&status.stdout).to_string();
    println!("Status stdout:\n{}", human);
    assert!(
        human.contains("running (health unverified)"),
        "human status must not present unverified Running as plain success. stdout:\n{}",
        human
    );

    let status_json = fed(&run.config_path, &run.workdir, &["status", "--json"]);
    let json = String::from_utf8_lossy(&status_json.stdout).to_string();
    println!("Status JSON:\n{}", json);
    assert!(
        json.contains("\"status\": \"running\""),
        "JSON `status` field stays compatible. json:\n{}",
        json
    );
    assert!(
        json.contains("\"health\": \"unknown\""),
        "JSON `health` bucket must stay \"unknown\" for unverified Running. json:\n{}",
        json
    );
}

/// An invalid healthcheck URL must not be silently skipped: the service
/// still starts (non-fatal, exit 0), but the start reports a health warning
/// naming the service, and the post-start status shows
/// "Running (healthcheck invalid)" instead of bare Running.
#[test]
fn invalid_healthcheck_url_warns_instead_of_silently_skipping() {
    let run = run_start(
        r#"
services:
  bad-url:
    process: "sleep 300"
    healthcheck:
      http_get: "not a valid url"
      timeout: "1s"
"#,
        &["bad-url"],
    );

    assert!(
        run.exit_success,
        "an invalid healthcheck is non-fatal — the process still starts, exit 0. stderr:\n{}",
        run.stderr
    );
    assert!(
        !run.stdout.contains("All services started successfully!"),
        "the success line is reserved for fully healthy starts. stdout:\n{}",
        run.stdout
    );
    assert!(
        run.stderr
            .contains("Services started with 1 health warning"),
        "an invalid checker must count as a health warning. stderr:\n{}",
        run.stderr
    );
    assert!(
        run.stderr.contains("bad-url") && run.stderr.contains("healthcheck is invalid"),
        "the warning must name the service and say the healthcheck is invalid. stderr:\n{}",
        run.stderr
    );
    assert!(
        run.stdout.contains("Running (healthcheck invalid)"),
        "post-start status must show the invalid-healthcheck state. stdout:\n{}",
        run.stdout
    );
}
