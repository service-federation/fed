use std::fs;
use std::process::Command;
use std::time::Duration;

fn create_test_config(content: &str) -> (tempfile::TempDir, std::path::PathBuf) {
    let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
    let config_path = temp_dir.path().join("test-config.yaml");
    fs::write(&config_path, content).expect("Failed to write test config");
    (temp_dir, config_path)
}

fn fed_binary() -> String {
    env!("CARGO_BIN_EXE_fed").to_string()
}

fn fed_start(config: &std::path::Path, workdir: &str, service: &str) -> std::process::Output {
    Command::new(fed_binary())
        .args([
            "-c",
            config.to_str().unwrap(),
            "-w",
            workdir,
            "start",
            service,
        ])
        .env("FED_NON_INTERACTIVE", "1")
        .output()
        .expect("Failed to run fed start")
}

fn fed_start_all(config: &std::path::Path, workdir: &str) -> std::process::Output {
    Command::new(fed_binary())
        .args(["-c", config.to_str().unwrap(), "-w", workdir, "start"])
        .env("FED_NON_INTERACTIVE", "1")
        .output()
        .expect("Failed to run fed start")
}

fn fed_stop(config: &std::path::Path, workdir: &str) {
    Command::new(fed_binary())
        .args(["-c", config.to_str().unwrap(), "-w", workdir, "stop"])
        .env("FED_NON_INTERACTIVE", "1")
        .output()
        .ok();
}

fn fed_status(config: &std::path::Path, workdir: &str) -> String {
    let output = Command::new(fed_binary())
        .args(["-c", config.to_str().unwrap(), "-w", workdir, "status"])
        .env("FED_NON_INTERACTIVE", "1")
        .output()
        .expect("Failed to run fed status");
    String::from_utf8_lossy(&output.stdout).to_string()
}

fn fed_logs(config: &std::path::Path, workdir: &str, service: &str) -> String {
    let output = Command::new(fed_binary())
        .args([
            "-c",
            config.to_str().unwrap(),
            "-w",
            workdir,
            "logs",
            service,
        ])
        .env("FED_NON_INTERACTIVE", "1")
        .output()
        .expect("Failed to run fed logs");
    String::from_utf8_lossy(&output.stdout).to_string()
}

fn assert_start_success(output: &std::process::Output) {
    assert!(
        output.status.success(),
        "fed start failed (exit {}):\nstdout: {}\nstderr: {}",
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );
}

const LONG_RUNNING_CONFIG: &str = r#"
services:
  long-running:
    process: |
      echo "Process started"
      sleep 300
"#;

#[test]
fn test_start_and_status() {
    let (temp_dir, config_path) = create_test_config(LONG_RUNNING_CONFIG);
    let workdir = temp_dir.path().to_str().unwrap();

    let output = fed_start(&config_path, workdir, "long-running");
    assert_start_success(&output);

    std::thread::sleep(Duration::from_secs(1));

    let status = fed_status(&config_path, workdir);
    assert!(
        status.contains("long-running"),
        "Status should list the service. Got:\n{}",
        status
    );

    fed_stop(&config_path, workdir);
}

#[test]
fn test_start_and_stop() {
    let (temp_dir, config_path) = create_test_config(LONG_RUNNING_CONFIG);
    let workdir = temp_dir.path().to_str().unwrap();

    let output = fed_start(&config_path, workdir, "long-running");
    assert_start_success(&output);

    std::thread::sleep(Duration::from_secs(1));

    let stop_output = Command::new(fed_binary())
        .args([
            "-c",
            config_path.to_str().unwrap(),
            "-w",
            workdir,
            "stop",
            "long-running",
        ])
        .env("FED_NON_INTERACTIVE", "1")
        .output()
        .expect("Failed to run fed stop");

    assert!(
        stop_output.status.success(),
        "fed stop failed (exit {}):\nstderr: {}",
        stop_output.status,
        String::from_utf8_lossy(&stop_output.stderr),
    );
}

#[test]
fn test_restart() {
    let (temp_dir, config_path) = create_test_config(LONG_RUNNING_CONFIG);
    let workdir = temp_dir.path().to_str().unwrap();

    let output = fed_start(&config_path, workdir, "long-running");
    assert_start_success(&output);

    std::thread::sleep(Duration::from_secs(1));

    let restart_output = Command::new(fed_binary())
        .args([
            "-c",
            config_path.to_str().unwrap(),
            "-w",
            workdir,
            "restart",
            "long-running",
        ])
        .env("FED_NON_INTERACTIVE", "1")
        .output()
        .expect("Failed to run fed restart");

    assert!(
        restart_output.status.success(),
        "fed restart failed (exit {}):\nstderr: {}",
        restart_output.status,
        String::from_utf8_lossy(&restart_output.stderr),
    );

    std::thread::sleep(Duration::from_secs(1));

    let status = fed_status(&config_path, workdir);
    assert!(
        status.contains("long-running"),
        "Service should still be listed after restart. Got:\n{}",
        status
    );

    fed_stop(&config_path, workdir);
}

#[test]
fn test_multiple_services() {
    let config = r#"
services:
  svc-a:
    process: |
      echo "Service A"
      sleep 300

  svc-b:
    process: |
      echo "Service B"
      sleep 300
"#;

    let (temp_dir, config_path) = create_test_config(config);
    let workdir = temp_dir.path().to_str().unwrap();

    let output = fed_start_all(&config_path, workdir);
    assert_start_success(&output);

    std::thread::sleep(Duration::from_secs(1));

    let status = fed_status(&config_path, workdir);
    assert!(
        status.contains("svc-a"),
        "Status should list svc-a. Got:\n{}",
        status
    );
    assert!(
        status.contains("svc-b"),
        "Status should list svc-b. Got:\n{}",
        status
    );

    fed_stop(&config_path, workdir);
}

#[test]
fn test_logs_persist_after_exit() {
    let config = r#"
services:
  logger:
    process: |
      echo "log-line-one"
      echo "log-line-two"
      sleep 5
"#;

    let (temp_dir, config_path) = create_test_config(config);
    let workdir = temp_dir.path().to_str().unwrap();

    let output = fed_start(&config_path, workdir, "logger");
    assert_start_success(&output);

    // Wait for the process to produce output and exit
    std::thread::sleep(Duration::from_secs(7));

    let logs = fed_logs(&config_path, workdir, "logger");
    assert!(
        logs.contains("log-line-one"),
        "Logs should contain output from the process. Got:\n{}",
        logs
    );

    fed_stop(&config_path, workdir);
}

// ============================================================================
// File-mode wrapper quoting fix (src/service/process.rs) regression coverage
// ============================================================================

#[test]
fn test_process_with_embedded_single_quote_starts_healthy() {
    // Reproduces the exact reported bug through the real `fed` binary
    // (not just the internal ProcessService API): a `process:` value
    // containing a single quote used to splice open the wrapper script's
    // own quoting, so `sh -c 'sleep 5'` ran as a bare `sleep` with the
    // wrong argument and exited with a usage error instead of sleeping.
    // Before the fix this made the service report Failing; after the fix
    // it should start and stay Running/Healthy.
    let config = r#"
services:
  quoted-service:
    process: "sh -c 'sleep 5'"
"#;
    let (temp_dir, config_path) = create_test_config(config);
    let workdir = temp_dir.path().to_str().unwrap();

    let output = fed_start(&config_path, workdir, "quoted-service");
    assert_start_success(&output);

    std::thread::sleep(Duration::from_secs(1));

    let status = fed_status(&config_path, workdir);
    assert!(
        status.contains("running") || status.contains("healthy"),
        "Service with an embedded single quote should start and stay \
         Running/Healthy, not crash with a `sleep` usage error. Got:\n{}",
        status
    );
    assert!(
        !status.contains("failing"),
        "Service should not report Failing (the mangled-wrapper symptom). Got:\n{}",
        status
    );

    fed_stop(&config_path, workdir);
}

#[test]
fn test_double_ampersand_without_quotes_still_works() {
    // Keep the simple, unquoted shell-operator case working alongside the
    // embedded-quote regression cases.
    let config = r#"
services:
  and-service:
    process: "echo one && echo two && sleep 300"
"#;
    let (temp_dir, config_path) = create_test_config(config);
    let workdir = temp_dir.path().to_str().unwrap();

    let output = fed_start(&config_path, workdir, "and-service");
    assert_start_success(&output);

    std::thread::sleep(Duration::from_secs(1));

    let logs = fed_logs(&config_path, workdir, "and-service");
    assert!(
        logs.contains("one") && logs.contains("two"),
        "Both && branches should have run. Got:\n{}",
        logs
    );

    fed_stop(&config_path, workdir);
}

/// Returns (pid, pgid) for every live process whose command line contains
/// `marker`, via `ps` (avoids depending on `pgrep`, which isn't guaranteed
/// to be installed everywhere `ps` is). `-ww` requests unlimited command
/// width from `ps` so the marker isn't truncated out of long command
/// lines; both BSD (macOS) and GNU (Linux) `ps` accept it.
fn find_processes_by_marker(marker: &str) -> Vec<(String, String)> {
    let output = Command::new("ps")
        .args(["-A", "-ww", "-o", "pid,pgid,command"])
        .output()
        .expect("Failed to run ps");
    let text = String::from_utf8_lossy(&output.stdout);
    text.lines()
        .skip(1) // header row
        .filter(|line| line.contains(marker))
        .filter_map(|line| {
            let mut fields = line.split_whitespace();
            let pid = fields.next()?.to_string();
            let pgid = fields.next()?.to_string();
            Some((pid, pgid))
        })
        .collect()
}

/// True if `ps -p pid` reports a live process (a data row beyond the header).
fn pid_is_alive(pid: &str) -> bool {
    match Command::new("ps").args(["-p", pid]).output() {
        Ok(out) => String::from_utf8_lossy(&out.stdout).lines().count() > 1,
        Err(_) => false,
    }
}

#[test]
fn background_mode_process_tree_fully_stopped() {
    // Sol's adversarial review (GPT-5.6, macOS bash 3.2.57) found a gap in
    // the existing test suite: the PID `echo $$` captures is not
    // guaranteed to be the process-group leader, so `fed stop` must
    // resolve and signal the whole process group (it already does - see
    // `ProcessService::get_process_group` / `killpg` in
    // src/service/process.rs - but nothing exercised that against a real
    // multi-process tree). This test spawns a launcher that backgrounds a
    // child and then waits on it, so at least two live PIDs share one
    // process group once the service reports Running, then confirms
    // `fed stop` leaves NONE of them behind - not just the originally
    // captured PID.
    //
    // This must pass against both the pre-fix and post-fix wrapper: the
    // wrapper rewrite is only supposed to change how process_cmd/the log
    // path are threaded through, not process-group/session structure.
    //
    // The marker is a large, distinctive `sleep` duration purely so `ps`
    // output can be matched unambiguously - it's never meant to actually
    // elapse (`fed stop` kills it long before then).
    const MARKER: &str = "194716213";
    let config = format!(
        "\nservices:\n  tree:\n    process: |\n      sleep {marker} &\n      wait\n",
        marker = MARKER
    );
    let (temp_dir, config_path) = create_test_config(&config);
    let workdir = temp_dir.path().to_str().unwrap();

    let output = fed_start(&config_path, workdir, "tree");
    assert_start_success(&output);

    // Give the backgrounded child a moment to actually appear in `ps`.
    std::thread::sleep(Duration::from_secs(1));

    let tree_pids = find_processes_by_marker(MARKER);
    assert!(
        !tree_pids.is_empty(),
        "expected to find the backgrounded `sleep {}` process tree via ps \
         - if this is empty the test fixture itself failed to create \
         anything to check, not necessarily a fed bug",
        MARKER
    );

    // Confirm the fixture actually created a multi-process tree sharing a
    // single process group (not just one process) - otherwise this test
    // would pass vacuously even against a broken killpg.
    let pgids: std::collections::HashSet<&str> =
        tree_pids.iter().map(|(_, pgid)| pgid.as_str()).collect();
    assert_eq!(
        pgids.len(),
        1,
        "all tree members should share exactly one process group, got: {:?}",
        tree_pids
    );

    fed_stop(&config_path, workdir);

    // Give signal delivery and reaping a moment.
    std::thread::sleep(Duration::from_secs(1));

    for (pid, _) in &tree_pids {
        assert!(
            !pid_is_alive(pid),
            "PID {} was part of the process tree under the service's \
             process group and should be gone after `fed stop` - fed stop \
             must kill the whole tree via killpg, not just the originally \
             captured PID (Sol review finding)",
            pid
        );
    }
}
