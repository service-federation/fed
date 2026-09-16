//! `fed start -i <service>`: one process service in the caller's terminal.
//!
//! The interesting half of interactive mode only exists under a real
//! terminal, so the first two tests drive fed through a pty (rexpect) and
//! assert on what the service does with the tty and on the exit code fed
//! adopts from it. Unix only, like the feature.
#![cfg(unix)]

use std::io::Write;
use std::process::{Command, Output, Stdio};
use std::time::{Duration, Instant};
use tempfile::TempDir;

fn fed_binary() -> &'static str {
    env!("CARGO_BIN_EXE_fed")
}

fn write_config(dir: &TempDir, yaml: &str) -> String {
    let path = dir.path().join("fed.yaml");
    std::fs::File::create(&path)
        .expect("create config")
        .write_all(yaml.as_bytes())
        .expect("write config");
    path.to_str().expect("utf-8 config path").to_string()
}

/// Run a fed subcommand to completion, failing the test rather than hanging
/// if it never returns.
fn run_fed(config: &str, args: &[&str], timeout: Duration) -> Output {
    let mut child = Command::new(fed_binary())
        .arg("-c")
        .arg(config)
        .args(args)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn fed");

    let deadline = Instant::now() + timeout;
    loop {
        match child.try_wait().expect("poll fed") {
            Some(_) => break,
            None if Instant::now() >= deadline => {
                let _ = child.kill();
                let _ = child.wait();
                panic!("fed {:?} did not finish within {:?}", args, timeout);
            }
            None => std::thread::sleep(Duration::from_millis(50)),
        }
    }

    child.wait_with_output().expect("collect fed output")
}

/// `fed status --json`, parsed. Runs in its own process, so against a live
/// `fed start -i` session this is the "other terminal" view of the stack.
fn status_json(config: &str) -> serde_json::Value {
    let output = run_fed(config, &["status", "--json"], Duration::from_secs(30));
    assert!(output.status.success(), "fed status --json must succeed");
    serde_json::from_slice(&output.stdout).expect("status output is json")
}

fn service_status(status: &serde_json::Value, service: &str) -> String {
    status[service]["status"]
        .as_str()
        .unwrap_or_else(|| panic!("{service} missing from status: {status}"))
        .to_string()
}

fn supervisor_running(status: &serde_json::Value, service: &str) -> bool {
    status[service]["supervisor_running"]
        .as_bool()
        .unwrap_or(false)
}

/// Wait for fed to exit and return its exit code.
///
/// Polls the process rather than waiting for EOF on the pty: rexpect leaks
/// its pty descriptors into the child, and from there into the nohup'd
/// services and supervisor fed spawns, so on Linux the slave stays open
/// (and EOF never comes) for as long as those daemons live.
fn exit_code(session: &mut rexpect::session::PtySession) -> i32 {
    use rexpect::process::wait::WaitStatus;
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        match session.process.status() {
            Some(WaitStatus::Exited(_, code)) => return code,
            Some(WaitStatus::StillAlive) | None => {}
            Some(other) => panic!("expected fed to exit normally, got {:?}", other),
        }
        assert!(Instant::now() < deadline, "fed did not exit within 30s");
        std::thread::sleep(Duration::from_millis(50));
    }
}

/// Stops everything this test file started, even when an assertion panics
/// first: a leaked `sleep` would outlive the test run.
struct StopGuard {
    config: String,
}

impl Drop for StopGuard {
    fn drop(&mut self) {
        let mut child = match Command::new(fed_binary())
            .arg("-c")
            .arg(&self.config)
            .arg("stop")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
        {
            Ok(child) => child,
            Err(_) => return,
        };
        let deadline = Instant::now() + Duration::from_secs(30);
        while Instant::now() < deadline {
            match child.try_wait() {
                Ok(Some(_)) => return,
                Ok(None) => std::thread::sleep(Duration::from_millis(50)),
                Err(_) => return,
            }
        }
        let _ = child.kill();
        let _ = child.wait();
    }
}

/// The service owns the terminal: it reads a command typed at fed's stdin,
/// writes the answer to fed's stdout, and its exit code becomes fed's.
#[test]
fn interactive_service_owns_the_terminal_and_its_exit_code() {
    let dir = TempDir::new().expect("temp dir");
    let config = write_config(
        &dir,
        r#"
services:
  shell:
    process: sh
"#,
    );

    let mut session = rexpect::spawn(
        &format!("{} -c {} start -i shell", fed_binary(), config),
        Some(10_000),
    )
    .expect("spawn fed under a pty");

    session.exp_string("Starting: shell").expect("start line");

    // Written so the marker cannot appear in the command itself: if the tty
    // ever echoes input back, the expectation below still only matches
    // output the service produced.
    session
        .send_line("printf 'marker-%s\\n' 42")
        .expect("send command");
    session.exp_string("marker-42").expect("service output");

    // The other-terminal view of a live session: the row is there and
    // reads as running.
    let status = status_json(&config);
    assert_eq!(service_status(&status, "shell"), "running");

    session.send_line("exit 3").expect("send exit");
    assert_eq!(
        exit_code(&mut session),
        3,
        "fed must exit with the service's exit code"
    );

    let status = status_json(&config);
    assert_eq!(
        service_status(&status, "shell"),
        "stopped",
        "shell must be unregistered after it exits"
    );
}

/// Dependencies start in the background as usual and keep running after the
/// foreground service exits. `fed stop` is what stops them.
///
/// Both services carry a restart policy, so this run spawns the supervisor
/// daemon and the foreground service is inside its config-derived scope. The
/// session then stays open past a reconcile tick (5s): the supervisor must
/// neither restart nor stop a service whose row says `foreground`.
#[test]
fn dependencies_outlive_the_interactive_service() {
    let dir = TempDir::new().expect("temp dir");
    let config = write_config(
        &dir,
        r#"
services:
  dep:
    process: "sleep 120"
    restart: !on_failure
      max_retries: 3
  shell:
    process: sh
    restart: !on_failure
      max_retries: 3
    depends_on: [dep]
"#,
    );
    let _guard = StopGuard {
        config: config.clone(),
    };

    let mut session = rexpect::spawn(
        &format!("{} -c {} start -i shell", fed_binary(), config),
        Some(60_000),
    )
    .expect("spawn fed under a pty");

    session
        .exp_string("has a restart policy")
        .expect("the ignored restart policy must be announced");

    // The supervisor must be live for the rest of this test to mean
    // anything: it is what could wrongly restart or stop the session.
    let status = status_json(&config);
    assert!(
        supervisor_running(&status, "dep"),
        "a restart policy must have spawned the supervisor, got: {status}"
    );
    assert_eq!(service_status(&status, "shell"), "running");

    // Outlives one supervisor poll tick (5s). The marker proves the service
    // was still alive and reading the terminal on the other side of it.
    session
        .send_line("sleep 7; printf 'marker-%s\\n' 99")
        .expect("send command");
    session.exp_string("marker-99").expect("service output");

    session.send_line("exit 0").expect("send exit");
    assert_eq!(exit_code(&mut session), 0);

    let status = status_json(&config);
    assert_eq!(
        service_status(&status, "dep"),
        "running",
        "dependency must survive the interactive service"
    );

    run_fed(&config, &["stop"], Duration::from_secs(60));
    let status = status_json(&config);
    assert!(
        !supervisor_running(&status, "dep"),
        "fed stop must take the supervisor with it, got: {status}"
    );
    for service in ["dep", "shell"] {
        assert_eq!(
            service_status(&status, service),
            "stopped",
            "fed stop must leave nothing running"
        );
    }
}

/// Ctrl+C typed at the terminal goes to the service alone: the service
/// dies of SIGINT, fed survives to report it and exits 130.
#[test]
fn ctrl_c_reaches_the_service_not_fed() {
    let dir = TempDir::new().expect("temp dir");
    let config = write_config(
        &dir,
        r#"
services:
  napper:
    process: "sleep 6011"
"#,
    );

    let mut session = rexpect::spawn(
        &format!("{} -c {} start -i napper", fed_binary(), config),
        Some(20_000),
    )
    .expect("spawn fed under a pty");
    session.exp_string("Starting: napper").expect("start line");

    let status = status_json(&config);
    assert_eq!(service_status(&status, "napper"), "running");

    session.send_control('c').expect("send ctrl-c");
    session
        .exp_string("napper exited (code 130)")
        .expect("fed reports the signal exit");
    assert_eq!(exit_code(&mut session), 130);
    assert!(!process_alive("sleep 6011"), "the service must be gone");
}

/// `fed stop <service>` from another terminal ends the session: it signals
/// the service's own process group, so a compound command's real program
/// dies too, and fed exits with the service's status instead of being
/// killed along with it.
#[test]
fn fed_stop_from_another_terminal_ends_the_session() {
    let dir = TempDir::new().expect("temp dir");
    let config = write_config(
        &dir,
        r#"
services:
  compound:
    process: "true && sleep 6012"
"#,
    );

    let mut session = rexpect::spawn(
        &format!("{} -c {} start -i compound", fed_binary(), config),
        Some(30_000),
    )
    .expect("spawn fed under a pty");
    session
        .exp_string("Starting: compound")
        .expect("start line");

    let deadline = Instant::now() + Duration::from_secs(10);
    while !process_alive("sleep 6012") && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(50));
    }
    assert!(process_alive("sleep 6012"), "the service must have started");

    let stop = run_fed(&config, &["stop", "compound"], Duration::from_secs(60));
    assert!(stop.status.success(), "fed stop must succeed");

    session
        .exp_string("compound exited (code 143)")
        .expect("fed reports the stop");
    assert_eq!(exit_code(&mut session), 143);
    assert!(
        !process_alive("sleep 6012"),
        "the compound command's program must be gone"
    );
}

/// Whether a process whose command line contains `needle` is running.
fn process_alive(needle: &str) -> bool {
    let output = Command::new("pgrep")
        .args(["-f", needle])
        .output()
        .expect("pgrep");
    output.status.success()
}

/// Two targets is a usage error, and it lands before anything is started.
#[test]
fn two_interactive_targets_start_nothing() {
    let dir = TempDir::new().expect("temp dir");
    let config = write_config(
        &dir,
        r#"
services:
  a:
    process: "sleep 30"
  b:
    process: "sleep 30"
"#,
    );

    let output = run_fed(&config, &["start", "-i", "a", "b"], Duration::from_secs(30));
    let stderr = String::from_utf8_lossy(&output.stderr);

    assert!(!output.status.success(), "two targets must fail");
    assert!(
        stderr.contains("interactive mode runs a single service"),
        "expected the usage error, got:\n{stderr}"
    );

    let status = status_json(&config);
    for service in ["a", "b"] {
        assert_eq!(
            service_status(&status, service),
            "stopped",
            "{service} must not have been started"
        );
    }
}
