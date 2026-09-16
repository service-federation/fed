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

fn status_line(config: &str, service: &str) -> String {
    let output = run_fed(config, &["status"], Duration::from_secs(30));
    let stdout = String::from_utf8_lossy(&output.stdout).to_string();
    stdout
        .lines()
        .find(|line| line.contains(service))
        .unwrap_or_default()
        .to_string()
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

    session.send_line("exit 3").expect("send exit");
    session.exp_eof().expect("fed exits with the service");

    match session.process.wait().expect("reap fed") {
        rexpect::process::wait::WaitStatus::Exited(_, code) => {
            assert_eq!(code, 3, "fed must exit with the service's exit code");
        }
        other => panic!("expected a normal exit, got {:?}", other),
    }

    let line = status_line(&config, "shell");
    assert!(
        !line.contains("running"),
        "shell should be unregistered after it exits, got: {line}"
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

    // Outlives one supervisor poll tick. The marker proves the service was
    // still alive and reading the terminal on the other side of it.
    session
        .send_line("sleep 7; printf 'marker-%s\n' 99")
        .expect("send command");
    session.exp_string("marker-99").expect("service output");

    session.send_line("exit 0").expect("send exit");
    session.exp_eof().expect("fed exits with the service");

    match session.process.wait().expect("reap fed") {
        rexpect::process::wait::WaitStatus::Exited(_, code) => assert_eq!(code, 0),
        other => panic!("expected a normal exit, got {:?}", other),
    }

    let dep = status_line(&config, "dep");
    assert!(
        dep.contains("running"),
        "dependency must survive the interactive service, got: {dep}"
    );

    run_fed(&config, &["stop"], Duration::from_secs(60));
    let status = run_fed(&config, &["status"], Duration::from_secs(30));
    let stdout = String::from_utf8_lossy(&status.stdout);
    assert!(
        stdout.contains("Supervisor: none"),
        "fed stop must take the supervisor with it, got:\n{stdout}"
    );
    assert!(
        !stdout.contains("running"),
        "fed stop must leave nothing running, got:\n{stdout}"
    );
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

    for service in ["a", "b"] {
        let line = status_line(&config, service);
        assert!(
            !line.contains("running"),
            "{service} must not have been started, got: {line}"
        );
    }
}
