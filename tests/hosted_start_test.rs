#![cfg(unix)]
mod support;

use fed::attach::protocol::{self, Frame};
use nix::libc;
use std::io::Write;
use std::os::unix::net::UnixStream;
use std::path::PathBuf;
use std::process::{Command, Output, Stdio};
use std::time::{Duration, Instant};
use tempfile::TempDir;

struct Project(TempDir);

impl Project {
    fn new(yaml: &str) -> Self {
        support::parse_checked(yaml);
        let project = Self(tempfile::tempdir().unwrap());
        std::fs::write(project.0.path().join("fed.yaml"), yaml).unwrap();
        project
    }

    fn command(&self, args: &[&str]) -> Command {
        let mut command = Command::new(env!("CARGO_BIN_EXE_fed"));
        command
            .arg("--workdir")
            .arg(self.0.path())
            .args(args)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        command
    }

    fn run(&self, args: &[&str]) -> Output {
        let mut child = self.command(args).spawn().unwrap();
        let deadline = Instant::now() + Duration::from_secs(30);
        while child.try_wait().unwrap().is_none() {
            if Instant::now() >= deadline {
                let _ = child.kill();
                let output = child.wait_with_output().unwrap();
                panic!(
                    "fed {args:?} timed out: {}",
                    String::from_utf8_lossy(&output.stderr)
                );
            }
            std::thread::sleep(Duration::from_millis(30));
        }
        child.wait_with_output().unwrap()
    }

    fn success(&self, args: &[&str]) -> Output {
        let output = self.run(args);
        assert!(
            output.status.success(),
            "fed {args:?}: {} {}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        output
    }

    fn row(&self, service: &str) -> (u32, u32, PathBuf) {
        let db = rusqlite::Connection::open(self.0.path().join(".fed/lock.db")).unwrap();
        db.query_row(
            "SELECT pid, host_pid, attach_socket FROM services WHERE id = ?1",
            [service],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    PathBuf::from(row.get::<_, String>(2)?),
                ))
            },
        )
        .unwrap()
    }
}

impl Drop for Project {
    fn drop(&mut self) {
        let _ = self.run(&["stop"]);
    }
}

fn alive(pid: u32) -> bool {
    unsafe { libc::kill(pid as i32, 0) == 0 }
}

fn gone(pid: u32) {
    let deadline = Instant::now() + Duration::from_secs(5);
    while alive(pid) && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(30));
    }
    assert!(!alive(pid), "process {pid} survived stop");
}

const CAT: &str = "services:\n  repl:\n    process: cat\n    tty: true\n";

#[test]
fn hosted_start_logs_restart_and_stop() {
    let project = Project::new(CAT);
    project.success(&["start", "repl"]);
    let (pid, host, socket) = project.row("repl");
    assert!(alive(pid) && alive(host) && socket.exists());
    let mut stream = UnixStream::connect(&socket).unwrap();
    stream
        .write_all(&protocol::encode(&Frame::Hello {
            version: protocol::VERSION,
            cols: 80,
            rows: 24,
            flags: 0,
        }))
        .unwrap();
    stream
        .write_all(&protocol::encode(&Frame::Input(b"hosted-line\n".to_vec())))
        .unwrap();
    std::thread::sleep(Duration::from_millis(100));
    let logs = project.success(&["logs", "repl"]);
    assert!(String::from_utf8_lossy(&logs.stdout).contains("hosted-line"));
    project.success(&["restart", "repl"]);
    let (new_pid, new_host, new_socket) = project.row("repl");
    assert_ne!(host, new_host);
    gone(host);
    gone(pid);
    project.success(&["stop", "repl"]);
    gone(new_pid);
    gone(new_host);
    assert!(!new_socket.exists());
}

#[test]
fn immediate_exit_reports_code_and_terminal_log() {
    let project = Project::new(
        "services:\n  repl:\n    process: echo startup-diagnostic; exit 7\n    tty: true\n",
    );
    let output = project.run(&["start", "repl"]);
    assert!(!output.status.success());
    let text = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        text.contains("exited with code 7 right after starting"),
        "{text}"
    );
    assert!(text.contains("startup-diagnostic"), "{text}");
    assert!(
        !text.contains("background services have no stdin"),
        "{text}"
    );
}

#[test]
fn dead_socket_does_not_block_start_and_stop_sweeps_it() {
    let project = Project::new(CAT);
    let socket = fed::fed_dir::attach_socket_path(project.0.path(), "repl");
    std::fs::create_dir_all(socket.parent().unwrap()).unwrap();
    drop(std::os::unix::net::UnixListener::bind(&socket).unwrap());
    project.success(&["start", "repl"]);
    project.success(&["stop"]);
    assert!(!socket.exists());
    let stray = socket.with_file_name("stray.sock");
    drop(std::os::unix::net::UnixListener::bind(&stray).unwrap());
    project.success(&["stop"]);
    assert!(!stray.exists());
}

#[test]
fn invalid_config_stop_reaps_host() {
    let project = Project::new(CAT);
    project.success(&["start", "repl"]);
    let (pid, host, socket) = project.row("repl");
    std::fs::write(project.0.path().join("fed.yaml"), "invalid: [").unwrap();
    project.success(&["stop"]);
    gone(pid);
    gone(host);
    assert!(!socket.exists());
}

#[test]
fn removed_service_stop_reaps_host() {
    let project = Project::new(CAT);
    project.success(&["start", "repl"]);
    let (pid, host, socket) = project.row("repl");
    std::fs::write(project.0.path().join("fed.yaml"), "services: {}\n").unwrap();
    project.success(&["stop"]);
    gone(pid);
    gone(host);
    assert!(!socket.exists());
}

#[test]
fn resolved_environment_and_cwd_reach_terminal() {
    let project = Project::new(
        "services:\n  repl:\n    process: 'printf \"%s:%s\\n\" \"$TEST_HOSTED_VALUE\" \"$PWD\"; cat'\n    tty: true\n    cwd: child\n    environment:\n      TEST_HOSTED_VALUE: resolved-value\n",
    );
    std::fs::create_dir(project.0.path().join("child")).unwrap();
    project.success(&["start", "repl"]);
    let logs = project.success(&["logs", "repl"]);
    let text = String::from_utf8_lossy(&logs.stdout);
    assert!(text.contains("resolved-value:"), "{text}");
    assert!(text.contains("/child"), "{text}");
}

#[test]
fn tty_dependency_is_hosted() {
    let project = Project::new(
        "services:\n  repl:\n    process: cat\n    tty: true\n  app:\n    process: sleep 60\n    depends_on: [repl]\n",
    );
    project.success(&["start", "app"]);
    let (pid, host, socket) = project.row("repl");
    assert!(alive(pid) && alive(host) && socket.exists());
}

#[test]
fn foreground_tty_does_not_spawn_a_host() {
    let project =
        Project::new("services:\n  repl:\n    process: echo foreground-output\n    tty: true\n");
    let output = project.success(&["start", "-i", "repl"]);
    assert!(String::from_utf8_lossy(&output.stdout).contains("foreground-output"));
    assert!(!fed::fed_dir::attach_socket_path(project.0.path(), "repl").exists());
}

#[test]
fn cancellation_during_startup_reaps_service_and_host() {
    let project = Project::new(
        "services:\n  repl:\n    process: 'echo $$ > service.pid; cat'\n    tty: true\n",
    );
    let mut start = project.command(&["start", "repl"]).spawn().unwrap();
    let marker = project.0.path().join("service.pid");
    let deadline = Instant::now() + Duration::from_secs(15);
    let pid = loop {
        if let Ok(text) = std::fs::read_to_string(&marker)
            && let Ok(pid) = text.trim().parse::<u32>()
        {
            break pid;
        }
        if Instant::now() >= deadline {
            let _ = start.kill();
            let output = start.wait_with_output().unwrap();
            panic!(
                "service did not start: {}",
                String::from_utf8_lossy(&output.stderr)
            );
        }
        std::thread::sleep(Duration::from_millis(5));
    };
    let output = Command::new("ps")
        .args(["-p", &pid.to_string(), "-o", "ppid="])
        .output()
        .unwrap();
    let host: u32 = String::from_utf8_lossy(&output.stdout)
        .trim()
        .parse()
        .unwrap();
    unsafe {
        libc::kill(start.id() as i32, libc::SIGINT);
    }
    let deadline = Instant::now() + Duration::from_secs(15);
    while start.try_wait().unwrap().is_none() {
        if Instant::now() >= deadline {
            let _ = start.kill();
            break;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    let _ = start.wait();
    gone(pid);
    gone(host);
}

#[test]
fn stop_kills_terminal_descendants_after_the_leader_exits() {
    for invalid_config in [false, true] {
        let project = Project::new(
            r#"services:
  repl:
    tty: true
    grace_period: 1s
    process: |
      bash -c 'trap "" TERM HUP; echo $$ > descendant.pid; while :; do sleep 1; done' &
      wait
"#,
        );
        project.success(&["start", "repl"]);
        let descendant: u32 = std::fs::read_to_string(project.0.path().join("descendant.pid"))
            .unwrap()
            .trim()
            .parse()
            .unwrap();
        let (pid, host, socket) = project.row("repl");
        if invalid_config {
            std::fs::write(project.0.path().join("fed.yaml"), "invalid: [").unwrap();
        }
        project.success(&["stop"]);
        gone(pid);
        gone(host);
        gone(descendant);
        assert!(!socket.exists());
    }
}

#[test]
fn host_pid_from_another_workspace_is_not_signalled() {
    let first = Project::new(CAT);
    let second = Project::new(CAT);
    first.success(&["start", "repl"]);
    second.success(&["start", "repl"]);
    let (first_pid, first_host, _) = first.row("repl");
    let (other_pid, other_host, other_socket) = second.row("repl");
    let db = rusqlite::Connection::open(first.0.path().join(".fed/lock.db")).unwrap();
    db.execute(
        "UPDATE services SET host_pid = ?1 WHERE id = 'repl'",
        [other_host],
    )
    .unwrap();
    first.success(&["stop"]);
    gone(first_pid);
    gone(first_host);
    assert!(alive(other_pid) && alive(other_host) && other_socket.exists());
}

#[test]
fn stale_dead_service_reaps_a_stopped_host_before_purging_state() {
    for invalid_config in [false, true] {
        let project = Project::new(CAT);
        project.success(&["start", "repl"]);
        let (pid, host, socket) = project.row("repl");
        unsafe {
            libc::kill(host as i32, libc::SIGSTOP);
        }
        if invalid_config {
            std::fs::write(project.0.path().join("fed.yaml"), "invalid: [").unwrap();
        } else {
            unsafe {
                libc::kill(pid as i32, libc::SIGKILL);
            }
            std::thread::sleep(Duration::from_millis(100));
        }
        project.success(&["stop"]);
        gone(pid);
        gone(host);
        assert!(!socket.exists());
    }
}

#[test]
fn hosted_variant_keeps_selection_and_attach_metadata_across_commands() {
    let project = Project::new(
        "defaults:\n  tty: true\nservices:\n  repl:\n    default_variant: plain\n    variants:\n      plain:\n        process: cat\n      selected:\n        process: cat\n",
    );
    project.success(&["--variant", "selected", "start", "--all"]);
    let (pid, host, socket) = project.row("repl");
    assert!(alive(pid) && alive(host) && socket.exists());

    // A separate command has neither the original flag nor its resolution.
    let output = project.success(&["status", "--json"]);
    let json: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    let service = &json["repl"];
    assert_eq!(service["variant"], "selected");
    assert_eq!(service["attachable"], true);
    project.success(&["stop", "repl"]);
    gone(pid);
    gone(host);
    assert!(!socket.exists());
}
