#![cfg(unix)]

mod support;

use std::io::Write;
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::Path;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use fed::attach::protocol::{FLAG_NO_STDIN, Frame, FrameReader, encode};
use fed::state::{ServiceState, SqliteStateTracker};
use tempfile::TempDir;

fn binary() -> &'static str {
    env!("CARGO_BIN_EXE_fed")
}

fn seed(dir: &Path, status: &str, socket: Option<&Path>) {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    runtime.block_on(async {
        let mut tracker = SqliteStateTracker::new(dir.to_path_buf()).await.unwrap();
        tracker.initialize().await.unwrap();
        let row: ServiceState = serde_json::from_value(serde_json::json!({
            "id": "repl", "status": status, "service_type": "Process",
            "pid": std::process::id(), "container_id": null, "port_allocations": {},
            "started_at": chrono::Utc::now(), "external_repo": null, "namespace": "root",
            "attach_socket": socket
        }))
        .unwrap();
        tracker.register_service(row).await.unwrap();
    });
}

fn attach(dir: &Path, args: &[&str]) -> Command {
    let mut command = Command::new(binary());
    command
        .arg("--workdir")
        .arg(dir)
        .arg("attach")
        .arg("repl")
        .args(args);
    command
}

fn pty_command(dir: &Path, args: &str) -> String {
    format!(
        "{} --workdir {} attach repl {}",
        binary(),
        dir.display(),
        args
    )
}

fn exit_code(session: &mut rexpect::session::PtySession) -> i32 {
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        if let Some(rexpect::process::wait::WaitStatus::Exited(_, code)) = session.process.status()
        {
            return code;
        }
        assert!(Instant::now() < deadline, "attach did not exit");
        std::thread::sleep(Duration::from_millis(20));
    }
}

fn fake_host(dir: &Path) -> UnixListener {
    let path = dir.join("host.sock");
    let listener = UnixListener::bind(&path).unwrap();
    seed(dir, "running", Some(&path));
    listener
}

fn hello(listener: UnixListener) -> (UnixStream, FrameReader<UnixStream>, u8) {
    let (stream, _) = listener.accept().unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    let mut reader = FrameReader::new(stream.try_clone().unwrap());
    let Frame::Hello { flags, .. } = reader.read_frame().unwrap() else {
        panic!("expected hello")
    };
    (stream, reader, flags)
}

#[test]
fn state_errors_have_actionable_exact_messages() {
    let dir = TempDir::new().unwrap();
    for (status, socket, expected) in [
        (
            None,
            None,
            "Service 'repl' is not running. Start it with: fed start repl",
        ),
        (
            Some("stopped"),
            None,
            "Service 'repl' is not running. Start it with: fed start repl",
        ),
        (
            Some("running"),
            None,
            "Service 'repl' is not attachable. Give it `tty: true` in fed.yaml to run it under a terminal, or follow its output with: fed logs -f repl",
        ),
        (
            Some("running"),
            Some(dir.path().join("gone.sock")),
            "Service 'repl' has no host to attach to. Its state is stale; restart it with: fed restart repl",
        ),
    ] {
        let case = TempDir::new().unwrap();
        if let Some(status) = status {
            seed(case.path(), status, socket.as_deref());
        }
        let output = attach(case.path(), &["--no-stdin"]).output().unwrap();
        assert_eq!(output.status.code(), Some(1));
        assert!(
            String::from_utf8_lossy(&output.stderr).contains(expected),
            "{:?}",
            output
        );
    }
}

#[test]
fn service_exit_codes_and_signals_are_preserved() {
    for (status, expected) in [(7 << 8, 7), (nix::libc::SIGTERM, 143)] {
        let dir = TempDir::new().unwrap();
        let listener = fake_host(dir.path());
        let host = std::thread::spawn(move || {
            let (mut stream, _, flags) = hello(listener);
            assert_eq!(flags, FLAG_NO_STDIN);
            stream
                .write_all(&encode(&Frame::Scrollback(b"recent output\n".to_vec())))
                .unwrap();
            stream.write_all(&encode(&Frame::Exit { status })).unwrap();
        });
        let output = attach(dir.path(), &["--no-stdin"]).output().unwrap();
        assert_eq!(output.status.code(), Some(expected));
        assert!(String::from_utf8_lossy(&output.stdout).contains("recent output"));
        host.join().unwrap();
    }
}

#[test]
fn no_stdin_does_not_forward_input() {
    let dir = TempDir::new().unwrap();
    let listener = fake_host(dir.path());
    let host = std::thread::spawn(move || {
        let (mut stream, mut reader, flags) = hello(listener);
        assert_eq!(flags, FLAG_NO_STDIN);
        stream
            .set_read_timeout(Some(Duration::from_millis(300)))
            .unwrap();
        assert!(reader.read_frame().is_err(), "no input frame should arrive");
        stream
            .write_all(&encode(&Frame::Exit { status: 0 }))
            .unwrap();
    });
    let mut child = attach(dir.path(), &["--no-stdin"])
        .stdin(Stdio::piped())
        .spawn()
        .unwrap();
    child
        .stdin
        .take()
        .unwrap()
        .write_all(b"must not reach service\n")
        .unwrap();
    assert!(child.wait().unwrap().success());
    host.join().unwrap();
}

#[test]
fn detach_forwards_input_and_restores_terminal() {
    let dir = TempDir::new().unwrap();
    let listener = fake_host(dir.path());
    let host = std::thread::spawn(move || {
        let (mut stream, mut reader, flags) = hello(listener);
        assert_eq!(flags, 0);
        stream
            .write_all(&encode(&Frame::Output(b"HOST_READY\r\n".to_vec())))
            .unwrap();
        let mut input = Vec::new();
        while let Ok(Frame::Input(bytes)) = reader.read_frame() {
            input.extend(bytes);
        }
        assert_eq!(input, b"client-line\r");
    });
    let command = format!(
        "bash -c 'before=$(stty -a | sed -E \"s/-?pendin//g\"); {}; result=$?; test \"$before\" = \"$(stty -a | sed -E \"s/-?pendin//g\")\" && echo TERMINAL_RESTORED; exit $result'",
        pty_command(dir.path(), "--detach-keys ctrl-a,ctrl-d")
    );
    let mut session = rexpect::spawn(&command, Some(10000)).unwrap();
    session.exp_string("HOST_READY").unwrap();
    session.send("client-line\r").unwrap();
    session.send("\x01\x04").unwrap();
    session.flush().unwrap();
    session.exp_string("TERMINAL_RESTORED").unwrap();
    assert_eq!(exit_code(&mut session), 0);
    host.join().unwrap();
}

#[test]
// PENDIN is a kernel retype flag, not a terminal mode selected by the client.
// Ignore it when comparing snapshots after switching back to canonical input.
fn connection_error_restores_terminal() {
    let dir = TempDir::new().unwrap();
    let listener = fake_host(dir.path());
    let host = std::thread::spawn(move || {
        let _ = hello(listener);
    });
    let command = format!(
        "bash -c 'before=$(stty -a | sed -E \"s/-?pendin//g\"); {}; result=$?; test \"$before\" = \"$(stty -a | sed -E \"s/-?pendin//g\")\" && echo TERMINAL_RESTORED; exit $result'",
        pty_command(dir.path(), "")
    );
    let mut session = rexpect::spawn(&command, Some(10000)).unwrap();
    session.exp_string("TERMINAL_RESTORED").unwrap();
    assert_eq!(exit_code(&mut session), 1);
    host.join().unwrap();
}

struct RunningService(TempDir);
impl RunningService {
    fn start(command: &str) -> Self {
        let dir = TempDir::new().unwrap();
        let yaml = format!("services:\n  repl:\n    process: '{command}'\n    tty: true\n");
        support::parse_checked(&yaml);
        std::fs::write(dir.path().join("fed.yaml"), yaml).unwrap();
        let service = Self(dir);
        let output = service.command("start").arg("repl").output().unwrap();
        assert!(output.status.success(), "{:?}", output);
        service
    }
    fn command(&self, subcommand: &str) -> Command {
        let mut command = Command::new(binary());
        command.current_dir(self.0.path()).arg(subcommand);
        command
    }
}
impl Drop for RunningService {
    fn drop(&mut self) {
        let _ = self.command("stop").output();
    }
}

#[test]
fn hosted_cat_two_clients_detach_and_logs() {
    let service = RunningService::start("cat");
    let mut first = rexpect::spawn(&pty_command(service.0.path(), ""), Some(10000)).unwrap();
    let mut second =
        rexpect::spawn(&pty_command(service.0.path(), "--no-stdin"), Some(10000)).unwrap();
    first.exp_string("Attached to repl").unwrap();
    second.exp_string("Attached to repl").unwrap();
    first.send_line("shared-client-marker").unwrap();
    first.exp_string("shared-client-marker").unwrap();
    second.exp_string("shared-client-marker").unwrap();
    first.send("\x10\x11").unwrap();
    first.flush().unwrap();
    assert_eq!(exit_code(&mut first), 0);
    let status = service.command("status").arg("--json").output().unwrap();
    let status: serde_json::Value = serde_json::from_slice(&status.stdout).unwrap();
    assert_eq!(status["repl"]["attachable"], true);
    let logs = service.command("logs").arg("repl").output().unwrap();
    assert!(String::from_utf8_lossy(&logs.stdout).contains("shared-client-marker"));
    second.send_control('c').unwrap();
    assert_eq!(exit_code(&mut second), 0);
}

#[test]
fn hosted_service_exit_reaches_client() {
    let service = RunningService::start("read line; exit 7");
    let mut session = rexpect::spawn(&pty_command(service.0.path(), ""), Some(10000)).unwrap();
    session.exp_string("Attached to repl").unwrap();
    session.send_line("exit").unwrap();
    session.exp_string("repl exited (code 7)").unwrap();
    assert_eq!(exit_code(&mut session), 7);
}

#[test]
fn termination_signal_restores_terminal() {
    let dir = TempDir::new().unwrap();
    let listener = fake_host(dir.path());
    let host = std::thread::spawn(move || {
        let (mut stream, mut reader, _) = hello(listener);
        stream
            .write_all(&encode(&Frame::Output(b"HOST_READY\r\n".to_vec())))
            .unwrap();
        assert!(reader.read_frame().is_err());
    });
    // The shell stays in the same pty to inspect attributes after the client exits.
    let pid_file = dir.path().join("client.pid");
    let script = dir.path().join("terminal-test.sh");
    std::fs::write(
        &script,
        format!(
            r#"before=$(stty -a | sed -E "s/-?pendin//g")
bash -c 'echo $$ > {}; exec {}'
result=$?
test "$before" = "$(stty -a | sed -E "s/-?pendin//g")" && echo TERMINAL_RESTORED
exit $result
"#,
            pid_file.display(),
            pty_command(dir.path(), "")
        ),
    )
    .unwrap();
    let command = format!("bash {}", script.display());
    let mut session = rexpect::spawn(&command, Some(10000)).unwrap();
    session.exp_string("HOST_READY").unwrap();
    let pid: i32 = std::fs::read_to_string(pid_file)
        .unwrap()
        .trim()
        .parse()
        .unwrap();
    nix::sys::signal::kill(
        nix::unistd::Pid::from_raw(pid),
        nix::sys::signal::Signal::SIGTERM,
    )
    .unwrap();
    session.exp_string("TERMINAL_RESTORED").unwrap();
    assert_eq!(exit_code(&mut session), 1);
    host.join().unwrap();
}

#[test]
fn terminal_resize_reaches_host() {
    let dir = TempDir::new().unwrap();
    let listener = fake_host(dir.path());
    let host = std::thread::spawn(move || {
        let (mut stream, mut reader, _) = hello(listener);
        stream
            .write_all(&encode(&Frame::Scrollback(b"HOST_READY\r\n".to_vec())))
            .unwrap();
        assert_eq!(
            reader.read_frame().unwrap(),
            Frame::Resize {
                cols: 103,
                rows: 41
            }
        );
        stream
            .write_all(&encode(&Frame::Exit { status: 0 }))
            .unwrap();
    });
    let mut session = rexpect::spawn(&pty_command(dir.path(), ""), Some(10000)).unwrap();
    session.exp_string("HOST_READY").unwrap();
    use std::os::fd::AsRawFd;
    let dimensions = nix::libc::winsize {
        ws_col: 103,
        ws_row: 41,
        ws_xpixel: 0,
        ws_ypixel: 0,
    };
    // The pty master stays alive, and ioctl reads a correctly sized winsize.
    assert_eq!(
        unsafe {
            nix::libc::ioctl(
                session.process.pty.as_raw_fd(),
                nix::libc::TIOCSWINSZ,
                &dimensions,
            )
        },
        0
    );
    nix::sys::signal::kill(
        nix::unistd::Pid::from_raw(session.process.child_pid.as_raw()),
        nix::sys::signal::Signal::SIGWINCH,
    )
    .unwrap();
    assert_eq!(exit_code(&mut session), 0);
    host.join().unwrap();
}

#[test]
fn old_state_schema_reports_not_attachable() {
    let dir = TempDir::new().unwrap();
    std::fs::create_dir(dir.path().join(".fed")).unwrap();
    let connection = rusqlite::Connection::open(dir.path().join(".fed/lock.db")).unwrap();
    connection.execute_batch("CREATE TABLE services (id TEXT, status TEXT); INSERT INTO services VALUES ('repl', 'running');").unwrap();
    let output = attach(dir.path(), &["--no-stdin"]).output().unwrap();
    assert_eq!(output.status.code(), Some(1));
    assert!(String::from_utf8_lossy(&output.stderr).contains("Service 'repl' is not attachable."));
}

fn flood_client(
    listener: UnixListener,
) -> (std::sync::mpsc::Receiver<()>, std::thread::JoinHandle<()>) {
    let (ready, waiting) = std::sync::mpsc::channel();
    let host = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        stream
            .set_write_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        let mut reader = FrameReader::new(stream.try_clone().unwrap());
        assert!(matches!(reader.read_frame().unwrap(), Frame::Hello { .. }));
        stream
            .write_all(&encode(&Frame::Scrollback(Vec::new())))
            .unwrap();
        let output = encode(&Frame::Output(vec![b'x'; 65536]));
        for index in 0..1024 {
            if stream.write_all(&output).is_err() {
                return;
            }
            if index == 7 {
                ready.send(()).unwrap();
            }
        }
        panic!("all output unexpectedly fit in the unread pipe");
    });
    (waiting, host)
}

#[test]
fn signal_preempts_blocked_stdout_pipe() {
    let dir = TempDir::new().unwrap();
    let listener = fake_host(dir.path());
    let (ready, host) = flood_client(listener);
    let mut child = attach(dir.path(), &["--no-stdin"])
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    ready.recv_timeout(Duration::from_secs(10)).unwrap();
    nix::sys::signal::kill(
        nix::unistd::Pid::from_raw(child.id() as i32),
        nix::sys::signal::Signal::SIGTERM,
    )
    .unwrap();
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        if let Some(status) = child.try_wait().unwrap() {
            assert_eq!(status.code(), Some(1));
            break;
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            panic!("SIGTERM did not interrupt blocked stdout");
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    host.join().unwrap();
}

#[test]
fn signal_restores_raw_terminal_with_blocked_stdout() {
    let dir = TempDir::new().unwrap();
    let listener = fake_host(dir.path());
    let fifo = dir.path().join("output.fifo");
    assert!(
        Command::new("mkfifo")
            .arg(&fifo)
            .status()
            .unwrap()
            .success()
    );
    let _unread_pipe = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(&fifo)
        .unwrap();
    let pid_file = dir.path().join("client.pid");
    let script = dir.path().join("blocked-output.sh");
    std::fs::write(
        &script,
        format!(
            r#"before=$(stty -a | sed -E "s/-?pendin//g")
bash -c 'echo $$ > {}; exec {} > {}'
result=$?
test "$before" = "$(stty -a | sed -E "s/-?pendin//g")" && echo TERMINAL_RESTORED
exit $result
"#,
            pid_file.display(),
            pty_command(dir.path(), ""),
            fifo.display()
        ),
    )
    .unwrap();
    let (ready, host) = flood_client(listener);
    let mut session = rexpect::spawn(&format!("bash {}", script.display()), Some(2000)).unwrap();
    ready.recv_timeout(Duration::from_secs(10)).unwrap();
    let pid: i32 = std::fs::read_to_string(pid_file)
        .unwrap()
        .trim()
        .parse()
        .unwrap();
    nix::sys::signal::kill(
        nix::unistd::Pid::from_raw(pid),
        nix::sys::signal::Signal::SIGTERM,
    )
    .unwrap();
    session.exp_string("TERMINAL_RESTORED").unwrap();
    assert_eq!(exit_code(&mut session), 1);
    host.join().unwrap();
}

#[test]
fn attach_finds_project_state_from_nested_directory_without_parsing_config() {
    let dir = TempDir::new().unwrap();
    let listener = fake_host(dir.path());
    std::fs::write(dir.path().join("fed.yaml"), "invalid: [yaml").unwrap();
    let nested = dir.path().join("deep/nested");
    std::fs::create_dir_all(&nested).unwrap();
    let host = std::thread::spawn(move || {
        let (mut stream, _, _) = hello(listener);
        stream
            .write_all(&encode(&Frame::Exit { status: 0 }))
            .unwrap();
    });
    let output = Command::new(binary())
        .current_dir(nested)
        .args(["attach", "repl", "--no-stdin"])
        .output()
        .unwrap();
    assert!(output.status.success(), "{output:?}");
    host.join().unwrap();
}

#[test]
fn service_exit_waits_for_all_output_to_drain() {
    use std::io::Read;
    let dir = TempDir::new().unwrap();
    let listener = fake_host(dir.path());
    let (sent, ready) = std::sync::mpsc::channel();
    let host = std::thread::spawn(move || {
        let (mut stream, _, _) = hello(listener);
        stream
            .write_all(&encode(&Frame::Scrollback(Vec::new())))
            .unwrap();
        stream
            .write_all(&encode(&Frame::Output(vec![b'x'; 262144])))
            .unwrap();
        stream
            .write_all(&encode(&Frame::Exit { status: 7 << 8 }))
            .unwrap();
        sent.send(()).unwrap();
    });
    let mut child = attach(dir.path(), &["--no-stdin"])
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    ready.recv_timeout(Duration::from_secs(10)).unwrap();
    assert!(child.try_wait().unwrap().is_none());
    let mut output = Vec::new();
    child
        .stdout
        .take()
        .unwrap()
        .read_to_end(&mut output)
        .unwrap();
    assert_eq!(child.wait().unwrap().code(), Some(7));
    assert_eq!(output, vec![b'x'; 262144]);
    host.join().unwrap();
}

#[test]
fn hosted_service_survives_client_termination() {
    let service = RunningService::start("cat");
    let mut session =
        rexpect::spawn(&pty_command(service.0.path(), "--no-stdin"), Some(10000)).unwrap();
    session.exp_string("Attached to repl").unwrap();
    nix::sys::signal::kill(
        nix::unistd::Pid::from_raw(session.process.child_pid.as_raw()),
        nix::sys::signal::Signal::SIGTERM,
    )
    .unwrap();
    assert_eq!(exit_code(&mut session), 1);
    let output = service.command("status").arg("--json").output().unwrap();
    let status: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(status["repl"]["attachable"], true);
}
