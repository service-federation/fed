#![cfg(unix)]

use fed::attach::launch::{HostEvent, LaunchSpec};
use fed::attach::protocol::{FLAG_NO_STDIN, Frame, FrameReader, VERSION, encode};
use nix::sys::signal::{Signal, kill, killpg};
use nix::unistd::Pid;
use std::collections::HashMap;
use std::io::{BufRead, BufReader, Write};
use std::os::unix::fs::PermissionsExt;
use std::os::unix::net::{UnixListener, UnixStream};
use std::process::{Child, Command, Stdio};
use std::sync::mpsc::{Receiver, channel};
use std::time::{Duration, Instant};

struct Host {
    child: Child,
    events: Receiver<HostEvent>,
    pid: Option<u32>,
}

impl Host {
    fn spawn(spec: &LaunchSpec) -> Self {
        let mut child = Command::new(env!("CARGO_BIN_EXE_fed"))
            .arg("--workdir")
            .arg(&spec.work_dir)
            .arg("host")
            .arg(&spec.service)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        child
            .stdin
            .take()
            .unwrap()
            .write_all(spec.to_line().unwrap().as_bytes())
            .unwrap();
        let stdout = child.stdout.take().unwrap();
        let (send, events) = channel();
        std::thread::spawn(move || {
            for line in BufReader::new(stdout).lines() {
                let Ok(line) = line else { break };
                if let Ok(event) = HostEvent::from_line(&line) {
                    let _ = send.send(event);
                }
            }
        });
        Self {
            child,
            events,
            pid: None,
        }
    }

    fn event(&self) -> HostEvent {
        self.events
            .recv_timeout(Duration::from_secs(10))
            .expect("host handshake timed out")
    }

    fn ready(&mut self) -> u32 {
        let HostEvent::Ready { pid } = self.event() else {
            panic!("host did not become ready")
        };
        self.pid = Some(pid);
        pid
    }

    fn exited(&mut self) {
        let until = Instant::now() + Duration::from_secs(5);
        loop {
            if self.child.try_wait().unwrap().is_some() {
                return;
            }
            assert!(Instant::now() < until, "host did not exit");
            std::thread::sleep(Duration::from_millis(10));
        }
    }
}

impl Drop for Host {
    fn drop(&mut self) {
        if let Some(pid) = self.pid {
            let _ = killpg(Pid::from_raw(pid as i32), Signal::SIGKILL);
        }
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn fixture(command: &str) -> (tempfile::TempDir, LaunchSpec) {
    let dir = tempfile::Builder::new()
        .prefix("fed-host-")
        .tempdir_in("/tmp")
        .unwrap();
    let root = dir.path().to_path_buf();
    let spec = LaunchSpec {
        service: "repl".into(),
        command: command.into(),
        work_dir: root.clone(),
        cwd: root.clone(),
        environment: HashMap::new(),
        log_path: root.join("logs/repl.log"),
        socket_path: root.join("attach/repl.sock"),
        resources: None,
    };
    (dir, spec)
}

fn connect(spec: &LaunchSpec, flags: u8) -> (UnixStream, FrameReader<UnixStream>) {
    let mut socket = UnixStream::connect(&spec.socket_path).unwrap();
    socket
        .set_read_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    socket
        .write_all(&encode(&Frame::Hello {
            version: VERSION,
            cols: 80,
            rows: 24,
            flags,
        }))
        .unwrap();
    let frames = FrameReader::new(socket.try_clone().unwrap());
    (socket, frames)
}

fn through_exit(frames: &mut FrameReader<UnixStream>) -> (String, i32) {
    let mut output = Vec::new();
    loop {
        match frames.read_frame().unwrap() {
            Frame::Output(bytes) | Frame::Scrollback(bytes) => output.extend(bytes),
            Frame::Exit { status } => {
                return (String::from_utf8_lossy(&output).into_owned(), status);
            }
            other => panic!("unexpected frame: {other:?}"),
        }
    }
}

#[test]
fn host_forwards_input_resize_output_and_exit_and_cleans_up() {
    let (_dir, spec) =
        fixture("read -r line; printf 'received:%s\\n' \"$line\"; stty size; exit 7");
    let mut host = Host::spawn(&spec);
    host.ready();
    assert_eq!(
        std::fs::metadata(&spec.socket_path)
            .unwrap()
            .permissions()
            .mode()
            & 0o777,
        0o600
    );
    assert_eq!(
        std::fs::metadata(spec.socket_path.parent().unwrap())
            .unwrap()
            .permissions()
            .mode()
            & 0o777,
        0o700
    );
    let (mut socket, mut frames) = connect(&spec, 0);
    assert!(matches!(frames.read_frame().unwrap(), Frame::Scrollback(_)));
    socket
        .write_all(&encode(&Frame::Resize {
            cols: 121,
            rows: 43,
        }))
        .unwrap();
    socket
        .write_all(&encode(&Frame::Input(b"hello\n".to_vec())))
        .unwrap();
    let (output, status) = through_exit(&mut frames);
    assert!(output.contains("received:hello"), "{output}");
    assert!(output.contains("43 121"), "{output}");
    assert_eq!(status, 7 << 8);
    assert_eq!(host.event(), HostEvent::Exited { status });
    host.exited();
    assert!(!spec.socket_path.exists());
    assert!(
        std::fs::read_to_string(&spec.log_path)
            .unwrap()
            .contains("received:hello")
    );
}

#[test]
fn immediate_exit_is_reported_on_stdout() {
    let (_dir, spec) = fixture("exit 3");
    let mut host = Host::spawn(&spec);
    host.ready();
    assert_eq!(host.event(), HostEvent::Exited { status: 3 << 8 });
    host.exited();
    assert!(!spec.socket_path.exists());
}

#[test]
fn live_host_rejects_a_second_host_and_recovers_stale_sockets() {
    let (_dir, spec) = fixture("cat");
    std::fs::create_dir_all(spec.socket_path.parent().unwrap()).unwrap();
    drop(UnixListener::bind(&spec.socket_path).unwrap());
    let mut first = Host::spawn(&spec);
    first.ready();
    let mut second = Host::spawn(&spec);
    assert!(matches!(second.event(), HostEvent::Failed { .. }));
    second.exited();
    let (mut socket, mut frames) = connect(&spec, 0);
    assert!(matches!(frames.read_frame().unwrap(), Frame::Scrollback(_)));
    socket.write_all(&encode(&Frame::Input(vec![4]))).unwrap();
    assert_eq!(through_exit(&mut frames).1, 0);
    first.exited();
}

#[test]
fn host_does_not_replace_regular_files() {
    let (_dir, spec) = fixture("cat");
    std::fs::create_dir_all(spec.socket_path.parent().unwrap()).unwrap();
    std::fs::write(&spec.socket_path, "keep").unwrap();
    let mut host = Host::spawn(&spec);
    assert!(matches!(host.event(), HostEvent::Failed { .. }));
    host.exited();
    assert_eq!(std::fs::read_to_string(&spec.socket_path).unwrap(), "keep");
}

#[test]
fn no_stdin_client_cannot_type_and_protocol_mismatch_is_rejected() {
    let (_dir, spec) = fixture("read -r line; printf 'received:%s\\n' \"$line\"");
    let mut host = Host::spawn(&spec);
    host.ready();
    let mut bad = UnixStream::connect(&spec.socket_path).unwrap();
    bad.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    bad.write_all(&encode(&Frame::Hello {
        version: VERSION + 1,
        cols: 80,
        rows: 24,
        flags: 0,
    }))
    .unwrap();
    assert!(matches!(
        FrameReader::new(bad).read_frame().unwrap(),
        Frame::Error(_)
    ));
    let (mut readonly, mut read_frames) = connect(&spec, FLAG_NO_STDIN);
    assert!(matches!(
        read_frames.read_frame().unwrap(),
        Frame::Scrollback(_)
    ));
    readonly
        .write_all(&encode(&Frame::Input(b"forbidden\n".to_vec())))
        .unwrap();
    let (mut writer, mut frames) = connect(&spec, 0);
    assert!(matches!(frames.read_frame().unwrap(), Frame::Scrollback(_)));
    writer
        .write_all(&encode(&Frame::Input(b"allowed\n".to_vec())))
        .unwrap();
    let (output, _) = through_exit(&mut frames);
    assert!(output.contains("received:allowed"), "{output}");
    assert!(!output.contains("forbidden"), "{output}");
    host.exited();
}

#[test]
fn nonreading_client_cannot_stall_logging_or_another_client() {
    let (_dir, spec) =
        fixture("read -r go; head -c 4194304 /dev/zero; echo finished; read -r stop");
    let mut host = Host::spawn(&spec);
    host.ready();
    let (_slow, mut slow_frames) = connect(&spec, 0);
    assert!(matches!(
        slow_frames.read_frame().unwrap(),
        Frame::Scrollback(_)
    ));
    let (mut socket, mut frames) = connect(&spec, 0);
    assert!(matches!(frames.read_frame().unwrap(), Frame::Scrollback(_)));
    socket
        .write_all(&encode(&Frame::Input(b"go\n".to_vec())))
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(4);
    let mut seen = Vec::new();
    while !seen.ends_with(b"finished\r\n") {
        let Frame::Output(bytes) = frames.read_frame().unwrap() else {
            panic!("expected output")
        };
        seen.extend(bytes);
        assert!(Instant::now() < deadline, "a slow peer stalled output");
    }
    assert!(std::fs::metadata(&spec.log_path).unwrap().len() >= 4194304);
    socket
        .write_all(&encode(&Frame::Input(b"stop\n".to_vec())))
        .unwrap();
    through_exit(&mut frames);
    host.exited();
}

#[test]
fn child_receives_hangup_even_though_host_ignores_it() {
    let (_dir, spec) = fixture("exec cat");
    let mut host = Host::spawn(&spec);
    let pid = host.ready();
    kill(Pid::from_raw(host.child.id() as i32), Signal::SIGHUP).unwrap();
    let (_socket, mut frames) = connect(&spec, 0);
    assert!(matches!(frames.read_frame().unwrap(), Frame::Scrollback(_)));
    kill(Pid::from_raw(pid as i32), Signal::SIGHUP).unwrap();
    assert_eq!(through_exit(&mut frames).1, Signal::SIGHUP as i32);
    host.exited();
}

#[test]
fn bare_terminal_invocation_fails_without_waiting_for_input() {
    let (_dir, spec) = fixture("unused");
    let pty = nix::pty::openpty(None, None).unwrap();
    let child = Command::new(env!("CARGO_BIN_EXE_fed"))
        .arg("--workdir")
        .arg(&spec.work_dir)
        .arg("host")
        .arg("repl")
        .stdin(Stdio::from(pty.slave))
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let (_send, events) = channel();
    let mut host = Host {
        child,
        events,
        pid: None,
    };
    host.exited();
    let output = host.child.stderr.take().unwrap();
    let message = std::io::read_to_string(output).unwrap();
    assert!(
        message.contains("fed host is started by fed start and reads its launch spec from stdin."),
        "{message}"
    );
    assert!(!host.child.wait().unwrap().success());
}

#[test]
fn terminating_the_host_reaps_a_service_that_ignores_term_and_hup() {
    let (_dir, spec) = fixture("trap '' TERM HUP; echo armed; while :; do sleep 1; done");
    let mut host = Host::spawn(&spec);
    let pid = host.ready();
    let (_socket, mut frames) = connect(&spec, 0);
    let mut output = Vec::new();
    while !String::from_utf8_lossy(&output).contains("armed") {
        match frames.read_frame().unwrap() {
            Frame::Scrollback(bytes) | Frame::Output(bytes) => output.extend(bytes),
            other => panic!("{other:?}"),
        }
    }
    kill(Pid::from_raw(host.child.id() as i32), Signal::SIGTERM).unwrap();
    assert_eq!(through_exit(&mut frames).1, Signal::SIGKILL as i32);
    host.exited();
    assert!(kill(Pid::from_raw(pid as i32), None).is_err());
    assert!(!spec.socket_path.exists());
}

#[test]
fn competing_starts_cannot_both_own_a_stale_socket() {
    let (_dir, spec) = fixture("cat");
    std::fs::create_dir_all(spec.socket_path.parent().unwrap()).unwrap();
    drop(UnixListener::bind(&spec.socket_path).unwrap());
    let mut first = Host::spawn(&spec);
    let mut second = Host::spawn(&spec);
    match (first.event(), second.event()) {
        (HostEvent::Ready { pid }, HostEvent::Failed { .. }) => first.pid = Some(pid),
        (HostEvent::Failed { .. }, HostEvent::Ready { pid }) => second.pid = Some(pid),
        pair => panic!("expected exactly one owner: {pair:?}"),
    }
    let (mut socket, mut frames) = connect(&spec, 0);
    assert!(matches!(frames.read_frame().unwrap(), Frame::Scrollback(_)));
    socket.write_all(&encode(&Frame::Input(vec![4]))).unwrap();
    through_exit(&mut frames);
    first.exited();
    second.exited();
}

#[test]
fn reconnect_receives_scrollback_and_disconnect_leaves_service_running() {
    let (_dir, spec) = fixture("cat");
    let mut host = Host::spawn(&spec);
    host.ready();
    {
        let (mut socket, mut frames) = connect(&spec, 0);
        assert!(matches!(frames.read_frame().unwrap(), Frame::Scrollback(_)));
        socket
            .write_all(&encode(&Frame::Input(b"remember-me\n".to_vec())))
            .unwrap();
        let Frame::Output(bytes) = frames.read_frame().unwrap() else {
            panic!("expected output")
        };
        assert!(String::from_utf8_lossy(&bytes).contains("remember-me"));
    }
    let (mut socket, mut frames) = connect(&spec, 0);
    let Frame::Scrollback(bytes) = frames.read_frame().unwrap() else {
        panic!("expected scrollback")
    };
    assert!(String::from_utf8_lossy(&bytes).contains("remember-me"));
    socket.write_all(&encode(&Frame::Input(vec![4]))).unwrap();
    assert_eq!(through_exit(&mut frames).1, 0);
    host.exited();
}

#[test]
fn blocked_service_input_does_not_block_output_or_exit() {
    let (_dir, spec) = fixture("stty raw -echo; echo ready; sleep 1; echo still-alive");
    let mut host = Host::spawn(&spec);
    host.ready();
    let (mut socket, mut frames) = connect(&spec, 0);
    let mut output = Vec::new();
    while !String::from_utf8_lossy(&output).contains("ready") {
        match frames.read_frame().unwrap() {
            Frame::Scrollback(bytes) | Frame::Output(bytes) => output.extend(bytes),
            other => panic!("{other:?}"),
        }
    }
    socket
        .write_all(&encode(&Frame::Input(vec![b'x'; 1024 * 1024])))
        .unwrap();
    let (output, status) = through_exit(&mut frames);
    assert!(output.contains("still-alive"), "{output}");
    assert_eq!(status, 0);
    host.exited();
}

#[cfg(target_os = "linux")]
#[test]
fn a_failed_log_sink_reports_once_and_keeps_client_output_working() {
    let (_dir, mut spec) = fixture("read -r line; echo first; sleep 0.1; echo second");
    spec.log_path = "/dev/full".into();
    let mut host = Host::spawn(&spec);
    host.ready();
    let (mut socket, mut frames) = connect(&spec, 0);
    assert!(matches!(frames.read_frame().unwrap(), Frame::Scrollback(_)));
    socket
        .write_all(&encode(&Frame::Input(b"go\n".to_vec())))
        .unwrap();
    let (output, status) = through_exit(&mut frames);
    assert!(
        output.contains("first") && output.contains("second"),
        "{output}"
    );
    assert_eq!(status, 0);
    host.exited();
    let diagnostic =
        std::fs::read_to_string(spec.work_dir.join(".fed/logs/repl-host.log")).unwrap();
    assert_eq!(
        diagnostic.matches("writing the service log failed").count(),
        1
    );
}

#[test]
fn stdout_exit_event_does_not_wait_for_a_descendant_holding_the_terminal() {
    let (_dir, spec) = fixture("trap '' HUP; sleep 30 & exit 9");
    let mut host = Host::spawn(&spec);
    host.ready();
    assert_eq!(
        host.events.recv_timeout(Duration::from_secs(1)).unwrap(),
        HostEvent::Exited { status: 9 << 8 }
    );
    host.exited();
}
