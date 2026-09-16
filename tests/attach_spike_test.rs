//! Spike: `fed host` (a pty-owning host process) and `fed attach` (a raw
//! client that proxies bytes over its unix socket).
//!
//! These tests drive the real binary. The client half only means anything
//! under a terminal, so it runs under rexpect's pty. Unix only, like the
//! commands themselves.
#![cfg(unix)]

use std::io::{Read, Write};
use std::os::unix::net::{UnixListener, UnixStream};
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};
use tempfile::TempDir;

fn fed_binary() -> &'static str {
    env!("CARGO_BIN_EXE_fed")
}

fn write_config(dir: &Path, yaml: &str) -> String {
    let path = dir.join("fed.yaml");
    std::fs::File::create(&path)
        .expect("create config")
        .write_all(yaml.as_bytes())
        .expect("write config");
    path.to_str().expect("utf-8 config path").to_string()
}

/// Kills the host by pid whatever happens to the test body: an escaped host
/// would hold a pty and a socket for the rest of the run.
struct HostGuard {
    child: Child,
    socket: PathBuf,
}

impl Drop for HostGuard {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let _ = std::fs::remove_file(&self.socket);
    }
}

impl HostGuard {
    /// `kill -0` would be no use here: the host is this process's child, so
    /// an exited host stays a zombie until it is reaped, and a signal to a
    /// zombie still succeeds.
    fn is_alive(&mut self) -> bool {
        matches!(self.child.try_wait(), Ok(None))
    }
}

/// Start `fed host <service>` detached, the way `fed start` would, and wait
/// for it to be listening.
fn start_host(config: &str, work_dir: &Path, service: &str) -> HostGuard {
    let socket = fed::fed_dir::attach_socket_path(work_dir, service);
    let child = Command::new(fed_binary())
        .args(["-c", config, "host", service])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .process_group(0)
        .spawn()
        .expect("spawn fed host");
    let guard = HostGuard {
        child,
        socket: socket.clone(),
    };
    wait_until(Duration::from_secs(10), || socket.exists())
        .unwrap_or_else(|| panic!("fed host never bound {}", socket.display()));
    guard
}

/// Poll `condition` until it holds or `timeout` expires. Every wait in this
/// file has a deadline; a hung host must fail the test, not the run.
fn wait_until(timeout: Duration, mut condition: impl FnMut() -> bool) -> Option<()> {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if condition() {
            return Some(());
        }
        std::thread::sleep(Duration::from_millis(25));
    }
    None
}

/// `cat` echoes whatever it reads, so a single service proves both
/// directions of the proxy at once.
const ECHO_CONFIG: &str = "services:\n  echo-svc:\n    process: cat\n";

#[test]
fn attach_proxies_both_directions_and_detaches_without_killing_the_service() {
    let dir = TempDir::new().expect("temp dir");
    let config = write_config(dir.path(), ECHO_CONFIG);
    let mut host = start_host(&config, dir.path(), "echo-svc");

    let mut session = rexpect::spawn(
        &format!("{} -c {} attach echo-svc", fed_binary(), config),
        Some(15_000),
    )
    .expect("spawn fed attach under a pty");
    session.exp_string("attached to echo-svc").expect("banner");

    session.send_line("marker-7").expect("send input");
    session.exp_string("marker-7").expect("service output");

    // Ctrl-P Ctrl-Q: detach, leaving the service running.
    session.send("\u{10}\u{11}").expect("send detach keys");
    session.flush().expect("flush detach keys");
    session.exp_eof().expect("client exits on detach");
    match session.process.wait().expect("reap fed attach") {
        rexpect::process::wait::WaitStatus::Exited(_, code) => {
            assert_eq!(code, 0, "detaching is a clean exit");
        }
        other => panic!("expected a normal exit, got {:?}", other),
    }

    assert!(host.is_alive(), "detach must leave the host running");
    let log = dir.path().join(".fed/logs/echo-svc.log");
    let contents = std::fs::read_to_string(&log).expect("read service log");
    assert!(
        contents.contains("marker-7"),
        "host must tee pty output to {}, got: {:?}",
        log.display(),
        contents
    );

    // Ctrl-D at the start of a line is eof for `cat`: the service exits, and
    // the host must follow it out and take its socket with it.
    let socket = fed::fed_dir::attach_socket_path(dir.path(), "echo-svc");
    let mut second = UnixStream::connect(&socket).expect("second attach");
    second.write_all(&[0x04]).expect("send ctrl-d");
    let mut sink = Vec::new();
    let _ = second.read_to_end(&mut sink);

    wait_until(Duration::from_secs(10), || !host.is_alive())
        .expect("host must exit when its service exits");
    assert!(
        !socket.exists(),
        "host must unlink {} on the way out",
        socket.display()
    );
}

#[test]
fn attach_without_a_host_fails_with_the_socket_path() {
    let dir = TempDir::new().expect("temp dir");
    let config = write_config(dir.path(), ECHO_CONFIG);

    let output = Command::new(fed_binary())
        .args(["-c", &config, "attach", "nothing"])
        .output()
        .expect("run fed attach");

    assert_eq!(output.status.code(), Some(1), "connect failure exits 1");
    let stderr = String::from_utf8_lossy(&output.stderr);
    let socket = fed::fed_dir::attach_socket_path(dir.path(), "nothing");
    assert!(
        stderr.contains(&socket.display().to_string()),
        "the message must name the socket path, got: {}",
        stderr
    );
}

/// Question 4, measured rather than guessed: `.fed/attach/<svc>.sock` under
/// a realistically deep workspace does not fit in `sun_path` on macOS, so
/// the path resolver has to fall back to `$TMPDIR`.
#[test]
fn deep_workspace_socket_path_is_measured_not_assumed() {
    let dir = TempDir::new().expect("temp dir");
    let mut deep = dir.path().to_path_buf();
    while deep.as_os_str().len() < 120 {
        deep.push("deep-workspace-segment");
    }
    std::fs::create_dir_all(deep.join(".fed/attach")).expect("create workspace");

    let in_checkout = deep.join(".fed/attach/echo-svc.sock");
    let direct = UnixListener::bind(&in_checkout);
    eprintln!(
        "workspace: {} bytes, .fed socket path: {} bytes, sun_path limit: {} bytes, bind: {:?}",
        deep.as_os_str().len(),
        in_checkout.as_os_str().len(),
        fed::fed_dir::max_socket_path_len(),
        direct.as_ref().map(|_| "ok")
    );
    assert!(
        direct.is_err(),
        "a {}-byte path is not supposed to fit in a {}-byte sun_path",
        in_checkout.as_os_str().len(),
        fed::fed_dir::max_socket_path_len()
    );

    let resolved = fed::fed_dir::attach_socket_path(&deep, "echo-svc");
    assert_ne!(
        resolved, in_checkout,
        "resolver must not pick a path that cannot bind"
    );
    std::fs::create_dir_all(resolved.parent().expect("socket parent")).expect("create socket dir");
    let _ = std::fs::remove_file(&resolved);
    let listener = UnixListener::bind(&resolved);
    eprintln!(
        "fallback socket path: {} ({} bytes), bind: {:?}",
        resolved.display(),
        resolved.as_os_str().len(),
        listener.as_ref().map(|_| "ok")
    );
    assert!(listener.is_ok(), "the fallback path must bind");
    drop(listener);
    let _ = std::fs::remove_file(&resolved);
}
