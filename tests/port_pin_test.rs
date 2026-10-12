//! `fed start --pin` and `fed ports resolve`, which `fed remote start` runs on
//! a remote environment, against the real binary and a real stack.

use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::time::{Duration, Instant};

fn fed(dir: &Path, args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_fed"))
        .arg("-w")
        .arg(dir)
        .arg("-c")
        .arg(dir.join("fed.yaml"))
        .args(args)
        .env("FED_NON_INTERACTIVE", "1")
        .output()
        .unwrap()
}

fn text(output: &Output) -> String {
    format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    )
}

fn free_port() -> u16 {
    std::net::TcpListener::bind("0.0.0.0:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

/// A stack whose `web` writes the port it got to `port.txt`, and whose
/// `other` uses no port and counts its starts in `other.txt`. Stopped when
/// dropped.
struct Stack {
    dir: tempfile::TempDir,
    default: u16,
}

impl Stack {
    fn new() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let default = free_port();
        std::fs::write(
            dir.path().join("fed.yaml"),
            format!(
                r#"
entrypoint: web
parameters:
  WEB_PORT:
    type: port
    default: {default}
services:
  web:
    process: "echo {{{{WEB_PORT}}}} > port.txt; exec sleep 300"
    depends_on: [other]
  other:
    process: "echo started >> other.txt; exec sleep 300"
"#
            ),
        )
        .unwrap();
        Self { dir, default }
    }

    fn path(&self) -> PathBuf {
        self.dir.path().to_path_buf()
    }

    fn fed(&self, args: &[&str]) -> Output {
        fed(self.dir.path(), args)
    }

    /// The port in `port.txt`, once `web` has written `expected`.
    fn web_port(&self, expected: u16) -> u16 {
        let path = self.dir.path().join("port.txt");
        let start = Instant::now();
        loop {
            let port = std::fs::read_to_string(&path)
                .ok()
                .and_then(|t| t.trim().parse().ok());
            if port == Some(expected) || start.elapsed() > Duration::from_secs(10) {
                return port.unwrap_or(0);
            }
            std::thread::sleep(Duration::from_millis(100));
        }
    }

    fn other_starts(&self) -> usize {
        std::fs::read_to_string(self.dir.path().join("other.txt"))
            .unwrap_or_default()
            .lines()
            .count()
    }

    fn saved(&self) -> serde_json::Value {
        let output = self.fed(&["ports", "list", "--json"]);
        serde_json::from_slice(&output.stdout).unwrap()
    }

    fn resolved(&self) -> serde_json::Value {
        let output = self.fed(&["ports", "resolve", "--json"]);
        assert!(output.status.success(), "{}", text(&output));
        let stdout = String::from_utf8_lossy(&output.stdout);
        serde_json::from_str(stdout.lines().last().unwrap()).unwrap()
    }
}

impl Drop for Stack {
    fn drop(&mut self) {
        let _ = self.fed(&["stop"]);
    }
}

/// A pin moves only the services that use the parameter, and saves the new
/// port. A pin that another program holds fails with exit status 75 and
/// changes nothing: the old port stays saved and `web` keeps running on it.
#[test]
fn a_pin_moves_its_services_and_a_taken_pin_changes_nothing() {
    let stack = Stack::new();
    let output = stack.fed(&["start"]);
    assert!(output.status.success(), "{}", text(&output));
    assert_eq!(stack.web_port(stack.default), stack.default);

    let saved = stack.saved();
    assert_eq!(
        stack.resolved()["WEB_PORT"],
        serde_json::json!({"port": stack.default, "state": "kept"})
    );
    assert_eq!(stack.saved(), saved, "fed ports resolve saved something");

    let pin = free_port();
    let output = stack.fed(&["start", "--pin", &format!("WEB_PORT={pin}")]);
    assert!(output.status.success(), "{}", text(&output));
    assert_eq!(stack.web_port(pin), pin);
    assert_eq!(stack.saved()["WEB_PORT"], pin);
    assert_eq!(stack.other_starts(), 1, "other does not use WEB_PORT");

    let held = std::net::TcpListener::bind("0.0.0.0:0").unwrap();
    let taken = held.local_addr().unwrap().port();
    let output = stack.fed(&["start", "--pin", &format!("WEB_PORT={taken}")]);
    assert_eq!(output.status.code(), Some(75), "{}", text(&output));
    assert!(
        text(&output).contains(&format!(
            "WEB_PORT is pinned to port {taken}, but another program uses that port."
        )),
        "{}",
        text(&output)
    );
    assert_eq!(stack.saved()["WEB_PORT"], pin);
    assert_eq!(stack.web_port(pin), pin);
    let status = stack.fed(&["status"]);
    assert!(text(&status).contains("web"), "{}", text(&status));
}

/// A saved pin that another program took fails the next start. fed does not
/// move it to a random port or back to the config default, the port it moved
/// away from. Once the program is gone, the pin applies again.
#[test]
fn a_saved_pin_another_program_took_fails_the_start() {
    let stack = Stack::new();
    let pin = free_port();
    let output = stack.fed(&["start", "--pin", &format!("WEB_PORT={pin}")]);
    assert!(output.status.success(), "{}", text(&output));
    assert_eq!(stack.web_port(pin), pin);
    let output = stack.fed(&["stop"]);
    assert!(output.status.success(), "{}", text(&output));

    let held = std::net::TcpListener::bind(("0.0.0.0", pin)).unwrap();
    assert_eq!(
        stack.resolved()["WEB_PORT"],
        serde_json::json!({"port": pin, "state": "taken"})
    );
    let output = stack.fed(&["start"]);
    assert_eq!(output.status.code(), Some(75), "{}", text(&output));
    assert_eq!(stack.saved()["WEB_PORT"], pin);

    drop(held);
    let output = stack.fed(&["start"]);
    assert!(output.status.success(), "{}", text(&output));
    assert_eq!(stack.web_port(pin), pin, "the free default must not win");
}

/// Two `fed start --pin` runs at once take turns under the start lock. The
/// port `web` runs on is the one that stays saved.
#[test]
fn two_pins_at_once_leave_the_saved_port_and_the_running_one_equal() {
    let stack = Stack::new();
    let output = stack.fed(&["start"]);
    assert!(output.status.success(), "{}", text(&output));
    assert_eq!(stack.web_port(stack.default), stack.default);

    let (a, b) = (free_port(), free_port());
    let runs: Vec<_> = [a, b]
        .into_iter()
        .map(|port| {
            let dir = stack.path();
            std::thread::spawn(move || fed(&dir, &["start", "--pin", &format!("WEB_PORT={port}")]))
        })
        .collect();
    for run in runs {
        let output = run.join().unwrap();
        assert!(output.status.success(), "{}", text(&output));
    }
    let saved = stack.saved()["WEB_PORT"].as_u64().unwrap() as u16;
    assert!(saved == a || saved == b, "{saved}");
    assert_eq!(stack.web_port(saved), saved);
}
