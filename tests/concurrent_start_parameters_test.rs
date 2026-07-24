//! End-to-end coverage for cross-process parameter agreement during `fed start`.
//!
//! The first start brings up a dependency on the configured default port, then
//! pauses in the dependent's migrate hook. The second start begins during that
//! pause. It must wait and rebuild from the first start's committed state,
//! rather than independently reallocating the now-occupied dependency port.

use std::fs;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::process::{Child, Command, Output, Stdio};
use std::time::{Duration, Instant};

#[path = "support/mod.rs"]
mod support;

fn fed_binary() -> String {
    env!("CARGO_BIN_EXE_fed").to_string()
}

fn shell_double_quote(value: &str) -> String {
    format!(
        "\"{}\"",
        value
            .replace('\\', "\\\\")
            .replace('"', "\\\"")
            .replace('$', "\\$")
            .replace('`', "\\`")
    )
}

fn spawn_start(config_path: &std::path::Path, work_dir: &std::path::Path) -> Child {
    Command::new(fed_binary())
        .args([
            "-c",
            config_path.to_str().unwrap(),
            "-w",
            work_dir.to_str().unwrap(),
            "start",
            "app",
        ])
        .env("FED_NON_INTERACTIVE", "1")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn fed start")
}

fn wait_output(child: Child) -> Output {
    child.wait_with_output().expect("wait for fed start")
}

fn wait_until(path: &std::path::Path, timeout: Duration) {
    let deadline = Instant::now() + timeout;
    while !path.exists() && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(25));
    }
    assert!(
        path.exists(),
        "timed out waiting for startup gate {}",
        path.display()
    );
}

fn stop(config_path: &std::path::Path, work_dir: &std::path::Path) {
    let _ = Command::new(fed_binary())
        .args([
            "-c",
            config_path.to_str().unwrap(),
            "-w",
            work_dir.to_str().unwrap(),
            "stop",
        ])
        .env("FED_NON_INTERACTIVE", "1")
        .output();
}

struct Cleanup<'a> {
    config_path: &'a std::path::Path,
    work_dir: &'a std::path::Path,
}

impl Drop for Cleanup<'_> {
    fn drop(&mut self) {
        stop(self.config_path, self.work_dir);
    }
}

#[test]
fn concurrent_start_listener_helper() {
    if std::env::var_os("FED_CONCURRENT_START_LISTENER").is_none() {
        return;
    }

    let port: u16 = std::env::var("PORT")
        .expect("PORT")
        .parse()
        .expect("numeric PORT");
    let listener = TcpListener::bind(("127.0.0.1", port)).expect("bind helper listener");

    for stream in listener.incoming() {
        let mut stream = stream.expect("accept helper connection");
        let mut request = [0_u8; 256];
        let _ = stream.read(&mut request);
        let _ = stream.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok");
    }
}

#[test]
fn simultaneous_starts_use_one_committed_port_set() {
    let temp_dir = tempfile::tempdir().expect("temp workspace");
    let work_dir = temp_dir.path();
    let config_path = work_dir.join("fed.yaml");
    let migrate_gate = work_dir.join("migrate-started");

    let db_holder = TcpListener::bind("127.0.0.1:0").expect("reserve DB port");
    let app_holder = TcpListener::bind("127.0.0.1:0").expect("reserve app port");
    let db_port = db_holder.local_addr().unwrap().port();
    let app_port = app_holder.local_addr().unwrap().port();
    drop((db_holder, app_holder));

    let helper = shell_double_quote(
        std::env::current_exe()
            .expect("current test executable")
            .to_str()
            .expect("UTF-8 test executable path"),
    );
    let config = format!(
        r#"
parameters:
  DB_PORT:
    type: port
    default: {db_port}
  APP_PORT:
    type: port
    default: {app_port}

services:
  db:
    process: '{helper} --exact concurrent_start_listener_helper --nocapture'
    environment:
      FED_CONCURRENT_START_LISTENER: "1"
      PORT: "{{{{DB_PORT}}}}"
    startup_message: "db=http://127.0.0.1:{{{{DB_PORT}}}}"
  app:
    process: '{helper} --exact concurrent_start_listener_helper --nocapture'
    environment:
      FED_CONCURRENT_START_LISTENER: "1"
      PORT: "{{{{APP_PORT}}}}"
      DATABASE_URL: "http://127.0.0.1:{{{{DB_PORT}}}}"
    depends_on:
      - db
    migrate: 'touch {migrate_gate} && sleep 2'
    startup_message: "app=http://127.0.0.1:{{{{APP_PORT}}}} db=http://127.0.0.1:{{{{DB_PORT}}}}"
"#,
        helper = helper.replace('\'', "'\"'\"'"),
        migrate_gate = shell_double_quote(migrate_gate.to_str().unwrap()),
    );
    support::parse_checked(&config);
    fs::write(&config_path, config).expect("write config");
    let _cleanup = Cleanup {
        config_path: &config_path,
        work_dir,
    };

    let first = spawn_start(&config_path, work_dir);
    wait_until(&migrate_gate, Duration::from_secs(15));
    assert!(
        TcpStream::connect(("127.0.0.1", db_port)).is_ok(),
        "the first start's dependency must be listening before the race"
    );

    let second = spawn_start(&config_path, work_dir);
    let first_output = wait_output(first);
    let second_output = wait_output(second);

    for (label, output) in [("first", first_output), ("second", second_output)] {
        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(
            output.status.success(),
            "{label} start failed\nstdout:\n{stdout}\nstderr:\n{stderr}"
        );
        assert!(
            stdout.contains(&format!("db=http://127.0.0.1:{db_port}")),
            "{label} start advertised a divergent DB port\nstdout:\n{stdout}\nstderr:\n{stderr}"
        );
        assert!(
            stdout.contains(&format!("app=http://127.0.0.1:{app_port}")),
            "{label} start advertised a divergent app port\nstdout:\n{stdout}\nstderr:\n{stderr}"
        );
    }

    let conn = rusqlite::Connection::open(work_dir.join(".fed/lock.db")).unwrap();
    let persisted: std::collections::HashMap<String, u16> = conn
        .prepare("SELECT param_name, port FROM persisted_ports WHERE isolation_id = ''")
        .unwrap()
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
        .unwrap()
        .map(Result::unwrap)
        .collect();
    assert_eq!(persisted.get("DB_PORT"), Some(&db_port));
    assert_eq!(persisted.get("APP_PORT"), Some(&app_port));
    assert!(TcpStream::connect(("127.0.0.1", db_port)).is_ok());
    assert!(TcpStream::connect(("127.0.0.1", app_port)).is_ok());
}
