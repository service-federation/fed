//! `fed remote` exists only in builds with the `remote-beta` feature. Without
//! it, the help text has no trace of it. With it, the commands run against a
//! local mock of the cloud API and fake `ssh` and `rsync` programs on PATH:
//! no network, no machines.

use std::process::Command;

/// Rust on macOS creates a socket and marks it close-on-exec in two steps.
/// A child spawned between the two inherits the socket and holds its port,
/// so another test finds that port taken. Every child spawn and every socket
/// the tests open hold this lock.
static SPAWN: std::sync::Mutex<()> = std::sync::Mutex::new(());

fn spawn_lock() -> std::sync::MutexGuard<'static, ()> {
    SPAWN.lock().unwrap_or_else(|e| e.into_inner())
}

fn fed_binary() -> &'static str {
    env!("CARGO_BIN_EXE_fed")
}

fn help_text(args: &[&str]) -> String {
    let _spawning = spawn_lock();
    let output = Command::new(fed_binary()).args(args).output().unwrap();
    format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    )
}

#[cfg(not(feature = "remote-beta"))]
#[test]
fn help_has_no_remote_without_the_feature() {
    let help = help_text(&["--help"]);
    assert!(help.contains("Commands:"), "{help}");
    assert!(!help.to_lowercase().contains("remote"), "{help}");
    let help = help_text(&["help", "remote"]);
    assert!(!help.contains("fed remote up"), "{help}");
    assert!(!help.contains("Remote environments"), "{help}");
}

#[cfg(feature = "remote-beta")]
#[test]
fn help_lists_remote_with_the_feature() {
    let help = help_text(&["--help"]);
    assert!(help.contains("remote"), "{help}");
    let help = help_text(&["remote", "--help"]);
    for command in ["up", "ls", "push", "start", "connect", "ssh", "down"] {
        assert!(help.contains(&format!("  {command} ")), "{command}: {help}");
    }
    assert!(!help.to_lowercase().contains("beta"), "{help}");
}

#[cfg(all(unix, feature = "remote-beta"))]
mod with_feature {
    use super::*;
    use std::os::unix::fs::PermissionsExt;
    use std::path::{Path, PathBuf};
    use std::sync::{Arc, Mutex};
    use tempfile::TempDir;

    /// A mock of the cloud API. Each request is answered by the first route
    /// whose prefix matches `METHOD /path`, and kept for the test to check.
    struct Cloud {
        url: String,
        requests: Arc<Mutex<Vec<String>>>,
    }

    impl Cloud {
        fn start(routes: Vec<(&'static str, &'static str, String)>) -> Self {
            let listener = {
                let _spawning = spawn_lock();
                std::net::TcpListener::bind("127.0.0.1:0").unwrap()
            };
            let url = format!("http://{}", listener.local_addr().unwrap());
            let requests = Arc::new(Mutex::new(Vec::new()));
            let seen = requests.clone();
            std::thread::spawn(move || {
                for stream in listener.incoming() {
                    let Ok(mut stream) = stream else { return };
                    let request = read_request(&mut stream);
                    let route = routes
                        .iter()
                        .find(|(prefix, _, _)| request.starts_with(prefix));
                    let (status, body) = match route {
                        Some((_, status, body)) => (*status, body.clone()),
                        None => ("404 Not Found", r#"{"error":"route"}"#.to_string()),
                    };
                    seen.lock().unwrap().push(request);
                    if status == HANG {
                        // Never answers. The connection stays open until the
                        // test process ends.
                        std::mem::forget(stream);
                        continue;
                    }
                    use std::io::Write;
                    let resp = format!(
                        "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                        body.len()
                    );
                    let _ = stream.write_all(resp.as_bytes());
                }
            });
            Self { url, requests }
        }

        fn requests(&self) -> Vec<String> {
            self.requests.lock().unwrap().clone()
        }

        fn request(&self, prefix: &str) -> Option<String> {
            self.requests().into_iter().find(|r| r.starts_with(prefix))
        }
    }

    fn read_request(stream: &mut std::net::TcpStream) -> String {
        use std::io::Read;
        let mut buf = Vec::new();
        let mut chunk = [0u8; 4096];
        loop {
            if let Some(end) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                let head = String::from_utf8_lossy(&buf[..end]).to_ascii_lowercase();
                let length = head
                    .lines()
                    .find_map(|l| l.strip_prefix("content-length:"))
                    .and_then(|v| v.trim().parse::<usize>().ok())
                    .unwrap_or(0);
                if buf.len() >= end + 4 + length {
                    break;
                }
            }
            match stream.read(&mut chunk) {
                Ok(0) | Err(_) => break,
                Ok(n) => buf.extend_from_slice(&chunk[..n]),
            }
        }
        String::from_utf8_lossy(&buf).to_string()
    }

    fn json_body(request: &str) -> serde_json::Value {
        serde_json::from_str(request.split_once("\r\n\r\n").unwrap().1).unwrap()
    }

    /// Logs every call as `name [arg] [arg] ...`, one line each, written at
    /// once so that parallel calls do not mix. Keeps each command's stdin in
    /// `<log>.stdin.<pid>`. `FAKE_SSH=dead` makes every connection fail as
    /// for a deleted machine, and `FAKE_SSH=stale` for a master that has not
    /// noticed yet. `FAKE_CLOUD_INIT` is the exit status of the setup wait
    /// in `up`. `FAKE_PORTS` is what the machine's `fed ports list --json`
    /// prints.
    ///
    /// A call with `-L` is the tunnel. It writes its pid to
    /// `<log>.forward.<pid>`, waits `FAKE_FORWARD_DELAY` seconds, prints
    /// `FAKE_SAY` to stderr, and then exits 255. `FAKE_FORWARD=hold` makes it
    /// listen on every `-L` port instead, through `FAKE_LISTEN`, and
    /// `FAKE_FORWARD=never` makes it stay up without listening.
    const FAKE_SSH: &str = r#"#!/bin/sh
line=ssh; for a in "$@"; do line="$line [$a]"; done
printf '%s\n' "$line" >> "$FAKE_LOG"
if [ "$1" = -V ]; then echo "OpenSSH_9.8p1" >&2; exit 0; fi
# Everything but the last argument: options and the target, or for a command,
# options only.
opts=""; last=""
for a in "$@"; do opts="$opts $last"; last="$a"; done
if [ "$FAKE_SSH" = dead ]; then
  echo "ssh: connect to host 203.0.113.7 port 23456: Operation timed out" >&2
  exit 255
fi
if [ "$FAKE_SSH" = stale ]; then
  # A master that has not noticed its machine is gone: the check passes,
  # every session fails.
  case "$opts " in *" -O "*) exit 0 ;; esac
  echo "mux_client_request_session: read from master failed: Broken pipe" >&2
  echo "ssh: connect to host 203.0.113.7 port 23456: Operation timed out" >&2
  exit 255
fi
case "$opts " in
  *" -O check "*) exit 1 ;;
  *" -O exit "*) exit 0 ;;
  *" -f -N "*) exit 0 ;;
  *" -L "*)
    specs=""; prev=""
    for a in "$@"; do
      if [ "$prev" = -L ]; then specs="$specs $a"; fi
      prev="$a"
    done
    echo $$ > "$FAKE_LOG.forward.$$"
    sleep "${FAKE_FORWARD_DELAY:-0}"
    if [ -n "$FAKE_SAY" ]; then printf '%s\n' "$FAKE_SAY" >&2; fi
    case "$FAKE_FORWARD" in
      hold) exec python3 "$(dirname "$0")/fake-listen.py" $specs ;;
      never) exec sleep 600 ;;
    esac
    exit 255 ;;
esac
case "$last" in
  "fed --version") echo "fed ${FAKE_VM_FED:-999.0.0}" ;;
  "cloud-init status --wait >/dev/null") exit "${FAKE_CLOUD_INIT:-0}" ;;
  "cloud-init status --long")
    printf 'status: error\nerrors:\n\t- (%s)\nrecoverable_errors: {}\n' "'init', RuntimeError('disk full')" ;;
  *"fed ports list --json"*) echo "$FAKE_PORTS" ;;
  *) cat > "$FAKE_LOG.stdin.$$" ;;
esac
exit 0
"#;

    /// How many seconds a fake tunnel stays up before it exits, in tests that
    /// need "Connected." first. fed checks who listens with `lsof`, which
    /// takes seconds when many tests run it at once.
    const UP_FOR: &str = "10";

    /// The listening part of the fake tunnel. It binds each `-L` it receives
    /// as OpenSSH does: the address given, or 127.0.0.1 and ::1 when the
    /// forward has none. A forward fails only when no address binds. Then it
    /// prints OpenSSH's lines and exits 255, as with ExitOnForwardFailure.
    /// Otherwise it accepts and closes connections for at most 600 seconds.
    /// `FAKE_SKIP_IPV6` makes it leave out the forwards from [::1], as if they
    /// were never requested. `FAKE_EXIT_AFTER` makes it exit 255 after that
    /// many seconds, or kill
    /// itself with the signal number in `FAKE_EXIT_SIGNAL`. After one second,
    /// `FAKE_STDERR_KB` makes it write that many 1 KB lines to stderr and
    /// then create `<log>.noise-done`. A stderr that nobody reads blocks it
    /// before that file appears.
    const FAKE_LISTEN: &str = r#"import os, select, signal, socket, sys, time

def addresses(spec):
    if spec.startswith("["):
        host, rest = spec[1:].split("]:", 1)
        return [host], int(rest.split(":")[0])
    parts = spec.split(":")
    if len(parts) == 4:
        return [parts[0]], int(parts[1])
    return ["127.0.0.1", "::1"], int(parts[0])

socks = []
for spec in sys.argv[1:]:
    if os.environ.get("FAKE_SKIP_IPV6") and spec.startswith("[::1]"):
        continue
    hosts, port = addresses(spec)
    bound = 0
    for host in hosts:
        family = socket.AF_INET6 if ":" in host else socket.AF_INET
        s = socket.socket(family, socket.SOCK_STREAM)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        if family == socket.AF_INET6:
            s.setsockopt(socket.IPPROTO_IPV6, socket.IPV6_V6ONLY, 1)
        try:
            s.bind((host, port))
            s.listen(64)
        except OSError as e:
            sys.stderr.write("bind [%s]:%d: %s\n" % (host, port, e.strerror))
            s.close()
            continue
        socks.append(s)
        bound += 1
    if not bound:
        sys.stderr.write("channel_setup_fwd_listener_tcpip: cannot listen to port: %d\n" % port)
        sys.stderr.write("Could not request local forwarding.\n")
        sys.stderr.flush()
        sys.exit(255)
sys.stderr.flush()
start = time.time()
exit_after = float(os.environ.get("FAKE_EXIT_AFTER", "0"))
exit_signal = int(os.environ.get("FAKE_EXIT_SIGNAL", "0"))
noise = int(os.environ.get("FAKE_STDERR_KB", "0"))
while time.time() - start < 600:
    if exit_after and time.time() - start >= exit_after:
        if exit_signal:
            os.kill(os.getpid(), exit_signal)
        sys.exit(255)
    if noise and time.time() - start >= 1:
        line = "debug1: channel 3: noise " + "x" * 999 + "\n"
        for _ in range(noise):
            sys.stderr.write(line)
        sys.stderr.flush()
        open(os.environ["FAKE_LOG"] + ".noise-done", "w").close()
        noise = 0
    ready, _, _ = select.select(socks, [], [], 0.1)
    for s in ready:
        conn, _ = s.accept()
        conn.close()
"#;

    /// The real `lsof`, slowed down by `FAKE_LSOF_DELAY` seconds. fed checks
    /// who listens with it, and it takes seconds on a busy machine. Each
    /// call is logged as `lsof <args>`.
    const FAKE_LSOF: &str = r#"#!/bin/sh
if [ -n "$FAKE_LOG" ]; then printf 'lsof %s\n' "$*" >> "$FAKE_LOG"; fi
sleep "${FAKE_LSOF_DELAY:-0}"
exec "REAL_LSOF" "$@"
"#;

    /// Where the real `lsof` is on this machine.
    fn real_lsof() -> Option<String> {
        ["/usr/sbin/lsof", "/usr/bin/lsof", "/bin/lsof", "/sbin/lsof"]
            .into_iter()
            .find(|path| Path::new(path).exists())
            .map(String::from)
    }

    const FAKE_RSYNC: &str = r#"#!/bin/sh
case " $* " in
  *" --version "*) echo "${FAKE_RSYNC_VERSION:-rsync  version 3.2.7  protocol version 31}"; exit 0 ;;
esac
line=rsync; for a in "$@"; do line="$line [$a]"; done
printf '%s CHOSEN_RSYNC=%s\n' "$line" "$CHOSEN_RSYNC" >> "$FAKE_LOG"
cat > "$FAKE_LOG.rsync-list"
exit "${FAKE_RSYNC_EXIT:-0}"
"#;

    /// A fake home with a git checkout `app` linked to acme/web, and the fake
    /// `ssh` and `rsync` in `bin`.
    struct Setup {
        home: TempDir,
        checkout: PathBuf,
        /// The stack's two ports on the machine, free on this one when the
        /// test starts: (WEB_PORT, DB_PORT).
        ports: (u16, u16),
    }

    /// A port nothing on this machine uses right now, and no other test of
    /// this run receives. The ports lie below the range the OS gives to
    /// outgoing connections (32768 and up on Linux, 49152 and up on macOS),
    /// so a connection of another test cannot take one before fed checks it.
    fn free_port() -> u16 {
        use std::sync::atomic::{AtomicU32, Ordering};
        static NEXT: AtomicU32 = AtomicU32::new(0);
        let start = std::process::id().wrapping_mul(37) % 12000;
        let _spawning = spawn_lock();
        loop {
            let n = NEXT.fetch_add(1, Ordering::SeqCst);
            let port = (20000 + (start + n) % 12000) as u16;
            if std::net::TcpListener::bind(("0.0.0.0", port)).is_ok()
                && std::net::TcpListener::bind(("127.0.0.1", port)).is_ok()
                && !matches!(
                    std::net::TcpListener::bind((std::net::Ipv6Addr::LOCALHOST, port)),
                    Err(e) if e.kind() == std::io::ErrorKind::AddrInUse
                )
            {
                return port;
            }
        }
    }

    impl Setup {
        fn new() -> Self {
            let home = TempDir::new().unwrap();
            let checkout = home.path().join("app");
            std::fs::create_dir_all(checkout.join(".fed")).unwrap();
            std::fs::write(
                checkout.join(".fed/cloud.yaml"),
                "org: acme\nproject: web\n",
            )
            .unwrap();
            std::fs::write(checkout.join("README.md"), "hello\n").unwrap();
            let ok = {
                let _spawning = spawn_lock();
                Command::new("git")
                    .args(["init", "-q"])
                    .arg(&checkout)
                    .status()
                    .unwrap()
                    .success()
            };
            assert!(ok);
            let bin = home.path().join("bin");
            std::fs::create_dir_all(&bin).unwrap();
            for (name, script) in [
                ("ssh", FAKE_SSH),
                ("rsync", FAKE_RSYNC),
                ("fake-listen.py", FAKE_LISTEN),
            ] {
                let path = bin.join(name);
                std::fs::write(&path, script).unwrap();
                std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
            }
            if let Some(lsof) = real_lsof() {
                let path = bin.join("lsof");
                std::fs::write(&path, FAKE_LSOF.replace("REAL_LSOF", &lsof)).unwrap();
                std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
            }
            let mut ports = (free_port(), free_port());
            while ports.0 == ports.1 {
                ports.1 = free_port();
            }
            Self {
                home,
                checkout,
                ports,
            }
        }

        fn ports_json(&self) -> String {
            format!(
                r#"{{"WEB_PORT": {}, "DB_PORT": {}}}"#,
                self.ports.0, self.ports.1
            )
        }

        fn home(&self) -> &Path {
            self.home.path()
        }

        fn log_path(&self) -> PathBuf {
            self.home().join("calls.log")
        }

        /// Every logged call, one per line.
        fn calls(&self) -> Vec<String> {
            std::fs::read_to_string(self.log_path())
                .unwrap_or_default()
                .lines()
                .map(String::from)
                .collect()
        }

        /// What the fake ssh commands read from stdin, concatenated.
        fn ssh_stdin(&self) -> Vec<u8> {
            let mut all = Vec::new();
            for entry in std::fs::read_dir(self.home()).unwrap().flatten() {
                if entry
                    .file_name()
                    .to_string_lossy()
                    .starts_with("calls.log.stdin.")
                {
                    all.extend(std::fs::read(entry.path()).unwrap());
                }
            }
            all
        }

        /// Write state for an environment, as `fed remote up` leaves it.
        fn save(&self, id: &str, name: &str, project: &str) -> PathBuf {
            let dir = self.home().join(".fed/remote").join(id);
            std::fs::create_dir_all(&dir).unwrap();
            let state = serde_json::json!({
                "id": id, "name": name, "org": "acme", "project": project,
                "ip": "203.0.113.7", "port": 23456, "deadline": "2099-01-01T00:00:00Z",
            });
            std::fs::write(dir.join("env.json"), state.to_string()).unwrap();
            dir
        }

        fn fed(&self, url: &str, args: &[&str], env: &[(&str, &str)]) -> (bool, String) {
            let path = format!(
                "{}:{}",
                self.home().join("bin").display(),
                std::env::var("PATH").unwrap_or_default()
            );
            let mut output = Command::new(fed_binary());
            output
                .arg("--workdir")
                .arg(&self.checkout)
                .args(args)
                .env("HOME", self.home())
                .env("PATH", path)
                .env("FAKE_LOG", self.log_path())
                .env("FED_TOKEN", "fed_test-token")
                .env("FED_CLOUD_URL", url)
                .env("FAKE_PORTS", self.ports_json())
                .envs(env.iter().copied())
                .stdin(std::process::Stdio::null())
                .stdout(std::process::Stdio::piped())
                .stderr(std::process::Stdio::piped());
            let child = {
                let _spawning = spawn_lock();
                output.spawn().unwrap()
            };
            let output = child.wait_with_output().unwrap();
            let combined = format!(
                "{}{}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
            (output.status.success(), combined)
        }

        fn remote_dirs(&self) -> Vec<String> {
            let mut dirs: Vec<String> = std::fs::read_dir(self.home().join(".fed/remote"))
                .map(|entries| {
                    entries
                        .flatten()
                        .map(|e| e.file_name().to_string_lossy().into_owned())
                        .collect()
                })
                .unwrap_or_default();
            dirs.sort();
            dirs
        }
    }

    fn environment(id: &str, name: &str) -> serde_json::Value {
        serde_json::json!({
            "id": id, "name": name, "type": "DEV1-S", "state": "ready",
            "ip": "203.0.113.7", "port": 23456,
            "created_at": "2026-10-10T10:00:00Z", "deadline": "2099-01-01T00:00:00Z",
        })
    }

    fn list(envs: &[serde_json::Value]) -> String {
        serde_json::json!({ "environments": envs }).to_string()
    }

    const WEB: &str = "GET /api/v1/orgs/acme/projects/web/environments ";

    /// A route status that makes the mock cloud never answer.
    const HANG: &str = "hang";

    /// A refusal reaches the user as a plain sentence, the keys went in the
    /// body, and no half-made state stays behind.
    #[test]
    fn up_refused_for_the_org_sends_keys_and_leaves_no_state() {
        let setup = Setup::new();
        let cloud = Cloud::start(vec![(
            "POST /api/v1/orgs/acme/projects/web/environments ",
            "403 Forbidden",
            r#"{"error":"compute_not_enabled"}"#.to_string(),
        )]);
        let (ok, output) = setup.fed(&cloud.url, &["remote", "up", "box"], &[]);
        assert!(!ok, "{output}");
        assert!(
            output.contains(
                "Error: Remote environments are not enabled for acme. Ask a Service Federation admin to turn them on.\n"
            ),
            "{output}"
        );
        assert!(!output.contains("Invalid configuration"), "{output}");

        let request = cloud
            .request("POST /api/v1/orgs/acme/projects/web/environments ")
            .unwrap();
        let json = json_body(&request);
        assert_eq!(json["name"], "box");
        assert!(json.get("type").is_none(), "{json}");
        assert!(json.get("root_volume_gb").is_none(), "{json}");
        assert!(
            json["client_public_key"]
                .as_str()
                .unwrap()
                .starts_with("ssh-ed25519 ")
        );
        assert!(
            json["host_public_key"]
                .as_str()
                .unwrap()
                .starts_with("ssh-ed25519 ")
        );
        assert!(
            json["host_private_key"]
                .as_str()
                .unwrap()
                .contains("BEGIN OPENSSH PRIVATE KEY")
        );
        assert!(!output.contains("PRIVATE KEY"), "{output}");

        let root = setup.home().join(".fed/remote");
        assert_eq!(
            std::fs::metadata(&root).unwrap().permissions().mode() & 0o777,
            0o700
        );
        assert!(setup.remote_dirs().is_empty(), "{:?}", setup.remote_dirs());
    }

    #[test]
    fn up_sends_the_disk_size() {
        let setup = Setup::new();
        let cloud = Cloud::start(vec![(
            "POST /api/v1/orgs/acme/projects/web/environments ",
            "403 Forbidden",
            r#"{"error":"compute_not_enabled"}"#.to_string(),
        )]);
        let (ok, output) = setup.fed(&cloud.url, &["remote", "up", "box", "--disk", "40"], &[]);
        assert!(!ok, "{output}");
        let request = cloud
            .request("POST /api/v1/orgs/acme/projects/web/environments ")
            .unwrap();
        assert_eq!(json_body(&request)["root_volume_gb"], 40);
    }

    #[test]
    fn up_refuses_a_bad_disk_size_before_any_request() {
        let setup = Setup::new();
        let cloud = Cloud::start(vec![]);
        for bad in ["5", "201", "40.5", "big"] {
            let (ok, output) = setup.fed(&cloud.url, &["remote", "up", "box", "--disk", bad], &[]);
            assert!(!ok, "{output}");
            assert!(
                output.contains(&format!(
                    "Error: fed remote up --disk needs a whole number of GB from 10 to 200, such as --disk 40. It received `{bad}`."
                )),
                "{output}"
            );
        }
        assert!(cloud.requests().is_empty(), "{:?}", cloud.requests());
    }

    /// The cloud's own sentence reaches the user, without the characters a
    /// terminal would act on.
    #[test]
    fn up_prints_the_cloud_message_for_a_refusal() {
        let setup = Setup::new();
        let cloud = Cloud::start(vec![(
            "POST /api/v1/orgs/acme/projects/web/environments ",
            "403 Forbidden",
            r#"{"error":"user_held","message":"Your account is on hold.\u001b[2J Contact support.\n"}"#
                .to_string(),
        )]);
        let (ok, output) = setup.fed(&cloud.url, &["remote", "up", "box"], &[]);
        assert!(!ok, "{output}");
        assert!(
            output.contains("Error: Your account is on hold. [2J Contact support.\n"),
            "{output}"
        );
        assert!(!output.contains('\u{1b}'), "{output:?}");
    }

    /// A refusal for the disk size keeps fed's hint after the cloud's message.
    #[test]
    fn up_adds_the_disk_hint_to_the_cloud_message() {
        let setup = Setup::new();
        let cloud = Cloud::start(vec![(
            "POST /api/v1/orgs/acme/projects/web/environments ",
            "422 Unprocessable Entity",
            r#"{"error":"limit","limit":"root_volume_gb","message":"acme may use disks of at most 100 GB."}"#
                .to_string(),
        )]);
        let (ok, output) = setup.fed(&cloud.url, &["remote", "up", "box", "--disk", "150"], &[]);
        assert!(!ok, "{output}");
        assert!(
            output.contains(
                "Error: acme may use disks of at most 100 GB. Pick another size with `--disk`.\n"
            ),
            "{output}"
        );
    }

    /// Without a message, fed's own wording stays.
    #[test]
    fn ls_prints_the_cloud_message_or_its_own_wording() {
        let setup = Setup::new();
        let cloud = Cloud::start(vec![(
            WEB,
            "403 Forbidden",
            r#"{"error":"user_not_approved","message":"An admin of acme has not approved you yet."}"#
                .to_string(),
        )]);
        let (ok, output) = setup.fed(&cloud.url, &["remote", "ls"], &[]);
        assert!(!ok, "{output}");
        assert!(
            output.contains("Error: An admin of acme has not approved you yet.\n"),
            "{output}"
        );

        let cloud = Cloud::start(vec![(
            WEB,
            "422 Unprocessable Entity",
            r#"{"error":"limit","limit":"org_limit"}"#.to_string(),
        )]);
        let (ok, output) = setup.fed(&cloud.url, &["remote", "ls"], &[]);
        assert!(!ok, "{output}");
        assert!(
            output.contains("Error: acme has reached a limit for remote environments.\n"),
            "{output}"
        );
    }

    #[test]
    fn up_refuses_a_bad_name_before_any_request() {
        let setup = Setup::new();
        let (ok, output) = setup.fed("http://127.0.0.1:9", &["remote", "up", "Bad_Name"], &[]);
        assert!(!ok);
        assert!(output.contains("invalid environment name"), "{output}");
    }

    #[test]
    fn up_refuses_openrsync_before_creating_anything() {
        let setup = Setup::new();
        let cloud = Cloud::start(vec![]);
        let (ok, output) = setup.fed(
            &cloud.url,
            &["remote", "up", "box"],
            &[("FAKE_RSYNC_VERSION", "openrsync: protocol version 29")],
        );
        assert!(!ok, "{output}");
        assert!(output.contains("openrsync"), "{output}");
        assert!(output.contains("brew install rsync"), "{output}");
        assert!(cloud.requests().is_empty(), "{:?}", cloud.requests());
    }

    /// Running `up box` again after the first box expired leaves one state
    /// directory for box, and the old one's vault token is revoked.
    #[test]
    fn up_again_with_the_same_name_replaces_the_old_state() {
        let setup = Setup::new();
        let old = setup.save("env-old", "box", "web");
        std::fs::create_dir_all(old.join("tokens")).unwrap();
        std::fs::write(
            old.join("tokens/app.json"),
            r#"{"id":"tok-old","org":"acme","project":"web"}"#,
        )
        .unwrap();
        let cloud = Cloud::start(vec![
            (
                "POST /api/v1/orgs/acme/projects/web/environments ",
                "201 Created",
                environment("env-new", "box").to_string(),
            ),
            (
                "DELETE /api/v1/orgs/acme/projects/web/env-tokens/tok-old ",
                "200 OK",
                r#"{"revoked":true}"#.to_string(),
            ),
        ]);
        let (ok, output) = setup.fed(&cloud.url, &["remote", "up", "box"], &[]);
        assert!(ok, "{output}");
        assert!(output.contains("box is ready at 203.0.113.7"), "{output}");
        assert_eq!(setup.remote_dirs(), vec!["env-new"]);
        assert!(
            cloud
                .request("DELETE /api/v1/orgs/acme/projects/web/env-tokens/tok-old ")
                .is_some(),
            "{:?}",
            cloud.requests()
        );
        let known =
            std::fs::read_to_string(setup.home().join(".fed/remote/env-new/known_hosts")).unwrap();
        assert!(
            known.starts_with("[203.0.113.7]:23456 ssh-ed25519 "),
            "{known}"
        );
        assert!(
            !setup.home().join(".fed/remote/env-new/host_key").exists(),
            "the host private key is gone after the request"
        );
    }

    #[test]
    fn push_copies_the_file_list_and_cleans_up_in_the_workspace() {
        let setup = Setup::new();
        setup.save("env-1", "box", "web");
        let (ok, output) = setup.fed("http://127.0.0.1:9", &["remote", "push", "box"], &[]);
        assert!(ok, "{output}");
        assert!(
            output.contains("Pushing 2 files to box:/srv/app"),
            "{output}"
        );

        let calls = setup.calls();
        let rsync = calls.iter().find(|c| c.starts_with("rsync ")).unwrap();
        assert!(
            rsync.starts_with(
                "rsync [-a] [--from0] [--files-from=-] [--timeout=120] [-e] [ssh -F none -i "
            ),
            "{rsync}"
        );
        assert!(rsync.contains(" -o 'ControlMaster=no' "), "{rsync}");
        assert!(
            rsync.ends_with(&format!(
                " [{}/] [root@203.0.113.7:/srv/app/] CHOSEN_RSYNC=rsync_samba",
                setup.checkout.canonicalize().unwrap().display()
            )),
            "{rsync}"
        );
        let mut sent: Vec<String> = std::fs::read(setup.home().join("calls.log.rsync-list"))
            .unwrap()
            .split(|c| *c == 0)
            .filter(|p| !p.is_empty())
            .map(|p| String::from_utf8_lossy(p).into_owned())
            .collect();
        sent.sort();
        assert_eq!(sent, vec![".fed/cloud.yaml", "README.md"]);

        // The cleanup script spans several lines of the log.
        let log = std::fs::read_to_string(setup.log_path()).unwrap();
        assert!(
            log.contains(" [root@203.0.113.7] [set -e\ncd /srv/app\n"),
            "{log}"
        );
        assert!(
            log.contains("mv -f \"$new\" .fed/remote-push-files"),
            "{log}"
        );
        assert!(setup.ssh_stdin().windows(10).any(|w| w == b"README.md\0"));
    }

    /// `start` mints a vault token tied to the environment, sends it over
    /// stdin, never in an argument, and runs `fed start` in the workspace.
    #[test]
    fn start_gives_the_workspace_a_vault_token_and_runs_fed_start() {
        let setup = Setup::new();
        setup.save("env-1", "box", "web");
        let token = "fedenv_token-must-not-leak";
        let cloud = Cloud::start(vec![(
            "POST /api/v1/orgs/acme/projects/web/env-tokens ",
            "201 Created",
            format!(r#"{{"id":"tok-1","token":"{token}","expires_at":"2099-01-01T00:00:00Z"}}"#),
        )]);
        let (ok, output) = setup.fed(&cloud.url, &["remote", "start", "box"], &[]);
        assert!(ok, "{output}");
        assert!(output.contains("Started /srv/app on box."), "{output}");

        let mint = json_body(
            &cloud
                .request("POST /api/v1/orgs/acme/projects/web/env-tokens ")
                .unwrap(),
        );
        assert_eq!(mint["environment_id"], "env-1");
        assert_eq!(mint["label"], "fed remote box/app");

        let calls = setup.calls();
        assert!(calls.iter().all(|c| !c.contains(token)), "{calls:#?}");
        assert!(
            calls
                .iter()
                .any(|c| c.ends_with(" [cd /srv/app && fed start]")),
            "{calls:#?}"
        );
        assert!(
            calls.iter().all(|c| !c.contains("releases/download")),
            "a newer fed on the machine stays"
        );
        let stdin = String::from_utf8_lossy(&setup.ssh_stdin()).into_owned();
        assert!(stdin.contains(token), "the token went over stdin");
        let record =
            std::fs::read_to_string(setup.home().join(".fed/remote/env-1/tokens/app.json"))
                .unwrap();
        assert!(
            record.contains("tok-1") && !record.contains(token),
            "{record}"
        );
    }

    #[test]
    fn start_installs_this_fed_release_on_an_older_machine() {
        let setup = Setup::new();
        setup.save("env-1", "box", "web");
        let cloud = Cloud::start(vec![(
            "POST /api/v1/orgs/acme/projects/web/env-tokens ",
            "201 Created",
            r#"{"id":"tok-1","token":"t","expires_at":"2099-01-01T00:00:00Z"}"#.to_string(),
        )]);
        let (ok, output) = setup.fed(
            &cloud.url,
            &["remote", "start", "box"],
            &[("FAKE_VM_FED", "0.0.1")],
        );
        assert!(ok, "{output}");
        let version = env!("CARGO_PKG_VERSION");
        assert!(
            output.contains(&format!(
                "Installing fed {version} on box, which has fed 0.0.1"
            )),
            "{output}"
        );
        let calls = setup.calls();
        assert!(
            calls.iter().any(|c| c.contains(&format!(
                "https://github.com/service-federation/fed/releases/download/v{version}/fed-$t.tar.xz"
            ))),
            "{calls:#?}"
        );
    }

    /// The tunnel is one ssh with a local forward per port. When it exits
    /// at once and the cloud still lists the environment, `connect` says the
    /// connection ended.
    #[test]
    fn connect_reports_an_ssh_that_exits_at_once() {
        let setup = Setup::new();
        setup.save("env-1", "box", "web");
        let cloud = Cloud::start(vec![(WEB, "200 OK", list(&[environment("env-1", "box")]))]);
        let (ok, output) = setup.fed(&cloud.url, &["remote", "connect", "box"], &[]);
        assert!(!ok, "{output}");
        assert!(
            output.contains(
                "Error: The connection to box ended (ssh exit status 255).\nRun `fed remote connect box` again."
            ),
            "{output}"
        );
        assert!(!output.contains("Connected."), "{output}");

        let calls = setup.calls();
        let forwards: Vec<&String> = calls.iter().filter(|c| c.contains(" [-L] ")).collect();
        assert_eq!(forwards.len(), 1, "{calls:#?}");
        let mut ports = [setup.ports.0, setup.ports.1];
        ports.sort();
        let expected: Vec<String> = ports.iter().flat_map(|port| forwards_for(*port)).collect();
        assert_eq!(local_forwards(forwards[0]), expected, "{}", forwards[0]);
        for option in [
            "[BatchMode=yes]",
            "[ExitOnForwardFailure=yes]",
            "[ControlPath=none]",
            "[-N]",
        ] {
            assert!(forwards[0].contains(option), "{option}: {}", forwards[0]);
        }
        assert!(
            calls
                .iter()
                .any(|c| c.ends_with(" [cd /srv/app && fed ports list --json]")),
            "{calls:#?}"
        );
    }

    /// `count` different ports that nothing on this machine uses right now.
    fn free_ports(count: usize) -> Vec<u16> {
        (0..count).map(|_| free_port()).collect()
    }

    /// The `-L` values `connect` passes for `port`: one per address, so
    /// ExitOnForwardFailure covers each. ::1 only where this machine has it.
    fn forwards_for(port: u16) -> Vec<String> {
        let mut forwards = vec![format!("127.0.0.1:{port}:127.0.0.1:{port}")];
        let ipv6 = {
            let _spawning = spawn_lock();
            std::net::TcpListener::bind((std::net::Ipv6Addr::LOCALHOST, 0)).is_ok()
        };
        if ipv6 {
            forwards.push(format!("[::1]:{port}:127.0.0.1:{port}"));
        }
        forwards
    }

    /// The `-L` values of one logged ssh call, in order.
    fn local_forwards(call: &str) -> Vec<String> {
        call.split(" [-L] [")
            .skip(1)
            .map(|rest| {
                rest.split("] ")
                    .next()
                    .unwrap()
                    .trim_end_matches(']')
                    .to_string()
            })
            .collect()
    }

    /// sshd drops new connections past 10 by default, so every port goes
    /// through one ssh: one `-L` per port, sorted by port.
    #[test]
    fn connect_forwards_twelve_ports_through_one_ssh() {
        let setup = Setup::new();
        setup.save("env-1", "box", "web");
        let mut ports = free_ports(12);
        let json = ports
            .iter()
            .enumerate()
            .map(|(i, port)| format!(r#""PORT_{i}": {port}"#))
            .collect::<Vec<_>>()
            .join(", ");
        let cloud = Cloud::start(vec![(WEB, "200 OK", list(&[environment("env-1", "box")]))]);
        let (ok, output) = setup.fed(
            &cloud.url,
            &["remote", "connect", "box"],
            &[("FAKE_PORTS", &format!("{{{json}}}"))],
        );
        assert!(!ok, "{output}");

        let calls = setup.calls();
        let forwards: Vec<&String> = calls.iter().filter(|c| c.contains(" [-L] ")).collect();
        assert_eq!(forwards.len(), 1, "{output}\n{forwards:#?}");
        ports.sort();
        let expected: Vec<String> = ports.iter().flat_map(|port| forwards_for(*port)).collect();
        assert_eq!(local_forwards(forwards[0]), expected, "{}", forwards[0]);
        assert!(
            forwards[0].ends_with(" [root@203.0.113.7]"),
            "{}",
            forwards[0]
        );
    }

    /// A port taken on this machine stops `connect` before any forward, with
    /// the port, its parameter, the program and what to do.
    #[test]
    fn connect_refuses_a_port_taken_here() {
        let setup = Setup::new();
        setup.save("env-1", "box", "web");
        let (web, _) = setup.ports;
        let _here = {
            let _spawning = spawn_lock();
            std::net::TcpListener::bind(("127.0.0.1", web)).unwrap()
        };
        let (ok, output) = setup.fed("http://127.0.0.1:9", &["remote", "connect", "box"], &[]);
        assert!(!ok, "{output}");
        assert!(
            output.contains(
                "Error: fed remote connect cannot forward every port of box, because programs on this machine use these ports:"
            ),
            "{output}"
        );
        assert!(
            output.contains(&format!("  {web} (WEB_PORT): ")),
            "{output}"
        );
        // The listener belongs to this test process, which the port lookup
        // finds by its PID.
        assert!(
            output.contains(&format!("(PID {})", std::process::id())),
            "{output}"
        );
        assert!(
            output.contains("Stop those programs, then run `fed remote connect box` again."),
            "{output}"
        );
        let calls = setup.calls();
        assert!(calls.iter().all(|c| !c.contains(" [-L] ")), "{calls:#?}");
    }

    /// The pids the fake tunnel wrote.
    fn tunnel_pids(setup: &Setup) -> Vec<i32> {
        std::fs::read_dir(setup.home())
            .unwrap()
            .flatten()
            .filter(|e| {
                e.file_name()
                    .to_string_lossy()
                    .starts_with("calls.log.forward.")
            })
            .filter_map(|e| std::fs::read_to_string(e.path()).ok())
            .filter_map(|pid| pid.trim().parse().ok())
            .collect()
    }

    /// The fake tunnel and every process whose command line holds the
    /// test's home, such as the supervisor. Those still alive.
    fn leftovers(setup: &Setup) -> Vec<i32> {
        let mut pids = tunnel_pids(setup);
        let found = {
            let _spawning = spawn_lock();
            Command::new("pgrep")
                .arg("-f")
                .arg(setup.home())
                .output()
                .unwrap()
        };
        pids.extend(
            String::from_utf8_lossy(&found.stdout)
                .lines()
                .filter_map(|pid| pid.trim().parse::<i32>().ok()),
        );
        pids.into_iter().filter(|pid| alive(*pid)).collect()
    }

    /// Wait until no process of the test is alive, for at most 5 seconds.
    fn assert_no_leftovers(setup: &Setup) {
        let start = std::time::Instant::now();
        while !leftovers(setup).is_empty() {
            assert!(
                start.elapsed() < std::time::Duration::from_secs(5),
                "still running: {:?}",
                leftovers(setup)
            );
            std::thread::sleep(std::time::Duration::from_millis(50));
        }
    }

    /// OpenSSH's lines for a local port it could not listen on at all.
    fn cannot_listen(port: u16) -> String {
        format!(
            "bind [127.0.0.1]:{port}: Address already in use\nchannel_setup_fwd_listener_tcpip: cannot listen to port: {port}\nCould not request local forwarding."
        )
    }

    /// Run `connect` with ssh failing to listen on WEB_PORT after `delay`
    /// seconds, and check that `connect` fails, names the port and leaves
    /// no process running.
    fn connect_with_a_port_ssh_cannot_listen_on(delay: &str) {
        let setup = Setup::new();
        setup.save("env-1", "box", "web");
        let (web, _) = setup.ports;
        let (ok, output) = setup.fed(
            "http://127.0.0.1:9",
            &["remote", "connect", "box"],
            &[
                ("FAKE_SAY", &cannot_listen(web)),
                ("FAKE_FORWARD_DELAY", delay),
            ],
        );
        assert!(!ok, "{output}");
        assert!(!output.contains("Connected."), "{output}");
        assert!(
            output.contains(&format!(
                "Error: fed remote connect could not forward port {web} (WEB_PORT) of box, because ssh could not listen on it here. A program on this machine may have taken the port.\nRun `fed remote connect box` again."
            )),
            "{output}"
        );
        assert!(
            output.contains(&format!(
                "box: channel_setup_fwd_listener_tcpip: cannot listen to port: {web}"
            )),
            "{output}"
        );
        assert_no_leftovers(&setup);
    }

    /// A program can take a port between the check and the tunnel. Then ssh
    /// exits, and `connect` names the port, so no port reaches whatever took
    /// it on this machine.
    #[test]
    fn connect_names_the_port_ssh_cannot_listen_on() {
        connect_with_a_port_ssh_cannot_listen_on("0");
    }

    /// The same when ssh needs 2 seconds to log in first. A fixed wait
    /// before "Connected." missed this case.
    #[test]
    fn connect_names_the_port_ssh_cannot_listen_on_after_a_slow_login() {
        connect_with_a_port_ssh_cannot_listen_on("2");
    }

    /// Whether `port` on 127.0.0.1 accepts a connection.
    fn accepts(port: u16) -> bool {
        let _spawning = spawn_lock();
        std::net::TcpStream::connect(("127.0.0.1", port)).is_ok()
    }

    /// `fed remote connect box` in the background, with stdout and stderr in
    /// files, so a full pipe never stops it. With `stuck_stderr`, stderr is a
    /// pipe that nobody reads instead.
    struct Background {
        child: std::process::Child,
        stdout: PathBuf,
        stderr: PathBuf,
    }

    impl Background {
        fn connect(setup: &Setup, env: &[(&str, &str)]) -> Self {
            Self::spawn(setup, env, false)
        }

        fn connect_with_stuck_stderr(setup: &Setup, env: &[(&str, &str)]) -> Self {
            Self::spawn(setup, env, true)
        }

        fn spawn(setup: &Setup, env: &[(&str, &str)], stuck_stderr: bool) -> Self {
            let path = format!(
                "{}:{}",
                setup.home().join("bin").display(),
                std::env::var("PATH").unwrap_or_default()
            );
            let stdout = setup.home().join("connect-stdout");
            let stderr = setup.home().join("connect-stderr");
            let mut child = Command::new(fed_binary());
            child
                .arg("--workdir")
                .arg(&setup.checkout)
                .args(["remote", "connect", "box"])
                .env("HOME", setup.home())
                .env("PATH", path)
                .env("FAKE_LOG", setup.log_path())
                .env("FAKE_PORTS", setup.ports_json())
                .env("FED_TOKEN", "fed_test-token")
                .env("FED_CLOUD_URL", "http://127.0.0.1:9")
                .envs(env.iter().copied())
                .stdout(std::fs::File::create(&stdout).unwrap());
            if stuck_stderr {
                // The pipe stays open in `child.stderr` and is never read.
                child.stderr(std::process::Stdio::piped());
            } else {
                child.stderr(std::fs::File::create(&stderr).unwrap());
            }
            let child = {
                let _spawning = spawn_lock();
                child.spawn().unwrap()
            };
            Self {
                child,
                stdout,
                stderr,
            }
        }

        fn output(&self) -> String {
            format!(
                "{}{}",
                std::fs::read_to_string(&self.stdout).unwrap_or_default(),
                std::fs::read_to_string(&self.stderr).unwrap_or_default()
            )
        }

        /// Wait until the output holds `text`, for at most 20 seconds.
        fn wait_for(&self, text: &str) {
            let start = std::time::Instant::now();
            while !self.output().contains(text) {
                assert!(
                    start.elapsed() < std::time::Duration::from_secs(20),
                    "no {text:?} in: {}",
                    self.output()
                );
                std::thread::sleep(std::time::Duration::from_millis(20));
            }
        }

        fn signal(&self, signal: &str) {
            let _spawning = spawn_lock();
            let ok = Command::new("kill")
                .args([&format!("-{signal}"), &self.child.id().to_string()])
                .status()
                .unwrap()
                .success();
            assert!(ok);
        }

        /// Wait for fed to exit. Returns whether it succeeded, and its
        /// output.
        fn finish(mut self) -> (bool, String) {
            let status = self.child.wait().unwrap();
            (status.success(), self.output())
        }
    }

    /// A test that fails before `finish` still ends fed. Its supervisor then
    /// ends the fake tunnel.
    impl Drop for Background {
        fn drop(&mut self) {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
    }

    /// A slow login: ssh listens only after 3 seconds. "Connected." waits
    /// until every port accepts.
    #[test]
    fn connect_waits_until_every_port_accepts() {
        let setup = Setup::new();
        setup.save("env-1", "box", "web");
        let start = std::time::Instant::now();
        let fed = Background::connect(
            &setup,
            &[("FAKE_FORWARD", "hold"), ("FAKE_FORWARD_DELAY", "3")],
        );
        fed.wait_for("Connected.");
        assert!(
            start.elapsed() >= std::time::Duration::from_secs(3),
            "{}",
            fed.output()
        );
        for port in [setup.ports.0, setup.ports.1] {
            assert!(accepts(port), "{port} does not accept: {}", fed.output());
        }
        fed.signal("INT");
        let (ok, output) = fed.finish();
        assert!(ok, "{output}");
        assert!(output.contains("Disconnected."), "{output}");
        assert_no_leftovers(&setup);
    }

    /// An ssh that never listens: `connect` gives up after the wait, says
    /// the tunnels did not open, and ends ssh.
    #[test]
    fn connect_gives_up_when_the_tunnels_do_not_open() {
        let setup = Setup::new();
        setup.save("env-1", "box", "web");
        let (ok, output) = setup.fed(
            "http://127.0.0.1:9",
            &["remote", "connect", "box"],
            &[
                ("FAKE_FORWARD", "never"),
                ("FED_TEST_TUNNEL_WAIT_SECS", "2"),
            ],
        );
        assert!(!ok, "{output}");
        assert!(!output.contains("Connected."), "{output}");
        assert!(
            output.contains(
                "Error: The tunnels to box did not open within 2 seconds. These ports never accepted a connection:"
            ),
            "{output}"
        );
        assert!(
            output.contains(&format!("  {} (WEB_PORT)", setup.ports.0)),
            "{output}"
        );
        assert_no_leftovers(&setup);
    }

    /// OpenSSH prints a `bind` line when one address of a port fails, such
    /// as `[::1]`, and goes on. When ssh exits later for another reason, the
    /// error does not blame that port.
    #[test]
    fn a_lone_bind_line_does_not_blame_its_port() {
        let setup = Setup::new();
        setup.save("env-1", "box", "web");
        let (web, _) = setup.ports;
        let (ok, output) = setup.fed(
            "http://127.0.0.1:9",
            &["remote", "connect", "box"],
            &[
                ("FAKE_FORWARD", "hold"),
                (
                    "FAKE_SAY",
                    &format!("bind [::1]:{web}: Cannot assign requested address"),
                ),
                ("FAKE_EXIT_AFTER", UP_FOR),
            ],
        );
        assert!(!ok, "{output}");
        assert!(output.contains("Connected."), "{output}");
        assert!(
            output.contains("box: bind [::1]:"),
            "the bind line did not reach the terminal: {output}"
        );
        assert!(
            output.contains("Error: The connection to box ended (ssh exit status 255)."),
            "{output}"
        );
        assert!(!output.contains("could not forward port"), "{output}");
        assert_no_leftovers(&setup);
    }

    /// A connection that ends after "Connected." gets one line, not one
    /// line per port.
    #[test]
    fn a_connection_that_ends_later_is_reported_once() {
        let setup = Setup::new();
        setup.save("env-1", "box", "web");
        let (ok, output) = setup.fed(
            "http://127.0.0.1:9",
            &["remote", "connect", "box"],
            &[("FAKE_FORWARD", "hold"), ("FAKE_EXIT_AFTER", UP_FOR)],
        );
        assert!(!ok, "{output}");
        assert!(output.contains("Connected."), "{output}");
        assert!(
            output.contains(
                "Error: The connection to box ended (ssh exit status 255).\nRun `fed remote connect box` again."
            ),
            "{output}"
        );
        for port in [setup.ports.0, setup.ports.1] {
            assert!(
                !output.contains(&format!("{port} (")),
                "a line for {port}: {output}"
            );
        }
        assert_no_leftovers(&setup);
    }

    /// ssh keeps printing while the tunnel runs, such as one line per
    /// refused channel. fed reads all of it, through the supervisor, so the
    /// pipe never fills and the tunnel stays up.
    #[test]
    fn connect_keeps_reading_what_ssh_prints() {
        let setup = Setup::new();
        setup.save("env-1", "box", "web");
        let fed = Background::connect(
            &setup,
            &[("FAKE_FORWARD", "hold"), ("FAKE_STDERR_KB", "200")],
        );
        fed.wait_for("Connected.");
        let done = setup.home().join("calls.log.noise-done");
        let start = std::time::Instant::now();
        while !done.exists() {
            assert!(
                start.elapsed() < std::time::Duration::from_secs(20),
                "ssh blocked on its stderr: {}",
                fed.output().len()
            );
            std::thread::sleep(std::time::Duration::from_millis(50));
        }
        fed.wait_for("box: debug1: channel 3: noise ");
        let lines = || {
            fed.output()
                .lines()
                .filter(|l| l.starts_with("box: debug1: channel 3: noise "))
                .count()
        };
        let start = std::time::Instant::now();
        while lines() < 200 {
            assert!(
                start.elapsed() < std::time::Duration::from_secs(20),
                "{} of 200 lines",
                lines()
            );
            std::thread::sleep(std::time::Duration::from_millis(50));
        }
        for port in [setup.ports.0, setup.ports.1] {
            assert!(accepts(port), "{port} stopped accepting");
        }
        fed.signal("INT");
        let (ok, output) = fed.finish();
        assert!(ok, "{}", output.len());
        assert!(output.contains("Disconnected."), "{}", output.len());
        assert_no_leftovers(&setup);
    }

    /// Run `connect` with ssh logging in for 2 seconds, while a program of
    /// this test binds WEB_PORT on `address` after the check. `connect` must
    /// not say "Connected." for a port that may reach that program, and the
    /// error names the program.
    fn connect_with_a_competitor(address: &str) {
        let setup = Setup::new();
        setup.save("env-1", "box", "web");
        let (web, _) = setup.ports;
        let fed = Background::connect(
            &setup,
            &[("FAKE_FORWARD", "hold"), ("FAKE_FORWARD_DELAY", "2")],
        );
        let start = std::time::Instant::now();
        while !setup.calls().iter().any(|c| c.contains(" [-L] ")) {
            assert!(
                start.elapsed() < std::time::Duration::from_secs(20),
                "no tunnel: {}",
                fed.output()
            );
            std::thread::sleep(std::time::Duration::from_millis(20));
        }
        let _competitor = {
            let _spawning = spawn_lock();
            std::net::TcpListener::bind((address, web)).unwrap()
        };
        let (ok, output) = fed.finish();
        assert!(!ok, "{output}");
        assert!(!output.contains("Connected."), "{output}");
        assert!(output.contains(&format!("{web} (WEB_PORT)")), "{output}");
        assert!(
            output.contains(&format!("(PID {})", std::process::id())),
            "the competitor is not named: {output}"
        );
        assert_no_leftovers(&setup);
    }

    /// A program on 127.0.0.1 while ssh gets only ::1 is no tunnel: a
    /// browser at localhost reaches the program.
    #[test]
    fn connect_refuses_a_port_another_program_takes_on_ipv4() {
        connect_with_a_competitor("127.0.0.1");
    }

    /// A program on the wildcard address shares the port with ssh on macOS.
    #[test]
    fn connect_refuses_a_port_another_program_takes_on_every_address() {
        connect_with_a_competitor("0.0.0.0");
    }

    /// A terminal that does not read fed's stderr must not stop the tunnel
    /// or Ctrl-C. fed and ssh end within 2 seconds.
    #[test]
    fn ctrl_c_works_while_nobody_reads_feds_stderr() {
        let setup = Setup::new();
        setup.save("env-1", "box", "web");
        let mut fed = Background::connect_with_stuck_stderr(
            &setup,
            &[("FAKE_FORWARD", "hold"), ("FAKE_STDERR_KB", "200")],
        );
        fed.wait_for("Connected.");
        std::thread::sleep(std::time::Duration::from_secs(3));
        fed.signal("INT");
        let start = std::time::Instant::now();
        loop {
            let exited = fed.child.try_wait().unwrap().is_some();
            let tunnel_alive = tunnel_pids(&setup).iter().any(|pid| alive(*pid));
            if exited && !tunnel_alive {
                break;
            }
            assert!(
                start.elapsed() < std::time::Duration::from_secs(2),
                "fed exited: {exited}, tunnel alive: {tunnel_alive}"
            );
            std::thread::sleep(std::time::Duration::from_millis(50));
        }
        assert!(fed.output().contains("Disconnected."), "{}", fed.output());
        assert_no_leftovers(&setup);
    }

    /// An ssh killed by a signal is reported with the signal.
    #[test]
    fn an_ssh_killed_by_a_signal_is_reported_with_it() {
        let setup = Setup::new();
        setup.save("env-1", "box", "web");
        let (ok, output) = setup.fed(
            "http://127.0.0.1:9",
            &["remote", "connect", "box"],
            &[
                ("FAKE_FORWARD", "hold"),
                ("FAKE_EXIT_AFTER", UP_FOR),
                ("FAKE_EXIT_SIGNAL", "9"),
            ],
        );
        assert!(!ok, "{output}");
        assert!(output.contains("Connected."), "{output}");
        assert!(
            output.contains("Error: The connection to box ended (ssh was killed by SIGKILL)."),
            "{output}"
        );
        assert_no_leftovers(&setup);
    }

    /// The test-only wait is clamped, so a huge value cannot crash fed.
    #[test]
    fn a_huge_test_wait_does_not_crash_connect() {
        let setup = Setup::new();
        setup.save("env-1", "box", "web");
        let (ok, output) = setup.fed(
            "http://127.0.0.1:9",
            &["remote", "connect", "box"],
            &[("FED_TEST_TUNNEL_WAIT_SECS", &u64::MAX.to_string())],
        );
        assert!(!ok, "{output}");
        assert!(!output.contains("panicked"), "{output}");
        assert!(
            output.contains("Error: The connection to box ended (ssh exit status 255)."),
            "{output}"
        );
    }

    /// The JSON of `fed ports list --json` for `ports`, as PORT_0, PORT_1...
    fn ports_json(ports: &[u16]) -> String {
        let entries: Vec<String> = ports
            .iter()
            .enumerate()
            .map(|(i, port)| format!(r#""PORT_{i}": {port}"#))
            .collect();
        format!("{{{}}}", entries.join(", "))
    }

    /// A 12-port tunnel opens, even when each `lsof` takes 2.5 seconds and
    /// the wait is 20 seconds. One `lsof` per port and round would need 30.
    #[test]
    fn connect_opens_twelve_ports_with_a_slow_lsof() {
        let setup = Setup::new();
        setup.save("env-1", "box", "web");
        let ports = free_ports(12);
        let json = ports_json(&ports);
        let fed = Background::connect(
            &setup,
            &[
                ("FAKE_PORTS", &json),
                ("FAKE_FORWARD", "hold"),
                ("FAKE_LSOF_DELAY", "2.5"),
                ("FED_TEST_TUNNEL_WAIT_SECS", "20"),
            ],
        );
        fed.wait_for("Connected.");
        for port in &ports {
            assert!(accepts(*port), "{port} does not accept: {}", fed.output());
            assert!(
                fed.output()
                    .contains(&format!("localhost:{port} -> box:{port}")),
                "{}",
                fed.output()
            );
        }
        fed.signal("INT");
        let (ok, output) = fed.finish();
        assert!(ok, "{output}");
        assert!(output.contains("Disconnected."), "{output}");
        assert_no_leftovers(&setup);
    }

    /// Ctrl-C during a slow `lsof` ends fed and ssh at once.
    #[test]
    fn ctrl_c_does_not_wait_for_a_slow_lsof() {
        let setup = Setup::new();
        setup.save("env-1", "box", "web");
        let fed = Background::connect(
            &setup,
            &[("FAKE_FORWARD", "hold"), ("FAKE_LSOF_DELAY", "20")],
        );
        let start = std::time::Instant::now();
        while !setup.calls().iter().any(|c| c.starts_with("lsof ")) {
            assert!(
                start.elapsed() < std::time::Duration::from_secs(20),
                "fed never checked the ports: {}",
                fed.output()
            );
            std::thread::sleep(std::time::Duration::from_millis(20));
        }
        fed.signal("INT");
        let mut fed = fed;
        let start = std::time::Instant::now();
        loop {
            let exited = fed.child.try_wait().unwrap().is_some();
            let tunnel_alive = tunnel_pids(&setup).iter().any(|pid| alive(*pid));
            if exited && !tunnel_alive {
                break;
            }
            assert!(
                start.elapsed() < std::time::Duration::from_secs(2),
                "fed exited: {exited}, tunnel alive: {tunnel_alive}"
            );
            std::thread::sleep(std::time::Duration::from_millis(50));
        }
        assert!(fed.output().contains("Disconnected."), "{}", fed.output());
    }

    /// A UDP socket on a port is no TCP listener, so it does not stop the
    /// tunnel.
    #[test]
    fn a_udp_socket_on_a_port_does_not_stop_connect() {
        let setup = Setup::new();
        setup.save("env-1", "box", "web");
        let (web, _) = setup.ports;
        let _udp = {
            let _spawning = spawn_lock();
            std::net::UdpSocket::bind(("127.0.0.1", web)).unwrap()
        };
        let fed = Background::connect(&setup, &[("FAKE_FORWARD", "hold")]);
        fed.wait_for("Connected.");
        fed.signal("INT");
        let (ok, output) = fed.finish();
        assert!(ok, "{output}");
        assert_no_leftovers(&setup);
    }

    /// Where this machine has ::1, ssh must listen there too. A browser that
    /// resolves localhost to ::1 would get "connection refused" otherwise.
    #[test]
    fn connect_waits_for_the_ipv6_listener_too() {
        let setup = Setup::new();
        if forwards_for(setup.ports.0).len() < 2 {
            return;
        }
        setup.save("env-1", "box", "web");
        let (ok, output) = setup.fed(
            "http://127.0.0.1:9",
            &["remote", "connect", "box"],
            &[
                ("FAKE_FORWARD", "hold"),
                ("FAKE_SKIP_IPV6", "1"),
                ("FED_TEST_TUNNEL_WAIT_SECS", "4"),
            ],
        );
        assert!(!ok, "{output}");
        assert!(!output.contains("Connected."), "{output}");
        assert!(
            output.contains("Error: The tunnels to box did not open within 4 seconds."),
            "{output}"
        );
        assert_no_leftovers(&setup);
    }

    /// An environment deleted under the user: the first SSH fails, the cloud
    /// list no longer has it, and fed says what happened and forgets it.
    #[test]
    fn a_deleted_environment_is_reported_and_forgotten() {
        for command in ["connect", "push", "start", "ssh"] {
            let setup = Setup::new();
            setup.save("env-1", "box", "web");
            let cloud = Cloud::start(vec![(WEB, "200 OK", list(&[]))]);
            let (ok, output) = setup.fed(
                &cloud.url,
                &["remote", command, "box"],
                &[("FAKE_SSH", "dead")],
            );
            assert!(!ok, "{command}: {output}");
            assert!(
                output.contains(
                    "Error: box no longer exists. It was deleted, or it expired (5 idle minutes or 6 hours). Create it again with `fed remote up box`.\n"
                ),
                "{command}: {output}"
            );
            assert!(
                !output.contains("ssh: connect to host"),
                "{command}: ssh's own text is left out: {output}"
            );
            assert!(
                setup.remote_dirs().is_empty(),
                "{command}: {:?}",
                setup.remote_dirs()
            );
        }
    }

    /// The shared connection passes its check but its machine is gone. fed
    /// prints the reason, not ssh's lines, unless the machine still exists.
    #[test]
    fn a_stale_connection_to_a_deleted_environment_prints_only_the_reason() {
        let setup = Setup::new();
        setup.save("env-1", "box", "web");
        let cloud = Cloud::start(vec![(WEB, "200 OK", list(&[]))]);
        let (ok, output) = setup.fed(
            &cloud.url,
            &["remote", "start", "box"],
            &[("FAKE_SSH", "stale")],
        );
        assert!(!ok, "{output}");
        assert!(output.contains("Error: box no longer exists."), "{output}");
        assert!(!output.contains("mux_client"), "{output}");
        assert!(!output.contains("ssh: connect to host"), "{output}");

        let setup = Setup::new();
        setup.save("env-1", "box", "web");
        let cloud = Cloud::start(vec![(WEB, "200 OK", list(&[environment("env-1", "box")]))]);
        let (ok, output) = setup.fed(
            &cloud.url,
            &["remote", "push", "box"],
            &[("FAKE_SSH", "stale")],
        );
        assert!(!ok, "{output}");
        assert!(
            output.contains("Error: cannot connect to box over SSH (exit status: 255). ssh said: ssh: connect to host 203.0.113.7 port 23456: Operation timed out"),
            "{output}"
        );
        assert_eq!(setup.remote_dirs(), vec!["env-1"]);
    }

    /// cloud-init exits with 2 when it finished with recoverable errors only.
    /// The machine is fine then. Exit 1 is a failure, with cloud-init's errors.
    #[test]
    fn up_accepts_cloud_init_done_with_recoverable_errors() {
        for (code, ready) in [("2", true), ("1", false)] {
            let setup = Setup::new();
            let cloud = Cloud::start(vec![(
                "POST /api/v1/orgs/acme/projects/web/environments ",
                "201 Created",
                environment("env-1", "box").to_string(),
            )]);
            let (ok, output) = setup.fed(
                &cloud.url,
                &["remote", "up", "box"],
                &[("FAKE_CLOUD_INIT", code)],
            );
            assert_eq!(ok, ready, "{code}: {output}");
            if ready {
                assert!(output.contains("box is ready"), "{output}");
            } else {
                assert!(
                    output.contains("Error: box failed to set itself up (exit status: 1). Its errors: ('init', RuntimeError('disk full')) Delete it with `fed remote down box` and try again."),
                    "{output}"
                );
            }
        }
    }

    #[test]
    fn an_unreachable_environment_the_cloud_still_has_keeps_its_state() {
        let setup = Setup::new();
        setup.save("env-1", "box", "web");
        let cloud = Cloud::start(vec![(WEB, "200 OK", list(&[environment("env-1", "box")]))]);
        let (ok, output) = setup.fed(
            &cloud.url,
            &["remote", "push", "box"],
            &[("FAKE_SSH", "dead")],
        );
        assert!(!ok, "{output}");
        assert!(
            output.contains("Error: cannot connect to box over SSH (exit status: 255). ssh said: ssh: connect to host 203.0.113.7 port 23456: Operation timed out"),
            "{output}"
        );
        assert_eq!(setup.remote_dirs(), vec!["env-1"]);
    }

    /// Two local environments called box in two projects, and the checkout's
    /// link picks neither: the one the cloud no longer has is dropped, and the
    /// other is used.
    #[test]
    fn an_ambiguous_name_is_settled_by_the_cloud_list() {
        let setup = Setup::new();
        std::fs::write(
            setup.checkout.join(".fed/cloud.yaml"),
            "org: acme\nproject: cli\n",
        )
        .unwrap();
        setup.save("env-web", "box", "web");
        setup.save("env-api", "box", "api");
        let cloud = Cloud::start(vec![
            (WEB, "200 OK", list(&[environment("env-web", "box")])),
            (
                "GET /api/v1/orgs/acme/projects/api/environments ",
                "200 OK",
                list(&[]),
            ),
        ]);
        let (ok, output) = setup.fed(
            &cloud.url,
            &["remote", "ssh", "box", "--", "echo", "a b"],
            &[],
        );
        assert!(ok, "{output}");
        assert_eq!(setup.remote_dirs(), vec!["env-web"]);
        let calls = setup.calls();
        let command = calls.iter().find(|c| c.contains("echo 'a b'")).unwrap();
        assert!(
            command.contains("[cd /srv/app 2>/dev/null || "),
            "{command}"
        );
    }

    #[test]
    fn down_deletes_the_environment_and_its_state() {
        let setup = Setup::new();
        setup.save("env-1", "box", "web");
        let cloud = Cloud::start(vec![(
            "DELETE /api/v1/orgs/acme/projects/web/environments/env-1 ",
            "200 OK",
            r#"{"deleted":true}"#.to_string(),
        )]);
        let (ok, output) = setup.fed(&cloud.url, &["remote", "down", "box"], &[]);
        assert!(ok, "{output}");
        assert!(output.contains("Deleted box."), "{output}");
        assert!(setup.remote_dirs().is_empty());
    }

    /// Without local state, `down` finds the environment in the cloud list,
    /// but only among the caller's own: an org admin sees everyone's.
    #[test]
    fn down_by_name_only_deletes_the_callers_own_environment() {
        let setup = Setup::new();
        let mut theirs = environment("env-bob", "box");
        theirs["owner"] = serde_json::json!({"id": "u-bob", "name": "Bob"});
        let mut mine = environment("env-me", "box");
        mine["owner"] = serde_json::json!({"id": "u-me", "name": "Me"});
        let cloud = Cloud::start(vec![
            (WEB, "200 OK", list(&[theirs.clone(), mine])),
            (
                "GET /api/v1/me ",
                "200 OK",
                r#"{"user":{"id":"u-me","name":"Me","email":null},"orgs":[]}"#.to_string(),
            ),
            (
                "DELETE /api/v1/orgs/acme/projects/web/environments/env-me ",
                "200 OK",
                r#"{"deleted":true}"#.to_string(),
            ),
        ]);
        let (ok, output) = setup.fed(&cloud.url, &["remote", "down", "box"], &[]);
        assert!(ok, "{output}");
        assert!(
            cloud
                .request("DELETE /api/v1/orgs/acme/projects/web/environments/env-me ")
                .is_some()
        );
        assert!(
            cloud
                .request("DELETE /api/v1/orgs/acme/projects/web/environments/env-bob ")
                .is_none()
        );

        let cloud = Cloud::start(vec![
            (WEB, "200 OK", list(&[theirs])),
            (
                "GET /api/v1/me ",
                "200 OK",
                r#"{"user":{"id":"u-me","name":"Me","email":null},"orgs":[]}"#.to_string(),
            ),
        ]);
        let (ok, output) = setup.fed(&cloud.url, &["remote", "down", "box"], &[]);
        assert!(!ok, "{output}");
        assert!(
            output.contains("you have no environment called box in acme/web"),
            "{output}"
        );
    }

    /// `ls` prints the cloud's list and forgets local keys for environments the
    /// cloud no longer has, in this project and in others with local state.
    #[test]
    fn ls_prints_the_list_and_drops_state_for_deleted_environments() {
        let setup = Setup::new();
        setup.save("env-live", "box", "web");
        setup.save("env-gone", "old", "web");
        setup.save("env-other", "box", "api");
        setup.save("env-other-gone", "old", "api");
        let mut theirs = environment("env-bob", "bobs");
        theirs["owner"] = serde_json::json!({"id": "u-bob", "name": "Bob"});
        let mut mine = environment("env-live", "box");
        mine["owner"] = serde_json::json!({"id": "u-me", "name": "Me"});
        mine["root_volume_gb"] = 40.into();
        let cloud = Cloud::start(vec![
            (WEB, "200 OK", list(&[mine, theirs])),
            (
                "GET /api/v1/orgs/acme/projects/api/environments ",
                "200 OK",
                list(&[environment("env-other", "box")]),
            ),
            (
                "GET /api/v1/me ",
                "200 OK",
                r#"{"user":{"id":"u-me","name":"Me","email":null},"orgs":[]}"#.to_string(),
            ),
        ]);
        let (ok, output) = setup.fed(&cloud.url, &["remote", "ls"], &[]);
        assert!(ok, "{output}");
        let lines: Vec<&str> = output.lines().collect();
        assert!(
            lines[0].starts_with("NAME") && lines[0].ends_with("OWNER"),
            "{output}"
        );
        assert!(lines[0].contains(" TYPE    DISK "), "{output}");
        assert!(
            lines[1].starts_with("box ") && lines[1].ends_with("you"),
            "{output}"
        );
        assert!(lines[1].contains(" DEV1-S  40 GB "), "{output}");
        assert!(lines[2].contains(" DEV1-S  -  "), "{output}");
        assert!(
            lines[2].starts_with("bobs ") && lines[2].ends_with("Bob"),
            "{output}"
        );
        assert!(output.contains("203.0.113.7:23456"), "{output}");
        assert_eq!(setup.remote_dirs(), vec!["env-live", "env-other"]);
    }

    #[test]
    fn push_warns_when_files_vanish_during_the_copy() {
        let setup = Setup::new();
        setup.save("env-1", "box", "web");
        let (ok, output) = setup.fed(
            "http://127.0.0.1:9",
            &["remote", "push", "box"],
            &[("FAKE_RSYNC_EXIT", "24")],
        );
        assert!(ok, "{output}");
        assert!(
            output.contains("Some files were deleted while fed copied them to box."),
            "{output}"
        );
        let log = std::fs::read_to_string(setup.log_path()).unwrap();
        assert!(
            log.contains(".fed/remote-push-files"),
            "the cleanup still runs"
        );

        let (ok, output) = setup.fed(
            "http://127.0.0.1:9",
            &["remote", "push", "box"],
            &[("FAKE_RSYNC_EXIT", "23")],
        );
        assert!(!ok, "{output}");
        assert!(output.contains("rsync to box failed"), "{output}");
    }

    /// In a checkout linked to acme/web, `down box` deletes the box of
    /// acme/web, even when this machine only has keys for a box in acme/api.
    #[test]
    fn down_prefers_the_linked_project() {
        let setup = Setup::new();
        let other = setup.save("env-api", "box", "api");
        let cloud = Cloud::start(vec![
            (WEB, "200 OK", list(&[environment("env-web", "box")])),
            (
                "DELETE /api/v1/orgs/acme/projects/web/environments/env-web ",
                "200 OK",
                r#"{"deleted":true}"#.to_string(),
            ),
        ]);
        let (ok, output) = setup.fed(&cloud.url, &["remote", "down", "box"], &[]);
        assert!(ok, "{output}");
        assert!(other.exists(), "the box of acme/api stays");
        assert!(
            cloud
                .request("DELETE /api/v1/orgs/acme/projects/web/environments/env-web ")
                .is_some()
        );

        let cloud = Cloud::start(vec![
            (WEB, "200 OK", list(&[])),
            (
                "DELETE /api/v1/orgs/acme/projects/api/environments/env-api ",
                "200 OK",
                r#"{"deleted":true}"#.to_string(),
            ),
        ]);
        let (ok, output) = setup.fed(&cloud.url, &["remote", "down", "box"], &[]);
        assert!(ok, "{output}");
        assert!(
            output.contains("Using box in acme/api. This checkout is linked to acme/web."),
            "{output}"
        );
        assert!(!other.exists());
    }

    fn alive(pid: i32) -> bool {
        let _spawning = spawn_lock();
        Command::new("kill")
            .args(["-0", &pid.to_string()])
            .stderr(std::process::Stdio::null())
            .status()
            .unwrap()
            .success()
    }

    /// The tunnel is a logged-in session that keeps the machine awake.
    /// Signals and uncatchable owner death must end it.
    #[test]
    fn connect_ends_its_tunnel_even_when_the_owner_is_killed() {
        for signal in ["TERM", "HUP", "KILL"] {
            let setup = Setup::new();
            setup.save("env-1", "box", "web");
            let fed = Background::connect(&setup, &[("FAKE_FORWARD", "hold")]);
            fed.wait_for("Connected.");
            let tunnel = tunnel_pids(&setup);
            assert_eq!(tunnel.len(), 1, "{signal}: {tunnel:?}");
            assert!(alive(tunnel[0]), "{signal}: the tunnel is not running");

            fed.signal(signal);
            let (ok, text) = fed.finish();
            assert_eq!(ok, signal != "KILL", "{signal}: {text}");
            // Every port is forwarded to the same number on this machine.
            for port in [setup.ports.0, setup.ports.1] {
                assert!(
                    text.contains(&format!("localhost:{port} -> box:{port}")),
                    "{signal}: {text}"
                );
            }
            if signal != "KILL" {
                assert!(text.contains("Disconnected."), "{signal}: {text}");
            }
            assert_no_leftovers(&setup);
        }
    }

    /// Ctrl-C while the create is pending makes fed check the list, and a
    /// second Ctrl-C skips that check. Neither is swallowed.
    #[test]
    fn ctrl_c_during_up_stops_the_wait_and_the_check() {
        let setup = Setup::new();
        let cloud = Cloud::start(vec![
            (
                "POST /api/v1/orgs/acme/projects/web/environments ",
                HANG,
                String::new(),
            ),
            (WEB, HANG, String::new()),
        ]);
        let path = format!(
            "{}:{}",
            setup.home().join("bin").display(),
            std::env::var("PATH").unwrap_or_default()
        );
        let mut child = Command::new(fed_binary());
        child
            .arg("--workdir")
            .arg(&setup.checkout)
            .args(["remote", "up", "box"])
            .env("HOME", setup.home())
            .env("PATH", path)
            .env("FAKE_LOG", setup.log_path())
            .env("FED_TOKEN", "fed_test-token")
            .env("FED_CLOUD_URL", &cloud.url)
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped());
        let child = {
            let _spawning = spawn_lock();
            child.spawn().unwrap()
        };
        let wait_for = |prefix: &str| {
            let start = std::time::Instant::now();
            while cloud.request(prefix).is_none() {
                assert!(
                    start.elapsed() < std::time::Duration::from_secs(20),
                    "no {prefix}"
                );
                std::thread::sleep(std::time::Duration::from_millis(50));
            }
        };
        let interrupt = || {
            let _spawning = spawn_lock();
            let ok = Command::new("kill")
                .args(["-INT", &child.id().to_string()])
                .status()
                .unwrap()
                .success();
            assert!(ok);
        };
        wait_for("POST /api/v1/orgs/acme/projects/web/environments ");
        interrupt();
        wait_for(WEB);
        interrupt();
        let output = child.wait_with_output().unwrap();
        let text = format!(
            "{}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(!output.status.success(), "{text}");
        assert!(
            text.contains(
                "Error: fed stopped waiting for box because you pressed Ctrl-C, and did not check whether it exists."
            ),
            "{text}"
        );
        assert!(setup.remote_dirs().is_empty(), "{:?}", setup.remote_dirs());
    }
}
