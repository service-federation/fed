//! `fed remote` exists only in builds with the `remote-beta` feature. Without
//! it, the help text has no trace of it. With it, the commands run against a
//! local mock of the cloud API and fake `ssh` and `rsync` programs on PATH:
//! no network, no machines.

use std::process::Command;

fn fed_binary() -> &'static str {
    env!("CARGO_BIN_EXE_fed")
}

fn help_text(args: &[&str]) -> String {
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
            let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
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
    /// for a deleted machine.
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
case "$opts " in
  *" -O check "*) exit 1 ;;
  *" -O exit "*) exit 0 ;;
  *" -f -N "*) exit 0 ;;
  *" -L "*) exit 255 ;;
esac
case "$last" in
  "fed --version") echo "fed ${FAKE_VM_FED:-999.0.0}" ;;
  *"fed ports list --json"*) echo '{"WEB_PORT": 18080, "DB_PORT": 15432}' ;;
  *) cat > "$FAKE_LOG.stdin.$$" ;;
esac
exit 0
"#;

    const FAKE_RSYNC: &str = r#"#!/bin/sh
case " $* " in
  *" --version "*) echo "${FAKE_RSYNC_VERSION:-rsync  version 3.2.7  protocol version 31}"; exit 0 ;;
esac
line=rsync; for a in "$@"; do line="$line [$a]"; done
printf '%s CHOSEN_RSYNC=%s\n' "$line" "$CHOSEN_RSYNC" >> "$FAKE_LOG"
cat > "$FAKE_LOG.rsync-list"
exit 0
"#;

    /// A fake home with a git checkout `app` linked to acme/web, and the fake
    /// `ssh` and `rsync` in `bin`.
    struct Setup {
        home: TempDir,
        checkout: PathBuf,
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
            let ok = Command::new("git")
                .args(["init", "-q"])
                .arg(&checkout)
                .status()
                .unwrap()
                .success();
            assert!(ok);
            let bin = home.path().join("bin");
            std::fs::create_dir_all(&bin).unwrap();
            for (name, script) in [("ssh", FAKE_SSH), ("rsync", FAKE_RSYNC)] {
                let path = bin.join(name);
                std::fs::write(&path, script).unwrap();
                std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
            }
            Self { home, checkout }
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
            let output = Command::new(fed_binary())
                .arg("--workdir")
                .arg(&self.checkout)
                .args(args)
                .env("HOME", self.home())
                .env("PATH", path)
                .env("FAKE_LOG", self.log_path())
                .env("FED_TOKEN", "fed_test-token")
                .env("FED_CLOUD_URL", url)
                .envs(env.iter().copied())
                .output()
                .unwrap();
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

    /// Each port gets its own ssh with a local forward. When every forward
    /// stops and the cloud still lists the environment, `connect` says so.
    #[test]
    fn connect_forwards_each_port_and_reports_when_all_stop() {
        let setup = Setup::new();
        setup.save("env-1", "box", "web");
        let cloud = Cloud::start(vec![(WEB, "200 OK", list(&[environment("env-1", "box")]))]);
        let (ok, output) = setup.fed(&cloud.url, &["remote", "connect", "box"], &[]);
        assert!(!ok, "{output}");
        assert!(
            output.contains("Error: every port forward to box stopped."),
            "{output}"
        );
        assert!(!output.contains("Connected."), "{output}");

        let calls = setup.calls();
        let forwards: Vec<&String> = calls.iter().filter(|c| c.contains(" [-L] ")).collect();
        assert_eq!(forwards.len(), 2, "{calls:#?}");
        for (local, remote) in [(25432, 15432), (28080, 18080)] {
            let forward = forwards
                .iter()
                .find(|c| c.contains(&format!("[-L] [{local}:127.0.0.1:{remote}]")))
                .unwrap_or_else(|| panic!("no forward for {remote}: {forwards:#?}"));
            for option in [
                "[BatchMode=yes]",
                "[ExitOnForwardFailure=yes]",
                "[ControlPath=none]",
                "[-N]",
            ] {
                assert!(forward.contains(option), "{option}: {forward}");
            }
        }
        assert!(
            calls
                .iter()
                .any(|c| c.ends_with(" [cd /srv/app && fed ports list --json]")),
            "{calls:#?}"
        );
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
                    "Error: box was deleted (it was idle for 5 minutes or reached its 6-hour limit). Create it again with `fed remote up box`.\n"
                ),
                "{command}: {output}"
            );
            assert!(
                setup.remote_dirs().is_empty(),
                "{command}: {:?}",
                setup.remote_dirs()
            );
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
        assert!(
            lines[1].starts_with("box ") && lines[1].ends_with("you"),
            "{output}"
        );
        assert!(
            lines[2].starts_with("bobs ") && lines[2].ends_with("Bob"),
            "{output}"
        );
        assert!(output.contains("203.0.113.7:23456"), "{output}");
        assert_eq!(setup.remote_dirs(), vec!["env-live", "env-other"]);
    }
}
