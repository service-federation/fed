//! `fed remote` exists only in builds with the `remote-beta` feature. Without
//! it, the help text has no trace of it. With it, the commands that need only
//! the cloud API run against a local mock server: no network, no machines.

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
}

#[cfg(feature = "remote-beta")]
mod with_feature {
    use super::*;
    use std::os::unix::fs::PermissionsExt;
    use std::path::Path;
    use std::time::Duration;
    use tempfile::TempDir;

    /// One-shot HTTP server: answers `status_line` with `body` and sends back
    /// the whole request.
    fn spawn_answering(
        status_line: &'static str,
        body: String,
    ) -> (String, std::sync::mpsc::Receiver<String>) {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let (tx, rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            use std::io::{Read, Write};
            let Ok((mut stream, _)) = listener.accept() else {
                return;
            };
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
            let _ = tx.send(String::from_utf8_lossy(&buf).to_string());
            let resp = format!(
                "HTTP/1.1 {status_line}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            let _ = stream.write_all(resp.as_bytes());
        });
        (format!("http://127.0.0.1:{port}"), rx)
    }

    /// A git checkout linked to acme/web, inside a fake home directory.
    fn linked_checkout() -> (TempDir, std::path::PathBuf) {
        let home = TempDir::new().unwrap();
        let checkout = home.path().join("app");
        std::fs::create_dir_all(checkout.join(".fed")).unwrap();
        std::fs::write(
            checkout.join(".fed/cloud.yaml"),
            "org: acme\nproject: web\n",
        )
        .unwrap();
        let ok = Command::new("git")
            .args(["init", "-q"])
            .arg(&checkout)
            .status()
            .unwrap()
            .success();
        assert!(ok);
        (home, checkout)
    }

    fn fed(home: &Path, checkout: &Path, url: &str, args: &[&str]) -> (bool, String) {
        let output = Command::new(fed_binary())
            .arg("--workdir")
            .arg(checkout)
            .args(args)
            .env("HOME", home)
            .env("FED_TOKEN", "fed_test-token")
            .env("FED_CLOUD_URL", url)
            .output()
            .unwrap();
        let combined = format!(
            "{}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        (output.status.success(), combined)
    }

    fn remote_dirs(home: &Path) -> Vec<String> {
        std::fs::read_dir(home.join(".fed/remote"))
            .map(|entries| {
                entries
                    .flatten()
                    .map(|e| e.file_name().to_string_lossy().into_owned())
                    .collect()
            })
            .unwrap_or_default()
    }

    /// The closed-beta refusal reaches the user as a plain sentence, the keys
    /// went in the body, and no half-made state stays behind.
    #[test]
    fn up_refused_by_the_closed_beta_sends_keys_and_leaves_no_state() {
        let (home, checkout) = linked_checkout();
        let (url, rx) = spawn_answering(
            "403 Forbidden",
            r#"{"error":"compute_not_enabled"}"#.to_string(),
        );
        let (ok, output) = fed(home.path(), &checkout, &url, &["remote", "up", "box"]);
        assert!(!ok, "{output}");
        assert!(
            output
                .contains("remote environments are in a closed beta and not enabled for this org"),
            "{output}"
        );

        let request = rx.recv_timeout(Duration::from_secs(5)).unwrap();
        let (head, body) = request.split_once("\r\n\r\n").unwrap();
        assert!(
            head.starts_with("POST /api/v1/orgs/acme/projects/web/environments HTTP/1.1\r\n"),
            "{head}"
        );
        let json: serde_json::Value = serde_json::from_str(body).unwrap();
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
        let host_private = json["host_private_key"].as_str().unwrap();
        assert!(host_private.contains("BEGIN OPENSSH PRIVATE KEY"));
        assert!(!output.contains("PRIVATE KEY"), "{output}");

        let root = home.path().join(".fed/remote");
        assert_eq!(
            std::fs::metadata(&root).unwrap().permissions().mode() & 0o777,
            0o700
        );
        assert!(
            remote_dirs(home.path()).is_empty(),
            "{:?}",
            remote_dirs(home.path())
        );
    }

    #[test]
    fn up_refuses_a_bad_name_before_any_request() {
        let (home, checkout) = linked_checkout();
        let (ok, output) = fed(
            home.path(),
            &checkout,
            "http://127.0.0.1:9",
            &["remote", "up", "Bad_Name"],
        );
        assert!(!ok);
        assert!(output.contains("invalid environment name"), "{output}");
    }

    /// `ls` prints the cloud's list and forgets local keys for this project's
    /// environments the cloud no longer has, but keeps other projects' keys.
    #[test]
    fn ls_prints_the_list_and_drops_state_for_deleted_environments() {
        let (home, checkout) = linked_checkout();
        let root = home.path().join(".fed/remote");
        let save = |id: &str, name: &str, project: &str| {
            let dir = root.join(id);
            std::fs::create_dir_all(&dir).unwrap();
            let state = serde_json::json!({
                "id": id, "name": name, "org": "acme", "project": project,
                "ip": "203.0.113.7", "port": 23456, "deadline": "2099-01-01T00:00:00Z",
            });
            std::fs::write(dir.join("env.json"), state.to_string()).unwrap();
        };
        save("env-live", "box", "web");
        save("env-gone", "old", "web");
        save("env-other", "box", "api");

        let body = serde_json::json!({ "environments": [{
            "id": "env-live", "name": "box", "type": "DEV1-S", "state": "running",
            "ip": "203.0.113.7", "port": 23456,
            "created_at": "2026-10-10T10:00:00Z", "deadline": "2099-01-01T00:00:00Z",
        }]})
        .to_string();
        let (url, rx) = spawn_answering("200 OK", body);
        let (ok, output) = fed(home.path(), &checkout, &url, &["remote", "ls"]);
        assert!(ok, "{output}");
        assert!(output.contains("NAME"), "{output}");
        assert!(output.contains("box"), "{output}");
        assert!(output.contains("203.0.113.7:23456"), "{output}");
        let request = rx.recv_timeout(Duration::from_secs(5)).unwrap();
        assert!(
            request.starts_with("GET /api/v1/orgs/acme/projects/web/environments HTTP/1.1\r\n"),
            "{request}"
        );

        let mut dirs = remote_dirs(home.path());
        dirs.sort();
        assert_eq!(dirs, vec!["env-live", "env-other"]);
    }
}
