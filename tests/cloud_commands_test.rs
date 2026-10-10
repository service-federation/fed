//! Integration tests for `fed login/logout/whoami/link/secrets` surfaces
//! that don't need a network.

use std::process::Command;
use tempfile::TempDir;

fn fed_binary() -> &'static str {
    env!("CARGO_BIN_EXE_fed")
}

/// whoami without credentials says so and exits 0 (informational, not an error).
#[test]
fn test_whoami_signed_out() {
    let tmp = TempDir::new().unwrap();
    let output = Command::new(fed_binary())
        .args(["whoami"])
        .env("HOME", tmp.path()) // no ~/.fed/credentials
        .env_remove("FED_TOKEN")
        .output()
        .unwrap();
    assert!(output.status.success());
    let combined = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        combined.contains("fed login"),
        "should hint at fed login: {combined}"
    );
}

/// link with an explicit target writes .fed/cloud.yaml without needing auth.
#[test]
fn test_link_writes_cloud_yaml() {
    let tmp = TempDir::new().unwrap();
    let output = Command::new(fed_binary())
        .args([
            "--workdir",
            tmp.path().to_str().unwrap(),
            "link",
            "acme/web",
        ])
        .env("HOME", tmp.path())
        .env_remove("FED_TOKEN")
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let written = std::fs::read_to_string(tmp.path().join(".fed/cloud.yaml")).unwrap();
    assert!(written.contains("org: acme"));
    assert!(written.contains("project: web"));
    assert!(written.contains("secret_cache: memory"));

    // fed self-manages .fed/.gitignore: everything ignored except cloud.yaml
    // (and the .gitignore itself).
    let gitignore = std::fs::read_to_string(tmp.path().join(".fed/.gitignore")).unwrap();
    assert_eq!(gitignore, "*\n!cloud.yaml\n!.gitignore\n");
}

#[test]
fn cloud_config_memory_policy_removes_and_refuses_the_file_cache() {
    assert_memory_policy_for_link("org: acme\nproject: web\nsecret_cache: memory\n");
}

/// A cloud.yaml without a `secret_cache` key gets the memory default.
#[test]
fn cloud_config_without_policy_defaults_to_memory() {
    assert_memory_policy_for_link("org: acme\nproject: web\n");
}

fn assert_memory_policy_for_link(cloud_yaml: &str) {
    let tmp = TempDir::new().unwrap();
    std::fs::create_dir_all(tmp.path().join(".fed")).unwrap();
    std::fs::write(tmp.path().join(".fed/cloud.yaml"), cloud_yaml).unwrap();
    std::fs::write(
        tmp.path().join(".fed/secrets.cache.env"),
        "API_KEY=must_not_be_used\n",
    )
    .unwrap();
    std::fs::write(
        tmp.path().join("fed.yaml"),
        "parameters:\n  API_KEY:\n    type: secret\n    source: manual\nservices:\n  app:\n    process: echo ok\n    environment:\n      API_KEY: '{{API_KEY}}'\nentrypoint: app\n",
    )
    .unwrap();

    let config_path = tmp.path().join("fed.yaml");
    let output = Command::new(fed_binary())
        .args([
            "--workdir",
            tmp.path().to_str().unwrap(),
            "--config",
            config_path.to_str().unwrap(),
            "--offline",
            "start",
            "--dry-run",
        ])
        .env("HOME", tmp.path())
        .env_remove("FED_TOKEN")
        .output()
        .unwrap();

    let combined = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(!output.status.success(), "{combined}");
    assert!(
        !tmp.path().join(".fed/secrets.cache.env").exists(),
        "cloud-config memory policy must remove and bypass an existing file cache: {combined}"
    );
    assert!(combined.contains("API_KEY"), "{combined}");
    assert!(!combined.contains("must_not_be_used"), "{combined}");
}

/// link rejects malformed targets.
#[test]
fn test_link_rejects_bad_target() {
    let tmp = TempDir::new().unwrap();
    let output = Command::new(fed_binary())
        .args([
            "--workdir",
            tmp.path().to_str().unwrap(),
            "link",
            "not-a-path",
        ])
        .env("HOME", tmp.path())
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("org/project"));
}

/// secrets ls without login fails with the login hint.
#[test]
fn test_secrets_ls_requires_login() {
    let tmp = TempDir::new().unwrap();
    let output = Command::new(fed_binary())
        .args(["--workdir", tmp.path().to_str().unwrap(), "secrets", "ls"])
        .env("HOME", tmp.path())
        .env_remove("FED_TOKEN")
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("fed login"));
}

/// logout when not signed in is a no-op, not an error.
#[test]
fn test_logout_signed_out() {
    let tmp = TempDir::new().unwrap();
    let output = Command::new(fed_binary())
        .args(["logout"])
        .env("HOME", tmp.path())
        .env_remove("FED_TOKEN")
        .output()
        .unwrap();
    assert!(output.status.success());
}

// ── fed secrets set / rm against a stub vault ───────────────────────────

const SECRET: &str = "s3cr3t-value-must-not-leak";

/// One-shot stub vault on loopback: answers `status_line` with `body` and
/// returns the raw request (headers and body) through the channel.
fn stub_vault(
    status_line: &'static str,
    body: &'static str,
) -> (String, std::sync::mpsc::Receiver<String>) {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
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
    (url, rx)
}

/// A linked checkout whose vault cache holds an old API_KEY and an unrelated
/// OTHER entry.
fn linked_checkout() -> TempDir {
    let tmp = TempDir::new().unwrap();
    std::fs::create_dir_all(tmp.path().join(".fed")).unwrap();
    std::fs::write(
        tmp.path().join(".fed/cloud.yaml"),
        "org: acme\nproject: web\nsecret_cache: file\n",
    )
    .unwrap();
    std::fs::write(
        tmp.path().join(".fed/secrets.cache.env"),
        "# fetched-at API_KEY 1721000000\nAPI_KEY=old\n# fetched-at OTHER 1721000000\nOTHER=kept\n",
    )
    .unwrap();
    tmp
}

/// Run `fed secrets <args>` in `dir` against `url`, feeding `stdin`.
fn fed_secrets(dir: &TempDir, url: &str, args: &[&str], stdin: &str) -> (bool, String) {
    use std::io::Write;
    use std::process::Stdio;
    let mut child = Command::new(fed_binary())
        .args(["--workdir", dir.path().to_str().unwrap(), "secrets"])
        .args(args)
        .env("HOME", dir.path())
        .env("FED_TOKEN", "fed_test-token")
        .env("FED_CLOUD_URL", url)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    child
        .stdin
        .take()
        .unwrap()
        .write_all(stdin.as_bytes())
        .unwrap();
    let output = child.wait_with_output().unwrap();
    let combined = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    (output.status.success(), combined)
}

fn cache(dir: &TempDir) -> String {
    std::fs::read_to_string(dir.path().join(".fed/secrets.cache.env")).unwrap_or_default()
}

#[test]
fn secrets_set_sends_stdin_minus_one_newline_and_drops_the_cached_value() {
    let dir = linked_checkout();
    let (url, rx) = stub_vault("201 Created", "{\"ok\":true}");
    let (ok, output) = fed_secrets(&dir, &url, &["set", "API_KEY"], &format!("{SECRET}\n\n"));
    assert!(ok, "{output}");
    assert!(output.contains("Set API_KEY in acme/web"), "{output}");
    assert!(!output.contains(SECRET), "value printed: {output}");

    let request = rx.recv().unwrap();
    assert!(
        request.starts_with("PUT /api/v1/orgs/acme/projects/web/secrets/API_KEY HTTP/1.1\r\n"),
        "{request}"
    );
    let body = request.split_once("\r\n\r\n").unwrap().1;
    let json: serde_json::Value = serde_json::from_str(body).unwrap();
    assert_eq!(
        json["value"],
        format!("{SECRET}\n"),
        "only one newline is stripped"
    );

    let cache = cache(&dir);
    assert!(
        !cache.contains("API_KEY"),
        "stale cache entry kept: {cache}"
    );
    assert!(
        cache.contains("OTHER=kept"),
        "unrelated entry lost: {cache}"
    );
}

#[test]
fn secrets_set_refused_by_the_vault_never_prints_the_value_or_touches_the_cache() {
    for (status, body, expected) in [
        (
            "403 Forbidden",
            "{\"error\":\"admin_only\"}",
            "only org admins",
        ),
        (
            "401 Unauthorized",
            "{\"error\":\"unauthenticated\"}",
            "fed login",
        ),
        ("404 Not Found", "{\"error\":\"project\"}", "does not exist"),
        (
            "429 Too Many Requests",
            "{\"error\":\"rate_limited\"}",
            "too many requests",
        ),
        ("500 Internal Server Error", "{}", "500"),
    ] {
        let dir = linked_checkout();
        let (url, _rx) = stub_vault(status, body);
        let (ok, output) = fed_secrets(&dir, &url, &["set", "API_KEY"], SECRET);
        assert!(!ok, "{status}: {output}");
        assert!(output.contains(expected), "{status}: {output}");
        assert!(
            !output.contains(SECRET),
            "{status} printed the value: {output}"
        );
        assert!(
            cache(&dir).contains("API_KEY=old"),
            "{status} touched the cache"
        );
    }
}

#[test]
fn secrets_set_refuses_an_empty_value_without_a_request() {
    let dir = linked_checkout();
    // Nothing listens here, so a request would fail with "cannot reach".
    let (ok, output) = fed_secrets(&dir, "http://127.0.0.1:9", &["set", "API_KEY"], "\n");
    assert!(!ok);
    assert!(output.contains("empty"), "{output}");
    assert!(!output.contains("cannot reach"), "{output}");
}

#[test]
fn secrets_rm_deletes_and_drops_the_cached_value() {
    let dir = linked_checkout();
    let (url, rx) = stub_vault("200 OK", "{\"ok\":true}");
    let (ok, output) = fed_secrets(&dir, &url, &["rm", "API_KEY"], "");
    assert!(ok, "{output}");
    assert!(output.contains("Removed API_KEY from acme/web"), "{output}");
    assert!(
        rx.recv()
            .unwrap()
            .starts_with("DELETE /api/v1/orgs/acme/projects/web/secrets/API_KEY HTTP/1.1\r\n")
    );
    let cache = cache(&dir);
    assert!(!cache.contains("API_KEY"), "{cache}");
    assert!(cache.contains("OTHER=kept"), "{cache}");
}

/// A name the vault does not have is still dropped from the local cache,
/// then reported as not set.
#[test]
fn secrets_rm_of_an_unset_name_still_drops_the_cached_value() {
    let dir = linked_checkout();
    let (url, _rx) = stub_vault("404 Not Found", "{\"error\":\"secret\"}");
    let (ok, output) = fed_secrets(&dir, &url, &["rm", "API_KEY"], "");
    assert!(!ok, "{output}");
    assert!(
        output.contains("API_KEY is not set in acme/web"),
        "{output}"
    );
    let cache = cache(&dir);
    assert!(!cache.contains("API_KEY"), "{cache}");
    assert!(cache.contains("OTHER=kept"), "{cache}");
}

#[test]
fn secrets_set_requires_login() {
    let tmp = TempDir::new().unwrap();
    let output = Command::new(fed_binary())
        .args([
            "--workdir",
            tmp.path().to_str().unwrap(),
            "secrets",
            "set",
            "API_KEY",
        ])
        .env("HOME", tmp.path())
        .env_remove("FED_TOKEN")
        .stdin(std::process::Stdio::null())
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("fed login"));
}
