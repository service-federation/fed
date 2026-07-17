//! `fed login` / `fed logout` / `fed whoami` — Service Federation Cloud auth.

use crate::output::UserOutput;
use anyhow::{anyhow, bail, Result};
use fed::cloud;
use std::io::{BufRead, BufReader, Write};
use std::net::TcpListener;

fn random_state() -> String {
    use rand::Rng;
    const CHARSET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
    let mut rng = rand::thread_rng();
    (0..32)
        .map(|_| CHARSET[rng.gen_range(0..CHARSET.len())] as char)
        .collect()
}

fn hostname() -> String {
    std::process::Command::new("hostname")
        .output()
        .ok()
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "this machine".to_string())
}

fn open_browser(url: &str) -> bool {
    #[cfg(target_os = "macos")]
    let cmd = "open";
    #[cfg(not(target_os = "macos"))]
    let cmd = "xdg-open";
    std::process::Command::new(cmd)
        .arg(url)
        .spawn()
        .map(|_| true)
        .unwrap_or(false)
}

fn http_response(status: u16, body: &str) -> String {
    let reason = if status == 200 { "OK" } else { "Bad Request" };
    format!(
        "HTTP/1.1 {} {}\r\nContent-Type: text/html; charset=utf-8\r\nContent-Length: {}\r\nCache-Control: no-store\r\nConnection: close\r\n\r\n{}",
        status,
        reason,
        body.len(),
        body
    )
}

/// Wait for the browser to hit http://127.0.0.1:<port>/callback?token=..&state=..
/// One request, hand-parsed — not a web server.
fn wait_for_callback(listener: TcpListener, expected_state: &str) -> Result<String> {
    // Non-blocking accept + poll, so an abandoned browser flow actually hits the
    // deadline instead of blocking in accept() forever.
    listener.set_nonblocking(true)?;
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(300);
    loop {
        if std::time::Instant::now() > deadline {
            bail!("timed out waiting for the browser (5 minutes) — run `fed login` again");
        }
        let (mut stream, _) = match listener.accept() {
            Ok(conn) => conn,
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                std::thread::sleep(std::time::Duration::from_millis(100));
                continue;
            }
            Err(e) => return Err(e.into()),
        };
        // The accepted socket inherits O_NONBLOCK on some platforms; clear it and
        // bound the read so a half-open connection can't stall the flow either.
        stream.set_nonblocking(false)?;
        stream.set_read_timeout(Some(std::time::Duration::from_secs(10)))?;
        let mut reader = BufReader::new(stream.try_clone()?);
        let mut request_line = String::new();
        reader.read_line(&mut request_line)?;
        // "GET /callback?token=...&state=... HTTP/1.1"
        let path = request_line.split_whitespace().nth(1).unwrap_or("");
        if !path.starts_with("/callback?") {
            let _ = stream.write_all(b"HTTP/1.1 404 Not Found\r\nConnection: close\r\n\r\n");
            continue;
        }
        let mut token = None;
        let mut state = None;
        for pair in path.trim_start_matches("/callback?").split('&') {
            match pair.split_once('=') {
                Some(("token", v)) => token = Some(v.to_string()),
                Some(("state", v)) => state = Some(v.to_string()),
                _ => {}
            }
        }
        // Validate before telling the browser it worked.
        if state.as_deref() != Some(expected_state) {
            let body = "<!doctype html><meta charset=utf-8><title>fed login</title>\
                <body style=\"font-family:sans-serif;padding:40px\">\
                <h2>Login failed.</h2><p>Return to your terminal and run <code>fed login</code> again.</p>";
            let _ = stream.write_all(http_response(400, body).as_bytes());
            bail!("state mismatch in login callback — run `fed login` again");
        }
        let body = "<!doctype html><meta charset=utf-8><title>fed login</title>\
            <body style=\"font-family:sans-serif;padding:40px\">\
            <h2>Signed in.</h2><p>You can close this tab and return to your terminal.</p>";
        let _ = stream.write_all(http_response(200, body).as_bytes());
        return token.ok_or_else(|| anyhow!("no token in login callback"));
    }
}

pub async fn run_login(
    no_browser: bool,
    url_override: Option<String>,
    out: &dyn UserOutput,
) -> Result<()> {
    let base_url = url_override.unwrap_or_else(|| {
        std::env::var("FED_CLOUD_URL").unwrap_or_else(|_| cloud::DEFAULT_URL.to_string())
    });
    let name = hostname();

    let token = if no_browser {
        let authorize = format!(
            "{}/cli/authorize?name={}",
            base_url,
            urlencoding_encode(&name)
        );
        out.status("Open this URL, sign in, and authorize fed:");
        out.status(&format!("  {}", authorize));
        out.blank();
        eprint!("Paste the token here: ");
        let mut input = String::new();
        std::io::stdin().read_line(&mut input)?;
        input.trim().to_string()
    } else {
        let listener = TcpListener::bind("127.0.0.1:0")?;
        let port = listener.local_addr()?.port();
        let state = random_state();
        let authorize = format!(
            "{}/cli/authorize?port={}&state={}&name={}",
            base_url,
            port,
            state,
            urlencoding_encode(&name)
        );
        out.status("Opening your browser to sign in…");
        out.status(&format!("  {}", authorize));
        if !open_browser(&authorize) {
            out.warning("Could not open a browser — open the URL above manually.");
        }
        // Blocking accept on a dedicated thread so tokio stays happy.
        let expected = state.clone();
        tokio::task::spawn_blocking(move || wait_for_callback(listener, &expected)).await??
    };

    if token.is_empty() {
        bail!("no token received");
    }

    let creds = cloud::Credentials {
        url: base_url,
        token,
    };
    let me = cloud::whoami(&creds).await?;
    cloud::save_credentials(&creds)?;

    let who = me
        .user
        .email
        .or(me.user.name)
        .unwrap_or_else(|| "you".to_string());
    out.success(&format!("Signed in as {}", who));
    if me.orgs.is_empty() {
        out.status("You're not in any org yet — create one at the dashboard.");
    } else {
        let orgs: Vec<String> = me.orgs.iter().map(|o| o.slug.clone()).collect();
        out.status(&format!("Orgs: {}", orgs.join(", ")));
    }
    Ok(())
}

/// What `logout_flow` did, so the caller can report it honestly. Kept separate
/// from the printing so the flow (revoke classification + always-delete-local)
/// is unit-testable without touching `~/.fed`.
enum LogoutReport {
    /// No stored credential and nothing on disk to remove.
    NotSignedIn,
    /// No parseable stored credential, but a local file existed and was removed.
    RemovedLocalOnly,
    /// Server confirmed the token is dead and the local credential was removed.
    RevokedAndRemoved,
    /// Local credential removed, but server revocation did not take effect
    /// (network/offline/429/…); carries the short reason.
    RemovedRevokeFailed(String),
}

/// Core of `fed logout`: attempt server-side revocation of the stored token,
/// then ALWAYS remove the local credential — even when revocation failed (the
/// plan is explicit: remove locally regardless). `delete` returns whether a
/// local file was actually removed. `offline` (or an empty cloud URL) skips the
/// network attempt entirely and reports it as a failed revoke, reason "offline".
async fn logout_flow(
    stored: Option<cloud::Credentials>,
    offline: bool,
    delete: impl FnOnce() -> Result<bool>,
) -> Result<LogoutReport> {
    let Some(creds) = stored else {
        // No credential we own. Preserve today's behavior: clean up a file if one
        // somehow exists, otherwise report not signed in.
        return Ok(if delete()? {
            LogoutReport::RemovedLocalOnly
        } else {
            LogoutReport::NotSignedIn
        });
    };

    let revocation = if offline || creds.url.is_empty() {
        cloud::Revocation::Failed("offline".to_string())
    } else {
        cloud::revoke_current_token(&creds).await
    };

    // Always remove the local credential, regardless of the revoke result.
    delete()?;

    Ok(match revocation {
        cloud::Revocation::Revoked => LogoutReport::RevokedAndRemoved,
        cloud::Revocation::Failed(reason) => LogoutReport::RemovedRevokeFailed(reason),
    })
}

pub async fn run_logout(offline: bool, out: &dyn UserOutput) -> Result<()> {
    let stored = cloud::load_stored_credentials();
    let delete = || cloud::delete_credentials().map_err(anyhow::Error::from);
    match logout_flow(stored, offline, delete).await? {
        LogoutReport::NotSignedIn => out.status("Not signed in."),
        LogoutReport::RemovedLocalOnly => {
            out.success("Logged out — local credentials removed.")
        }
        LogoutReport::RevokedAndRemoved => {
            out.success("Logged out (token revoked server-side).")
        }
        LogoutReport::RemovedRevokeFailed(reason) => out.warning(&format!(
            "Logged out locally; server revocation failed ({}) — the token may remain valid until expiry.",
            reason
        )),
    }
    Ok(())
}

pub async fn run_whoami(out: &dyn UserOutput) -> Result<()> {
    let Some(creds) = cloud::load_credentials() else {
        out.status("Not signed in — run `fed login`.");
        return Ok(());
    };
    let me = cloud::whoami(&creds).await?;
    let who = me
        .user
        .email
        .or(me.user.name)
        .unwrap_or_else(|| "unknown".to_string());
    out.success(&format!("{} ({})", who, creds.url));
    for org in me.orgs {
        out.status(&format!("  {} ({}) — {}", org.slug, org.name, org.role));
    }
    Ok(())
}

/// Minimal percent-encoding for query values (avoids a dependency).
fn urlencoding_encode(s: &str) -> String {
    s.bytes()
        .map(|b| match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                (b as char).to_string()
            }
            _ => format!("%{:02X}", b),
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{Duration, Instant};

    /// One-shot HTTP server: replies to the first request with `status_line`
    /// (e.g. "200 OK") and `body`, then closes. Returns the base URL.
    fn spawn_one_shot(status_line: &'static str, body: &'static str) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        std::thread::spawn(move || {
            use std::io::Read;
            if let Ok((mut stream, _)) = listener.accept() {
                let mut buf = [0u8; 1024];
                let _ = stream.read(&mut buf);
                let resp = format!(
                    "HTTP/1.1 {}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    status_line,
                    body.len(),
                    body
                );
                let _ = stream.write_all(resp.as_bytes());
            }
        });
        format!("http://127.0.0.1:{}", port)
    }

    fn creds_at(url: String) -> cloud::Credentials {
        cloud::Credentials {
            url,
            token: "super-secret-token".to_string(),
        }
    }

    /// No stored credential and nothing on disk: unchanged "not signed in"
    /// behavior, and delete is never expected to have removed anything.
    #[tokio::test]
    async fn logout_no_credentials_is_not_signed_in() {
        let report = logout_flow(None, false, || Ok(false)).await.unwrap();
        assert!(matches!(report, LogoutReport::NotSignedIn));
    }

    /// 200 revoked:true → the success ("revoked server-side") path, and the
    /// local credential is deleted.
    #[tokio::test]
    async fn logout_200_revokes_and_removes_local() {
        let url = spawn_one_shot("200 OK", "{\"revoked\":true}");
        let deleted = std::cell::Cell::new(false);
        let report = logout_flow(Some(creds_at(url)), false, || {
            deleted.set(true);
            Ok(true)
        })
        .await
        .unwrap();
        assert!(matches!(report, LogoutReport::RevokedAndRemoved));
        assert!(deleted.get(), "local credential must be removed");
    }

    /// 429 → the failed-revoke path, and the local file is STILL deleted. Proven
    /// against a real temp file so the delete is genuinely exercised.
    #[tokio::test]
    async fn logout_429_fails_revoke_but_still_deletes_local() {
        let url = spawn_one_shot("429 Too Many Requests", "{\"error\":\"rate_limited\"}");
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("credentials");
        std::fs::write(&path, "url: x\ntoken: y\n").unwrap();
        let report = logout_flow(Some(creds_at(url)), false, || {
            Ok(std::fs::remove_file(&path).is_ok())
        })
        .await
        .unwrap();
        match report {
            LogoutReport::RemovedRevokeFailed(reason) => {
                assert!(
                    !reason.contains("super-secret-token"),
                    "reason leaked the token"
                );
            }
            _ => panic!("429 must classify as a failed revoke"),
        }
        assert!(
            !path.exists(),
            "local credential must be deleted even when revoke fails"
        );
    }

    /// Connection refused → failed-revoke path, fast, local still deleted.
    #[tokio::test]
    async fn logout_connection_refused_fails_fast_and_deletes_local() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        drop(listener);
        let deleted = std::cell::Cell::new(false);
        let start = Instant::now();
        let report = logout_flow(
            Some(creds_at(format!("http://127.0.0.1:{}", port))),
            false,
            || {
                deleted.set(true);
                Ok(true)
            },
        )
        .await
        .unwrap();
        assert!(matches!(report, LogoutReport::RemovedRevokeFailed(_)));
        assert!(deleted.get(), "local credential must be removed");
        assert!(
            start.elapsed() < Duration::from_secs(5),
            "connect-refused must fail fast, took {:?}",
            start.elapsed()
        );
    }

    /// --offline skips the network entirely (reason "offline") yet still removes
    /// the local credential. The URL points at TEST-NET-1, which would hang if
    /// contacted — so completing quickly proves no request was made.
    #[tokio::test]
    async fn logout_offline_skips_network_and_deletes_local() {
        let deleted = std::cell::Cell::new(false);
        let start = Instant::now();
        let report = logout_flow(
            Some(creds_at("http://192.0.2.1:9".to_string())),
            true,
            || {
                deleted.set(true);
                Ok(true)
            },
        )
        .await
        .unwrap();
        match report {
            LogoutReport::RemovedRevokeFailed(reason) => assert_eq!(reason, "offline"),
            _ => panic!("offline must be a failed revoke with reason 'offline'"),
        }
        assert!(deleted.get(), "local credential must be removed");
        assert!(
            start.elapsed() < Duration::from_secs(2),
            "offline must not make a network attempt"
        );
    }
}
