//! `fed login` / `fed logout` / `fed whoami` — Service Federation Cloud auth.

use crate::output::UserOutput;
use anyhow::{Result, anyhow, bail};
use fed::cloud;
use std::io::{Read, Write};
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

/// Shape check for server-minted login identifiers: `prefix` followed by
/// exactly 43 base64url characters (32 random bytes, base64url, unpadded).
/// Rejecting anything else makes contract drift fail early — and the check
/// happens without ever echoing the received value.
fn valid_prefixed_id(s: &str, prefix: &str) -> bool {
    s.strip_prefix(prefix).is_some_and(|rest| {
        rest.len() == 43
            && rest
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
    })
}

/// Wait for the browser to hit http://127.0.0.1:<port>/callback?code=..&state=..
/// Hand-parsed — not a web server.
///
/// The callback carries a short-lived, single-use EXCHANGE CODE — never the
/// bearer token; the token is only minted later, when the code is redeemed
/// over HTTPS. `state` is CSRF/correlation defense in depth between this CLI
/// and its own loopback callback: it is not a bearer credential and not the
/// single-use mechanism (the server enforces single use atomically).
///
/// Hardening, since anything local can hit this port:
/// - only `GET /callback` is considered; everything else gets a 404 and the
///   listener KEEPS WAITING — a stray probe must not kill the login;
/// - a wrong/missing state or malformed/missing code gets a 400 and the
///   listener keeps waiting too — only the 5-minute deadline ends the flow;
/// - the 200 "Signed in" page is sent ONLY for a valid code+state;
/// - no response or error message ever contains a received value;
/// - each connection is bounded by an ABSOLUTE 10-second deadline and an
///   8 KB cap on the request line (see [`read_request_line`]) — the residual
///   local-DoS bound is one connection at a time for at most 10s each.
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
        // The accepted socket inherits O_NONBLOCK on some platforms; clear it.
        stream.set_nonblocking(false)?;
        let Some(request_line) = read_request_line(&mut stream) else {
            // Timed out, oversized, closed early, or unreadable: drop the
            // connection and keep waiting for the real callback.
            continue;
        };
        // "GET /callback?code=...&state=... HTTP/1.1"
        let mut parts = request_line.split_whitespace();
        let method = parts.next().unwrap_or("");
        let target = parts.next().unwrap_or("");
        let (path, query) = target.split_once('?').unwrap_or((target, ""));
        if method != "GET" || path != "/callback" {
            let _ = stream.write_all(b"HTTP/1.1 404 Not Found\r\nConnection: close\r\n\r\n");
            continue;
        }
        let mut code = None;
        let mut state = None;
        for pair in query.split('&') {
            match pair.split_once('=') {
                Some(("code", v)) => code = Some(v.to_string()),
                Some(("state", v)) => state = Some(v.to_string()),
                _ => {}
            }
        }
        // Validate BEFORE telling the browser anything worked: the state must
        // be ours and the code must be shaped like a real exchange code. A
        // failed callback gets a 400 (with a static page — never the received
        // values) and the listener continues: a local port-scanner must not
        // be able to abort a login, and the genuine callback still needs the
        // correct state to be accepted.
        let state_ok = state.as_deref() == Some(expected_state);
        let code_ok = code
            .as_deref()
            .is_some_and(|c| valid_prefixed_id(c, "fedac_"));
        if !(state_ok && code_ok) {
            let body = "<!doctype html><meta charset=utf-8><title>fed login</title>\
                <body style=\"font-family:sans-serif;padding:40px\">\
                <h2>Login failed.</h2><p>Return to your terminal and run <code>fed login</code> again.</p>";
            let _ = stream.write_all(http_response(400, body).as_bytes());
            continue;
        }
        let body = "<!doctype html><meta charset=utf-8><title>fed login</title>\
            <body style=\"font-family:sans-serif;padding:40px\">\
            <h2>Signed in.</h2><p>You can close this tab and return to your terminal.</p>";
        let _ = stream.write_all(http_response(200, body).as_bytes());
        return code.ok_or_else(|| anyhow!("no code in login callback"));
    }
}

/// Read the first LF-terminated line of an HTTP request, bounded for the
/// WHOLE connection: an absolute 10-second deadline plus an 8 KB size cap.
///
/// A socket read timeout alone bounds each individual read, not the
/// connection — a client trickling one byte per read (slowloris) would reset
/// it forever. So this uses a manual read loop with a short per-read timeout
/// purely so the absolute deadline gets re-checked between reads. Returns
/// `None` on deadline, overflow, early close, or error; the caller drops the
/// connection and keeps listening.
fn read_request_line(stream: &mut std::net::TcpStream) -> Option<String> {
    const MAX: usize = 8192;
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    stream
        .set_read_timeout(Some(std::time::Duration::from_millis(500)))
        .ok()?;
    let mut buf: Vec<u8> = Vec::with_capacity(1024);
    let mut chunk = [0u8; 1024];
    loop {
        if std::time::Instant::now() > deadline || buf.len() >= MAX {
            return None;
        }
        let want = chunk.len().min(MAX - buf.len());
        match stream.read(&mut chunk[..want]) {
            Ok(0) => return None, // closed before a full request line
            Ok(n) => {
                buf.extend_from_slice(&chunk[..n]);
                if let Some(pos) = buf.iter().position(|&b| b == b'\n') {
                    return Some(String::from_utf8_lossy(&buf[..pos]).into_owned());
                }
            }
            // Per-read timeout (WouldBlock on unix, TimedOut on windows):
            // loop back around so the absolute deadline gets checked.
            Err(e)
                if e.kind() == std::io::ErrorKind::WouldBlock
                    || e.kind() == std::io::ErrorKind::TimedOut =>
            {
                continue;
            }
            Err(_) => return None,
        }
    }
}

/// Mask an email for the login success line: first character + `…@` + domain
/// (`adrian@example.com` → `a…@example.com`). An unparseable address falls
/// back to "you" rather than echoing whatever the server sent.
fn mask_email(email: &str) -> String {
    match email.split_once('@') {
        Some((local, domain)) if !local.is_empty() && !domain.is_empty() => {
            let first = local.chars().next().expect("non-empty local part");
            format!("{}…@{}", first, domain)
        }
        _ => "you".to_string(),
    }
}

/// How `fed login` should behave, bundled so [`login_flow`] stays at a sane
/// arity. `label` identifies this device on the authorization page and in the
/// token list — it travels only in the request body over HTTPS, never in a URL.
struct LoginOptions {
    no_browser: bool,
    print_url: bool,
    label: String,
}

/// Core of `fed login`, separated from `run_login` (which supplies the real
/// browser opener, stdin prompt, and credential file) so the whole flow is
/// unit-testable against stub HTTP servers — the `logout_flow` pattern.
///
/// Output invariants (asserted by tests): the bearer token, the exchange
/// code, and the callback URL are NEVER printed — in any mode, on any path.
/// The authorize URL (safe: it carries only the opaque request id) is printed
/// only when the browser could not be opened, when `--print-url` was given,
/// or always in `--no-browser` mode.
///
/// Ordering after the code is obtained: exchange → stage to the PENDING file
/// → ACTIVATE → PROMOTE pending over the active credentials → whoami. The
/// exchanged token is PROVISIONAL (10-minute server-side expiry) until
/// activated, so a crash before activation strands nothing durable, and
/// staging means the pre-existing working credential is untouched until the
/// new token is proven durable. On failure the previous login keeps working:
/// a failed stage-write is revoked best-effort; a definitively dead token
/// (401) is revoked and its staging file dropped; an AMBIGUOUS activation
/// failure only drops the staging file when the revoke is CONFIRMED —
/// otherwise the pending file is kept for [`recover_pending_login`] to
/// probe, because it may be the only copy of a live token. If only the
/// PROMOTION rename fails, the pending file holds a live activated token: do
/// NOT revoke — the next `fed login` recovers it. A whoami failure after
/// promotion is only a cosmetic loss: the login is already durable, so it
/// downgrades to a warning, never an error.
async fn login_flow(
    base_url: &str,
    opts: &LoginOptions,
    opener: &dyn Fn(&str) -> bool,
    read_code: impl FnOnce() -> Result<String>,
    files: &cloud::CredentialFiles,
    out: &dyn UserOutput,
) -> Result<()> {
    let code = if opts.no_browser {
        let request = checked_auth_request(
            base_url,
            &cloud::AuthRequestBody::manual(opts.label.clone()),
        )
        .await?;
        // Manual mode always prints the URL — there is no browser to open.
        let authorize = authorize_url(base_url, &request);
        out.status("Open this URL, sign in, and approve the request:");
        out.status(&format!("  {}", authorize));
        out.blank();
        read_code()?
    } else {
        // Bind the loopback listener first so the port sent to the server is
        // already ours, then register the authorization request. The label,
        // port, and state travel only in the POST body over HTTPS — the
        // browser URL carries nothing but the opaque request id.
        let listener = TcpListener::bind("127.0.0.1:0")?;
        let port = listener.local_addr()?.port();
        let state = random_state();
        let request = checked_auth_request(
            base_url,
            &cloud::AuthRequestBody::browser(port, state.clone(), opts.label.clone()),
        )
        .await?;
        let authorize = authorize_url(base_url, &request);
        out.status("Opening your browser to sign in…");
        if !opener(&authorize) {
            out.warning("Could not open a browser — open this URL manually:");
            out.status(&format!("  {}", authorize));
        } else if opts.print_url {
            out.status(&format!("  {}", authorize));
        }
        out.status("Waiting for approval in the browser… (times out after 5 minutes)");
        // Blocking accept on a dedicated thread so tokio stays happy.
        let expected = state;
        tokio::task::spawn_blocking(move || wait_for_callback(listener, &expected)).await??
    };

    // The browser path already validated the code shape; re-checking here
    // covers the pasted --no-browser code too, without echoing it.
    if !valid_prefixed_id(&code, "fedac_") {
        bail!("that doesn't look like a sign-in code — run `fed login` again");
    }

    // Redeem the single-use code for the bearer token over HTTPS. The token
    // goes straight into the 0600 credential file — never through a browser,
    // URL, or terminal. A token that isn't shaped like ours means contract
    // drift; treat it like a failed exchange (and never echo it).
    let token = cloud::exchange_code(base_url, &code).await?;
    if !token.starts_with("fed_") {
        bail!("cloud: malformed token from server — run `fed login` again");
    }
    let creds = cloud::Credentials {
        url: base_url.to_string(),
        token,
    };

    // STAGE before ACTIVATE: the provisional token goes to the pending file,
    // never over the active credential — a previous working login must
    // survive anything that goes wrong from here. Staging before activation
    // also means a durable token is never off-disk. If the local write
    // fails, best-effort revoke the provisional token — no orphan.
    if let Err(e) = files.save_pending_credentials(&creds) {
        let _ = cloud::revoke_current_token(&creds).await;
        return Err(e.into());
    }
    match cloud::activate_token(&creds).await {
        cloud::Activation::Activated => {}
        cloud::Activation::Dead => {
            // A definitive 401: the token is provably not live. Best-effort
            // revoke (harmless on a dead token) and drop the staging file;
            // the previous credential file was never touched and keeps
            // working. The message never carries the token.
            let _ = cloud::revoke_current_token(&creds).await;
            let _ = files.delete_pending_credentials();
            bail!("sign-in could not be completed — run `fed login` again");
        }
        cloud::Activation::Failed(_) => {
            // AMBIGUOUS: the activation may have committed while its response
            // was lost — this token could be live for a year. Deleting the
            // pending file is only safe once the server CONFIRMS the token is
            // dead; on anything less, deleting would strand a possibly-live
            // token with no local copy (an unkillable orphan).
            match cloud::revoke_current_token(&creds).await {
                cloud::Revocation::Revoked => {
                    // Confirmed dead — the staging file is now worthless.
                    let _ = files.delete_pending_credentials();
                    bail!("sign-in could not be completed — run `fed login` again");
                }
                cloud::Revocation::Failed(_) => {
                    // Both outcomes unknown: KEEP the pending file. The next
                    // `fed login` probes it (recover_pending_login): live →
                    // promoted, dead → cleaned up. Never the token in the
                    // message.
                    bail!(
                        "sign-in could not be completed or cleaned up — run `fed login` again to finish or clean up"
                    );
                }
            }
        }
    }
    // PROMOTE only now that the token is durable. A failed rename must NOT
    // revoke: the pending file still holds a live, activated token, and the
    // next `fed login` recovers it (see `recover_pending_login`).
    if files.promote_pending_credentials().is_err() {
        bail!(
            "signed in, but the credential file could not be updated — run `fed login` again to finish"
        );
    }
    finish_login(&creds, out).await;
    Ok(())
}

/// The post-login tail: whoami for the privacy-conscious success line —
/// display name if present, else masked email; orgs as a count only
/// (`fed whoami` shows the full detail). The login is already durable when
/// this runs, so a failed whoami downgrades to a warning, never an error.
async fn finish_login(creds: &cloud::Credentials, out: &dyn UserOutput) {
    match cloud::whoami(creds).await {
        Ok(me) => {
            let who = me
                .user
                .name
                .as_deref()
                .filter(|n| !n.trim().is_empty())
                .map(str::to_string)
                .or_else(|| me.user.email.as_deref().map(mask_email))
                .unwrap_or_else(|| "you".to_string());
            out.success(&format!("Signed in as {}.", who));
            match me.orgs.len() {
                0 => out.status("You're not in any org yet — create one at the dashboard."),
                1 => out.status("1 org — run 'fed whoami' for details."),
                n => out.status(&format!("{} orgs — run 'fed whoami' for details.", n)),
            }
        }
        Err(_) => {
            out.success("Signed in.");
            out.warning("Couldn't fetch your account details — run `fed whoami` to see them.");
        }
    }
}

/// Take the cross-process login lock, converting "someone else holds it"
/// into the user-facing error. Two concurrent `fed login` runs would race on
/// the single pending file — A stages and activates its token, B overwrites
/// the pending file, A promotes B's token and strands its own — so the
/// second invocation fails fast instead of queueing.
fn acquire_login_lock(files: &cloud::CredentialFiles) -> Result<cloud::LoginLock> {
    match files.try_lock_login()? {
        Some(lock) => Ok(lock),
        None => bail!("another `fed login` appears to be running — finish it or try again"),
    }
}

/// Crash recovery for staged logins: a pending credential exists only when a
/// previous `fed login` died (or failed the rename) between activation and
/// promotion. Returns `true` when the pending credential completed a login —
/// the caller must then skip the fresh flow entirely.
///
/// - Server confirms durability (200, `activated` true or false — the
///   endpoint is idempotent) → promote and finish: sign-in restored with no
///   new browser round.
/// - Server says the token is dead (401) → delete the stale pending file and
///   let a fresh login proceed.
/// - Unverifiable (network/5xx after bounded retries) → error out, KEEPING
///   the pending file: a fresh flow would overwrite a possibly-live token,
///   and the cloud is unreachable for a fresh login anyway.
async fn recover_pending_login(
    files: &cloud::CredentialFiles,
    out: &dyn UserOutput,
) -> Result<bool> {
    let Some(creds) = files.load_pending_credentials() else {
        return Ok(false);
    };
    match cloud::activate_token(&creds).await {
        cloud::Activation::Activated => {
            files.promote_pending_credentials()?;
            out.status("Restored a sign-in that was interrupted before it finished.");
            finish_login(&creds, out).await;
            Ok(true)
        }
        cloud::Activation::Dead => {
            let _ = files.delete_pending_credentials();
            Ok(false)
        }
        cloud::Activation::Failed(reason) => Err(anyhow!(
            "could not verify an interrupted sign-in ({}) — try again in a moment",
            reason
        )),
    }
}

/// Create an authorization request and validate the returned id's shape
/// (`fedar_` + 43 base64url chars) before it goes anywhere near a URL.
/// Contract drift fails here, early — without echoing the received value.
async fn checked_auth_request(base_url: &str, body: &cloud::AuthRequestBody) -> Result<String> {
    let request = cloud::create_auth_request(base_url, body).await?;
    if !valid_prefixed_id(&request, "fedar_") {
        bail!("cloud: malformed authorize request id from server — run `fed login` again");
    }
    Ok(request)
}

pub async fn run_login(
    no_browser: bool,
    print_url: bool,
    label: Option<String>,
    url_override: Option<String>,
    out: &dyn UserOutput,
) -> Result<()> {
    let base_url = url_override.unwrap_or_else(|| {
        std::env::var("FED_CLOUD_URL").unwrap_or_else(|_| cloud::DEFAULT_URL.to_string())
    });
    let Some(files) = cloud::CredentialFiles::default_paths() else {
        bail!("cannot determine home directory");
    };
    // Cross-process guard over the whole sequence (recovery through
    // promotion): concurrent logins would race on the single pending file.
    // Held until this function returns.
    let _lock = acquire_login_lock(&files)?;
    // A pending credential from an interrupted login completes first — no
    // new browser round if the previous token is still good.
    if recover_pending_login(&files, out).await? {
        return Ok(());
    }
    let opts = LoginOptions {
        no_browser,
        print_url,
        label: label
            .map(|l| l.trim().to_string())
            .filter(|l| !l.is_empty())
            .unwrap_or_else(hostname),
    };
    login_flow(
        &base_url,
        &opts,
        &open_browser,
        || {
            eprint!("Paste the code here: ");
            let mut input = String::new();
            std::io::stdin().read_line(&mut input)?;
            Ok(input.trim().to_string())
        },
        &files,
        out,
    )
    .await
}

/// The browser URL: base + the opaque request id, nothing else — no port, no
/// state, no hostname/label. The id is an unguessable handle, not a
/// credential; approval still requires an authenticated session and a click.
fn authorize_url(base_url: &str, request_id: &str) -> String {
    format!(
        "{}/cli/authorize?request={}",
        base_url,
        urlencoding_encode(request_id)
    )
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

    /// 401 → the failed-revoke path (the endpoint never emits 401, so it no
    /// longer proves the server revoked the token), yet the local credential is
    /// STILL deleted. The reported reason must not leak the token.
    #[tokio::test]
    async fn logout_401_fails_revoke_but_still_deletes_local() {
        let url = spawn_one_shot("401 Unauthorized", "{}");
        let deleted = std::cell::Cell::new(false);
        let report = logout_flow(Some(creds_at(url)), false, || {
            deleted.set(true);
            Ok(true)
        })
        .await
        .unwrap();
        match report {
            LogoutReport::RemovedRevokeFailed(reason) => assert!(
                !reason.contains("super-secret-token"),
                "reason leaked the token"
            ),
            _ => panic!("401 must classify as a failed revoke, not a confirmed revocation"),
        }
        assert!(
            deleted.get(),
            "local credential must be removed even on a failed revoke"
        );
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

    // ── Login flow ────────────────────────────────────────────────────

    use crate::output::RecordingOutput;
    use std::cell::RefCell;
    use std::io::{BufRead, BufReader};
    use std::net::TcpStream;
    use std::sync::{Arc, Mutex};

    // Production-valid shapes (prefix + exactly 43 base64url chars): the CLI
    // now validates these, so non-conforming stub values would silently mask
    // contract regressions. `stub_ids_are_production_valid` keeps them honest.
    const STUB_CODE: &str = "fedac_c0dec0dec0dec0dec0dec0dec0dec0dec0dec0deZx-";
    const STUB_REQUEST: &str = "fedar_reqidreqidreqidreqidreqidreqidreqidreqidQz_";
    const STUB_TOKEN: &str = "fed_stub-bearer-token-value";

    #[test]
    fn stub_ids_are_production_valid() {
        assert!(valid_prefixed_id(STUB_CODE, "fedac_"));
        assert!(valid_prefixed_id(STUB_REQUEST, "fedar_"));
        assert!(STUB_TOKEN.starts_with("fed_"));
    }

    #[test]
    fn valid_prefixed_id_rejects_malformed_ids() {
        assert!(!valid_prefixed_id("", "fedac_"));
        assert!(!valid_prefixed_id("fedac_", "fedac_"));
        assert!(!valid_prefixed_id("fedac_short", "fedac_"));
        // right length, wrong prefix
        assert!(!valid_prefixed_id(STUB_CODE, "fedar_"));
        // right length, invalid character
        let bad = format!("fedac_{}!", &STUB_CODE[7..49]);
        assert!(!valid_prefixed_id(&bad, "fedac_"));
    }

    /// Send a raw HTTP request to the CLI's loopback listener and return the
    /// full response (so tests can assert on status and on what the response
    /// does NOT contain).
    fn hit_raw(port: u16, request: &str) -> String {
        use std::io::Read;
        // The listener is already bound when the port becomes known, so the
        // connect lands in its backlog even before accept() runs.
        let mut stream = TcpStream::connect(("127.0.0.1", port)).unwrap();
        let _ = stream.write_all(request.as_bytes());
        let mut buf = Vec::new();
        let _ = stream.read_to_end(&mut buf);
        String::from_utf8_lossy(&buf).into_owned()
    }

    /// Play the approving browser: hit the CLI's loopback callback with the
    /// exchange code and a state value. Returns the HTTP response.
    fn hit_callback(port: u16, code: &str, state: &str) -> String {
        hit_raw(
            port,
            &format!(
                "GET /callback?code={}&state={} HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n",
                code, state
            ),
        )
    }

    /// Read one HTTP request (line + headers + Content-Length body) off a
    /// stream. Minimal — for the stub cloud server only. Also reports whether
    /// an Authorization header was presented.
    fn read_http_request(stream: &TcpStream) -> (String, bool, String) {
        use std::io::Read;
        let mut reader = BufReader::new(stream.try_clone().unwrap());
        let mut request_line = String::new();
        reader.read_line(&mut request_line).unwrap();
        let mut content_length = 0usize;
        let mut has_auth = false;
        loop {
            let mut line = String::new();
            reader.read_line(&mut line).unwrap();
            let line = line.trim_end().to_ascii_lowercase();
            if line.is_empty() {
                break;
            }
            if let Some(v) = line.strip_prefix("content-length:") {
                content_length = v.trim().parse().unwrap_or(0);
            }
            if line.starts_with("authorization:") {
                has_auth = true;
            }
        }
        let mut body = vec![0u8; content_length];
        if content_length > 0 {
            reader.read_exact(&mut body).unwrap();
        }
        (
            request_line,
            has_auth,
            String::from_utf8_lossy(&body).into_owned(),
        )
    }

    fn respond_json(mut stream: &TcpStream, status_line: &str, body: &str) {
        let resp = format!(
            "HTTP/1.1 {}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
            status_line,
            body.len(),
            body
        );
        let _ = stream.write_all(resp.as_bytes());
    }

    /// Stub of the cloud API with call recording, for full login-flow tests.
    /// Serves every endpoint the flow can touch; behaviors are configurable
    /// so failure paths (activate down, whoami down) can be exercised. When
    /// an authorize request carries port+state it also plays the approving
    /// browser, hitting the CLI's loopback callback with [`STUB_CODE`] and
    /// the CLI's own state. The token is only issued for the right code, so
    /// a flow that completes proves the exchange happened.
    struct StubCloud {
        base: String,
        calls: Arc<Mutex<Vec<String>>>,
    }

    impl StubCloud {
        /// The recorded calls, as "METHOD /path" (+ " (auth)" when the
        /// request carried an Authorization header), in arrival order.
        fn calls(&self) -> Vec<String> {
            self.calls.lock().unwrap().clone()
        }
    }

    fn spawn_stub_cloud() -> StubCloud {
        spawn_stub_cloud_with(200, true)
    }

    fn spawn_stub_cloud_with(activate_status: u16, me_ok: bool) -> StubCloud {
        spawn_stub_cloud_cfg(activate_status, me_ok, true)
    }

    /// `activate_status`: HTTP status for `POST /api/v1/cli/activate`
    /// (200 → activated, 401 → dead token, anything else → 500-style outage).
    /// `revoke_ok`: whether `DELETE /api/v1/cli/session` confirms the revoke
    /// (false models "both responses lost" — the ambiguous worst case).
    fn spawn_stub_cloud_cfg(activate_status: u16, me_ok: bool, revoke_ok: bool) -> StubCloud {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let calls = Arc::new(Mutex::new(Vec::new()));
        let recorded = calls.clone();
        std::thread::spawn(move || {
            loop {
                let Ok((stream, _)) = listener.accept() else {
                    return;
                };
                let (request_line, has_auth, body) = read_http_request(&stream);
                let mut parts = request_line.split_whitespace();
                let method = parts.next().unwrap_or("").to_string();
                let target = parts.next().unwrap_or("");
                let path = target.split('?').next().unwrap_or("").to_string();
                recorded.lock().unwrap().push(format!(
                    "{} {}{}",
                    method,
                    path,
                    if has_auth { " (auth)" } else { "" }
                ));
                match (method.as_str(), path.as_str()) {
                    ("POST", "/api/v1/cli/authorize-request") => {
                        let v: serde_json::Value = serde_json::from_str(&body).unwrap();
                        if let (Some(cb_port), Some(state)) =
                            (v["port"].as_u64(), v["state"].as_str())
                        {
                            let state = state.to_string();
                            std::thread::spawn(move || {
                                hit_callback(cb_port as u16, STUB_CODE, &state)
                            });
                        }
                        respond_json(
                            &stream,
                            "201 Created",
                            &format!("{{\"request\":\"{}\",\"expires_in\":300}}", STUB_REQUEST),
                        );
                    }
                    ("POST", "/api/v1/cli/token") => {
                        let v: serde_json::Value = serde_json::from_str(&body).unwrap();
                        if v["code"].as_str() == Some(STUB_CODE) {
                            respond_json(
                                &stream,
                                "201 Created",
                                &format!("{{\"token\":\"{}\"}}", STUB_TOKEN),
                            );
                        } else {
                            respond_json(&stream, "400 Bad Request", "{\"error\":\"code\"}");
                        }
                    }
                    ("POST", "/api/v1/cli/activate") => match activate_status {
                        200 => respond_json(&stream, "200 OK", "{\"activated\":true}"),
                        401 => respond_json(&stream, "401 Unauthorized", "{}"),
                        _ => respond_json(&stream, "500 Internal Server Error", "{}"),
                    },
                    ("GET", "/api/v1/me") => {
                        if me_ok {
                            respond_json(
                                &stream,
                                "200 OK",
                                "{\"user\":{\"name\":null,\"email\":\"adrian@example.com\"},\
                                 \"orgs\":[{\"slug\":\"acme\",\"name\":\"Acme\",\"role\":\"admin\"},\
                                           {\"slug\":\"beta\",\"name\":\"Beta\",\"role\":\"member\"}]}",
                            );
                        } else {
                            respond_json(&stream, "500 Internal Server Error", "{}");
                        }
                    }
                    ("DELETE", "/api/v1/cli/session") => {
                        if revoke_ok {
                            respond_json(&stream, "200 OK", "{\"revoked\":true}");
                        } else {
                            respond_json(&stream, "500 Internal Server Error", "{}");
                        }
                    }
                    _ => respond_json(&stream, "404 Not Found", "{}"),
                }
            }
        });
        StubCloud {
            base: format!("http://127.0.0.1:{}", port),
            calls,
        }
    }

    fn login_opts(no_browser: bool, print_url: bool) -> LoginOptions {
        LoginOptions {
            no_browser,
            print_url,
            label: "test-device".to_string(),
        }
    }

    /// A real credential-file pair rooted in a fresh temp dir, so tests
    /// exercise the genuine staging/promotion file behavior.
    fn temp_files() -> (tempfile::TempDir, cloud::CredentialFiles) {
        let dir = tempfile::tempdir().unwrap();
        let files = cloud::CredentialFiles::in_dir(dir.path());
        (dir, files)
    }

    /// The token currently in the ACTIVE credentials file, if any.
    fn active_token(dir: &tempfile::TempDir) -> Option<String> {
        let raw = std::fs::read_to_string(dir.path().join("credentials")).ok()?;
        serde_yaml::from_str::<cloud::Credentials>(&raw)
            .ok()
            .map(|c| c.token)
    }

    fn pending_exists(dir: &tempfile::TempDir) -> bool {
        dir.path().join("credentials.pending").exists()
    }

    /// Assert none of the secret material — bearer token, exchange code,
    /// callback URL — appears in the recorded output.
    fn assert_no_secrets(text: &str) {
        assert!(
            !text.contains(STUB_TOKEN),
            "output leaked the bearer token: {}",
            text
        );
        assert!(
            !text.contains(STUB_CODE),
            "output leaked the exchange code: {}",
            text
        );
        assert!(
            !text.contains("/callback"),
            "output leaked the callback URL: {}",
            text
        );
    }

    /// The callback parses `code` (not the old `token`) and returns it when
    /// the state matches — replying 200 only then.
    #[test]
    fn callback_parses_code_when_state_matches() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let browser = std::thread::spawn(move || {
            let resp = hit_callback(port, STUB_CODE, "expected-state-value");
            assert!(
                resp.starts_with("HTTP/1.1 200"),
                "valid callback must get 200: {}",
                resp
            );
        });
        let code = wait_for_callback(listener, "expected-state-value").unwrap();
        assert_eq!(code, STUB_CODE);
        browser.join().unwrap();
    }

    /// Hostile or stray local requests — wrong state, malformed code, wrong
    /// path, wrong method, an oversized request line — each get an error
    /// response that never echoes the received values, and the listener KEEPS
    /// WAITING: a local port-scanner must not be able to abort a login. The
    /// genuine callback afterwards still completes the flow.
    #[test]
    fn callback_survives_bad_requests_then_completes() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let browser = std::thread::spawn(move || {
            // Wrong state (correct-shaped code): 400, nothing echoed.
            let resp = hit_callback(port, STUB_CODE, "attacker-supplied-state");
            assert!(
                resp.starts_with("HTTP/1.1 400"),
                "wrong state must get 400: {}",
                resp
            );
            assert!(
                !resp.contains(STUB_CODE) && !resp.contains("attacker-supplied-state"),
                "400 response echoed received values: {}",
                resp
            );
            // Malformed code with the correct state: still 400, not echoed.
            let resp = hit_callback(port, "not-a-real-code", "expected-state-value");
            assert!(
                resp.starts_with("HTTP/1.1 400"),
                "bad code must get 400: {}",
                resp
            );
            assert!(
                !resp.contains("not-a-real-code"),
                "400 echoed the code: {}",
                resp
            );
            // Missing code entirely: 400.
            let resp = hit_raw(
                port,
                "GET /callback?state=expected-state-value HTTP/1.1\r\nConnection: close\r\n\r\n",
            );
            assert!(
                resp.starts_with("HTTP/1.1 400"),
                "missing code must get 400: {}",
                resp
            );
            // Wrong path: 404.
            let resp = hit_raw(port, "GET /admin HTTP/1.1\r\nConnection: close\r\n\r\n");
            assert!(
                resp.starts_with("HTTP/1.1 404"),
                "wrong path must get 404: {}",
                resp
            );
            // Wrong method on the right path: 404 (only GET is served).
            let resp = hit_raw(port, "POST /callback HTTP/1.1\r\nConnection: close\r\n\r\n");
            assert!(
                resp.starts_with("HTTP/1.1 404"),
                "POST must get 404: {}",
                resp
            );
            // Oversized request line: the 8 KB cap stops the read; the flow
            // survives (response may or may not arrive before the reset).
            let _ = hit_raw(
                port,
                &format!(
                    "GET /{} HTTP/1.1\r\nConnection: close\r\n\r\n",
                    "A".repeat(100_000)
                ),
            );
            // The genuine callback still works after all of the above.
            let resp = hit_callback(port, STUB_CODE, "expected-state-value");
            assert!(
                resp.starts_with("HTTP/1.1 200"),
                "real callback must get 200: {}",
                resp
            );
        });
        let code = wait_for_callback(listener, "expected-state-value").unwrap();
        assert_eq!(code, STUB_CODE, "the login must still complete");
        browser.join().unwrap();
    }

    /// A slowloris-style client (bytes trickled forever, never a full request
    /// line) is cut off near the 10-second absolute per-connection deadline —
    /// a per-read timeout alone would be reset by every byte — and the flow
    /// survives to serve the genuine callback afterwards.
    #[test]
    fn callback_cuts_off_slowloris_and_still_completes() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let browser = std::thread::spawn(move || {
            let start = Instant::now();
            let mut stream = TcpStream::connect(("127.0.0.1", port)).unwrap();
            // Trickle one byte at a time; each write resets a naive per-read
            // timeout, so only an absolute deadline can end this.
            loop {
                if stream.write_all(b"A").is_err() {
                    break; // server hung up on us — the deadline fired
                }
                std::thread::sleep(std::time::Duration::from_millis(200));
                assert!(
                    start.elapsed() < Duration::from_secs(30),
                    "server never cut the slow connection"
                );
            }
            let cut = start.elapsed();
            // Loose bounds for CI: the deadline is 10s; detection lags a
            // write or two behind the close.
            assert!(
                cut >= Duration::from_secs(5) && cut <= Duration::from_secs(25),
                "cutoff should land near the 10s deadline, got {:?}",
                cut
            );
            // The genuine callback still completes the login.
            let resp = hit_callback(port, STUB_CODE, "expected-state-value");
            assert!(
                resp.starts_with("HTTP/1.1 200"),
                "real callback must get 200: {}",
                resp
            );
        });
        let code = wait_for_callback(listener, "expected-state-value").unwrap();
        assert_eq!(code, STUB_CODE, "the login must survive a slowloris client");
        browser.join().unwrap();
    }

    /// Full simulated browser flow: create-request → stub 'browser' hits the
    /// loopback callback → code exchange → whoami. With a succeeding opener,
    /// the authorize URL is NOT printed, and no secret material ever is. The
    /// success line masks the email and gives an org count, not slugs.
    #[tokio::test]
    async fn browser_flow_hides_url_and_secrets_when_open_succeeds() {
        let stub = spawn_stub_cloud();
        let out = RecordingOutput::new();
        let opened = RefCell::new(None::<String>);
        let (dir, files) = temp_files();
        login_flow(
            &stub.base,
            &login_opts(false, false),
            &|url: &str| {
                opened.replace(Some(url.to_string()));
                true
            },
            || Err(anyhow!("the prompt must not be used in browser mode")),
            &files,
            &out,
        )
        .await
        .unwrap();

        // The exchanged token was PROMOTED into the active credentials file
        // and the staging file is gone…
        assert_eq!(active_token(&dir).as_deref(), Some(STUB_TOKEN));
        assert!(
            !pending_exists(&dir),
            "promotion must consume the pending file"
        );
        // …and the token got nowhere near the terminal.
        let text = out.combined();
        assert_no_secrets(&text);

        // The full server-side sequence, in order: register → exchange →
        // ACTIVATE (authenticated, before any success output can be built) →
        // whoami. No revoke.
        assert_eq!(
            stub.calls(),
            vec![
                "POST /api/v1/cli/authorize-request",
                "POST /api/v1/cli/token",
                "POST /api/v1/cli/activate (auth)",
                "GET /api/v1/me (auth)",
            ],
            "unexpected call sequence"
        );

        // The opener got the id-only authorize URL; the output did not.
        let authorize = opened.borrow().clone().expect("opener must be called");
        assert!(authorize.contains(&format!("/cli/authorize?request={}", STUB_REQUEST)));
        assert!(
            !authorize.contains("port") && !authorize.contains("state"),
            "authorize URL must carry only the request id: {}",
            authorize
        );
        assert!(
            !text.contains("/cli/authorize"),
            "authorize URL must not be printed when the browser opened: {}",
            text
        );

        // Privacy-conscious success: masked email, org count, no slugs.
        assert!(
            text.contains("Signed in as a…@example.com."),
            "output: {}",
            text
        );
        assert!(
            !text.contains("adrian@example.com"),
            "full email leaked: {}",
            text
        );
        assert!(text.contains("2 orgs"), "org count missing: {}", text);
        assert!(
            !text.contains("acme") && !text.contains("beta"),
            "org slugs leaked: {}",
            text
        );
    }

    /// When the browser cannot be opened, the authorize URL IS printed (the
    /// user has to get there somehow) — still with no secret material.
    #[tokio::test]
    async fn browser_flow_prints_url_when_open_fails() {
        let stub = spawn_stub_cloud();
        let out = RecordingOutput::new();
        let (_dir, files) = temp_files();
        login_flow(
            &stub.base,
            &login_opts(false, false),
            &|_: &str| false,
            || Err(anyhow!("the prompt must not be used in browser mode")),
            &files,
            &out,
        )
        .await
        .unwrap();
        let text = out.combined();
        assert!(
            text.contains(&format!("/cli/authorize?request={}", STUB_REQUEST)),
            "URL must be printed when the browser fails to open: {}",
            text
        );
        assert_no_secrets(&text);
    }

    /// `--print-url` prints the authorize URL even when the browser opens.
    #[tokio::test]
    async fn browser_flow_print_url_flag_always_prints_url() {
        let stub = spawn_stub_cloud();
        let out = RecordingOutput::new();
        let (_dir, files) = temp_files();
        login_flow(
            &stub.base,
            &login_opts(false, true),
            &|_: &str| true,
            || Err(anyhow!("the prompt must not be used in browser mode")),
            &files,
            &out,
        )
        .await
        .unwrap();
        let text = out.combined();
        assert!(
            text.contains(&format!("/cli/authorize?request={}", STUB_REQUEST)),
            "--print-url must print the URL: {}",
            text
        );
        assert_no_secrets(&text);
    }

    /// --no-browser: the URL is always printed, the pasted code (injected
    /// reader) is exchanged for the token, and neither code nor token reach
    /// the output.
    #[tokio::test]
    async fn no_browser_flow_prints_url_and_exchanges_pasted_code() {
        let stub = spawn_stub_cloud();
        let out = RecordingOutput::new();
        let (dir, files) = temp_files();
        // A pre-existing login: promotion must REPLACE it on success.
        std::fs::write(
            dir.path().join("credentials"),
            "url: https://old.example.com\ntoken: fed_old-working-token\n",
        )
        .unwrap();
        login_flow(
            &stub.base,
            &login_opts(true, false),
            &|_: &str| panic!("no browser may be opened in --no-browser mode"),
            || Ok(STUB_CODE.to_string()),
            &files,
            &out,
        )
        .await
        .unwrap();
        assert_eq!(
            active_token(&dir).as_deref(),
            Some(STUB_TOKEN),
            "promotion must replace the previous credential"
        );
        assert!(
            !pending_exists(&dir),
            "promotion must consume the pending file"
        );
        let text = out.combined();
        assert!(
            text.contains(&format!("/cli/authorize?request={}", STUB_REQUEST)),
            "--no-browser must print the URL: {}",
            text
        );
        assert_no_secrets(&text);
        // The pasted code went through the full exchange + activation.
        assert!(
            stub.calls()
                .contains(&"POST /api/v1/cli/activate (auth)".to_string()),
            "activation must run in --no-browser mode too: {:?}",
            stub.calls()
        );
    }

    /// whoami failing AFTER promotion must not fail the login — the token is
    /// already durable and installed. A generic success line plus a warning
    /// pointing at `fed whoami`, still with zero secret material.
    #[tokio::test]
    async fn whoami_failure_after_activation_is_still_success() {
        let stub = spawn_stub_cloud_with(200, false);
        let out = RecordingOutput::new();
        let (dir, files) = temp_files();
        login_flow(
            &stub.base,
            &login_opts(true, false),
            &|_: &str| panic!("no browser in --no-browser mode"),
            || Ok(STUB_CODE.to_string()),
            &files,
            &out,
        )
        .await
        .expect("whoami failure after activation must not fail the login");
        assert_eq!(active_token(&dir).as_deref(), Some(STUB_TOKEN));
        assert!(!pending_exists(&dir));
        let text = out.combined();
        assert!(
            text.contains("Signed in."),
            "generic success line missing: {}",
            text
        );
        assert!(
            text.contains("fed whoami"),
            "warning should point at fed whoami: {}",
            text
        );
        assert_no_secrets(&text);
        let calls = stub.calls();
        assert!(
            calls.contains(&"POST /api/v1/cli/activate (auth)".to_string()),
            "token must have been activated: {:?}",
            calls
        );
        assert!(
            !calls.contains(&"DELETE /api/v1/cli/session (auth)".to_string()),
            "a durable login must not be revoked: {:?}",
            calls
        );
    }

    /// A failed pending-file write (real file, injected via a read-only dir)
    /// must best-effort revoke the fresh token — no orphaned server-side
    /// credential — and never activate it. Skipped under root, which
    /// bypasses directory permissions.
    #[cfg(unix)]
    #[tokio::test]
    async fn save_failure_revokes_the_fresh_token() {
        use std::os::unix::fs::PermissionsExt;
        let stub = spawn_stub_cloud();
        let out = RecordingOutput::new();
        let (dir, files) = temp_files();

        // Probe: can we still create files in a 0500 dir (i.e. are we root)?
        std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o500)).unwrap();
        let probe = dir.path().join(".probe");
        let blocked = std::fs::File::create(&probe).is_err();
        let _ = std::fs::remove_file(&probe);
        if !blocked {
            std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
            return; // running as root — injection can't work; skip.
        }

        let result = login_flow(
            &stub.base,
            &login_opts(true, false),
            &|_: &str| panic!("no browser in --no-browser mode"),
            || Ok(STUB_CODE.to_string()),
            &files,
            &out,
        )
        .await;
        std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
        let err = result.expect_err("a failed pending write must fail the login");
        assert!(
            !format!("{:#}", err).contains(STUB_TOKEN),
            "save error leaked the token"
        );
        assert_no_secrets(&out.combined());
        assert!(!pending_exists(&dir), "nothing may be left staged");
        let calls = stub.calls();
        assert!(
            calls.contains(&"DELETE /api/v1/cli/session (auth)".to_string()),
            "the fresh token must be revoked when staging fails: {:?}",
            calls
        );
        assert!(
            !calls.contains(&"POST /api/v1/cli/activate (auth)".to_string()),
            "an unsaved token must never be activated: {:?}",
            calls
        );
    }

    /// Ambiguous activation failure whose revoke is CONFIRMED: the token is
    /// provably dead, so the staging file is deleted — and, crucially, a
    /// PRE-EXISTING working credential survives byte-identical: the
    /// provisional token only ever touched the pending file.
    #[tokio::test]
    async fn activate_failure_leaves_previous_credentials_untouched() {
        let stub = spawn_stub_cloud_with(500, true);
        let out = RecordingOutput::new();
        let (dir, files) = temp_files();
        let previous = "url: https://old.example.com\ntoken: fed_old-working-token\n";
        std::fs::write(dir.path().join("credentials"), previous).unwrap();

        let err = login_flow(
            &stub.base,
            &login_opts(true, false),
            &|_: &str| panic!("no browser in --no-browser mode"),
            || Ok(STUB_CODE.to_string()),
            &files,
            &out,
        )
        .await
        .expect_err("failed activation must fail the login");
        let msg = format!("{:#}", err);
        assert!(
            msg.contains("sign-in could not be completed") && msg.contains("fed login"),
            "unexpected activation-failure message: {}",
            msg
        );
        assert!(
            !msg.contains(STUB_TOKEN),
            "activation error leaked the token: {}",
            msg
        );
        assert_no_secrets(&out.combined());

        assert_eq!(
            std::fs::read_to_string(dir.path().join("credentials")).unwrap(),
            previous,
            "the previous working credential must survive byte-identical"
        );
        assert!(
            !pending_exists(&dir),
            "the failed staging file must be deleted"
        );
        let calls = stub.calls();
        assert!(
            calls.contains(&"DELETE /api/v1/cli/session (auth)".to_string()),
            "the provisional token must be best-effort revoked: {:?}",
            calls
        );
    }

    /// Ambiguous activation failure whose revoke ALSO fails (both responses
    /// lost): the pending file may be the only copy of a live one-year
    /// token, so it must SURVIVE for the next `fed login` to probe — and the
    /// error tells the user to re-run. The previous credential is untouched.
    #[tokio::test]
    async fn ambiguous_activation_with_failed_revoke_keeps_pending() {
        let stub = spawn_stub_cloud_cfg(500, true, false);
        let out = RecordingOutput::new();
        let (dir, files) = temp_files();
        let previous = "url: https://old.example.com\ntoken: fed_old-working-token\n";
        std::fs::write(dir.path().join("credentials"), previous).unwrap();

        let err = login_flow(
            &stub.base,
            &login_opts(true, false),
            &|_: &str| panic!("no browser in --no-browser mode"),
            || Ok(STUB_CODE.to_string()),
            &files,
            &out,
        )
        .await
        .expect_err("ambiguous activation must fail the login");
        let msg = format!("{:#}", err);
        assert!(
            msg.contains("fed login"),
            "error must point at re-running fed login: {}",
            msg
        );
        assert!(!msg.contains(STUB_TOKEN), "error leaked the token: {}", msg);
        assert_no_secrets(&out.combined());

        assert!(
            pending_exists(&dir),
            "an unconfirmed revoke must KEEP the pending file — it may hold the only copy of a live token"
        );
        assert_eq!(
            files.load_pending_credentials().unwrap().token,
            STUB_TOKEN,
            "the kept pending file must still hold the possibly-live token"
        );
        assert_eq!(
            std::fs::read_to_string(dir.path().join("credentials")).unwrap(),
            previous,
            "the previous credential must be untouched"
        );
        assert!(
            stub.calls()
                .contains(&"DELETE /api/v1/cli/session (auth)".to_string()),
            "a revoke must have been attempted: {:?}",
            stub.calls()
        );
    }

    /// A definitive 401 from activation: the token is provably dead, so the
    /// staging file is deleted (nothing worth recovering) and the previous
    /// credential is untouched.
    #[tokio::test]
    async fn dead_token_activation_deletes_pending() {
        let stub = spawn_stub_cloud_with(401, true);
        let out = RecordingOutput::new();
        let (dir, files) = temp_files();
        let previous = "url: https://old.example.com\ntoken: fed_old-working-token\n";
        std::fs::write(dir.path().join("credentials"), previous).unwrap();

        let err = login_flow(
            &stub.base,
            &login_opts(true, false),
            &|_: &str| panic!("no browser in --no-browser mode"),
            || Ok(STUB_CODE.to_string()),
            &files,
            &out,
        )
        .await
        .expect_err("a dead token must fail the login");
        let msg = format!("{:#}", err);
        assert!(msg.contains("fed login") && !msg.contains(STUB_TOKEN));
        assert!(
            !pending_exists(&dir),
            "a provably dead token's staging file must be deleted"
        );
        assert_eq!(
            std::fs::read_to_string(dir.path().join("credentials")).unwrap(),
            previous,
            "the previous credential must be untouched"
        );
    }

    /// The cross-process login lock: a second attempt while the lock is held
    /// fails fast with the friendly message and touches neither credentials
    /// file; after a completed flow releases it, a subsequent login can lock
    /// again.
    #[tokio::test]
    async fn login_lock_blocks_second_attempt_until_released() {
        let stub = spawn_stub_cloud();
        let (dir, files) = temp_files();
        let previous = "url: https://old.example.com\ntoken: fed_old-working-token\n";
        std::fs::write(dir.path().join("credentials"), previous).unwrap();

        let guard = acquire_login_lock(&files).expect("first lock must succeed");

        // A second "process": a separate open of the same lock file (flock
        // treats separate open descriptions independently, so this genuinely
        // contends even in-process).
        let files2 = cloud::CredentialFiles::in_dir(dir.path());
        let start = Instant::now();
        let err = acquire_login_lock(&files2).expect_err("second login must not proceed");
        assert!(
            start.elapsed() < Duration::from_secs(1),
            "the second attempt must fail fast, not block; took {:?}",
            start.elapsed()
        );
        assert!(
            format!("{:#}", err).contains("another `fed login`"),
            "friendly contention message expected: {:#}",
            err
        );
        assert_eq!(
            std::fs::read_to_string(dir.path().join("credentials")).unwrap(),
            previous,
            "a refused login must not touch the credentials"
        );
        assert!(
            !pending_exists(&dir),
            "a refused login must not stage anything"
        );

        // Complete a flow under the lock, then release it — a subsequent
        // login can take the lock again.
        let out = RecordingOutput::new();
        login_flow(
            &stub.base,
            &login_opts(true, false),
            &|_: &str| panic!("no browser in --no-browser mode"),
            || Ok(STUB_CODE.to_string()),
            &files,
            &out,
        )
        .await
        .unwrap();
        assert_eq!(active_token(&dir).as_deref(), Some(STUB_TOKEN));
        drop(guard);
        let _relock =
            acquire_login_lock(&files2).expect("the lock must be free after a completed flow");
    }

    /// Crash recovery: a pending credential whose token the server confirms
    /// (activation is idempotent) is promoted on the next `fed login` — no
    /// new browser round — replacing the older active credential.
    #[tokio::test]
    async fn recovery_promotes_pending_with_live_token() {
        let stub = spawn_stub_cloud();
        let out = RecordingOutput::new();
        let (dir, files) = temp_files();
        std::fs::write(
            dir.path().join("credentials"),
            "url: https://old.example.com\ntoken: fed_old-working-token\n",
        )
        .unwrap();
        files
            .save_pending_credentials(&cloud::Credentials {
                url: stub.base.clone(),
                token: STUB_TOKEN.to_string(),
            })
            .unwrap();

        let recovered = recover_pending_login(&files, &out).await.unwrap();
        assert!(
            recovered,
            "a live pending credential must complete the login"
        );
        assert_eq!(active_token(&dir).as_deref(), Some(STUB_TOKEN));
        assert!(!pending_exists(&dir));
        let calls = stub.calls();
        assert!(
            calls.contains(&"POST /api/v1/cli/activate (auth)".to_string()),
            "recovery must verify via the activate endpoint: {:?}",
            calls
        );
        let text = out.combined();
        assert!(
            text.contains("interrupted"),
            "the user must be told the sign-in was restored: {}",
            text
        );
        assert_no_secrets(&text);
    }

    /// Crash recovery with a DEAD pending token: the stale staging file is
    /// deleted, the previous active credential is untouched, and the caller
    /// is told to proceed with a fresh flow.
    #[tokio::test]
    async fn recovery_deletes_dead_pending_and_lets_fresh_login_proceed() {
        let stub = spawn_stub_cloud_with(401, true);
        let out = RecordingOutput::new();
        let (dir, files) = temp_files();
        let previous = "url: https://old.example.com\ntoken: fed_old-working-token\n";
        std::fs::write(dir.path().join("credentials"), previous).unwrap();
        files
            .save_pending_credentials(&cloud::Credentials {
                url: stub.base.clone(),
                token: "fed_dead-pending-token".to_string(),
            })
            .unwrap();

        let recovered = recover_pending_login(&files, &out).await.unwrap();
        assert!(
            !recovered,
            "a dead pending token must fall through to a fresh flow"
        );
        assert!(
            !pending_exists(&dir),
            "the stale pending file must be deleted"
        );
        assert_eq!(
            std::fs::read_to_string(dir.path().join("credentials")).unwrap(),
            previous,
            "the previous credential must be untouched"
        );
        assert!(
            !out.combined().contains("fed_dead-pending-token"),
            "recovery output leaked the dead token"
        );
    }

    /// No pending file: recovery is a silent no-op (and makes no network
    /// calls — there is no stub to answer any).
    #[tokio::test]
    async fn recovery_without_pending_is_a_noop() {
        let out = RecordingOutput::new();
        let (_dir, files) = temp_files();
        assert!(!recover_pending_login(&files, &out).await.unwrap());
        assert_eq!(out.combined(), "");
    }

    /// The masked email keeps the first character and the domain only.
    #[test]
    fn mask_email_masks_local_part() {
        assert_eq!(mask_email("adrian@example.com"), "a…@example.com");
        assert_eq!(mask_email("x@y.z"), "x…@y.z");
        // Unparseable addresses are not echoed back.
        assert_eq!(mask_email("not-an-email"), "you");
        assert_eq!(mask_email("@nodomain"), "you");
    }

    // ── Logout flow ───────────────────────────────────────────────────

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
