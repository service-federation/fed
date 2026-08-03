//! `fed login` / `fed logout` / `fed whoami` — Service Federation Cloud auth.

use crate::output::UserOutput;
use anyhow::{Result, anyhow, bail};
use fed::cloud;

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

/// Whether attempting to open a browser on THIS machine is worth it, judged
/// from the environment (`have` reports whether a variable is set and
/// non-empty). Sign-in itself never depends on this — the URL is always
/// printed and the CLI collects its code by polling the server — so a miss
/// costs only noise: over SSH the browser would open on the wrong machine
/// (or xdg-open would spam "command not found" for every terminal browser it
/// tries), and a display-less Linux box has nothing for xdg-open to launch.
fn browser_login_viable(have: impl Fn(&str) -> bool) -> bool {
    if have("SSH_CONNECTION") || have("SSH_TTY") || have("SSH_CLIENT") {
        return false;
    }
    // macOS and Windows always have their GUI; elsewhere a real browser
    // needs a display server.
    cfg!(any(target_os = "macos", target_os = "windows"))
        || have("DISPLAY")
        || have("WAYLAND_DISPLAY")
}

fn env_is_set(key: &str) -> bool {
    std::env::var_os(key).is_some_and(|v| !v.is_empty())
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

/// Poll the server until approval yields the exchange code.
///
/// One immediate poll, then a ~1.5s cadence. The client-side deadline sits
/// just past the server's 5-minute request expiry as a second line of
/// defense: the server answers `Gone` for an expired request, so even a CLI
/// with a broken clock stops within one poll of expiry — no abandoned fleet
/// of logins keeps hammering the endpoint. A 429 backs off to 5s; transient
/// transport errors are retried until the deadline (this machine's network
/// blipping while the user approves elsewhere must not kill the login).
async fn poll_for_code(base_url: &str, auth: &cloud::AuthRequest) -> Result<String> {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(330);
    loop {
        let wait = match cloud::poll_auth_request(base_url, auth).await {
            Ok(cloud::PollOutcome::Code(code)) => return Ok(code),
            Ok(cloud::PollOutcome::Gone) => {
                bail!("the sign-in request expired or was already used — run `fed login` again")
            }
            Ok(cloud::PollOutcome::Pending) => std::time::Duration::from_millis(1500),
            Ok(cloud::PollOutcome::RateLimited) => std::time::Duration::from_secs(5),
            Err(e) => {
                tracing::debug!("login poll attempt failed (will retry): {}", e);
                std::time::Duration::from_millis(1500)
            }
        };
        if std::time::Instant::now() + wait > deadline {
            bail!("timed out waiting for approval (5 minutes) — run `fed login` again");
        }
        tokio::time::sleep(wait).await;
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
    /// Skip the browser-open attempt (explicit `--no-browser`, an SSH
    /// session, or a display-less box). The flow is otherwise identical: the
    /// URL is printed and approval arrives via polling.
    no_browser: bool,
    label: String,
}

/// Core of `fed login`, separated from `run_login` (which supplies the real
/// browser opener and credential files) so the whole flow is unit-testable
/// against stub HTTP servers — the `logout_flow` pattern.
///
/// Output invariants (asserted by tests): the bearer token, the exchange
/// code, and the poll secret are NEVER printed — on any path. The authorize
/// URL is safe to always print (and always is): it carries only the opaque
/// request id, and polling additionally requires the poll secret, so the URL
/// alone can neither approve nor collect anything.
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
    files: &cloud::CredentialFiles,
    out: &dyn UserOutput,
) -> Result<()> {
    let auth = checked_auth_request(base_url, &opts.label).await?;
    let authorize = authorize_url(base_url, &auth.request);
    // The URL is ALWAYS printed, even when a browser opens: an opener can
    // "succeed" without landing anywhere useful (xdg-open falling through
    // to terminal browsers), and the printed URL works from any machine —
    // approval comes back via the server, not this machine's loopback.
    if !opts.no_browser && opener(&authorize) {
        out.status("Opening your browser to sign in… or use this URL on any machine:");
    } else {
        out.status("Open this URL on any machine, sign in, and approve the request:");
    }
    out.status(&format!("  {}", authorize));
    out.status("Waiting for approval… (times out after 5 minutes)");
    let code = poll_for_code(base_url, &auth).await?;

    // The code came over HTTPS from the server; anything not shaped like our
    // exchange code is contract drift. Checked without echoing it.
    if !valid_prefixed_id(&code, "fedac_") {
        bail!("cloud: malformed sign-in code from server — run `fed login` again");
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

/// Create an authorization request and validate the shapes of the returned
/// id (`fedar_` + 43 base64url chars, the only thing that may go near a URL)
/// and poll secret (`fedps_` + 43, which never does). Contract drift fails
/// here, early — without echoing either received value.
async fn checked_auth_request(base_url: &str, label: &str) -> Result<cloud::AuthRequest> {
    let auth = cloud::create_auth_request(base_url, label).await?;
    if !valid_prefixed_id(&auth.request, "fedar_") {
        bail!("cloud: malformed authorize request id from server — run `fed login` again");
    }
    if !valid_prefixed_id(&auth.poll_secret, "fedps_") {
        bail!("cloud: malformed poll secret from server — run `fed login` again");
    }
    Ok(auth)
}

pub async fn run_login(
    no_browser: bool,
    // Accepted for compatibility; the sign-in URL is now always printed.
    _print_url: bool,
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
    // Sign-in works the same either way (the URL is printed and approval
    // arrives via polling); skipping the opener over SSH or without a
    // display just avoids launching a browser nobody is looking at.
    let opts = LoginOptions {
        no_browser: no_browser || !browser_login_viable(env_is_set),
        label: label
            .map(|l| l.trim().to_string())
            .filter(|l| !l.is_empty())
            .unwrap_or_else(hostname),
    };
    login_flow(&base_url, &opts, &open_browser, &files, out).await
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
    use std::io::Write;
    use std::net::TcpListener;
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
    const STUB_POLL_SECRET: &str = "fedps_p0llp0llp0llp0llp0llp0llp0llp0llp0llp0llQw_";
    const STUB_TOKEN: &str = "fed_stub-bearer-token-value";

    #[test]
    fn stub_ids_are_production_valid() {
        assert!(valid_prefixed_id(STUB_CODE, "fedac_"));
        assert!(valid_prefixed_id(STUB_REQUEST, "fedar_"));
        assert!(valid_prefixed_id(STUB_POLL_SECRET, "fedps_"));
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
    /// so failure paths (activate down, whoami down, expired request) can be
    /// exercised. The poll endpoint verifies the CLI presents BOTH the
    /// request id and the poll secret, and the token is only issued for the
    /// right code — a flow that completes proves the whole chain ran.
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

    fn spawn_stub_cloud_cfg(activate_status: u16, me_ok: bool, revoke_ok: bool) -> StubCloud {
        spawn_stub_cloud_full(activate_status, me_ok, revoke_ok, 0, false)
    }

    /// `activate_status`: HTTP status for `POST /api/v1/cli/activate`
    /// (200 → activated, 401 → dead token, anything else → 500-style outage).
    /// `revoke_ok`: whether `DELETE /api/v1/cli/session` confirms the revoke
    /// (false models "both responses lost" — the ambiguous worst case).
    /// `pending_polls`: how many polls answer "pending" before the code is
    /// delivered (the user approving while the CLI waits).
    /// `poll_gone`: every poll answers 410 — an expired or already-used
    /// request.
    fn spawn_stub_cloud_full(
        activate_status: u16,
        me_ok: bool,
        revoke_ok: bool,
        pending_polls: u32,
        poll_gone: bool,
    ) -> StubCloud {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let calls = Arc::new(Mutex::new(Vec::new()));
        let recorded = calls.clone();
        let pending_left = Arc::new(Mutex::new(pending_polls));
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
                        assert_eq!(
                            v["poll"].as_bool(),
                            Some(true),
                            "the CLI must register poll-mode requests"
                        );
                        respond_json(
                            &stream,
                            "201 Created",
                            &format!(
                                "{{\"request\":\"{}\",\"poll_secret\":\"{}\",\"expires_in\":300}}",
                                STUB_REQUEST, STUB_POLL_SECRET
                            ),
                        );
                    }
                    ("POST", "/api/v1/cli/poll") => {
                        let v: serde_json::Value = serde_json::from_str(&body).unwrap();
                        if v["request"].as_str() != Some(STUB_REQUEST)
                            || v["secret"].as_str() != Some(STUB_POLL_SECRET)
                        {
                            // Wrong or missing credentials never yield a code.
                            respond_json(&stream, "410 Gone", "{\"error\":\"request_gone\"}");
                        } else if poll_gone {
                            respond_json(&stream, "410 Gone", "{\"error\":\"request_gone\"}");
                        } else {
                            let mut left = pending_left.lock().unwrap();
                            if *left > 0 {
                                *left -= 1;
                                respond_json(&stream, "200 OK", "{\"status\":\"pending\"}");
                            } else {
                                respond_json(
                                    &stream,
                                    "200 OK",
                                    &format!("{{\"code\":\"{}\"}}", STUB_CODE),
                                );
                            }
                        }
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

    fn login_opts(no_browser: bool) -> LoginOptions {
        LoginOptions {
            no_browser,
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
    /// poll secret — appears in the recorded output.
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
            !text.contains(STUB_POLL_SECRET) && !text.contains("fedps_"),
            "output leaked the poll secret: {}",
            text
        );
    }

    /// Full flow: create-request → poll (approved immediately) → code
    /// exchange → activate → whoami. The URL is printed even though the
    /// opener succeeded (an opener can "succeed" into nothing useful), the
    /// opener gets the id-only URL, and no secret material appears anywhere.
    /// The success line masks the email and gives an org count, not slugs.
    #[tokio::test]
    async fn login_completes_via_polling_and_prints_the_url() {
        let stub = spawn_stub_cloud();
        let out = RecordingOutput::new();
        let opened = RefCell::new(None::<String>);
        let (dir, files) = temp_files();
        login_flow(
            &stub.base,
            &login_opts(false),
            &|url: &str| {
                opened.replace(Some(url.to_string()));
                true
            },
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
        // …and neither the token nor the poll secret got near the terminal.
        let text = out.combined();
        assert_no_secrets(&text);

        // The full server-side sequence, in order: register → poll → exchange
        // → ACTIVATE (authenticated, before any success output can be built)
        // → whoami. No revoke.
        assert_eq!(
            stub.calls(),
            vec![
                "POST /api/v1/cli/authorize-request",
                "POST /api/v1/cli/poll",
                "POST /api/v1/cli/token",
                "POST /api/v1/cli/activate (auth)",
                "GET /api/v1/me (auth)",
            ],
            "unexpected call sequence"
        );

        // The opener got the id-only authorize URL; the printed URL is the
        // same one, and neither carries the poll secret.
        let authorize = opened.borrow().clone().expect("opener must be called");
        assert!(authorize.contains(&format!("/cli/authorize?request={}", STUB_REQUEST)));
        assert!(
            !authorize.contains("secret") && !authorize.contains("fedps_"),
            "authorize URL must carry only the request id: {}",
            authorize
        );
        assert!(
            text.contains(&format!("/cli/authorize?request={}", STUB_REQUEST)),
            "the URL must be printed even when the browser opens: {}",
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

    /// A failing opener changes nothing but the wording: the URL is printed
    /// and polling completes the login.
    #[tokio::test]
    async fn login_completes_when_the_browser_cannot_open() {
        let stub = spawn_stub_cloud();
        let out = RecordingOutput::new();
        let (dir, files) = temp_files();
        login_flow(
            &stub.base,
            &login_opts(false),
            &|_: &str| false,
            &files,
            &out,
        )
        .await
        .unwrap();
        assert_eq!(active_token(&dir).as_deref(), Some(STUB_TOKEN));
        let text = out.combined();
        assert!(
            text.contains(&format!("/cli/authorize?request={}", STUB_REQUEST)),
            "URL must be printed when the browser fails to open: {}",
            text
        );
        assert_no_secrets(&text);
    }

    /// Pending answers are retried until approval delivers the code — the
    /// user taking a moment in the browser while the CLI waits.
    #[tokio::test]
    async fn pending_polls_are_retried_until_the_code_arrives() {
        let stub = spawn_stub_cloud_full(200, true, true, 1, false);
        let out = RecordingOutput::new();
        let (dir, files) = temp_files();
        login_flow(&stub.base, &login_opts(true), &|_: &str| true, &files, &out)
            .await
            .unwrap();
        assert_eq!(active_token(&dir).as_deref(), Some(STUB_TOKEN));
        let polls = stub
            .calls()
            .iter()
            .filter(|c| c.as_str() == "POST /api/v1/cli/poll")
            .count();
        assert_eq!(
            polls,
            2,
            "one pending answer, then the code: {:?}",
            stub.calls()
        );
        assert_no_secrets(&out.combined());
    }

    /// A `gone` poll answer (expired or already-used request) fails the
    /// login with an actionable message that leaks nothing.
    #[tokio::test]
    async fn gone_poll_answer_fails_with_a_friendly_error() {
        let stub = spawn_stub_cloud_full(200, true, true, 0, true);
        let out = RecordingOutput::new();
        let (dir, files) = temp_files();
        let err = login_flow(&stub.base, &login_opts(true), &|_: &str| true, &files, &out)
            .await
            .expect_err("a gone request must fail the login");
        let msg = format!("{:#}", err);
        assert!(
            msg.contains("expired or was already used") && msg.contains("fed login"),
            "unexpected gone message: {}",
            msg
        );
        assert_no_secrets(&msg);
        assert_no_secrets(&out.combined());
        assert!(active_token(&dir).is_none(), "nothing may be installed");
        assert!(!pending_exists(&dir), "nothing may be staged");
    }

    /// SSH sessions are never browser-viable (the URL opens on a different
    /// machine); otherwise a display marks a Linux box viable. macOS/Windows
    /// without SSH are always viable, which the DISPLAY case subsumes here.
    #[test]
    fn browser_viability_from_environment() {
        let with = |vars: &'static [&'static str]| move |k: &str| vars.contains(&k);
        assert!(!browser_login_viable(with(&["SSH_CONNECTION", "DISPLAY"])));
        assert!(!browser_login_viable(with(&["SSH_TTY"])));
        assert!(!browser_login_viable(with(&["SSH_CLIENT"])));
        assert!(browser_login_viable(with(&["DISPLAY"])));
        assert!(browser_login_viable(with(&["WAYLAND_DISPLAY"])));
        #[cfg(not(any(target_os = "macos", target_os = "windows")))]
        assert!(
            !browser_login_viable(with(&[])),
            "a display-less Linux box must fall back to code sign-in"
        );
        #[cfg(any(target_os = "macos", target_os = "windows"))]
        assert!(browser_login_viable(with(&[])));
    }

    /// --no-browser: the opener is never invoked, the URL is printed, and
    /// polling completes the login exactly as in browser mode — replacing a
    /// pre-existing credential on promotion.
    #[tokio::test]
    async fn no_browser_flow_skips_the_opener_and_completes_via_polling() {
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
            &login_opts(true),
            &|_: &str| panic!("no browser may be opened in --no-browser mode"),
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
            &login_opts(true),
            &|_: &str| panic!("no browser in --no-browser mode"),
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
            &login_opts(true),
            &|_: &str| panic!("no browser in --no-browser mode"),
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
            &login_opts(true),
            &|_: &str| panic!("no browser in --no-browser mode"),
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
            &login_opts(true),
            &|_: &str| panic!("no browser in --no-browser mode"),
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
            &login_opts(true),
            &|_: &str| panic!("no browser in --no-browser mode"),
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
            &login_opts(true),
            &|_: &str| panic!("no browser in --no-browser mode"),
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
