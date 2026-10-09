//! Service Federation Cloud: `fed login` credentials, the per-checkout
//! project link, and the vault API client.
//!
//! fed works fully offline and logged out — everything here is additive.

use crate::error::{Error, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};

pub const DEFAULT_URL: &str = "https://app.service-federation.com";

// ── Credentials (~/.fed/credentials) ─────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Credentials {
    pub url: String,
    pub token: String,
}

fn fed_home() -> Option<PathBuf> {
    dirs::home_dir().map(|h| h.join(".fed"))
}

pub fn credentials_path() -> Option<PathBuf> {
    fed_home().map(|h| h.join("credentials"))
}

/// Load credentials. `FED_TOKEN` (+ optional `FED_CLOUD_URL`) override the
/// credentials file — that's the CI path.
pub fn load_credentials() -> Option<Credentials> {
    if let Ok(token) = std::env::var("FED_TOKEN")
        && !token.is_empty()
    {
        return Some(Credentials {
            url: std::env::var("FED_CLOUD_URL").unwrap_or_else(|_| DEFAULT_URL.to_string()),
            token,
        });
    }
    load_credentials_from(&credentials_path()?)
}

/// Load the on-disk credential (`~/.fed/credentials`) only, ignoring the
/// `FED_TOKEN` CI override. `fed logout` acts on the credential it wrote at
/// login — the environment token is the caller's to manage, not ours to revoke
/// or claim to remove.
pub fn load_stored_credentials() -> Option<Credentials> {
    load_credentials_from(&credentials_path()?)
}

fn load_credentials_from(path: &Path) -> Option<Credentials> {
    use std::io::Read;
    // Open the file ONCE and operate on the handle: reading the bytes and
    // tightening the permissions must target the same file. Reading by path and
    // then chmodding by path is a TOCTOU — a swap between the two operations
    // would chmod a different file than the one we read. fchmod through the held
    // handle can't be redirected by a path swap.
    let mut file = std::fs::File::open(path).ok()?;
    let mut raw = String::new();
    file.read_to_string(&mut raw).ok()?;
    let creds = serde_yaml::from_str(&raw).ok()?;
    // Tighten a pre-existing over-permissive credentials file (e.g. one written
    // by an older fed that chmodded after the fact, or copied in by hand).
    // Best-effort: a chmod failure must not block login.
    let _ = crate::fsutil::tighten_to_owner_only(&file, path);
    Some(creds)
}

pub fn save_credentials(creds: &Credentials) -> Result<()> {
    let path = credentials_path()
        .ok_or_else(|| Error::Validation("cannot determine home directory".into()))?;
    save_credentials_to(&path, creds)
}

fn save_credentials_to(path: &Path, creds: &Credentials) -> Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .map_err(|e| Error::Filesystem(format!("creating {}: {}", parent.display(), e)))?;
    }
    let yaml = serde_yaml::to_string(creds)
        .map_err(|e| Error::Validation(format!("serializing credentials: {}", e)))?;
    // Credentials hold a bearer token, so write via the shared atomic 0600
    // helper: never a world-readable window on first creation (unlike the old
    // write-then-chmod), and crash-atomic. sync=true — the write happens only on
    // `fed login`, so the one fsync is free, and a token silently lost to a
    // crash would force an out-of-band re-login.
    crate::fsutil::write_owner_only_atomic(path, yaml.as_bytes(), true)
}

pub fn delete_credentials() -> Result<bool> {
    let Some(path) = credentials_path() else {
        return Ok(false);
    };
    match std::fs::remove_file(&path) {
        Ok(()) => Ok(true),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(e) => Err(Error::Filesystem(format!(
            "removing {}: {}",
            path.display(),
            e
        ))),
    }
}

// ── Staged credential promotion (login) ───────────────────────────────

/// The on-disk credential pair: the ACTIVE file (`~/.fed/credentials`) and
/// its staging sibling (`credentials.pending`).
///
/// `fed login` writes the freshly-exchanged token — still provisional — to
/// the pending file first, and renames it over the active file only after
/// the server confirms activation (durability). The previous working
/// credential is therefore never destroyed by a login that ultimately
/// fails, and a crash between activation and promotion is recoverable from
/// the pending file on the next `fed login`.
pub struct CredentialFiles {
    active: PathBuf,
    pending: PathBuf,
    lock: PathBuf,
}

/// Cross-process guard for the login sequence, backed by an advisory
/// `flock`/`LockFileEx` on `login.lock` beside the credentials (via `fs2`,
/// the same mechanism as the supervisor lock). Released on drop — and by the
/// OS if the process dies, so a crashed login never wedges future ones.
#[derive(Debug)]
pub struct LoginLock {
    file: std::fs::File,
}

impl Drop for LoginLock {
    fn drop(&mut self) {
        let _ = fs2::FileExt::unlock(&self.file);
    }
}

impl CredentialFiles {
    /// The real `~/.fed` locations; `None` when no home dir is available.
    pub fn default_paths() -> Option<Self> {
        Some(Self::for_active(credentials_path()?))
    }

    /// Rooted at `dir` (`dir/credentials` + `dir/credentials.pending` +
    /// `dir/login.lock`) — lets tests exercise the real file behavior
    /// against a temp dir.
    pub fn in_dir(dir: &Path) -> Self {
        Self::for_active(dir.join("credentials"))
    }

    fn for_active(active: PathBuf) -> Self {
        let pending = active.with_extension("pending");
        let lock = active
            .parent()
            .map(|p| p.join("login.lock"))
            .unwrap_or_else(|| active.with_extension("lock"));
        Self {
            active,
            pending,
            lock,
        }
    }

    /// Try to take the cross-process login lock. `Ok(None)` means another
    /// process holds it right now — concurrent logins would race on the
    /// single pending file (one login promoting the other's token and
    /// stranding its own), so the caller should fail fast, not queue.
    /// Never blocks.
    pub fn try_lock_login(&self) -> Result<Option<LoginLock>> {
        if let Some(parent) = self.lock.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|e| Error::Filesystem(format!("creating {}: {}", parent.display(), e)))?;
        }
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&self.lock)
            .map_err(|e| Error::Filesystem(format!("opening {}: {}", self.lock.display(), e)))?;
        match fs2::FileExt::try_lock_exclusive(&file) {
            Ok(()) => Ok(Some(LoginLock { file })),
            // Any lock failure means a live holder (the supervisor-lock
            // pattern): report contended rather than hard-failing.
            Err(_) => Ok(None),
        }
    }

    /// Stage a (provisional) credential. Same atomic 0600 writer as the
    /// active file; the active file is untouched.
    pub fn save_pending_credentials(&self, creds: &Credentials) -> Result<()> {
        save_credentials_to(&self.pending, creds)
    }

    pub fn load_pending_credentials(&self) -> Option<Credentials> {
        load_credentials_from(&self.pending)
    }

    /// Promote the pending credential over the active one — a single atomic
    /// rename, so there is never a moment without a valid credentials file
    /// and the 0600 mode carries over.
    pub fn promote_pending_credentials(&self) -> Result<()> {
        std::fs::rename(&self.pending, &self.active).map_err(|e| {
            Error::Filesystem(format!(
                "promoting {} to {}: {}",
                self.pending.display(),
                self.active.display(),
                e
            ))
        })
    }

    /// Remove the staging file (e.g. after a failed activation). Returns
    /// whether a file was actually removed.
    pub fn delete_pending_credentials(&self) -> Result<bool> {
        match std::fs::remove_file(&self.pending) {
            Ok(()) => Ok(true),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(false),
            Err(e) => Err(Error::Filesystem(format!(
                "removing {}: {}",
                self.pending.display(),
                e
            ))),
        }
    }
}

// ── Project link (.fed/cloud.yaml, committed) ────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CloudLink {
    pub org: String,
    pub project: String,
    /// Persistence policy for values fetched from this project's team vault.
    #[serde(default)]
    pub secret_cache: crate::orchestrator::SecretCacheMode,
}

pub fn link_path(work_dir: &Path) -> PathBuf {
    work_dir.join(".fed").join("cloud.yaml")
}

pub fn load_link(work_dir: &Path) -> Option<CloudLink> {
    let raw = std::fs::read_to_string(link_path(work_dir)).ok()?;
    serde_yaml::from_str(&raw).ok()
}

pub fn save_link(work_dir: &Path, link: &CloudLink) -> Result<PathBuf> {
    let path = link_path(work_dir);
    // Creates .fed/ with its self-ignoring .gitignore (which unignores cloud.yaml).
    crate::fed_dir::ensure_fed_dir(work_dir)?;
    let yaml = format!(
        "# Binds this checkout to a Service Federation Cloud project.\n# Commit this file — teammates inherit the link.\n{}",
        serde_yaml::to_string(link)
            .map_err(|e| Error::Validation(format!("serializing link: {}", e)))?
    );
    std::fs::write(&path, yaml)
        .map_err(|e| Error::Filesystem(format!("writing {}: {}", path.display(), e)))?;
    Ok(path)
}

#[cfg(test)]
mod cloud_link_tests {
    use super::*;

    #[test]
    fn link_without_policy_defaults_to_memory_cache() {
        let link: CloudLink = serde_yaml::from_str("org: acme\nproject: web\n").unwrap();
        assert_eq!(
            link.secret_cache,
            crate::orchestrator::SecretCacheMode::Memory
        );
    }

    #[test]
    fn file_cache_is_an_explicit_opt_in() {
        let link: CloudLink =
            serde_yaml::from_str("org: acme\nproject: web\nsecret_cache: file\n").unwrap();
        assert_eq!(
            link.secret_cache,
            crate::orchestrator::SecretCacheMode::File
        );
    }

    #[test]
    fn memory_cache_round_trips_in_cloud_config() {
        let link: CloudLink =
            serde_yaml::from_str("org: acme\nproject: web\nsecret_cache: memory\n").unwrap();
        assert_eq!(
            link.secret_cache,
            crate::orchestrator::SecretCacheMode::Memory
        );
        let yaml = serde_yaml::to_string(&link).unwrap();
        assert!(yaml.contains("secret_cache: memory"));
    }

    #[test]
    fn removed_keychain_policy_still_parses_and_resolves_to_memory() {
        // A cloud.yaml committed by fed 7.6.x must keep loading — rejecting the
        // value would take the org/project link down with it and break `fed
        // start` entirely for everyone on the team who hasn't re-linked yet.
        let link: CloudLink =
            serde_yaml::from_str("org: acme\nproject: web\nsecret_cache: keychain\n").unwrap();
        assert_eq!(link.org, "acme");
        assert_eq!(
            link.secret_cache.effective(),
            crate::orchestrator::SecretCacheMode::Memory
        );
    }
}

// ── Vault timing knobs ──────────────────────────────────────────────────
//
// A booting (scale-to-zero) backend is not a failing one: the timeout must be
// a function of whether we can proceed without the answer, not a constant.
// Four knobs realize that, each with a default chosen so it rarely matters:
//
// - `FED_VAULT_GRACE` (2s): how long the resolver waits before falling back to
//   a fresh cache. Warm vaults answer in ~0.17s, well inside this.
// - `FED_VAULT_TIMEOUT` (60s): the blocking budget when the cache can't cover
//   the run and we must wait for a cold start. Also the HTTP client timeout.
// - `FED_VAULT_MAX_AGE` (24h): freshness bound on cached values. Beyond it, a
//   run blocks to refresh rather than serve a stale value forever. Applies
//   only once a fetch has already been fired and grace has expired (deciding
//   whether to keep waiting), and as the fallback bound when the vault is
//   unreachable.
// - `FED_VAULT_TTL` (5m): freshness window in which a fully-cached run skips
//   the vault call entirely — no fetch is fired at all. Distinct from
//   `FED_VAULT_MAX_AGE`: this bound decides whether to fire the fetch in the
//   first place, checked before any network activity. 0 disables the skip
//   (every run with queried names always calls the vault, matching behavior
//   before this knob existed).

use std::time::Duration;

fn env_duration(var: &str, default: Duration) -> Duration {
    duration_or_default(std::env::var(var).ok().as_deref(), default)
}

/// The parse-and-fall-back half of [`env_duration`], split out so it can be
/// tested without mutating the process environment (`set_var` is unsafe as of
/// Rust 2024, and a shared env races other tests).
fn duration_or_default(raw: Option<&str>, default: Duration) -> Duration {
    raw.and_then(crate::config::parse_duration_string)
        .unwrap_or(default)
}

/// Grace window: how long the resolver waits for the vault before consulting a
/// fresh cache (`FED_VAULT_GRACE`, default 2s).
pub fn vault_grace() -> Duration {
    env_duration("FED_VAULT_GRACE", Duration::from_secs(2))
}

/// Blocking budget when the cache cannot cover the run (`FED_VAULT_TIMEOUT`,
/// default 60s). Doubles as the HTTP client timeout.
pub fn vault_timeout() -> Duration {
    env_duration("FED_VAULT_TIMEOUT", Duration::from_secs(60))
}

/// Freshness bound on cached secret values (`FED_VAULT_MAX_AGE`, default 24h).
pub fn vault_max_age() -> Duration {
    env_duration("FED_VAULT_MAX_AGE", Duration::from_secs(24 * 60 * 60))
}

/// Freshness window in which a fully-cached run skips the vault call
/// entirely — no fetch is fired at all (`FED_VAULT_TTL`, default 5m). Distinct
/// from `FED_VAULT_MAX_AGE` (24h): that bound only applies once a fetch has
/// already been fired and grace has expired; this bound decides whether to
/// fire the fetch at all. 0 disables the skip (every run with queried names
/// always calls the vault, today's behavior).
pub fn vault_ttl() -> Duration {
    env_duration("FED_VAULT_TTL", Duration::from_secs(5 * 60))
}

// ── API client ────────────────────────────────────────────────────────

/// Header carrying this CLI's version on every cloud request. The server
/// compares it against its minimum supported protocol version and answers
/// `426 Upgrade Required` to clients that are too old — see `api_error`
/// (private helper below, which maps that status to an upgrade error).
/// Clients ≤ 7.2.0 predate this header; the server must treat its absence as
/// "too old to say".
pub const VERSION_HEADER: &str = "x-fed-version";
/// The cloud API contract is independent of the CLI's release version.
pub const API_VERSION_HEADER: &str = "x-fed-api-version";
pub const API_VERSION: &str = "2";

/// Builder with everything both cloud clients share: the version header (so
/// the server can enforce a minimum CLI version) and a matching user agent.
/// Callers add their own timeout — the vault client and the logout revoke
/// client budget very differently.
fn client_builder() -> reqwest::ClientBuilder {
    let mut headers = reqwest::header::HeaderMap::new();
    headers.insert(
        VERSION_HEADER,
        reqwest::header::HeaderValue::from_static(env!("CARGO_PKG_VERSION")),
    );
    headers.insert(
        API_VERSION_HEADER,
        reqwest::header::HeaderValue::from_static(API_VERSION),
    );
    reqwest::Client::builder()
        .user_agent(concat!("fed/", env!("CARGO_PKG_VERSION")))
        .default_headers(headers)
        // Neither bearer tokens nor single-use login codes may be replayed to
        // a destination chosen by a redirect response.
        .redirect(reqwest::redirect::Policy::none())
}

/// Validate the configured origin before building any cloud request. Supplying
/// a loopback HTTP URL is an explicit local development choice; all remote
/// origins must use HTTPS.
pub fn cloud_base_url(raw: &str) -> Result<reqwest::Url> {
    let url = reqwest::Url::parse(raw)
        .map_err(|_| Error::Validation("cloud: invalid vault URL".into()))?;
    let loopback = matches!(url.host_str(), Some("localhost" | "127.0.0.1" | "[::1]"));
    let local_http = url.scheme() == "http" && loopback;
    if !(url.scheme() == "https" || local_http)
        || url.host_str().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
        || url.path() != "/"
    {
        return Err(Error::Validation(
            "cloud: vault URL must be an HTTPS origin or an explicit local loopback HTTP origin"
                .into(),
        ));
    }
    Ok(url)
}

fn api_url(base: &str, path: &str) -> Result<reqwest::Url> {
    let mut url = cloud_base_url(base)?;
    url.set_path(path);
    Ok(url)
}

fn client() -> &'static reqwest::Client {
    static CLIENT: std::sync::OnceLock<reqwest::Client> = std::sync::OnceLock::new();
    // The timeout is the blocking budget — long enough to ride out a cold
    // start (a booting backend answers, it just answers slowly). The resolver
    // enforces the *short* grace wait itself; this cap only bites the honest
    // block. Read from the env here (rather than a hardcoded constant) so
    // FED_VAULT_TIMEOUT tunes both in one place (D6).
    CLIENT.get_or_init(|| {
        client_builder()
            .timeout(vault_timeout())
            .build()
            .expect("building HTTP client")
    })
}

fn api_error(status: reqwest::StatusCode, context: &str) -> Error {
    let hint = match status.as_u16() {
        401 => " — your token is invalid or revoked; run `fed login`",
        403 => " — you no longer have access; ask an org admin",
        404 => " — org or project not found; check `fed link`",
        // The server saw our x-fed-version header (or its absence) and refused:
        // this build no longer speaks the protocol it requires.
        426 => {
            " — this version of fed is too old for the server; upgrade fed (`brew upgrade fed`) and retry"
        }
        429 => " — rate limited; try again in a minute",
        _ => "",
    };
    Error::Validation(format!("cloud: {} failed ({}){}", context, status, hint))
}

#[derive(Deserialize)]
pub struct Me {
    pub user: MeUser,
    pub orgs: Vec<MeOrg>,
}

#[derive(Deserialize)]
pub struct MeUser {
    pub name: Option<String>,
    pub email: Option<String>,
}

#[derive(Deserialize)]
pub struct MeOrg {
    pub slug: String,
    pub name: String,
    pub role: String,
}

pub async fn whoami(creds: &Credentials) -> Result<Me> {
    let res = client()
        .get(api_url(&creds.url, "/api/v1/me")?)
        .bearer_auth(&creds.token)
        .send()
        .await
        .map_err(|e| Error::Validation(format!("cloud: cannot reach {}: {}", creds.url, e)))?;
    if !res.status().is_success() {
        return Err(api_error(res.status(), "whoami"));
    }
    res.json()
        .await
        .map_err(|e| Error::Validation(format!("cloud: bad whoami response: {}", e)))
}

// ── Login: authorization request + poll + code exchange ───────────────
//
// `fed login` never receives the bearer token through the browser. The CLI
// first registers a poll-mode AUTHORIZATION REQUEST server-side; the browser
// URL carries only that request's opaque id — an unguessable handle, not a
// credential: approval still requires an authenticated browser session plus
// an explicit click. The CLI then POLLS the server (with the poll secret,
// which never travels in a URL) until approval yields the short-lived,
// single-use EXCHANGE CODE, redeemed for the bearer token over HTTPS via
// `POST /api/v1/cli/token`. Nothing loopback, so the approving browser can
// be on any machine — this one or one that reaches it over SSH. The token
// never appears in a URL, browser page, redirect, log line, or terminal
// output.

/// A registered authorization request: the opaque id (the only thing that
/// may ever appear in the authorize URL) and the poll secret the CLI
/// presents to collect the exchange code. Holding the URL alone is never
/// enough to poll — the secret exists only in the create response and the
/// poll bodies, all over HTTPS.
pub struct AuthRequest {
    pub request: String,
    pub poll_secret: String,
    pub pairing_code: String,
}

/// Redacting Debug: the poll secret must never reach a log line or panic
/// message, and the request id is kept out for good measure.
impl std::fmt::Debug for AuthRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AuthRequest")
            .field("request", &"<redacted>")
            .field("poll_secret", &"<redacted>")
            .field("pairing_code", &self.pairing_code)
            .finish()
    }
}

#[derive(Serialize)]
struct AuthRequestBody<'a> {
    poll: bool,
    label: &'a str,
}

#[derive(Deserialize)]
struct AuthRequestResponse {
    request: String,
    poll_secret: String,
    pairing_code: String,
}

/// Create a server-side poll-mode authorization request. The device label
/// travels only in this POST body over HTTPS — never in a URL.
pub async fn create_auth_request(base_url: &str, label: &str) -> Result<AuthRequest> {
    let res = client()
        .post(api_url(base_url, "/api/v1/cli/authorize-request")?)
        .json(&AuthRequestBody { poll: true, label })
        .send()
        .await
        .map_err(|e| Error::Validation(format!("cloud: cannot reach {}: {}", base_url, e)))?;
    if !res.status().is_success() {
        return Err(api_error(res.status(), "starting login"));
    }
    if res
        .headers()
        .get(API_VERSION_HEADER)
        .and_then(|value| value.to_str().ok())
        != Some(API_VERSION)
    {
        return Err(Error::Validation(
            "cloud: server did not confirm API protocol version 2; update the server or use a compatible fed build"
                .into(),
        ));
    }
    let body: AuthRequestResponse = res
        .json()
        .await
        .map_err(|e| Error::Validation(format!("cloud: bad authorize-request response: {}", e)))?;
    Ok(AuthRequest {
        request: body.request,
        poll_secret: body.poll_secret,
        pairing_code: body.pairing_code,
    })
}

/// One poll answer. `Gone` is terminal (expired, already delivered, or never
/// ours); `RateLimited` asks the caller to back off; transport errors are
/// `Err` so the caller can keep trying until its own deadline.
pub enum PollOutcome {
    Pending,
    Code(String),
    RateLimited,
    Gone,
}

/// Redacting Debug: `Code` carries the single-use exchange code, which must
/// never reach a log line or panic message.
impl std::fmt::Debug for PollOutcome {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Pending => f.write_str("Pending"),
            Self::Code(_) => f.write_str("Code(<redacted>)"),
            Self::RateLimited => f.write_str("RateLimited"),
            Self::Gone => f.write_str("Gone"),
        }
    }
}

#[derive(Serialize)]
struct PollBody<'a> {
    request: &'a str,
    secret: &'a str,
}

#[derive(Deserialize)]
struct PollResponse {
    status: Option<String>,
    code: Option<String>,
}

/// Ask once whether the authorization request has been approved, collecting
/// the exchange code when it has. The code never appears in any error.
pub async fn poll_auth_request(base_url: &str, auth: &AuthRequest) -> Result<PollOutcome> {
    let res = client()
        .post(api_url(base_url, "/api/v1/cli/poll")?)
        .json(&PollBody {
            request: &auth.request,
            secret: &auth.poll_secret,
        })
        .send()
        .await
        .map_err(|e| Error::Validation(format!("cloud: cannot reach {}: {}", base_url, e)))?;
    match res.status().as_u16() {
        410 => Ok(PollOutcome::Gone),
        429 => Ok(PollOutcome::RateLimited),
        s if !(200..300).contains(&s) => Err(api_error(res.status(), "waiting for approval")),
        _ => {
            let body: PollResponse = res
                .json()
                .await
                .map_err(|e| Error::Validation(format!("cloud: bad poll response: {}", e)))?;
            match (body.code, body.status.as_deref()) {
                (Some(code), _) => Ok(PollOutcome::Code(code)),
                (None, Some("pending")) => Ok(PollOutcome::Pending),
                _ => Err(Error::Validation(
                    "cloud: bad poll response — run `fed login` again".to_string(),
                )),
            }
        }
    }
}

#[derive(Serialize)]
struct ExchangeCodeBody<'a> {
    code: &'a str,
}

#[derive(Deserialize)]
struct ExchangeCodeResponse {
    token: String,
}

/// Redeem a single-use exchange code for the bearer token via
/// `POST /api/v1/cli/token`.
///
/// Security invariant: the code never appears in any error message. The
/// server's 400 covers invalid, expired, and already-used codes alike, and
/// maps to one fixed, friendly message here.
pub async fn exchange_code(base_url: &str, code: &str) -> Result<String> {
    let res = client()
        .post(api_url(base_url, "/api/v1/cli/token")?)
        .json(&ExchangeCodeBody { code })
        .send()
        .await
        .map_err(|e| Error::Validation(format!("cloud: cannot reach {}: {}", base_url, e)))?;
    if res.status().as_u16() == 400 {
        return Err(Error::Validation(
            "the sign-in link expired or was already used — run `fed login` again".to_string(),
        ));
    }
    if !res.status().is_success() {
        return Err(api_error(res.status(), "completing login"));
    }
    let body: ExchangeCodeResponse = res
        .json()
        .await
        .map_err(|e| Error::Validation(format!("cloud: bad token response: {}", e)))?;
    Ok(body.token)
}

/// Outcome of asking the server to activate a freshly-exchanged token.
pub enum Activation {
    /// A 200 with a parsed `{"activated": bool}` body — the token is durable
    /// (`true`: activated just now; `false`: already activated — idempotent
    /// retry). Only a parsed 200 proves the activation endpoint ran.
    Activated,
    /// 401 — the token is dead; no retry can resurrect it.
    Dead,
    /// Activation could not be confirmed (network, 5xx, 429, or an
    /// unparseable 200 body) after the bounded retries. Carries a short
    /// reason — never the token.
    Failed(String),
}

#[derive(Deserialize)]
struct ActivateResponse {
    activated: bool,
}

/// Activate a freshly-exchanged token via `POST /api/v1/cli/activate`.
///
/// The server mints exchange tokens PROVISIONAL (10-minute expiry); this
/// authenticated call extends the presented token to its full lifetime
/// exactly once. Because this call is what makes a login durable, transient
/// failures get a small bounded retry, and success FAILS CLOSED: a 200 whose
/// body is not `{"activated": bool}` (endpoint misrouting, interposed proxy)
/// is a failure, not a success. A stranded provisional token simply
/// self-expires — no orphaned one-year credential is ever left behind.
pub async fn activate_token(creds: &Credentials) -> Activation {
    let url = match api_url(&creds.url, "/api/v1/cli/activate") {
        Ok(url) => url,
        Err(e) => return Activation::Failed(e.to_string()),
    };
    let mut last = String::new();
    for attempt in 0..3u32 {
        if attempt > 0 {
            tokio::time::sleep(Duration::from_millis(250 * u64::from(attempt))).await;
        }
        let res = client()
            .post(url.clone())
            .bearer_auth(&creds.token)
            .json(&serde_json::json!({}))
            .send()
            .await;
        match res {
            Ok(res) if res.status().is_success() => match res.json::<ActivateResponse>().await {
                Ok(body) => {
                    // true = first activation, false = already durable —
                    // either way the token is now long-lived.
                    let _ = body.activated;
                    return Activation::Activated;
                }
                Err(e) => last = format!("cloud: bad activate response: {}", e),
            },
            // Dead token: no retry will resurrect it.
            Ok(res) if res.status().as_u16() == 401 => return Activation::Dead,
            // Upgrade Required is just as terminal — retrying the same
            // protocol version cannot succeed.
            Ok(res) if res.status().as_u16() == 426 => {
                return Activation::Failed(api_error(res.status(), "activating login").to_string());
            }
            Ok(res) => last = api_error(res.status(), "activating login").to_string(),
            Err(e) => last = format!("cloud: cannot reach {}: {}", creds.url, e),
        }
    }
    Activation::Failed(last)
}

#[derive(Deserialize)]
struct ProjectsResponse {
    projects: Vec<ProjectEntry>,
}

#[derive(Deserialize)]
pub struct ProjectEntry {
    pub slug: String,
    pub name: String,
}

pub async fn list_projects(creds: &Credentials, org: &str) -> Result<Vec<ProjectEntry>> {
    let res = client()
        .get(api_url(
            &creds.url,
            &format!("/api/v1/orgs/{org}/projects"),
        )?)
        .bearer_auth(&creds.token)
        .send()
        .await
        .map_err(|e| Error::Validation(format!("cloud: cannot reach {}: {}", creds.url, e)))?;
    if !res.status().is_success() {
        return Err(api_error(res.status(), "listing projects"));
    }
    let body: ProjectsResponse = res
        .json()
        .await
        .map_err(|e| Error::Validation(format!("cloud: bad projects response: {}", e)))?;
    Ok(body.projects)
}

#[derive(Deserialize)]
struct SecretListResponse {
    secrets: Vec<SecretEntry>,
}

#[derive(Deserialize)]
pub struct SecretEntry {
    pub name: String,
    pub updated_at: String,
    pub updated_by: String,
}

pub async fn list_secrets(creds: &Credentials, link: &CloudLink) -> Result<Vec<SecretEntry>> {
    let res = client()
        .get(api_url(
            &creds.url,
            &format!(
                "/api/v1/orgs/{}/projects/{}/secrets",
                link.org, link.project
            ),
        )?)
        .bearer_auth(&creds.token)
        .send()
        .await
        .map_err(|e| Error::Validation(format!("cloud: cannot reach {}: {}", creds.url, e)))?;
    if !res.status().is_success() {
        return Err(api_error(res.status(), "listing secrets"));
    }
    let body: SecretListResponse = res
        .json()
        .await
        .map_err(|e| Error::Validation(format!("cloud: bad secrets response: {}", e)))?;
    Ok(body.secrets)
}

/// Secret names the vault accepts. Checked before a request so that a name
/// can never change the request path.
pub fn valid_secret_name(name: &str) -> bool {
    let mut chars = name.chars();
    matches!(chars.next(), Some(c) if c.is_ascii_alphabetic() || c == '_')
        && name.len() <= 128
        && chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
}

pub const INVALID_SECRET_NAME: &str =
    "invalid secret name: use up to 128 letters, digits and _, not starting with a digit";

fn secret_url(creds: &Credentials, link: &CloudLink, name: &str) -> Result<reqwest::Url> {
    if !valid_secret_name(name) {
        // The name is not echoed: `NAME=value` typed by mistake would put the
        // value in the error.
        return Err(Error::Validation(INVALID_SECRET_NAME.into()));
    }
    api_url(
        &creds.url,
        &format!(
            "/api/v1/orgs/{}/projects/{}/secrets/{}",
            link.org, link.project, name
        ),
    )
}

/// Which secret write a refused response belongs to.
#[derive(Clone, Copy)]
enum SecretWrite {
    Set,
    Remove,
}

/// The message for a refused secret write. Built from the status, the server's
/// `{"error": code}` and the name only: the value never reaches this function.
fn secret_write_error(
    status: reqwest::StatusCode,
    code: Option<&str>,
    write: SecretWrite,
    name: &str,
    link: &CloudLink,
) -> Error {
    let verb = match write {
        SecretWrite::Set => "set",
        SecretWrite::Remove => "remove",
    };
    let project = format!("{}/{}", link.org, link.project);
    let message = match (status.as_u16(), code) {
        (401, _) => {
            "cloud: your login is missing, expired or revoked — run `fed login`".to_string()
        }
        (403, Some("session_only")) => format!(
            "cloud: this vault does not accept secret changes from the CLI yet — {verb} {name} in the dashboard"
        ),
        (403, _) => format!("cloud: only org admins can {verb} secrets in {project}"),
        (404, _) => format!(
            "cloud: project {project} not found, or you are not a member — check `fed link`"
        ),
        (400, Some("value")) => {
            "cloud: the vault rejected the value — it must be 1 to 65536 characters".to_string()
        }
        (400, Some("name")) => format!("cloud: the vault rejected the secret name `{name}`"),
        (413, _) => "cloud: the value is too large for the vault".to_string(),
        (429, _) => "cloud: rate limited — try again in a minute".to_string(),
        _ => return api_error(status, &format!("trying to {verb} {name}")),
    };
    Error::Validation(message)
}

#[derive(Deserialize)]
struct ErrorBody {
    error: String,
}

/// A non-2xx answer to a secret write: the status and the server's error code.
struct Refused {
    status: reqwest::StatusCode,
    code: Option<String>,
}

impl Refused {
    fn into_error(self, write: SecretWrite, name: &str, link: &CloudLink) -> Error {
        secret_write_error(self.status, self.code.as_deref(), write, name, link)
    }
}

/// Send a secret write. The outer error is a transport failure; the inner one
/// is the server's refusal, left to the caller to word.
async fn send_secret_write(
    req: reqwest::RequestBuilder,
    creds: &Credentials,
) -> Result<std::result::Result<(), Refused>> {
    let res = req
        .bearer_auth(&creds.token)
        .send()
        .await
        .map_err(|e| Error::Validation(format!("cloud: cannot reach {}: {}", creds.url, e)))?;
    let status = res.status();
    if status.is_success() {
        return Ok(Ok(()));
    }
    let code = res.json::<ErrorBody>().await.ok().map(|b| b.error);
    Ok(Err(Refused { status, code }))
}

/// Create or replace one secret in the linked project:
/// `PUT /api/v1/orgs/{org}/projects/{project}/secrets/{name}` with
/// `{"value": ...}`. The body has no `env`, so the server uses its default
/// environment, the same one `fetch_values` and `list_secrets` read.
pub async fn put_secret(
    creds: &Credentials,
    link: &CloudLink,
    name: &str,
    value: &str,
) -> Result<()> {
    let url = secret_url(creds, link, name)?;
    let req = client()
        .put(url)
        .json(&serde_json::json!({ "value": value }));
    send_secret_write(req, creds)
        .await?
        .map_err(|r| r.into_error(SecretWrite::Set, name, link))
}

/// What `delete_secret` found on the server.
#[derive(Debug, PartialEq, Eq)]
pub enum Deletion {
    Removed,
    /// The vault answered 404 `secret`: the name was not set in the project.
    NotSet,
}

/// Delete one secret from the linked project:
/// `DELETE /api/v1/orgs/{org}/projects/{project}/secrets/{name}`.
pub async fn delete_secret(creds: &Credentials, link: &CloudLink, name: &str) -> Result<Deletion> {
    let url = secret_url(creds, link, name)?;
    match send_secret_write(client().delete(url), creds).await? {
        Ok(()) => Ok(Deletion::Removed),
        Err(r) if r.status.as_u16() == 404 && r.code.as_deref() == Some("secret") => {
            Ok(Deletion::NotSet)
        }
        Err(r) => Err(r.into_error(SecretWrite::Remove, name, link)),
    }
}

#[derive(Deserialize)]
pub struct SecretValues {
    pub values: HashMap<String, String>,
    #[serde(default)]
    pub missing: Vec<String>,
}

/// Why a vault fetch could not return values, classified so the caller can
/// pick the right wait. `reqwest` distinguishes the two cases and the cold
/// probe confirms which fires:
///
/// - `Unreachable`: `is_connect()` / DNS — nothing is listening, or we're
///   offline. Waiting is pointless; a recent cache may be used.
/// - `Failed`: a timeout or server/rate-limit error. A recent cache may be used.
/// - `Denied`: a refusal (401/403), another 4xx, a redirect, an unreadable
///   response or an invalid vault URL. The cache must not answer this online
///   request. The message opens with which of these it was.
#[derive(Debug, Clone)]
pub enum VaultFailure {
    Unreachable(String),
    Failed(String),
    Denied(String),
}

impl VaultFailure {
    /// Human-readable reason, for warnings and the missing-secret error.
    pub fn message(&self) -> &str {
        match self {
            VaultFailure::Unreachable(m) | VaultFailure::Failed(m) | VaultFailure::Denied(m) => m,
        }
    }

    /// Whether nothing is listening (connect/DNS). Such failures short-circuit
    /// the blocking budget — there is no cold start to wait out.
    pub fn is_unreachable(&self) -> bool {
        matches!(self, VaultFailure::Unreachable(_))
    }
}

/// Classify a `reqwest` send error into a [`VaultFailure`]. `is_connect()`
/// (and DNS, which surfaces during connect) means unreachable; everything else
/// (notably `is_timeout()`) means the backend was reached but did not answer
/// usefully in time.
fn classify_send_error(url: &str, e: &reqwest::Error) -> VaultFailure {
    if e.is_connect() {
        VaultFailure::Unreachable(format!("cannot reach {}: {}", url, e))
    } else {
        VaultFailure::Failed(format!("cloud: {}", e))
    }
}

type VaultResult = std::result::Result<HashMap<String, String>, VaultFailure>;

async fn fetch_values_inner(
    creds: &Credentials,
    link: &CloudLink,
    names: &[String],
) -> VaultResult {
    let mut url = api_url(
        &creds.url,
        &format!(
            "/api/v1/orgs/{}/projects/{}/secrets/values",
            link.org, link.project
        ),
    )
    .map_err(|e| VaultFailure::Denied(format!("the vault URL is invalid: {e}")))?;
    url.query_pairs_mut().append_pair("names", &names.join(","));
    let res = client()
        .get(url)
        .bearer_auth(&creds.token)
        .send()
        .await
        .map_err(|e| classify_send_error(&creds.url, &e))?;
    if !res.status().is_success() {
        let status = res.status();
        let message = api_error(status, "fetching secret values").to_string();
        return Err(match status.as_u16() {
            401 | 403 => VaultFailure::Denied(format!("team vault denied the request: {message}")),
            429 => VaultFailure::Failed(message),
            _ if status.is_redirection() || status.is_client_error() => {
                VaultFailure::Denied(format!("the team vault rejected the request ({message})"))
            }
            _ => VaultFailure::Failed(message),
        });
    }
    let body: SecretValues = res.json().await.map_err(|e| {
        VaultFailure::Denied(format!(
            "the team vault sent a response fed cannot read: {e}"
        ))
    })?;
    Ok(body.values)
}

/// Result of joining an in-flight vault fetch within a deadline.
pub enum VaultJoin {
    /// The fetch completed (with values or a classified failure).
    Answered(VaultResult),
    /// The deadline elapsed with the request still in flight.
    Pending,
}

/// Handle to a vault fetch running on its own OS thread (with a current-thread
/// tokio runtime, so it is safe to spawn from inside tokio). The fetch is fired
/// eagerly; the caller joins it at the point of use with a chosen budget.
///
/// Dropping the handle abandons the request — the thread runs to completion in
/// the background. That is deliberate: an abandoned cold-start request has
/// already triggered the container boot and DB resume, so it doubles as the
/// warm ping for the next run.
pub struct VaultHandle {
    rx: std::sync::mpsc::Receiver<VaultResult>,
    /// Cloud URL, for warnings that name the unreachable backend.
    pub url: String,
}

impl VaultHandle {
    /// Wait up to `budget` for the fetch to complete.
    pub fn join(&self, budget: Duration) -> VaultJoin {
        use std::sync::mpsc::RecvTimeoutError;
        match self.rx.recv_timeout(budget) {
            Ok(result) => VaultJoin::Answered(result),
            Err(RecvTimeoutError::Timeout) => VaultJoin::Pending,
            Err(RecvTimeoutError::Disconnected) => VaultJoin::Answered(Err(VaultFailure::Failed(
                "cloud: vault lookup thread ended unexpectedly".to_string(),
            ))),
        }
    }
}

/// Fire a vault fetch on a background thread and return a handle to join later.
///
/// Returns `None` when not logged in or the checkout isn't linked — the caller
/// falls back to local/cache behavior. A panicked thread surfaces as a
/// `Disconnected` join, landing in the warning path rather than aborting.
pub fn spawn_fetch_values(work_dir: &Path, names: &[String]) -> Option<VaultHandle> {
    let creds = load_credentials()?;
    let link = load_link(work_dir)?;
    let url = creds.url.clone();
    let names = names.to_vec();
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let out = (|| -> VaultResult {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|e| VaultFailure::Failed(format!("cloud: runtime: {}", e)))?;
            rt.block_on(fetch_values_inner(&creds, &link, &names))
        })();
        let _ = tx.send(out);
    });
    Some(VaultHandle { rx, url })
}

/// Run a cloud future, printing a one-line progress hint to stderr if it
/// takes longer than the grace window. Used by the deliberately-blocking
/// `fed secrets ls` command (D5): the user asked the cloud a question, so
/// correctness wins over latency — they get the generous budget and the hint,
/// never a cache fallback.
pub async fn with_slow_fetch_hint<F, T>(fut: F) -> T
where
    F: std::future::Future<Output = T>,
{
    tokio::pin!(fut);
    match tokio::time::timeout(vault_grace(), &mut fut).await {
        Ok(v) => v,
        Err(_) => {
            eprintln!("Fetching secrets from the team vault…");
            fut.await
        }
    }
}

// ── Logout revocation ───────────────────────────────────────────────────

/// Outcome of asking the server to revoke the presented bearer token via
/// `DELETE /api/v1/cli/session`. The endpoint is idempotent: it revokes the
/// token identified by the bearer's hash, so the CLI never needs a token UUID.
pub enum Revocation {
    /// The token is dead server-side. Emitted ONLY for 200 — whether the body
    /// says `revoked:true` (a live token was just killed) or `revoked:false`
    /// (already dead / unknown), the token no longer authenticates. Nothing else
    /// proves the revoke handler ran, so no other status maps here.
    Revoked,
    /// Revocation did not take effect — the token may remain valid until it
    /// expires. Carries a short human reason for the honest logout message
    /// (never the token itself). Emitted for 401 (the deployed endpoint never
    /// emits 401, so it now means an unexpected intermediary or auth failure —
    /// NOT a confirmed revoke), 429 (the IP rate limiter refused the revoke),
    /// any other non-2xx, and network/timeout errors.
    Failed(String),
}

/// Revoke the currently-presented bearer token. A single, bounded attempt with
/// no retry: `fed logout` removes the local credential regardless of the result,
/// so a booting backend is not worth the vault budget here — a modest ~10s cap,
/// and connect failures (`is_connect`) fail fast rather than waiting it out.
pub async fn revoke_current_token(creds: &Credentials) -> Revocation {
    let url = match api_url(&creds.url, "/api/v1/cli/session") {
        Ok(url) => url,
        Err(e) => return Revocation::Failed(e.to_string()),
    };
    let client = match client_builder().timeout(Duration::from_secs(10)).build() {
        Ok(client) => client,
        Err(e) => return Revocation::Failed(format!("cloud client: {}", e)),
    };
    let res = match client.delete(url).bearer_auth(&creds.token).send().await {
        Ok(res) => res,
        // Reuse the classified send-error handling: connect/DNS fails fast,
        // timeouts and other transport errors are likewise a failed revoke.
        Err(e) => {
            return Revocation::Failed(classify_send_error(&creds.url, &e).message().to_string());
        }
    };
    match res.status().as_u16() {
        // Only a 200 proves the server-side revoke handler ran. The endpoint
        // never emits 401, so a 401 means an unexpected intermediary or auth
        // failure — reporting it as a confirmed revocation would be unsafe.
        200 => Revocation::Revoked,
        401 => Revocation::Failed("server rejected the token (401)".to_string()),
        426 => Revocation::Failed(
            "this version of fed is too old for the server; upgrade fed".to_string(),
        ),
        429 => Revocation::Failed("rate limited".to_string()),
        _ => Revocation::Failed(format!("server returned {}", res.status())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_creds() -> Credentials {
        Credentials {
            url: "https://app.example.com".to_string(),
            token: "super-secret-token".to_string(),
        }
    }

    #[cfg(unix)]
    fn mode_of(path: &Path) -> u32 {
        use std::os::unix::fs::PermissionsExt;
        std::fs::metadata(path).unwrap().permissions().mode() & 0o777
    }

    /// Credentials are written 0600 on first creation — never a broader window.
    #[test]
    fn save_credentials_creates_owner_only_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(".fed").join("credentials");
        save_credentials_to(&path, &sample_creds()).unwrap();
        assert!(path.exists());
        #[cfg(unix)]
        assert_eq!(mode_of(&path), 0o600);
    }

    /// Overwriting existing credentials keeps them at 0600.
    #[test]
    fn overwrite_credentials_keeps_owner_only() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("credentials");
        save_credentials_to(&path, &sample_creds()).unwrap();
        let mut updated = sample_creds();
        updated.token = "rotated-token".to_string();
        save_credentials_to(&path, &updated).unwrap();
        let loaded = load_credentials_from(&path).unwrap();
        assert_eq!(loaded.token, "rotated-token");
        #[cfg(unix)]
        assert_eq!(mode_of(&path), 0o600);
    }

    /// A failed save leaves the previously valid credentials intact — no partial
    /// destination. The failure is injected by making the destination directory
    /// read-only so the atomic writer cannot create its (randomly-named) temp
    /// sibling. Skipped under root, which bypasses directory permissions.
    #[cfg(unix)]
    #[test]
    fn failed_save_preserves_previous_credentials() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("credentials");
        save_credentials_to(&path, &sample_creds()).unwrap();

        // Probe: can we still create files in a 0500 dir (i.e. are we root)?
        std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o500)).unwrap();
        let probe = dir.path().join(".probe");
        let blocked = std::fs::File::create(&probe).is_err();
        let _ = std::fs::remove_file(&probe);
        if !blocked {
            std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
            return; // running as root — injection can't work; skip.
        }

        let mut updated = sample_creds();
        updated.token = "would-be-lost".to_string();
        let result = save_credentials_to(&path, &updated);
        std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
        assert!(result.is_err());

        let loaded = load_credentials_from(&path).unwrap();
        assert_eq!(
            loaded.token, "super-secret-token",
            "the previous valid credentials must survive a failed save"
        );
    }

    /// Save then reload round-trips the credentials.
    #[test]
    fn save_then_reload_round_trips() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("credentials");
        save_credentials_to(&path, &sample_creds()).unwrap();
        let loaded = load_credentials_from(&path).unwrap();
        assert_eq!(loaded.url, "https://app.example.com");
        assert_eq!(loaded.token, "super-secret-token");
    }

    /// Loading an over-permissive credentials file tightens it to 0600.
    #[cfg(unix)]
    #[test]
    fn load_tightens_overpermissive_credentials() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("credentials");
        let yaml = serde_yaml::to_string(&sample_creds()).unwrap();
        std::fs::write(&path, yaml).unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).unwrap();
        assert_eq!(mode_of(&path), 0o644);

        let loaded = load_credentials_from(&path).unwrap();
        assert_eq!(loaded.token, "super-secret-token");
        assert_eq!(mode_of(&path), 0o600, "load must tighten a loose file");
    }

    /// A `reqwest` error from connecting to a port with nothing listening must
    /// classify as `Unreachable` — waiting is pointless.
    #[tokio::test]
    async fn connect_refused_classifies_as_unreachable() {
        // Bind then drop to obtain a definitely-closed port.
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        drop(listener);

        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(5))
            .build()
            .unwrap();
        let url = format!("http://127.0.0.1:{}", port);
        let err = client.get(&url).send().await.unwrap_err();
        let failure = classify_send_error(&url, &err);
        assert!(
            failure.is_unreachable(),
            "connect-refused must be Unreachable, got: {:?}",
            failure
        );
    }

    /// A server that accepts the connection but never responds must classify as
    /// `Failed` (a timeout), not `Unreachable`: the backend is alive but slow,
    /// which is exactly the cold-start case worth waiting on.
    #[tokio::test]
    async fn accepted_but_silent_classifies_as_failed_timeout() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        // Accept connections and hold them open without replying.
        std::thread::spawn(move || {
            let mut held = Vec::new();
            for stream in listener.incoming() {
                match stream {
                    Ok(s) => held.push(s),
                    Err(_) => break,
                }
            }
        });

        let client = reqwest::Client::builder()
            .timeout(Duration::from_millis(300))
            .build()
            .unwrap();
        let url = format!("http://127.0.0.1:{}", port);
        let err = client.get(&url).send().await.unwrap_err();
        let failure = classify_send_error(&url, &err);
        assert!(
            !failure.is_unreachable(),
            "an accepted-but-silent server is a timeout (Failed), not Unreachable: {:?}",
            failure
        );
        assert!(err.is_timeout(), "sanity: the error should be a timeout");
    }

    /// One-shot HTTP server: replies to the first request with `status_line`
    /// (e.g. "200 OK") and `body`, then closes. Returns the base URL. Used to
    /// exercise the logout revocation status classification against real HTTP.
    fn spawn_one_shot(status_line: &'static str, body: &'static str) -> String {
        spawn_one_shot_with_headers(status_line, body, "")
    }

    fn spawn_one_shot_with_headers(
        status_line: &'static str,
        body: &'static str,
        headers: &'static str,
    ) -> String {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        std::thread::spawn(move || {
            use std::io::{Read, Write};
            if let Ok((mut stream, _)) = listener.accept() {
                let mut buf = [0u8; 1024];
                let _ = stream.read(&mut buf);
                let resp = format!(
                    "HTTP/1.1 {}\r\n{}Content-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    status_line,
                    headers,
                    body.len(),
                    body
                );
                let _ = stream.write_all(resp.as_bytes());
            }
        });
        format!("http://127.0.0.1:{}", port)
    }

    fn creds_at(url: String) -> Credentials {
        Credentials {
            url,
            token: "super-secret-token".to_string(),
        }
    }

    #[tokio::test]
    async fn vault_fetch_distinguishes_denial_from_transient_failure() {
        let link = CloudLink {
            org: "acme".into(),
            project: "web".into(),
            secret_cache: crate::orchestrator::SecretCacheMode::Memory,
        };
        let names = vec!["API_KEY".to_string()];
        for (status, opening) in [
            ("401 Unauthorized", "team vault denied the request: "),
            ("403 Forbidden", "team vault denied the request: "),
            ("404 Not Found", "the team vault rejected the request ("),
            (
                "426 Upgrade Required",
                "the team vault rejected the request (",
            ),
            (
                "307 Temporary Redirect",
                "the team vault rejected the request (",
            ),
        ] {
            let creds = creds_at(spawn_one_shot(status, "{}"));
            match fetch_values_inner(&creds, &link, &names).await {
                Err(VaultFailure::Denied(m)) => {
                    assert!(m.starts_with(opening), "{status}: {m}");
                    let code = status.split(' ').next().unwrap();
                    assert!(m.contains(code), "{status}: detail kept: {m}");
                }
                other => panic!("{status}: expected Denied, got {other:?}"),
            }
        }
        let creds = creds_at(spawn_one_shot("200 OK", "not json"));
        match fetch_values_inner(&creds, &link, &names).await {
            Err(VaultFailure::Denied(m)) => assert!(
                m.starts_with("the team vault sent a response fed cannot read: "),
                "{m}"
            ),
            other => panic!("expected Denied, got {other:?}"),
        }
        let creds = creds_at("http://vault.example.com".into());
        match fetch_values_inner(&creds, &link, &names).await {
            Err(VaultFailure::Denied(m)) => assert!(
                m.starts_with("the vault URL is invalid: ")
                    && m.contains("vault URL must be an HTTPS origin"),
                "{m}"
            ),
            other => panic!("expected Denied, got {other:?}"),
        }
        for status in ["429 Too Many Requests", "503 Service Unavailable"] {
            let creds = creds_at(spawn_one_shot(status, "{}"));
            assert!(matches!(
                fetch_values_inner(&creds, &link, &names).await,
                Err(VaultFailure::Failed(_))
            ));
        }
    }

    /// A vault request answered with 426 Upgrade Required must tell the user
    /// their fed is too old and how to upgrade — not just echo the status.
    #[tokio::test]
    async fn vault_426_tells_user_to_upgrade_fed() {
        let url = spawn_one_shot("426 Upgrade Required", "{}");
        let err = match whoami(&creds_at(url)).await {
            Err(e) => e.to_string(),
            Ok(_) => panic!("a 426 response must surface as an error"),
        };
        assert!(
            err.contains("too old") && err.contains("brew upgrade fed"),
            "426 must explain the upgrade path, got: {}",
            err
        );
    }

    /// One-shot server that captures the raw request and hands it back over a
    /// channel, then answers 200 with `body`. For asserting what we send.
    fn spawn_capturing(body: &'static str) -> (String, std::sync::mpsc::Receiver<String>) {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let (tx, rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            use std::io::{Read, Write};
            if let Ok((mut stream, _)) = listener.accept() {
                // Read until end-of-headers: a single read may legally return
                // only a prefix, which would drop headers from the capture.
                let mut buf = Vec::new();
                let mut chunk = [0u8; 1024];
                while !buf.windows(4).any(|w| w == b"\r\n\r\n") {
                    match stream.read(&mut chunk) {
                        Ok(0) | Err(_) => break,
                        Ok(n) => buf.extend_from_slice(&chunk[..n]),
                    }
                }
                let _ = tx.send(String::from_utf8_lossy(&buf).to_string());
                let resp = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    body.len(),
                    body
                );
                let _ = stream.write_all(resp.as_bytes());
            }
        });
        (format!("http://127.0.0.1:{}", port), rx)
    }

    /// Every cloud request must carry this build's version in `x-fed-version`
    /// — that header is what lets the server answer 426 to outdated clients.
    #[tokio::test]
    async fn cloud_requests_send_version_header() {
        let (url, rx) = spawn_capturing("{\"user\":{\"name\":null,\"email\":null},\"orgs\":[]}");
        whoami(&creds_at(url)).await.unwrap();
        let request = rx
            .recv_timeout(Duration::from_secs(5))
            .expect("server captured a request")
            .to_lowercase();
        let expected = format!("{}: {}", VERSION_HEADER, env!("CARGO_PKG_VERSION"));
        assert!(
            request.contains(&expected),
            "request must carry `{}`, got:\n{}",
            expected,
            request
        );
        assert!(request.contains("x-fed-api-version: 2\r\n"));
        assert!(
            request.contains(concat!("fed/", env!("CARGO_PKG_VERSION"))),
            "user agent should also name fed and its version:\n{}",
            request
        );
    }

    /// The revoke client is built separately from the vault client, but shares
    /// `client_builder()` — it must send the version header too.
    #[tokio::test]
    async fn revoke_requests_send_version_header() {
        let (url, rx) = spawn_capturing("{\"revoked\":true}");
        let outcome = revoke_current_token(&creds_at(url)).await;
        assert!(matches!(outcome, Revocation::Revoked));
        let request = rx
            .recv_timeout(Duration::from_secs(5))
            .expect("server captured a request")
            .to_lowercase();
        let expected = format!("{}: {}", VERSION_HEADER, env!("CARGO_PKG_VERSION"));
        assert!(
            request.contains(&expected),
            "revoke request must carry `{}`, got:\n{}",
            expected,
            request
        );
        assert!(request.contains("x-fed-api-version: 2\r\n"));
    }

    /// A 426 on revoke is a failed revoke that names the version problem.
    #[tokio::test]
    async fn revoke_426_is_failed_with_upgrade_hint() {
        let url = spawn_one_shot("426 Upgrade Required", "{}");
        match revoke_current_token(&creds_at(url)).await {
            Revocation::Failed(reason) => assert!(
                reason.contains("too old"),
                "426 revoke should mention the version problem, got: {}",
                reason
            ),
            Revocation::Revoked => panic!("426 must not classify as revoked"),
        }
    }

    /// 200 with `revoked:true` — a live token was killed — is a clean revoke.
    #[tokio::test]
    async fn revoke_200_revoked_true_is_revoked() {
        let url = spawn_one_shot("200 OK", "{\"revoked\":true}");
        assert!(matches!(
            revoke_current_token(&creds_at(url)).await,
            Revocation::Revoked
        ));
    }

    /// 200 with `revoked:false` (already dead / unknown token) is still a
    /// success: the endpoint is idempotent and the token is not live.
    #[tokio::test]
    async fn revoke_200_revoked_false_is_revoked() {
        let url = spawn_one_shot("200 OK", "{\"revoked\":false}");
        assert!(matches!(
            revoke_current_token(&creds_at(url)).await,
            Revocation::Revoked
        ));
    }

    /// 401 is not emitted by this endpoint today; it now means an unexpected
    /// intermediary or auth failure — never a proof that the revoke handler ran.
    /// It must classify as a FAILED revoke (honest), not a success.
    #[tokio::test]
    async fn revoke_401_is_failed_not_revoked() {
        let url = spawn_one_shot("401 Unauthorized", "{}");
        match revoke_current_token(&creds_at(url)).await {
            Revocation::Failed(reason) => assert!(
                !reason.contains("super-secret-token"),
                "reason leaked the token"
            ),
            Revocation::Revoked => panic!("401 must not classify as a confirmed revoke"),
        }
    }

    /// 429 means the server's revoke FAILED behind the IP limiter — never a
    /// success — and the reason must not leak the token.
    #[tokio::test]
    async fn revoke_429_is_failed_without_leaking_token() {
        let url = spawn_one_shot("429 Too Many Requests", "{\"error\":\"rate_limited\"}");
        match revoke_current_token(&creds_at(url)).await {
            Revocation::Failed(reason) => {
                assert!(
                    !reason.contains("super-secret-token"),
                    "reason leaked the token"
                );
            }
            Revocation::Revoked => panic!("429 must not classify as revoked"),
        }
    }

    /// Connection refused (nothing listening) is a failed revoke, and it must
    /// fail fast — well inside the 10s cap — because `is_connect` short-circuits.
    #[tokio::test]
    async fn revoke_connection_refused_is_failed_fast() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        drop(listener);
        let creds = creds_at(format!("http://127.0.0.1:{}", port));
        let start = std::time::Instant::now();
        let outcome = revoke_current_token(&creds).await;
        assert!(matches!(outcome, Revocation::Failed(_)));
        assert!(
            start.elapsed() < Duration::from_secs(5),
            "connect-refused must fail fast, took {:?}",
            start.elapsed()
        );
    }

    /// Sequential HTTP stub: serves the given (status, body) responses one
    /// connection at a time, in order. For exercising bounded retries.
    fn spawn_shots(responses: &'static [(&'static str, &'static str)]) -> String {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        std::thread::spawn(move || {
            use std::io::{Read, Write};
            for (status_line, body) in responses {
                let Ok((mut stream, _)) = listener.accept() else {
                    return;
                };
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

    /// First activation: 200 `activated:true` is durable.
    #[tokio::test]
    async fn activate_200_true_is_activated() {
        let url = spawn_one_shot("200 OK", "{\"activated\":true}");
        assert!(matches!(
            activate_token(&creds_at(url)).await,
            Activation::Activated
        ));
    }

    /// Idempotent retry: 200 `activated:false` (already activated) is still
    /// durable.
    #[tokio::test]
    async fn activate_200_false_is_activated() {
        let url = spawn_one_shot("200 OK", "{\"activated\":false}");
        assert!(matches!(
            activate_token(&creds_at(url)).await,
            Activation::Activated
        ));
    }

    /// FAIL CLOSED: a 200 whose body is not `{"activated": bool}` (endpoint
    /// misrouting) must not read as success — and the reason must not leak
    /// the token.
    #[tokio::test]
    async fn activate_200_garbage_body_fails_closed() {
        let url = spawn_shots(&[
            ("200 OK", "<html>welcome to the marketing site</html>"),
            ("200 OK", "<html>welcome to the marketing site</html>"),
            ("200 OK", "<html>welcome to the marketing site</html>"),
        ]);
        match activate_token(&creds_at(url)).await {
            Activation::Failed(reason) => assert!(
                !reason.contains("super-secret-token"),
                "reason leaked the token: {}",
                reason
            ),
            _ => panic!("an unparseable 200 body must fail closed"),
        }
    }

    /// 401 (dead token) classifies immediately — no pointless retries.
    #[tokio::test]
    async fn activate_401_is_dead_fast() {
        let url = spawn_one_shot("401 Unauthorized", "{}");
        let start = std::time::Instant::now();
        assert!(matches!(
            activate_token(&creds_at(url)).await,
            Activation::Dead
        ));
        assert!(
            start.elapsed() < Duration::from_secs(2),
            "401 must not be retried, took {:?}",
            start.elapsed()
        );
    }

    /// A transient failure (5xx) is retried and the login still becomes
    /// durable when a later attempt succeeds.
    #[tokio::test]
    async fn activate_retries_transient_failure_then_succeeds() {
        let url = spawn_shots(&[
            ("500 Internal Server Error", "{}"),
            ("200 OK", "{\"activated\":true}"),
        ]);
        assert!(matches!(
            activate_token(&creds_at(url)).await,
            Activation::Activated
        ));
    }

    /// All attempts failing yields Failed (no token leak) after the bounded
    /// number of tries.
    #[tokio::test]
    async fn activate_gives_up_after_bounded_retries() {
        let url = spawn_shots(&[
            ("500 Internal Server Error", "{}"),
            ("500 Internal Server Error", "{}"),
            ("500 Internal Server Error", "{}"),
        ]);
        match activate_token(&creds_at(url)).await {
            Activation::Failed(reason) => assert!(
                !reason.contains("super-secret-token"),
                "reason leaked the token: {}",
                reason
            ),
            _ => panic!("exhausted retries must classify as Failed"),
        }
    }

    /// Staged promotion: saving a pending credential leaves the active file
    /// untouched (and the staging file is 0600); promotion atomically
    /// replaces the active file and removes the pending one.
    #[test]
    fn staged_promotion_replaces_active_and_removes_pending() {
        let dir = tempfile::tempdir().unwrap();
        let files = CredentialFiles::in_dir(dir.path());
        let active_path = dir.path().join("credentials");
        let pending_path = dir.path().join("credentials.pending");

        save_credentials_to(&active_path, &sample_creds()).unwrap();
        let mut rotated = sample_creds();
        rotated.token = "rotated-token".to_string();
        files.save_pending_credentials(&rotated).unwrap();

        #[cfg(unix)]
        assert_eq!(mode_of(&pending_path), 0o600, "pending must be 0600");
        assert_eq!(
            load_credentials_from(&active_path).unwrap().token,
            "super-secret-token",
            "staging must not touch the active credential"
        );

        files.promote_pending_credentials().unwrap();
        assert_eq!(
            load_credentials_from(&active_path).unwrap().token,
            "rotated-token",
            "promotion must install the pending credential"
        );
        assert!(
            !pending_path.exists(),
            "promotion must consume the pending file"
        );
        #[cfg(unix)]
        assert_eq!(mode_of(&active_path), 0o600, "promoted file keeps 0600");

        // delete_pending on nothing reports false, not an error.
        assert!(!files.delete_pending_credentials().unwrap());
    }

    /// A 201 from the authorize-request endpoint yields the opaque request
    /// id and the poll secret.
    #[tokio::test]
    async fn create_auth_request_returns_id_and_poll_secret() {
        let url = spawn_one_shot_with_headers(
            "201 Created",
            "{\"request\":\"fedar_stub-request-id\",\"poll_secret\":\"fedps_stub-poll-secret\",\"pairing_code\":\"12345678\",\"expires_in\":300}",
            "X-Fed-Api-Version: 2\r\n",
        );
        let auth = create_auth_request(&url, "dev-box").await.unwrap();
        assert_eq!(auth.request, "fedar_stub-request-id");
        assert_eq!(auth.poll_secret, "fedps_stub-poll-secret");
        assert_eq!(auth.pairing_code, "12345678");
    }

    #[tokio::test]
    async fn login_rejects_missing_or_mismatched_api_version_before_parsing_body() {
        for headers in ["", "X-Fed-Api-Version: 1\r\n", "X-Fed-Api-Version: 3\r\n"] {
            let url = spawn_one_shot_with_headers("201 Created", "{}", headers);
            let err = create_auth_request(&url, "dev-box").await.unwrap_err();
            assert!(
                err.to_string()
                    .contains("server did not confirm API protocol version 2"),
                "unexpected error: {err}"
            );
        }
    }

    fn stub_auth() -> AuthRequest {
        AuthRequest {
            request: "fedar_stub-request-id".to_string(),
            poll_secret: "fedps_stub-poll-secret".to_string(),
            pairing_code: "12345678".to_string(),
        }
    }

    #[test]
    fn cloud_origin_rejects_insecure_or_ambiguous_urls() {
        for raw in [
            "http://vault.example.com",
            "http://localhost.evil.example",
            "https://user@vault.example.com",
            "https://vault.example.com/other",
            "https://vault.example.com?next=http://evil.example",
            "https://vault.example.com#fragment",
            "file:///tmp/vault",
        ] {
            assert!(cloud_base_url(raw).is_err(), "accepted {raw}");
        }
        assert!(cloud_base_url("https://vault.example.com").is_ok());
        assert!(cloud_base_url("http://127.0.0.1:1234").is_ok());
        assert!(cloud_base_url("http://[::1]:1234").is_ok());
    }

    /// `fed login` stores the origin form, so a typed trailing slash does not
    /// end up as `//cli/authorize` in the printed sign-in URL.
    #[test]
    fn cloud_origin_serializes_without_trailing_slash() {
        for (raw, origin) in [
            ("https://vault.example.com/", "https://vault.example.com"),
            (
                "https://vault.example.com:8443",
                "https://vault.example.com:8443",
            ),
            ("http://[::1]:1234/", "http://[::1]:1234"),
        ] {
            let url = cloud_base_url(raw).unwrap();
            assert_eq!(url.origin().ascii_serialization(), origin);
        }
    }

    #[tokio::test]
    async fn exchange_code_never_sends_its_body_to_a_redirect_target() {
        let destination = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        destination.set_nonblocking(true).unwrap();
        let destination_url = format!("http://{}", destination.local_addr().unwrap());
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        std::thread::spawn(move || {
            use std::io::{Read, Write};
            let (mut stream, _) = listener.accept().unwrap();
            let mut request = [0u8; 1024];
            let _ = stream.read(&mut request);
            let response = format!(
                "HTTP/1.1 307 Temporary Redirect\r\nLocation: {destination_url}/capture\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
            );
            stream.write_all(response.as_bytes()).unwrap();
        });
        let err = match exchange_code(&url, "single-use-secret").await {
            Ok(_) => panic!("redirect must not complete a token exchange"),
            Err(err) => err.to_string(),
        };
        assert!(
            err.contains("307"),
            "redirect must surface as an error: {err}"
        );
        assert!(matches!(
            destination.accept(),
            Err(err) if err.kind() == std::io::ErrorKind::WouldBlock
        ));
    }

    /// The poll answers map onto their outcomes; 410 and 429 are outcomes,
    /// not errors, so the caller's retry logic can tell them apart.
    #[tokio::test]
    async fn poll_auth_request_maps_answers_to_outcomes() {
        let url = spawn_one_shot("200 OK", "{\"status\":\"pending\"}");
        assert!(matches!(
            poll_auth_request(&url, &stub_auth()).await.unwrap(),
            PollOutcome::Pending
        ));
        let url = spawn_one_shot("200 OK", "{\"code\":\"fedac_stub-code\"}");
        match poll_auth_request(&url, &stub_auth()).await.unwrap() {
            PollOutcome::Code(code) => assert_eq!(code, "fedac_stub-code"),
            _ => panic!("a code answer must map to PollOutcome::Code"),
        }
        let url = spawn_one_shot("410 Gone", "{\"error\":\"request_gone\"}");
        assert!(matches!(
            poll_auth_request(&url, &stub_auth()).await.unwrap(),
            PollOutcome::Gone
        ));
        let url = spawn_one_shot("429 Too Many Requests", "{\"error\":\"rate_limited\"}");
        assert!(matches!(
            poll_auth_request(&url, &stub_auth()).await.unwrap(),
            PollOutcome::RateLimited
        ));
    }

    /// A 200 with neither a code nor a pending status is contract drift and
    /// must fail — without echoing the poll secret anywhere.
    #[tokio::test]
    async fn poll_auth_request_rejects_malformed_answers() {
        let url = spawn_one_shot("200 OK", "{\"unexpected\":true}");
        let err = poll_auth_request(&url, &stub_auth()).await.unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("fed login"), "actionable message: {}", msg);
        assert!(
            !msg.contains("fedps_stub-poll-secret"),
            "poll errors must never carry the secret: {}",
            msg
        );
    }

    /// A 426 (Upgrade Required — the server refusing this CLI's protocol
    /// version; reserved for future breaking changes) maps to a clear
    /// upgrade hint rather than a generic failure.
    #[tokio::test]
    async fn status_426_maps_to_upgrade_hint_on_login_start() {
        let url = spawn_one_shot("426 Upgrade Required", "{\"error\":\"upgrade_fed\"}");
        let err = create_auth_request(&url, "dev-box").await.unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("too old") && msg.contains("upgrade fed"),
            "426 must map to the upgrade hint: {}",
            msg
        );
    }

    /// The same 426 mapping applies at code exchange.
    #[tokio::test]
    async fn status_426_maps_to_upgrade_hint_on_exchange() {
        let url = spawn_one_shot("426 Upgrade Required", "{\"error\":\"upgrade_fed\"}");
        let err = exchange_code(&url, "fedac_stub-code").await.unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("too old") && msg.contains("upgrade fed"),
            "426 must map to the upgrade hint: {}",
            msg
        );
    }

    /// Activation treats 426 as terminal — retrying the same protocol
    /// version cannot succeed, so the bounded retry loop must not spin.
    #[tokio::test]
    async fn activate_426_is_terminal_failure_with_upgrade_hint() {
        let url = spawn_one_shot("426 Upgrade Required", "{\"error\":\"upgrade_fed\"}");
        match activate_token(&creds_at(url)).await {
            Activation::Failed(reason) => assert!(
                reason.contains("too old") && !reason.contains("super-secret-token"),
                "426 activation failure must carry the upgrade hint and no token: {}",
                reason
            ),
            _ => panic!("426 must be a terminal activation failure"),
        }
    }

    /// A 201 from the token endpoint yields the bearer token.
    #[tokio::test]
    async fn exchange_code_returns_token() {
        let url = spawn_one_shot("201 Created", "{\"token\":\"fed_stub-bearer\"}");
        let token = exchange_code(&url, "fedac_stub-code").await.unwrap();
        assert_eq!(token, "fed_stub-bearer");
    }

    /// A 400 (invalid/expired/used code — the server does not distinguish)
    /// maps to the fixed friendly message, which must NOT contain the code.
    #[tokio::test]
    async fn exchange_code_400_is_friendly_and_never_leaks_the_code() {
        let url = spawn_one_shot("400 Bad Request", "{\"error\":\"code\"}");
        let err = exchange_code(&url, "fedac_super-secret-code")
            .await
            .unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("expired or was already used"),
            "message should explain what happened: {}",
            msg
        );
        assert!(
            msg.contains("fed login"),
            "message should say what to do: {}",
            msg
        );
        assert!(
            !msg.contains("fedac_super-secret-code") && !msg.contains("super-secret"),
            "error must never contain the exchange code: {}",
            msg
        );
    }

    #[test]
    fn vault_knob_defaults_are_sane() {
        assert_eq!(vault_grace(), Duration::from_secs(2));
        assert_eq!(vault_timeout(), Duration::from_secs(60));
        assert_eq!(vault_max_age(), Duration::from_secs(24 * 60 * 60));
        assert_eq!(vault_ttl(), Duration::from_secs(300));
    }

    #[test]
    fn vault_knobs_parse_their_value_and_fall_back_on_junk() {
        let default = Duration::from_secs(9);

        // A set, well-formed value wins over the default. Without this, the
        // knobs could be ignored entirely and every other test would still pass.
        assert_eq!(
            duration_or_default(Some("500ms"), default),
            Duration::from_millis(500)
        );
        assert_eq!(
            duration_or_default(Some("30m"), default),
            Duration::from_secs(30 * 60)
        );

        // The hour suffix matters most here: FED_VAULT_MAX_AGE defaults to 24h,
        // so an hour-scale override is the natural thing to write.
        assert_eq!(
            duration_or_default(Some("1h"), default),
            Duration::from_secs(3600)
        );

        // Unparseable values fall back rather than panicking or zeroing the
        // knob — a zero grace would turn every cold vault into a hard block.
        assert_eq!(
            duration_or_default(Some("not-a-duration"), default),
            default
        );
        assert_eq!(duration_or_default(Some(""), default), default);

        // Unset falls back too.
        assert_eq!(duration_or_default(None, default), default);
    }
}

#[cfg(test)]
mod secret_write_tests {
    use super::*;
    use std::time::Duration;

    const VALUE: &str = "s3cr3t-value-must-not-leak";

    /// One-shot server that answers `status_line` with `body` and hands back the
    /// whole request, headers and body, so a test can check what was sent.
    fn spawn_answering(
        status_line: &'static str,
        body: &'static str,
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

    fn creds(url: String) -> Credentials {
        Credentials {
            url,
            token: "fed_test-token".into(),
        }
    }

    fn link() -> CloudLink {
        CloudLink {
            org: "acme".into(),
            project: "web".into(),
            secret_cache: crate::orchestrator::SecretCacheMode::Memory,
        }
    }

    #[tokio::test]
    async fn put_secret_sends_the_value_only_in_the_json_body() {
        let (url, rx) = spawn_answering("201 Created", "{\"ok\":true}");
        put_secret(&creds(url), &link(), "API_KEY", VALUE)
            .await
            .unwrap();
        let request = rx.recv_timeout(Duration::from_secs(5)).unwrap();
        let (head, body) = request.split_once("\r\n\r\n").unwrap();
        assert!(
            head.starts_with("PUT /api/v1/orgs/acme/projects/web/secrets/API_KEY HTTP/1.1\r\n"),
            "{head}"
        );
        let head = head.to_ascii_lowercase();
        assert!(head.contains("authorization: bearer fed_test-token\r\n"));
        assert!(head.contains("x-fed-api-version: 2\r\n"));
        assert!(head.contains("content-type: application/json\r\n"));
        assert!(!head.contains(VALUE));
        let json: serde_json::Value = serde_json::from_str(body).unwrap();
        assert_eq!(json, serde_json::json!({ "value": VALUE }));
    }

    #[tokio::test]
    async fn delete_secret_sends_a_bodyless_delete() {
        let (url, rx) = spawn_answering("200 OK", "{\"ok\":true}");
        delete_secret(&creds(url), &link(), "API_KEY")
            .await
            .unwrap();
        let request = rx.recv_timeout(Duration::from_secs(5)).unwrap();
        assert!(
            request
                .starts_with("DELETE /api/v1/orgs/acme/projects/web/secrets/API_KEY HTTP/1.1\r\n"),
            "{request}"
        );
        assert!(
            request
                .to_ascii_lowercase()
                .contains("x-fed-api-version: 2\r\n")
        );
        assert!(request.ends_with("\r\n\r\n"), "no body: {request}");
    }

    #[tokio::test]
    async fn secret_writes_map_refusals_to_clear_messages_without_the_value() {
        let cases: [(&'static str, &'static str, &str); 9] = [
            (
                "401 Unauthorized",
                "{\"error\":\"unauthenticated\"}",
                "run `fed login`",
            ),
            (
                "403 Forbidden",
                "{\"error\":\"admin_only\"}",
                "only org admins can set secrets in acme/web",
            ),
            (
                "403 Forbidden",
                "{\"error\":\"session_only\"}",
                "does not accept secret changes from the CLI yet",
            ),
            (
                "404 Not Found",
                "{\"error\":\"project\"}",
                "project acme/web not found",
            ),
            (
                "404 Not Found",
                "{\"error\":\"org\"}",
                "project acme/web not found",
            ),
            (
                "429 Too Many Requests",
                "{\"error\":\"rate_limited\"}",
                "rate limited",
            ),
            (
                "400 Bad Request",
                "{\"error\":\"value\"}",
                "rejected the value",
            ),
            (
                "426 Upgrade Required",
                "{\"error\":\"unsupported_api_version\"}",
                "too old",
            ),
            ("500 Internal Server Error", "", "500"),
        ];
        for (status, body, expected) in cases {
            let (url, _rx) = spawn_answering(status, body);
            let err = put_secret(&creds(url), &link(), "API_KEY", VALUE)
                .await
                .unwrap_err()
                .to_string();
            assert!(err.contains(expected), "{status}: {err}");
            assert!(!err.contains(VALUE), "{status} leaked the value: {err}");
        }
    }

    #[tokio::test]
    async fn delete_secret_reports_a_missing_secret() {
        let (url, _rx) = spawn_answering("404 Not Found", "{\"error\":\"secret\"}");
        let found = delete_secret(&creds(url), &link(), "API_KEY")
            .await
            .unwrap();
        assert_eq!(found, Deletion::NotSet);
        let (url, _rx) = spawn_answering("404 Not Found", "{\"error\":\"project\"}");
        let err = delete_secret(&creds(url), &link(), "API_KEY")
            .await
            .unwrap_err()
            .to_string();
        assert!(err.contains("project acme/web not found"), "{err}");
        let (url, _rx) = spawn_answering("403 Forbidden", "{\"error\":\"admin_only\"}");
        let err = delete_secret(&creds(url), &link(), "API_KEY")
            .await
            .unwrap_err()
            .to_string();
        assert!(err.contains("only org admins can remove secrets"), "{err}");
    }

    /// A redirect is a refusal: the token and the value must not follow it.
    #[tokio::test]
    async fn put_secret_does_not_follow_a_redirect() {
        let (url, _rx) = spawn_answering("307 Temporary Redirect", "");
        let err = put_secret(&creds(url), &link(), "API_KEY", VALUE)
            .await
            .unwrap_err()
            .to_string();
        assert!(err.contains("307"), "{err}");
    }

    #[tokio::test]
    async fn secret_writes_refuse_bad_names_and_insecure_urls_before_sending() {
        let remote_http = creds("http://vault.example.com".into());
        assert!(
            put_secret(&remote_http, &link(), "API_KEY", VALUE)
                .await
                .is_err()
        );
        let local = creds("http://127.0.0.1:9".into());
        for name in ["", "1KEY", "A/B", "../x", "KEY=value", &"A".repeat(129)] {
            let err = put_secret(&local, &link(), name, VALUE)
                .await
                .unwrap_err()
                .to_string();
            assert!(err.contains("invalid secret name"), "{name}: {err}");
            assert!(!err.contains("value"), "the name is not echoed: {err}");
        }
        assert!(valid_secret_name("_A1"));
        assert!(valid_secret_name(&"A".repeat(128)));
    }
}
