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

// ── Vault timing knobs ──────────────────────────────────────────────────
//
// A booting (scale-to-zero) backend is not a failing one: the timeout must be
// a function of whether we can proceed without the answer, not a constant.
// Four knobs realize that (see 02-cold-vault.md and 04-vault-ttl-cache.md),
// each with a default chosen so it rarely matters:
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
/// `426 Upgrade Required` to clients that are too old — see [`api_error`].
/// Clients ≤ 7.2.0 predate this header; the server must treat its absence as
/// "too old to say".
pub const VERSION_HEADER: &str = "x-fed-version";

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
    reqwest::Client::builder()
        .user_agent(concat!("fed/", env!("CARGO_PKG_VERSION")))
        .default_headers(headers)
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
        .get(format!("{}/api/v1/me", creds.url))
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

// ── Login: authorization request + code exchange ──────────────────────
//
// `fed login` never receives the bearer token through the browser. The CLI
// first registers an AUTHORIZATION REQUEST server-side; the browser URL
// carries only that request's opaque id — an unguessable handle, not a
// credential: approval still requires an authenticated browser session plus
// an explicit click. Approval yields a short-lived, single-use EXCHANGE CODE
// (hashed at rest server-side), which the CLI redeems for the bearer token
// over HTTPS via `POST /api/v1/cli/token`. The token therefore never appears
// in a URL, browser page, redirect, log line, or terminal output.

/// Body for `POST /api/v1/cli/authorize-request`. Built via [`Self::browser`]
/// or [`Self::manual`] so the two shapes the server accepts (`{port, state,
/// label}` and `{manual: true, label}`) cannot be mixed. The device label
/// travels only in this POST body over HTTPS — never in a URL.
#[derive(Serialize)]
pub struct AuthRequestBody {
    #[serde(skip_serializing_if = "Option::is_none")]
    port: Option<u16>,
    #[serde(skip_serializing_if = "Option::is_none")]
    state: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    manual: Option<bool>,
    label: String,
}

impl AuthRequestBody {
    /// Browser mode: the server will redirect the approving browser to the
    /// CLI's loopback listener on `port`, echoing `state` (CSRF/correlation
    /// defense in depth between the CLI and its own callback — not a bearer
    /// credential).
    pub fn browser(port: u16, state: String, label: String) -> Self {
        Self {
            port: Some(port),
            state: Some(state),
            manual: None,
            label,
        }
    }

    /// Manual (`--no-browser`) mode: the approval page displays the exchange
    /// code for copy-paste instead of redirecting to a loopback port.
    pub fn manual(label: String) -> Self {
        Self {
            port: None,
            state: None,
            manual: Some(true),
            label,
        }
    }
}

#[derive(Deserialize)]
struct AuthRequestResponse {
    request: String,
}

/// Create a server-side authorization request and return its opaque id — the
/// only thing that may ever appear in the authorize URL.
pub async fn create_auth_request(base_url: &str, body: &AuthRequestBody) -> Result<String> {
    let res = client()
        .post(format!("{}/api/v1/cli/authorize-request", base_url))
        .json(body)
        .send()
        .await
        .map_err(|e| Error::Validation(format!("cloud: cannot reach {}: {}", base_url, e)))?;
    if !res.status().is_success() {
        return Err(api_error(res.status(), "starting login"));
    }
    let body: AuthRequestResponse = res
        .json()
        .await
        .map_err(|e| Error::Validation(format!("cloud: bad authorize-request response: {}", e)))?;
    Ok(body.request)
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
        .post(format!("{}/api/v1/cli/token", base_url))
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
    let mut last = String::new();
    for attempt in 0..3u32 {
        if attempt > 0 {
            tokio::time::sleep(Duration::from_millis(250 * u64::from(attempt))).await;
        }
        let res = client()
            .post(format!("{}/api/v1/cli/activate", creds.url))
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
        .get(format!("{}/api/v1/orgs/{}/projects", creds.url, org))
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
        .get(format!(
            "{}/api/v1/orgs/{}/projects/{}/secrets",
            creds.url, link.org, link.project
        ))
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

#[derive(Deserialize)]
pub struct SecretValues {
    pub values: HashMap<String, String>,
    #[serde(default)]
    pub missing: Vec<String>,
}

/// Why a vault fetch could not return values, classified so the caller can
/// pick the right wait. `reqwest` distinguishes the two cases and the cold
/// probe confirms which fires (see 02-cold-vault.md):
///
/// - `Unreachable`: `is_connect()` / DNS — nothing is listening, or we're
///   offline. Waiting is pointless; fall back to cache immediately.
/// - `Failed`: a timeout (the backend is alive but booting) or an HTTP/parse
///   error. The value could not be had this attempt.
#[derive(Debug, Clone)]
pub enum VaultFailure {
    Unreachable(String),
    Failed(String),
}

impl VaultFailure {
    /// Human-readable reason, for warnings and the missing-secret error.
    pub fn message(&self) -> &str {
        match self {
            VaultFailure::Unreachable(m) | VaultFailure::Failed(m) => m,
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
    let res = client()
        .get(format!(
            "{}/api/v1/orgs/{}/projects/{}/secrets/values?names={}",
            creds.url,
            link.org,
            link.project,
            names.join(",")
        ))
        .bearer_auth(&creds.token)
        .send()
        .await
        .map_err(|e| classify_send_error(&creds.url, &e))?;
    if !res.status().is_success() {
        return Err(VaultFailure::Failed(
            api_error(res.status(), "fetching secret values").to_string(),
        ));
    }
    let body: SecretValues = res
        .json()
        .await
        .map_err(|e| VaultFailure::Failed(format!("cloud: bad values response: {}", e)))?;
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

/// Run a cloud future, printing a one-line "waking vault…" hint to stderr if it
/// takes longer than the grace window. Used by the deliberately-blocking
/// `fed secrets ls` command (D5): the user asked the cloud a question, so
/// correctness wins over latency — they get the generous budget and the hint,
/// never a cache fallback.
pub async fn with_waking_hint<F, T>(fut: F) -> T
where
    F: std::future::Future<Output = T>,
{
    tokio::pin!(fut);
    match tokio::time::timeout(vault_grace(), &mut fut).await {
        Ok(v) => v,
        Err(_) => {
            eprintln!("waking vault… (cold start can take ~20s)");
            fut.await
        }
    }
}

// ── Logout revocation (05-token-scope.md "Logout revocation") ───────────

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
    let client = match client_builder().timeout(Duration::from_secs(10)).build() {
        Ok(client) => client,
        Err(e) => return Revocation::Failed(format!("cloud client: {}", e)),
    };
    let res = match client
        .delete(format!("{}/api/v1/cli/session", creds.url))
        .bearer_auth(&creds.token)
        .send()
        .await
    {
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
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        std::thread::spawn(move || {
            use std::io::{Read, Write};
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

    fn creds_at(url: String) -> Credentials {
        Credentials {
            url,
            token: "super-secret-token".to_string(),
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
                let mut buf = [0u8; 4096];
                let n = stream.read(&mut buf).unwrap_or(0);
                let _ = tx.send(String::from_utf8_lossy(&buf[..n]).to_string());
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

    /// A 201 from the authorize-request endpoint yields the opaque request id.
    #[tokio::test]
    async fn create_auth_request_returns_request_id() {
        let url = spawn_one_shot(
            "201 Created",
            "{\"request\":\"fedar_stub-request-id\",\"expires_in\":300}",
        );
        let id = create_auth_request(&url, &AuthRequestBody::manual("dev-box".to_string()))
            .await
            .unwrap();
        assert_eq!(id, "fedar_stub-request-id");
    }

    /// A 426 (Upgrade Required — the server refusing this CLI's protocol
    /// version; reserved for future breaking changes) maps to a clear
    /// upgrade hint rather than a generic failure.
    #[tokio::test]
    async fn status_426_maps_to_upgrade_hint_on_login_start() {
        let url = spawn_one_shot("426 Upgrade Required", "{\"error\":\"upgrade_fed\"}");
        let err = create_auth_request(&url, &AuthRequestBody::manual("dev-box".to_string()))
            .await
            .unwrap_err();
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
