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
// Three knobs realize that (see 02-cold-vault.md), each with a default chosen
// so it rarely matters:
//
// - `FED_VAULT_GRACE` (2s): how long the resolver waits before falling back to
//   a fresh cache. Warm vaults answer in ~0.17s, well inside this.
// - `FED_VAULT_TIMEOUT` (60s): the blocking budget when the cache can't cover
//   the run and we must wait for a cold start. Also the HTTP client timeout.
// - `FED_VAULT_MAX_AGE` (24h): freshness bound on cached values. Beyond it, a
//   run blocks to refresh rather than serve a stale value forever.

use std::time::Duration;

fn env_duration(var: &str, default: Duration) -> Duration {
    std::env::var(var)
        .ok()
        .and_then(|s| crate::config::parse_duration_string(&s))
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

// ── API client ────────────────────────────────────────────────────────

fn client() -> &'static reqwest::Client {
    static CLIENT: std::sync::OnceLock<reqwest::Client> = std::sync::OnceLock::new();
    // The timeout is the blocking budget — long enough to ride out a cold
    // start (a booting backend answers, it just answers slowly). The resolver
    // enforces the *short* grace wait itself; this cap only bites the honest
    // block. Read from the env here (rather than a hardcoded constant) so
    // FED_VAULT_TIMEOUT tunes both in one place (D6).
    CLIENT.get_or_init(|| {
        reqwest::Client::builder()
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
    pub environment: String,
    pub updated_at: String,
    pub updated_by: String,
}

pub async fn list_secrets(
    creds: &Credentials,
    link: &CloudLink,
    env: &str,
) -> Result<Vec<SecretEntry>> {
    let res = client()
        .get(format!(
            "{}/api/v1/orgs/{}/projects/{}/secrets?env={}",
            creds.url, link.org, link.project, env
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
    env: &str,
    names: &[String],
) -> VaultResult {
    let res = client()
        .get(format!(
            "{}/api/v1/orgs/{}/projects/{}/secrets/values?env={}&names={}",
            creds.url,
            link.org,
            link.project,
            env,
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
pub fn spawn_fetch_values(work_dir: &Path, env: &str, names: &[String]) -> Option<VaultHandle> {
    let creds = load_credentials()?;
    let link = load_link(work_dir)?;
    let url = creds.url.clone();
    let env = env.to_string();
    let names = names.to_vec();
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let out = (|| -> VaultResult {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|e| VaultFailure::Failed(format!("cloud: runtime: {}", e)))?;
            rt.block_on(fetch_values_inner(&creds, &link, &env, &names))
        })();
        let _ = tx.send(out);
    });
    Some(VaultHandle { rx, url })
}

/// Run a cloud future, printing a one-line "waking vault…" hint to stderr if it
/// takes longer than the grace window. Used by the deliberately-blocking
/// `fed secrets` commands (D5): the user asked the cloud a question, so
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

pub async fn put_secret(
    creds: &Credentials,
    link: &CloudLink,
    env: &str,
    name: &str,
    value: &str,
) -> Result<()> {
    let res = client()
        .put(format!(
            "{}/api/v1/orgs/{}/projects/{}/secrets/{}",
            creds.url, link.org, link.project, name
        ))
        .bearer_auth(&creds.token)
        .json(&serde_json::json!({ "value": value, "env": env }))
        .send()
        .await
        .map_err(|e| Error::Validation(format!("cloud: cannot reach {}: {}", creds.url, e)))?;
    if !res.status().is_success() {
        return Err(api_error(
            res.status(),
            format!("setting {}", name).as_str(),
        ));
    }
    Ok(())
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
    let client = match reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
    {
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

    #[test]
    fn vault_knob_defaults_are_sane() {
        assert_eq!(vault_grace(), Duration::from_secs(2));
        assert_eq!(vault_timeout(), Duration::from_secs(60));
        assert_eq!(vault_max_age(), Duration::from_secs(24 * 60 * 60));
    }

    #[test]
    fn env_duration_parses_duration_strings_and_falls_back() {
        // Unique var name so we don't race other tests over the process env.
        let var = "FED_VAULT_TEST_KNOB_XYZ";
        std::env::set_var(var, "500ms");
        assert_eq!(
            env_duration(var, Duration::from_secs(9)),
            Duration::from_millis(500)
        );
        std::env::set_var(var, "not-a-duration");
        assert_eq!(
            env_duration(var, Duration::from_secs(9)),
            Duration::from_secs(9),
            "garbage falls back to the default"
        );
        std::env::remove_var(var);
        assert_eq!(
            env_duration(var, Duration::from_secs(9)),
            Duration::from_secs(9)
        );
    }
}
