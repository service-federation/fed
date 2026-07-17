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
    if let Ok(token) = std::env::var("FED_TOKEN") {
        if !token.is_empty() {
            return Some(Credentials {
                url: std::env::var("FED_CLOUD_URL").unwrap_or_else(|_| DEFAULT_URL.to_string()),
                token,
            });
        }
    }
    let path = credentials_path()?;
    let raw = std::fs::read_to_string(path).ok()?;
    serde_yaml::from_str(&raw).ok()
}

pub fn save_credentials(creds: &Credentials) -> Result<()> {
    let path = credentials_path()
        .ok_or_else(|| Error::Validation("cannot determine home directory".into()))?;
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .map_err(|e| Error::Filesystem(format!("creating {}: {}", parent.display(), e)))?;
    }
    let yaml = serde_yaml::to_string(creds)
        .map_err(|e| Error::Validation(format!("serializing credentials: {}", e)))?;
    std::fs::write(&path, yaml)
        .map_err(|e| Error::Filesystem(format!("writing {}: {}", path.display(), e)))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600))
            .map_err(|e| Error::Filesystem(format!("chmod {}: {}", path.display(), e)))?;
    }
    Ok(())
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

#[cfg(test)]
mod tests {
    use super::*;

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
