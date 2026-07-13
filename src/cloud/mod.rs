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

// ── API client ────────────────────────────────────────────────────────

fn client() -> &'static reqwest::Client {
    static CLIENT: std::sync::OnceLock<reqwest::Client> = std::sync::OnceLock::new();
    CLIENT.get_or_init(|| {
        reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(15))
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

pub async fn fetch_values(
    creds: &Credentials,
    link: &CloudLink,
    env: &str,
    names: &[String],
) -> Result<SecretValues> {
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
        .map_err(|e| Error::Validation(format!("cloud: cannot reach {}: {}", creds.url, e)))?;
    if !res.status().is_success() {
        return Err(api_error(res.status(), "fetching secret values"));
    }
    res.json()
        .await
        .map_err(|e| Error::Validation(format!("cloud: bad values response: {}", e)))
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

/// Synchronous vault lookup for the parameter resolver (which is sync).
/// Runs the async fetch on a throwaway single-thread runtime in its own
/// OS thread, so it's safe to call from inside a tokio runtime.
///
/// Returns `None` when not logged in or the checkout isn't linked — the
/// caller falls back to existing behavior. Network errors surface as `Err`
/// so the caller can turn them into a warning (cache may still cover it).
pub fn fetch_values_blocking(
    work_dir: &Path,
    env: &str,
    names: &[String],
) -> Option<Result<HashMap<String, String>>> {
    let creds = load_credentials()?;
    let link = load_link(work_dir)?;
    let env = env.to_string();
    let names = names.to_vec();
    let handle = std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| Error::Validation(format!("cloud: runtime: {}", e)))?;
        rt.block_on(async { fetch_values(&creds, &link, &env, &names).await })
            .map(|v| v.values)
    });
    Some(handle.join().unwrap_or_else(|_| {
        Err(Error::Validation(
            "cloud: vault lookup thread panicked".into(),
        ))
    }))
}
