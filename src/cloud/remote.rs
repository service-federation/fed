//! Remote environments: the cloud API that creates, lists and deletes them,
//! and the env tokens a remote environment uses to read the team vault.
//!
//! Every request goes through `client_builder()`, so it carries the version
//! headers, and every refusal ends in `api_error` or a message built from the
//! status and the server's error code. Tokens and key material travel only in
//! JSON bodies and never reach an error message.

use super::{Credentials, api_error, api_url, bad_response, client_builder, unreachable};
use crate::error::{Error, Result};
use serde::{Deserialize, Serialize};
use std::time::Duration;

/// The default server type when `fed remote up` does not receive `--type`.
pub const DEFAULT_TYPE: &str = "DEV1-S";

/// Shortest and longest env token lifetimes the cloud accepts.
pub const MIN_TOKEN_TTL: i64 = 60;
pub const MAX_TOKEN_TTL: i64 = 21_600;

/// The project a remote environment belongs to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectRef {
    pub org: String,
    pub project: String,
}

impl std::fmt::Display for ProjectRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{}", self.org, self.project)
    }
}

/// One remote environment as the cloud describes it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Environment {
    pub id: String,
    pub name: String,
    #[serde(rename = "type")]
    pub kind: String,
    pub state: String,
    pub ip: Option<String>,
    pub port: Option<u16>,
    pub created_at: String,
    pub deadline: String,
    /// Who created it. An org admin's list holds everyone's environments in
    /// the project. `None` when the server leaves it out or the user is gone.
    #[serde(default)]
    pub owner: Option<Owner>,
}

/// The user who created an environment.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Owner {
    pub id: String,
    #[serde(default)]
    pub name: Option<String>,
}

impl Environment {
    /// Whether the user with id `me` created this environment. Without an
    /// owner or an id to compare, it counts as the caller's: servers that do
    /// not send owners list only the caller's environments.
    pub fn is_owned_by(&self, me: Option<&str>) -> bool {
        match (&self.owner, me) {
            (Some(owner), Some(me)) => owner.id == me,
            _ => true,
        }
    }
}

/// How long `fed remote up` waits for the create request. The cloud answers
/// once the provider has powered the machine on, which can take minutes.
pub const CREATE_TIMEOUT: Duration = Duration::from_secs(300);

/// Environment names the cloud accepts: `^[a-z0-9][a-z0-9-]{0,30}$`.
pub fn valid_environment_name(name: &str) -> bool {
    let bytes = name.as_bytes();
    matches!(bytes.first(), Some(c) if c.is_ascii_lowercase() || c.is_ascii_digit())
        && bytes.len() <= 31
        && bytes
            .iter()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || *c == b'-')
}

pub const INVALID_ENVIRONMENT_NAME: &str = "invalid environment name. Use 1 to 31 lowercase letters, digits and -, starting with a letter or digit.";

/// What `fed remote up` sends to create an environment. The host private key
/// is in here, so `Debug` leaves the keys out.
#[derive(Serialize)]
pub struct CreateEnvironment<'a> {
    pub name: &'a str,
    #[serde(rename = "type", skip_serializing_if = "Option::is_none")]
    pub kind: Option<&'a str>,
    pub client_public_key: &'a str,
    pub host_private_key: &'a str,
    pub host_public_key: &'a str,
}

impl std::fmt::Debug for CreateEnvironment<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CreateEnvironment")
            .field("name", &self.name)
            .field("type", &self.kind)
            .finish_non_exhaustive()
    }
}

/// An env token: read access to one project's vault, for one workspace on a
/// remote environment. `Debug` leaves the token out.
#[derive(Clone, Deserialize)]
pub struct EnvToken {
    pub id: String,
    pub token: String,
    pub expires_at: String,
}

impl std::fmt::Debug for EnvToken {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EnvToken")
            .field("id", &self.id)
            .field("expires_at", &self.expires_at)
            .finish_non_exhaustive()
    }
}

fn client() -> &'static reqwest::Client {
    static CLIENT: std::sync::OnceLock<reqwest::Client> = std::sync::OnceLock::new();
    // Creating an environment waits for the cloud provider, so its request
    // sets a longer timeout of its own.
    CLIENT.get_or_init(|| {
        client_builder()
            .connect_timeout(Duration::from_secs(15))
            .timeout(Duration::from_secs(60))
            .build()
            .expect("building HTTP client")
    })
}

fn environments_path(project: &ProjectRef) -> String {
    format!(
        "/api/v1/orgs/{}/projects/{}/environments",
        project.org, project.project
    )
}

fn env_tokens_path(project: &ProjectRef) -> String {
    format!(
        "/api/v1/orgs/{}/projects/{}/env-tokens",
        project.org, project.project
    )
}

/// Ids come back from the server and go into URL and file paths, so they may
/// only hold characters that cannot change a path.
pub fn valid_id(id: &str) -> bool {
    !id.is_empty()
        && id.len() <= 64
        && id
            .bytes()
            .all(|c| c.is_ascii_alphanumeric() || c == b'-' || c == b'_')
}

fn checked_id(id: &str) -> Result<&str> {
    if valid_id(id) {
        Ok(id)
    } else {
        Err(Error::Cloud(
            "Service Federation Cloud sent an environment or token id that fed cannot use".into(),
        ))
    }
}

#[derive(Deserialize, Default)]
struct ErrorBody {
    #[serde(default)]
    error: Option<String>,
    #[serde(default)]
    limit: Option<String>,
}

async fn send(req: reqwest::RequestBuilder, creds: &Credentials) -> Result<reqwest::Response> {
    req.bearer_auth(&creds.token)
        .send()
        .await
        .map_err(|e| unreachable(&creds.url, &e))
}

async fn refusal(res: reqwest::Response) -> (reqwest::StatusCode, ErrorBody) {
    let status = res.status();
    let body = res.json::<ErrorBody>().await.unwrap_or_default();
    (status, body)
}

/// Which environment request a refusal belongs to.
#[derive(Debug, Clone, Copy)]
pub enum EnvironmentRequest<'a> {
    Create {
        name: &'a str,
        kind: Option<&'a str>,
    },
    List,
    Delete,
    MintToken,
    RevokeToken,
}

/// The message for a refused environment or env token request. Built from the
/// status and the server's `{"error", "limit"}` only. Known error codes are
/// matched first, because the server may move one to another status.
pub fn environment_error(
    status: reqwest::StatusCode,
    code: Option<&str>,
    limit: Option<&str>,
    request: EnvironmentRequest<'_>,
    project: &ProjectRef,
) -> Error {
    let org = &project.org;
    let created = match request {
        EnvironmentRequest::Create { name, .. } => Some(name),
        _ => None,
    };
    let limit = match code {
        Some("limit") => Some(limit.unwrap_or("")),
        _ => None,
    };
    let message = match (status.as_u16(), code, limit) {
        (401, _, _) => "your login is missing, expired or revoked. Run `fed login`.".to_string(),
        (_, Some("compute_not_enabled"), _) => format!(
            "Remote environments are not enabled for {org}. Ask a Service Federation admin to turn them on."
        ),
        (_, Some("token_scope"), _) => "this login cannot manage remote environments. Run `fed login` on this machine to get one that can.".to_string(),
        (_, Some("cleanup_unavailable"), _) => "Service Federation Cloud has paused new remote environments for a moment. Try again in a few minutes.".to_string(),
        (_, Some("capacity"), _) => "Service Federation Cloud has no room for new remote environments right now. Try again in a few minutes.".to_string(),
        (_, Some("compute_unavailable"), _) => "Service Federation Cloud cannot create remote environments right now. Try again later, or ask a Service Federation admin.".to_string(),
        (_, _, Some("user_environments")) => "you already have as many remote environments as one person may have. Delete one with `fed remote down NAME`, or wait until one deletes itself.".to_string(),
        (_, _, Some("max_environments")) => format!(
            "{org} already has as many remote environments as it may have. Delete one with `fed remote down NAME`, or wait until one deletes itself."
        ),
        (_, _, Some("allowed_types")) => {
            let kind = match request {
                EnvironmentRequest::Create { kind, .. } => kind.unwrap_or(DEFAULT_TYPE),
                _ => "this type of",
            };
            format!("{org} may not create {kind} environments. Pick another type with `--type`.")
        }
        (_, _, Some("monthly_hours")) => format!(
            "{org} has used all its remote environment hours for this month."
        ),
        (_, _, Some(_)) => format!("{org} has reached a limit for remote environments."),
        (409, Some("name_taken"), _) => match created {
            Some(name) => format!(
                "{project} already has an environment called {name}. Pick another name, or delete it with `fed remote down {name}`."
            ),
            None => format!("that name is already taken in {project}."),
        },
        (409, Some("busy"), _) => {
            "the environment is still being set up. Try again in a few seconds.".to_string()
        }
        (410, _, _) => match created {
            Some(name) => format!(
                "{name} was deleted while it was being created. Create it again with `fed remote up {name}`."
            ),
            None => "the environment was deleted.".to_string(),
        },
        (404, Some("environment"), _) => {
            format!("{project} has no such environment. See `fed remote ls`.")
        }
        (404, _, _) => format!(
            "project {project} does not exist, or you are not a member of it. Check `fed link`."
        ),
        (400, Some("type"), _) => {
            let kind = match request {
                EnvironmentRequest::Create { kind, .. } => kind.unwrap_or(DEFAULT_TYPE),
                _ => "this",
            };
            format!("Service Federation Cloud does not know the server type {kind}. Pick another type with `--type`.")
        }
        (429, _, _) => "too many requests. Try again in a minute.".to_string(),
        (502, _, _) => match request {
            EnvironmentRequest::Delete => "the cloud provider did not delete the environment. Try again in a minute.".to_string(),
            _ => "the cloud provider failed to create the environment. Try again in a minute.".to_string(),
        },
        _ => {
            let context = match request {
                EnvironmentRequest::Create { .. } => "creating the environment",
                EnvironmentRequest::List => "listing environments",
                EnvironmentRequest::Delete => "deleting the environment",
                EnvironmentRequest::MintToken => "creating a vault token",
                EnvironmentRequest::RevokeToken => "revoking a vault token",
            };
            return api_error(status, context);
        }
    };
    Error::Cloud(message)
}

async fn into_error(
    res: reqwest::Response,
    request: EnvironmentRequest<'_>,
    project: &ProjectRef,
) -> Error {
    let (status, body) = refusal(res).await;
    environment_error(
        status,
        body.error.as_deref(),
        body.limit.as_deref(),
        request,
        project,
    )
}

/// `POST /api/v1/orgs/{org}/projects/{project}/environments`.
pub async fn create_environment(
    creds: &Credentials,
    project: &ProjectRef,
    body: &CreateEnvironment<'_>,
) -> Result<Environment> {
    if !valid_environment_name(body.name) {
        return Err(Error::Validation(INVALID_ENVIRONMENT_NAME.into()));
    }
    let url = api_url(&creds.url, &environments_path(project))?;
    let res = send(client().post(url).timeout(CREATE_TIMEOUT).json(body), creds).await?;
    if !res.status().is_success() {
        let request = EnvironmentRequest::Create {
            name: body.name,
            kind: body.kind,
        };
        return Err(into_error(res, request, project).await);
    }
    res.json()
        .await
        .map_err(|e| bad_response("new environment", e))
}

#[derive(Deserialize)]
struct EnvironmentList {
    environments: Vec<Environment>,
}

/// `GET /api/v1/orgs/{org}/projects/{project}/environments`: the caller's
/// active environments in the project.
pub async fn list_environments(
    creds: &Credentials,
    project: &ProjectRef,
) -> Result<Vec<Environment>> {
    let url = api_url(&creds.url, &environments_path(project))?;
    let res = send(client().get(url), creds).await?;
    if !res.status().is_success() {
        return Err(into_error(res, EnvironmentRequest::List, project).await);
    }
    let body: EnvironmentList = res
        .json()
        .await
        .map_err(|e| bad_response("list of environments", e))?;
    Ok(body.environments)
}

#[derive(Deserialize)]
struct Deleted {
    deleted: bool,
}

/// `DELETE /api/v1/orgs/{org}/projects/{project}/environments/{id}`. Returns
/// false when the environment was already gone, or the cloud does not know
/// it (404 `environment`).
pub async fn delete_environment(
    creds: &Credentials,
    project: &ProjectRef,
    id: &str,
) -> Result<bool> {
    let path = format!("{}/{}", environments_path(project), checked_id(id)?);
    let res = send(client().delete(api_url(&creds.url, &path)?), creds).await?;
    if !res.status().is_success() {
        let (status, body) = refusal(res).await;
        if status.as_u16() == 404 && body.error.as_deref() == Some("environment") {
            return Ok(false);
        }
        return Err(environment_error(
            status,
            body.error.as_deref(),
            body.limit.as_deref(),
            EnvironmentRequest::Delete,
            project,
        ));
    }
    let body: Deleted = res
        .json()
        .await
        .map_err(|e| bad_response("delete confirmation", e))?;
    Ok(body.deleted)
}

/// `POST /api/v1/orgs/{org}/projects/{project}/env-tokens`. With an
/// `environment_id`, the cloud revokes the token when that environment ends.
pub async fn mint_env_token(
    creds: &Credentials,
    project: &ProjectRef,
    label: &str,
    ttl_seconds: i64,
    environment_id: Option<&str>,
) -> Result<EnvToken> {
    if !(MIN_TOKEN_TTL..=MAX_TOKEN_TTL).contains(&ttl_seconds) {
        return Err(Error::Validation(format!(
            "a vault token must live {MIN_TOKEN_TTL} to {MAX_TOKEN_TTL} seconds, not {ttl_seconds}"
        )));
    }
    let url = api_url(&creds.url, &env_tokens_path(project))?;
    let mut body = serde_json::json!({ "label": label, "ttl_seconds": ttl_seconds });
    if let Some(id) = environment_id {
        body["environment_id"] = checked_id(id)?.into();
    }
    let res = send(client().post(url).json(&body), creds).await?;
    if !res.status().is_success() {
        return Err(into_error(res, EnvironmentRequest::MintToken, project).await);
    }
    res.json()
        .await
        .map_err(|_| bad_response("vault token", "the answer is not the expected JSON"))
}

/// `DELETE /api/v1/orgs/{org}/projects/{project}/env-tokens/{id}`. A token
/// the server no longer knows (404) counts as revoked.
pub async fn revoke_env_token(creds: &Credentials, project: &ProjectRef, id: &str) -> Result<()> {
    let path = format!("{}/{}", env_tokens_path(project), checked_id(id)?);
    let res = send(client().delete(api_url(&creds.url, &path)?), creds).await?;
    if res.status().is_success() || res.status().as_u16() == 404 {
        return Ok(());
    }
    Err(into_error(res, EnvironmentRequest::RevokeToken, project).await)
}

#[cfg(test)]
mod tests {
    use super::*;

    const HOST_PRIVATE: &str = "-----BEGIN OPENSSH PRIVATE KEY-----\nhost-private-must-not-leak\n-----END OPENSSH PRIVATE KEY-----\n";
    const TOKEN: &str = "fedenv_token-must-not-leak";

    /// One-shot server that answers `status_line` with `body` and hands back the
    /// whole request, so a test can check what was sent.
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

    fn creds(url: String) -> Credentials {
        Credentials {
            url,
            token: "fed_test-token".into(),
        }
    }

    fn project() -> ProjectRef {
        ProjectRef {
            org: "acme".into(),
            project: "web".into(),
        }
    }

    fn environment_json() -> String {
        serde_json::json!({
            "id": "0b6f0c1e-1111-2222-3333-444455556666",
            "name": "box",
            "type": "DEV1-S",
            "state": "running",
            "ip": "203.0.113.7",
            "port": 23456,
            "created_at": "2026-10-10T10:00:00Z",
            "deadline": "2026-10-10T16:00:00Z",
        })
        .to_string()
    }

    fn create_body() -> CreateEnvironment<'static> {
        CreateEnvironment {
            name: "box",
            kind: None,
            client_public_key: "ssh-ed25519 AAAAclient fed-remote box client",
            host_private_key: HOST_PRIVATE,
            host_public_key: "ssh-ed25519 AAAAhost fed-remote box host",
        }
    }

    fn split(request: &str) -> (String, serde_json::Value) {
        let (head, body) = request.split_once("\r\n\r\n").unwrap();
        let json = if body.is_empty() {
            serde_json::Value::Null
        } else {
            serde_json::from_str(body).unwrap()
        };
        (head.to_string(), json)
    }

    fn assert_cloud_headers(head: &str) {
        let head = head.to_ascii_lowercase();
        assert!(
            head.contains("authorization: bearer fed_test-token\r\n"),
            "{head}"
        );
        assert!(head.contains("x-fed-api-version: 2\r\n"), "{head}");
        assert!(head.contains("x-fed-version: "), "{head}");
    }

    #[tokio::test]
    async fn create_sends_the_keys_only_in_the_json_body() {
        let (url, rx) = spawn_answering("201 Created", environment_json());
        let env = create_environment(&creds(url), &project(), &create_body())
            .await
            .unwrap();
        assert_eq!(env.name, "box");
        assert_eq!(env.port, Some(23456));
        let request = rx.recv_timeout(Duration::from_secs(5)).unwrap();
        let (head, json) = split(&request);
        assert!(
            head.starts_with("POST /api/v1/orgs/acme/projects/web/environments HTTP/1.1\r\n"),
            "{head}"
        );
        assert_cloud_headers(&head);
        assert!(!head.contains("host-private-must-not-leak"));
        assert_eq!(
            json,
            serde_json::json!({
                "name": "box",
                "client_public_key": "ssh-ed25519 AAAAclient fed-remote box client",
                "host_private_key": HOST_PRIVATE,
                "host_public_key": "ssh-ed25519 AAAAhost fed-remote box host",
            })
        );
    }

    #[tokio::test]
    async fn create_sends_the_type_when_given() {
        let (url, rx) = spawn_answering("201 Created", environment_json());
        let body = CreateEnvironment {
            kind: Some("DEV1-M"),
            ..create_body()
        };
        create_environment(&creds(url), &project(), &body)
            .await
            .unwrap();
        let (_, json) = split(&rx.recv_timeout(Duration::from_secs(5)).unwrap());
        assert_eq!(json["type"], "DEV1-M");
    }

    #[test]
    fn create_body_debug_leaves_the_keys_out() {
        let debug = format!("{:?}", create_body());
        assert!(!debug.contains("host-private-must-not-leak"), "{debug}");
        assert!(!debug.contains("AAAA"), "{debug}");
    }

    #[tokio::test]
    async fn create_refuses_a_bad_name_before_sending() {
        let local = creds("http://127.0.0.1:9".into());
        for name in ["", "Box", "-box", "a b", "../x", &"a".repeat(32)] {
            let body = CreateEnvironment {
                name,
                ..create_body()
            };
            let err = create_environment(&local, &project(), &body)
                .await
                .unwrap_err()
                .to_string();
            assert!(err.contains("invalid environment name"), "{name}: {err}");
        }
        assert!(valid_environment_name("box-1"));
        assert!(valid_environment_name(&"a".repeat(31)));
    }

    /// What the user reads after `Error: `, for every refusal the cloud has.
    #[tokio::test]
    async fn create_maps_refusals_to_clear_messages() {
        let cases: &[(&str, &str, &str)] = &[
            (
                "401 Unauthorized",
                r#"{"error":"unauthenticated"}"#,
                "your login is missing, expired or revoked. Run `fed login`.",
            ),
            (
                "403 Forbidden",
                r#"{"error":"compute_not_enabled"}"#,
                "Remote environments are not enabled for acme. Ask a Service Federation admin to turn them on.",
            ),
            (
                "403 Forbidden",
                r#"{"error":"token_scope"}"#,
                "this login cannot manage remote environments. Run `fed login` on this machine to get one that can.",
            ),
            (
                "404 Not Found",
                r#"{"error":"project"}"#,
                "project acme/web does not exist, or you are not a member of it. Check `fed link`.",
            ),
            (
                "404 Not Found",
                r#"{"error":"org"}"#,
                "project acme/web does not exist, or you are not a member of it. Check `fed link`.",
            ),
            (
                "409 Conflict",
                r#"{"error":"name_taken"}"#,
                "acme/web already has an environment called box. Pick another name, or delete it with `fed remote down box`.",
            ),
            (
                "410 Gone",
                r#"{"error":"deleted"}"#,
                "box was deleted while it was being created. Create it again with `fed remote up box`.",
            ),
            (
                "422 Unprocessable Entity",
                r#"{"error":"limit","limit":"max_environments"}"#,
                "acme already has as many remote environments as it may have. Delete one with `fed remote down NAME`, or wait until one deletes itself.",
            ),
            (
                "422 Unprocessable Entity",
                r#"{"error":"limit","limit":"user_environments"}"#,
                "you already have as many remote environments as one person may have. Delete one with `fed remote down NAME`, or wait until one deletes itself.",
            ),
            (
                "422 Unprocessable Entity",
                r#"{"error":"limit","limit":"allowed_types"}"#,
                "acme may not create DEV1-S environments. Pick another type with `--type`.",
            ),
            (
                "422 Unprocessable Entity",
                r#"{"error":"limit","limit":"monthly_hours"}"#,
                "acme has used all its remote environment hours for this month.",
            ),
            (
                "422 Unprocessable Entity",
                r#"{"error":"limit","limit":"something_new"}"#,
                "acme has reached a limit for remote environments.",
            ),
            (
                "400 Bad Request",
                r#"{"error":"type"}"#,
                "Service Federation Cloud does not know the server type DEV1-S. Pick another type with `--type`.",
            ),
            (
                "429 Too Many Requests",
                r#"{"error":"rate_limited"}"#,
                "too many requests. Try again in a minute.",
            ),
            (
                "502 Bad Gateway",
                r#"{"error":"provider"}"#,
                "the cloud provider failed to create the environment. Try again in a minute.",
            ),
            (
                "503 Service Unavailable",
                r#"{"error":"capacity"}"#,
                "Service Federation Cloud has no room for new remote environments right now. Try again in a few minutes.",
            ),
            (
                "503 Service Unavailable",
                r#"{"error":"cleanup_unavailable"}"#,
                "Service Federation Cloud has paused new remote environments for a moment. Try again in a few minutes.",
            ),
            (
                "503 Service Unavailable",
                r#"{"error":"compute_unavailable"}"#,
                "Service Federation Cloud cannot create remote environments right now. Try again later, or ask a Service Federation admin.",
            ),
            (
                "500 Internal Server Error",
                r#"{"error":"internal"}"#,
                "creating the environment failed: Service Federation Cloud answered 500 Internal Server Error.",
            ),
            (
                "426 Upgrade Required",
                r#"{"error":"unsupported_api_version"}"#,
                "creating the environment failed: Service Federation Cloud answered 426 Upgrade Required. This version of fed is too old for the server. Upgrade fed (`brew upgrade fed`) and try again.",
            ),
        ];
        for (status, body, expected) in cases {
            let (url, _rx) = spawn_answering(status, body.to_string());
            let err = create_environment(&creds(url), &project(), &create_body())
                .await
                .unwrap_err();
            assert!(matches!(err, Error::Cloud(_)), "{status} {body}: {err:?}");
            assert_eq!(err.to_string(), *expected, "{status} {body}");
        }
    }

    #[tokio::test]
    async fn an_unreachable_cloud_is_a_plain_sentence() {
        let err = list_environments(&creds("http://127.0.0.1:9".into()), &project())
            .await
            .unwrap_err()
            .to_string();
        assert!(
            err.starts_with("cannot reach Service Federation Cloud at http://127.0.0.1:9: "),
            "{err}"
        );
    }

    #[tokio::test]
    async fn an_unreadable_answer_is_a_plain_sentence() {
        let (url, _rx) = spawn_answering("200 OK", "not json".into());
        let err = list_environments(&creds(url), &project())
            .await
            .unwrap_err()
            .to_string();
        assert!(
            err.starts_with(
                "Service Federation Cloud sent a list of environments that fed cannot read: "
            ),
            "{err}"
        );
    }

    #[tokio::test]
    async fn delete_maps_its_own_refusals() {
        let (url, _rx) = spawn_answering("404 Not Found", r#"{"error":"environment"}"#.into());
        assert!(
            !delete_environment(&creds(url), &project(), "env-1")
                .await
                .unwrap()
        );
        let (url, _rx) = spawn_answering("409 Conflict", r#"{"error":"busy"}"#.into());
        let err = delete_environment(&creds(url), &project(), "env-1")
            .await
            .unwrap_err()
            .to_string();
        assert_eq!(
            err,
            "the environment is still being set up. Try again in a few seconds."
        );
        let (url, _rx) = spawn_answering("502 Bad Gateway", r#"{"error":"provider"}"#.into());
        let err = delete_environment(&creds(url), &project(), "env-1")
            .await
            .unwrap_err()
            .to_string();
        assert_eq!(
            err,
            "the cloud provider did not delete the environment. Try again in a minute."
        );
    }

    #[test]
    fn owners_decide_whose_environment_it_is() {
        let mut env: Environment = serde_json::from_str(&environment_json()).unwrap();
        assert_eq!(env.owner, None);
        assert!(env.is_owned_by(Some("u-1")));
        env = serde_json::from_value(serde_json::json!({
            "id": "e", "name": "box", "type": "DEV1-S", "state": "ready",
            "ip": null, "port": null, "created_at": "x", "deadline": "y",
            "project": "web", "owner": {"id": "u-2", "name": "Bob"},
        }))
        .unwrap();
        assert!(!env.is_owned_by(Some("u-1")));
        assert!(env.is_owned_by(Some("u-2")));
        assert!(env.is_owned_by(None));
    }

    #[tokio::test]
    async fn list_reads_the_environments() {
        let body = format!("{{\"environments\":[{}]}}", environment_json());
        let (url, rx) = spawn_answering("200 OK", body);
        let list = list_environments(&creds(url), &project()).await.unwrap();
        assert_eq!(list.len(), 1);
        assert_eq!(list[0].kind, "DEV1-S");
        let (head, _) = split(&rx.recv_timeout(Duration::from_secs(5)).unwrap());
        assert!(
            head.starts_with("GET /api/v1/orgs/acme/projects/web/environments HTTP/1.1\r\n"),
            "{head}"
        );
        assert_cloud_headers(&head);
    }

    #[tokio::test]
    async fn delete_sends_a_delete_for_the_id() {
        let (url, rx) = spawn_answering("200 OK", r#"{"deleted":false}"#.into());
        let deleted = delete_environment(&creds(url), &project(), "env-1")
            .await
            .unwrap();
        assert!(!deleted);
        let (head, _) = split(&rx.recv_timeout(Duration::from_secs(5)).unwrap());
        assert!(
            head.starts_with(
                "DELETE /api/v1/orgs/acme/projects/web/environments/env-1 HTTP/1.1\r\n"
            ),
            "{head}"
        );
        assert_cloud_headers(&head);
    }

    #[tokio::test]
    async fn ids_that_could_change_the_path_are_refused() {
        let local = creds("http://127.0.0.1:9".into());
        for id in ["", "../x", "a/b", "a?b", "a b"] {
            assert!(
                delete_environment(&local, &project(), id).await.is_err(),
                "{id}"
            );
            assert!(
                revoke_env_token(&local, &project(), id).await.is_err(),
                "{id}"
            );
        }
    }

    #[tokio::test]
    async fn mint_sends_label_and_ttl_and_keeps_the_token_out_of_debug() {
        let body = format!(
            "{{\"id\":\"tok-1\",\"token\":\"{TOKEN}\",\"expires_at\":\"2026-10-10T16:00:00Z\"}}"
        );
        let (url, rx) = spawn_answering("201 Created", body);
        let token = mint_env_token(
            &creds(url),
            &project(),
            "fed remote box/alice",
            3600,
            Some("env-1"),
        )
        .await
        .unwrap();
        assert_eq!(token.token, TOKEN);
        assert!(!format!("{token:?}").contains(TOKEN));
        let (head, json) = split(&rx.recv_timeout(Duration::from_secs(5)).unwrap());
        assert!(
            head.starts_with("POST /api/v1/orgs/acme/projects/web/env-tokens HTTP/1.1\r\n"),
            "{head}"
        );
        assert_cloud_headers(&head);
        assert_eq!(
            json,
            serde_json::json!({
                "label": "fed remote box/alice",
                "ttl_seconds": 3600,
                "environment_id": "env-1",
            })
        );
    }

    #[tokio::test]
    async fn mint_refuses_a_ttl_outside_the_cloud_range() {
        let local = creds("http://127.0.0.1:9".into());
        for ttl in [59, 21_601, -1] {
            assert!(
                mint_env_token(&local, &project(), "x", ttl, None)
                    .await
                    .is_err(),
                "{ttl}"
            );
        }
    }

    #[tokio::test]
    async fn revoke_treats_an_unknown_token_as_revoked() {
        let (url, rx) = spawn_answering("404 Not Found", r#"{"error":"token"}"#.into());
        revoke_env_token(&creds(url), &project(), "tok-1")
            .await
            .unwrap();
        let (head, _) = split(&rx.recv_timeout(Duration::from_secs(5)).unwrap());
        assert!(
            head.starts_with("DELETE /api/v1/orgs/acme/projects/web/env-tokens/tok-1 HTTP/1.1\r\n"),
            "{head}"
        );
        let (url, _rx) = spawn_answering("500 Internal Server Error", String::new());
        let err = revoke_env_token(&creds(url), &project(), "tok-1")
            .await
            .unwrap_err()
            .to_string();
        assert!(err.contains("revoking a vault token"), "{err}");
    }
}
