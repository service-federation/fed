//! Team credentials for pulling images from private registries.
//!
//! A `registry_auth:` entry in fed.yaml maps a registry host to a username and
//! a `{{PARAM}}` reference to a secret parameter. When that secret has a value,
//! fed pulls with a throwaway Docker config directory that holds a login for
//! that one registry. When it has none, fed pulls with whatever credentials the
//! environment already has, exactly as before.
//!
//! Safety properties of the throwaway directory ([`TempAuthDir`]):
//!
//! - It is created with mode 0700 and its auth file with mode 0600.
//! - It is deleted when the guard drops, which covers success, error, timeout
//!   and cancellation of the pull future.
//! - The password reaches the runtime only through that file. It is never in
//!   argv, never in an environment variable and never in a log line.
//! - The user's own `~/.docker` is never written. For Docker, the throwaway
//!   config copies `currentContext` and links the user's `contexts` directory,
//!   so the pull talks to the same daemon.

use crate::config::Config;
use crate::parameter::get_template_regex;
use base64::Engine;
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::io::Write;
use std::os::unix::fs::{DirBuilderExt, OpenOptionsExt};
use std::path::{Path, PathBuf};
use std::sync::Mutex;

/// The registry an image ref without a host comes from.
pub const DOCKER_HUB: &str = "docker.io";

/// The key Docker's own `docker login` writes for Docker Hub in `auths`.
const DOCKER_HUB_AUTHS_KEY: &str = "https://index.docker.io/v1/";

/// The registry host of an image ref, following Docker's rule: the first path
/// component is a host if it contains '.' or ':' or is `localhost`. Everything
/// else (`postgres`, `library/redis`, `acme/api`) is on Docker Hub.
pub fn registry_host(image: &str) -> &str {
    match image.split_once('/') {
        Some((first, _)) if looks_like_registry_host(first) => first,
        _ => DOCKER_HUB,
    }
}

/// Whether a string would be read as a registry host by [`registry_host`].
/// Used both to parse image refs and to validate `registry_auth` keys.
pub fn looks_like_registry_host(s: &str) -> bool {
    !s.is_empty()
        && !s.contains('/')
        && !s.chars().any(char::is_whitespace)
        && (s.contains('.') || s.contains(':') || s == "localhost")
}

/// The parameter a `registry_auth` password refers to, if the password is
/// exactly one `{{PARAM}}` placeholder.
pub fn password_parameter(password: &str) -> Option<&str> {
    let trimmed = password.trim();
    let cap = get_template_regex().captures(trimmed)?;
    let whole = cap.get(0)?;
    if whole.start() != 0 || whole.end() != trimmed.len() {
        return None;
    }
    let name = cap.get(1)?.as_str().trim();
    (!name.is_empty()).then_some(name)
}

/// A resolved team login for one registry.
///
/// `Debug` prints the password as `********`, and no `Display` exists, so the
/// value cannot reach a log line or an error string by formatting this type.
#[derive(Clone)]
pub struct RegistryCredential {
    registry: String,
    username: String,
    password: String,
    parameter: String,
}

impl fmt::Debug for RegistryCredential {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RegistryCredential")
            .field("registry", &self.registry)
            .field("username", &self.username)
            .field("password", &crate::parameter::sensitivity::REDACTED_DISPLAY)
            .field("parameter", &self.parameter)
            .finish()
    }
}

impl RegistryCredential {
    pub fn new(
        registry: impl Into<String>,
        username: impl Into<String>,
        password: impl Into<String>,
        parameter: impl Into<String>,
    ) -> Self {
        Self {
            registry: registry.into(),
            username: username.into(),
            password: password.into(),
            parameter: parameter.into(),
        }
    }

    /// The registry host this login is for.
    pub fn registry(&self) -> &str {
        &self.registry
    }

    /// The secret parameter the password came from, for messages.
    pub fn parameter(&self) -> &str {
        &self.parameter
    }

    /// The base64 `user:password` string Docker and Podman store in `auths`.
    fn auth_field(&self) -> String {
        base64::engine::general_purpose::STANDARD
            .encode(format!("{}:{}", self.username, self.password))
    }
}

/// The team logins for every registry in `registry_auth` whose secret has a
/// value this run.
#[derive(Debug, Clone, Default)]
pub struct RegistryCredentials {
    by_registry: HashMap<String, RegistryCredential>,
}

impl RegistryCredentials {
    /// Resolve `config.registry_auth` against the resolved parameter values.
    ///
    /// An entry whose secret is empty or was not resolved this run is left
    /// out, so pulls from that registry use the environment's credentials.
    /// That is the normal case for a developer without vault access, so it
    /// is logged at debug level only.
    pub fn resolve(config: &Config, parameters: &HashMap<String, String>) -> Self {
        let mut by_registry = HashMap::new();
        for (registry, auth) in &config.registry_auth {
            let Some(parameter) = password_parameter(&auth.password) else {
                // Validation rejects this shape; never treat it as a password.
                tracing::debug!(
                    "registry_auth for '{}': password is not a {{{{PARAM}}}} reference; using the environment's credentials",
                    registry
                );
                continue;
            };
            let password = parameters.get(parameter).cloned().unwrap_or_default();
            if password.is_empty() {
                tracing::debug!(
                    "registry_auth for '{}': '{}' has no value; using the environment's credentials",
                    registry,
                    parameter
                );
                continue;
            }
            let username = match crate::parameter::Resolver::resolve_template_static(
                &auth.username,
                parameters,
            ) {
                Ok(u) => u,
                Err(e) => {
                    tracing::debug!(
                        "registry_auth for '{}': username did not resolve ({}); using the environment's credentials",
                        registry,
                        e
                    );
                    continue;
                }
            };
            by_registry.insert(
                registry.clone(),
                RegistryCredential::new(registry.clone(), username, password, parameter),
            );
        }
        Self { by_registry }
    }

    /// The team login for the registry an image is pulled from, if any.
    pub fn for_image(&self, image: &str) -> Option<&RegistryCredential> {
        self.by_registry.get(registry_host(image))
    }

    pub fn is_empty(&self) -> bool {
        self.by_registry.is_empty()
    }
}

/// Which auth file format the runtime reads.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RuntimeFlavor {
    /// Reads `$DOCKER_CONFIG/config.json`.
    Docker,
    /// Reads the file given to `--authfile`.
    Podman,
}

impl RuntimeFlavor {
    /// The flavor of a runtime binary, by its file name.
    pub fn of_binary(binary: &str) -> Self {
        let name = Path::new(binary)
            .file_name()
            .map(|n| n.to_string_lossy().into_owned())
            .unwrap_or_default();
        if name.contains("podman") {
            RuntimeFlavor::Podman
        } else {
            RuntimeFlavor::Docker
        }
    }
}

/// The user's Docker config directory: `$DOCKER_CONFIG`, else `~/.docker`.
pub fn user_docker_config_dir() -> Option<PathBuf> {
    match std::env::var_os("DOCKER_CONFIG") {
        Some(dir) if !dir.is_empty() => Some(PathBuf::from(dir)),
        _ => dirs::home_dir().map(|h| h.join(".docker")),
    }
}

/// The `currentContext` from the user's Docker config, if set.
fn current_context(user_config_dir: &Path) -> Option<String> {
    let text = std::fs::read_to_string(user_config_dir.join("config.json")).ok()?;
    let json: serde_json::Value = serde_json::from_str(&text).ok()?;
    json.get("currentContext")?
        .as_str()
        .filter(|c| !c.is_empty())
        .map(str::to_string)
}

/// The JSON body of the throwaway auth file.
fn auth_file_json(
    credential: &RegistryCredential,
    flavor: RuntimeFlavor,
    current_context: Option<&str>,
) -> String {
    // Docker files its Docker Hub login under the legacy index URL. Podman
    // uses the plain host.
    let key = if flavor == RuntimeFlavor::Docker && credential.registry == DOCKER_HUB {
        DOCKER_HUB_AUTHS_KEY
    } else {
        credential.registry.as_str()
    };
    let mut root = serde_json::json!({
        "auths": { key: { "auth": credential.auth_field() } }
    });
    if flavor == RuntimeFlavor::Docker
        && let Some(ctx) = current_context
    {
        root["currentContext"] = serde_json::Value::String(ctx.to_string());
    }
    root.to_string()
}

/// A private temporary directory holding a one-registry auth file.
///
/// The directory and everything in it are removed when this guard drops.
#[derive(Debug)]
pub struct TempAuthDir {
    dir: PathBuf,
    flavor: RuntimeFlavor,
}

impl TempAuthDir {
    /// Create the directory under the system temp dir.
    pub fn create(
        credential: &RegistryCredential,
        flavor: RuntimeFlavor,
        user_config_dir: Option<&Path>,
    ) -> std::io::Result<Self> {
        Self::create_in(&std::env::temp_dir(), credential, flavor, user_config_dir)
    }

    /// Create the directory under `parent`. Split out for tests.
    pub fn create_in(
        parent: &Path,
        credential: &RegistryCredential,
        flavor: RuntimeFlavor,
        user_config_dir: Option<&Path>,
    ) -> std::io::Result<Self> {
        let dir = Self::make_private_dir(parent)?;
        // From here on the guard owns the dir, so a failure below removes it.
        let guard = Self { dir, flavor };

        let context = user_config_dir.and_then(current_context);
        let body = auth_file_json(credential, flavor, context.as_deref());
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(guard.auth_file())?;
        file.write_all(body.as_bytes())?;
        file.sync_all()?;

        // Same daemon: Docker resolves `currentContext` through the contexts
        // store next to config.json, so point that at the user's store.
        if flavor == RuntimeFlavor::Docker
            && let Some(user_dir) = user_config_dir
        {
            let contexts = user_dir.join("contexts");
            if contexts.is_dir() {
                std::os::unix::fs::symlink(&contexts, guard.dir.join("contexts"))?;
            }
        }
        Ok(guard)
    }

    fn make_private_dir(parent: &Path) -> std::io::Result<PathBuf> {
        use rand::Rng;
        let mut builder = std::fs::DirBuilder::new();
        builder.mode(0o700);
        for _ in 0..16 {
            let suffix: u64 = rand::thread_rng().r#gen();
            let dir = parent.join(format!(
                "fed-registry-auth-{}-{:016x}",
                std::process::id(),
                suffix
            ));
            match builder.create(&dir) {
                Ok(()) => return Ok(dir),
                Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => continue,
                Err(e) => return Err(e),
            }
        }
        Err(std::io::Error::new(
            std::io::ErrorKind::AlreadyExists,
            "could not pick a unique temp dir name for registry auth",
        ))
    }

    /// The directory path.
    pub fn path(&self) -> &Path {
        &self.dir
    }

    /// The auth file inside the directory.
    pub fn auth_file(&self) -> PathBuf {
        match self.flavor {
            RuntimeFlavor::Docker => self.dir.join("config.json"),
            RuntimeFlavor::Podman => self.dir.join("auth.json"),
        }
    }

    /// Extra arguments for `<runtime> pull`, placed before the image.
    pub fn pull_args(&self) -> Vec<String> {
        match self.flavor {
            RuntimeFlavor::Docker => Vec::new(),
            RuntimeFlavor::Podman => vec![
                "--authfile".to_string(),
                self.auth_file().to_string_lossy().into_owned(),
            ],
        }
    }

    /// Extra environment for the pull command only.
    pub fn pull_env(&self) -> Vec<(String, String)> {
        match self.flavor {
            RuntimeFlavor::Docker => vec![(
                "DOCKER_CONFIG".to_string(),
                self.dir.to_string_lossy().into_owned(),
            )],
            RuntimeFlavor::Podman => Vec::new(),
        }
    }
}

impl Drop for TempAuthDir {
    fn drop(&mut self) {
        // remove_dir_all removes the `contexts` symlink itself, never the
        // user's directory it points at.
        if let Err(e) = std::fs::remove_dir_all(&self.dir) {
            tracing::debug!("Could not remove registry auth dir {:?}: {}", self.dir, e);
        }
    }
}

/// Whether a failed pull's stderr says the registry refused the login.
pub fn stderr_indicates_auth_failure(stderr: &str) -> bool {
    let lower = stderr.to_lowercase();
    lower.contains("unauthorized")
        || lower.contains("denied")
        || lower.contains("authentication required")
        // containerd's wording when the token step fails, whatever status
        // the registry chose for it (Scaleway answers a wrong key with 404)
        || lower.contains("failed to authorize")
}

/// Registries already warned about in this process, so parallel pulls from
/// one registry print one warning.
static WARNED: Mutex<Option<HashSet<String>>> = Mutex::new(None);

/// The warning shown when the team credential is refused.
pub fn auth_failure_warning(credential: &RegistryCredential) -> String {
    format!(
        "The registry {} refused the team credential from parameter '{}'. \
         Pulling with your own Docker credentials instead.",
        credential.registry, credential.parameter
    )
}

/// Print [`auth_failure_warning`] once per registry per process.
pub fn warn_auth_failure_once(credential: &RegistryCredential) {
    let first = {
        let mut warned = WARNED.lock().unwrap_or_else(|e| e.into_inner());
        warned
            .get_or_insert_with(HashSet::new)
            .insert(credential.registry.clone())
    };
    if first {
        tracing::warn!("{}", auth_failure_warning(credential));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{Parameter, RegistryAuth};
    use std::os::unix::fs::PermissionsExt;

    const PASSWORD: &str = "ghp_s3cr3t-value";

    fn credential(registry: &str) -> RegistryCredential {
        RegistryCredential::new(registry, "acme-bot", PASSWORD, "REGISTRY_TOKEN")
    }

    fn decode_auth(json: &str, key: &str) -> String {
        let v: serde_json::Value = serde_json::from_str(json).unwrap();
        let auth = v["auths"][key]["auth"].as_str().unwrap();
        String::from_utf8(
            base64::engine::general_purpose::STANDARD
                .decode(auth)
                .unwrap(),
        )
        .unwrap()
    }

    #[test]
    fn registry_host_follows_dockers_rule() {
        assert_eq!(registry_host("postgres"), "docker.io");
        assert_eq!(registry_host("postgres:16"), "docker.io");
        assert_eq!(registry_host("library/redis:7"), "docker.io");
        assert_eq!(registry_host("acme/api"), "docker.io");
        assert_eq!(registry_host("ghcr.io/acme/api:1.2"), "ghcr.io");
        assert_eq!(
            registry_host("registry.local:5000/api"),
            "registry.local:5000"
        );
        assert_eq!(registry_host("localhost:5000/api"), "localhost:5000");
        assert_eq!(registry_host("localhost/api"), "localhost");
        assert_eq!(registry_host("docker.io/library/postgres"), "docker.io");
        assert_eq!(registry_host("ghcr.io/acme/api@sha256:abcd"), "ghcr.io");
    }

    #[test]
    fn registry_keys_must_look_like_hosts() {
        assert!(looks_like_registry_host("ghcr.io"));
        assert!(looks_like_registry_host("docker.io"));
        assert!(looks_like_registry_host("localhost"));
        assert!(looks_like_registry_host("registry:5000"));
        assert!(!looks_like_registry_host("ghcr"));
        assert!(!looks_like_registry_host(""));
        assert!(!looks_like_registry_host("ghcr.io/acme"));
        assert!(!looks_like_registry_host("https://ghcr.io"));
        assert!(!looks_like_registry_host("ghcr .io"));
    }

    #[test]
    fn password_parameter_requires_a_single_placeholder() {
        assert_eq!(
            password_parameter("{{REGISTRY_TOKEN}}"),
            Some("REGISTRY_TOKEN")
        );
        assert_eq!(
            password_parameter(" {{ REGISTRY_TOKEN }} "),
            Some("REGISTRY_TOKEN")
        );
        assert_eq!(password_parameter("hunter2"), None);
        assert_eq!(password_parameter("x{{REGISTRY_TOKEN}}"), None);
        assert_eq!(password_parameter("{{A}}{{B}}"), None);
    }

    #[test]
    fn docker_config_has_one_registry_and_the_current_context() {
        let json = auth_file_json(
            &credential("ghcr.io"),
            RuntimeFlavor::Docker,
            Some("colima"),
        );
        let v: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(v["auths"].as_object().unwrap().len(), 1);
        assert_eq!(
            decode_auth(&json, "ghcr.io"),
            format!("acme-bot:{PASSWORD}")
        );
        assert_eq!(v["currentContext"], "colima");
        assert!(v.get("credsStore").is_none());
    }

    #[test]
    fn docker_hub_uses_the_legacy_index_key_for_docker_only() {
        let docker = auth_file_json(&credential("docker.io"), RuntimeFlavor::Docker, None);
        assert_eq!(
            decode_auth(&docker, "https://index.docker.io/v1/"),
            format!("acme-bot:{PASSWORD}")
        );
        let podman = auth_file_json(&credential("docker.io"), RuntimeFlavor::Podman, Some("x"));
        assert_eq!(
            decode_auth(&podman, "docker.io"),
            format!("acme-bot:{PASSWORD}")
        );
        let v: serde_json::Value = serde_json::from_str(&podman).unwrap();
        assert!(v.get("currentContext").is_none());
    }

    #[test]
    fn temp_dir_is_private_and_removed_on_drop() {
        let parent = tempfile::tempdir().unwrap();
        let user = tempfile::tempdir().unwrap();
        std::fs::write(
            user.path().join("config.json"),
            r#"{"currentContext":"colima","credsStore":"desktop"}"#,
        )
        .unwrap();
        std::fs::create_dir(user.path().join("contexts")).unwrap();
        std::fs::write(user.path().join("contexts").join("keep"), "x").unwrap();

        let guard = TempAuthDir::create_in(
            parent.path(),
            &credential("ghcr.io"),
            RuntimeFlavor::Docker,
            Some(user.path()),
        )
        .unwrap();
        let dir = guard.path().to_path_buf();
        let mode = |p: &Path| std::fs::metadata(p).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode(&dir), 0o700);
        assert_eq!(mode(&guard.auth_file()), 0o600);
        assert!(
            std::fs::symlink_metadata(dir.join("contexts"))
                .unwrap()
                .file_type()
                .is_symlink()
        );
        let body = std::fs::read_to_string(guard.auth_file()).unwrap();
        assert!(body.contains("\"currentContext\":\"colima\""));
        assert_eq!(
            guard.pull_env(),
            vec![(
                "DOCKER_CONFIG".to_string(),
                dir.to_string_lossy().into_owned()
            )]
        );
        assert!(guard.pull_args().is_empty());

        drop(guard);
        assert!(!dir.exists());
        // The user's contexts store survives the cleanup.
        assert!(user.path().join("contexts").join("keep").exists());
        assert!(std::fs::read_dir(parent.path()).unwrap().next().is_none());
    }

    #[test]
    fn podman_uses_an_authfile_argument_and_no_env() {
        let parent = tempfile::tempdir().unwrap();
        let guard = TempAuthDir::create_in(
            parent.path(),
            &credential("ghcr.io"),
            RuntimeFlavor::Podman,
            None,
        )
        .unwrap();
        assert!(guard.auth_file().ends_with("auth.json"));
        assert_eq!(
            guard.pull_args(),
            vec![
                "--authfile".to_string(),
                guard.auth_file().to_string_lossy().into_owned()
            ]
        );
        assert!(guard.pull_env().is_empty());
    }

    #[test]
    fn runtime_flavor_reads_the_binary_name() {
        assert_eq!(RuntimeFlavor::of_binary("docker"), RuntimeFlavor::Docker);
        assert_eq!(RuntimeFlavor::of_binary("podman"), RuntimeFlavor::Podman);
        assert_eq!(
            RuntimeFlavor::of_binary("/opt/homebrew/bin/podman"),
            RuntimeFlavor::Podman
        );
    }

    fn config_with_auth() -> Config {
        let mut config = Config::default();
        config.parameters.insert(
            "REGISTRY_TOKEN".to_string(),
            Parameter {
                param_type: Some("secret".to_string()),
                source: Some("manual".to_string()),
                optional: Some(true),
                ..Default::default()
            },
        );
        config.registry_auth.insert(
            "ghcr.io".to_string(),
            RegistryAuth {
                username: "acme-bot".to_string(),
                password: "{{REGISTRY_TOKEN}}".to_string(),
            },
        );
        config
    }

    #[test]
    fn empty_password_falls_back_to_the_environment() {
        let config = config_with_auth();
        let params = HashMap::from([("REGISTRY_TOKEN".to_string(), String::new())]);
        let creds = RegistryCredentials::resolve(&config, &params);
        assert!(creds.is_empty());
        assert!(creds.for_image("ghcr.io/acme/api").is_none());

        // Not resolved at all this run behaves the same.
        let creds = RegistryCredentials::resolve(&config, &HashMap::new());
        assert!(creds.is_empty());
    }

    #[test]
    fn supplied_password_resolves_for_matching_images_only() {
        let config = config_with_auth();
        let params = HashMap::from([("REGISTRY_TOKEN".to_string(), PASSWORD.to_string())]);
        let creds = RegistryCredentials::resolve(&config, &params);
        let c = creds.for_image("ghcr.io/acme/api:1").unwrap();
        assert_eq!(c.registry(), "ghcr.io");
        assert_eq!(c.parameter(), "REGISTRY_TOKEN");
        assert!(creds.for_image("postgres:16").is_none());
    }

    #[test]
    fn password_never_appears_in_messages() {
        let c = credential("ghcr.io");
        let creds = {
            let config = config_with_auth();
            let params = HashMap::from([("REGISTRY_TOKEN".to_string(), PASSWORD.to_string())]);
            RegistryCredentials::resolve(&config, &params)
        };
        let parent = tempfile::tempdir().unwrap();
        let guard = TempAuthDir::create_in(parent.path(), &c, RuntimeFlavor::Docker, None).unwrap();
        let strings = [
            format!("{c:?}"),
            format!("{creds:?}"),
            format!("{guard:?}"),
            format!("{:?}", guard.pull_args()),
            format!("{:?}", guard.pull_env()),
            auth_failure_warning(&c),
        ];
        for s in &strings {
            assert!(!s.contains(PASSWORD), "password leaked: {s}");
            assert!(!s.contains(&c.auth_field()), "encoded password leaked: {s}");
        }
        assert!(auth_failure_warning(&c).contains("ghcr.io"));
        assert!(auth_failure_warning(&c).contains("REGISTRY_TOKEN"));
    }

    #[test]
    fn auth_failures_are_recognised() {
        assert!(stderr_indicates_auth_failure(
            "Error response from daemon: Head \"https://ghcr.io/v2/acme/api/manifests/1\": unauthorized"
        ));
        assert!(stderr_indicates_auth_failure(
            "Error response from daemon: pull access denied for acme/api"
        ));
        assert!(stderr_indicates_auth_failure(
            "Error: initializing source: authentication required"
        ));
        // Scaleway refuses a wrong key at the token step, with a 404.
        assert!(stderr_indicates_auth_failure(
            "Error response from daemon: error from registry: failed to resolve reference \"rg.fr-par.scw.cloud/acme/api:latest\": failed to authorize: failed to fetch oauth token: unexpected status from GET request to https://api.scaleway.com/registry-internal/v1/regions/fr-par/tokens?scope=repository%3Aacme%2Fapi%3Apull&service=registry: 404 Not Found"
        ));
        assert!(!stderr_indicates_auth_failure(
            "Error response from daemon: manifest unknown"
        ));
    }
}
