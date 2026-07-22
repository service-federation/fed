use super::PortAllocator;
use crate::config::Config;
use crate::error::{Error, Result};
use crate::port::{PortConflict, PortConflictAction, handle_port_conflict};
use regex::Regex;
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::OnceLock;

/// Global template regex compiled once
static TEMPLATE_REGEX: OnceLock<Regex> = OnceLock::new();

pub(crate) fn get_template_regex() -> &'static Regex {
    TEMPLATE_REGEX
        .get_or_init(|| Regex::new(r"\{\{([^}]+)\}\}").expect("static regex pattern is valid"))
}

/// Outcome of consulting the team vault for a set of queried names, after the
/// grace window / freshness policy has been applied (see 02-cold-vault.md).
enum VaultOutcome {
    /// The vault answered with values (within grace, or after an honest block).
    /// Authoritative — the cache is rewritten with fresh stamps.
    Values(HashMap<String, String>),
    /// Grace expired but the cache covers every queried name freshly. Proceed on
    /// the cache; the abandoned request is left to warm the backend.
    CacheFresh,
    /// The vault could not be reached or used. Fall back to the cache regardless
    /// of age (with a warning); the reason names the cloud in any missing error.
    Failed(String),
    /// Not logged in / checkout not linked — ordinary local mode.
    Local,
}

/// Current unix time in whole seconds (0 if the clock predates the epoch).
fn unix_now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// Whether the cache can cover every queried name *freshly*: each name is
/// cached and its stamp is younger than `max_age`. A missing name or a missing
/// stamp (pre-upgrade entry) counts as not-fresh, forcing an honest refresh.
///
/// A stamp in the future (`stamped > now`) is also not fresh: `saturating_sub`
/// would report age 0 and treat it as fresh until wall time caught up, so a
/// clock skew or a tampered stamp could pin a rotated value as "fresh" for
/// hours. Requiring `stamped <= now` closes that.
pub(crate) fn cache_covers_fresh(
    names: &[String],
    cache_values: &HashMap<String, String>,
    cache_stamps: &HashMap<String, u64>,
    now: u64,
    max_age_secs: u64,
) -> bool {
    names.iter().all(|name| {
        cache_values.contains_key(name)
            && cache_stamps
                .get(name)
                .is_some_and(|stamped| *stamped <= now && now - *stamped < max_age_secs)
    })
}

/// Escape a string for safe use in shell commands.
/// Wraps the string in single quotes and escapes any single quotes within.
pub(crate) fn shell_escape(s: &str) -> String {
    // If string is empty, return empty quoted string
    if s.is_empty() {
        return "''".to_string();
    }

    // If string contains no special characters, return as-is
    // Safe characters: alphanumeric, dash, underscore, dot only
    // Note: '/' and ':' are intentionally NOT in the safe list as they can be
    // exploited in path traversal or certain shell constructs
    if s.chars()
        .all(|c| c.is_alphanumeric() || c == '-' || c == '_' || c == '.')
    {
        return s.to_string();
    }

    // Wrap in single quotes and escape any single quotes by replacing ' with '\''
    format!("'{}'", s.replace('\'', r"'\''"))
}

/// Name of the built-in project-identifier parameter.
///
/// Injected automatically into every service/script templating context; may not
/// be declared as a user parameter (enforced by config validation).
pub const FED_PROJECT_ID: &str = "FED_PROJECT_ID";

/// Restrict a string to the cookie-safe alphabet `[a-z0-9-]`.
///
/// Lowercases, replaces every other character with `-`, and trims leading and
/// trailing dashes. An empty result falls back to `project`.
fn sanitize_project_component(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        if c.is_ascii_alphanumeric() {
            out.push(c.to_ascii_lowercase());
        } else {
            out.push('-');
        }
    }
    let trimmed = out.trim_matches('-');
    if trimmed.is_empty() {
        "project".to_string()
    } else {
        trimmed.to_string()
    }
}

/// Compute the built-in `FED_PROJECT_ID` value for a work dir and optional
/// isolation scope.
///
/// Shape: `<project>-<hash>[-<isolation>]`, lowercased and restricted to
/// `[a-z0-9-]` so it is safe to embed inside a cookie name. `<project>` is the
/// sanitized basename of `work_dir`, `<hash>` is the 8-hex
/// [`crate::service::hash_work_dir`] digest (stable across runs, unique per
/// path), and `<isolation>` is appended when an isolation session is active.
pub fn compute_project_id(work_dir: &Path, isolation_id: Option<&str>) -> String {
    let project = work_dir
        .file_name()
        .map(|n| sanitize_project_component(&n.to_string_lossy()))
        .unwrap_or_else(|| "project".to_string());
    let hash = crate::service::hash_work_dir(work_dir);
    let mut id = format!("{}-{}", project, hash);
    if let Some(iso) = isolation_id {
        let iso = sanitize_project_component(iso);
        if !iso.is_empty() {
            id.push('-');
            id.push_str(&iso);
        }
    }
    id
}

/// Reason a port was resolved to its final value
#[derive(Debug, Clone, PartialEq)]
pub enum PortResolutionReason {
    /// Default port was available and used directly
    DefaultAvailable,
    /// Default port had a conflict, auto-resolved to a different port
    ConflictAutoResolved {
        default_port: u16,
        conflict_pid: Option<u32>,
        conflict_process: Option<String>,
    },
    /// Port was restored from cache
    Cached,
    /// No default available, allocated a random port
    Random,
}

impl std::fmt::Display for PortResolutionReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::DefaultAvailable => write!(f, "default port available"),
            Self::ConflictAutoResolved {
                default_port,
                conflict_pid,
                conflict_process,
            } => {
                write!(f, "default port {} conflicted", default_port)?;
                match (conflict_pid, conflict_process) {
                    (Some(pid), Some(name)) => write!(f, " with '{}' (PID {})", name, pid),
                    (Some(pid), None) => write!(f, " with PID {}", pid),
                    _ => write!(f, " with unknown process"),
                }
            }
            Self::Cached => write!(f, "restored from cache"),
            Self::Random => write!(f, "randomly allocated"),
        }
    }
}

/// Record of how a port parameter was resolved
#[derive(Debug, Clone)]
pub struct PortResolution {
    pub param_name: String,
    pub resolved_port: u16,
    pub reason: PortResolutionReason,
}

/// Resolver handles parameter resolution and template substitution.
///
/// The resolver is responsible for:
/// - Resolving `{{parameter}}` template syntax in configuration values
/// - Allocating ports for `type: port` parameters with TOCTOU prevention
/// - Loading values from `.env` files (strict mode: variables must be declared)
/// - Applying environment-specific values (development/staging/production)
/// - Shell-escaping parameter values in process commands for security
///
/// # Port Allocation
///
/// Port parameters are allocated with the following priority:
/// 1. Explicit `value:` field (validated but not allocated)
/// 2. Preferred port from `default:` (if available and defaults are
///    preferred — normal, non-isolated `fed start`)
/// 3. Persisted port from previous run (if available)
/// 4. Random available port (fallback)
///
/// In isolated mode, persisted ports take priority over `default:` — the
/// whole point of isolation is stable random ports, so a config default
/// must not pull allocations back to well-known ports. Fresh (uncached)
/// parameters skip the default entirely and allocate a random port.
///
/// In force-random mode (`fed start --randomize`, `fed ports randomize`),
/// steps 2 and 3 are skipped and all port parameters allocate random ports.
///
/// Port listeners are held until services start to prevent TOCTOU races.
///
/// # .env File Handling
///
/// The resolver enforces strict .env file handling:
/// - All variables in .env files MUST be declared as parameters
/// - Undeclared variables cause an error (prevents typos/hidden config)
/// - `.env` values are applied to parameters, not directly to service environments
/// - Explicit `value:` fields take precedence over `.env` values
///
/// # Shell Escaping
///
/// Process commands and scripts have parameter values shell-escaped to prevent
/// command injection. Environment variables in service configs are NOT escaped
/// (they are passed directly to the process environment).
pub struct Resolver {
    port_allocator: PortAllocator,
    resolved_parameters: HashMap<String, String>,
    /// Names of parameters with type: port
    port_parameter_names: Vec<String>,
    environment: crate::config::Environment,
    /// When true, port conflicts are auto-resolved using alternative ports (no interactive prompt)
    auto_resolve_conflicts: bool,
    /// When true, kill blocking processes/containers and use original ports (for --replace flag)
    replace_mode: bool,
    /// Working directory for resolving relative paths (e.g., .env files)
    work_dir: Option<PathBuf>,
    /// Tracks how each port parameter was resolved (for dry-run display and debuggability)
    port_resolutions: Vec<PortResolution>,
    /// Ports owned by already-running managed services.
    /// These are trusted without bind-checking during resolution because we manage the processes.
    managed_ports: HashSet<u16>,
    /// Unified port store — either SQLite-backed or no-op (isolated mode).
    /// The resolver doesn't need to know which backend is active.
    port_store: Box<dyn crate::port::PortStore>,
    /// When true, ignore configured default ports and allocate fresh random ports.
    force_random_ports: bool,
    /// When true, an available config `default:` port supersedes a cached port.
    /// Enabled for normal (non-isolated) starts; isolated mode keeps cache priority.
    prefer_config_defaults: bool,
    /// Whether stdin is a TTY (for interactive secret generation prompts).
    is_interactive: bool,
    offline: bool,
    /// Test seam: when set, used instead of the real cloud vault lookup.
    test_vault_values: Option<HashMap<String, String>>,
    /// Test seam: when set, the vault lookup is simulated as failing with this
    /// message (an unreachable cloud / revoked token). Takes precedence over
    /// `test_vault_values`.
    test_vault_failure: Option<String>,
    /// Active isolation session id, if any. Scopes the built-in
    /// `FED_PROJECT_ID` so parallel isolated stacks get distinct identifiers.
    isolation_id: Option<String>,
    /// Manual-secret names the current target (script + transitive deps) may
    /// reference. `None` = no scoping (fetch every missing manual secret) — the
    /// safe default for interactive `fed` and unknown commands. `Some(set)`
    /// scopes the vault query and the missing-secret failure to names the run
    /// actually needs.
    required_names: Option<HashSet<String>>,
    /// Parameter names deferred this run: out-of-scope missing manual secrets
    /// plus every parameter that transitively references one. Computed during
    /// `resolve_parameters` (empty for unscoped runs). A deferred parameter is
    /// neither failed for unresolved templates nor executed (`generate`), and
    /// any service or external service referencing one is dropped from the
    /// resolved config. See `compute_deferred_params` and `service_should_defer`.
    deferred_params: HashSet<String>,
}

impl Resolver {
    pub fn new() -> Self {
        Self {
            port_allocator: PortAllocator::new(),
            resolved_parameters: HashMap::new(),
            port_parameter_names: Vec::new(),
            environment: crate::config::Environment::default(),
            auto_resolve_conflicts: false,
            replace_mode: false,
            work_dir: None,
            port_resolutions: Vec::new(),
            managed_ports: HashSet::new(),
            port_store: Box::new(crate::port::NoopPortStore),
            force_random_ports: false,
            prefer_config_defaults: true,
            is_interactive: false,
            offline: false,
            test_vault_values: None,
            test_vault_failure: None,
            required_names: None,
            isolation_id: None,
            deferred_params: HashSet::new(),
        }
    }

    /// Create a new resolver with a specific environment
    pub fn with_environment(environment: crate::config::Environment) -> Self {
        Self {
            port_allocator: PortAllocator::new(),
            resolved_parameters: HashMap::new(),
            port_parameter_names: Vec::new(),
            environment,
            auto_resolve_conflicts: false,
            replace_mode: false,
            work_dir: None,
            port_resolutions: Vec::new(),
            managed_ports: HashSet::new(),
            port_store: Box::new(crate::port::NoopPortStore),
            force_random_ports: false,
            prefer_config_defaults: true,
            is_interactive: false,
            offline: false,
            test_vault_values: None,
            test_vault_failure: None,
            required_names: None,
            isolation_id: None,
            deferred_params: HashSet::new(),
        }
    }

    /// Set the port store backend.
    ///
    /// The resolver uses this for all port lookups and saves. Pass
    /// [`crate::port::NoopPortStore`] for isolated mode or
    /// [`crate::port::SqlitePortStore`] for persisted ports.
    pub fn set_port_store(&mut self, store: Box<dyn crate::port::PortStore>) {
        self.port_store = store;
    }

    /// Enable auto-resolve mode for port conflicts (use in TUI mode to avoid interactive prompts)
    pub fn set_auto_resolve_conflicts(&mut self, auto_resolve: bool) {
        self.auto_resolve_conflicts = auto_resolve;
    }

    /// Enable force-random port allocation mode.
    ///
    /// When enabled, port parameters skip configured default ports and
    /// always allocate random available ports.
    pub fn set_force_random_ports(&mut self, force_random: bool) {
        self.force_random_ports = force_random;
    }

    /// Prefer config `default:` ports over cached ports when the default is available.
    ///
    /// Enable for normal (non-isolated) starts so that editing `default:` in the
    /// config takes effect even when a stale port is cached from a previous run.
    /// Leave disabled in isolated mode, where cached random ports are intentional.
    pub fn set_prefer_config_defaults(&mut self, prefer: bool) {
        self.prefer_config_defaults = prefer;
    }

    /// Enable replace mode - kill blocking processes/containers and use original ports.
    /// Use this for `--replace` flag behavior.
    pub fn set_replace_mode(&mut self, replace: bool) {
        self.replace_mode = replace;
    }

    /// Set whether stdin is a TTY (for interactive secret generation prompts).
    pub fn set_is_interactive(&mut self, is_interactive: bool) {
        self.is_interactive = is_interactive;
    }

    /// Offline mode: never call the cloud vault for manual secrets.
    pub fn set_offline(&mut self, offline: bool) {
        self.offline = offline;
    }

    /// Whether this resolver is in offline mode.
    pub fn get_offline(&self) -> bool {
        self.offline
    }

    /// Test seam: stub the team-vault lookup with fixed values.
    #[cfg(test)]
    pub(crate) fn set_test_vault_values(&mut self, values: HashMap<String, String>) {
        self.test_vault_values = Some(values);
    }

    /// Test seam: stub the team-vault lookup as failing (unreachable cloud).
    #[cfg(test)]
    pub(crate) fn set_test_vault_failure(&mut self, message: &str) {
        self.test_vault_failure = Some(message.to_string());
    }

    /// Register ports owned by already-running managed services.
    ///
    /// These ports are trusted during resolution without bind-checking, because
    /// the port is held by a service we manage. This prevents `fed start` from
    /// prompting to kill our own services when they're already running.
    pub fn set_managed_ports(&mut self, ports: HashSet<u16>) {
        self.managed_ports = ports;
    }

    /// Set the environment for resolution
    pub fn set_environment(&mut self, environment: crate::config::Environment) {
        self.environment = environment;
    }

    /// Get the current environment
    pub fn get_environment(&self) -> crate::config::Environment {
        self.environment
    }

    /// Set working directory for resolving relative paths (e.g., .env files)
    pub fn set_work_dir<P: Into<PathBuf>>(&mut self, work_dir: P) {
        self.work_dir = Some(work_dir.into());
    }

    /// Set the active isolation session id.
    ///
    /// This scopes the built-in `{{FED_PROJECT_ID}}` so that parallel isolated
    /// stacks of the same project (e.g. worktrees under `fed isolate enable`)
    /// each get a distinct, stable identifier.
    pub fn set_isolation_id(&mut self, isolation_id: Option<String>) {
        self.isolation_id = isolation_id;
    }

    /// Scope the vault query to the manual-secret names the target actually
    /// references (see [`crate::parameter::scanner`]).
    ///
    /// `None` (the default) fetches every missing manual secret — the safe
    /// behavior for interactive `fed` and unknown commands. `Some(set)`
    /// restricts both the vault query and the missing-secret failure to `set`,
    /// so a script never blocks on (or fails for) a secret it doesn't use.
    pub fn set_required_names(&mut self, names: Option<HashSet<String>>) {
        self.required_names = names;
    }

    /// The scope actually stored on this resolver (`None` = unscoped). Used to
    /// propagate a parent run's scope verbatim into an ephemeral isolated-child
    /// orchestrator, instead of re-deriving it — see
    /// `run_script_isolated`. Re-derivation is provably identical for CLI runs
    /// but diverges for a public-API caller who set a custom scope (or `None`).
    pub fn required_names(&self) -> Option<HashSet<String>> {
        self.required_names.clone()
    }

    /// Whether a manual-secret name is in scope for this run. Names outside the
    /// scope are neither queried from the vault nor treated as required.
    fn name_in_scope(&self, name: &str) -> bool {
        match &self.required_names {
            Some(required) => required.contains(name),
            None => true,
        }
    }

    /// Compute the set of parameters deferred this run.
    ///
    /// The base set ("poison") is the manual secrets that are out of this run's
    /// scope: scoping (`fed <script>`) only fetches the secrets the target
    /// script transitively references, so anything outside that closure is never
    /// fetched and cannot be resolved. On top of that, any parameter whose value
    /// transitively depends on a poison name (via `default`, `generate`, or
    /// environment-specific interpolation) cannot be resolved either. The union
    /// is what must be deferred rather than failed.
    ///
    /// Determined from scope alone — it must be computed *before* secret
    /// resolution (whose `generate` DAG would otherwise execute a deferred
    /// generate over a secret it never fetches). Every deferred name is provably
    /// outside the scanned closure (an in-scope reference would put the secret
    /// in scope). A deferred name is therefore inert for this run: every
    /// downstream stage skips it, so it is never validated (port, `either`, or
    /// unresolved-template), never allocated a port, and never persisted — the
    /// secret-generate fallback writes nothing for it, so no random value can be
    /// stored and later mistaken for a real one. Over-approximation only ever
    /// touches things this run never spawns, exactly like the service-deferral
    /// rule.
    ///
    /// Returns an empty set for unscoped runs (`required_names == None`), so
    /// `fed start` and interactive `fed` stay exactly as strict as before: with
    /// nothing deferred, every unresolved template and every `generate` runs
    /// (and fails) as it does today.
    fn compute_deferred_params(&self, config: &Config) -> HashSet<String> {
        if self.required_names.is_none() {
            return HashSet::new();
        }
        let poison: HashSet<String> = config
            .get_effective_parameters()
            .iter()
            .filter(|(name, param)| param.is_manual_secret() && !self.name_in_scope(name))
            .map(|(name, _)| name.clone())
            .collect();
        if poison.is_empty() {
            return HashSet::new();
        }
        // Close over parameters that transitively reference a poison name, then
        // union the poison names themselves.
        let mut deferred = super::scanner::parameters_tainted_by(config, &poison);
        deferred.extend(poison);
        deferred
    }

    /// Whether a service must be deferred (dropped from the resolved config)
    /// this run because it references a deferred parameter — an out-of-scope
    /// missing manual secret, or a parameter that transitively depends on one.
    ///
    /// Scoping (`fed <script>`) only fetches the secrets the target script
    /// transitively references, so a deferred name is provably outside the
    /// scanned closure: the scanner walks every in-scope service whole-struct,
    /// so any name an in-scope service references is in scope (and not deferred)
    /// by construction. A reference to a deferred name can therefore only appear
    /// in a service this run will never spawn. Such services are dropped instead
    /// of hard-failing the whole run on a value it doesn't need; if one is
    /// somehow spawned anyway it fails loudly with `ServiceNotFound` rather than
    /// running with an unresolved value.
    ///
    /// Returns `false` for unscoped runs (`deferred_params` is empty), so
    /// `fed start` and interactive `fed` stay exactly as strict as before.
    pub(crate) fn service_should_defer(&self, service: &crate::config::Service) -> bool {
        if self.deferred_params.is_empty() {
            return false;
        }
        // Serialize the whole service and sweep every {{NAME}} out of it (the
        // same over-approximation the scanner uses), deferring on any reference
        // to a deferred parameter.
        let Ok(yaml) = serde_yaml::to_string(service) else {
            return false;
        };
        get_template_regex()
            .captures_iter(&yaml)
            .any(|cap| self.deferred_params.contains(cap[1].trim()))
    }

    /// Resolve template placeholders {{VAR}} with their values
    pub fn resolve_template(
        &self,
        template: &str,
        parameters: &HashMap<String, String>,
    ) -> Result<String> {
        Self::resolve_template_static(template, parameters)
    }

    /// Resolve `{{PARAM}}` placeholders in a template string.
    /// Static version for use outside the resolver (e.g., generate commands).
    pub fn resolve_template_static(
        template: &str,
        parameters: &HashMap<String, String>,
    ) -> Result<String> {
        Self::replace_placeholders(template, parameters, false)
    }

    /// Substitute `{{PARAM}}` placeholders in a single pass over the template.
    /// Substituted values are never re-scanned, so a value that itself contains
    /// `{{...}}` is inserted literally instead of being expanded (which would
    /// let one parameter's value smuggle in another's — including past shell
    /// escaping in the shell-safe variant).
    fn replace_placeholders(
        template: &str,
        parameters: &HashMap<String, String>,
        escape: bool,
    ) -> Result<String> {
        if template.is_empty() {
            return Ok(String::new());
        }

        let mut missing: Option<String> = None;
        let result = get_template_regex().replace_all(template, |cap: &regex::Captures| {
            // Trim so `{{ FOO }}` resolves like `{{FOO}}`, matching how
            // generate_dependencies extracts names for DAG ordering.
            let var_name = cap[1].trim();
            match parameters.get(var_name) {
                Some(value) => {
                    if escape {
                        shell_escape(value)
                    } else {
                        value.clone()
                    }
                }
                None => {
                    missing.get_or_insert_with(|| var_name.to_string());
                    String::new()
                }
            }
        });

        if let Some(name) = missing {
            return Err(Error::ParameterNotFound(name));
        }

        Ok(result.into_owned())
    }

    /// Resolve template placeholders with shell escaping for safe use in shell commands.
    /// Public for use in script execution at runtime.
    pub fn resolve_template_shell_safe(
        &self,
        template: &str,
        parameters: &HashMap<String, String>,
    ) -> Result<String> {
        Self::replace_placeholders(template, parameters, true)
    }

    /// Resolve environment variables
    pub fn resolve_environment(
        &self,
        environment: &HashMap<String, String>,
        parameters: &HashMap<String, String>,
    ) -> Result<HashMap<String, String>> {
        let mut resolved = HashMap::new();

        for (key, value) in environment {
            let resolved_value = self.resolve_template(value, parameters)?;
            resolved.insert(key.clone(), resolved_value);
        }

        Ok(resolved)
    }

    /// Consult the team vault for `queried_names`, applying the grace-window +
    /// freshness policy (see 02-cold-vault.md).
    ///
    /// The single async fetch is fired here and joined with a short grace: a
    /// warm vault (~0.17s) answers well inside it, so values are fresh every
    /// run. If grace expires, a fresh cache short-circuits the wait (and the
    /// abandoned request warms the backend); otherwise we block honestly on a
    /// cold start, announced on stderr, up to the generous budget. Connect/DNS
    /// failures never burn the budget — nothing is listening.
    fn obtain_vault_outcome(
        &self,
        work_dir: &Path,
        queried_names: &[String],
        analysis: &super::secret::SecretAnalysis,
    ) -> VaultOutcome {
        // Test seams bypass the timing machinery with a fixed outcome.
        if let Some(msg) = &self.test_vault_failure {
            return VaultOutcome::Failed(msg.clone());
        }
        if let Some(stub) = &self.test_vault_values {
            let values: HashMap<String, String> = queried_names
                .iter()
                .filter_map(|n| stub.get(n).map(|v| (n.clone(), v.clone())))
                .collect();
            return VaultOutcome::Values(values);
        }

        let Some(handle) = crate::cloud::spawn_fetch_values(
            work_dir,
            &self.environment.to_string(),
            queried_names,
        ) else {
            return VaultOutcome::Local;
        };

        let classify = |join: crate::cloud::VaultJoin, url: &str| -> Option<VaultOutcome> {
            match join {
                crate::cloud::VaultJoin::Answered(Ok(values)) => Some(VaultOutcome::Values(values)),
                crate::cloud::VaultJoin::Answered(Err(f)) => {
                    // Both unreachable and reached-but-failed fall back to the
                    // cache; carry a message that names the cloud.
                    Some(VaultOutcome::Failed(format!("{} ({})", f.message(), url)))
                }
                crate::cloud::VaultJoin::Pending => None,
            }
        };

        // Phase 1: the short grace wait.
        if let Some(outcome) = classify(handle.join(crate::cloud::vault_grace()), &handle.url) {
            return outcome;
        }

        // Grace expired. If the cache can cover every queried name freshly,
        // proceed on it and abandon the request (it warms the backend).
        if cache_covers_fresh(
            queried_names,
            &analysis.cache_values,
            &analysis.cache_stamps,
            unix_now(),
            crate::cloud::vault_max_age().as_secs(),
        ) {
            return VaultOutcome::CacheFresh;
        }

        // Phase 2: the cache can't cover it — block honestly on the cold start.
        eprintln!("waking vault… (cold start can take ~20s)");
        match classify(handle.join(crate::cloud::vault_timeout()), &handle.url) {
            Some(outcome) => outcome,
            None => VaultOutcome::Failed(format!(
                "cloud: no response within {}s ({})",
                crate::cloud::vault_timeout().as_secs(),
                handle.url
            )),
        }
    }

    /// Resolve secret parameters: generate missing auto-secrets, fail on missing manual secrets.
    ///
    /// This runs before `.env` loading so that newly-generated values are picked up
    /// by the normal `apply_env_file_to_parameters` path.
    fn resolve_secrets(&self, config: &mut Config) -> Result<()> {
        let work_dir = match self.work_dir.as_ref() {
            Some(dir) => dir.clone(),
            None => return Ok(()), // No work dir → skip (unit tests, etc.)
        };

        // Skip early (and avoid creating .fed/) when there are no secret params.
        if !config
            .get_effective_parameters()
            .values()
            .any(|p| p.is_secret_type())
        {
            return Ok(());
        }

        // .fed/ holds the vault cache and (by default) generated secrets.
        // Ensure it exists — with its self-ignoring .gitignore — before any
        // git-status checks so the default paths analyze as ignored.
        crate::fed_dir::ensure_fed_dir(&work_dir)?;

        // Generated secrets always live at .fed/secrets.generated.env.
        let secrets_file_path = crate::fed_dir::default_generated_secrets_path(&work_dir);
        // The env_file key under which the generated secrets file is loaded.
        let generated_env_key = crate::fed_dir::GENERATED_SECRETS_REL.to_string();
        let cache_path = crate::fed_dir::secrets_cache_path(&work_dir);

        // Cache safety gate: the cache holds real secret values, so it must
        // never sit in a commit-eligible location. With the self-managed
        // .fed/.gitignore this always passes; a user-edited permissive
        // .fed/.gitignore disables caching entirely — an existing cache file
        // is DELETED (leaving secrets on disk where git can pick them up is
        // the unsafe option) and its values are neither read nor rewritten.
        let cache_usable = {
            let (in_repo, ignored) = super::secret::path_git_status(&cache_path);
            if in_repo && !ignored {
                let existed = cache_path.exists();
                if existed && let Err(e) = std::fs::remove_file(&cache_path) {
                    tracing::warn!(
                        "could not remove commit-eligible secrets cache {}: {}",
                        cache_path.display(),
                        e
                    );
                }
                tracing::warn!(
                    "vault secret caching disabled: {} is not gitignored (was .fed/.gitignore \
                     edited?).{} Offline runs won't have vault values until .fed/.gitignore \
                     ignores it again.",
                    cache_path.display(),
                    if existed {
                        " The existing cache file was removed."
                    } else {
                        ""
                    }
                );
                false
            } else {
                true
            }
        };

        let mut analysis = match super::secret::analyze_secrets(
            config,
            &work_dir,
            &secrets_file_path,
            &cache_path,
        )? {
            Some(a) => a,
            None => return Ok(()), // No secret parameters at all
        };
        if !cache_usable {
            analysis.cache_values.clear();
        }

        // Team vault: when online and linked, the vault is authoritative for
        // manual secrets — query it for every missing (required AND optional)
        // name, including names the cache could satisfy, so rotated or revoked
        // values are picked up. Requires `fed login` + `fed link`; skipped
        // with --offline.
        let mut vault_resolved: Vec<(String, String)> = Vec::new();
        let mut vault_query_succeeded = false;
        // Captures why the vault lookup failed (network/auth), so a later
        // missing-secret error can name the real cause instead of blaming the
        // user's env_file for an unreachable cloud.
        let mut vault_failure: Option<String> = None;
        // Scope the vault query to names this run actually references. The
        // analysis itself stays project-wide (D2): its `cache_values` feed the
        // cache rewrite below, and scoping them would prune other scripts'
        // cached secrets on every run. Only what we *fetch* — and, at the end,
        // what we *fail on* — is scoped.
        let queried_names: Vec<String> = analysis
            .missing_manual
            .iter()
            .map(|(name, _)| name.clone())
            .chain(analysis.missing_optional_manual.iter().cloned())
            .filter(|name| self.name_in_scope(name))
            .collect();
        if !queried_names.is_empty() && !self.offline {
            match self.obtain_vault_outcome(&work_dir, &queried_names, &analysis) {
                VaultOutcome::Values(values) => {
                    vault_query_succeeded = true;
                    for (name, value) in values {
                        if let Some(param) = config.get_effective_parameters_mut().get_mut(&name) {
                            param.value = Some(value.clone());
                        }
                        vault_resolved.push((name, value));
                    }
                    let resolved_names: HashSet<&str> =
                        vault_resolved.iter().map(|(n, _)| n.as_str()).collect();
                    analysis
                        .missing_manual
                        .retain(|(name, _)| !resolved_names.contains(name.as_str()));
                    analysis
                        .missing_optional_manual
                        .retain(|name| !resolved_names.contains(name.as_str()));
                    if !vault_resolved.is_empty() {
                        tracing::info!(
                            "Resolved {} secret(s) from the team vault",
                            vault_resolved.len()
                        );
                    }
                }
                VaultOutcome::CacheFresh => {
                    // Grace expired but the cache covers every queried name
                    // freshly — proceed on it, no warning. The abandoned
                    // in-flight request has already warmed the backend.
                    tracing::debug!("team vault slow to answer; proceeding on fresh cached values");
                }
                VaultOutcome::Local => {} // not logged in / not linked — local mode
                VaultOutcome::Failed(reason) => {
                    // Reached-but-unusable or unreachable: fall back to the
                    // cache regardless of age (offline work must keep working),
                    // and remember the reason so a missing-secret failure names
                    // the cloud instead of the user's env_file.
                    tracing::warn!(
                        "team vault unavailable ({}); proceeding on cached secret values where available",
                        reason
                    );
                    vault_failure = Some(reason);
                }
            }
        }

        if vault_query_succeeded {
            // The vault answered: rewrite the cache to mirror it, stamping the
            // freshly-fetched names with the current time. Non-queried entries
            // are carried forward with their existing stamps; queried names the
            // vault no longer has are dropped (rotation/revocation), as are keys
            // for parameters no longer declared (analyze_secrets filtered those).
            let now = unix_now();
            let mut new_cache: HashMap<String, super::secret::CacheEntry> = analysis
                .cache_values
                .iter()
                .map(|(k, v)| {
                    (
                        k.clone(),
                        super::secret::CacheEntry {
                            value: v.clone(),
                            fetched_at: analysis.cache_stamps.get(k).copied(),
                        },
                    )
                })
                .collect();
            for name in &queried_names {
                new_cache.remove(name);
            }
            for (name, value) in &vault_resolved {
                new_cache.insert(
                    name.clone(),
                    super::secret::CacheEntry {
                        value: value.clone(),
                        fetched_at: Some(now),
                    },
                );
            }
            // cache_usable was decided (and warned about) up front; an unsafe
            // path means no cache writes at all.
            if cache_usable && let Err(e) = super::secret::write_cache_file(&cache_path, &new_cache)
            {
                tracing::warn!(
                    "could not cache vault secrets to {}: {}",
                    cache_path.display(),
                    e
                );
            }
        } else {
            // Vault unavailable (--offline, unlinked, or lookup failed): the
            // cache satisfies missing manual secrets, required ones included.
            let cache = &analysis.cache_values;
            for (name, _) in &analysis.missing_manual {
                if let Some(value) = cache.get(name)
                    && let Some(param) = config.get_effective_parameters_mut().get_mut(name)
                {
                    param.value = Some(value.clone());
                }
            }
            for name in &analysis.missing_optional_manual {
                if let Some(value) = cache.get(name)
                    && let Some(param) = config.get_effective_parameters_mut().get_mut(name)
                {
                    param.value = Some(value.clone());
                }
            }
            analysis
                .missing_manual
                .retain(|(name, _)| !cache.contains_key(name));
            analysis
                .missing_optional_manual
                .retain(|name| !cache.contains_key(name));
        }

        // Optional manual secrets the vault couldn't supply fall back to an
        // empty string so they resolve without error.
        for name in &analysis.missing_optional_manual {
            if let Some(param) = config.get_effective_parameters_mut().get_mut(name)
                && param.value.is_none()
            {
                param.value = Some(String::new());
            }
        }

        // Fail on missing manual secrets — user must provide these. Only
        // names in scope for this run count: a script must not fail on a
        // project-wide secret it never references (that is the whole point of
        // scoping — see 01-secret-scoping.md). Unscoped names stay in the
        // analysis (so the cache logic above is unaffected) but are excluded
        // from the failure here.
        let unmet: Vec<&(String, Option<String>)> = analysis
            .missing_manual
            .iter()
            .filter(|(name, _)| self.name_in_scope(name))
            .collect();
        if !unmet.is_empty() {
            let details: Vec<String> = unmet
                .iter()
                .map(|(name, desc)| match desc {
                    Some(d) => format!("  - {} ({})", name, d),
                    None => format!("  - {}", name),
                })
                .collect();
            let env_files_hint = if config.env_file.is_empty() {
                "your env_file".to_string()
            } else {
                config.env_file.join(", ")
            };
            // When the vault lookup itself failed (unreachable cloud, revoked
            // token), name that cause — otherwise the user is told to "add it to
            // your env_file" for a secret that was actually sitting in the vault.
            if let Some(reason) = &vault_failure {
                return Err(Error::Validation(format!(
                    "Missing secret values — the team vault could not be reached, so these could not be fetched ({}):\n{}\n\nOnce the vault is reachable again fed will fetch them; or add them to your env_file ({}) to proceed offline. These secrets have source: manual, so fed won't generate them.",
                    reason,
                    details.join("\n"),
                    env_files_hint
                )));
            }
            return Err(Error::Validation(format!(
                "Missing secret values — add them to your env_file ({}), or put them in your team vault (fed login, fed link, then set them in the dashboard):\n{}\n\nThese secrets have source: manual, so fed won't generate them.",
                env_files_hint,
                details.join("\n")
            )));
        }

        // Prepend the generated secrets file to env_file so it's loaded by
        // apply_env_file_to_parameters (lowest priority — user's .env files can
        // override). Only when it exists — it may not on the very first run.
        // The vault cache is deliberately NOT loaded as an env file: its values
        // are applied directly above, so leftover entries can never trip strict
        // env loading or shadow the vault.
        if secrets_file_path.exists() && !config.env_file.contains(&generated_env_key) {
            config.env_file.insert(0, generated_env_key.clone());
        }

        // Run DAG-based resolution for all secrets with `generate` commands.
        // This handles invalidation cascading even for secrets that have existing values.
        //
        // Deferred params are dropped from the DAG input: a deferred `generate`
        // references an out-of-scope missing manual secret this run never
        // fetches, so executing it here would hard-fail on a value the run
        // doesn't need. Every deferred name is provably outside the scanned
        // closure, so this can't drop a generate the target actually uses.
        //
        // The DAG is seeded with an EMPTY resolved map: a secret generator that
        // references any parameter (`printf %s {{SEED}}`) fails with
        // ParameterNotFound, exactly as before. A reference-less generator
        // (`openssl rand -hex 32`, `uuidgen`) still generates and persists.
        let effective_params: HashMap<String, crate::config::Parameter> = config
            .get_effective_parameters()
            .iter()
            .filter(|(name, _param)| !self.deferred_params.contains(*name))
            .map(|(name, param)| (name.clone(), param.clone()))
            .collect();
        let generate_results = super::generate::resolve_generate_params(
            &effective_params,
            &analysis.existing_values,
            &HashMap::new(),
        )?;

        let dag_generated: HashSet<String> =
            generate_results.iter().map(|r| r.name.clone()).collect();

        let mut generated: Vec<(String, String)> = generate_results
            .into_iter()
            .filter(|r| r.was_generated)
            .map(|r| (r.name, r.value))
            .collect();

        // Simple secrets (no generate command) — use random alphanumeric.
        //
        // Two kinds of name must never reach this random fallback:
        //   - deferred names: a scoped run persists nothing for a parameter it
        //     defers (its value is out of scope this run); writing a random value
        //     would be an out-of-scope side effect.
        //   - anything carrying a `generate` command: if its generator did not run
        //     in the DAG above (it was deferred, or a dependency was), the fix is
        //     to run that generator on a later in-scope run — never to substitute
        //     randomness. A random value here would be persisted and then kept by
        //     the next run (generate.rs preserves existing secret values),
        //     permanently poisoning the derived secret.
        let effective = config.get_effective_parameters();
        for name in &analysis.needs_generation {
            if dag_generated.contains(name) || self.deferred_params.contains(name) {
                continue;
            }
            if effective.get(name).is_some_and(|p| p.has_generate()) {
                continue;
            }
            generated.push((name.clone(), super::secret::generate_secret()));
        }

        // Nothing to write? We're done.
        if generated.is_empty() {
            return Ok(());
        }

        // Gitignore gate: if the secrets file is in a git repo and not ignored,
        // refuse. For the default .fed/ location this only trips if the user
        // edited .fed/.gitignore to unignore it.
        if analysis.in_git_repo && !analysis.is_gitignored {
            return Err(Error::Validation(format!(
                "Refusing to write secrets: '{}' is inside a git repository and is not gitignored.\n\n\
                 Restoring fed's .fed/.gitignore is enough — fed keeps it out of git \
                 automatically. If the file is already tracked by git, also run \
                 `git rm --cached {}`.",
                generated_env_key, generated_env_key
            )));
        }

        // Interactive confirmation when running in a TTY
        if self.is_interactive {
            eprint!(
                "Secret parameters need values. Generate and write to {}? [Y/n] ",
                analysis.env_path.display()
            );
            let mut input = String::new();
            if std::io::stdin().read_line(&mut input).is_ok() {
                let trimmed = input.trim().to_lowercase();
                if !trimmed.is_empty() && trimmed != "y" && trimmed != "yes" {
                    return Err(Error::Aborted);
                }
            }
        } else {
            tracing::info!("Generating secret values → {}", analysis.env_path.display());
        }

        super::secret::write_env_file(&analysis.env_path, &generated)?;

        // Ensure the generated secrets file is in env_file now that it exists
        if !config.env_file.contains(&generated_env_key) {
            config.env_file.insert(0, generated_env_key.clone());
        }

        Ok(())
    }

    /// Load .env files and apply values to parameters.
    /// Returns error if .env file sets a variable that isn't declared as a parameter.
    ///
    /// All variables are loaded first, then applied. This means:
    /// - Later .env files override earlier ones for the same variable
    /// - The error message for undeclared variables references the last file that set it
    fn apply_env_file_to_parameters(&self, config: &mut Config) -> Result<()> {
        if config.env_file.is_empty() {
            return Ok(());
        }

        let config_dir = self.work_dir.as_ref().ok_or_else(|| {
            Error::TemplateResolution(
                "Work directory not set, cannot resolve global env_file paths".to_string(),
            )
        })?;

        // Load all .env files first, tracking which file each variable came from.
        // Later files override earlier ones (consistent with documented behavior).
        let mut all_env_vars: HashMap<String, (String, String)> = HashMap::new();

        for env_file_path in &config.env_file {
            let full_path = super::expand_tilde(Path::new(env_file_path));
            let full_path = if full_path.is_absolute() {
                full_path
            } else {
                config_dir.join(full_path)
            };
            let env_vars = match crate::config::env_loader::load_env_file_optional(&full_path)
                .map_err(|e| {
                    Error::TemplateResolution(format!(
                        "Failed to load environment file '{}' (resolved to '{}'): {}",
                        env_file_path,
                        full_path.display(),
                        e
                    ))
                })? {
                Some(vars) => vars,
                None => {
                    tracing::warn!(
                        "env_file '{}' (resolved to '{}') does not exist — continuing without it. \
                         Parameters that depend on values from this file may be unset.",
                        env_file_path,
                        full_path.display()
                    );
                    continue;
                }
            };

            // Track value and source file (later files override earlier)
            for (key, value) in env_vars {
                all_env_vars.insert(key, (value, env_file_path.clone()));
            }
        }

        // Now apply with a single mutable borrow
        let effective_params = config.get_effective_parameters_mut();
        for (key, (value, env_file_path)) in all_env_vars {
            if let Some(param) = effective_params.get_mut(&key) {
                // Only set if parameter doesn't already have an explicit value
                // (explicit values take precedence over .env files)
                if param.value.is_none() {
                    param.value = Some(value);
                }
            } else {
                // Variable is not declared as a parameter - this is an error
                return Err(Error::UndeclaredEnvVariable {
                    name: key,
                    env_file: env_file_path,
                });
            }
        }

        Ok(())
    }

    /// Resolve just the parameters (first pass, before external service expansion)
    pub fn resolve_parameters(&mut self, config: &mut Config) -> Result<()> {
        // Clear any stale resolutions from a previous call (e.g. dry-run then real start)
        self.port_resolutions.clear();

        // Determine which parameters are deferred this run BEFORE anything is
        // resolved — secret resolution's `generate` DAG (below), default
        // resolution, and non-secret generates all consult this set to skip a
        // value that (transitively) depends on an out-of-scope missing manual
        // secret. Empty for unscoped runs, so nothing below changes there.
        self.deferred_params = self.compute_deferred_params(config);

        // Resolve secrets first — may generate .env entries and add ".env" to config.env_file
        self.resolve_secrets(config)?;

        // Apply .env file values to parameters (strict mode: must be declared)
        self.apply_env_file_to_parameters(config)?;

        // Build parameters map - first pass for direct values and port allocation
        let mut parameters = HashMap::new();

        // Inject the built-in FED_PROJECT_ID before user parameters so their
        // defaults can interpolate it. It needs the work_dir (basename + hash);
        // without one (e.g. bare unit tests) there is nothing stable to derive.
        if let Some(ref work_dir) = self.work_dir {
            let project_id = compute_project_id(work_dir, self.isolation_id.as_deref());
            parameters.insert(FED_PROJECT_ID.to_string(), project_id.clone());
            self.resolved_parameters
                .insert(FED_PROJECT_ID.to_string(), project_id);
        }

        // Use effective parameters (variables take precedence over parameters)
        let effective_params = config.get_effective_parameters().clone();

        for (name, param) in &effective_params {
            // A deferred parameter's value (transitively) depends on an
            // out-of-scope missing manual secret this run never fetches. Skip it
            // whole: no port validation/allocation (a deferred port must allocate
            // nothing), no `either`/default handling, no resolved-value side
            // effect. In-scope names are never deferred, so this changes nothing
            // for them; unscoped runs defer nothing at all.
            if self.deferred_params.contains(name) {
                continue;
            }

            // Track port-type parameters
            if param.is_port_type() {
                self.port_parameter_names.push(name.clone());
            }

            if let Some(ref value) = param.value {
                // Validate port values if parameter is port type
                if param.is_port_type() {
                    let port_num = value.parse::<u16>().map_err(|_| {
                        Error::TemplateResolution(format!(
                            "Parameter '{}' has invalid port value '{}': must be a number between 1 and 65535",
                            name, value
                        ))
                    })?;

                    if port_num == 0 {
                        return Err(Error::TemplateResolution(format!(
                            "Parameter '{}' has invalid port value '0': must be between 1 and 65535",
                            name
                        )));
                    }
                }

                parameters.insert(name.clone(), value.clone());
                self.resolved_parameters.insert(name.clone(), value.clone());
            } else if param.is_port_type() {
                // Validate the config default up front. Without this, an
                // unparseable default (a template like {{BASE_PORT}}, or an
                // out-of-range value like 70000) silently falls back to a
                // random port, and `default: 0` "binds" successfully because
                // the OS assigns an ephemeral port — while the same values in
                // `value:` are hard errors.
                if let Some(env_value) = param.get_value_for_environment(&self.environment) {
                    let default_str = Self::value_to_string(env_value);
                    let default_port = default_str.parse::<u16>().map_err(|_| {
                        Error::TemplateResolution(format!(
                            "Parameter '{}' has invalid port default '{}': must be a literal number between 1 and 65535 (templates are not supported in port defaults)",
                            name, default_str
                        ))
                    })?;
                    if default_port == 0 {
                        return Err(Error::TemplateResolution(format!(
                            "Parameter '{}' has invalid port default '0': must be between 1 and 65535",
                            name
                        )));
                    }
                }

                // Port resolution via unified PortStore.
                // The store may be SQLite-backed or no-op (isolated).
                // One lookup path, one save path — no dual-cache priority chain.
                let (port, reason) = if let Some(cached_port) = self.port_store.get_port(name) {
                    // A config default that's available supersedes the cached port
                    // (normal mode only) — otherwise editing `default:` in the
                    // config would silently have no effect.
                    if let Some(reclaimed) = self.try_reclaim_config_default(param, cached_port) {
                        reclaimed
                    } else {
                        // Port store has a cached value — validate it's still usable
                        self.validate_cached_port(cached_port, name)?
                    }
                } else {
                    // No cached port — allocate from config default or random
                    self.allocate_fresh_port(param, name)?
                };

                // Save back to the store for future reuse
                self.port_store.save_port(name, port)?;

                self.port_resolutions.push(PortResolution {
                    param_name: name.clone(),
                    resolved_port: port,
                    reason,
                });

                let port_str = port.to_string();
                parameters.insert(name.clone(), port_str.clone());
                self.resolved_parameters.insert(name.clone(), port_str);
            } else if let Some(env_value) = param.get_value_for_environment(&self.environment) {
                // Non-port parameter with environment-specific value
                let default_str = Self::value_to_string(env_value);
                parameters.insert(name.clone(), default_str.clone());
                self.resolved_parameters.insert(name.clone(), default_str);
            }
        }

        // Multiple passes: resolve templates in parameter default values
        const MAX_PASSES: usize = 10;
        let mut pass_count = 0;
        for _ in 0..MAX_PASSES {
            pass_count += 1;
            let mut any_resolved = false;

            for (name, param) in &effective_params {
                // Deferred params are left untouched everywhere (see the first
                // pass): their template references an out-of-scope secret and
                // could only ever partially resolve into a value nothing uses.
                if self.deferred_params.contains(name) {
                    continue;
                }
                // Port parameters were already resolved by the allocator above;
                // re-resolving their default here would overwrite the allocated
                // port with an unchecked value.
                if param.is_port_type() {
                    continue;
                }
                if let Some(env_value) = param.get_value_for_environment(&self.environment) {
                    let default_str = Self::value_to_string(env_value);
                    if default_str.contains("{{")
                        && let Ok(resolved_default) =
                            self.resolve_template(&default_str, &parameters)
                        && resolved_default != default_str
                    {
                        parameters.insert(name.clone(), resolved_default.clone());
                        self.resolved_parameters
                            .insert(name.clone(), resolved_default);
                        any_resolved = true;
                    }
                }
            }

            if !any_resolved {
                break;
            }
        }

        // Check for unresolved templates (circular references or missing parameters)
        for (name, param) in &effective_params {
            // A deferred parameter's unresolved references are (transitively)
            // out-of-scope missing manual secrets this run never fetches — not a
            // failure. In-scope failures are not in the set and still error.
            if self.deferred_params.contains(name) {
                continue;
            }
            if let Some(env_value) = param.get_value_for_environment(&self.environment) {
                let default_str = Self::value_to_string(env_value);
                if default_str.contains("{{") {
                    // Check if parameter was resolved
                    if let Some(resolved_value) = parameters.get(name)
                        && resolved_value.contains("{{")
                    {
                        // Extract the unresolved variable names
                        let unresolved_vars = self.extract_template_variables(resolved_value);
                        if pass_count >= MAX_PASSES {
                            return Err(Error::TemplateResolution(format!(
                                "Circular parameter reference detected in parameter '{}'. \
                                     Unresolved variables after {} passes: {:?}",
                                name, MAX_PASSES, unresolved_vars
                            )));
                        } else {
                            return Err(Error::TemplateResolution(format!(
                                "Parameter '{}' has unresolved template variables: {:?}",
                                name, unresolved_vars
                            )));
                        }
                    }
                }
            }
        }

        // Resolve non-secret `generate` parameters (recompute every start).
        // Secret generate params were already handled in resolve_secrets.
        // A deferred generate references an out-of-scope missing manual secret
        // (directly or transitively); executing it would fail on a value this
        // run never fetches — and running an unrelated generate in a scoped run
        // is wasteful anyway. Skip them; unscoped runs defer nothing.
        let non_secret_generate: HashMap<String, crate::config::Parameter> = effective_params
            .iter()
            .filter(|(name, p)| {
                p.has_generate() && !p.is_secret_type() && !self.deferred_params.contains(*name)
            })
            .map(|(name, p)| (name.clone(), p.clone()))
            .collect();

        if !non_secret_generate.is_empty() {
            let generate_results = super::generate::resolve_generate_params(
                &non_secret_generate,
                &HashMap::new(), // Non-secrets have no persisted values.
                &parameters,     // Already-resolved params (ports, secrets, defaults).
            )?;
            for result in generate_results {
                if result.was_generated {
                    parameters.insert(result.name.clone(), result.value.clone());
                    self.resolved_parameters.insert(result.name, result.value);
                }
            }
        }

        // Validate 'either' constraints
        for (name, param) in &effective_params {
            // A deferred param's value is (transitively) an out-of-scope secret
            // this run never resolves — validating it against `either` would fail
            // on the raw `{{...}}` placeholder. In-scope names are never deferred.
            if self.deferred_params.contains(name) {
                continue;
            }
            if !param.either.is_empty()
                && let Some(resolved_value) = parameters.get(name)
                && !param.either.contains(resolved_value)
            {
                return Err(Error::TemplateResolution(format!(
                    "Parameter '{}' has value '{}' which is not in the allowed values: {:?}",
                    name, resolved_value, param.either
                )));
            }
        }

        Ok(())
    }

    /// Resolve all templates in configuration
    pub fn resolve_config(&mut self, config: &Config) -> Result<Config> {
        // Get parameters (already resolved by resolve_parameters, or resolve them now if not called yet)
        let parameters = if self.resolved_parameters.is_empty() {
            let mut params = HashMap::new();
            let effective_params = config.get_effective_parameters();
            for (name, param) in effective_params {
                if let Some(ref value) = param.value {
                    params.insert(name.clone(), value.clone());
                } else if let Some(env_value) = param.get_value_for_environment(&self.environment) {
                    params.insert(name.clone(), Self::value_to_string(env_value));
                } else if param.is_port_type() {
                    let port = self.port_allocator.allocate_random_port()?;
                    params.insert(name.clone(), port.to_string());
                }
            }
            params
        } else {
            self.resolved_parameters.clone()
        };

        // Create resolved config
        let mut resolved = config.clone();

        // Resolve services - environment variables now only come from inline config
        // (.env files are applied to parameters, not directly to service environments)
        //
        // Under scoping, a service that references a deferred parameter (an
        // out-of-scope missing manual secret, or a value derived from one) is
        // out of the scanned closure and will never be spawned; defer it
        // (collect here, drop after the loop) rather than hard-failing the whole
        // run on a value it doesn't need. See `service_should_defer`.
        let mut deferred_services: Vec<String> = Vec::new();
        for (name, service) in &mut resolved.services {
            if self.service_should_defer(service) {
                tracing::debug!(
                    "deferring out-of-scope service '{}': it references a deferred parameter (out-of-scope missing secret or a value derived from one)",
                    name
                );
                deferred_services.push(name.clone());
                continue;
            }
            // Resolve templates in service environment
            service.environment = self
                .resolve_environment(&service.environment, &parameters)
                .map_err(|e| {
                    Error::TemplateResolution(format!(
                        "Failed to resolve environment for service '{}': {}",
                        name, e
                    ))
                })?;

            // Resolve process command with shell escaping for security
            if let Some(ref process) = service.process {
                service.process = Some(
                    self.resolve_template_shell_safe(process, &parameters)
                        .map_err(|e| {
                            Error::TemplateResolution(format!(
                                "Failed to resolve process for service '{}': {}",
                                name, e
                            ))
                        })?,
                );
            }

            // Resolve install command with shell escaping for security
            if let Some(ref install) = service.install {
                service.install = Some(
                    self.resolve_template_shell_safe(install, &parameters)
                        .map_err(|e| {
                            Error::TemplateResolution(format!(
                                "Failed to resolve install for service '{}': {}",
                                name, e
                            ))
                        })?,
                );
            }

            // Resolve migrate command with shell escaping for security.
            // (Hook-only nodes and process services both express staged
            // setup through `migrate:`, so it must resolve templates the
            // same way.)
            if let Some(ref migrate) = service.migrate {
                service.migrate = Some(
                    self.resolve_template_shell_safe(migrate, &parameters)
                        .map_err(|e| {
                            Error::TemplateResolution(format!(
                                "Failed to resolve migrate for service '{}': {}",
                                name, e
                            ))
                        })?,
                );
            }

            // Resolve ports
            if !service.ports.is_empty() {
                let mut resolved_ports = Vec::new();
                for port in &service.ports {
                    resolved_ports.push(self.resolve_template(port, &parameters).map_err(|e| {
                        Error::TemplateResolution(format!(
                            "Failed to resolve port for service '{}': {}",
                            name, e
                        ))
                    })?);
                }
                service.ports = resolved_ports;
            }

            // Resolve health check
            if let Some(ref healthcheck) = service.healthcheck {
                match healthcheck {
                    crate::config::HealthCheck::HttpGet { http_get, timeout } => {
                        let resolved_url =
                            self.resolve_template(http_get, &parameters).map_err(|e| {
                                Error::TemplateResolution(format!(
                                    "Failed to resolve health check for service '{}': {}",
                                    name, e
                                ))
                            })?;
                        service.healthcheck = Some(crate::config::HealthCheck::HttpGet {
                            http_get: resolved_url,
                            timeout: timeout.clone(),
                        });
                    }
                    crate::config::HealthCheck::CommandMap { command, timeout } => {
                        let resolved_cmd =
                            self.resolve_template(command, &parameters).map_err(|e| {
                                Error::TemplateResolution(format!(
                                    "Failed to resolve health check command for service '{}': {}",
                                    name, e
                                ))
                            })?;
                        service.healthcheck = Some(crate::config::HealthCheck::CommandMap {
                            command: resolved_cmd,
                            timeout: timeout.clone(),
                        });
                    }
                    crate::config::HealthCheck::Command(cmd) => {
                        let resolved_cmd =
                            self.resolve_template(cmd, &parameters).map_err(|e| {
                                Error::TemplateResolution(format!(
                                    "Failed to resolve health check command for service '{}': {}",
                                    name, e
                                ))
                            })?;
                        service.healthcheck =
                            Some(crate::config::HealthCheck::Command(resolved_cmd));
                    }
                }
            }

            // Resolve external service parameters
            if !service.parameters.is_empty() {
                let mut resolved_params = HashMap::new();
                for (key, value) in &service.parameters {
                    let resolved_value =
                        self.resolve_template(value, &parameters).map_err(|e| {
                            Error::TemplateResolution(format!(
                                "Failed to resolve parameter '{}' for service '{}': {}",
                                key, name, e
                            ))
                        })?;
                    resolved_params.insert(key.clone(), resolved_value);
                }
                service.parameters = resolved_params;
            }

            // Resolve startup_message template (display string, not shell-escaped)
            if let Some(ref msg) = service.startup_message {
                service.startup_message =
                    Some(self.resolve_template(msg, &parameters).map_err(|e| {
                        Error::TemplateResolution(format!(
                            "Failed to resolve startup_message for service '{}': {}",
                            name, e
                        ))
                    })?);
            }
        }

        // Drop the deferred out-of-scope services from the resolved config so
        // they are never created or started. A stray spawn attempt then fails
        // loudly with `ServiceNotFound` instead of silently running with an
        // unresolved secret value.
        for name in deferred_services {
            resolved.services.remove(&name);
        }

        // NOTE: Scripts are NOT resolved here. Script environments and commands are
        // resolved at execution time in run_script_interactive() or run_script_isolated().
        // This allows isolated scripts to use fresh port allocations instead of
        // the parent orchestrator's cached ports.

        Ok(resolved)
    }

    /// Convert YAML value to string
    fn value_to_string(value: &serde_yaml::Value) -> String {
        match value {
            serde_yaml::Value::String(s) => s.clone(),
            serde_yaml::Value::Number(n) => n.to_string(),
            serde_yaml::Value::Bool(b) => b.to_string(),
            _ => format!("{:?}", value),
        }
    }

    /// Extract template variables from a string
    pub fn extract_template_variables(&self, template: &str) -> Vec<String> {
        get_template_regex()
            .captures_iter(template)
            .filter_map(|cap| cap.get(1).map(|m| m.as_str().to_string()))
            .collect()
    }

    /// Get resolved parameters
    pub fn get_resolved_parameters(&self) -> &HashMap<String, String> {
        &self.resolved_parameters
    }

    /// Get all allocated ports
    pub fn get_allocated_ports(&self) -> Vec<u16> {
        self.port_allocator.allocated_ports()
    }

    /// Get names of all port-type parameters
    pub fn get_port_parameter_names(&self) -> &[String] {
        &self.port_parameter_names
    }

    /// Get port resolution decisions for display in dry-run and status commands
    pub fn get_port_resolutions(&self) -> &[PortResolution] {
        &self.port_resolutions
    }

    /// Release port listeners.
    ///
    /// This method uses interior mutability in the port allocator to allow
    /// calling with `&self`, enabling concurrent start operations.
    pub(crate) fn release_port_listeners(&self) {
        self.port_allocator.release_listeners();
    }

    /// Validate a cached port (from the port store) is still usable.
    ///
    /// If the port is owned by a managed service, trust it. If it's free, allocate it.
    /// If it's taken by something else, resolve the conflict.
    fn validate_cached_port(
        &mut self,
        cached_port: u16,
        param_name: &str,
    ) -> Result<(u16, PortResolutionReason)> {
        if self.managed_ports.contains(&cached_port) {
            tracing::debug!(
                "Reusing cached port {} for '{}' (owned by running service)",
                cached_port,
                param_name
            );
            self.port_allocator.mark_allocated(cached_port);
            Ok((cached_port, PortResolutionReason::Cached))
        } else if self.port_allocator.try_allocate_port(cached_port).is_ok() {
            tracing::debug!(
                "Reusing cached port {} for '{}' (port is free)",
                cached_port,
                param_name
            );
            Ok((cached_port, PortResolutionReason::Cached))
        } else {
            tracing::warn!(
                "Cached port {} for '{}' is no longer available, resolving conflict...",
                cached_port,
                param_name
            );
            let (new_port, conflict) =
                self.handle_port_conflict_interactive(cached_port, param_name)?;
            let first = conflict.as_ref().and_then(|c| c.processes.first());
            Ok((
                new_port,
                PortResolutionReason::ConflictAutoResolved {
                    default_port: cached_port,
                    conflict_pid: first.map(|p| p.pid),
                    conflict_process: first.map(|p| p.name.clone()),
                },
            ))
        }
    }

    /// Try to reclaim the config `default:` port when it differs from the cached port.
    ///
    /// Returns `Some` only when all of these hold:
    /// - defaults are preferred (normal mode, not isolated, not force-random)
    /// - the cached port is not held by a running managed service (switching a
    ///   live service's port would break connections between services)
    /// - the config declares a parseable default that differs from the cache
    /// - the default port is actually available right now
    ///
    /// Otherwise returns `None` and the caller falls back to the cached port.
    fn try_reclaim_config_default(
        &mut self,
        param: &crate::config::Parameter,
        cached_port: u16,
    ) -> Option<(u16, PortResolutionReason)> {
        if !self.prefer_config_defaults || self.force_random_ports {
            return None;
        }
        if self.managed_ports.contains(&cached_port) {
            return None;
        }
        let env_value = param.get_value_for_environment(&self.environment)?;
        let default_port = Self::value_to_string(env_value).parse::<u16>().ok()?;
        if default_port == cached_port {
            return None;
        }
        if self.managed_ports.contains(&default_port) {
            self.port_allocator.mark_allocated(default_port);
        } else if self.port_allocator.try_allocate_port(default_port).is_err() {
            return None;
        }
        tracing::info!(
            "Using config default port {} (supersedes cached port {})",
            default_port,
            cached_port
        );
        Some((default_port, PortResolutionReason::DefaultAvailable))
    }

    /// Allocate a fresh port from the config default or a random one.
    ///
    /// Called when the port store has no cached value for this parameter.
    fn allocate_fresh_port(
        &mut self,
        param: &crate::config::Parameter,
        param_name: &str,
    ) -> Result<(u16, PortResolutionReason)> {
        // Isolated scopes (and --randomize) must not allocate the config
        // default: well-known ports defeat the point of isolation — a fresh
        // parameter would collide with the non-isolated stack or another
        // workspace. Cached ports are handled before we get here.
        if self.force_random_ports || !self.prefer_config_defaults {
            return Ok((
                self.port_allocator.allocate_random_port()?,
                PortResolutionReason::Random,
            ));
        }

        if let Some(env_value) = param.get_value_for_environment(&self.environment) {
            let default_str = Self::value_to_string(env_value);
            if let Ok(default_port) = default_str.parse::<u16>() {
                if self.managed_ports.contains(&default_port) {
                    self.port_allocator.mark_allocated(default_port);
                    Ok((default_port, PortResolutionReason::DefaultAvailable))
                } else if self.port_allocator.try_allocate_port(default_port).is_ok() {
                    Ok((default_port, PortResolutionReason::DefaultAvailable))
                } else {
                    let (p, conflict) =
                        self.handle_port_conflict_interactive(default_port, param_name)?;
                    let first = conflict.as_ref().and_then(|c| c.processes.first());
                    Ok((
                        p,
                        PortResolutionReason::ConflictAutoResolved {
                            default_port,
                            conflict_pid: first.map(|p| p.pid),
                            conflict_process: first.map(|p| p.name.clone()),
                        },
                    ))
                }
            } else {
                Ok((
                    self.port_allocator.allocate_random_port()?,
                    PortResolutionReason::Random,
                ))
            }
        } else {
            Ok((
                self.port_allocator.allocate_random_port()?,
                PortResolutionReason::Random,
            ))
        }
    }

    /// Handle port conflict with interactive prompt or error.
    ///
    /// Returns `(resolved_port, Option<PortConflict>)` — the conflict is `Some` when the
    /// port was reassigned due to a conflict, carrying pid/process info for display.
    fn handle_port_conflict_interactive(
        &mut self,
        port: u16,
        param_name: &str,
    ) -> Result<(u16, Option<PortConflict>)> {
        // Check for conflict
        let Some(conflict) = PortConflict::check(port) else {
            // Port is actually available, just allocate it
            return Ok((port, None));
        };

        // Allocate alternative port (we may need it as fallback)
        let alternative_port = self.port_allocator.allocate_random_port()?;

        // In replace mode (--replace flag), kill blocking processes and use original port
        if self.replace_mode {
            match conflict.free_port() {
                Ok(msg) => {
                    tracing::info!(
                        "Port {} ({}) was in use, freed it: {}",
                        port,
                        param_name,
                        msg
                    );
                    // Try to allocate the original port now that it's free
                    match self.port_allocator.try_allocate_port(port) {
                        Ok(_) => return Ok((port, None)),
                        Err(_) => {
                            // Something else grabbed it, fall through to alternative
                            tracing::warn!(
                                "Port {} freed but couldn't allocate, using {}",
                                port,
                                alternative_port
                            );
                        }
                    }
                }
                Err(e) => {
                    tracing::warn!(
                        "Failed to free port {} ({}): {}, using {}",
                        port,
                        param_name,
                        e,
                        alternative_port
                    );
                }
            }
            return Ok((alternative_port, Some(conflict)));
        }

        // In auto-resolve mode (e.g., TUI), skip interactive prompt and use alternative port
        if self.auto_resolve_conflicts {
            tracing::info!(
                "Port {} ({}) is in use, auto-resolving to {}",
                port,
                param_name,
                alternative_port
            );
            return Ok((alternative_port, Some(conflict)));
        }

        // Handle conflict (interactive or error)
        match handle_port_conflict(port, param_name, alternative_port, &conflict)? {
            PortConflictAction::KillAndRetry => {
                // Kill all blocking processes and verify with retries
                if let Err(e) = conflict.kill_and_verify(3) {
                    return Err(Error::Process(e));
                }
                // Try to allocate the original port again (dual-stack: checks both IPv4 and 0.0.0.0)
                match self.port_allocator.try_allocate_port(port) {
                    Ok(_) => Ok((port, None)),
                    Err(_) => Ok((alternative_port, Some(conflict))),
                }
            }
            PortConflictAction::Retry => {
                // Try to allocate the original port again (dual-stack: checks both IPv4 and 0.0.0.0)
                match self.port_allocator.try_allocate_port(port) {
                    Ok(_) => Ok((port, None)),
                    Err(_) => Ok((alternative_port, Some(conflict))),
                }
            }
            PortConflictAction::Ignore => {
                // Use alternative port
                Ok((alternative_port, Some(conflict)))
            }
            PortConflictAction::Abort => Err(Error::Aborted),
        }
    }

    /// Cleanup all resources
    pub fn cleanup(&mut self) {
        self.port_allocator.release_all();
    }

    /// Cleanup resources that can be cleaned with &self.
    ///
    /// This is useful when called from contexts that only have shared access.
    /// Note: This only releases port listeners, not the allocated_ports set.
    pub fn cleanup_shared(&self) {
        self.port_allocator.release_listeners_for_cleanup();
    }
}

impl Default for Resolver {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
#[allow(clippy::field_reassign_with_default)]
mod tests {
    use super::*;

    #[test]
    fn test_resolve_template() {
        let resolver = Resolver::new();
        let mut params = HashMap::new();
        params.insert("PORT".to_string(), "8080".to_string());
        params.insert("HOST".to_string(), "localhost".to_string());

        let result = resolver
            .resolve_template("http://{{HOST}}:{{PORT}}/api", &params)
            .unwrap();

        assert_eq!(result, "http://localhost:8080/api");
    }

    #[test]
    fn test_resolve_template_missing_param() {
        let resolver = Resolver::new();
        let params = HashMap::new();

        let result = resolver.resolve_template("{{MISSING}}", &params);

        assert!(matches!(result, Err(Error::ParameterNotFound(_))));
    }

    #[test]
    fn test_extract_template_variables() {
        let resolver = Resolver::new();
        let vars = resolver.extract_template_variables("{{FOO}} and {{BAR}} and {{FOO}}");

        assert!(vars.contains(&"FOO".to_string()));
        assert!(vars.contains(&"BAR".to_string()));
    }

    #[test]
    fn test_port_allocation_with_default_available() {
        use crate::config::{Config, Parameter};

        // Use a fixed high port that's unlikely to be in use
        // Note: There's an inherent race window between checking availability and
        // the resolver allocating, so we verify behavior rather than exact port
        let default_port: u16 = 59123;

        let mut resolver = Resolver::new();
        let mut config = Config::default();

        // Create a port parameter with a default
        let param = Parameter {
            development: None,
            develop: None,
            staging: None,
            production: None,
            param_type: Some("port".to_string()),
            default: Some(serde_yaml::Value::Number(default_port.into())),
            either: vec![],
            source: None,
            description: None,
            optional: None,
            ..Default::default()
        };

        config.parameters.insert("API_PORT".to_string(), param);

        resolver.resolve_parameters(&mut config).unwrap();

        let resolved = resolver.get_resolved_parameters();
        let port_str = resolved.get("API_PORT").unwrap();
        let port: u16 = port_str.parse().unwrap();

        // Should allocate a valid port - either the default or a fallback
        // The exact port depends on system state (race condition avoidance)
        assert!(port > 0, "Should allocate a valid port");

        // If default was available, it should be used; otherwise fallback
        // We can't assert the exact value due to race conditions with other tests
        if port != default_port {
            // Fallback was used - still a valid outcome
            assert!(port != 0, "Fallback port should be valid");
        }
    }

    #[test]
    fn test_port_allocation_with_default_in_use() {
        use crate::config::{Config, Parameter};

        // NoopPortStore is the default — forces fresh allocation (no cache hits)
        let mut resolver = Resolver::new();
        let mut config = Config::default();

        // Occupy a port
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let occupied_port = listener.local_addr().unwrap().port();

        // Create a port parameter with the occupied port as default
        let param = Parameter {
            development: None,
            develop: None,
            staging: None,
            production: None,
            param_type: Some("port".to_string()),
            default: Some(serde_yaml::Value::Number(occupied_port.into())),
            either: vec![],
            source: None,
            description: None,
            optional: None,
            ..Default::default()
        };

        config.parameters.insert("API_PORT".to_string(), param);

        resolver.resolve_parameters(&mut config).unwrap();

        let resolved = resolver.get_resolved_parameters();
        let port_str = resolved.get("API_PORT").unwrap();
        let port: u16 = port_str.parse().unwrap();

        // Should have allocated a different port (fallback to random)
        assert_ne!(port, occupied_port);
        assert!(port > 0);

        drop(listener);
    }

    #[test]
    fn test_port_allocation_without_default() {
        use crate::config::{Config, Parameter};

        let mut resolver = Resolver::new();
        let mut config = Config::default();

        // Create a port parameter without default
        let param = Parameter {
            development: None,
            develop: None,
            staging: None,
            production: None,
            param_type: Some("port".to_string()),
            default: None,
            either: vec![],
            source: None,
            description: None,
            optional: None,
            ..Default::default()
        };

        config.parameters.insert("API_PORT".to_string(), param);

        resolver.resolve_parameters(&mut config).unwrap();

        let resolved = resolver.get_resolved_parameters();
        let port_str = resolved.get("API_PORT").unwrap();
        let port: u16 = port_str.parse().unwrap();

        // Should have allocated a random port
        assert!(port > 0);
    }

    #[test]
    fn test_force_random_ports_ignores_defaults() {
        use crate::config::{Config, Parameter};

        let mut resolver = Resolver::new();
        resolver.set_force_random_ports(true);

        let mut config = Config::default();
        config.parameters.insert(
            "API_PORT".to_string(),
            Parameter {
                development: None,
                develop: None,
                staging: None,
                production: None,
                param_type: Some("port".to_string()),
                default: Some(serde_yaml::Value::Number(18380.into())),
                either: vec![],
                source: None,
                description: None,
                optional: None,
                ..Default::default()
            },
        );
        config.parameters.insert(
            "DB_PORT".to_string(),
            Parameter {
                development: None,
                develop: None,
                staging: None,
                production: None,
                param_type: Some("port".to_string()),
                default: Some(serde_yaml::Value::Number(15732.into())),
                either: vec![],
                source: None,
                description: None,
                optional: None,
                ..Default::default()
            },
        );

        resolver.resolve_parameters(&mut config).unwrap();
        let resolved = resolver.get_resolved_parameters();
        let api_port: u16 = resolved.get("API_PORT").unwrap().parse().unwrap();
        let db_port: u16 = resolved.get("DB_PORT").unwrap().parse().unwrap();

        assert_ne!(api_port, 18380);
        assert_ne!(db_port, 15732);
        assert_ne!(api_port, db_port);
    }

    /// Helper: a `type: port` parameter with the given default.
    fn port_param_with_default(port: u16) -> crate::config::Parameter {
        crate::config::Parameter {
            param_type: Some("port".to_string()),
            default: Some(serde_yaml::Value::Number(port.into())),
            ..Default::default()
        }
    }

    #[test]
    fn test_config_default_supersedes_cached_port() {
        use crate::config::Config;

        let mut resolver = Resolver::new();
        resolver.set_prefer_config_defaults(true);

        // Cache holds a stale port; config default is different and available
        let mut cached = HashMap::new();
        cached.insert("API_PORT".to_string(), 59911u16);
        resolver.set_port_store(Box::new(crate::port::SqlitePortStore::new(cached)));

        let mut config = Config::default();
        config
            .parameters
            .insert("API_PORT".to_string(), port_param_with_default(59912));

        resolver.resolve_parameters(&mut config).unwrap();
        let port: u16 = resolver
            .get_resolved_parameters()
            .get("API_PORT")
            .unwrap()
            .parse()
            .unwrap();

        assert_eq!(
            port, 59912,
            "available config default should supersede cached port"
        );
    }

    #[test]
    fn test_cached_port_wins_when_defaults_not_preferred() {
        use crate::config::Config;

        let mut resolver = Resolver::new();
        resolver.set_prefer_config_defaults(false); // isolated-mode behavior

        let mut cached = HashMap::new();
        cached.insert("API_PORT".to_string(), 59913u16);
        resolver.set_port_store(Box::new(crate::port::SqlitePortStore::new(cached)));

        let mut config = Config::default();
        config
            .parameters
            .insert("API_PORT".to_string(), port_param_with_default(59914));

        resolver.resolve_parameters(&mut config).unwrap();
        let port: u16 = resolver
            .get_resolved_parameters()
            .get("API_PORT")
            .unwrap()
            .parse()
            .unwrap();

        assert_eq!(
            port, 59913,
            "cached port should win when defaults are not preferred (isolated mode)"
        );
    }

    #[test]
    fn test_fresh_port_is_random_when_defaults_not_preferred() {
        use crate::config::Config;

        let mut resolver = Resolver::new();
        resolver.set_prefer_config_defaults(false); // isolated-mode behavior
        // No cached port in the store — this is a fresh allocation.
        resolver.set_port_store(Box::new(crate::port::SqlitePortStore::new(HashMap::new())));

        // Hold the default port open so allocating it would succeed if tried.
        let default_port = 59915u16;
        let _listener = std::net::TcpListener::bind(("127.0.0.1", 0)).unwrap();

        let mut config = Config::default();
        config.parameters.insert(
            "API_PORT".to_string(),
            port_param_with_default(default_port),
        );

        resolver.resolve_parameters(&mut config).unwrap();
        let port: u16 = resolver
            .get_resolved_parameters()
            .get("API_PORT")
            .unwrap()
            .parse()
            .unwrap();

        assert_ne!(
            port, default_port,
            "fresh allocation in isolated mode must not use the well-known config default"
        );
    }

    #[test]
    fn test_cached_port_kept_when_default_busy() {
        use crate::config::Config;

        let mut resolver = Resolver::new();
        resolver.set_prefer_config_defaults(true);

        // Occupy the default port so reclaiming fails
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let busy_default = listener.local_addr().unwrap().port();

        let mut cached = HashMap::new();
        cached.insert("API_PORT".to_string(), 59915u16);
        resolver.set_port_store(Box::new(crate::port::SqlitePortStore::new(cached)));

        let mut config = Config::default();
        config.parameters.insert(
            "API_PORT".to_string(),
            port_param_with_default(busy_default),
        );

        resolver.resolve_parameters(&mut config).unwrap();
        let port: u16 = resolver
            .get_resolved_parameters()
            .get("API_PORT")
            .unwrap()
            .parse()
            .unwrap();

        assert_eq!(
            port, 59915,
            "cached port should be kept when the config default is busy"
        );
        drop(listener);
    }

    #[test]
    fn test_port_allocation_with_invalid_default() {
        use crate::config::{Config, Parameter};

        let mut resolver = Resolver::new();
        let mut config = Config::default();

        // Create a port parameter with an invalid default
        let param = Parameter {
            development: None,
            develop: None,
            staging: None,
            production: None,
            param_type: Some("port".to_string()),
            default: Some(serde_yaml::Value::String("not-a-port".to_string())),
            either: vec![],
            source: None,
            description: None,
            optional: None,
            ..Default::default()
        };

        config.parameters.insert("API_PORT".to_string(), param);

        // An unparseable default is a config error, not a silent random port.
        let err = resolver.resolve_parameters(&mut config).unwrap_err();
        assert!(err.to_string().contains("invalid port default"), "{err}");
    }

    #[test]
    fn test_multiple_port_allocations_with_defaults() {
        use crate::config::{Config, Parameter};

        let mut resolver = Resolver::new();
        let mut config = Config::default();

        // Find two available ports
        let listener1 = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port1 = listener1.local_addr().unwrap().port();
        drop(listener1);

        let listener2 = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port2 = listener2.local_addr().unwrap().port();
        drop(listener2);

        // Create two port parameters with defaults
        let param1 = Parameter {
            development: None,
            develop: None,
            staging: None,
            production: None,
            param_type: Some("port".to_string()),
            default: Some(serde_yaml::Value::Number(port1.into())),
            either: vec![],
            source: None,
            description: None,
            optional: None,
            ..Default::default()
        };

        let param2 = Parameter {
            development: None,
            develop: None,
            staging: None,
            production: None,
            param_type: Some("port".to_string()),
            default: Some(serde_yaml::Value::Number(port2.into())),
            either: vec![],
            source: None,
            description: None,
            optional: None,
            ..Default::default()
        };

        config.parameters.insert("API_PORT".to_string(), param1);
        config.parameters.insert("DB_PORT".to_string(), param2);

        resolver.resolve_parameters(&mut config).unwrap();

        let resolved = resolver.get_resolved_parameters();
        let api_port: u16 = resolved.get("API_PORT").unwrap().parse().unwrap();
        let db_port: u16 = resolved.get("DB_PORT").unwrap().parse().unwrap();

        // Both should have gotten their default ports
        assert_eq!(api_port, port1);
        assert_eq!(db_port, port2);
        assert_ne!(api_port, db_port);
    }

    #[test]
    fn test_port_validation_with_user_value() {
        use crate::config::{Config, Parameter};

        let mut resolver = Resolver::new();
        let mut config = Config::default();

        // Create a port parameter with a valid user-provided value
        let mut param = Parameter {
            development: None,
            develop: None,
            staging: None,
            production: None,
            param_type: Some("port".to_string()),
            default: None,
            either: vec![],
            source: None,
            description: None,
            optional: None,
            ..Default::default()
        };
        param.value = Some("8080".to_string());

        config.parameters.insert("API_PORT".to_string(), param);

        resolver.resolve_parameters(&mut config).unwrap();

        let resolved = resolver.get_resolved_parameters();
        assert_eq!(resolved.get("API_PORT").unwrap(), "8080");
    }

    #[test]
    fn test_port_validation_rejects_invalid_string() {
        use crate::config::{Config, Parameter};

        let mut resolver = Resolver::new();
        let mut config = Config::default();

        // Create a port parameter with invalid string value
        let mut param = Parameter {
            development: None,
            develop: None,
            staging: None,
            production: None,
            param_type: Some("port".to_string()),
            default: None,
            either: vec![],
            source: None,
            description: None,
            optional: None,
            ..Default::default()
        };
        param.value = Some("invalid".to_string());

        config.parameters.insert("API_PORT".to_string(), param);

        let result = resolver.resolve_parameters(&mut config);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("invalid port value")
        );
    }

    #[test]
    fn test_port_validation_rejects_zero() {
        use crate::config::{Config, Parameter};

        let mut resolver = Resolver::new();
        let mut config = Config::default();

        // Create a port parameter with zero value
        let mut param = Parameter {
            development: None,
            develop: None,
            staging: None,
            production: None,
            param_type: Some("port".to_string()),
            default: None,
            either: vec![],
            source: None,
            description: None,
            optional: None,
            ..Default::default()
        };
        param.value = Some("0".to_string());

        config.parameters.insert("API_PORT".to_string(), param);

        let result = resolver.resolve_parameters(&mut config);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("invalid port value '0'")
        );
    }

    #[test]
    fn test_port_validation_rejects_out_of_range() {
        use crate::config::{Config, Parameter};

        let mut resolver = Resolver::new();
        let mut config = Config::default();

        // Create a port parameter with out-of-range value
        let mut param = Parameter {
            development: None,
            develop: None,
            staging: None,
            production: None,
            param_type: Some("port".to_string()),
            default: None,
            either: vec![],
            source: None,
            description: None,
            optional: None,
            ..Default::default()
        };
        param.value = Some("99999".to_string());

        config.parameters.insert("API_PORT".to_string(), param);

        let result = resolver.resolve_parameters(&mut config);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("invalid port value")
        );
    }

    #[test]
    fn test_non_port_parameter_accepts_any_value() {
        use crate::config::{Config, Parameter};

        let mut resolver = Resolver::new();
        let mut config = Config::default();

        // Create a non-port parameter with "invalid" value (which is valid for non-ports)
        let mut param = Parameter {
            development: None,
            develop: None,
            staging: None,
            production: None,
            param_type: Some("string".to_string()),
            default: None,
            either: vec![],
            source: None,
            description: None,
            optional: None,
            ..Default::default()
        };
        param.value = Some("invalid".to_string());

        config.parameters.insert("SOME_PARAM".to_string(), param);

        resolver.resolve_parameters(&mut config).unwrap();

        let resolved = resolver.get_resolved_parameters();
        assert_eq!(resolved.get("SOME_PARAM").unwrap(), "invalid");
    }

    #[test]
    fn test_circular_parameter_detection_simple() {
        use crate::config::{Config, Parameter};

        let mut resolver = Resolver::new();
        let mut config = Config::default();

        // Create circular reference: A -> B -> A
        let param_a = Parameter {
            development: None,
            develop: None,
            staging: None,
            production: None,
            param_type: None,
            default: Some(serde_yaml::Value::String("{{B}}".to_string())),
            either: vec![],
            source: None,
            description: None,
            optional: None,
            ..Default::default()
        };

        let param_b = Parameter {
            development: None,
            develop: None,
            staging: None,
            production: None,
            param_type: None,
            default: Some(serde_yaml::Value::String("{{A}}".to_string())),
            either: vec![],
            source: None,
            description: None,
            optional: None,
            ..Default::default()
        };

        config.parameters.insert("A".to_string(), param_a);
        config.parameters.insert("B".to_string(), param_b);

        let result = resolver.resolve_parameters(&mut config);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("Circular parameter reference")
                || err.contains("unresolved template variables")
        );
    }

    #[test]
    fn test_circular_parameter_detection_complex() {
        use crate::config::{Config, Parameter};

        let mut resolver = Resolver::new();
        let mut config = Config::default();

        // Create circular reference: A -> B -> C -> A
        let param_a = Parameter {
            development: None,
            develop: None,
            staging: None,
            production: None,
            param_type: None,
            default: Some(serde_yaml::Value::String("{{B}}".to_string())),
            either: vec![],
            source: None,
            description: None,
            optional: None,
            ..Default::default()
        };

        let param_b = Parameter {
            development: None,
            develop: None,
            staging: None,
            production: None,
            param_type: None,
            default: Some(serde_yaml::Value::String("{{C}}".to_string())),
            either: vec![],
            source: None,
            description: None,
            optional: None,
            ..Default::default()
        };

        let param_c = Parameter {
            development: None,
            develop: None,
            staging: None,
            production: None,
            param_type: None,
            default: Some(serde_yaml::Value::String("{{A}}".to_string())),
            either: vec![],
            source: None,
            description: None,
            optional: None,
            ..Default::default()
        };

        config.parameters.insert("A".to_string(), param_a);
        config.parameters.insert("B".to_string(), param_b);
        config.parameters.insert("C".to_string(), param_c);

        let result = resolver.resolve_parameters(&mut config);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("Circular parameter reference")
                || err.contains("unresolved template variables")
        );
    }

    #[test]
    fn test_valid_parameter_chain() {
        use crate::config::{Config, Parameter};

        let mut resolver = Resolver::new();
        let mut config = Config::default();

        // Create valid chain: A -> B -> C (no cycle)
        let param_c = Parameter {
            development: None,
            develop: None,
            staging: None,
            production: None,
            param_type: None,
            default: Some(serde_yaml::Value::String("value_c".to_string())),
            either: vec![],
            source: None,
            description: None,
            optional: None,
            ..Default::default()
        };

        let param_b = Parameter {
            development: None,
            develop: None,
            staging: None,
            production: None,
            param_type: None,
            default: Some(serde_yaml::Value::String("{{C}}".to_string())),
            either: vec![],
            source: None,
            description: None,
            optional: None,
            ..Default::default()
        };

        let param_a = Parameter {
            development: None,
            develop: None,
            staging: None,
            production: None,
            param_type: None,
            default: Some(serde_yaml::Value::String("{{B}}".to_string())),
            either: vec![],
            source: None,
            description: None,
            optional: None,
            ..Default::default()
        };

        config.parameters.insert("A".to_string(), param_a);
        config.parameters.insert("B".to_string(), param_b);
        config.parameters.insert("C".to_string(), param_c);

        resolver.resolve_parameters(&mut config).unwrap();

        let resolved = resolver.get_resolved_parameters();
        assert_eq!(resolved.get("A").unwrap(), "value_c");
        assert_eq!(resolved.get("B").unwrap(), "value_c");
        assert_eq!(resolved.get("C").unwrap(), "value_c");
    }

    #[test]
    fn test_missing_parameter_reference() {
        use crate::config::{Config, Parameter};

        let mut resolver = Resolver::new();
        let mut config = Config::default();

        // Create parameter that references non-existent parameter
        let param_a = Parameter {
            development: None,
            develop: None,
            staging: None,
            production: None,
            param_type: None,
            default: Some(serde_yaml::Value::String("{{MISSING}}".to_string())),
            either: vec![],
            source: None,
            description: None,
            optional: None,
            ..Default::default()
        };

        config.parameters.insert("A".to_string(), param_a);

        let result = resolver.resolve_parameters(&mut config);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("unresolved template variables") || err.contains("MISSING"));
    }

    #[test]
    fn test_either_constraint_valid() {
        use crate::config::{Config, Parameter};

        let mut resolver = Resolver::new();
        let mut config = Config::default();

        // Create parameter with either constraint and valid default
        let param = Parameter {
            development: None,
            develop: None,
            staging: None,
            production: None,
            param_type: None,
            default: Some(serde_yaml::Value::String("dev".to_string())),
            either: vec!["dev".to_string(), "staging".to_string(), "prod".to_string()],
            source: None,
            description: None,
            optional: None,
            ..Default::default()
        };

        config.parameters.insert("ENV".to_string(), param);

        resolver.resolve_parameters(&mut config).unwrap();

        let resolved = resolver.get_resolved_parameters();
        assert_eq!(resolved.get("ENV").unwrap(), "dev");
    }

    #[test]
    fn test_either_constraint_invalid() {
        use crate::config::{Config, Parameter};

        let mut resolver = Resolver::new();
        let mut config = Config::default();

        // Create parameter with either constraint and invalid default
        let param = Parameter {
            development: None,
            develop: None,
            staging: None,
            production: None,
            param_type: None,
            default: Some(serde_yaml::Value::String("invalid".to_string())),
            either: vec!["dev".to_string(), "staging".to_string(), "prod".to_string()],
            source: None,
            description: None,
            optional: None,
            ..Default::default()
        };

        config.parameters.insert("ENV".to_string(), param);

        let result = resolver.resolve_parameters(&mut config);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("not in the allowed values"));
        assert!(err.contains("invalid"));
    }

    #[test]
    fn test_either_constraint_with_user_value() {
        use crate::config::{Config, Parameter};

        let mut resolver = Resolver::new();
        let mut config = Config::default();

        // Create parameter with either constraint and user-provided value
        let mut param = Parameter {
            development: None,
            develop: None,
            staging: None,
            production: None,
            param_type: None,
            default: None,
            either: vec!["dev".to_string(), "staging".to_string(), "prod".to_string()],
            source: None,
            description: None,
            optional: None,
            ..Default::default()
        };
        param.value = Some("prod".to_string());

        config.parameters.insert("ENV".to_string(), param);

        resolver.resolve_parameters(&mut config).unwrap();

        let resolved = resolver.get_resolved_parameters();
        assert_eq!(resolved.get("ENV").unwrap(), "prod");
    }

    #[test]
    fn test_either_constraint_with_invalid_user_value() {
        use crate::config::{Config, Parameter};

        let mut resolver = Resolver::new();
        let mut config = Config::default();

        // Create parameter with either constraint and invalid user-provided value
        let mut param = Parameter {
            development: None,
            develop: None,
            staging: None,
            production: None,
            param_type: None,
            default: None,
            either: vec!["dev".to_string(), "staging".to_string(), "prod".to_string()],
            source: None,
            description: None,
            optional: None,
            ..Default::default()
        };
        param.value = Some("test".to_string());

        config.parameters.insert("ENV".to_string(), param);

        let result = resolver.resolve_parameters(&mut config);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("not in the allowed values"));
        assert!(err.contains("test"));
    }

    #[test]
    fn test_either_constraint_with_template() {
        use crate::config::{Config, Parameter};

        let mut resolver = Resolver::new();
        let mut config = Config::default();

        // Create base parameter
        let base_param = Parameter {
            development: None,
            develop: None,
            staging: None,
            production: None,
            param_type: None,
            default: Some(serde_yaml::Value::String("staging".to_string())),
            either: vec![],
            source: None,
            description: None,
            optional: None,
            ..Default::default()
        };

        // Create parameter with either constraint that references base
        let env_param = Parameter {
            development: None,
            develop: None,
            staging: None,
            production: None,
            param_type: None,
            default: Some(serde_yaml::Value::String("{{BASE}}".to_string())),
            either: vec!["dev".to_string(), "staging".to_string(), "prod".to_string()],
            source: None,
            description: None,
            optional: None,
            ..Default::default()
        };

        config.parameters.insert("BASE".to_string(), base_param);
        config.parameters.insert("ENV".to_string(), env_param);

        resolver.resolve_parameters(&mut config).unwrap();

        let resolved = resolver.get_resolved_parameters();
        assert_eq!(resolved.get("ENV").unwrap(), "staging");
    }

    #[test]
    fn test_shell_escape_simple() {
        let result = shell_escape("hello");
        assert_eq!(result, "hello");
    }

    #[test]
    fn test_shell_escape_with_semicolon() {
        let result = shell_escape("; rm -rf /");
        assert_eq!(result, "'; rm -rf /'");
    }

    #[test]
    fn test_shell_escape_with_pipe() {
        let result = shell_escape("foo | bar");
        assert_eq!(result, "'foo | bar'");
    }

    #[test]
    fn test_shell_escape_with_quotes() {
        let result = shell_escape("it's");
        // Single quote ' is escaped as '\'' in the middle of the string
        // "it's" becomes 'it'\''s'
        assert_eq!(result, "'it'\\''s'");
    }

    #[test]
    fn test_shell_escape_empty() {
        let result = shell_escape("");
        assert_eq!(result, "''");
    }

    #[test]
    fn test_shell_escape_safe_characters() {
        let result = shell_escape("hello_world-123.txt");
        assert_eq!(result, "hello_world-123.txt");
    }

    #[test]
    fn test_shell_escape_path_with_slash() {
        // '/' should be quoted now (security hardening)
        let result = shell_escape("/path/to/file");
        assert_eq!(result, "'/path/to/file'");
    }

    #[test]
    fn test_shell_escape_with_colon() {
        // ':' should be quoted now (security hardening)
        let result = shell_escape("host:port");
        assert_eq!(result, "'host:port'");
    }

    #[test]
    fn test_resolve_template_shell_safe() {
        use crate::config::{Config, Parameter, Service};

        let mut resolver = Resolver::new();
        let mut config = Config::default();

        // Create parameter with dangerous value
        let mut param = Parameter {
            development: None,
            develop: None,
            staging: None,
            production: None,
            param_type: None,
            default: None,
            either: vec![],
            source: None,
            description: None,
            optional: None,
            ..Default::default()
        };
        param.value = Some("; rm -rf /".to_string());

        config.parameters.insert("USER_INPUT".to_string(), param);

        // Create service with process command that uses the parameter
        let service = Service {
            process: Some("echo {{USER_INPUT}}".to_string()),
            ..Default::default()
        };

        config.services.insert("test".to_string(), service);

        resolver.resolve_parameters(&mut config).unwrap();
        let resolved_config = resolver.resolve_config(&config).unwrap();

        // The dangerous parameter should be escaped
        let resolved_service = resolved_config.services.get("test").unwrap();
        let process = resolved_service.process.as_ref().unwrap();

        // Should be escaped to prevent command injection
        assert_eq!(process, "echo '; rm -rf /'");
        // Should NOT be the unescaped dangerous version
        assert_ne!(process, "echo ; rm -rf /");
    }

    #[test]
    fn test_resolve_template_no_double_expansion() {
        // A value that literally contains another placeholder must be inserted
        // verbatim, not re-expanded — re-expansion after escaping would break
        // out of the quoting and defeat shell_escape entirely.
        let mut params = HashMap::new();
        params.insert("A".to_string(), "{{B}}".to_string());
        params.insert("B".to_string(), "x; rm -rf ~".to_string());

        let shell = Resolver::replace_placeholders("run {{A}} {{B}}", &params, true).unwrap();
        assert_eq!(shell, "run '{{B}}' 'x; rm -rf ~'");

        let plain = Resolver::resolve_template_static("run {{A}} {{B}}", &params).unwrap();
        assert_eq!(plain, "run {{B}} x; rm -rf ~");
    }

    #[test]
    fn test_resolve_template_trims_placeholder_names() {
        // generate_dependencies trims captured names; resolution must agree so
        // `{{ FOO }}` doesn't pass DAG validation and then fail at runtime.
        let mut params = HashMap::new();
        params.insert("FOO".to_string(), "bar".to_string());

        let result = Resolver::resolve_template_static("v={{ FOO }}", &params).unwrap();
        assert_eq!(result, "v=bar");
    }

    #[test]
    fn test_resolve_template_missing_parameter_errors() {
        let params = HashMap::new();
        let err = Resolver::resolve_template_static("{{MISSING}}", &params).unwrap_err();
        assert!(matches!(err, Error::ParameterNotFound(name) if name == "MISSING"));
    }

    #[test]
    fn test_port_default_zero_rejected() {
        use crate::config::{Config, Parameter};

        let mut resolver = Resolver::new();
        let mut config = Config::default();
        let param = Parameter {
            param_type: Some("port".to_string()),
            default: Some(serde_yaml::Value::Number(0.into())),
            ..Default::default()
        };
        config.parameters.insert("PORT".to_string(), param);

        let err = resolver.resolve_parameters(&mut config).unwrap_err();
        assert!(
            err.to_string().contains("invalid port default '0'"),
            "{err}"
        );
    }

    #[test]
    fn test_port_default_template_rejected() {
        use crate::config::{Config, Parameter};

        let mut resolver = Resolver::new();
        let mut config = Config::default();
        config.parameters.insert(
            "BASE_PORT".to_string(),
            Parameter {
                default: Some(serde_yaml::Value::Number(3000.into())),
                ..Default::default()
            },
        );
        config.parameters.insert(
            "API_PORT".to_string(),
            Parameter {
                param_type: Some("port".to_string()),
                default: Some(serde_yaml::Value::String("{{BASE_PORT}}".to_string())),
                ..Default::default()
            },
        );

        let err = resolver.resolve_parameters(&mut config).unwrap_err();
        assert!(err.to_string().contains("invalid port default"), "{err}");
    }

    #[test]
    fn test_port_default_out_of_range_rejected() {
        use crate::config::{Config, Parameter};

        let mut resolver = Resolver::new();
        let mut config = Config::default();
        config.parameters.insert(
            "PORT".to_string(),
            Parameter {
                param_type: Some("port".to_string()),
                default: Some(serde_yaml::Value::Number(70000.into())),
                ..Default::default()
            },
        );

        let err = resolver.resolve_parameters(&mut config).unwrap_err();
        assert!(err.to_string().contains("invalid port default"), "{err}");
    }

    #[test]
    fn test_try_allocate_port_zero_fails() {
        let mut allocator = PortAllocator::new();
        assert!(allocator.try_allocate_port(0).is_err());
    }

    #[test]
    fn test_env_file_sets_parameter_value() {
        use crate::config::{Config, Parameter};
        use std::fs;
        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();
        let env_path = temp_dir.path().join(".env");
        fs::write(&env_path, "MY_PARAM=from_env_file\n").unwrap();

        let mut resolver = Resolver::new();
        resolver.set_work_dir(temp_dir.path());

        let mut config = Config::default();

        // Declare the parameter (must exist for .env file to work)
        let param = Parameter {
            development: None,
            develop: None,
            staging: None,
            production: None,
            param_type: None,
            default: Some(serde_yaml::Value::String("default_value".to_string())),
            either: vec![],
            source: None,
            description: None,
            optional: None,
            ..Default::default()
        };

        config.parameters.insert("MY_PARAM".to_string(), param);
        config.env_file = vec![".env".to_string()];

        resolver.resolve_parameters(&mut config).unwrap();

        let resolved = resolver.get_resolved_parameters();
        // .env file value should override the default
        assert_eq!(resolved.get("MY_PARAM").unwrap(), "from_env_file");
    }

    #[test]
    fn test_env_file_respects_explicit_value() {
        use crate::config::{Config, Parameter};
        use std::fs;
        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();
        let env_path = temp_dir.path().join(".env");
        fs::write(&env_path, "MY_PARAM=from_env_file\n").unwrap();

        let mut resolver = Resolver::new();
        resolver.set_work_dir(temp_dir.path());

        let mut config = Config::default();

        // Declare the parameter with an explicit value already set
        let param = Parameter {
            development: None,
            develop: None,
            staging: None,
            production: None,
            param_type: None,
            default: Some(serde_yaml::Value::String("default_value".to_string())),
            either: vec![],
            source: None,
            description: None,
            optional: None,
            generate: None,
            value: Some("explicit_value".to_string()), // Explicit value takes precedence
        };

        config.parameters.insert("MY_PARAM".to_string(), param);
        config.env_file = vec![".env".to_string()];

        resolver.resolve_parameters(&mut config).unwrap();

        let resolved = resolver.get_resolved_parameters();
        // Explicit value should NOT be overridden by .env file
        assert_eq!(resolved.get("MY_PARAM").unwrap(), "explicit_value");
    }

    #[test]
    fn test_env_file_rejects_undeclared_variable() {
        use crate::config::Config;
        use std::fs;
        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();
        let env_path = temp_dir.path().join(".env");
        // This variable is NOT declared in parameters - should error
        fs::write(&env_path, "UNDECLARED_VAR=some_value\n").unwrap();

        let mut resolver = Resolver::new();
        resolver.set_work_dir(temp_dir.path());

        let mut config = Config::default();
        config.env_file = vec![".env".to_string()];

        let result = resolver.resolve_parameters(&mut config);

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("UNDECLARED_VAR"));
        assert!(err.to_string().contains("undeclared parameter"));
    }

    #[test]
    fn test_env_file_works_with_service_environment() {
        use crate::config::{Config, Parameter, Service};
        use std::fs;
        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();
        let env_path = temp_dir.path().join(".env");
        fs::write(&env_path, "API_KEY=secret123\n").unwrap();

        let mut resolver = Resolver::new();
        resolver.set_work_dir(temp_dir.path());

        let mut config = Config::default();

        // Declare the parameter
        let param = Parameter {
            development: None,
            develop: None,
            staging: None,
            production: None,
            param_type: None,
            default: Some(serde_yaml::Value::String("".to_string())),
            either: vec![],
            source: None,
            description: None,
            optional: None,
            ..Default::default()
        };

        config.parameters.insert("API_KEY".to_string(), param);
        config.env_file = vec![".env".to_string()];

        // Service references parameter in its environment
        let mut env = HashMap::new();
        env.insert("SECRET".to_string(), "{{API_KEY}}".to_string());
        let service = Service {
            process: Some("echo test".to_string()),
            environment: env,
            ..Default::default()
        };

        config.services.insert("api".to_string(), service);

        resolver.resolve_parameters(&mut config).unwrap();
        let resolved_config = resolver.resolve_config(&config).unwrap();

        let resolved_service = resolved_config.services.get("api").unwrap();
        // The service environment should have the value from .env file via the parameter
        assert_eq!(
            resolved_service.environment.get("SECRET").unwrap(),
            "secret123"
        );
    }

    #[test]
    fn test_env_file_empty_file() {
        use crate::config::Config;
        use std::fs;
        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();
        let env_path = temp_dir.path().join(".env");
        // Empty file with only comments
        fs::write(&env_path, "# Just comments\n# No actual variables\n").unwrap();

        let mut resolver = Resolver::new();
        resolver.set_work_dir(temp_dir.path());

        let mut config = Config::default();
        config.env_file = vec![".env".to_string()];

        // Should succeed - empty env file is valid
        resolver.resolve_parameters(&mut config).unwrap();
    }

    #[test]
    fn test_env_file_multiple_files_later_overrides() {
        use crate::config::{Config, Parameter};
        use std::fs;
        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();
        let env1_path = temp_dir.path().join(".env1");
        let env2_path = temp_dir.path().join(".env2");

        fs::write(&env1_path, "MY_PARAM=from_first\n").unwrap();
        fs::write(&env2_path, "MY_PARAM=from_second\n").unwrap();

        let mut resolver = Resolver::new();
        resolver.set_work_dir(temp_dir.path());

        let mut config = Config::default();

        let param = Parameter {
            development: None,
            develop: None,
            staging: None,
            production: None,
            param_type: None,
            default: Some(serde_yaml::Value::String("default".to_string())),
            either: vec![],
            source: None,
            description: None,
            optional: None,
            ..Default::default()
        };

        config.parameters.insert("MY_PARAM".to_string(), param);
        config.env_file = vec![".env1".to_string(), ".env2".to_string()];

        resolver.resolve_parameters(&mut config).unwrap();

        let resolved = resolver.get_resolved_parameters();
        // Second file should win
        assert_eq!(resolved.get("MY_PARAM").unwrap(), "from_second");
    }

    #[test]
    fn test_env_file_undeclared_error_shows_file_name() {
        use crate::config::Config;
        use std::fs;
        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();
        let env_path = temp_dir.path().join(".env.test");

        // Variable is NOT declared in parameters - should error
        fs::write(&env_path, "UNDECLARED=value\n").unwrap();

        let mut resolver = Resolver::new();
        resolver.set_work_dir(temp_dir.path());

        let mut config = Config::default();
        config.env_file = vec![".env.test".to_string()];

        let result = resolver.resolve_parameters(&mut config);
        assert!(result.is_err());

        let err = result.unwrap_err();
        let err_str = err.to_string();
        assert!(err_str.contains("UNDECLARED"));
        // Error should reference the source file
        assert!(err_str.contains(".env.test"));
    }

    // ========================================================================
    // Secret resolution tests
    // ========================================================================

    #[test]
    fn secret_resolved_from_existing_env() {
        use crate::config::{Config, Parameter};
        use std::fs;
        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();
        let env_path = temp_dir.path().join(".env");
        fs::write(&env_path, "SESSION_KEY=existing_secret\n").unwrap();

        let mut resolver = Resolver::new();
        resolver.set_work_dir(temp_dir.path());

        let mut config = Config::default();
        config.parameters.insert(
            "SESSION_KEY".to_string(),
            Parameter {
                param_type: Some("secret".to_string()),
                ..Default::default()
            },
        );
        config.env_file = vec![".env".to_string()];

        resolver.resolve_parameters(&mut config).unwrap();

        let resolved = resolver.get_resolved_parameters();
        assert_eq!(resolved.get("SESSION_KEY").unwrap(), "existing_secret");
    }

    #[test]
    fn missing_manual_secret_errors_with_description() {
        use crate::config::{Config, Parameter};
        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();

        let mut resolver = Resolver::new();
        resolver.set_work_dir(temp_dir.path());

        let mut config = Config::default();
        config.parameters.insert(
            "GITHUB_SECRET".to_string(),
            Parameter {
                param_type: Some("secret".to_string()),
                source: Some("manual".to_string()),
                description: Some("GitHub OAuth client secret".to_string()),
                ..Default::default()
            },
        );

        let err = resolver.resolve_parameters(&mut config).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("GITHUB_SECRET"),
            "Error should name the param: {}",
            msg
        );
        assert!(
            msg.contains("GitHub OAuth client secret"),
            "Error should include description: {}",
            msg
        );
    }

    #[test]
    fn optional_manual_secret_resolves_to_empty_string() {
        use crate::config::{Config, Parameter};
        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();

        let mut resolver = Resolver::new();
        resolver.set_work_dir(temp_dir.path());

        let mut config = Config::default();
        config.parameters.insert(
            "STRIPE_KEY".to_string(),
            Parameter {
                param_type: Some("secret".to_string()),
                source: Some("manual".to_string()),
                optional: Some(true),
                description: Some("Stripe API key".to_string()),
                ..Default::default()
            },
        );

        // Should succeed, not error
        resolver.resolve_parameters(&mut config).unwrap();

        let param = config.parameters.get("STRIPE_KEY").unwrap();
        assert_eq!(param.value.as_deref(), Some(""));
    }

    #[test]
    fn non_optional_manual_secret_still_errors() {
        use crate::config::{Config, Parameter};
        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();

        let mut resolver = Resolver::new();
        resolver.set_work_dir(temp_dir.path());

        let mut config = Config::default();
        config.parameters.insert(
            "REQUIRED_SECRET".to_string(),
            Parameter {
                param_type: Some("secret".to_string()),
                source: Some("manual".to_string()),
                ..Default::default()
            },
        );

        let err = resolver.resolve_parameters(&mut config).unwrap_err();
        assert!(err.to_string().contains("REQUIRED_SECRET"));
    }

    #[test]
    fn gitignore_gate_fires() {
        use crate::config::{Config, Parameter};
        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();
        git2::Repository::init(temp_dir.path()).unwrap();
        // A user-edited .fed/.gitignore that no longer ignores the generated
        // secrets file — the gate must still refuse to write into git's view.
        std::fs::create_dir_all(temp_dir.path().join(".fed")).unwrap();
        std::fs::write(temp_dir.path().join(".fed/.gitignore"), "").unwrap();

        let mut resolver = Resolver::new();
        resolver.set_work_dir(temp_dir.path());

        let mut config = Config::default();
        config.parameters.insert(
            "SESSION_KEY".to_string(),
            Parameter {
                param_type: Some("secret".to_string()),
                ..Default::default()
            },
        );

        let err = resolver.resolve_parameters(&mut config).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains(".gitignore"),
            "Should mention .gitignore: {}",
            msg
        );
        assert!(
            msg.contains("git rm --cached"),
            "Should cover tracked files: {}",
            msg
        );
    }

    #[test]
    fn existing_secrets_file_loaded_on_subsequent_run() {
        use crate::config::{Config, Parameter};
        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();

        // Simulate a previous run that already generated the secrets file at
        // the default location.
        std::fs::create_dir_all(temp_dir.path().join(".fed")).unwrap();
        std::fs::write(
            temp_dir.path().join(".fed/secrets.generated.env"),
            "SESSION_KEY=previously_generated_value\n",
        )
        .unwrap();

        let mut resolver = Resolver::new();
        resolver.set_work_dir(temp_dir.path());

        let mut config = Config::default();
        config.parameters.insert(
            "SESSION_KEY".to_string(),
            Parameter {
                param_type: Some("secret".to_string()),
                ..Default::default()
            },
        );

        resolver.resolve_parameters(&mut config).unwrap();

        let resolved = resolver.get_resolved_parameters();
        assert_eq!(
            resolved.get("SESSION_KEY").unwrap(),
            "previously_generated_value",
            "Should load secret from existing .fed/secrets.generated.env on subsequent runs"
        );
    }

    #[test]
    fn generated_secret_defaults_to_fed_dir() {
        use crate::config::{Config, Parameter};
        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();
        // Git repo with NO root .gitignore — fed's self-managed .fed/.gitignore
        // must make the default location safe on its own.
        git2::Repository::init(temp_dir.path()).unwrap();

        let mut resolver = Resolver::new();
        resolver.set_work_dir(temp_dir.path());

        let mut config = Config::default();
        config.parameters.insert(
            "SESSION_KEY".to_string(),
            Parameter {
                param_type: Some("secret".to_string()),
                ..Default::default()
            },
        );

        resolver.resolve_parameters(&mut config).unwrap();

        let resolved = resolver.get_resolved_parameters();
        assert_eq!(resolved.get("SESSION_KEY").unwrap().len(), 32);

        let generated_env = temp_dir.path().join(".fed/secrets.generated.env");
        let content = std::fs::read_to_string(&generated_env).unwrap();
        assert!(content.contains("SESSION_KEY="));
        assert_eq!(config.env_file[0], ".fed/secrets.generated.env");

        // .fed/.gitignore was self-managed into existence
        let gi = std::fs::read_to_string(temp_dir.path().join(".fed/.gitignore")).unwrap();
        assert!(gi.contains("!cloud.yaml"));
    }

    #[test]
    fn fed_gitignore_not_clobbered_by_resolution() {
        use crate::config::{Config, Parameter};
        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();
        std::fs::create_dir_all(temp_dir.path().join(".fed")).unwrap();
        std::fs::write(
            temp_dir.path().join(".fed/.gitignore"),
            "*\n!cloud.yaml\n!my-notes.md\n!.gitignore\n",
        )
        .unwrap();

        let mut resolver = Resolver::new();
        resolver.set_work_dir(temp_dir.path());

        let mut config = Config::default();
        config.parameters.insert(
            "SESSION_KEY".to_string(),
            Parameter {
                param_type: Some("secret".to_string()),
                ..Default::default()
            },
        );
        resolver.resolve_parameters(&mut config).unwrap();

        let gi = std::fs::read_to_string(temp_dir.path().join(".fed/.gitignore")).unwrap();
        assert!(gi.contains("!my-notes.md"), "user edits must survive");
    }

    #[test]
    fn optional_manual_secret_resolved_from_vault() {
        use crate::config::{Config, Parameter};
        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();

        let mut resolver = Resolver::new();
        resolver.set_work_dir(temp_dir.path());
        resolver.set_test_vault_values(HashMap::from([(
            "STRIPE_KEY".to_string(),
            "sk_test_from_vault".to_string(),
        )]));

        let mut config = Config::default();
        config.parameters.insert(
            "STRIPE_KEY".to_string(),
            Parameter {
                param_type: Some("secret".to_string()),
                source: Some("manual".to_string()),
                optional: Some(true),
                ..Default::default()
            },
        );

        resolver.resolve_parameters(&mut config).unwrap();

        let resolved = resolver.get_resolved_parameters();
        assert_eq!(
            resolved.get("STRIPE_KEY").unwrap(),
            "sk_test_from_vault",
            "Optional manual secret should take the vault value when available"
        );

        // Vault hit is cached in .fed/secrets.cache.env, not the generated file
        let cache = temp_dir.path().join(".fed/secrets.cache.env");
        let content = std::fs::read_to_string(&cache).unwrap();
        assert!(content.contains("STRIPE_KEY=sk_test_from_vault"));
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(&cache).unwrap().permissions().mode();
            assert_eq!(mode & 0o777, 0o600, "cache must be owner-only");
        }
    }

    #[test]
    fn optional_manual_secret_vault_miss_falls_back_to_empty() {
        use crate::config::{Config, Parameter};
        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();

        let mut resolver = Resolver::new();
        resolver.set_work_dir(temp_dir.path());
        // Vault reachable but has no value for this name
        resolver.set_test_vault_values(HashMap::new());

        let mut config = Config::default();
        config.parameters.insert(
            "STRIPE_KEY".to_string(),
            Parameter {
                param_type: Some("secret".to_string()),
                source: Some("manual".to_string()),
                optional: Some(true),
                ..Default::default()
            },
        );

        resolver.resolve_parameters(&mut config).unwrap();
        assert_eq!(
            config
                .parameters
                .get("STRIPE_KEY")
                .unwrap()
                .value
                .as_deref(),
            Some(""),
            "Vault miss on an optional secret must fall back to empty string"
        );
        assert!(
            !temp_dir.path().join(".fed/secrets.cache.env").exists(),
            "Nothing to cache on a vault miss"
        );
    }

    #[test]
    fn required_manual_secret_cached_then_resolved_offline() {
        use crate::config::{Config, Parameter};
        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();

        let make_config = || {
            let mut config = Config::default();
            config.parameters.insert(
                "API_KEY".to_string(),
                Parameter {
                    param_type: Some("secret".to_string()),
                    source: Some("manual".to_string()),
                    ..Default::default()
                },
            );
            config
        };

        // First run: online, vault supplies the value → cached.
        let mut resolver = Resolver::new();
        resolver.set_work_dir(temp_dir.path());
        resolver.set_test_vault_values(HashMap::from([(
            "API_KEY".to_string(),
            "vault_value".to_string(),
        )]));
        let mut config = make_config();
        resolver.resolve_parameters(&mut config).unwrap();
        assert!(temp_dir.path().join(".fed/secrets.cache.env").exists());

        // Second run: offline — the cache alone must resolve it.
        let mut offline_resolver = Resolver::new();
        offline_resolver.set_work_dir(temp_dir.path());
        offline_resolver.set_offline(true);
        let mut config = make_config();
        offline_resolver.resolve_parameters(&mut config).unwrap();
        assert_eq!(
            offline_resolver
                .get_resolved_parameters()
                .get("API_KEY")
                .unwrap(),
            "vault_value",
            "--offline must be served from .fed/secrets.cache.env"
        );
    }

    #[test]
    fn unignored_but_populated_does_not_error() {
        // Presence of the generated secrets file alone must not break a
        // previously-working setup — the gate fires only on actual writes.
        use crate::config::{Config, Parameter};
        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();
        git2::Repository::init(temp_dir.path()).unwrap();
        std::fs::create_dir_all(temp_dir.path().join(".fed")).unwrap();
        // A user-edited .fed/.gitignore that no longer ignores the generated
        // secrets file.
        std::fs::write(temp_dir.path().join(".fed/.gitignore"), "").unwrap();
        std::fs::write(
            temp_dir.path().join(".fed/secrets.generated.env"),
            "SESSION_KEY=already_here\n",
        )
        .unwrap();

        let mut resolver = Resolver::new();
        resolver.set_work_dir(temp_dir.path());

        let mut config = Config::default();
        config.parameters.insert(
            "SESSION_KEY".to_string(),
            Parameter {
                param_type: Some("secret".to_string()),
                ..Default::default()
            },
        );

        resolver.resolve_parameters(&mut config).unwrap();
        assert_eq!(
            resolver
                .get_resolved_parameters()
                .get("SESSION_KEY")
                .unwrap(),
            "already_here"
        );
    }

    #[test]
    fn write_attempt_to_unignored_path_errors() {
        // ...but as soon as fed would WRITE to the unsafe path, it refuses,
        // and the message covers the tracked-file case (git rm --cached).
        use crate::config::{Config, Parameter};
        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();
        git2::Repository::init(temp_dir.path()).unwrap();
        std::fs::create_dir_all(temp_dir.path().join(".fed")).unwrap();
        std::fs::write(temp_dir.path().join(".fed/.gitignore"), "").unwrap();
        std::fs::write(
            temp_dir.path().join(".fed/secrets.generated.env"),
            "SESSION_KEY=already_here\n",
        )
        .unwrap();

        let mut resolver = Resolver::new();
        resolver.set_work_dir(temp_dir.path());

        let mut config = Config::default();
        config.parameters.insert(
            "SESSION_KEY".to_string(),
            Parameter {
                param_type: Some("secret".to_string()),
                ..Default::default()
            },
        );
        // A second secret that has no value forces a write.
        config.parameters.insert(
            "NEW_KEY".to_string(),
            Parameter {
                param_type: Some("secret".to_string()),
                ..Default::default()
            },
        );

        let err = resolver.resolve_parameters(&mut config).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("not gitignored"),
            "loud error expected: {}",
            msg
        );
        assert!(
            msg.contains("git rm --cached"),
            "must cover the tracked-file case: {}",
            msg
        );
    }

    #[test]
    fn cache_covers_fresh_requires_present_stamped_and_young() {
        let now = 1_000_000u64;
        let max_age = 3600u64; // 1h
        let names = vec!["A".to_string(), "B".to_string()];

        let mut values = HashMap::new();
        values.insert("A".to_string(), "va".to_string());
        values.insert("B".to_string(), "vb".to_string());

        // Both present, both fresh → covered.
        let mut stamps = HashMap::new();
        stamps.insert("A".to_string(), now - 10);
        stamps.insert("B".to_string(), now - 20);
        assert!(cache_covers_fresh(&names, &values, &stamps, now, max_age));

        // B too old → not covered.
        stamps.insert("B".to_string(), now - max_age - 1);
        assert!(!cache_covers_fresh(&names, &values, &stamps, now, max_age));

        // B present but unstamped (pre-upgrade) → treated as too old.
        stamps.remove("B");
        assert!(!cache_covers_fresh(&names, &values, &stamps, now, max_age));

        // B value missing entirely → not covered.
        stamps.insert("B".to_string(), now);
        values.remove("B");
        assert!(!cache_covers_fresh(&names, &values, &stamps, now, max_age));
    }

    #[test]
    fn cache_covers_fresh_rejects_future_stamps() {
        // A stamp in the future must NOT count as fresh: saturating_sub would
        // report age 0 and pin a rotated value as fresh until wall time caught
        // up. Requiring stamped <= now closes that (clock skew / tampering).
        let now = 1_000_000u64;
        let max_age = 3600u64;
        let names = vec!["A".to_string()];
        let mut values = HashMap::new();
        values.insert("A".to_string(), "va".to_string());

        let mut stamps = HashMap::new();
        // Stamped one hour into the future.
        stamps.insert("A".to_string(), now + 3600);
        assert!(
            !cache_covers_fresh(&names, &values, &stamps, now, max_age),
            "a future-dated stamp must not be treated as fresh"
        );

        // Stamped exactly now → fresh (boundary).
        stamps.insert("A".to_string(), now);
        assert!(cache_covers_fresh(&names, &values, &stamps, now, max_age));
    }

    #[test]
    fn vault_failure_proceeds_on_cached_value_regardless_of_age() {
        // 02 done-when (airplane mode + cached values): when the vault is
        // unreachable, a required secret already in the cache resolves from it
        // regardless of the entry's age — offline work must keep working.
        use crate::config::{Config, Parameter};
        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();
        crate::fed_dir::ensure_fed_dir(temp_dir.path()).unwrap();
        // Ancient, unstamped cache entry (would be "too old" for a refresh
        // decision) — but with the vault down we proceed on it anyway.
        std::fs::write(
            temp_dir.path().join(".fed/secrets.cache.env"),
            "API_KEY=cached_old\n",
        )
        .unwrap();

        let mut resolver = Resolver::new();
        resolver.set_work_dir(temp_dir.path());
        resolver.set_test_vault_failure("cloud: cannot reach https://app.service-federation.com");

        let mut config = Config::default();
        config.parameters.insert(
            "API_KEY".to_string(),
            Parameter {
                param_type: Some("secret".to_string()),
                source: Some("manual".to_string()),
                ..Default::default()
            },
        );

        resolver
            .resolve_parameters(&mut config)
            .expect("cached value must satisfy the run when the vault is down");
        assert_eq!(
            resolver.get_resolved_parameters().get("API_KEY").unwrap(),
            "cached_old"
        );
        // The cache is NOT rewritten on failure — the value survives.
        let cache =
            std::fs::read_to_string(temp_dir.path().join(".fed/secrets.cache.env")).unwrap();
        assert!(cache.contains("API_KEY=cached_old"));
    }

    #[test]
    fn successful_vault_run_stamps_the_cache() {
        // A successful online run writes a fetched-at stamp, so a later run can
        // apply the freshness bound.
        use crate::config::{Config, Parameter};
        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();
        let mut resolver = Resolver::new();
        resolver.set_work_dir(temp_dir.path());
        resolver.set_test_vault_values(HashMap::from([(
            "API_KEY".to_string(),
            "fresh".to_string(),
        )]));

        let mut config = Config::default();
        config.parameters.insert(
            "API_KEY".to_string(),
            Parameter {
                param_type: Some("secret".to_string()),
                source: Some("manual".to_string()),
                ..Default::default()
            },
        );

        resolver.resolve_parameters(&mut config).unwrap();
        let cache_path = temp_dir.path().join(".fed/secrets.cache.env");
        let stamps = super::super::secret::load_cache_stamps(&cache_path);
        assert!(
            stamps.contains_key("API_KEY"),
            "a vault hit must be stamped with its fetched-at time"
        );
    }

    #[test]
    fn scoped_run_ignores_out_of_scope_secret_with_vault_down() {
        // 01 done-when: a run scoped to names it references makes zero cloud
        // requests for an unreferenced STRIPE_SECRET and succeeds even with the
        // vault unreachable.
        use crate::config::{Config, Parameter};
        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();

        let mut resolver = Resolver::new();
        resolver.set_work_dir(temp_dir.path());
        // Scope excludes STRIPE_SECRET entirely.
        resolver.set_required_names(Some(HashSet::new()));
        // If anything were queried, this failure would surface — it must not.
        resolver.set_test_vault_failure("cloud: cannot reach vault");

        let mut config = Config::default();
        config.parameters.insert(
            "STRIPE_SECRET".to_string(),
            Parameter {
                param_type: Some("secret".to_string()),
                source: Some("manual".to_string()),
                ..Default::default()
            },
        );

        // Must not error despite the required manual secret being unresolved:
        // it is out of scope for this run.
        resolver
            .resolve_parameters(&mut config)
            .expect("out-of-scope required secret must not fail the run");
        assert!(
            !temp_dir.path().join(".fed/secrets.cache.env").exists(),
            "no vault query means nothing cached"
        );
    }

    #[test]
    fn scoped_run_defers_out_of_scope_service_referencing_missing_secret() {
        // RB-1: a scoped run must not hard-fail during resolution because an
        // *unrelated* service references a manual secret this run never uses.
        // The service is out of the scanned closure, so it is dropped from the
        // resolved config rather than failing the run.
        use crate::config::{Config, Parameter, Service};
        use tempfile::TempDir;

        let temp = TempDir::new().unwrap();
        let mut resolver = Resolver::new();
        resolver.set_work_dir(temp.path());
        // Secret-free target → nothing in scope.
        resolver.set_required_names(Some(HashSet::new()));

        let mut config = Config::default();
        config.parameters.insert(
            "UNRELATED_SECRET".to_string(),
            Parameter {
                param_type: Some("secret".to_string()),
                source: Some("manual".to_string()),
                ..Default::default()
            },
        );
        config.services.insert(
            "unrelated".to_string(),
            Service {
                process: Some("serve {{UNRELATED_SECRET}}".to_string()),
                ..Default::default()
            },
        );

        resolver
            .resolve_parameters(&mut config)
            .expect("scoped parameter resolution must not fail");
        let resolved = resolver
            .resolve_config(&config)
            .expect("an out-of-scope service must not hard-fail the scoped run");
        assert!(
            !resolved.services.contains_key("unrelated"),
            "the out-of-scope service must be dropped from the resolved config"
        );
    }

    #[test]
    fn scoped_run_defers_derived_default_on_out_of_scope_secret() {
        // RB: a parameter whose default interpolates an out-of-scope missing
        // manual secret must be deferred, not fail the scoped run.
        use crate::config::{Config, Parameter};
        use tempfile::TempDir;

        let temp = TempDir::new().unwrap();
        let mut resolver = Resolver::new();
        resolver.set_work_dir(temp.path());
        resolver.set_required_names(Some(HashSet::new())); // nothing in scope

        let mut config = Config::default();
        config.parameters.insert(
            "UNUSED_SECRET".to_string(),
            Parameter {
                param_type: Some("secret".to_string()),
                source: Some("manual".to_string()),
                ..Default::default()
            },
        );
        config.parameters.insert(
            "UNUSED_DERIVED".to_string(),
            Parameter {
                default: Some(serde_yaml::Value::String(
                    "prefix-{{UNUSED_SECRET}}".to_string(),
                )),
                ..Default::default()
            },
        );

        resolver
            .resolve_parameters(&mut config)
            .expect("a derived default over an out-of-scope secret must be deferred, not fatal");
    }

    #[test]
    fn scoped_run_defers_generate_referencing_out_of_scope_secret() {
        // RB (a): a non-secret generate that references an out-of-scope missing
        // manual secret must be deferred (its command not executed), not fatal.
        use crate::config::{Config, Parameter};
        use tempfile::TempDir;

        let temp = TempDir::new().unwrap();
        let mut resolver = Resolver::new();
        resolver.set_work_dir(temp.path());
        resolver.set_required_names(Some(HashSet::new()));

        let mut config = Config::default();
        config.parameters.insert(
            "UNUSED_SECRET".to_string(),
            Parameter {
                param_type: Some("secret".to_string()),
                source: Some("manual".to_string()),
                ..Default::default()
            },
        );
        config.parameters.insert(
            "UNUSED_GEN".to_string(),
            Parameter {
                // Would fail if executed — `false` exits non-zero — but must be
                // deferred (skipped) entirely because it references a secret out
                // of scope.
                generate: Some("false {{UNUSED_SECRET}}".to_string()),
                ..Default::default()
            },
        );

        resolver
            .resolve_parameters(&mut config)
            .expect("a generate over an out-of-scope secret must be deferred, not executed");
        assert!(
            !resolver
                .get_resolved_parameters()
                .contains_key("UNUSED_GEN"),
            "a deferred generate must not produce a value"
        );
    }

    #[test]
    fn unscoped_run_still_fails_on_generate_referencing_missing_secret() {
        // Control for the test above: with no scoping the same generate runs and
        // fails on the missing secret — unscoped behavior is unchanged.
        use crate::config::{Config, Parameter};
        use tempfile::TempDir;

        let temp = TempDir::new().unwrap();
        let mut resolver = Resolver::new();
        resolver.set_work_dir(temp.path());
        resolver.set_offline(true); // secret genuinely missing

        let mut config = Config::default();
        config.parameters.insert(
            "UNUSED_SECRET".to_string(),
            Parameter {
                param_type: Some("secret".to_string()),
                source: Some("manual".to_string()),
                ..Default::default()
            },
        );
        config.parameters.insert(
            "UNUSED_GEN".to_string(),
            Parameter {
                generate: Some("false {{UNUSED_SECRET}}".to_string()),
                ..Default::default()
            },
        );

        assert!(
            resolver.resolve_parameters(&mut config).is_err(),
            "unscoped run must still fail on the generate's missing secret"
        );
    }

    #[test]
    fn scoped_run_still_fails_on_in_scope_derived_missing_secret() {
        // Strictness preserved: a derived default over an IN-scope secret that is
        // missing must still fail — only out-of-scope dependencies are deferred.
        use crate::config::{Config, Parameter};
        use tempfile::TempDir;

        let temp = TempDir::new().unwrap();
        let mut resolver = Resolver::new();
        resolver.set_work_dir(temp.path());
        resolver.set_offline(true); // in-scope secret is genuinely missing
        resolver.set_required_names(Some(HashSet::from(["IN_SCOPE_SECRET".to_string()])));

        let mut config = Config::default();
        config.parameters.insert(
            "IN_SCOPE_SECRET".to_string(),
            Parameter {
                param_type: Some("secret".to_string()),
                source: Some("manual".to_string()),
                ..Default::default()
            },
        );
        config.parameters.insert(
            "DERIVED".to_string(),
            Parameter {
                default: Some(serde_yaml::Value::String(
                    "prefix-{{IN_SCOPE_SECRET}}".to_string(),
                )),
                ..Default::default()
            },
        );

        assert!(
            resolver.resolve_parameters(&mut config).is_err(),
            "an in-scope missing secret must still fail the run"
        );
    }

    #[test]
    fn unscoped_run_still_fails_on_service_referencing_missing_secret() {
        // Control: with no scoping, the same missing manual secret still fails —
        // fed start stays exactly as strict as before.
        use crate::config::{Config, Parameter, Service};
        use tempfile::TempDir;

        let temp = TempDir::new().unwrap();
        let mut resolver = Resolver::new();
        resolver.set_work_dir(temp.path());
        resolver.set_offline(true); // no vault, no cache → genuinely missing

        let mut config = Config::default();
        config.parameters.insert(
            "UNRELATED_SECRET".to_string(),
            Parameter {
                param_type: Some("secret".to_string()),
                source: Some("manual".to_string()),
                ..Default::default()
            },
        );
        config.services.insert(
            "unrelated".to_string(),
            Service {
                process: Some("serve {{UNRELATED_SECRET}}".to_string()),
                ..Default::default()
            },
        );

        // Fails during secret resolution (the required secret is missing);
        // the service is never dropped because nothing is scoped.
        assert!(
            resolver.resolve_parameters(&mut config).is_err(),
            "unscoped run must still fail on the missing required secret"
        );
    }

    #[test]
    fn unscoped_run_still_fails_on_missing_required_secret() {
        // Control for the test above: with no scoping (None), the same missing
        // required manual secret still fails — scoping is what changes it.
        use crate::config::{Config, Parameter};
        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();
        let mut resolver = Resolver::new();
        resolver.set_work_dir(temp_dir.path());
        resolver.set_offline(true); // no vault, no cache → genuinely missing

        let mut config = Config::default();
        config.parameters.insert(
            "STRIPE_SECRET".to_string(),
            Parameter {
                param_type: Some("secret".to_string()),
                source: Some("manual".to_string()),
                ..Default::default()
            },
        );

        assert!(
            resolver.resolve_parameters(&mut config).is_err(),
            "unscoped run must still fail on a missing required secret"
        );
    }

    #[test]
    fn scoped_run_queries_only_in_scope_secret() {
        // A run scoped to API_KEY resolves it from the vault while leaving an
        // out-of-scope STRIPE_SECRET untouched (not queried, not failed).
        use crate::config::{Config, Parameter};
        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();
        let mut resolver = Resolver::new();
        resolver.set_work_dir(temp_dir.path());
        resolver.set_required_names(Some(HashSet::from(["API_KEY".to_string()])));
        resolver.set_test_vault_values(HashMap::from([
            ("API_KEY".to_string(), "from_vault".to_string()),
            (
                "STRIPE_SECRET".to_string(),
                "should_not_be_used".to_string(),
            ),
        ]));

        let mut config = Config::default();
        for name in ["API_KEY", "STRIPE_SECRET"] {
            config.parameters.insert(
                name.to_string(),
                Parameter {
                    param_type: Some("secret".to_string()),
                    source: Some("manual".to_string()),
                    ..Default::default()
                },
            );
        }

        resolver.resolve_parameters(&mut config).unwrap();
        assert_eq!(
            resolver.get_resolved_parameters().get("API_KEY").unwrap(),
            "from_vault"
        );
        let cache =
            std::fs::read_to_string(temp_dir.path().join(".fed/secrets.cache.env")).unwrap();
        assert!(cache.contains("API_KEY=from_vault"));
        assert!(
            !cache.contains("STRIPE_SECRET"),
            "out-of-scope secret must not be queried or cached: {cache}"
        );
    }

    #[test]
    fn scoped_run_preserves_cache_entries_for_unqueried_secrets() {
        // 01 done-when: cache entries for secrets NOT queried this run survive.
        use crate::config::{Config, Parameter};
        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();
        crate::fed_dir::ensure_fed_dir(temp_dir.path()).unwrap();
        std::fs::write(
            temp_dir.path().join(".fed/secrets.cache.env"),
            "API_KEY=old\nSTRIPE_SECRET=cached_stripe\n",
        )
        .unwrap();

        let mut resolver = Resolver::new();
        resolver.set_work_dir(temp_dir.path());
        resolver.set_required_names(Some(HashSet::from(["API_KEY".to_string()])));
        resolver.set_test_vault_values(HashMap::from([(
            "API_KEY".to_string(),
            "rotated".to_string(),
        )]));

        let mut config = Config::default();
        for name in ["API_KEY", "STRIPE_SECRET"] {
            config.parameters.insert(
                name.to_string(),
                Parameter {
                    param_type: Some("secret".to_string()),
                    source: Some("manual".to_string()),
                    ..Default::default()
                },
            );
        }

        resolver.resolve_parameters(&mut config).unwrap();
        let cache =
            std::fs::read_to_string(temp_dir.path().join(".fed/secrets.cache.env")).unwrap();
        assert!(cache.contains("API_KEY=rotated"), "queried name refreshed");
        assert!(
            cache.contains("STRIPE_SECRET=cached_stripe"),
            "unqueried cache entry must survive the scoped run: {cache}"
        );
    }

    #[test]
    fn missing_secret_with_unreachable_vault_names_the_cloud() {
        // Step 0: a required secret that the vault could not supply because the
        // cloud was unreachable must produce an error naming that cause — not
        // "add it to your env_file", which misdirects the user away from the
        // real problem.
        use crate::config::{Config, Parameter};
        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();

        let mut resolver = Resolver::new();
        resolver.set_work_dir(temp_dir.path());
        resolver.set_test_vault_failure(
            "cloud: cannot reach https://app.service-federation.com: error sending request",
        );

        let mut config = Config::default();
        config.parameters.insert(
            "API_KEY".to_string(),
            Parameter {
                param_type: Some("secret".to_string()),
                source: Some("manual".to_string()),
                ..Default::default()
            },
        );

        let err = resolver.resolve_parameters(&mut config).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("team vault could not be reached"),
            "error must name the unreachable cloud as the cause: {msg}"
        );
        assert!(
            msg.contains("app.service-federation.com"),
            "error should carry the underlying reason: {msg}"
        );
    }

    #[test]
    fn missing_secret_without_vault_keeps_env_file_hint() {
        // With no vault failure (simply not provided anywhere), the classic
        // "add them to your env_file" guidance still applies.
        use crate::config::{Config, Parameter};
        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();

        let mut resolver = Resolver::new();
        resolver.set_work_dir(temp_dir.path());
        resolver.set_offline(true);

        let mut config = Config::default();
        config.parameters.insert(
            "API_KEY".to_string(),
            Parameter {
                param_type: Some("secret".to_string()),
                source: Some("manual".to_string()),
                ..Default::default()
            },
        );

        let err = resolver.resolve_parameters(&mut config).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("add them to your env_file"),
            "offline missing secret keeps the env_file guidance: {msg}"
        );
        assert!(
            !msg.contains("team vault could not be reached"),
            "no vault failure means no unreachable-cloud message: {msg}"
        );
        // Writes are dashboard-only since fed 7.0 — the hint points there and
        // must never mention the removed `fed secrets set` command.
        assert!(
            msg.contains("set them in the dashboard"),
            "hint should direct writes to the dashboard: {msg}"
        );
        assert!(
            !msg.contains("fed secrets set"),
            "the removed `fed secrets set` command must not appear: {msg}"
        );
    }

    #[test]
    fn vault_refetch_overwrites_stale_cache_when_online() {
        // P1-2: a cached value must not shadow the vault — online runs
        // re-query and the fresh value wins, in params and in the cache file.
        use crate::config::{Config, Parameter};
        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();
        crate::fed_dir::ensure_fed_dir(temp_dir.path()).unwrap();
        std::fs::write(
            temp_dir.path().join(".fed/secrets.cache.env"),
            "API_KEY=stale_value\n",
        )
        .unwrap();

        let mut resolver = Resolver::new();
        resolver.set_work_dir(temp_dir.path());
        resolver.set_test_vault_values(HashMap::from([(
            "API_KEY".to_string(),
            "rotated_value".to_string(),
        )]));

        let mut config = Config::default();
        config.parameters.insert(
            "API_KEY".to_string(),
            Parameter {
                param_type: Some("secret".to_string()),
                source: Some("manual".to_string()),
                ..Default::default()
            },
        );

        resolver.resolve_parameters(&mut config).unwrap();
        assert_eq!(
            resolver.get_resolved_parameters().get("API_KEY").unwrap(),
            "rotated_value",
            "vault must win over the cache when online"
        );
        let cache =
            std::fs::read_to_string(temp_dir.path().join(".fed/secrets.cache.env")).unwrap();
        assert!(cache.contains("API_KEY=rotated_value"));
        assert!(!cache.contains("stale_value"));
    }

    #[test]
    fn stale_cache_keys_filtered_and_pruned() {
        // P1-3: keys no longer declared must neither break resolution nor
        // survive the next successful cache write.
        use crate::config::{Config, Parameter};
        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();
        crate::fed_dir::ensure_fed_dir(temp_dir.path()).unwrap();
        std::fs::write(
            temp_dir.path().join(".fed/secrets.cache.env"),
            "REMOVED_PARAM=leftover\nAPI_KEY=old\n",
        )
        .unwrap();

        let mut resolver = Resolver::new();
        resolver.set_work_dir(temp_dir.path());
        resolver.set_test_vault_values(HashMap::from([(
            "API_KEY".to_string(),
            "fresh".to_string(),
        )]));

        let mut config = Config::default();
        config.parameters.insert(
            "API_KEY".to_string(),
            Parameter {
                param_type: Some("secret".to_string()),
                source: Some("manual".to_string()),
                ..Default::default()
            },
        );

        // Strict env loading must not choke on REMOVED_PARAM.
        resolver.resolve_parameters(&mut config).unwrap();

        let cache =
            std::fs::read_to_string(temp_dir.path().join(".fed/secrets.cache.env")).unwrap();
        assert!(cache.contains("API_KEY=fresh"));
        assert!(
            !cache.contains("REMOVED_PARAM"),
            "undeclared keys are pruned on write: {}",
            cache
        );
    }

    #[test]
    fn stale_cache_key_does_not_break_offline_resolution() {
        // P1-3 (load side): offline, the filtered cache satisfies declared
        // secrets and the leftover key is simply ignored.
        use crate::config::{Config, Parameter};
        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();
        crate::fed_dir::ensure_fed_dir(temp_dir.path()).unwrap();
        std::fs::write(
            temp_dir.path().join(".fed/secrets.cache.env"),
            "REMOVED_PARAM=leftover\nAPI_KEY=cached\n",
        )
        .unwrap();

        let mut resolver = Resolver::new();
        resolver.set_work_dir(temp_dir.path());
        resolver.set_offline(true);

        let mut config = Config::default();
        config.parameters.insert(
            "API_KEY".to_string(),
            Parameter {
                param_type: Some("secret".to_string()),
                source: Some("manual".to_string()),
                ..Default::default()
            },
        );

        resolver.resolve_parameters(&mut config).unwrap();
        assert_eq!(
            resolver.get_resolved_parameters().get("API_KEY").unwrap(),
            "cached"
        );
    }

    #[test]
    fn cache_write_declined_when_cache_path_not_gitignored() {
        // P1-4: a user-edited permissive .fed/.gitignore must not turn the
        // cache into a committable secrets file — fed declines to cache.
        use crate::config::{Config, Parameter};
        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();
        git2::Repository::init(temp_dir.path()).unwrap();
        std::fs::create_dir_all(temp_dir.path().join(".fed")).unwrap();
        // Permissive: nothing ignored.
        std::fs::write(temp_dir.path().join(".fed/.gitignore"), "").unwrap();

        let mut resolver = Resolver::new();
        resolver.set_work_dir(temp_dir.path());
        resolver.set_test_vault_values(HashMap::from([(
            "API_KEY".to_string(),
            "vault_value".to_string(),
        )]));

        let mut config = Config::default();
        config.parameters.insert(
            "API_KEY".to_string(),
            Parameter {
                param_type: Some("secret".to_string()),
                source: Some("manual".to_string()),
                ..Default::default()
            },
        );

        resolver.resolve_parameters(&mut config).unwrap();
        assert_eq!(
            resolver.get_resolved_parameters().get("API_KEY").unwrap(),
            "vault_value",
            "resolution itself still succeeds"
        );
        assert!(
            !temp_dir.path().join(".fed/secrets.cache.env").exists(),
            "cache must not be written to a commit-eligible path"
        );
    }

    #[test]
    fn unsafe_cache_path_deletes_existing_cache_online() {
        // P1-4 residual: a pre-existing cache on a commit-eligible path is
        // removed (not kept and reused), its values are ignored, and online
        // resolution still succeeds from the vault.
        use crate::config::{Config, Parameter};
        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();
        git2::Repository::init(temp_dir.path()).unwrap();
        std::fs::create_dir_all(temp_dir.path().join(".fed")).unwrap();
        // Permissive: nothing ignored → cache is commit-eligible.
        std::fs::write(temp_dir.path().join(".fed/.gitignore"), "").unwrap();
        std::fs::write(
            temp_dir.path().join(".fed/secrets.cache.env"),
            "API_KEY=stale_committable\n",
        )
        .unwrap();

        let mut resolver = Resolver::new();
        resolver.set_work_dir(temp_dir.path());
        resolver.set_test_vault_values(HashMap::from([(
            "API_KEY".to_string(),
            "vault_value".to_string(),
        )]));

        let mut config = Config::default();
        config.parameters.insert(
            "API_KEY".to_string(),
            Parameter {
                param_type: Some("secret".to_string()),
                source: Some("manual".to_string()),
                ..Default::default()
            },
        );

        resolver.resolve_parameters(&mut config).unwrap();
        assert_eq!(
            resolver.get_resolved_parameters().get("API_KEY").unwrap(),
            "vault_value",
            "online resolution still succeeds"
        );
        assert!(
            !temp_dir.path().join(".fed/secrets.cache.env").exists(),
            "commit-eligible cache must be deleted and never rewritten"
        );
    }

    #[test]
    fn unsafe_cache_path_refuses_cached_values_offline() {
        // P1-4 residual: on an unsafe path the cache is deleted and its values
        // are refused — offline, a required secret it used to satisfy is
        // reported missing rather than served from a commit-eligible file.
        use crate::config::{Config, Parameter};
        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();
        git2::Repository::init(temp_dir.path()).unwrap();
        std::fs::create_dir_all(temp_dir.path().join(".fed")).unwrap();
        std::fs::write(temp_dir.path().join(".fed/.gitignore"), "").unwrap();
        std::fs::write(
            temp_dir.path().join(".fed/secrets.cache.env"),
            "API_KEY=stale_committable\n",
        )
        .unwrap();

        let mut resolver = Resolver::new();
        resolver.set_work_dir(temp_dir.path());
        resolver.set_offline(true);

        let mut config = Config::default();
        config.parameters.insert(
            "API_KEY".to_string(),
            Parameter {
                param_type: Some("secret".to_string()),
                source: Some("manual".to_string()),
                ..Default::default()
            },
        );

        let err = resolver.resolve_parameters(&mut config).unwrap_err();
        assert!(err.to_string().contains("API_KEY"));
        assert!(
            !temp_dir.path().join(".fed/secrets.cache.env").exists(),
            "unsafe cache file must be deleted"
        );
    }

    #[test]
    fn absolute_env_file_path_loaded() {
        use crate::config::{Config, Parameter};
        use tempfile::TempDir;

        let work_dir = TempDir::new().unwrap();
        let secrets_dir = TempDir::new().unwrap();
        let secrets_path = secrets_dir.path().join("secrets.env");
        std::fs::write(&secrets_path, "API_TOKEN=from_absolute_path\n").unwrap();

        let mut resolver = Resolver::new();
        resolver.set_work_dir(work_dir.path());

        let mut config = Config::default();
        config.env_file = vec![secrets_path.to_string_lossy().to_string()];
        config.parameters.insert(
            "API_TOKEN".to_string(),
            Parameter {
                ..Default::default()
            },
        );

        resolver.resolve_parameters(&mut config).unwrap();

        let resolved = resolver.get_resolved_parameters();
        assert_eq!(
            resolved.get("API_TOKEN").unwrap(),
            "from_absolute_path",
            "Absolute env_file paths should be loaded directly, not joined with work_dir"
        );
    }

    #[test]
    fn tilde_env_file_path_expanded() {
        use crate::config::{Config, Parameter};
        use tempfile::TempDir;

        let home = dirs::home_dir().expect("test requires home dir");
        let test_dir = home.join(".fed-test-tmp");
        std::fs::create_dir_all(&test_dir).unwrap();
        let env_file = test_dir.join("test.env");
        std::fs::write(&env_file, "TILDE_VAR=it_works\n").unwrap();

        let work_dir = TempDir::new().unwrap();
        let mut resolver = Resolver::new();
        resolver.set_work_dir(work_dir.path());

        let mut config = Config::default();
        config.env_file = vec!["~/.fed-test-tmp/test.env".to_string()];
        config.parameters.insert(
            "TILDE_VAR".to_string(),
            Parameter {
                ..Default::default()
            },
        );

        let result = resolver.resolve_parameters(&mut config);
        // Clean up before asserting
        let _ = std::fs::remove_dir_all(&test_dir);

        result.unwrap();
        let resolved = resolver.get_resolved_parameters();
        assert_eq!(resolved.get("TILDE_VAR").unwrap(), "it_works");
    }

    #[test]
    fn missing_env_file_warns_and_continues() {
        use crate::config::{Config, Parameter};
        use tempfile::TempDir;

        let work_dir = TempDir::new().unwrap();
        let mut resolver = Resolver::new();
        resolver.set_work_dir(work_dir.path());

        // Reference an env_file that doesn't exist on disk.
        let mut config = Config::default();
        config.env_file = vec!["does-not-exist.env".to_string()];
        config.parameters.insert(
            "API_KEY".to_string(),
            Parameter {
                value: Some("from-default".to_string()),
                ..Default::default()
            },
        );

        // Resolution should succeed; the parameter keeps its default value.
        resolver.resolve_parameters(&mut config).unwrap();
        let resolved = resolver.get_resolved_parameters();
        assert_eq!(resolved.get("API_KEY").unwrap(), "from-default");
    }

    #[test]
    fn missing_env_file_still_errors_on_parse_failure() {
        use crate::config::{Config, Parameter};
        use tempfile::TempDir;

        let work_dir = TempDir::new().unwrap();
        let env_path = work_dir.path().join("bad.env");
        // Invalid env name — file exists but parse/validate fails.
        std::fs::write(&env_path, "INVALID-NAME=value\n").unwrap();

        let mut resolver = Resolver::new();
        resolver.set_work_dir(work_dir.path());

        let mut config = Config::default();
        config.env_file = vec!["bad.env".to_string()];
        config
            .parameters
            .insert("WHATEVER".to_string(), Parameter::default());

        let err = resolver
            .resolve_parameters(&mut config)
            .expect_err("malformed env file should still error");
        assert!(err.to_string().contains("bad.env"));
    }

    // ── The round-3 rule: a deferred OR generatable secret is NEVER randomly
    //    generated (it must not fall through to the random-alphanumeric
    //    fallback and be persisted, where a later run would keep it). ─────────

    #[test]
    fn deferred_or_generatable_secret_is_never_randomly_generated() {
        // SEED (manual) + DERIVED_SECRET (secret with a generator over SEED).
        use crate::config::{Config, Parameter};
        use tempfile::TempDir;

        let temp = TempDir::new().unwrap();
        let gen_path = temp.path().join(crate::fed_dir::GENERATED_SECRETS_REL);

        let make_config = |with_env: bool| {
            let mut config = Config::default();
            if with_env {
                config.env_file = vec!["seed.env".to_string()];
            }
            config.parameters.insert(
                "SEED".to_string(),
                Parameter {
                    param_type: Some("secret".to_string()),
                    source: Some("manual".to_string()),
                    ..Default::default()
                },
            );
            config.parameters.insert(
                "DERIVED_SECRET".to_string(),
                Parameter {
                    param_type: Some("secret".to_string()),
                    generate: Some("printf %s {{SEED}}".to_string()),
                    ..Default::default()
                },
            );
            config
        };

        // Scoped run: SEED out of scope → DERIVED_SECRET is deferred. It must
        // persist NOTHING — never a random value that a later run would keep
        // (generate.rs preserves existing secret values, which would poison it).
        let mut resolver = Resolver::new();
        resolver.set_work_dir(temp.path());
        resolver.set_offline(true);
        resolver.set_required_names(Some(HashSet::new()));
        let mut config = make_config(false);
        resolver
            .resolve_parameters(&mut config)
            .expect("a scoped run must defer the generated secret, not fail");
        assert!(
            !gen_path.exists(),
            "a deferred generated secret must persist nothing — no random value written"
        );

        // Unscoped run with SEED available: the generator runs but references
        // another secret, which under v6.2 semantics fails with ParameterNotFound.
        // It must surface that error, NOT silently fall back to a random value.
        std::fs::write(temp.path().join("seed.env"), "SEED=myseed\n").unwrap();
        let mut resolver = Resolver::new();
        resolver.set_work_dir(temp.path());
        resolver.set_offline(true);
        let mut config = make_config(true);
        let err = resolver.resolve_parameters(&mut config).expect_err(
            "a generatable secret referencing another secret must fail, not be randomized",
        );
        assert!(
            matches!(err, Error::ParameterNotFound(ref n) if n == "SEED"),
            "must fail with ParameterNotFound(SEED), got: {err}"
        );
        assert!(
            !gen_path.exists(),
            "no random value must be persisted for the generatable secret"
        );
    }

    // ── RB-2: deferred params must skip port and `either` validation ─────────

    #[test]
    fn scoped_run_skips_port_validation_and_allocation_for_deferred_param() {
        // A deferred port parameter (its default interpolates an out-of-scope
        // secret) must not be validated or allocated a port.
        use crate::config::{Config, Parameter};
        use tempfile::TempDir;

        let temp = TempDir::new().unwrap();
        let mut resolver = Resolver::new();
        resolver.set_work_dir(temp.path());
        resolver.set_offline(true);
        resolver.set_required_names(Some(HashSet::new()));

        let mut config = Config::default();
        config.parameters.insert(
            "UNUSED_SECRET".to_string(),
            Parameter {
                param_type: Some("secret".to_string()),
                source: Some("manual".to_string()),
                ..Default::default()
            },
        );
        config.parameters.insert(
            "UNUSED_PORT".to_string(),
            Parameter {
                param_type: Some("port".to_string()),
                default: Some(serde_yaml::Value::String("{{UNUSED_SECRET}}".to_string())),
                ..Default::default()
            },
        );

        resolver
            .resolve_parameters(&mut config)
            .expect("a deferred port param must not be validated or allocated");
        assert!(
            !resolver
                .get_port_parameter_names()
                .contains(&"UNUSED_PORT".to_string()),
            "a deferred port param must not be tracked for allocation"
        );
        assert!(
            resolver
                .get_port_resolutions()
                .iter()
                .all(|r| r.param_name != "UNUSED_PORT"),
            "a deferred port param must allocate no port"
        );
        assert!(
            !resolver
                .get_resolved_parameters()
                .contains_key("UNUSED_PORT"),
            "a deferred port param must produce no resolved value"
        );
    }

    #[test]
    fn unscoped_run_still_fails_on_invalid_port_default() {
        // Control: with no scoping the same template port default is validated
        // and rejected — port strictness is unchanged.
        use crate::config::{Config, Parameter};
        use tempfile::TempDir;

        let temp = TempDir::new().unwrap();
        std::fs::write(temp.path().join("seed.env"), "UNUSED_SECRET=whatever\n").unwrap();
        let mut resolver = Resolver::new();
        resolver.set_work_dir(temp.path());
        resolver.set_offline(true);

        let mut config = Config::default();
        config.env_file = vec!["seed.env".to_string()];
        config.parameters.insert(
            "UNUSED_SECRET".to_string(),
            Parameter {
                param_type: Some("secret".to_string()),
                source: Some("manual".to_string()),
                ..Default::default()
            },
        );
        config.parameters.insert(
            "UNUSED_PORT".to_string(),
            Parameter {
                param_type: Some("port".to_string()),
                default: Some(serde_yaml::Value::String("{{UNUSED_SECRET}}".to_string())),
                ..Default::default()
            },
        );

        let err = resolver
            .resolve_parameters(&mut config)
            .expect_err("an unscoped run must still reject the invalid port default");
        assert!(
            err.to_string().contains("invalid port default"),
            "got: {err}"
        );
    }

    #[test]
    fn scoped_run_skips_either_validation_for_deferred_param() {
        // A deferred `either`-constrained param (its default interpolates an
        // out-of-scope secret) must not be validated against its allowed values.
        use crate::config::{Config, Parameter};
        use tempfile::TempDir;

        let temp = TempDir::new().unwrap();
        let mut resolver = Resolver::new();
        resolver.set_work_dir(temp.path());
        resolver.set_offline(true);
        resolver.set_required_names(Some(HashSet::new()));

        let mut config = Config::default();
        config.parameters.insert(
            "UNUSED_SECRET".to_string(),
            Parameter {
                param_type: Some("secret".to_string()),
                source: Some("manual".to_string()),
                ..Default::default()
            },
        );
        config.parameters.insert(
            "UNUSED_CHOICE".to_string(),
            Parameter {
                either: vec!["a".to_string(), "b".to_string()],
                default: Some(serde_yaml::Value::String("{{UNUSED_SECRET}}".to_string())),
                ..Default::default()
            },
        );

        resolver
            .resolve_parameters(&mut config)
            .expect("a deferred either-constrained param must not be validated");
    }

    #[test]
    fn unscoped_run_still_fails_on_either_constraint() {
        // Control: with no scoping the resolved value is validated against the
        // allowed set and rejected — `either` strictness is unchanged.
        use crate::config::{Config, Parameter};
        use tempfile::TempDir;

        let temp = TempDir::new().unwrap();
        std::fs::write(temp.path().join("seed.env"), "UNUSED_SECRET=zzz\n").unwrap();
        let mut resolver = Resolver::new();
        resolver.set_work_dir(temp.path());
        resolver.set_offline(true);

        let mut config = Config::default();
        config.env_file = vec!["seed.env".to_string()];
        config.parameters.insert(
            "UNUSED_SECRET".to_string(),
            Parameter {
                param_type: Some("secret".to_string()),
                source: Some("manual".to_string()),
                ..Default::default()
            },
        );
        config.parameters.insert(
            "UNUSED_CHOICE".to_string(),
            Parameter {
                either: vec!["a".to_string(), "b".to_string()],
                default: Some(serde_yaml::Value::String("{{UNUSED_SECRET}}".to_string())),
                ..Default::default()
            },
        );

        let err = resolver
            .resolve_parameters(&mut config)
            .expect_err("an unscoped run must still reject the out-of-set value");
        assert!(
            err.to_string().contains("not in the allowed values"),
            "got: {err}"
        );
    }

    // ── Generated-secret interpolation semantics (v6.2) ───────────────────────
    // The secret-generate DAG runs with an EMPTY resolved map: a reference-less
    // generator still generates and is preserved across runs; a generator that
    // references any parameter fails with ParameterNotFound. (A generator that
    // interpolates another secret — `derive {{SEED}}` — is a deferred follow-up.)

    fn generator_run_count(marker: &std::path::Path) -> usize {
        std::fs::read_to_string(marker)
            .map(|s| s.len())
            .unwrap_or(0)
    }

    #[test]
    fn referenceless_generated_secret_persists_and_is_preserved() {
        use crate::config::{Config, Parameter};
        use tempfile::TempDir;

        let temp = TempDir::new().unwrap();
        let marker = temp.path().join("gen-runs.log");
        let gen_path = temp.path().join(crate::fed_dir::GENERATED_SECRETS_REL);

        // A reference-less secret generator that records each execution via
        // `marker` and always prints the same value.
        let make_config = || {
            let mut config = Config::default();
            config.parameters.insert(
                "SECRET".to_string(),
                Parameter {
                    param_type: Some("secret".to_string()),
                    generate: Some(format!(
                        "printf x >> '{}'; printf %s fixedvalue",
                        marker.display()
                    )),
                    ..Default::default()
                },
            );
            config
        };

        // First run: generates and persists.
        let mut r1 = Resolver::new();
        r1.set_work_dir(temp.path());
        r1.set_offline(true);
        let mut c1 = make_config();
        r1.resolve_parameters(&mut c1)
            .expect("first run must succeed");
        assert_eq!(generator_run_count(&marker), 1, "first run generates");
        let persisted = std::fs::read_to_string(&gen_path).unwrap();
        assert!(
            persisted.contains("SECRET=fixedvalue"),
            "the reference-less secret must persist: {persisted}"
        );

        // Second run: the persisted value is preserved, the generator does NOT
        // rerun.
        let mut r2 = Resolver::new();
        r2.set_work_dir(temp.path());
        r2.set_offline(true);
        let mut c2 = make_config();
        r2.resolve_parameters(&mut c2)
            .expect("second run must succeed");
        assert_eq!(
            generator_run_count(&marker),
            1,
            "a reference-less generated secret must be preserved, not regenerated"
        );
    }

    #[test]
    fn secret_generator_referencing_param_fails_with_parameter_not_found() {
        use crate::config::{Config, Parameter};
        use tempfile::TempDir;

        let temp = TempDir::new().unwrap();
        std::fs::write(temp.path().join("seed.env"), "SEED=abc\n").unwrap();

        let mut resolver = Resolver::new();
        resolver.set_work_dir(temp.path());
        resolver.set_offline(true);

        // DERIVED interpolates the manual secret SEED. With the empty DAG seed
        // (v6.2 semantics) it can never see SEED's value and must fail.
        let mut config = Config::default();
        config.env_file = vec!["seed.env".to_string()];
        config.parameters.insert(
            "SEED".to_string(),
            Parameter {
                param_type: Some("secret".to_string()),
                source: Some("manual".to_string()),
                ..Default::default()
            },
        );
        config.parameters.insert(
            "DERIVED".to_string(),
            Parameter {
                param_type: Some("secret".to_string()),
                generate: Some("printf %s {{SEED}}".to_string()),
                ..Default::default()
            },
        );

        let err = resolver
            .resolve_parameters(&mut config)
            .expect_err("a secret generator referencing a parameter must fail (v6.2)");
        assert!(
            matches!(err, Error::ParameterNotFound(ref n) if n == "SEED"),
            "must fail with ParameterNotFound(SEED), got: {err}"
        );
    }
}
