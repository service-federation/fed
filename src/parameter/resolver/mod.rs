use super::PortAllocator;
use crate::config::Config;
use crate::error::{Error, Result};
use crate::port::{PortConflict, PortConflictAction, handle_port_conflict};
use regex::Regex;
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::OnceLock;

mod env_file;
mod ports;
mod secrets;
mod template;

pub use ports::{PortResolution, PortResolutionReason};
pub(crate) use template::get_template_regex;
pub(crate) use template::shell_escape;

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

/// Resolver handles parameter resolution and template substitution.
///
/// The resolver is responsible for:
/// - Resolving `{{parameter}}` template syntax in configuration values
/// - Allocating ports for `type: port` parameters with TOCTOU prevention
/// - Loading values from `.env` files (strict mode: variables must be declared)
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
    /// Names of sensitive parameters: declared `type: secret` plus everything
    /// transitively derived from one (see [`super::sensitivity`]). Computed in
    /// `resolve_parameters` and consumed by [`Self::get_parameter_views`] so
    /// display surfaces never receive raw secret material.
    sensitive_params: HashSet<String>,
}

impl Resolver {
    pub fn new() -> Self {
        Self {
            port_allocator: PortAllocator::new(),
            resolved_parameters: HashMap::new(),
            port_parameter_names: Vec::new(),
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
            sensitive_params: HashSet::new(),
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

    /// Whether this resolver was told stdin is a TTY. Used to propagate
    /// `is_interactive` into a `RunContext` read back off an orchestrator
    /// (`Orchestrator::current_run_context`).
    pub fn get_is_interactive(&self) -> bool {
        self.is_interactive
    }

    /// Offline mode: never call the cloud vault for manual secrets.
    pub fn set_offline(&mut self, offline: bool) {
        self.offline = offline;
    }

    /// Whether this resolver is in offline mode.
    pub fn get_offline(&self) -> bool {
        self.offline
    }

    /// Register ports owned by already-running managed services.
    ///
    /// These ports are trusted during resolution without bind-checking, because
    /// the port is held by a service we manage. This prevents `fed start` from
    /// prompting to kill our own services when they're already running.
    pub fn set_managed_ports(&mut self, ports: HashSet<u16>) {
        self.managed_ports = ports;
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

        // Classify sensitivity from declarations and provenance up front, so
        // it reflects the config as declared — independent of which values end
        // up resolved, deferred, or defaulted below.
        self.sensitive_params = super::sensitivity::sensitive_parameter_names(config);

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
                if let Some(default_value) = param.default.as_ref() {
                    let default_str = Self::value_to_string(default_value);
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
            } else if let Some(default_value) = param.default.as_ref() {
                // Non-port parameter with a default value
                let default_str = Self::value_to_string(default_value);
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
                if let Some(default_value) = param.default.as_ref() {
                    let default_str = Self::value_to_string(default_value);
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
            if let Some(default_value) = param.default.as_ref() {
                let default_str = Self::value_to_string(default_value);
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
                } else if let Some(default_value) = param.default.as_ref() {
                    params.insert(name.clone(), Self::value_to_string(default_value));
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

    /// Get resolved parameters
    pub fn get_resolved_parameters(&self) -> &HashMap<String, String> {
        &self.resolved_parameters
    }

    /// Names of sensitive parameters (declared `type: secret`, transitively
    /// derived from one, or caught by the name heuristic). Populated by
    /// [`Self::resolve_parameters`].
    pub fn get_sensitive_parameter_names(&self) -> &HashSet<String> {
        &self.sensitive_params
    }

    /// Resolved parameters as display views, sorted by name.
    ///
    /// Sensitive parameters come back as [`super::ParameterValue::Redacted`]
    /// with no raw material attached — this is the boundary display surfaces
    /// (the TUI) should consume instead of [`Self::get_resolved_parameters`].
    pub fn get_parameter_views(&self) -> Vec<super::ParameterView> {
        let mut views: Vec<super::ParameterView> = self
            .resolved_parameters
            .iter()
            .map(|(name, value)| super::ParameterView::new(name, value, &self.sensitive_params))
            .collect();
        views.sort_by(|a, b| a.name.cmp(&b.name));
        views
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
    fn test_circular_parameter_detection_simple() {
        use crate::config::{Config, Parameter};

        let mut resolver = Resolver::new();
        let mut config = Config::default();

        // Create circular reference: A -> B -> A
        let param_a = Parameter {
            param_type: None,
            default: Some(serde_yaml::Value::String("{{B}}".to_string())),
            either: vec![],
            source: None,
            description: None,
            optional: None,
            ..Default::default()
        };

        let param_b = Parameter {
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
            param_type: None,
            default: Some(serde_yaml::Value::String("{{B}}".to_string())),
            either: vec![],
            source: None,
            description: None,
            optional: None,
            ..Default::default()
        };

        let param_b = Parameter {
            param_type: None,
            default: Some(serde_yaml::Value::String("{{C}}".to_string())),
            either: vec![],
            source: None,
            description: None,
            optional: None,
            ..Default::default()
        };

        let param_c = Parameter {
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
            param_type: None,
            default: Some(serde_yaml::Value::String("value_c".to_string())),
            either: vec![],
            source: None,
            description: None,
            optional: None,
            ..Default::default()
        };

        let param_b = Parameter {
            param_type: None,
            default: Some(serde_yaml::Value::String("{{C}}".to_string())),
            either: vec![],
            source: None,
            description: None,
            optional: None,
            ..Default::default()
        };

        let param_a = Parameter {
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
    fn parameter_views_redact_secrets_and_derived_values() {
        use crate::config::{Config, Parameter};

        const SENTINEL: &str = "sentinel-raw-secret-4242";

        let mut resolver = Resolver::new();
        let mut config = Config::default();

        // A declared secret whose name contains no secret-like substring.
        let mut license = Parameter {
            param_type: Some("secret".to_string()),
            ..Default::default()
        };
        license.value = Some(SENTINEL.to_string());
        config.parameters.insert("LICENSE".to_string(), license);

        // A parameter derived from the secret via its default template.
        config.parameters.insert(
            "CONNECTION".to_string(),
            Parameter {
                default: Some(serde_yaml::Value::String(
                    "db://user:{{LICENSE}}@localhost".to_string(),
                )),
                ..Default::default()
            },
        );

        // An ordinary parameter whose name merely contains a KEY token.
        config.parameters.insert(
            "KEY_COUNT".to_string(),
            Parameter {
                default: Some(serde_yaml::Value::String("7".to_string())),
                ..Default::default()
            },
        );

        resolver.resolve_parameters(&mut config).unwrap();

        let views = resolver.get_parameter_views();
        let get = |name: &str| views.iter().find(|v| v.name == name).unwrap();

        assert!(get("LICENSE").value.is_sensitive());
        assert_eq!(get("LICENSE").value.clipboard_payload(), None);
        assert!(get("CONNECTION").value.is_sensitive());
        assert_eq!(get("CONNECTION").value.clipboard_payload(), None);
        assert!(!get("KEY_COUNT").value.is_sensitive());
        assert_eq!(get("KEY_COUNT").value.clipboard_payload(), Some("7"));

        // Views are sorted by name for deterministic display indexing.
        let names: Vec<&str> = views.iter().map(|v| v.name.as_str()).collect();
        let mut sorted = names.clone();
        sorted.sort();
        assert_eq!(names, sorted);

        // No view (including its Debug representation) carries the raw value.
        let debug_dump = format!("{:?}", views);
        assert!(!debug_dump.contains(SENTINEL));
    }

    #[test]
    fn test_either_constraint_with_template() {
        use crate::config::{Config, Parameter};

        let mut resolver = Resolver::new();
        let mut config = Config::default();

        // Create base parameter
        let base_param = Parameter {
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
}
