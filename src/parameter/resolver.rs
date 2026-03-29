use super::PortAllocator;
use crate::config::Config;
use crate::error::{Error, Result};
use crate::port::{handle_port_conflict, PortConflict, PortConflictAction};
use regex::Regex;
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::OnceLock;

/// Global template regex compiled once
static TEMPLATE_REGEX: OnceLock<Regex> = OnceLock::new();

fn get_template_regex() -> &'static Regex {
    TEMPLATE_REGEX
        .get_or_init(|| Regex::new(r"\{\{([^}]+)\}\}").expect("static regex pattern is valid"))
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
/// 2. Persisted port from previous run (if available)
/// 3. Preferred port from `default:` (if available)
/// 4. Random available port (fallback)
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
    /// Whether stdin is a TTY (for interactive secret generation prompts).
    is_interactive: bool,
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
            is_interactive: false,
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
            is_interactive: false,
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

    /// Enable replace mode - kill blocking processes/containers and use original ports.
    /// Use this for `--replace` flag behavior.
    pub fn set_replace_mode(&mut self, replace: bool) {
        self.replace_mode = replace;
    }

    /// Set whether stdin is a TTY (for interactive secret generation prompts).
    pub fn set_is_interactive(&mut self, is_interactive: bool) {
        self.is_interactive = is_interactive;
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
        if template.is_empty() {
            return Ok(String::new());
        }

        let mut result = template.to_string();

        for cap in get_template_regex().captures_iter(template) {
            let full_match = &cap[0]; // {{VAR}}
            let var_name = &cap[1]; // VAR

            let value = parameters
                .get(var_name)
                .ok_or_else(|| Error::ParameterNotFound(var_name.to_string()))?;

            result = result.replace(full_match, value);
        }

        Ok(result)
    }

    /// Resolve template placeholders with shell escaping for safe use in shell commands.
    /// Public for use in script execution at runtime.
    pub fn resolve_template_shell_safe(
        &self,
        template: &str,
        parameters: &HashMap<String, String>,
    ) -> Result<String> {
        if template.is_empty() {
            return Ok(String::new());
        }

        let mut result = template.to_string();

        for cap in get_template_regex().captures_iter(template) {
            let full_match = &cap[0]; // {{VAR}}
            let var_name = &cap[1]; // VAR

            let value = parameters
                .get(var_name)
                .ok_or_else(|| Error::ParameterNotFound(var_name.to_string()))?;

            // Apply shell escaping to the value before substitution
            let escaped_value = shell_escape(value);
            result = result.replace(full_match, &escaped_value);
        }

        Ok(result)
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

    /// Resolve secret parameters: generate missing auto-secrets, fail on missing manual secrets.
    ///
    /// This runs before `.env` loading so that newly-generated values are picked up
    /// by the normal `apply_env_file_to_parameters` path.
    fn resolve_secrets(&self, config: &mut Config) -> Result<()> {
        let work_dir = match self.work_dir.as_ref() {
            Some(dir) => dir.clone(),
            None => return Ok(()), // No work dir → skip (unit tests, etc.)
        };

        let secrets_file_path = config.generated_secrets_file.as_ref().map(|gsf| {
            let expanded = super::expand_tilde(Path::new(gsf));
            if expanded.is_absolute() {
                expanded
            } else {
                work_dir.join(expanded)
            }
        });

        let analysis = match super::secret::analyze_secrets(
            config,
            &work_dir,
            secrets_file_path.as_deref(),
        )? {
            Some(a) => a,
            None => return Ok(()), // No secret parameters at all
        };

        // Set optional manual secrets to empty string so they resolve without error
        for (name, param) in config.get_effective_parameters_mut() {
            if param.is_manual_secret()
                && param.is_optional()
                && param.value.is_none()
                && !analysis.existing_values.contains_key(name.as_str())
            {
                param.value = Some(String::new());
            }
        }

        // Fail on missing manual secrets — user must provide these
        if !analysis.missing_manual.is_empty() {
            let details: Vec<String> = analysis
                .missing_manual
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
            return Err(Error::Validation(format!(
                "Missing secret values — add them to your env_file ({}):\n{}\n\nThese secrets have source: manual, so fed won't generate them.",
                env_files_hint,
                details.join("\n")
            )));
        }

        // Prepend generated_secrets_file to env_file so it's loaded by apply_env_file_to_parameters
        // (lowest priority — user's .env files can override generated values).
        // Only prepend if the file exists — it may not on the very first run before generation.
        if let Some(gsf) = config.generated_secrets_file.as_ref() {
            let gsf_path = super::expand_tilde(Path::new(gsf));
            let gsf_abs = if gsf_path.is_absolute() {
                gsf_path
            } else {
                work_dir.join(gsf_path)
            };
            if gsf_abs.exists() && !config.env_file.contains(gsf) {
                config.env_file.insert(0, gsf.clone());
            }
        }

        // Run DAG-based resolution for all secrets with `generate` commands.
        // This handles invalidation cascading even for secrets that have existing values.
        let effective_params = config.get_effective_parameters().clone();
        let generate_results = super::generate::resolve_generate_params(
            &effective_params,
            &analysis.existing_values,
            &HashMap::new(),
        )?;

        let dag_generated: HashSet<String> = generate_results
            .iter()
            .map(|r| r.name.clone())
            .collect();

        let mut generated: Vec<(String, String)> = generate_results
            .into_iter()
            .filter(|r| r.was_generated)
            .map(|r| (r.name, r.value))
            .collect();

        // Simple secrets (no generate command) — use random alphanumeric.
        for name in &analysis.needs_generation {
            if !dag_generated.contains(name) {
                generated.push((name.clone(), super::secret::generate_secret()));
            }
        }

        // Nothing to write? We're done.
        if generated.is_empty() {
            return Ok(());
        }

        let gsf = config.generated_secrets_file.as_ref().expect(
            "generated_secrets_file must be set when auto-generated secrets exist (validation ensures this)"
        );

        // Gitignore gate: if we're in a git repo and the secrets file isn't ignored, refuse
        if analysis.in_git_repo && !analysis.is_gitignored {
            return Err(Error::Validation(format!(
                "Refusing to generate secrets: '{}' is not in .gitignore.\n\nAdd '{}' to your .gitignore to prevent committing secrets.",
                gsf, gsf
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

        // Ensure generated_secrets_file is in env_file now that the file exists
        if !config.env_file.contains(gsf) {
            config.env_file.insert(0, gsf.clone());
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
            let env_vars = crate::config::env_loader::load_env_file(&full_path).map_err(|e| {
                Error::TemplateResolution(format!(
                    "Failed to load environment file '{}' (resolved to '{}'): {}",
                    env_file_path,
                    full_path.display(),
                    e
                ))
            })?;

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

        // Resolve secrets first — may generate .env entries and add ".env" to config.env_file
        self.resolve_secrets(config)?;

        // Apply .env file values to parameters (strict mode: must be declared)
        self.apply_env_file_to_parameters(config)?;

        // Build parameters map - first pass for direct values and port allocation
        let mut parameters = HashMap::new();

        // Use effective parameters (variables take precedence over parameters)
        let effective_params = config.get_effective_parameters().clone();

        for (name, param) in &effective_params {
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
                // Port resolution via unified PortStore.
                // The store may be SQLite-backed or no-op (isolated).
                // One lookup path, one save path — no dual-cache priority chain.
                let (port, reason) = if let Some(cached_port) = self.port_store.get_port(name) {
                    // Port store has a cached value — validate it's still usable
                    self.validate_cached_port(cached_port, name)?
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
                if let Some(env_value) = param.get_value_for_environment(&self.environment) {
                    let default_str = Self::value_to_string(env_value);
                    if default_str.contains("{{") {
                        if let Ok(resolved_default) =
                            self.resolve_template(&default_str, &parameters)
                        {
                            if resolved_default != default_str {
                                parameters.insert(name.clone(), resolved_default.clone());
                                self.resolved_parameters
                                    .insert(name.clone(), resolved_default);
                                any_resolved = true;
                            }
                        }
                    }
                }
            }

            if !any_resolved {
                break;
            }
        }

        // Check for unresolved templates (circular references or missing parameters)
        for (name, param) in &effective_params {
            if let Some(env_value) = param.get_value_for_environment(&self.environment) {
                let default_str = Self::value_to_string(env_value);
                if default_str.contains("{{") {
                    // Check if parameter was resolved
                    if let Some(resolved_value) = parameters.get(name) {
                        if resolved_value.contains("{{") {
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
        }

        // Resolve non-secret `generate` parameters (recompute every start).
        // Secret generate params were already handled in resolve_secrets.
        let non_secret_generate: HashMap<String, crate::config::Parameter> = effective_params
            .iter()
            .filter(|(_, p)| p.has_generate() && !p.is_secret_type())
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
                    self.resolved_parameters
                        .insert(result.name, result.value);
                }
            }
        }

        // Validate 'either' constraints
        for (name, param) in &effective_params {
            if !param.either.is_empty() {
                if let Some(resolved_value) = parameters.get(name) {
                    if !param.either.contains(resolved_value) {
                        return Err(Error::TemplateResolution(format!(
                            "Parameter '{}' has value '{}' which is not in the allowed values: {:?}",
                            name, resolved_value, param.either
                        )));
                    }
                }
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
        for (name, service) in &mut resolved.services {
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

    /// Allocate a fresh port from the config default or a random one.
    ///
    /// Called when the port store has no cached value for this parameter.
    fn allocate_fresh_port(
        &mut self,
        param: &crate::config::Parameter,
        param_name: &str,
    ) -> Result<(u16, PortResolutionReason)> {
        if self.force_random_ports {
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

        resolver.resolve_parameters(&mut config).unwrap();

        let resolved = resolver.get_resolved_parameters();
        let port_str = resolved.get("API_PORT").unwrap();
        let port: u16 = port_str.parse().unwrap();

        // Should have allocated a random port (fallback due to invalid default)
        assert!(port > 0);
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
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("invalid port value"));
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
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("invalid port value '0'"));
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
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("invalid port value"));
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
        config.generated_secrets_file = Some(".env.secrets".to_string());
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
        // Init a git repo WITHOUT .gitignore
        git2::Repository::init(temp_dir.path()).unwrap();

        let mut resolver = Resolver::new();
        resolver.set_work_dir(temp_dir.path());

        let mut config = Config::default();
        config.generated_secrets_file = Some(".env.secrets".to_string());
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
    }

    #[test]
    fn secret_auto_generated_when_gitignored() {
        use crate::config::{Config, Parameter};
        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();
        // Init repo with .env.secrets in .gitignore
        git2::Repository::init(temp_dir.path()).unwrap();
        std::fs::write(temp_dir.path().join(".gitignore"), ".env.secrets\n").unwrap();

        let mut resolver = Resolver::new();
        resolver.set_work_dir(temp_dir.path());

        let mut config = Config::default();
        config.generated_secrets_file = Some(".env.secrets".to_string());
        config.parameters.insert(
            "SESSION_KEY".to_string(),
            Parameter {
                param_type: Some("secret".to_string()),
                ..Default::default()
            },
        );

        // Non-TTY mode → should auto-generate without prompting
        resolver.resolve_parameters(&mut config).unwrap();

        let resolved = resolver.get_resolved_parameters();
        let value = resolved.get("SESSION_KEY").unwrap();
        assert_eq!(value.len(), 32, "Generated secret should be 32 chars");
        assert!(value.chars().all(|c| c.is_ascii_alphanumeric()));

        // secrets file should exist
        let env_content = std::fs::read_to_string(temp_dir.path().join(".env.secrets")).unwrap();
        assert!(env_content.contains("SESSION_KEY="));

        // generated_secrets_file should be prepended to env_file
        assert_eq!(config.env_file[0], ".env.secrets");
    }

    #[test]
    fn existing_secrets_file_loaded_on_subsequent_run() {
        use crate::config::{Config, Parameter};
        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();
        git2::Repository::init(temp_dir.path()).unwrap();
        std::fs::write(temp_dir.path().join(".gitignore"), ".env.secrets\n").unwrap();

        // Simulate a previous run that already generated the secrets file
        std::fs::write(
            temp_dir.path().join(".env.secrets"),
            "SESSION_KEY=previously_generated_value\n",
        )
        .unwrap();

        let mut resolver = Resolver::new();
        resolver.set_work_dir(temp_dir.path());

        let mut config = Config::default();
        config.generated_secrets_file = Some(".env.secrets".to_string());
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
            "Should load secret from existing .env.secrets on subsequent runs"
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
}
