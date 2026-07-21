use super::Orchestrator;
use crate::config::Config;
use crate::error::Result;
use crate::service::OutputMode;
use std::path::PathBuf;
use std::time::Duration;

/// Builder for constructing an `Orchestrator` with a fluent API.
///
/// This builder pattern makes orchestrator initialization less error-prone by:
/// - Ensuring `initialize()` is called automatically
/// - Providing clear, chainable configuration methods
/// - Validating configuration before construction
///
/// # Example
///
/// ```no_run
/// use fed::{Config, Orchestrator};
/// use fed::service::OutputMode;
/// use std::path::PathBuf;
///
/// # async fn example() -> Result<(), fed::Error> {
/// let config = Config::default();
/// let orchestrator = Orchestrator::builder()
///     .config(config)
///     .work_dir(PathBuf::from("."))
///     .output_mode(OutputMode::Captured)
///     .auto_resolve_conflicts(true)
///     .build()
///     .await?;
/// // initialize() is called automatically
/// # Ok(())
/// # }
/// ```
pub struct OrchestratorBuilder {
    config: Option<Config>,
    work_dir: Option<PathBuf>,
    output_mode: OutputMode,
    auto_resolve_conflicts: bool,
    randomize_ports: bool,
    replace_mode: bool,
    dry_run: bool,
    readonly: bool,
    is_interactive: bool,
    offline: bool,
    profiles: Vec<String>,
    startup_timeout: Option<Duration>,
    stop_timeout: Option<Duration>,
    required_secret_names: Option<std::collections::HashSet<String>>,
    environment: Option<crate::config::Environment>,
}

impl OrchestratorBuilder {
    /// Create a new builder with default settings.
    pub fn new() -> Self {
        Self {
            config: None,
            work_dir: None,
            output_mode: OutputMode::default(),
            auto_resolve_conflicts: false,
            randomize_ports: false,
            replace_mode: false,
            dry_run: false,
            readonly: false,
            is_interactive: false,
            offline: false,
            profiles: Vec::new(),
            startup_timeout: None,
            stop_timeout: None,
            required_secret_names: None,
            environment: None,
        }
    }

    /// Set the configuration.
    ///
    /// This is required to build the orchestrator.
    pub fn config(mut self, config: Config) -> Self {
        self.config = Some(config);
        self
    }

    /// Set the working directory for services.
    ///
    /// If not set, defaults to the current directory (".").
    pub fn work_dir(mut self, dir: PathBuf) -> Self {
        self.work_dir = Some(dir);
        self
    }

    /// Set the output mode for process services.
    ///
    /// - `OutputMode::File`: Background mode, logs to files
    /// - `OutputMode::Captured`: Interactive mode, logs to memory (default)
    /// - `OutputMode::Passthrough`: Pass-through mode, inherit stdio
    pub fn output_mode(mut self, mode: OutputMode) -> Self {
        self.output_mode = mode;
        self
    }

    /// Enable auto-resolve mode for port conflicts.
    ///
    /// When enabled, port conflicts are resolved automatically without prompting.
    /// This is useful in TUI mode to avoid blocking on interactive prompts.
    pub fn auto_resolve_conflicts(mut self, auto_resolve: bool) -> Self {
        self.auto_resolve_conflicts = auto_resolve;
        self
    }

    /// Set active profiles for service filtering.
    ///
    /// Only services matching at least one of these profiles will be started.
    /// If no profiles are set, all services are included.
    pub fn profiles(mut self, profiles: Vec<String>) -> Self {
        self.profiles = profiles;
        self
    }

    /// Set the startup timeout for service operations.
    ///
    /// If not set, uses the default timeout (2 minutes).
    pub fn startup_timeout(mut self, timeout: Duration) -> Self {
        self.startup_timeout = Some(timeout);
        self
    }

    /// Set the stop timeout for service operations.
    ///
    /// If not set, uses the default timeout (30 seconds).
    pub fn stop_timeout(mut self, timeout: Duration) -> Self {
        self.stop_timeout = Some(timeout);
        self
    }

    /// Enable randomized port allocation.
    ///
    /// Skips persisted ports and allocates fresh random ports for all
    /// port-type parameters. Also enables auto-resolve to avoid interactive
    /// conflict prompts.
    pub fn randomize_ports(mut self, randomize: bool) -> Self {
        self.randomize_ports = randomize;
        self
    }

    /// Enable replace mode — kill blocking processes/containers and use original ports.
    ///
    /// Use this for `--replace` flag behavior.
    pub fn replace_mode(mut self, replace: bool) -> Self {
        self.replace_mode = replace;
        self
    }

    /// Enable dry-run initialization.
    ///
    /// When enabled, `build()` calls `initialize_dry_run()` and avoids
    /// persisting resolved ports.
    pub fn dry_run(mut self, dry_run: bool) -> Self {
        self.dry_run = dry_run;
        self
    }

    /// Set whether stdin is a TTY (for interactive prompts like secret generation).
    pub fn is_interactive(mut self, is_interactive: bool) -> Self {
        self.is_interactive = is_interactive;
        self
    }

    /// Offline mode: skip cloud vault lookups for manual secrets.
    pub fn offline(mut self, offline: bool) -> Self {
        self.offline = offline;
        self
    }

    /// Scope the vault query to the manual-secret names the target script
    /// transitively references.
    ///
    /// `None` (the default) fetches every missing manual secret — used for
    /// interactive `fed`, `fed start`, unknown commands, and the deprecated
    /// `generated_secrets_file` config. Pass `Some(set)` (derived by
    /// [`crate::parameter::scanner::required_parameter_names`]) for a specific
    /// script run so it never blocks on secrets it doesn't use.
    pub fn required_secret_names(
        mut self,
        names: Option<std::collections::HashSet<String>>,
    ) -> Self {
        self.required_secret_names = names;
        self
    }

    /// Set the environment for parameter resolution and vault fetches
    /// (development/staging/production).
    ///
    /// If not set, defaults to development. Selects which per-parameter
    /// environment value (`development:`/`staging:`/`production:`) resolves and
    /// which vault environment manual secrets are fetched from.
    pub fn environment(mut self, environment: crate::config::Environment) -> Self {
        self.environment = Some(environment);
        self
    }

    /// Enable readonly initialization.
    ///
    /// When enabled, `build()` calls `initialize_readonly()` instead of
    /// `initialize()`, skipping parameter resolution and Docker cleanup.
    /// Use this for read-only commands like `status`, `logs`, and `stop`.
    pub fn readonly(mut self, readonly: bool) -> Self {
        self.readonly = readonly;
        self
    }

    /// Build the orchestrator and initialize it.
    ///
    /// This method performs the following steps:
    /// 1. Validates that required fields are set
    /// 2. Creates the orchestrator instance
    /// 3. Applies optional configuration (work_dir, output_mode, etc.)
    /// 4. Calls `initialize()` automatically
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Config is not set
    /// - Initialization fails (see [`Orchestrator::initialize`])
    pub async fn build(self) -> Result<Orchestrator> {
        // Validate required fields
        let config = self
            .config
            .ok_or_else(|| crate::error::Error::Validation("config is required".to_string()))?;

        // Create orchestrator
        let work_dir = self.work_dir.unwrap_or_else(|| PathBuf::from("."));
        let mut orchestrator = if self.profiles.is_empty() {
            if self.dry_run {
                Orchestrator::new_ephemeral(config, work_dir).await?
            } else {
                Orchestrator::new(config, work_dir).await?
            }
        } else if self.dry_run {
            Orchestrator::new_ephemeral(config, work_dir)
                .await?
                .with_profiles(self.profiles)
        } else {
            Orchestrator::new(config, work_dir)
                .await?
                .with_profiles(self.profiles)
        };

        orchestrator.set_output_mode(self.output_mode);
        orchestrator.set_auto_resolve_conflicts(self.auto_resolve_conflicts);

        if self.randomize_ports {
            orchestrator.set_randomize_ports(true);
        }
        if self.replace_mode {
            orchestrator.set_replace_mode(true);
        }
        if self.is_interactive {
            orchestrator.set_is_interactive(true);
        }
        if self.offline {
            orchestrator.set_offline(true);
        }

        // Scope the vault query before initialize() runs secret resolution.
        // None keeps today's fetch-everything behavior.
        orchestrator.set_required_secret_names(self.required_secret_names);

        // Must precede initialize(): parameter resolution reads the environment.
        if let Some(environment) = self.environment {
            orchestrator.set_environment(environment);
        }

        if let Some(timeout) = self.startup_timeout {
            orchestrator.startup_timeout = timeout;
        }

        if let Some(timeout) = self.stop_timeout {
            orchestrator.stop_timeout = timeout;
        }

        // Initialize mode selection:
        // - dry_run: resolve-only preview path (no persistent state writes)
        // - readonly: status/logs path (skip parameter resolution)
        // - default: full initialization
        if self.dry_run {
            orchestrator.initialize_dry_run().await?;
        } else if self.readonly {
            orchestrator.initialize_readonly().await?;
        } else {
            orchestrator.initialize().await?;
        }

        Ok(orchestrator)
    }
}

impl Default for OrchestratorBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_builder_requires_config() {
        let result = OrchestratorBuilder::new().build().await;
        assert!(result.is_err());
        if let Err(e) = result {
            assert!(e.to_string().contains("config"));
        }
    }

    #[tokio::test]
    async fn test_builder_creates_orchestrator() {
        let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
        let config = Config::default();
        let result = OrchestratorBuilder::new()
            .config(config)
            .work_dir(temp_dir.path().to_path_buf())
            .build()
            .await;
        assert!(result.is_ok(), "Builder failed: {:?}", result.err());
    }

    #[tokio::test]
    async fn test_builder_fluent_api() {
        let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
        let config = Config::default();
        let result = OrchestratorBuilder::new()
            .config(config)
            .work_dir(temp_dir.path().to_path_buf())
            .output_mode(OutputMode::Captured)
            .auto_resolve_conflicts(true)
            .build()
            .await;
        assert!(result.is_ok(), "Builder failed: {:?}", result.err());
    }
}
