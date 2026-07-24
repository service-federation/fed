use super::{Orchestrator, RunContext};
use crate::config::Config;
use crate::error::Result;
#[cfg(test)]
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
/// use fed::{Config, Orchestrator, RunContext};
/// use fed::service::OutputMode;
/// use std::path::PathBuf;
///
/// # async fn example() -> Result<(), fed::Error> {
/// let config = Config::default();
/// let orchestrator = Orchestrator::builder()
///     .config(config)
///     .work_dir(PathBuf::from("."))
///     .run_context(RunContext { output_mode: OutputMode::Captured, ..Default::default() })
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
    /// Session-scoped run settings — see `RunContext`'s doc comment for the
    /// context-vs-operation-flag split. Applied to the built `Orchestrator`
    /// in one call to `apply_run_context`, replacing what used to be 5
    /// separate fields (`output_mode`, `secret_cache`, `is_interactive`,
    /// `offline`, `required_secret_names`, `profiles`).
    run_context: RunContext,
    auto_resolve_conflicts: bool,
    randomize_ports: bool,
    replace_mode: bool,
    dry_run: bool,
    readonly: bool,
    supervisor_attach: bool,
    isolation_id: Option<String>,
    startup_timeout: Option<Duration>,
    stop_timeout: Option<Duration>,
}

impl OrchestratorBuilder {
    /// Create a new builder with default settings.
    pub fn new() -> Self {
        Self {
            config: None,
            work_dir: None,
            run_context: RunContext::default(),
            auto_resolve_conflicts: false,
            randomize_ports: false,
            replace_mode: false,
            dry_run: false,
            readonly: false,
            supervisor_attach: false,
            isolation_id: None,
            startup_timeout: None,
            stop_timeout: None,
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

    /// Set the session-scoped run settings (offline, secret_cache,
    /// is_interactive, output_mode, profiles, required_secret_names) in one call. See
    /// `RunContext`'s doc comment for why these fields are grouped
    /// separately from the operation flags below.
    pub fn run_context(mut self, ctx: RunContext) -> Self {
        self.run_context = ctx;
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

    /// Enable readonly initialization.
    ///
    /// When enabled, `build()` calls `initialize_readonly()` instead of
    /// `initialize()`, skipping parameter resolution and Docker cleanup.
    /// Use this for read-only commands like `status`, `logs`, and `stop`.
    pub fn readonly(mut self, readonly: bool) -> Self {
        self.readonly = readonly;
        self
    }

    /// Enable supervisor-attach initialization.
    ///
    /// When enabled, `build()` calls `initialize_supervisor()` instead of
    /// `initialize()`/`initialize_readonly()`/`initialize_dry_run()` — the
    /// attach-and-reconcile path used exclusively by the `fed supervise`
    /// process (`07-supervisor.md` Design §1). This is the operation flag
    /// this plan adds alongside `.readonly()`/`.dry_run()`, per
    /// `01-run-context.md`'s categorization of per-orchestrator behavior
    /// switches as operation flags rather than `RunContext` session state —
    /// and per this plan's own re-anchoring note, it is the *only*
    /// construction path that keeps both `clippy::disallowed_methods` and
    /// `scripts/check-orchestrator-construction.sh` green, so no
    /// `ALLOW-RAW-ORCHESTRATOR-CONSTRUCTION` marker is needed anywhere for
    /// the supervisor's construction.
    pub fn supervisor_attach(mut self, supervisor_attach: bool) -> Self {
        self.supervisor_attach = supervisor_attach;
        self
    }

    /// Set an isolation ID for this orchestrator's Docker containers and
    /// persisted port scope.
    ///
    /// Must be applied before `initialize()` runs — `initialize()` only
    /// adopts a *persisted* isolation_id when none is already set
    /// (`core.rs`'s `initialize` doc comment), so a caller generating a
    /// fresh isolation_id up front (e.g. `fed isolate enable`/`rotate`)
    /// needs it in place before `build()`'s internal `initialize()` call —
    /// setting it afterward would resolve and persist ports under the
    /// wrong (previous/shared) scope.
    pub fn isolation_id(mut self, id: String) -> Self {
        self.isolation_id = Some(id);
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

        // Create orchestrator. This is the one sanctioned construction site —
        // clippy::disallowed_methods (clippy.toml) forbids Orchestrator::new/
        // new_ephemeral everywhere else so settings-threading always goes
        // through apply_run_context below, not hand-rolled setter calls.
        //
        // supervisor_attach also takes the new_ephemeral (in-memory, no
        // `.fed/.lock`) branch: initialize_supervisor() is responsible for
        // swapping in the real, unlocked, on-disk tracker
        // (`StateTracker::new_for_supervisor`) as its first step, so the
        // supervisor's construction never touches `.fed/.lock` at any point
        // — not even transiently during `Orchestrator::new`'s own
        // lock-then-immediately-drop, which would otherwise be a brief but
        // real acquisition (`07-supervisor.md` Design §1).
        let work_dir = self.work_dir.unwrap_or_else(|| PathBuf::from("."));
        #[allow(clippy::disallowed_methods)]
        let mut orchestrator = if self.dry_run || self.supervisor_attach {
            Orchestrator::new_ephemeral(config, work_dir).await?
        } else {
            Orchestrator::new(config, work_dir).await?
        };

        // Applies offline / secret_cache / is_interactive / output_mode /
        // profiles / required_secret_names in the order initialize() requires
        // (required_secret_names before anything that reads it — see
        // apply_run_context's doc comment).
        orchestrator.apply_run_context(&self.run_context);

        orchestrator.set_auto_resolve_conflicts(self.auto_resolve_conflicts);

        if self.randomize_ports {
            orchestrator.set_randomize_ports(true);
        }
        if self.replace_mode {
            orchestrator.set_replace_mode(true);
        }
        // Must precede initialize(): see isolation_id()'s doc comment.
        if let Some(id) = self.isolation_id {
            orchestrator.set_isolation_id(id);
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
        // - supervisor_attach: `fed supervise`'s attach-and-reconcile path
        //   (`07-supervisor.md` Design §1)
        // - default: full initialization
        if self.dry_run {
            orchestrator.initialize_dry_run().await?;
        } else if self.readonly {
            orchestrator.initialize_readonly().await?;
        } else if self.supervisor_attach {
            orchestrator.initialize_supervisor().await?;
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
            .run_context(RunContext {
                output_mode: OutputMode::Captured,
                ..Default::default()
            })
            .auto_resolve_conflicts(true)
            .build()
            .await;
        assert!(result.is_ok(), "Builder failed: {:?}", result.err());
    }

    /// The fourth arm added by `07-supervisor.md` Design §1:
    /// `.supervisor_attach(true)` must route through
    /// `Orchestrator::initialize_supervisor()`, not `initialize()`/
    /// `initialize_readonly()`/`initialize_dry_run()`. Verified two ways:
    /// the supervisor-safe tracker never holds `.fed/.lock` (unlike a
    /// normal `initialize()`-backed tracker would), and monitoring is
    /// started unconditionally (part of what `initialize_supervisor`
    /// alone does).
    #[tokio::test]
    async fn test_builder_supervisor_attach_arm() {
        let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
        let config = Config::default();
        let orchestrator = OrchestratorBuilder::new()
            .config(config)
            .work_dir(temp_dir.path().to_path_buf())
            .supervisor_attach(true)
            .build()
            .await
            .expect("supervisor_attach build should succeed on a fresh directory");

        assert!(
            !temp_dir.path().join(".fed").join(".lock").exists(),
            "supervisor_attach must never create/hold .fed/.lock"
        );

        assert!(
            orchestrator.monitoring_task.lock().await.is_some(),
            "initialize_supervisor must start monitoring unconditionally, \
             regardless of output mode"
        );
    }

    /// `.supervisor_attach(true)` takes priority in the same way `.dry_run()`
    /// and `.readonly()` do today — asserted here by checking the negative
    /// space: without the flag, a fresh directory build still goes through
    /// plain `initialize()`, which is exercised by the other tests in this
    /// module; this test exists so a future refactor that accidentally
    /// drops the `else if self.supervisor_attach` arm (falling through to
    /// plain `initialize()`) is caught by the `.fed/.lock` assertion above,
    /// not silently passing because `initialize_supervisor()` and
    /// `initialize()` both happen to succeed on an empty config.
    #[tokio::test]
    async fn test_builder_without_supervisor_attach_holds_normal_lock() {
        let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
        let config = Config::default();
        let _orchestrator = OrchestratorBuilder::new()
            .config(config)
            .work_dir(temp_dir.path().to_path_buf())
            .build()
            .await
            .expect("plain build should succeed on a fresh directory");

        assert!(
            temp_dir.path().join(".fed").join(".lock").exists(),
            "a plain (non-supervisor_attach) build must go through the normal, \
             locked StateTracker::new — .fed/.lock should exist"
        );
    }
}
