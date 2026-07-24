use crate::config::{Config, RestartPolicy, ServiceType};
use crate::dependency::{ExternalServiceExpander, Graph};
use crate::error::{Error, Result};
use crate::parameter::Resolver;
use crate::service::{OutputMode, ServiceManager, Status};
use crate::state::{DesiredState, ServiceState, StateTracker};
use tokio_util::sync::CancellationToken;
use tracing::Instrument;
// Using tokio::sync::RwLock for async-aware locking
// Previous implementation used parking_lot::RwLock which required HashMap removal pattern
// to avoid blocking tokio threads across .await points.
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use super::health::{SharedHealthCheckerRegistry, StartHealth, StartOutcome};

/// Default timeout for service startup operations (2 minutes)
pub const DEFAULT_STARTUP_TIMEOUT: Duration = Duration::from_secs(120);

/// Default timeout for service stop operations (30 seconds)
pub const DEFAULT_STOP_TIMEOUT: Duration = Duration::from_secs(30);

/// How long a start that lost the registration race waits for the winning
/// attempt to resolve. Generous, because the winner may be running install/
/// migrate hooks; bounded, because a hard-killed winner leaves a `Starting`
/// row behind that would otherwise hang us forever. Deliberately the same
/// constant liveness sweeps use to exempt young `Starting` rows: past this
/// age both the waiter and the sweeper agree the row is dead.
const CONCURRENT_START_WAIT_TIMEOUT: Duration = crate::state::STARTING_STALE_GRACE;

/// Type alias for the shared service registry
pub(super) type SharedServiceManager = Arc<tokio::sync::Mutex<Box<dyn ServiceManager>>>;
pub(super) type ServiceRegistry = HashMap<String, SharedServiceManager>;
/// Type alias for the shared, async-safe service registry
pub(super) type SharedServiceRegistry = Arc<tokio::sync::RwLock<ServiceRegistry>>;

/// The central coordinator for managing service lifecycles in Service Federation.
///
/// The Orchestrator is responsible for:
/// - Starting services in dependency-aware order
/// - Stopping services (and their dependents) gracefully
/// - Monitoring service health and restarting failed services
/// - Managing port allocation and TOCTOU race prevention
/// - Running install commands and scripts
///
/// # Concurrency Model
///
/// The Orchestrator is designed for safe concurrent access:
/// - Most methods take `&self` instead of `&mut self`
/// - Interior mutability is used via `RwLock`, `Mutex`, and atomic types
/// - A `CancellationToken` allows graceful cancellation of in-progress operations
/// - Operations can be cancelled by calling `cancel_operations()`
///
/// # Lock Ordering (to prevent deadlocks)
///
/// When acquiring multiple locks, always acquire in this order:
/// 1. `services` (RwLock)
/// 2. `health_checkers` (RwLock)
/// 3. `state_tracker` (RwLock)
/// 4. Individual service `Mutex`es (from services map)
///
/// # Example
///
/// ```no_run
/// use fed::{Config, Orchestrator};
///
/// # async fn example() -> Result<(), fed::Error> {
/// let config = Config::default(); // Load from YAML in practice
/// let mut orchestrator = Orchestrator::new(config, std::path::PathBuf::from(".")).await?;
/// orchestrator.initialize().await?;
/// orchestrator.start_all().await?;
///
/// // When shutting down:
/// orchestrator.cleanup().await;
/// # Ok(())
/// # }
/// ```
///
/// # Service Lifecycle
///
/// 1. **Initialization**: Load config, resolve parameters, create service managers
/// 2. **Start**: Start services in parallel groups respecting dependencies
/// 3. **Monitor**: Continuously check health and restart failed services
/// 4. **Stop**: Stop services in reverse dependency order
pub struct Orchestrator {
    pub(super) config: Config,
    /// Original unresolved config - used by isolated to create child orchestrators
    /// that can re-resolve templates with fresh port allocations.
    pub(super) original_config: Option<Config>,
    pub(super) resolver: Resolver,
    dep_graph: Graph,
    pub(super) services: SharedServiceRegistry,
    pub(super) health_checkers: SharedHealthCheckerRegistry,
    pub(super) work_dir: PathBuf,
    pub state_tracker: Arc<tokio::sync::RwLock<StateTracker>>,
    namespace: String,
    /// Output mode for process services.
    pub(super) output_mode: OutputMode,
    active_profiles: Vec<String>,
    pub(super) monitoring_task: Arc<tokio::sync::Mutex<Option<tokio::task::JoinHandle<()>>>>,
    pub(super) startup_complete: Arc<AtomicBool>,
    /// Track if port listeners have been released (to prevent TOCTOU races)
    port_listeners_released: AtomicBool,
    /// Cancellation token for in-progress operations.
    /// Call `cancel_operations()` to cancel all ongoing start/stop operations.
    pub(super) cancellation_token: CancellationToken,
    /// Timeout for service startup operations
    pub startup_timeout: Duration,
    /// Timeout for service stop operations
    pub stop_timeout: Duration,
    /// Guard to ensure cleanup runs exactly once
    cleanup_started: AtomicBool,
    /// Guard to ensure `stop_monitoring_only` runs exactly once. Separate
    /// from `cleanup_started` — the two shutdown paths are mutually
    /// exclusive by construction (a supervisor orchestrator calls only
    /// `stop_monitoring_only`, never `cleanup`, since `cleanup` stops every
    /// service — see its own doc comment), so sharing one guard would let an
    /// unrelated future caller of the other method silently no-op.
    monitoring_stop_started: AtomicBool,
    /// When true, skip port cache and allocate fresh random ports
    randomize_ports: bool,
    /// When set, Docker containers use this ID instead of the work-dir hash.
    /// Used by isolated script execution to give child orchestrators their own
    /// container namespace, preventing collisions with parent containers.
    pub(super) isolation_id: Option<String>,
    /// Names of services whose start THIS orchestrator instance owns: it won
    /// the cross-process registration race (or ran a oneshot's hooks). The
    /// failure-path [`Orchestrator::cleanup`] scopes its teardown to this set
    /// — services registered by a concurrent winning `fed` process, or left
    /// running from a previous start, must survive this run's failed start.
    /// Leaf lock: never held across an await or another lock acquisition.
    owned_services: std::sync::Mutex<std::collections::HashSet<String>>,
}

impl Orchestrator {
    /// Create a builder for constructing an `Orchestrator` with a fluent API.
    ///
    /// This is the preferred way to create an orchestrator as it automatically
    /// calls `initialize()` and provides a cleaner API.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use fed::{Config, Orchestrator, RunContext};
    /// use fed::service::OutputMode;
    ///
    /// # async fn example() -> Result<(), fed::Error> {
    /// let config = Config::default();
    /// let orchestrator = Orchestrator::builder()
    ///     .config(config)
    ///     .run_context(RunContext { output_mode: OutputMode::Captured, ..Default::default() })
    ///     .build()
    ///     .await?;
    /// // initialize() is called automatically
    /// # Ok(())
    /// # }
    /// ```
    pub fn builder() -> crate::orchestrator::OrchestratorBuilder {
        crate::orchestrator::OrchestratorBuilder::new()
    }

    /// Create a new orchestrator from configuration
    pub async fn new(config: Config, work_dir: PathBuf) -> Result<Self> {
        // Store original config for isolated child orchestrators
        let original_config = Some(config.clone());
        Ok(Self {
            config,
            original_config,
            resolver: Resolver::new(),
            dep_graph: Graph::new(),
            services: Arc::new(tokio::sync::RwLock::new(HashMap::new())),
            health_checkers: Arc::new(tokio::sync::RwLock::new(HashMap::new())),
            state_tracker: Arc::new(tokio::sync::RwLock::new(
                StateTracker::new(work_dir.clone()).await?,
            )),
            work_dir,
            namespace: "root".to_string(),
            output_mode: OutputMode::default(),

            active_profiles: Vec::new(),
            monitoring_task: Arc::new(tokio::sync::Mutex::new(None)),
            startup_complete: Arc::new(AtomicBool::new(false)),
            port_listeners_released: AtomicBool::new(false),
            cancellation_token: CancellationToken::new(),
            startup_timeout: DEFAULT_STARTUP_TIMEOUT,
            stop_timeout: DEFAULT_STOP_TIMEOUT,
            cleanup_started: AtomicBool::new(false),
            monitoring_stop_started: AtomicBool::new(false),
            randomize_ports: false,
            isolation_id: None,
            owned_services: std::sync::Mutex::new(std::collections::HashSet::new()),
        })
    }

    /// Create an ephemeral orchestrator with an in-memory state tracker.
    ///
    /// Used for isolated script execution where the child orchestrator should
    /// not touch the parent's `.fed/lock.db`. All state operations stay in-memory
    /// and are discarded when the orchestrator is dropped.
    pub async fn new_ephemeral(config: Config, work_dir: PathBuf) -> Result<Self> {
        let original_config = Some(config.clone());
        Ok(Self {
            config,
            original_config,
            resolver: Resolver::new(),
            dep_graph: Graph::new(),
            services: Arc::new(tokio::sync::RwLock::new(HashMap::new())),
            health_checkers: Arc::new(tokio::sync::RwLock::new(HashMap::new())),
            state_tracker: Arc::new(tokio::sync::RwLock::new(
                StateTracker::new_ephemeral().await?,
            )),
            work_dir,
            namespace: "root".to_string(),
            output_mode: OutputMode::default(),
            active_profiles: Vec::new(),
            monitoring_task: Arc::new(tokio::sync::Mutex::new(None)),
            startup_complete: Arc::new(AtomicBool::new(false)),
            port_listeners_released: AtomicBool::new(false),
            cancellation_token: CancellationToken::new(),
            startup_timeout: DEFAULT_STARTUP_TIMEOUT,
            stop_timeout: DEFAULT_STOP_TIMEOUT,
            cleanup_started: AtomicBool::new(false),
            monitoring_stop_started: AtomicBool::new(false),
            randomize_ports: false,
            isolation_id: None,
            owned_services: std::sync::Mutex::new(std::collections::HashSet::new()),
        })
    }

    /// Create a nested orchestrator with a namespace (for external services)
    pub async fn new_with_namespace(
        config: Config,
        namespace: String,
        work_dir: PathBuf,
    ) -> Result<Self> {
        // Store original config for isolated child orchestrators
        let original_config = Some(config.clone());
        Ok(Self {
            config,
            original_config,
            resolver: Resolver::new(),
            dep_graph: Graph::new(),
            services: Arc::new(tokio::sync::RwLock::new(HashMap::new())),
            health_checkers: Arc::new(tokio::sync::RwLock::new(HashMap::new())),
            state_tracker: Arc::new(tokio::sync::RwLock::new(
                StateTracker::new(work_dir.clone()).await?,
            )),
            work_dir,
            namespace,
            output_mode: OutputMode::default(),

            active_profiles: Vec::new(),
            monitoring_task: Arc::new(tokio::sync::Mutex::new(None)),
            startup_complete: Arc::new(AtomicBool::new(false)),
            port_listeners_released: AtomicBool::new(false),
            cancellation_token: CancellationToken::new(),
            startup_timeout: DEFAULT_STARTUP_TIMEOUT,
            stop_timeout: DEFAULT_STOP_TIMEOUT,
            cleanup_started: AtomicBool::new(false),
            monitoring_stop_started: AtomicBool::new(false),
            randomize_ports: false,
            isolation_id: None,
            owned_services: std::sync::Mutex::new(std::collections::HashSet::new()),
        })
    }

    /// Get the working directory for services
    pub fn work_dir(&self) -> &std::path::Path {
        &self.work_dir
    }

    /// Release port listeners once, just before services start.
    fn release_port_listeners_once(&self) {
        super::ports::release_port_listeners_once(&self.port_listeners_released, &self.resolver);
    }

    /// Cancel all in-progress operations.
    ///
    /// This will cause any ongoing `start`, `stop`, or `start_all` operations
    /// to return `Error::Cancelled`. The cancellation is cooperative - operations
    /// check the cancellation token at key points and exit gracefully.
    ///
    /// After cancellation, operations may leave services in a partially started
    /// or stopped state. Call `stop_all()` after cancellation to ensure cleanup.
    pub fn cancel_operations(&self) {
        self.cancellation_token.cancel();
    }

    /// Check if operations have been cancelled.
    pub fn is_cancelled(&self) -> bool {
        self.cancellation_token.is_cancelled()
    }

    /// Reset the cancellation token for new operations.
    ///
    /// This should be called after handling a cancellation if you want to
    /// perform new operations on the orchestrator.
    pub fn reset_cancellation(&mut self) {
        self.cancellation_token = CancellationToken::new();
    }

    /// Get a child cancellation token for use in spawned tasks.
    ///
    /// Child tokens are automatically cancelled when the parent is cancelled.
    pub fn child_token(&self) -> CancellationToken {
        self.cancellation_token.child_token()
    }

    /// Set the working directory for services
    pub async fn set_work_dir(&mut self, dir: PathBuf) -> Result<()> {
        self.work_dir = dir.clone();
        // Create new state tracker for the target directory.
        // Note: Both state trackers may briefly hold locks if directories differ,
        // but this is fine since they're different lock files. If they're the same
        // directory, the lock acquisition will gracefully handle the conflict.
        self.state_tracker = Arc::new(tokio::sync::RwLock::new(StateTracker::new(dir).await?));
        Ok(())
    }

    /// Set the output mode for process services.
    ///
    /// - `OutputMode::File`: Background mode, logs to files (used by `fed start`)
    /// - `OutputMode::Captured`: Interactive mode, logs to memory (used by TUI, `fed start -w`)
    /// - `OutputMode::Passthrough`: Pass-through mode, inherit stdio (used for testing, CI/CD)
    pub fn set_output_mode(&mut self, mode: OutputMode) {
        self.output_mode = mode;
    }

    /// Enable auto-resolve mode for port conflicts (use in TUI mode to avoid interactive prompts)
    pub fn set_auto_resolve_conflicts(&mut self, auto_resolve: bool) {
        self.resolver.set_auto_resolve_conflicts(auto_resolve);
    }

    /// Enable replace mode - kill blocking processes/containers and use original ports.
    /// Use this for `--replace` flag behavior.
    pub fn set_replace_mode(&mut self, replace: bool) {
        self.resolver.set_replace_mode(replace);
    }

    /// Set whether stdin is interactive (for interactive prompts like secret generation).
    pub fn set_is_interactive(&mut self, interactive: bool) {
        self.resolver.set_is_interactive(interactive);
    }

    pub fn set_offline(&mut self, offline: bool) {
        self.resolver.set_offline(offline);
    }

    pub fn set_secret_cache(&mut self, mode: super::SecretCacheMode) {
        self.resolver.set_secret_cache(mode);
    }

    pub fn get_secret_cache(&self) -> super::SecretCacheMode {
        self.resolver.get_secret_cache()
    }

    /// Whether this orchestrator resolves secrets offline. Used to propagate
    /// `--offline` into isolated-child orchestrators.
    pub fn get_offline(&self) -> bool {
        self.resolver.get_offline()
    }

    /// Scope the vault query to the manual-secret names the target script
    /// transitively references. `None` fetches every missing manual secret (the
    /// safe default). Must be set before [`Orchestrator::initialize`], which
    /// runs secret resolution.
    pub fn set_required_secret_names(&mut self, names: Option<std::collections::HashSet<String>>) {
        self.resolver.set_required_names(names);
    }

    /// The secret scope actually stored on this orchestrator's resolver. Used to
    /// propagate a parent run's scope verbatim into an ephemeral isolated-child
    /// orchestrator, so a public-API caller who set a custom scope (or `None`)
    /// gets a child that inherits it instead of re-deriving its own.
    pub fn get_required_secret_names(&self) -> Option<std::collections::HashSet<String>> {
        self.resolver.required_names()
    }

    /// Enable randomized port allocation.
    ///
    /// Skips persisted ports and allocates fresh random ports for all
    /// port-type parameters. Also enables auto-resolve to avoid interactive
    /// conflict prompts. Useful for running a second instance of the same
    /// project in a different worktree.
    pub fn set_randomize_ports(&mut self, randomize: bool) {
        self.randomize_ports = randomize;
        self.resolver.set_auto_resolve_conflicts(randomize);
        self.resolver.set_force_random_ports(randomize);
    }

    /// Set an isolation ID for this orchestrator's Docker containers.
    ///
    /// When set, Docker containers are named using this ID instead of the
    /// work-dir hash, giving isolated scripts their own container namespace
    /// and preventing collisions with the parent orchestrator's containers.
    pub fn set_isolation_id(&mut self, id: String) {
        self.isolation_id = Some(id);
    }

    /// Apply every field of `ctx` to this orchestrator via the existing
    /// per-field setters (plus a direct write to the private
    /// `active_profiles` field, which has no post-construction setter
    /// today), in the order `initialize()` requires (required_secret_names
    /// before anything that reads them). This is the single place that
    /// encodes that ordering constraint; every other caller —
    /// `OrchestratorBuilder::build()` and isolated-script child
    /// orchestrators (`scripts.rs`) — calls this instead of re-deriving the
    /// order.
    ///
    /// Every `RunContext` field is applied here, including `profiles` —
    /// omitting it would silently drop profile-gated services from an
    /// isolated-script child's config (the child is built from the
    /// parent's unfiltered `original_config`, so `active_profiles`
    /// defaulting to empty means every profile-gated service is filtered
    /// out before `depends_on` is ever resolved).
    pub fn apply_run_context(&mut self, ctx: &super::RunContext) {
        self.set_required_secret_names(ctx.required_secret_names.clone());
        self.set_offline(ctx.offline);
        self.set_secret_cache(ctx.secret_cache);
        self.set_is_interactive(ctx.is_interactive);
        self.set_output_mode(ctx.output_mode);
        self.active_profiles = ctx.profiles.clone();
    }

    /// Build a `RunContext` describing this orchestrator's current session
    /// settings — the mirror of `apply_run_context`, used to inherit a
    /// parent's context into a child instead of copying fields one by one.
    ///
    /// Every field here round-trips losslessly through `apply_run_context`
    /// — this method does not invent or default any value it can't
    /// actually read back off `self`.
    pub fn current_run_context(&self) -> super::RunContext {
        super::RunContext {
            offline: self.get_offline(),
            secret_cache: self.get_secret_cache(),
            is_interactive: self.resolver.get_is_interactive(),
            output_mode: self.output_mode,
            profiles: self.active_profiles.clone(),
            required_secret_names: self.get_required_secret_names(),
        }
    }

    /// Set active profiles for service filtering
    pub fn with_profiles(mut self, profiles: Vec<String>) -> Self {
        self.active_profiles = profiles;
        self
    }

    /// Remove orphaned processes for this project.
    ///
    /// Finds processes with PIDs in state DB that are still running
    /// but the service is marked as stopped. Kills them with SIGKILL.
    ///
    /// Returns the number of processes killed.
    ///
    /// Delegates to `OrphanCleaner`.
    pub async fn remove_orphaned_processes(&self) -> usize {
        let cleaner = super::orphans::OrphanCleaner::new(self);
        cleaner.remove_orphaned_processes().await
    }

    /// Collect ports owned by running managed services.
    ///
    /// Safe to call after `state_tracker.initialize()` because dead services are
    /// marked as `stale` (not deleted) — their port_allocations remain readable.
    /// Call `purge_stale_services()` after this method to clean up.
    ///
    /// Collect ports owned by running managed services so the resolver can
    /// avoid re-allocating them.
    async fn collect_managed_ports(&mut self) {
        super::ports::collect_managed_ports(
            &mut self.resolver,
            &self.state_tracker,
            self.isolation_id.as_deref(),
        )
        .await;
    }

    /// Configure resolver port store for the current mode.
    ///
    /// When `read_only` is true, the resolver receives an in-memory snapshot so
    /// `resolve_parameters()` cannot mutate SQLite state.
    async fn configure_port_store(&mut self, read_only: bool) {
        let scope = self.isolation_id.clone();
        let port_store: Box<dyn crate::port::PortStore> = if self.randomize_ports {
            tracing::debug!("Randomize mode: using NoopPortStore for fresh allocation");
            Box::new(crate::port::NoopPortStore)
        } else {
            let persisted_ports = if read_only {
                Self::load_persisted_ports_read_only(&self.work_dir, scope.as_deref())
            } else {
                self.state_tracker
                    .read()
                    .await
                    .get_global_port_allocations(scope.as_deref())
                    .await
            };
            if !persisted_ports.is_empty() {
                tracing::debug!(
                    "Using SqlitePortStore with {} persisted port(s)",
                    persisted_ports.len()
                );
            }
            Box::new(crate::port::SqlitePortStore::new(persisted_ports))
        };
        self.resolver.set_port_store(port_store);
        // Normal (non-isolated) starts honor config `default:` ports over cached
        // ports. Isolated scopes keep cache priority — their random ports are
        // intentional and must not snap back to config defaults.
        self.resolver
            .set_prefer_config_defaults(scope.is_none() && !self.randomize_ports);
    }

    /// Read persisted port allocations directly from SQLite in read-only mode.
    ///
    /// Used by dry-run initialization so we can preview with existing persisted
    /// allocations without opening state tracker tables in write mode.
    /// Read the active isolation id directly from SQLite in read-only mode.
    ///
    /// Used by dry-run, whose ephemeral in-memory state tracker has no project
    /// settings: we consult the real `.fed/lock.db` so the preview adopts the
    /// same isolation scope a real `fed start` would. Returns `None` when
    /// isolation is disabled or the database is absent/unreadable.
    fn load_isolation_id_read_only(work_dir: &std::path::Path) -> Option<String> {
        let db_path = work_dir.join(".fed").join("lock.db");
        if !db_path.exists() {
            return None;
        }
        let conn = rusqlite::Connection::open_with_flags(
            &db_path,
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
        )
        .ok()?;
        let enabled: String = conn
            .query_row(
                "SELECT value FROM project_settings WHERE key = 'isolation_enabled'",
                [],
                |row| row.get(0),
            )
            .ok()?;
        if enabled != "true" {
            return None;
        }
        conn.query_row(
            "SELECT value FROM project_settings WHERE key = 'isolation_id'",
            [],
            |row| row.get(0),
        )
        .ok()
    }

    fn load_persisted_ports_read_only(
        work_dir: &std::path::Path,
        isolation_id: Option<&str>,
    ) -> HashMap<String, u16> {
        let scope = isolation_id.unwrap_or("");
        let db_path = work_dir.join(".fed").join("lock.db");
        if !db_path.exists() {
            return HashMap::new();
        }

        let conn = match rusqlite::Connection::open_with_flags(
            &db_path,
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
        ) {
            Ok(conn) => conn,
            Err(e) => {
                tracing::debug!(
                    "Dry-run could not open persisted state at '{}': {}",
                    db_path.display(),
                    e
                );
                return HashMap::new();
            }
        };

        let mut stmt = match conn
            .prepare("SELECT param_name, port FROM persisted_ports WHERE isolation_id = ?1")
        {
            Ok(stmt) => stmt,
            Err(e) => {
                tracing::debug!(
                    "Dry-run could not read persisted_ports table from '{}': {}",
                    db_path.display(),
                    e
                );
                return HashMap::new();
            }
        };

        let rows = match stmt.query_map(rusqlite::params![scope], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, u16>(1)?))
        }) {
            Ok(rows) => rows,
            Err(e) => {
                tracing::debug!(
                    "Dry-run query failed for persisted_ports in '{}': {}",
                    db_path.display(),
                    e
                );
                return HashMap::new();
            }
        };

        let mut ports = HashMap::new();
        for row in rows {
            match row {
                Ok((name, port)) => {
                    ports.insert(name, port);
                }
                Err(e) => {
                    tracing::debug!("Dry-run skipped unreadable persisted port row: {}", e);
                }
            }
        }
        ports
    }

    /// Lightweight initialization for read-only commands (status, logs).
    ///
    /// Unlike [`Orchestrator::initialize`], this skips parameter resolution, Docker orphan cleanup,
    /// external service expansion, and profile filtering. It only loads state,
    /// builds the dependency graph, and creates+restores service managers.
    ///
    /// This prevents `fed status` from hanging on interactive port prompts or
    /// from showing all services as Stopped due to re-creating managers from scratch.
    pub async fn initialize_readonly(&mut self) -> Result<()> {
        // Initialize state tracker (loads existing DB)
        self.state_tracker.write().await.initialize().await?;

        // Build dependency graph from unresolved config
        self.build_dependency_graph()?;

        // Create service managers and restore state (PIDs, container IDs)
        self.create_services().await?;

        Ok(())
    }

    /// Initialization path for the supervisor's attach-and-reconcile flow
    /// (`07-supervisor.md` Design §1). Used exclusively via
    /// `OrchestratorBuilder::supervisor_attach(true)` — never called
    /// directly by CLI commands.
    ///
    /// Unlike every other `initialize*` variant, this one must never let
    /// [`crate::state::SqliteStateTracker::mark_dead_services`]'s stale-row filtering silently
    /// swallow a service that crashed while genuinely unsupervised (the
    /// exact case this whole feature exists for — see the module-level
    /// "Attach/self-heal reality" note in `07-supervisor.md`). It:
    ///
    /// 1. Swaps in the unlocked, supervisor-safe state tracker
    ///    ([`crate::state::StateTracker::new_for_supervisor`]) — the
    ///    orchestrator was constructed with an ephemeral (in-memory) tracker
    ///    by the builder specifically so this swap is the *first* thing that
    ///    ever touches the real on-disk `lock.db`, and it never acquires
    ///    `.fed/.lock`.
    /// 2. Runs the schema-exists/migrations/cleanup sequence via
    ///    [`crate::state::StateTracker::initialize_for_supervisor`], capturing
    ///    which rows just went stale in *this* pass — before they become
    ///    invisible to `get_services()`.
    /// 3. Builds the dependency graph and creates service managers, honoring
    ///    `desired_state`: only rows with `desired_state == Running` get
    ///    PID/container/status restored (`Orchestrator::create_services_for_supervisor`).
    ///    A row with `desired_state == Stopped` is never attached, even if
    ///    it's technically still alive and not yet purged.
    /// 4. For each newly-stale row, re-derives whether it should come back:
    ///    if `desired_state == Running` and its configured restart policy
    ///    isn't `No`, drives it through a fresh `manager.start()` (there's
    ///    nothing alive to "attach" to) and records a restart event, so
    ///    `restart_history`/circuit-breaker accounting stays consistent with
    ///    any other crash-restart. A `desired_state == Stopped` newly-stale
    ///    row is left alone — never resurrected.
    /// 5. Starts monitoring unconditionally (regardless of output mode),
    ///    scoped to [`super::monitoring::supervised_service_names`] — this is
    ///    the one initialize path that must run its health-check loop even
    ///    in `OutputMode::File`, since backgrounded services are exactly what
    ///    the supervisor exists to protect.
    pub async fn initialize_supervisor(&mut self) -> Result<()> {
        // The builder constructs a supervisor-attach orchestrator via
        // new_ephemeral (in-memory tracker, never touches .fed/.lock) so
        // this swap is the first real disk access — see this method's doc
        // comment.
        self.state_tracker = Arc::new(tokio::sync::RwLock::new(
            StateTracker::new_for_supervisor(self.work_dir.clone()).await?,
        ));

        let mut newly_stale = self
            .state_tracker
            .write()
            .await
            .initialize_for_supervisor()
            .await?;

        self.build_dependency_graph()?;

        // Restore managers, honoring desired_state (never resurrect stopped).
        // A service can die after initialize_for_supervisor's liveness sweep
        // but before its manager is restored. Treat that second-check miss as
        // newly stale too; unregistering it here would leave a Stopped manager
        // that the monitoring loop permanently skips.
        newly_stale.extend(self.create_services_for_supervisor().await?);
        newly_stale.sort();
        newly_stale.dedup();

        // Re-derive whether each newly-stale row should come back. A row
        // marked stale by mark_dead_services this pass is, by definition,
        // no longer visible to get_services() — read it back directly via
        // get_service (which does not filter on status) to recover its
        // desired_state.
        for service_id in &newly_stale {
            let desired_state = {
                let tracker = self.state_tracker.read().await;
                tracker
                    .get_service(service_id)
                    .await
                    .map(|s| s.desired_state)
            };

            if desired_state != Some(DesiredState::Running) {
                // Either genuinely unknown (shouldn't happen — the row
                // existed a moment ago) or the user stopped it: never
                // resurrect.
                continue;
            }

            let service_name = service_id.split('/').next_back().unwrap_or(service_id);
            let restart_policy = self
                .config
                .services
                .get(service_name)
                .and_then(|s| s.restart.clone())
                .unwrap_or(RestartPolicy::No);

            if matches!(restart_policy, RestartPolicy::No) {
                continue;
            }

            let manager_opt = {
                let services = self.services.read().await;
                services.get(service_name).map(Arc::clone)
            };
            let Some(manager_arc) = manager_opt else {
                tracing::warn!(
                    "Supervisor attach: no manager for newly-stale service '{}', skipping",
                    service_name
                );
                continue;
            };

            let start_result = {
                let mut manager = manager_arc.lock().await;
                manager.start().await
            };

            match start_result {
                Ok(_) => {
                    tracing::info!(
                        "Supervisor attach: restarted '{}' after discovering it crashed while unsupervised",
                        service_name
                    );

                    // LOCK ORDER: state_tracker before service mutex (see
                    // monitoring.rs's lock_order.rs note).
                    let mut tracker = self.state_tracker.write().await;
                    if let Err(e) = tracker.record_restart(service_name).await {
                        tracing::warn!(
                            "Supervisor attach: failed to record restart for '{}': {}",
                            service_name,
                            e
                        );
                    }
                    // `record_restart` only appends to `restart_history`
                    // (the circuit-breaker's own accounting) — it does not
                    // touch the separate `services.restart_count` column
                    // that `fed status`/`debug state --json` display and
                    // that the ordinary monitoring-loop restart path
                    // (`batch_increment_restart_counts`, `monitoring.rs`)
                    // increments after every crash-restart. Without this, a
                    // service discovered-and-restarted here (as opposed to
                    // one that crashes again later under this same
                    // supervisor's regular health checks) would silently
                    // under-report its restart count.
                    if let Err(e) = tracker
                        .batch_increment_restart_counts(vec![service_name.to_string()])
                        .await
                    {
                        tracing::warn!(
                            "Supervisor attach: failed to increment restart_count for '{}': {}",
                            service_name,
                            e
                        );
                    }
                    // The row's `status` column is still 'stale' — this
                    // pass is exactly what just marked it so, before
                    // discovering it should come back. Left uncorrected,
                    // `get_services()`'s stale filter (`service_crud.rs`)
                    // would hide this row from every future command
                    // (`fed status`, the next `fed start`, even this same
                    // supervisor's own next `mark_dead_services` sweep)
                    // forever, despite the service being alive and
                    // supervised again right now.
                    if let Err(e) = tracker
                        .update_service_status(service_name, Status::Running)
                        .await
                    {
                        tracing::warn!(
                            "Supervisor attach: failed to clear stale status for '{}': {}",
                            service_name,
                            e
                        );
                    }
                    let manager = manager_arc.lock().await;
                    if let Some(pid) = manager.get_pid()
                        && let Err(e) = tracker.update_service_pid(service_name, pid).await
                    {
                        tracing::warn!(
                            "Supervisor attach: failed to update PID for '{}': {}",
                            service_name,
                            e
                        );
                    }
                    if let Some(container_id) = manager.get_container_id()
                        && let Err(e) = tracker
                            .update_service_container_id(service_name, container_id)
                            .await
                    {
                        tracing::warn!(
                            "Supervisor attach: failed to update container ID for '{}': {}",
                            service_name,
                            e
                        );
                    }
                }
                Err(e) => {
                    tracing::error!(
                        "Supervisor attach: failed to restart '{}': {}",
                        service_name,
                        e
                    );
                }
            }
        }

        self.state_tracker.write().await.save().await?;

        // The monitoring loop no-ops every tick until this flag is set
        // (`run_monitoring_loop`'s `startup_complete` check exists to avoid
        // racing a normal `fed start`'s own graduated startup sequence).
        // The supervisor has no such sequence to race — it either attached
        // to already-running services or just fresh-started the newly-stale
        // ones above — so there is no "still starting up" phase to wait
        // out. Without this, the supervisor's monitoring loop would spin
        // forever without ever running a single health check.
        self.mark_startup_complete();

        // Start monitoring unconditionally — a supervisor exists precisely
        // to watch backgrounded (File-mode) services, so the output-mode
        // skip in start_monitoring() must not apply here.
        self.start_monitoring_for_supervisor().await;

        Ok(())
    }

    /// Initialization path for `fed start --dry-run`.
    ///
    /// This resolves parameters and builds dependency ordering, but intentionally
    /// avoids mutating persistent state (no service registration, no port
    /// persistence, no Docker cleanup).
    pub async fn initialize_dry_run(&mut self) -> Result<()> {
        // Work dir is required for resolving global env_file paths.
        self.resolver.set_work_dir(&self.work_dir);

        // Adopt the persisted isolation scope so the dry-run previews the same
        // ports a real `fed start` would use (configure_port_store reads it).
        // Dry-run's state tracker is an ephemeral in-memory DB with no project
        // settings, so we must read isolation state from the real `.fed/lock.db`.
        if self.isolation_id.is_none() {
            self.isolation_id = Self::load_isolation_id_read_only(&self.work_dir);
        }

        // Use a read-only port-store snapshot so resolve_parameters() can't persist.
        self.configure_port_store(true).await;

        // Scope the built-in FED_PROJECT_ID to the active isolation session (if any).
        self.resolver.set_isolation_id(self.isolation_id.clone());

        // Resolve parameters and expand external dependencies.
        self.resolver.resolve_parameters(&mut self.config)?;
        let expander =
            ExternalServiceExpander::new(&self.config, &self.resolver, self.work_dir.clone());
        self.config = expander.expand().await?;

        // Resolve templates in full config.
        self.resolver.set_work_dir(&self.work_dir);
        let resolved = self.resolver.resolve_config(&self.config)?;
        self.config = resolved;

        // Apply profile filtering (same semantics as full initialize).
        let active_profiles = &self.active_profiles;
        self.config.services.retain(|_, service| {
            if service.profiles.is_empty() {
                return true;
            }
            service.profiles.iter().any(|p| active_profiles.contains(p))
        });

        // Build dependency graph for dry-run output.
        self.build_dependency_graph()?;
        Ok(())
    }

    /// Initialize the orchestrator for service management.
    ///
    /// This must be called after creating the orchestrator and before starting any services.
    /// Initialization performs the following steps:
    ///
    /// 1. Load any existing state from the lock file
    /// 2. Resolve configuration parameters (including port allocation)
    /// 3. Expand external service dependencies
    /// 4. Apply service profiles filtering
    /// 5. Build the dependency graph
    /// 6. Create service managers for all configured services
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - State file is corrupted
    /// - Parameter resolution fails (e.g., port conflict)
    /// - External dependency cannot be loaded
    /// - Circular dependencies are detected
    /// - Service configuration is invalid
    pub async fn initialize(&mut self) -> Result<()> {
        // Initialize state tracker (marks dead services as stale, doesn't delete)
        self.state_tracker.write().await.initialize().await?;

        // Determine the active isolation scope BEFORE anything reads or writes
        // ports. If project-level isolation is enabled, adopt its persisted
        // isolation_id so port reads/writes, container names, and volumes all
        // resolve to the same scope. An explicitly-set isolation_id (e.g. an
        // isolated script) takes precedence and is left untouched.
        if self.isolation_id.is_none() {
            let tracker = self.state_tracker.read().await;
            let (isolated, isolation_id) = tracker.get_isolation_mode().await;
            if isolated && let Some(id) = isolation_id {
                tracing::debug!("Applying persisted isolation_id: {}", id);
                self.isolation_id = Some(id);
            }
        }

        // Collect ports owned by running managed services.
        // Safe to call after initialize because dead services are marked stale
        // (not deleted), so port_allocations for live services remain intact.
        self.collect_managed_ports().await;

        // Now purge stale services — managed ports have been collected
        self.state_tracker
            .write()
            .await
            .purge_stale_services()
            .await?;

        // Detect containers for this project that aren't tracked in state DB
        // (Conservative approach: warn but don't auto-remove)
        let cleaner = super::orphans::OrphanCleaner::new(self);
        match cleaner.detect_untracked_containers().await {
            Ok(orphans) if !orphans.is_empty() => {
                for container in &orphans {
                    tracing::warn!(
                        "Found container '{}' for this project that isn't tracked in state. \
                        This may indicate a previous stop operation failed. \
                        Run 'fed stop' or 'fed clean' to remove it.",
                        container
                    );
                }
            }
            Ok(_) => {} // No untracked containers
            Err(e) => {
                tracing::debug!("Failed to detect untracked containers: {}", e);
                // Continue initialization even if detection fails
            }
        }

        // Work dir is required for resolving global env_file paths.
        self.resolver.set_work_dir(&self.work_dir);

        // Scope the built-in FED_PROJECT_ID to the active isolation session (if any)
        // — determined just above — so parallel isolated stacks get distinct ids.
        self.resolver.set_isolation_id(self.isolation_id.clone());

        // Build the port store based on mode:
        // - Randomize mode → NoopPortStore (forces fresh random allocation)
        // - Otherwise → SqlitePortStore (reads/writes persisted_ports table)
        self.configure_port_store(false).await;

        // First pass: resolve parent parameters only (not services yet)
        // This allows us to use resolved parameter values when expanding external services
        self.resolver.resolve_parameters(&mut self.config)?;

        // Expand external services using resolved parent parameters
        let expander =
            ExternalServiceExpander::new(&self.config, &self.resolver, self.work_dir.clone());
        self.config = expander.expand().await?;

        // Set work directory for .env file resolution
        self.resolver.set_work_dir(&self.work_dir);

        // Second pass: full config resolution including external services
        let resolved = self.resolver.resolve_config(&self.config)?;
        self.config = resolved;

        // Track allocated ports in state
        for port in self.resolver.get_allocated_ports() {
            self.state_tracker.write().await.track_port(port).await;
        }

        // Persist resolved port parameters globally so that on next `fed start`,
        // collect_managed_ports can detect ports owned by process/Gradle services
        // (not just Docker).
        let port_resolutions: Vec<(String, u16)> = self
            .resolver
            .get_port_resolutions()
            .iter()
            .map(|r| (r.param_name.clone(), r.resolved_port))
            .collect();
        if !port_resolutions.is_empty() {
            self.state_tracker
                .write()
                .await
                .save_port_resolutions(&port_resolutions, self.isolation_id.as_deref())
                .await?;
        }

        // NOTE: Port listeners are held until services actually start.
        // This prevents TOCTOU race conditions where another process could
        // steal the port between allocation and service bind.
        // Listeners are released in start_all() or start_service().

        // Filter services based on active profiles (Docker Compose semantics):
        // - Services without profiles: always included
        // - Services with profiles: only included if at least one profile is active
        // Borrow active_profiles before the mutable borrow of services
        let active_profiles = &self.active_profiles;
        self.config.services.retain(|_, service| {
            // Services without profiles are always included
            if service.profiles.is_empty() {
                return true;
            }

            // Services with profiles are only included if at least one
            // of their profiles is active
            service.profiles.iter().any(|p| active_profiles.contains(p))
        });

        // Build dependency graph
        self.build_dependency_graph()?;

        // Create services
        self.create_services().await?;

        // Create health checkers
        self.create_health_checkers().await;

        // Save initial state
        self.state_tracker.write().await.save().await?;

        // Start background monitoring (if not in detached mode)
        self.start_monitoring().await;

        Ok(())
    }

    /// Build the dependency graph from config
    fn build_dependency_graph(&mut self) -> Result<()> {
        self.dep_graph = Graph::new();

        // Add all services as nodes
        for name in self.config.services.keys() {
            self.dep_graph.add_node(name.clone());
        }

        // Add dependency edges
        for (name, service) in &self.config.services {
            for dep in &service.depends_on {
                self.dep_graph
                    .add_edge(name.clone(), dep.service_name().to_string());
            }
        }

        // Check for circular dependencies (topological_sort returns the cycle if one exists)
        self.dep_graph.topological_sort()?;

        Ok(())
    }

    /// Create health checkers for services.
    ///
    /// Delegates to `HealthCheckRunner`.
    async fn create_health_checkers(&mut self) {
        let runner = super::health::HealthCheckRunner::new(self);
        runner.create_health_checkers().await;
    }

    /// Wait for a service to become healthy (used by script dependencies).
    /// Returns Ok(()) when healthy, or Err after timeout.
    ///
    /// Delegates to `HealthCheckRunner`.
    pub async fn wait_for_healthy(&self, service_name: &str, timeout: Duration) -> Result<()> {
        let runner = super::health::HealthCheckRunner::new(self);
        runner.wait_for_healthy(service_name, timeout).await
    }

    /// Await a service's healthcheck during startup.
    ///
    /// If the service has a registered healthcheck, polls it until healthy or timeout.
    /// Also monitors process/container liveness to detect early crashes without
    /// waiting for the full timeout. If no healthcheck is registered, returns immediately.
    ///
    /// Respects the orchestrator's cancellation token for responsive Ctrl-C handling.
    ///
    /// Delegates to `HealthCheckRunner`.
    async fn await_healthcheck(
        &self,
        name: &str,
        manager_arc: &Arc<tokio::sync::Mutex<Box<dyn ServiceManager>>>,
    ) -> Result<StartHealth> {
        let runner = super::health::HealthCheckRunner::new(self);
        runner.await_healthcheck(name, manager_arc).await
    }

    /// Force run install command for a service (clears install state first).
    pub async fn run_install(&self, service_name: &str) -> Result<()> {
        let lifecycle = crate::orchestrator::ServiceLifecycleCommands::new(
            &self.config,
            &self.work_dir,
            self.isolation_id.clone(),
        );
        lifecycle.run_install(service_name).await
    }

    /// Run install command for a service if needed.
    async fn run_install_if_needed(&self, service_name: &str) -> Result<()> {
        let lifecycle = crate::orchestrator::ServiceLifecycleCommands::new(
            &self.config,
            &self.work_dir,
            self.isolation_id.clone(),
        );
        lifecycle.run_install_if_needed(service_name).await
    }

    /// Force run migrate command for a service (clears migrate state first).
    pub async fn run_migrate(&self, service_name: &str) -> Result<()> {
        let lifecycle = crate::orchestrator::ServiceLifecycleCommands::new(
            &self.config,
            &self.work_dir,
            self.isolation_id.clone(),
        );
        lifecycle.run_migrate(service_name).await
    }

    /// Run migrate command for a service if needed.
    async fn run_migrate_if_needed(&self, service_name: &str) -> Result<()> {
        let lifecycle = crate::orchestrator::ServiceLifecycleCommands::new(
            &self.config,
            &self.work_dir,
            self.isolation_id.clone(),
        );
        lifecycle.run_migrate_if_needed(service_name).await
    }

    /// Run clean command for a service.
    ///
    /// This will:
    /// 1. Run the user-defined clean command (if present)
    /// 2. Remove any Docker volumes associated with the service
    pub async fn run_clean(&self, service_name: &str) -> Result<()> {
        let lifecycle = crate::orchestrator::ServiceLifecycleCommands::new(
            &self.config,
            &self.work_dir,
            self.isolation_id.clone(),
        );
        lifecycle.run_clean(service_name).await
    }

    /// Run build command for a service.
    pub async fn run_build(
        &self,
        service_name: &str,
        tag: Option<&str>,
        cli_build_args: &[String],
    ) -> Result<Option<crate::config::DockerBuildResult>> {
        let lifecycle = crate::orchestrator::ServiceLifecycleCommands::new(
            &self.config,
            &self.work_dir,
            self.isolation_id.clone(),
        );
        lifecycle.run_build(service_name, tag, cli_build_args).await
    }

    /// Start a specific service and its dependencies.
    ///
    /// This method is cancellable via `cancel_operations()` and respects
    /// the `startup_timeout` setting. Dependencies are started first in
    /// topological order.
    ///
    /// # Cancellation
    ///
    /// If cancelled during startup, returns `Error::Cancelled`. Services that
    /// have already started will remain running - call `stop_all()` to clean up.
    ///
    /// # Timeout
    ///
    /// Each service has `startup_timeout` to complete startup. If exceeded,
    /// returns `Error::Timeout`.
    ///
    /// The returned [`StartOutcome`] carries the per-service startup health
    /// result (healthy / healthcheck timed out / unchecked) for the named
    /// service and every dependency this call started — a healthcheck
    /// timeout is deliberately not an `Err` (the process is alive and
    /// dependents proceed), so it must be read from the outcome.
    pub async fn start(&self, service_name: &str) -> Result<StartOutcome> {
        // Check for early cancellation
        if self.cancellation_token.is_cancelled() {
            return Err(Error::Cancelled(service_name.to_string()));
        }

        // Release port listeners just before starting services.
        // This minimizes the TOCTOU race window.
        self.release_port_listeners_once();

        // Get services to start in order
        let deps = self.dep_graph.get_dependencies(service_name);

        let mut outcome = StartOutcome::default();

        // Start dependencies first
        for dep in deps {
            // Check for cancellation before each dependency
            if self.cancellation_token.is_cancelled() {
                return Err(Error::Cancelled(dep.clone()));
            }
            let health = self.start_service_with_timeout(&dep).await?;
            outcome.record(&dep, health);
        }

        // Start the requested service
        let health = self.start_service_with_timeout(service_name).await?;
        outcome.record(service_name, health);
        Ok(outcome)
    }

    /// Start a service with timeout and cancellation support.
    ///
    /// A per-service `startup_timeout` (set in the service config) takes
    /// precedence over the orchestrator-wide default.
    async fn start_service_with_timeout(&self, name: &str) -> Result<StartHealth> {
        let cancel_token = self.cancellation_token.clone();
        let timeout = self
            .config
            .services
            .get(name)
            .and_then(|s| s.get_startup_timeout())
            .unwrap_or(self.startup_timeout);

        // If the service was already running — or being started by a
        // concurrent attempt — before this call, an interruption must NOT
        // kill it: cleanup is only for a process/container this attempt may
        // have spawned itself.
        let was_active_before = {
            let manager_arc = {
                let services = self.services.read().await;
                services.get(name).map(Arc::clone)
            };
            match manager_arc {
                Some(arc) => {
                    let manager = arc.lock().await;
                    matches!(
                        manager.status(),
                        Status::Running | Status::Healthy | Status::Starting
                    )
                }
                None => false,
            }
        };

        // Set by the start future once THIS attempt owns the service (won
        // the registration race, or entered the oneshot path). Until then
        // there is nothing of ours to clean up — and cleaning up anyway
        // would force-kill and unregister a registration owned by a
        // concurrent winning attempt this call is merely waiting on
        // (`await_concurrent_start`), whose row must survive our
        // cancellation or timeout.
        let attempt_owns_service = Arc::new(AtomicBool::new(false));

        tokio::select! {
            biased;

            _ = cancel_token.cancelled() => {
                if !was_active_before && attempt_owns_service.load(Ordering::SeqCst) {
                    self.stop_interrupted_start(name).await;
                }
                Err(Error::Cancelled(name.to_string()))
            }

            result = tokio::time::timeout(
                timeout,
                self.start_service(name, Arc::clone(&attempt_owns_service)),
            ) => {
                match result {
                    Ok(inner_result) => inner_result,
                    Err(_elapsed) => {
                        if !was_active_before && attempt_owns_service.load(Ordering::SeqCst) {
                            self.stop_interrupted_start(name).await;
                        }
                        Err(Error::Timeout(name.to_string()))
                    }
                }
            }
        }
    }

    /// Best-effort stop after an interrupted (timed-out or cancelled) start.
    ///
    /// Dropping the start future mid-await can land after the process or
    /// container was already spawned but before registration committed — the
    /// registration guard then removes the state entry, so nothing tracks the
    /// live process. It can also land *after* the commit (e.g. during the
    /// healthcheck wait), where the state row would outlive the killed
    /// process and block a retry with AlreadyExists. Kill and unregister so
    /// neither a leaked process nor a stale row survives the timeout.
    async fn stop_interrupted_start(&self, name: &str) {
        if let Err(e) = self.force_kill_service(name).await {
            tracing::warn!(
                "Cleanup after interrupted start of '{}' failed: {}",
                name,
                e
            );
        }
    }

    /// Start a single service
    async fn start_service(
        &self,
        name: &str,
        attempt_owns_service: Arc<AtomicBool>,
    ) -> Result<StartHealth> {
        self.start_service_impl(name, attempt_owns_service)
            .instrument(tracing::info_span!("start_service", service.name = %name))
            .await
    }

    /// Implementation of start_service (separate to allow instrumentation)
    async fn start_service_impl(
        &self,
        name: &str,
        attempt_owns_service: Arc<AtomicBool>,
    ) -> Result<StartHealth> {
        // Get Arc clone of manager
        let manager_arc = {
            let services = self.services.read().await;
            if let Some(arc) = services.get(name) {
                Arc::clone(arc)
            } else {
                // Normal user-error path (typo'd service name) — the returned
                // ServiceNotFound carries the user-facing message. Keep the
                // registered-service list at debug level for diagnosing the
                // rarer removed-during-startup case.
                let available: Vec<_> = services.keys().collect();
                tracing::debug!(
                    "Service '{}' not found in service map. Registered services: {:?}",
                    name,
                    available
                );
                return Err(Error::ServiceNotFound(name.to_string()));
            }
        };

        // Oneshot (hook-only) services take a dedicated run-to-completion path:
        // execute once, gate dependents on completion, re-run every startup.
        if self
            .config
            .services
            .get(name)
            .map(|s| s.service_type() == ServiceType::Oneshot)
            .unwrap_or(false)
        {
            // The oneshot path runs hooks this attempt owns — an
            // interruption may leave their processes behind for cleanup.
            attempt_owns_service.store(true, Ordering::SeqCst);
            self.record_owned(name);
            return self
                .run_oneshot(name, &manager_arc)
                .await
                .map(|()| StartHealth::Unchecked);
        }

        // Check if already running (deduplication) and check for cancellation
        // IMPORTANT: We check cancellation inside the lock to prevent TOCTOU race
        // where cancellation happens between check and acquiring the lock
        {
            let mut manager = manager_arc.lock().await;

            // Check cancellation while holding the lock - this is the critical fix for TOCTOU
            if self.cancellation_token.is_cancelled() {
                tracing::debug!(
                    "start_service: cancellation detected inside lock for '{}'",
                    name
                );
                return Err(Error::Cancelled(name.to_string()));
            }

            let status = manager.status();
            if status == Status::Running || status == Status::Healthy {
                // Verify the service is actually alive — its process/container may
                // have died since we last checked (e.g. OOM kill, Docker restart).
                if self.verify_service_alive(&**manager) {
                    return Ok(StartHealth::Unchecked);
                }
                tracing::warn!(
                    "Service '{}' reports {} but is no longer alive, restarting",
                    name,
                    status
                );
                // Reset status so the start path below proceeds normally.
                // stop() handles already-dead services gracefully.
                let _ = manager.stop().await;
            }
        }

        // Determine service type
        let service_type = if let Some(service_config) = self.config.services.get(name) {
            service_config.service_type()
        } else {
            ServiceType::Undefined
        };

        // Register with scope guard — Drop auto-unregisters on any failure path.
        // SQLite's ACID transactions ensure only one thread wins registration.
        let mut service_state =
            ServiceState::new(name.to_string(), service_type, self.namespace.clone());
        if let Some(service_config) = self.config.services.get(name) {
            service_state.startup_message = service_config.startup_message.clone();
            // 07-supervisor.md Design §3: captured once at registration
            // (an already-registered row is left untouched, so this only
            // ever matters for a fresh row) — read back by
            // `mark_dead_services` to decide whether this service's
            // container-liveness check gets a stale-grace period.
            service_state.native_restart_enabled = service_config.docker_native_restart_enabled();
        }

        let Some(registration) =
            super::registration::ServiceRegistration::register(&self.state_tracker, service_state)
                .await?
        else {
            // Another start (a concurrent task in this process, or another
            // `fed` process sharing the state DB) won the registration race.
            // Returning `Unchecked` immediately would let our caller — and
            // its dependents — proceed while the winner is still mid-start,
            // and would report success even if that start ultimately fails.
            // Wait for the winning attempt to resolve and report what
            // actually happened.
            //
            // The wait deadline is capped just under this service's startup
            // timeout: `start_service_with_timeout` wraps this whole call in
            // that timeout, and hitting it yields a generic Timeout error —
            // the deadline must fire first so the user gets the actionable
            // stuck-Starting message instead. Floor of 1s so a tiny
            // configured startup timeout can't turn the wait into an
            // instant misleading error.
            let startup_timeout = self
                .config
                .services
                .get(name)
                .and_then(|s| s.get_startup_timeout())
                .unwrap_or(self.startup_timeout);
            let deadline = CONCURRENT_START_WAIT_TIMEOUT
                .min(startup_timeout.saturating_sub(Duration::from_secs(5)))
                .max(Duration::from_secs(1));
            return self
                .await_concurrent_start(name, &manager_arc, deadline)
                .instrument(tracing::info_span!("await_concurrent_start"))
                .await;
        };

        // Registration won: from here on this attempt may spawn a process/
        // container, so an interrupted start must clean up after itself.
        attempt_owns_service.store(true, Ordering::SeqCst);
        self.record_owned(name);

        // All ? from here to commit() are safe — the guard cleans up on drop.

        self.run_install_if_needed(name)
            .instrument(tracing::info_span!("install_if_needed"))
            .await?;

        // Run migrate after install (deps are already healthy at this point)
        self.run_migrate_if_needed(name)
            .instrument(tracing::info_span!("migrate_if_needed"))
            .await?;

        if self.cancellation_token.is_cancelled() {
            return Err(Error::Cancelled(name.to_string()));
        }

        // Refuse to start if something foreign already answers the healthcheck —
        // once our process is up we could never tell the two apart.
        super::health::HealthCheckRunner::new(self)
            .preflight_foreign_listener(name)
            .instrument(tracing::info_span!("preflight_foreign_listener"))
            .await?;

        // Start the service — hold manager lock for cancellation check + start
        async {
            let mut manager = manager_arc.lock().await;
            if self.cancellation_token.is_cancelled() {
                return Err(Error::Cancelled(name.to_string()));
            }
            manager.start().await
        }
        .instrument(tracing::info_span!("spawn_service"))
        .await?;

        // Update state to running and save atomically
        // IMPORTANT: Hold write lock across all updates AND save to prevent race conditions
        // when multiple services start concurrently
        async {
            let mut tracker = self.state_tracker.write().await;

            // Store PID if available (for process services)
            // Store container ID if available (for docker services)
            //
            // Written BEFORE the status flips to Running: each update
            // commits its own SQLite transaction, so a concurrent `fed`
            // process polling the row (`await_concurrent_start`) may observe
            // any prefix of this block. Running is the signal that the start
            // is committed — it must be the last thing an observer can see,
            // never a state where the PID/ports are still missing or a
            // later write in this block can still fail.
            {
                let manager = manager_arc.lock().await;
                if let Some(pid) = manager.get_pid() {
                    tracker.update_service_pid(name, pid).await?;
                }
                if let Some(container_id) = manager.get_container_id() {
                    tracker
                        .update_service_container_id(name, container_id)
                        .await?;
                }
                // Store port mappings if available (for docker services)
                let port_mappings = manager.get_port_mappings().await;
                if !port_mappings.is_empty() {
                    tracker
                        .update_service_port_mappings(name, port_mappings)
                        .await?;
                }
            }

            // Defensive belt-and-suspenders: `register_service`'s INSERT
            // already writes `desired_state = Running` for a brand-new row
            // (see `ServiceState::new`'s default), so this is not
            // load-bearing for the common case — but it's cheap and keeps
            // intent explicit at every place a service is confirmed started.
            // See `07-supervisor.md` Design §1.
            tracker
                .set_desired_state(name, DesiredState::Running)
                .await?;

            tracker.update_service_status(name, Status::Running).await?;

            // Save while holding the write lock to ensure atomicity
            tracker.save().await?;
            Result::<()>::Ok(())
        }
        .instrument(tracing::info_span!("update_state"))
        .await?;

        // Service is Running in DB — commit the guard so Drop won't unregister.
        registration.commit();

        // If a healthcheck is registered, poll it before declaring the service ready.
        // This ensures services that crash immediately or need warmup time are detected
        // during startup rather than appearing as "Running" when they're actually dead.
        // A timeout with the process still alive is non-fatal and surfaces as
        // `StartHealth::TimedOut` in the returned outcome.
        self.await_healthcheck(name, &manager_arc)
            .instrument(tracing::info_span!("await_healthcheck"))
            .await
    }

    /// Wait for a concurrent start of `name` (which won the registration
    /// race) to resolve, then report its real outcome.
    ///
    /// Polls the state DB — the only channel shared with a winner in another
    /// process — until the service leaves `Starting`:
    ///
    /// - `Running`/`Failing`: the winner's process is up; await the
    ///   healthcheck ourselves so we return a real observation. In-process
    ///   the manager is shared, so liveness monitoring works too; cross-
    ///   process we have no PID and only poll the checker, which is still an
    ///   honest observation.
    /// - `Healthy`/`Completed`: the winner already verified it.
    /// - Row gone, `Stopped`, or `Stopping`: the winning attempt failed (its
    ///   registration guard unregisters on failure) — surface an error
    ///   instead of the old silent success.
    ///
    /// `deadline` bounds the wait (parameterized for tests): a winner that
    /// was SIGKILLed leaves a `Starting` row behind forever, and hanging on
    /// it would be worse than the honest error.
    async fn await_concurrent_start(
        &self,
        name: &str,
        manager_arc: &Arc<tokio::sync::Mutex<Box<dyn ServiceManager>>>,
        deadline: Duration,
    ) -> Result<StartHealth> {
        tracing::info!(
            "Start of '{}' lost the registration race; waiting for the winning attempt to finish",
            name
        );
        let started = std::time::Instant::now();
        let poll_interval = Duration::from_millis(250);

        loop {
            if self.cancellation_token.is_cancelled() {
                return Err(Error::Cancelled(name.to_string()));
            }

            let status = {
                let tracker = self.state_tracker.read().await;
                tracker.get_service(name).await.map(|s| s.status)
            };

            match status {
                // Winner failed and its guard cleaned the row up — a plain
                // retry can win the registration next time.
                None => {
                    return Err(Error::ServiceStartFailed(
                        name.to_string(),
                        format!(
                            "a concurrent start of '{}' failed; see that command's \
                             output for the reason, then retry",
                            name
                        ),
                    ));
                }
                // A Stopped/Stopping row can outlive its failed start (e.g.
                // the process died during the healthcheck wait after the
                // registration committed) and would send every retry down
                // this same path — the remedy is clearing the row, not
                // retrying.
                Some(Status::Stopped) | Some(Status::Stopping) => {
                    return Err(Error::ServiceStartFailed(
                        name.to_string(),
                        format!(
                            "'{}' is registered but not running — likely a previous \
                             start failed after registering, or a concurrent stop won. \
                             Run `fed stop {}` to clear its state, then start again",
                            name, name
                        ),
                    ));
                }
                Some(Status::Healthy) => return Ok(StartHealth::Healthy),
                // Completed is the oneshot terminal state; nothing to verify.
                Some(Status::Completed) => return Ok(StartHealth::Unchecked),
                Some(Status::Running) | Some(Status::Failing) => {
                    return self.await_healthcheck(name, manager_arc).await;
                }
                Some(Status::Starting) => {}
            }

            if started.elapsed() >= deadline {
                return Err(Error::ServiceStartFailed(
                    name.to_string(),
                    format!(
                        "waited {:?} for a concurrent start of '{}' to finish, but it is \
                         still marked Starting. If no other `fed` command is running, the \
                         previous attempt likely died without cleanup — run `fed stop {}` \
                         to clear it, then retry",
                        deadline, name, name
                    ),
                ));
            }

            tokio::time::sleep(poll_interval).await;
        }
    }

    /// Run a hook-only node (the oneshot node) to completion.
    ///
    /// A hook-only service declares `install:` and/or `migrate:` but no
    /// process/image/etc. Semantics:
    /// - Dependencies are already healthy (dependency-graph ordering starts them
    ///   before this node is reached), so this runs the install hook (marker-
    ///   gated) and the migrate hook (every start), streaming their output, then
    ///   marks the node completed.
    /// - Hooks succeeding satisfies the node → dependents may proceed. A hook
    ///   failure is a startup error naming this service, aborting `fed start`.
    /// - `migrate:` re-runs on every `fed start`/`fed restart`: managers are
    ///   rebuilt fresh each process, so a `Completed` state from a previous
    ///   session (restored only for display) never suppresses re-execution.
    ///
    /// Concurrency: the per-service manager mutex is held across the whole run,
    /// so two dependents that reach the same node are serialized — the second
    /// blocks until the first execution finishes, then sees `has_run` and skips
    /// re-running the hooks (never proceeding before they completed). The mutex is
    /// released before the state-tracker write so the documented
    /// Services < StateTracker < ServiceMutex lock ordering is respected.
    async fn run_oneshot(
        &self,
        name: &str,
        manager_arc: &Arc<tokio::sync::Mutex<Box<dyn ServiceManager>>>,
    ) -> Result<()> {
        {
            let mut manager = manager_arc.lock().await;

            if self.cancellation_token.is_cancelled() {
                return Err(Error::Cancelled(name.to_string()));
            }

            // Already executed in THIS startup? A concurrent dependent that waited
            // on the mutex lands here after the first run finished — skip re-running.
            {
                let oneshot = manager
                    .as_any_mut()
                    .downcast_mut::<crate::service::OneshotService>()
                    .ok_or_else(|| {
                        Error::Config(format!("Service '{}' is not a oneshot service", name))
                    })?;
                if oneshot.has_run() {
                    return Ok(());
                }
            }

            // Run the hooks: install is marker-gated (once per scope), migrate
            // runs every start. A hook failure returns an error naming this
            // service and aborts the start before the node is marked complete.
            self.run_install_if_needed(name)
                .instrument(tracing::info_span!("install_if_needed"))
                .await?;
            self.run_migrate_if_needed(name)
                .instrument(tracing::info_span!("migrate_if_needed"))
                .await?;

            if self.cancellation_token.is_cancelled() {
                return Err(Error::Cancelled(name.to_string()));
            }

            // Mark the node completed (sets Completed + has_run). The hooks above
            // are the node's actual work; start() is just the completion signal.
            manager
                .start()
                .instrument(tracing::info_span!("run_oneshot"))
                .await?;
        } // manager mutex released before touching the state tracker

        // Record completion for `fed status` (cross-process display). Replace any
        // stale entry from a previous session so the status is fresh and the
        // oneshot is never wrongly short-circuited on a later start.
        {
            let mut tracker = self.state_tracker.write().await;
            let _ = tracker.unregister_service(name).await;
            let mut state = ServiceState::new(
                name.to_string(),
                ServiceType::Oneshot,
                self.namespace.clone(),
            );
            state.status = Status::Completed;
            if let Some(cfg) = self.config.services.get(name) {
                state.startup_message = cfg.startup_message.clone();
            }
            tracker.register_service(state).await?;
            tracker.save().await?;
        }

        Ok(())
    }

    /// Start all services respecting dependencies.
    ///
    /// Services are started in parallel groups based on dependency order.
    /// All services in a group are started concurrently, but groups are
    /// processed sequentially.
    ///
    /// # Cancellation
    ///
    /// This method is cancellable via `cancel_operations()`. If cancelled,
    /// the current group will complete but subsequent groups will not start.
    pub async fn start_all(&self) -> Result<StartOutcome> {
        // Check for early cancellation
        if self.cancellation_token.is_cancelled() {
            return Err(Error::Cancelled("start_all".to_string()));
        }

        // Release port listeners just before starting services.
        // This minimizes the TOCTOU race window.
        self.release_port_listeners_once();

        // Get parallel groups
        let groups = self.dep_graph.get_parallel_groups()?;

        let mut outcome = StartOutcome::default();

        // Start each group sequentially, but services within each group start concurrently
        for group in groups {
            // Check for cancellation before each group
            if self.cancellation_token.is_cancelled() {
                return Err(Error::Cancelled("start_all".to_string()));
            }

            // Start all services in this group concurrently with timeout
            let futures: Vec<_> = group
                .iter()
                .map(|service_name| self.start_service_with_timeout(service_name))
                .collect();

            // Wait for all services in the group to start
            let results = futures::future::join_all(futures).await;

            // Collect errors; record health outcomes for services that started
            let mut errors: Vec<Error> = Vec::new();
            for (service_name, result) in group.iter().zip(results) {
                match result {
                    Ok(health) => outcome.record(service_name, health),
                    Err(e) => errors.push(e),
                }
            }

            // If any service failed, return aggregated errors
            if !errors.is_empty() {
                if errors.len() == 1 {
                    return Err(errors.into_iter().next().expect("errors not empty"));
                } else {
                    return Err(Error::Multiple(errors));
                }
            }
        }

        Ok(outcome)
    }

    /// Stop a service and its dependents.
    ///
    /// Dependents are stopped first in deepest-first order so no service
    /// loses a dependency while still running. `get_all_dependents` returns
    /// in this order already; iterate forward, do not reverse.
    ///
    /// # Cancellation
    ///
    /// This method respects cancellation but will attempt to stop services
    /// even if cancelled - a cancelled stop is less dangerous than a
    /// cancelled start. Returns `Error::Cancelled` after completing stops.
    pub async fn stop(&self, service_name: &str) -> Result<()> {
        let dependents = self.get_all_dependents(service_name);
        let was_cancelled = self.cancellation_token.is_cancelled();

        for dependent in &dependents {
            self.stop_service_with_timeout(dependent).await?;
        }

        // Stop the requested service
        self.stop_service_with_timeout(service_name).await?;

        // Report cancellation after completing the stop
        if was_cancelled {
            return Err(Error::Cancelled(service_name.to_string()));
        }

        Ok(())
    }

    /// Stop a service with timeout support.
    async fn stop_service_with_timeout(&self, name: &str) -> Result<()> {
        let timeout = self.stop_timeout;

        match tokio::time::timeout(timeout, self.stop_service(name)).await {
            Ok(result) => result,
            Err(_elapsed) => {
                // The graceful stop future was dropped mid-flight (possibly
                // after SIGTERM but before its own SIGKILL escalation and
                // before unregistering). Force-kill and unregister here —
                // returning Ok while the process still runs would report a
                // successful stop for a service that is still alive.
                tracing::warn!(
                    "Stop timeout exceeded for service '{}', force-killing",
                    name
                );
                self.force_kill_service(name).await
            }
        }
    }

    /// Force-kill a service whose graceful stop timed out, then unregister it.
    ///
    /// Returns an error if the kill itself fails or times out — in that case
    /// the state entry is kept so the still-running process remains tracked.
    async fn force_kill_service(&self, name: &str) -> Result<()> {
        // Persist the stop intent *before* the kill signal goes out. A
        // separate `fed` process (a future restart-policy supervisor) reads
        // this column, not this in-process manager's `Status` — see
        // `07-supervisor.md` Design §1. Best-effort: a missing row (never
        // registered) is not an error here.
        let _ = self
            .state_tracker
            .write()
            .await
            .set_desired_state(name, DesiredState::Stopped)
            .await;

        let manager_arc = {
            let services = self.services.read().await;
            services.get(name).map(Arc::clone)
        };

        if let Some(manager_arc) = manager_arc {
            let kill_result = tokio::time::timeout(Duration::from_secs(10), async {
                let mut manager = manager_arc.lock().await;
                if manager.status() == Status::Stopped {
                    return Ok(());
                }
                manager.kill().await
            })
            .await;

            match kill_result {
                Ok(Ok(())) => {}
                Ok(Err(e)) => {
                    tracing::error!("Failed to force-kill service '{}': {}", name, e);
                    return Err(e);
                }
                Err(_) => {
                    return Err(Error::Timeout(name.to_string()));
                }
            }
        }

        let mut tracker = self.state_tracker.write().await;
        tracker.unregister_service(name).await?;
        tracker.save().await?;
        Ok(())
    }

    /// Get all transitive dependents of a service in **deepest-first** order
    /// (post-order DFS).
    ///
    /// For `A <- B <- C` (B depends on A, C depends on B), this returns
    /// `["C", "B"]` — callers can iterate forward to stop dependents before
    /// their dependencies, or iterate in reverse to start them dependency-first.
    fn get_all_dependents(&self, service_name: &str) -> Vec<String> {
        let mut visited = std::collections::HashSet::new();
        let mut result = Vec::new();

        self.traverse_dependents(service_name, &mut visited, &mut result);

        result
    }

    fn traverse_dependents(
        &self,
        service_name: &str,
        visited: &mut std::collections::HashSet<String>,
        result: &mut Vec<String>,
    ) {
        if visited.contains(service_name) {
            return;
        }
        visited.insert(service_name.to_string());

        let dependents = self.dep_graph.get_dependents(service_name);
        for dependent in dependents {
            self.traverse_dependents(&dependent, visited, result);
            result.push(dependent);
        }
    }

    /// Stop a single service
    async fn stop_service(&self, name: &str) -> Result<()> {
        self.stop_service_impl(name)
            .instrument(tracing::info_span!("stop_service", service.name = %name))
            .await
    }

    /// Implementation of stop_service (separate to allow instrumentation)
    async fn stop_service_impl(&self, name: &str) -> Result<()> {
        // Persist the stop intent *before* the kill signal goes out, same
        // reasoning as `force_kill_service` above. Best-effort: a missing
        // row (never registered) is not an error here.
        let _ = self
            .state_tracker
            .write()
            .await
            .set_desired_state(name, DesiredState::Stopped)
            .await;

        // Get Arc clone of manager
        let manager_arc = {
            let services = self.services.read().await;
            if let Some(arc) = services.get(name) {
                Arc::clone(arc)
            } else {
                // Service not found, nothing to stop
                return Ok(());
            }
        };

        // Lock and stop the service
        async {
            let mut manager = manager_arc.lock().await;
            if manager.status() == Status::Stopped {
                return Result::<()>::Ok(());
            }
            manager.stop().await?;
            Result::<()>::Ok(())
        }
        .instrument(tracing::info_span!("stop_service_manager"))
        .await?;

        // Unregister from state tracker
        async {
            let mut tracker = self.state_tracker.write().await;
            tracker.unregister_service(name).await?;
            tracker.save().await?;
            Result::<()>::Ok(())
        }
        .instrument(tracing::info_span!("unregister_state"))
        .await?;

        Ok(())
    }

    /// Stop all running services.
    ///
    /// Services are stopped in reverse topological order to ensure
    /// dependents are stopped before their dependencies.
    ///
    /// Unlike `start` methods, stop operations continue even if cancelled
    /// to ensure proper cleanup.
    pub async fn stop_all(&self) -> Result<()> {
        // Get services in reverse topological order
        let order = self.dep_graph.topological_sort()?;

        let mut errors = Vec::new();

        // Stop in reverse order with timeout
        for name in order.iter().rev() {
            if let Err(e) = self.stop_service_with_timeout(name).await {
                errors.push(e);
            }
        }

        if !errors.is_empty() {
            return Err(Error::Multiple(errors));
        }

        Ok(())
    }

    /// Remove orphaned containers for this project.
    ///
    /// Finds containers matching `fed-{work_dir_hash}-*` that aren't tracked
    /// in the state database and removes them with `docker rm -f`.
    ///
    /// Returns the number of containers removed.
    ///
    /// Delegates to `OrphanCleaner`.
    pub async fn remove_orphaned_containers(&self) -> Result<usize> {
        let cleaner = super::orphans::OrphanCleaner::new(self);
        cleaner.remove_orphaned_containers().await
    }

    /// Restart all services in dependency-aware order.
    ///
    /// Stops all services first (in reverse dependency order), then starts
    /// them all (in dependency order).
    ///
    /// # Cancellation
    ///
    /// The stop phase will complete even if cancelled, but the start phase
    /// can be cancelled.
    pub async fn restart_all(&self) -> Result<StartOutcome> {
        // First, stop all services in reverse dependency order
        self.stop_all().await?;

        // Check for cancellation before starting
        if self.cancellation_token.is_cancelled() {
            return Err(Error::Cancelled("restart_all".to_string()));
        }

        // Then, start all services in dependency order
        self.start_all().await
    }

    /// Restart a single service, preserving the running state of its
    /// transitive dependents.
    ///
    /// `stop` cascades down to dependents; `start` only walks dependencies.
    /// A naive stop+start therefore leaves dependents stopped. This method:
    ///
    /// 1. Snapshots which transitive dependents are `Running`/`Healthy`.
    /// 2. Stops the service (cascades to all dependents).
    /// 3. Starts the service back (with its dependencies).
    /// 4. Restarts each previously-running dependent in shallowest-first
    ///    order so its own dependencies are healthy first.
    ///
    /// Dependents that were already stopped stay stopped — restart does not
    /// resurrect services the operator chose to stop.
    ///
    /// # Cancellation & Timeouts
    ///
    /// Each phase respects `startup_timeout` / `stop_timeout` and the
    /// orchestrator's cancellation token, matching `start` and `stop`.
    pub async fn restart(&self, service_name: &str) -> Result<StartOutcome> {
        let dependents = self.get_all_dependents(service_name);
        let statuses = self.get_status_passive().await;
        let was_running: Vec<String> = dependents
            .iter()
            .filter(|name| {
                matches!(
                    statuses.get(name.as_str()).copied(),
                    Some(Status::Running) | Some(Status::Healthy)
                )
            })
            .cloned()
            .collect();

        self.stop(service_name).await?;

        if self.cancellation_token.is_cancelled() {
            return Err(Error::Cancelled(service_name.to_string()));
        }

        let mut outcome = self.start(service_name).await?;

        // Dependents come back in shallowest-first order (reverse of the
        // stop iteration) so each one's own dependencies are already up.
        for dependent in was_running.iter().rev() {
            if self.cancellation_token.is_cancelled() {
                return Err(Error::Cancelled(dependent.clone()));
            }
            let health = self.start_service_with_timeout(dependent).await?;
            outcome.record(dependent, health);
        }

        Ok(outcome)
    }

    /// Returns current service statuses without triggering active health checks.
    /// Used for the initial post-startup display where containers may still be initializing.
    pub async fn get_status_passive(&self) -> HashMap<String, Status> {
        let services = self.services.read().await;
        let mut result = HashMap::new();
        for (name, arc) in services.iter() {
            let manager = arc.lock().await;
            result.insert(name.clone(), manager.status());
        }
        result
    }

    /// Get status of all services
    /// Also triggers health checks for running services to detect exits promptly
    pub async fn get_status(&self) -> HashMap<String, Status> {
        // Clone the Arcs and drop the map lock so health checks don't hold it
        let managers: Vec<(String, SharedServiceManager)> = {
            let services = self.services.read().await;
            services
                .iter()
                .map(|(name, arc)| (name.clone(), Arc::clone(arc)))
                .collect()
        };

        // Check all services concurrently: one slow docker healthcheck must not
        // serialize behind the others (the TUI polls this every tick)
        let checks = managers.into_iter().map(|(name, arc)| async move {
            let manager = arc.lock().await;
            let status = manager.status();

            // For running services, trigger a health check to detect exits
            // The health check is cached (500ms TTL) so this is inexpensive
            if status == Status::Running || status == Status::Healthy {
                // Health check updates status internally if process has died
                let _ = manager.health().await;
            }

            // Get status again after potential health check update
            (name, manager.status())
        });

        futures::future::join_all(checks)
            .await
            .into_iter()
            .collect()
    }

    /// Get a service manager
    pub async fn get_service(&self, name: &str) -> Option<Status> {
        let services = self.services.read().await;
        match services.get(name) {
            Some(arc) => {
                let manager = arc.lock().await;
                Some(manager.status())
            }
            None => None,
        }
    }

    /// Get the dependency graph
    pub fn get_dependency_graph(&self) -> &Graph {
        &self.dep_graph
    }

    /// Get a reference to the resolved config (templates substituted).
    pub fn get_config(&self) -> &Config {
        &self.config
    }

    /// Get a reference to resolved parameters.
    ///
    /// This is more efficient than [`Self::get_resolved_parameters_owned`] when you only
    /// need to read the parameters without taking ownership.
    pub fn get_resolved_parameters(&self) -> &HashMap<String, String> {
        self.resolver.get_resolved_parameters()
    }

    /// Get an owned copy of resolved parameters.
    ///
    /// Use [`Self::get_resolved_parameters`] instead if you only need to read the parameters.
    pub fn get_resolved_parameters_owned(&self) -> HashMap<String, String> {
        self.resolver.get_resolved_parameters().clone()
    }

    /// Resolved parameters as display views, sorted by name, with sensitive
    /// values redacted at the boundary (no raw secret material attached).
    ///
    /// Display surfaces (the TUI) must use this instead of
    /// [`Self::get_resolved_parameters_owned`].
    pub fn get_parameter_views(&self) -> Vec<crate::parameter::ParameterView> {
        self.resolver.get_parameter_views()
    }

    /// Get port resolution decisions for display in dry-run and status commands.
    pub fn get_port_resolutions(&self) -> &[crate::parameter::PortResolution] {
        self.resolver.get_port_resolutions()
    }

    /// Release port listeners so that port conflict checks can detect external processes.
    ///
    /// In dry-run mode, the resolver holds TcpListeners on resolved ports to prevent TOCTOU races.
    /// These must be released before checking for conflicts, otherwise the conflict checker
    /// detects our own listeners as "conflicts".
    pub fn release_port_listeners(&self) {
        self.release_port_listeners_once();
    }

    /// Check if any services in the config are Docker-based.
    pub fn has_docker_services(&self) -> bool {
        self.config.services.values().any(|svc| svc.image.is_some())
    }

    /// Check if a specific service is Docker-based (has an image).
    pub fn is_docker_service(&self, name: &str) -> bool {
        self.config
            .services
            .get(name)
            .map(|svc| svc.image.is_some())
            .unwrap_or(false)
    }

    /// Check if a specific service is process-based (has a process command).
    pub fn is_process_service(&self, name: &str) -> bool {
        self.config
            .services
            .get(name)
            .map(|svc| svc.process.is_some())
            .unwrap_or(false)
    }

    /// Get cloned services Arc for external access (e.g., signal handlers)
    pub fn get_services_arc(&self) -> SharedServiceRegistry {
        Arc::clone(&self.services)
    }

    /// Get service logs
    pub async fn get_logs(&self, service_name: &str, tail: Option<usize>) -> Result<Vec<String>> {
        let manager_arc = {
            let services = self.services.read().await;
            if let Some(arc) = services.get(service_name) {
                Arc::clone(arc)
            } else {
                return Err(Error::ServiceNotFound(service_name.to_string()));
            }
        };

        let manager = manager_arc.lock().await;
        manager.logs(tail).await
    }

    /// Get last error for a service (if any)
    pub async fn get_last_error(&self, service_name: &str) -> Option<String> {
        let manager_arc = {
            let services = self.services.read().await;
            services.get(service_name).map(Arc::clone)?
        };
        let manager = manager_arc.lock().await;
        manager.get_last_error()
    }

    /// Get service PID (if applicable)
    pub async fn get_service_pid(&self, service_name: &str) -> Result<Option<u32>> {
        let manager_arc = {
            let services = self.services.read().await;
            if let Some(arc) = services.get(service_name) {
                Arc::clone(arc)
            } else {
                return Err(Error::ServiceNotFound(service_name.to_string()));
            }
        };

        let manager = manager_arc.lock().await;
        Ok(manager.get_pid())
    }

    /// Run a script non-interactively, capturing output.
    ///
    /// This is a top-level entry point: services started to satisfy the script's
    /// dependencies (that weren't already running) are stopped after it finishes
    /// ("borrow-or-own"). Use `fed start` beforehand to keep a service up across runs.
    ///
    /// Delegates to `ScriptRunner`.
    pub async fn run_script(&self, script_name: &str) -> Result<std::process::Output> {
        let runner = super::scripts::ScriptRunner::new(self);
        runner.run_script(script_name, true).await
    }

    /// Run a script interactively with stdin/stdout/stderr passthrough.
    /// This is suitable for interactive TUIs like jest --watch.
    /// Returns only the exit status since output goes directly to terminal.
    ///
    /// Extra arguments are appended to the script command with proper shell escaping.
    /// Example: `fed test -- -t "specific test"` passes `-t "specific test"` to the script.
    ///
    /// If the script has `isolated: true`, it runs in an isolated context
    /// with fresh port allocations and isolated service instances.
    ///
    /// This is a top-level entry point: services started to satisfy the script's
    /// dependencies (that weren't already running) are stopped after it finishes
    /// ("borrow-or-own"). Use `fed start` beforehand to keep a service up across runs.
    ///
    /// Delegates to `ScriptRunner`.
    pub async fn run_script_interactive(
        &self,
        script_name: &str,
        extra_args: &[String],
    ) -> Result<std::process::ExitStatus> {
        let runner = super::scripts::ScriptRunner::new(self);
        runner
            .run_script_interactive(script_name, extra_args, true)
            .await
    }

    /// Run a script interactively as a *nested* dependency of another script.
    ///
    /// Identical to [`run_script_interactive`](Self::run_script_interactive) but
    /// marks the invocation as non-top-level, so it does not perform borrow-or-own
    /// cleanup — the outermost script owns tearing down everything the tree started.
    /// Used by `ScriptRunner` when resolving script-to-script dependencies.
    pub(crate) async fn run_script_interactive_nested(
        &self,
        script_name: &str,
        extra_args: &[String],
    ) -> Result<std::process::ExitStatus> {
        let runner = super::scripts::ScriptRunner::new(self);
        runner
            .run_script_interactive(script_name, extra_args, false)
            .await
    }

    /// Check if a service is currently running or healthy.
    pub async fn is_service_running(&self, service_name: &str) -> bool {
        let services = self.services.read().await;
        match services.get(service_name) {
            Some(arc) => {
                let manager = arc.lock().await;
                let status = manager.status();
                status == Status::Running || status == Status::Healthy
            }
            None => false,
        }
    }

    /// Get list of available scripts.
    ///
    /// Delegates to `ScriptRunner`.
    pub fn list_scripts(&self) -> Vec<String> {
        let runner = super::scripts::ScriptRunner::new(self);
        runner.list_scripts()
    }

    /// Mark startup as complete - allows monitoring to clean up dead services
    /// Called after all initial services have been started to prevent race conditions
    pub fn mark_startup_complete(&self) {
        self.startup_complete.store(true, Ordering::SeqCst);
        tracing::debug!("Startup complete - monitoring cleanup enabled");
    }

    /// Cleanup resources.
    ///
    /// This method:
    /// 1. Ensures cleanup runs exactly once (using atomic guard)
    /// 2. Signals monitoring loop to shut down
    /// 3. Waits for monitoring task to finish (with timeout)
    /// 4. Stops all running services
    /// 5. Releases port listeners
    /// 6. Clears the lock file if all services are stopped
    ///
    /// Check whether a service's process or container is actually alive.
    ///
    /// Used before skipping a start for a service that claims to be Running/Healthy,
    /// to catch cases where the process was killed or container died externally.
    fn verify_service_alive(&self, manager: &dyn ServiceManager) -> bool {
        if let Some(pid) = manager.get_pid() {
            return is_pid_alive(pid);
        }
        if let Some(container_id) = manager.get_container_id() {
            return crate::docker::is_container_running_sync(&container_id);
        }
        // No PID or container ID — can't verify, assume alive
        true
    }

    /// Can be called with `&self` for concurrent access patterns.
    /// Multiple concurrent calls are safe - only the first will execute.
    pub async fn cleanup(&self) {
        // Use compare_exchange to ensure cleanup runs exactly once
        if self
            .cleanup_started
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            tracing::debug!("Cleanup already in progress or completed, skipping");
            return;
        }

        self.shutdown_monitoring_for_cleanup().await;

        tracing::debug!("Cleanup: stopping all services");
        let _ = self.stop_all().await;
        self.release_ports_and_maybe_clear().await;
        tracing::debug!("Cleanup: complete");
    }

    /// Failure-path rollback for an aborted or failed `start`: tear down only
    /// what THIS run's start attempts actually created, leaving everything
    /// else — a concurrent winning `fed` process's registrations, services
    /// left healthy by a previous start — untouched. This is the
    /// orchestrator-wide counterpart of the per-service `attempt_owns_service`
    /// gate: an unscoped [`Orchestrator::cleanup`] here used to let a LOSING
    /// invocation's failed start stop and unregister the winner's live
    /// services. Full-session shutdown (watch-mode Ctrl-C) must keep calling
    /// [`Orchestrator::cleanup`] instead — its contract is "stop the stack".
    ///
    /// Two safeguards scope the teardown:
    /// - Only services in `owned_services` (recorded at the two sites that
    ///   set `attempt_owns_service`) are touched, via THIS process's manager
    ///   handles — never by state-DB row.
    /// - A state row is unregistered only when it still carries this run's
    ///   identity (our manager's PID / container id). Rows without identity
    ///   are left alone: a registered-but-never-spawned row was already
    ///   removed by the registration guard's drop, and after that another
    ///   process may have re-registered the name — deleting by name would
    ///   destroy THEIR registration. Identity-less completed-oneshot rows
    ///   are kept for the same reason (harmless, re-run on next start).
    ///
    /// Shares `cleanup_started` with [`Orchestrator::cleanup`]: the two are
    /// alternative terminal teardowns of one orchestrator lifecycle and must
    /// not both run.
    pub async fn cleanup_failed_start(&self) {
        if self
            .cleanup_started
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            tracing::debug!("Cleanup already in progress or completed, skipping");
            return;
        }

        self.shutdown_monitoring_for_cleanup().await;

        tracing::debug!("Failed-start rollback: stopping services owned by this run");
        let owned = self
            .owned_services
            .lock()
            .expect("owned_services lock poisoned")
            .clone();
        if !owned.is_empty() {
            // Reverse topological order, as in stop_all: dependents first.
            // Owned services absent from the graph (never built, or a cycle
            // error) are appended so they still get torn down.
            let order = self.dep_graph.topological_sort().unwrap_or_default();
            let mut remaining: std::collections::HashSet<&String> = owned.iter().collect();
            let mut plan: Vec<&String> = order
                .iter()
                .rev()
                .filter(|n| remaining.remove(*n))
                .collect();
            plan.extend(remaining);

            for name in plan {
                let manager_arc = {
                    let services = self.services.read().await;
                    services.get(name.as_str()).map(Arc::clone)
                };
                let Some(manager_arc) = manager_arc else {
                    continue;
                };
                // Capture identity BEFORE stopping — stop may clear it.
                let (pid, container_id) = {
                    let manager = manager_arc.lock().await;
                    (manager.get_pid(), manager.get_container_id())
                };
                if pid.is_none() && container_id.is_none() {
                    // Nothing was spawned by this run (hook-only node, or a
                    // start that failed before spawn). The registration
                    // guard already handled the row; leave state alone.
                    continue;
                }
                let stop_result = tokio::time::timeout(self.stop_timeout, async {
                    let mut manager = manager_arc.lock().await;
                    manager.stop().await
                })
                .await;
                match stop_result {
                    Ok(Ok(())) => {}
                    Ok(Err(e)) => {
                        // Keep the row: the process/container may still be
                        // alive and must stay tracked.
                        tracing::warn!(
                            "Failed-start rollback: stop of '{}' failed; its state row is kept: {}",
                            name,
                            e
                        );
                        continue;
                    }
                    Err(_) => {
                        tracing::warn!(
                            "Failed-start rollback: stop of '{}' timed out; its state row is kept",
                            name
                        );
                        continue;
                    }
                }
                // Conditional on identity so a name re-registered by another
                // process between our failure and this rollback survives.
                let mut tracker = self.state_tracker.write().await;
                if let Err(e) = tracker
                    .unregister_service_matching(name, pid, container_id.as_deref())
                    .await
                {
                    tracing::warn!(
                        "Failed-start rollback: unregister of '{}' failed: {}",
                        name,
                        e
                    );
                }
                let _ = tracker.save().await;
            }
        }

        self.release_ports_and_maybe_clear().await;
        tracing::debug!("Failed-start rollback: complete");
    }

    /// Shared preamble of both teardown paths: cancel in-progress operations
    /// and wait (bounded) for the monitoring task to wind down.
    async fn shutdown_monitoring_for_cleanup(&self) {
        // Cancel all in-progress operations first
        // This ensures any running start/stop operations bail out quickly
        tracing::debug!("Cleanup: canceling in-progress operations");
        self.cancellation_token.cancel();

        // CancellationToken is permanent — once cancelled, it remains cancelled.
        // The monitoring loop will see it on its next select! iteration and break,
        // even if it was mid-health-check when cancel() was called above.

        // Wait for monitoring task to finish (with timeout to avoid hanging)
        {
            let mut task_opt = self.monitoring_task.lock().await;
            if let Some(handle) = task_opt.take() {
                // Drop the guard before awaiting the handle
                drop(task_opt);
                // Wait for the task with a timeout to prevent indefinite hanging
                // No sleep needed - the cancel + notify should cause immediate response
                tracing::debug!("Cleanup: waiting for monitoring task");
                match tokio::time::timeout(Duration::from_secs(5), handle).await {
                    Ok(_) => tracing::debug!("Cleanup: monitoring task completed"),
                    Err(_) => tracing::warn!("Cleanup: monitoring task join timed out, continuing"),
                }
            }
        }
    }

    /// Shared tail of both teardown paths: release port listeners, then clear
    /// residual state bookkeeping only when no service rows remain.
    async fn release_ports_and_maybe_clear(&self) {
        tracing::debug!("Cleanup: releasing port listeners");
        // Use shared cleanup since we only have &self
        self.resolver.cleanup_shared();

        // Clear lock file if all services are stopped
        tracing::debug!("Cleanup: checking state tracker");
        if self
            .state_tracker
            .read()
            .await
            .get_services()
            .await
            .is_empty()
        {
            let _ = self.state_tracker.write().await.clear().await;
        }
    }

    /// Record that this orchestrator instance's start attempt owns `name` —
    /// it won the cross-process registration race or ran a oneshot's hooks.
    /// Consulted by [`Orchestrator::cleanup_failed_start`] to scope
    /// failure-path teardown.
    fn record_owned(&self, name: &str) {
        self.owned_services
            .lock()
            .expect("owned_services lock poisoned")
            .insert(name.to_string());
    }

    /// Stop monitoring without stopping any service.
    ///
    /// This is the narrow shutdown used by every supervisor-exit path
    /// (SIGTERM from `fed stop`'s teardown check, the watch/tui pre-flight
    /// handoff, the "nothing supervised left running" self-exit condition —
    /// `07-supervisor.md` Design §1): cancels the monitoring loop's
    /// cancellation token and awaits the monitoring task with a timeout, but
    /// **never** calls [`Orchestrator::stop_all`] and never calls
    /// `manager.stop()` on anything.
    ///
    /// [`Orchestrator::cleanup`] is deliberately not reused here — its own
    /// doc comment states it stops every running service, which is exactly
    /// backwards for a supervisor: the supervisor exiting must only ever
    /// mean "stop watching," never "stop the things I was watching," whether
    /// the exit is because a foreground `--watch`/`tui` is taking over or a
    /// SIGTERM arrived.
    ///
    /// Uses a guard separate from `cleanup_started` — see that field's doc
    /// comment for why the two shutdown paths don't share one.
    ///
    /// Safe to call multiple times or concurrently — like `cleanup()`, only
    /// the first caller does the work.
    pub async fn stop_monitoring_only(&self) {
        if self
            .monitoring_stop_started
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            tracing::debug!("stop_monitoring_only: already stopped or in progress, skipping");
            return;
        }

        tracing::debug!("stop_monitoring_only: canceling monitoring loop");
        self.cancellation_token.cancel();

        let mut task_opt = self.monitoring_task.lock().await;
        if let Some(handle) = task_opt.take() {
            drop(task_opt);
            tracing::debug!("stop_monitoring_only: waiting for monitoring task");
            match tokio::time::timeout(Duration::from_secs(5), handle).await {
                Ok(_) => tracing::debug!("stop_monitoring_only: monitoring task completed"),
                Err(_) => {
                    tracing::warn!(
                        "stop_monitoring_only: monitoring task join timed out, continuing"
                    )
                }
            }
        }

        tracing::debug!("stop_monitoring_only: complete (services left running)");
    }

    /// Whether at least one service inside the supervisor's health-check
    /// scope ([`super::monitoring::supervised_service_names`]) is currently
    /// `desired_state == Running`.
    ///
    /// Used by `fed supervise`'s per-tick self-exit check
    /// (`07-supervisor.md` Design §1/§7): once nothing the supervisor
    /// cares about remains desired-running (every such service has either
    /// been explicitly `fed stop`'d or was never started), the daemon has
    /// nothing left to protect and should exit rather than run forever.
    ///
    /// A service in scope with no persisted row at all (never started)
    /// correctly counts as "not desired-running" here — see
    /// [`crate::state::SqliteStateTracker::is_desired_running`]'s own doc
    /// comment on why missing and stopped collapse to the same answer.
    pub async fn any_supervised_service_desired_running(&self) -> bool {
        let scope = super::monitoring::supervised_service_names(&self.config);
        if scope.is_empty() {
            return false;
        }

        let tracker = self.state_tracker.read().await;
        for name in &scope {
            if tracker.is_desired_running(name).await {
                return true;
            }
        }
        false
    }

    /// Stop any supervised service that is alive but has
    /// `desired_state = stopped` — the reconcile half of the desired-state
    /// contract. The restart gate only prevents future restarts of dead
    /// services; a restart already in flight when a partial `fed stop`
    /// wrote Stopped lands after the kill and leaves a live process the
    /// user asked to stop. Called from the supervisor's poll tick.
    pub async fn stop_supervised_not_desired_running(&self) {
        let scope = super::monitoring::supervised_service_names(&self.config);
        if scope.is_empty() {
            return;
        }

        let mut to_stop = Vec::new();
        {
            let tracker = self.state_tracker.read().await;
            let statuses = self.get_status_passive().await;
            for name in &scope {
                let live = statuses
                    .get(name)
                    .is_some_and(|s| !matches!(s, crate::service::Status::Stopped));
                if live && !tracker.is_desired_running(name).await {
                    to_stop.push(name.clone());
                }
            }
        }
        for name in to_stop {
            tracing::info!(
                "fed supervise: reconciling '{}': alive but desired_state=stopped; stopping it",
                name
            );
            if let Err(e) = self.stop(&name).await {
                tracing::warn!("fed supervise: reconcile stop of '{}' failed: {}", name, e);
            }
        }
    }

    /// Pre-pull Docker images needed by the given services.
    ///
    /// Checks which images are missing locally and pulls them in parallel.
    /// Returns results only for images that needed pulling (already-local images are skipped).
    pub async fn pre_pull_images(&self, services: &[String]) -> Vec<ImagePullResult> {
        use crate::docker::DockerClient;

        // Collect unique images from Docker-type services
        let mut images: Vec<String> = Vec::new();
        for name in services {
            if let Some(svc) = self.config.services.get(name)
                && let Some(ref image) = svc.image
                && !images.contains(image)
            {
                images.push(image.clone());
            }
        }

        if images.is_empty() {
            return Vec::new();
        }

        let client = DockerClient::new();

        // Check which images exist locally (parallel)
        let exist_checks: Vec<_> = images
            .iter()
            .map(|img| {
                let client = client.clone();
                let img = img.clone();
                async move {
                    let exists = client.image_exists(&img).await;
                    (img, exists)
                }
            })
            .collect();
        let exist_results = futures::future::join_all(exist_checks).await;

        // Filter to only missing images
        let missing: Vec<String> = exist_results
            .into_iter()
            .filter(|(_, exists)| !exists)
            .map(|(img, _)| img)
            .collect();

        if missing.is_empty() {
            return Vec::new();
        }

        // Pull missing images in parallel
        let pull_timeout = Duration::from_secs(300); // 5 minutes, matches DOCKER_PULL_TIMEOUT
        let pull_futures: Vec<_> = missing
            .iter()
            .map(|img| {
                let client = client.clone();
                let img = img.clone();
                async move {
                    let outcome = match client.pull(&img, pull_timeout).await {
                        Ok(()) => Ok(()),
                        Err(e) => Err(e.to_string()),
                    };
                    ImagePullResult {
                        image: img,
                        outcome,
                    }
                }
            })
            .collect();

        futures::future::join_all(pull_futures).await
    }
}

/// Result of a Docker image pull attempt.
pub struct ImagePullResult {
    pub image: String,
    pub outcome: std::result::Result<(), String>,
}

/// Check if a PID is alive (signal 0 check).
///
/// This is a free function shared across orchestrator submodules.
/// Both `orphans` (orphan process detection) and `ports` (managed port
/// collection) need it, so it lives here to avoid cross-module dependencies.
pub(super) fn is_pid_alive(pid: u32) -> bool {
    #[cfg(unix)]
    {
        use crate::error::validate_pid_for_check;
        use nix::sys::signal::kill;
        if let Some(nix_pid) = validate_pid_for_check(pid) {
            return kill(nix_pid, None).is_ok();
        }
        false
    }
    #[cfg(not(unix))]
    {
        let _ = pid;
        true // Can't check on non-unix, assume alive
    }
}

impl Drop for Orchestrator {
    fn drop(&mut self) {
        // Cancel the token so the monitoring loop breaks on its next select! iteration.
        // CancellationToken::cancel() is synchronous and sticky — safe to call from Drop.
        self.cancellation_token.cancel();
    }
}

impl std::fmt::Debug for Orchestrator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Orchestrator")
            .field("namespace", &self.namespace)
            .field("work_dir", &self.work_dir)
            .field("output_mode", &self.output_mode)
            .field("active_profiles", &self.active_profiles)
            .field("startup_timeout", &self.startup_timeout)
            .field("stop_timeout", &self.stop_timeout)
            .field(
                "startup_complete",
                &self.startup_complete.load(Ordering::Relaxed),
            )
            .field(
                "port_listeners_released",
                &self.port_listeners_released.load(Ordering::Relaxed),
            )
            .field(
                "cleanup_started",
                &self.cleanup_started.load(Ordering::Relaxed),
            )
            .field(
                "monitoring_stop_started",
                &self.monitoring_stop_started.load(Ordering::Relaxed),
            )
            .field("is_cancelled", &self.is_cancelled())
            .field("resolver", &"<resolver>")
            .field("state_tracker", &"<state_tracker>")
            .field("services", &"<async>")
            .field("health_checkers", &"<async>")
            .field("monitoring_task", &"<async>")
            .field("cancellation_token", &"<token>")
            .field("config", &self.config)
            .field("dep_graph", &self.dep_graph)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
// clippy::disallowed_methods (clippy.toml) fires on these library-internal
// unit-test call sites too (it matches on the resolved item path, not crate
// boundaries) — same-crate, lower-risk, already bounded by #[cfg(test)], so
// they're allowed here rather than migrated to a test-only helper.
#[allow(clippy::disallowed_methods)]
mod tests {
    use super::*;
    use crate::config::Config;

    /// Test that concurrent cleanup calls are handled safely - only one executes
    #[tokio::test]
    async fn test_concurrent_cleanup_guard() {
        let temp_dir = tempfile::tempdir().unwrap();
        let config = Config::default();
        let orchestrator = Orchestrator::new(config, temp_dir.path().to_path_buf())
            .await
            .unwrap();

        // Verify cleanup_started is initially false
        assert!(!orchestrator.cleanup_started.load(Ordering::SeqCst));

        // Run cleanup multiple times concurrently
        let orch = Arc::new(orchestrator);
        let orch1 = Arc::clone(&orch);
        let orch2 = Arc::clone(&orch);
        let orch3 = Arc::clone(&orch);

        let (r1, r2, r3) = tokio::join!(orch1.cleanup(), orch2.cleanup(), orch3.cleanup());

        // All should complete without panic (one executes, others skip)
        let _ = (r1, r2, r3);

        // Verify cleanup_started is now true
        assert!(orch.cleanup_started.load(Ordering::SeqCst));
    }

    /// Test that cleanup runs exactly once even with sequential calls
    #[tokio::test]
    async fn test_cleanup_runs_once() {
        let temp_dir = tempfile::tempdir().unwrap();
        let config = Config::default();
        let orchestrator = Orchestrator::new(config, temp_dir.path().to_path_buf())
            .await
            .unwrap();
        let orch = Arc::new(orchestrator);

        // First cleanup should execute
        orch.cleanup().await;
        assert!(orch.cleanup_started.load(Ordering::SeqCst));

        // Second cleanup should be skipped (no-op)
        orch.cleanup().await;

        // Should still be true (no reset)
        assert!(orch.cleanup_started.load(Ordering::SeqCst));
    }

    /// Test that cleanup completes within a reasonable timeout
    #[tokio::test]
    async fn test_cleanup_does_not_hang() {
        let temp_dir = tempfile::tempdir().unwrap();
        let config = Config::default();
        let orchestrator = Orchestrator::new(config, temp_dir.path().to_path_buf())
            .await
            .unwrap();
        let orch = Arc::new(orchestrator);

        // Cleanup should complete within 10 seconds even in worst case
        let result = tokio::time::timeout(Duration::from_secs(10), orch.cleanup()).await;

        // Should complete without timeout
        assert!(result.is_ok(), "Cleanup should complete within timeout");
    }

    /// Test that cleanup cancels the cancellation token
    #[tokio::test]
    async fn test_cleanup_cancels_operations() {
        let temp_dir = tempfile::tempdir().unwrap();
        let config = Config::default();
        let orchestrator = Orchestrator::new(config, temp_dir.path().to_path_buf())
            .await
            .unwrap();

        // Before cleanup, cancellation should not be set
        assert!(!orchestrator.is_cancelled());

        let orch = Arc::new(orchestrator);
        orch.cleanup().await;

        // After cleanup, cancellation should be set
        assert!(orch.is_cancelled());
    }

    /// Test that verify_service_alive returns false for dead PIDs
    #[tokio::test]
    async fn test_verify_service_alive_dead_pid() {
        use crate::service::ProcessService;
        use std::collections::HashMap;

        let temp_dir = tempfile::tempdir().unwrap();
        let config = Config::default();
        let orchestrator = Orchestrator::new(config, temp_dir.path().to_path_buf())
            .await
            .unwrap();

        // Create a process service with a PID that doesn't exist
        let service_config = crate::config::Service {
            process: Some("echo hello".to_string()),
            ..Default::default()
        };
        let service = ProcessService::new(
            "test".into(),
            service_config,
            HashMap::new(),
            "/tmp".into(),
            OutputMode::Captured,
            None,
        );
        service.set_pid(999_999); // PID that almost certainly doesn't exist

        assert!(
            !orchestrator.verify_service_alive(&service),
            "Dead PID should not be considered alive"
        );
    }

    /// Test that verify_service_alive returns true when no PID or container
    #[tokio::test]
    async fn test_verify_service_alive_no_tracking_info() {
        use crate::service::ProcessService;
        use std::collections::HashMap;

        let temp_dir = tempfile::tempdir().unwrap();
        let config = Config::default();
        let orchestrator = Orchestrator::new(config, temp_dir.path().to_path_buf())
            .await
            .unwrap();

        // Fresh service with no PID set
        let service_config = crate::config::Service {
            process: Some("echo hello".to_string()),
            ..Default::default()
        };
        let service = ProcessService::new(
            "test".into(),
            service_config,
            HashMap::new(),
            "/tmp".into(),
            OutputMode::Captured,
            None,
        );

        assert!(
            orchestrator.verify_service_alive(&service),
            "Service with no PID/container should be assumed alive"
        );
    }

    /// Build an orchestrator whose dependency graph is `a <- b <- c`
    /// (`b` depends on `a`, `c` depends on `b`).
    async fn orch_with_chain_a_b_c() -> Orchestrator {
        use crate::config::{DependsOn, Service};

        let mut config = Config::default();
        config.services.insert(
            "a".to_string(),
            Service {
                process: Some("sleep 1".to_string()),
                ..Default::default()
            },
        );
        config.services.insert(
            "b".to_string(),
            Service {
                process: Some("sleep 1".to_string()),
                depends_on: vec![DependsOn::Simple("a".to_string())],
                ..Default::default()
            },
        );
        config.services.insert(
            "c".to_string(),
            Service {
                process: Some("sleep 1".to_string()),
                depends_on: vec![DependsOn::Simple("b".to_string())],
                ..Default::default()
            },
        );

        let temp_dir = tempfile::tempdir().unwrap();
        let mut orchestrator = Orchestrator::new(config, temp_dir.path().to_path_buf())
            .await
            .unwrap();
        orchestrator.build_dependency_graph().unwrap();
        // The state tracker holds an open SQLite handle inside `temp_dir`;
        // leaking keeps it valid for the lifetime of the test.
        std::mem::forget(temp_dir);
        orchestrator
    }

    /// `get_all_dependents` must return deepest-first (post-order DFS) so
    /// callers can iterate forward when stopping. `stop` relies on this.
    #[tokio::test]
    async fn test_get_all_dependents_returns_deepest_first() {
        let orchestrator = orch_with_chain_a_b_c().await;
        let dependents = orchestrator.get_all_dependents("a");

        assert_eq!(
            dependents,
            vec!["c".to_string(), "b".to_string()],
            "expected deepest-first order [c, b]; got {:?}",
            dependents
        );
    }

    /// `a` has two unrelated direct dependents — both must appear in the
    /// result, in either order.
    #[tokio::test]
    async fn test_get_all_dependents_fan_out() {
        use crate::config::{DependsOn, Service};

        let mut config = Config::default();
        config.services.insert(
            "a".to_string(),
            Service {
                process: Some("sleep 1".to_string()),
                ..Default::default()
            },
        );
        for name in ["b", "c"] {
            config.services.insert(
                name.to_string(),
                Service {
                    process: Some("sleep 1".to_string()),
                    depends_on: vec![DependsOn::Simple("a".to_string())],
                    ..Default::default()
                },
            );
        }

        let temp_dir = tempfile::tempdir().unwrap();
        let mut orchestrator = Orchestrator::new(config, temp_dir.path().to_path_buf())
            .await
            .unwrap();
        orchestrator.build_dependency_graph().unwrap();
        std::mem::forget(temp_dir);

        let dependents = orchestrator.get_all_dependents("a");
        assert_eq!(
            dependents.len(),
            2,
            "expected both fan-out dependents; got {:?}",
            dependents
        );
        assert!(
            dependents.contains(&"b".to_string()) && dependents.contains(&"c".to_string()),
            "expected b and c in dependents; got {:?}",
            dependents
        );
    }

    /// `apply_run_context` followed by `current_run_context` must reproduce
    /// every field of the original `RunContext` exactly — including
    /// `profiles`, which an earlier draft of the RunContext plan omitted
    /// from `apply_run_context` while still reading it back in
    /// `current_run_context`, an asymmetric pairing that would have shipped
    /// a silent round-trip gap.
    #[tokio::test]
    async fn apply_run_context_round_trips_every_field() {
        let temp_dir = tempfile::tempdir().unwrap();
        let config = Config::default();
        let mut orchestrator = Orchestrator::new(config, temp_dir.path().to_path_buf())
            .await
            .unwrap();

        let mut required_secret_names = std::collections::HashSet::new();
        required_secret_names.insert("API_KEY".to_string());

        let ctx = super::super::RunContext {
            offline: true,
            secret_cache: super::super::SecretCacheMode::Memory,
            is_interactive: true,
            output_mode: OutputMode::Passthrough,
            profiles: vec!["a".to_string(), "b".to_string()],
            required_secret_names: Some(required_secret_names),
        };

        orchestrator.apply_run_context(&ctx);
        let round_tripped = orchestrator.current_run_context();

        assert_eq!(round_tripped.offline, ctx.offline);
        assert_eq!(round_tripped.secret_cache, ctx.secret_cache);
        assert_eq!(round_tripped.is_interactive, ctx.is_interactive);
        assert_eq!(round_tripped.output_mode, ctx.output_mode);
        assert_eq!(round_tripped.profiles, ctx.profiles);
        assert_eq!(
            round_tripped.required_secret_names,
            ctx.required_secret_names
        );
    }

    // --- initialize_supervisor (07-supervisor.md Design §1) ---

    /// Direct test for the "attach" half of Design §1 step 4: a row that's
    /// genuinely still alive (not stale) and `desired_state == Running` must
    /// get its manager restored/attached, while a row with
    /// `desired_state == Stopped` must not — even though its row still
    /// exists (not yet purged) and nothing about it looks stale on its own
    /// (a `Stopped` status with no PID/container is never considered stale
    /// by `mark_dead_services`).
    #[tokio::test]
    async fn test_initialize_supervisor_restores_only_desired_running_rows() {
        use crate::config::Service;
        use crate::state::{DesiredState, ServiceState};

        // A real, long-lived child process so mark_dead_services sees it as
        // genuinely alive (not stale). Long enough to survive a slow CI
        // runner: with only 2 seconds, the child could exit before
        // initialize_supervisor's liveness check under load, making the
        // attach assertion flake (observed on GitHub Actions). The test
        // kills the child on exit.
        let mut child = std::process::Command::new("sleep")
            .arg("300")
            .spawn()
            .expect("failed to spawn helper process");
        let live_pid = child.id();

        let mut config = Config::default();
        config.services.insert(
            "alive".to_string(),
            Service {
                process: Some("sleep 2".to_string()),
                ..Default::default()
            },
        );
        config.services.insert(
            "stopped".to_string(),
            Service {
                process: Some("sleep 2".to_string()),
                ..Default::default()
            },
        );

        let temp_dir = tempfile::tempdir().unwrap();
        let mut orchestrator = Orchestrator::new(config, temp_dir.path().to_path_buf())
            .await
            .unwrap();

        {
            let mut tracker = orchestrator.state_tracker.write().await;
            tracker.initialize().await.unwrap();

            tracker
                .register_service(ServiceState {
                    id: "alive".to_string(),
                    status: Status::Running,
                    service_type: ServiceType::Process,
                    pid: Some(live_pid),
                    container_id: None,
                    started_at: chrono::Utc::now(),
                    external_repo: None,
                    namespace: "root".to_string(),
                    restart_count: 0,
                    last_restart_at: None,
                    consecutive_failures: 0,
                    port_allocations: Default::default(),
                    startup_message: None,
                    desired_state: DesiredState::Running,
                    native_restart_enabled: false,
                })
                .await
                .unwrap();

            tracker
                .register_service(ServiceState {
                    id: "stopped".to_string(),
                    status: Status::Stopped,
                    service_type: ServiceType::Process,
                    pid: None,
                    container_id: None,
                    started_at: chrono::Utc::now(),
                    external_repo: None,
                    namespace: "root".to_string(),
                    restart_count: 0,
                    last_restart_at: None,
                    consecutive_failures: 0,
                    port_allocations: Default::default(),
                    startup_message: None,
                    desired_state: DesiredState::Stopped,
                    native_restart_enabled: false,
                })
                .await
                .unwrap();
        }

        orchestrator
            .initialize_supervisor()
            .await
            .expect("initialize_supervisor should succeed");

        {
            let services = orchestrator.services.read().await;

            let alive_manager = services.get("alive").expect("alive manager must exist");
            let alive_pid = alive_manager.lock().await.get_pid();
            assert_eq!(
                alive_pid,
                Some(live_pid),
                "desired_state=Running row must have its PID restored/attached"
            );

            let stopped_manager = services
                .get("stopped")
                .expect("stopped manager must exist (a manager object, just not attached)");
            let stopped_pid = stopped_manager.lock().await.get_pid();
            assert_eq!(
                stopped_pid, None,
                "desired_state=Stopped row must never be restored/attached, \
                 even though its row still exists and isn't stale"
            );
        }

        let _ = child.kill();
        let _ = child.wait();
    }

    /// Direct test for the "crash-then-nobody-was-watching" half of Design
    /// §1 step 3: a row that goes stale *during this very
    /// `initialize_supervisor()` call* (its PID is dead) must be
    /// re-derived, not silently lost — restarted if `desired_state ==
    /// Running` and its restart policy allows it, left alone if
    /// `desired_state == Stopped` (never resurrected, regardless of restart
    /// policy).
    #[tokio::test]
    async fn test_initialize_supervisor_restarts_newly_stale_running_not_stopped() {
        use crate::config::{RestartPolicy, Service};
        use crate::state::{DesiredState, ServiceState};

        // PIDs guaranteed to be dead (the process has already exited).
        let mut dead1 = std::process::Command::new("true").spawn().unwrap();
        let dead_pid_restart = dead1.id();
        dead1.wait().unwrap();
        let mut dead2 = std::process::Command::new("true").spawn().unwrap();
        let dead_pid_stopped = dead2.id();
        dead2.wait().unwrap();

        let mut config = Config::default();
        config.services.insert(
            "crash-restart".to_string(),
            Service {
                process: Some("sleep 2".to_string()),
                restart: Some(RestartPolicy::Always),
                ..Default::default()
            },
        );
        config.services.insert(
            "crash-no-resurrect".to_string(),
            Service {
                process: Some("sleep 2".to_string()),
                restart: Some(RestartPolicy::Always),
                ..Default::default()
            },
        );

        let temp_dir = tempfile::tempdir().unwrap();
        let mut orchestrator = Orchestrator::new(config, temp_dir.path().to_path_buf())
            .await
            .unwrap();

        {
            let mut tracker = orchestrator.state_tracker.write().await;
            tracker.initialize().await.unwrap();

            tracker
                .register_service(ServiceState {
                    id: "crash-restart".to_string(),
                    status: Status::Running,
                    service_type: ServiceType::Process,
                    pid: Some(dead_pid_restart),
                    container_id: None,
                    started_at: chrono::Utc::now(),
                    external_repo: None,
                    namespace: "root".to_string(),
                    restart_count: 0,
                    last_restart_at: None,
                    consecutive_failures: 0,
                    port_allocations: Default::default(),
                    startup_message: None,
                    desired_state: DesiredState::Running,
                    native_restart_enabled: false,
                })
                .await
                .unwrap();

            tracker
                .register_service(ServiceState {
                    id: "crash-no-resurrect".to_string(),
                    status: Status::Running,
                    service_type: ServiceType::Process,
                    pid: Some(dead_pid_stopped),
                    container_id: None,
                    started_at: chrono::Utc::now(),
                    external_repo: None,
                    namespace: "root".to_string(),
                    restart_count: 0,
                    last_restart_at: None,
                    consecutive_failures: 0,
                    port_allocations: Default::default(),
                    startup_message: None,
                    desired_state: DesiredState::Stopped,
                    native_restart_enabled: false,
                })
                .await
                .unwrap();
        }

        orchestrator
            .initialize_supervisor()
            .await
            .expect("initialize_supervisor should succeed");

        let restarted_pid = {
            let services = orchestrator.services.read().await;

            let restart_manager = services
                .get("crash-restart")
                .expect("crash-restart manager must exist");
            let restarted_pid = restart_manager.lock().await.get_pid();
            assert!(
                matches!(restarted_pid, Some(pid) if pid != dead_pid_restart),
                "a newly-stale, desired_state=Running, restart:always service must be \
                 driven through a fresh start (there's nothing alive to attach to) — got {:?}",
                restarted_pid
            );

            let no_resurrect_manager = services
                .get("crash-no-resurrect")
                .expect("crash-no-resurrect manager must exist (just never started)");
            let no_resurrect_pid = no_resurrect_manager.lock().await.get_pid();
            assert_eq!(
                no_resurrect_pid, None,
                "a newly-stale, desired_state=Stopped service must never be \
                 resurrected, even with restart:always configured"
            );

            restarted_pid
        };

        // Clean up the real process spawned by the restart path.
        if let Some(pid) = restarted_pid {
            let _ = std::process::Command::new("kill")
                .args(["-9", &pid.to_string()])
                .status();
        }
    }

    /// Minimal manager stub for `await_concurrent_start` tests: the loser
    /// path only touches the manager through `await_healthcheck`, which with
    /// no registered checker never calls it at all.
    struct StubManager;

    #[async_trait::async_trait]
    impl ServiceManager for StubManager {
        async fn start(&mut self) -> Result<()> {
            Ok(())
        }
        async fn stop(&mut self) -> Result<()> {
            Ok(())
        }
        async fn kill(&mut self) -> Result<()> {
            Ok(())
        }
        async fn health(&self) -> Result<bool> {
            Ok(true)
        }
        fn status(&self) -> Status {
            Status::Running
        }
        fn name(&self) -> &str {
            "stub"
        }
        fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
            self
        }
    }

    /// Orchestrator + registered `Starting` row for `svc`, as a lost
    /// registration race would observe it.
    async fn orchestrator_with_starting_row(
        temp_dir: &tempfile::TempDir,
    ) -> (
        Orchestrator,
        Arc<tokio::sync::Mutex<Box<dyn ServiceManager>>>,
    ) {
        let orchestrator = Orchestrator::new(Config::default(), temp_dir.path().to_path_buf())
            .await
            .unwrap();
        {
            let mut tracker = orchestrator.state_tracker.write().await;
            tracker.initialize().await.unwrap();
            tracker
                .register_service(ServiceState::new(
                    "svc".to_string(),
                    ServiceType::Process,
                    "root".to_string(),
                ))
                .await
                .unwrap();
        }
        let manager: Arc<tokio::sync::Mutex<Box<dyn ServiceManager>>> =
            Arc::new(tokio::sync::Mutex::new(Box::new(StubManager)));
        (orchestrator, manager)
    }

    /// The loser must block until the winner resolves, then report the
    /// winner's real observation — here, `Healthy`.
    #[tokio::test]
    async fn concurrent_start_loser_waits_for_winner_and_reports_healthy() {
        let temp_dir = tempfile::tempdir().unwrap();
        let (orchestrator, manager) = orchestrator_with_starting_row(&temp_dir).await;

        let tracker = Arc::clone(&orchestrator.state_tracker);
        let winner = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(400)).await;
            let mut t = tracker.write().await;
            t.update_service_status("svc", Status::Healthy)
                .await
                .unwrap();
            t.save().await.unwrap();
        });

        let health = orchestrator
            .await_concurrent_start("svc", &manager, Duration::from_secs(10))
            .await
            .expect("loser must succeed once the winner reports Healthy");
        assert_eq!(health, StartHealth::Healthy);
        winner.await.unwrap();
    }

    /// A winner that reaches `Running` with no healthcheck configured
    /// resolves the loser as `Unchecked` — same observation the winner made.
    #[tokio::test]
    async fn concurrent_start_loser_reports_unchecked_when_winner_runs_without_checker() {
        let temp_dir = tempfile::tempdir().unwrap();
        let (orchestrator, manager) = orchestrator_with_starting_row(&temp_dir).await;

        {
            let mut t = orchestrator.state_tracker.write().await;
            t.update_service_status("svc", Status::Running)
                .await
                .unwrap();
            t.save().await.unwrap();
        }

        let health = orchestrator
            .await_concurrent_start("svc", &manager, Duration::from_secs(10))
            .await
            .expect("Running with no checker resolves immediately");
        assert_eq!(health, StartHealth::Unchecked);
    }

    /// A winner whose start fails unregisters the row; the loser must
    /// surface an error, never a silent success.
    #[tokio::test]
    async fn concurrent_start_loser_errors_when_winner_fails() {
        let temp_dir = tempfile::tempdir().unwrap();
        let (orchestrator, manager) = orchestrator_with_starting_row(&temp_dir).await;

        let tracker = Arc::clone(&orchestrator.state_tracker);
        let winner = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(400)).await;
            let mut t = tracker.write().await;
            t.unregister_service("svc").await.unwrap();
        });

        let error = orchestrator
            .await_concurrent_start("svc", &manager, Duration::from_secs(10))
            .await
            .expect_err("a failed winning attempt must fail the loser too");
        assert!(
            matches!(error, Error::ServiceStartFailed(ref name, _) if name == "svc"),
            "expected ServiceStartFailed, got {:?}",
            error
        );
        winner.await.unwrap();
    }

    /// A start that loses the registration race and is then cancelled while
    /// waiting on the winner must NOT run interrupted-start cleanup — that
    /// would force-kill and unregister the WINNER's registration.
    #[tokio::test]
    async fn cancelled_loser_leaves_winners_registration_alone() {
        use crate::service::ProcessService;

        let temp_dir = tempfile::tempdir().unwrap();
        let mut config = Config::default();
        config.services.insert(
            "svc".to_string(),
            crate::config::Service {
                process: Some("sleep 300".to_string()),
                ..Default::default()
            },
        );
        let orchestrator = Orchestrator::new(config.clone(), temp_dir.path().to_path_buf())
            .await
            .unwrap();
        {
            let mut tracker = orchestrator.state_tracker.write().await;
            tracker.initialize().await.unwrap();
            // The "winner": a concurrent attempt's registration, mid-hooks.
            tracker
                .register_service(ServiceState::new(
                    "svc".to_string(),
                    ServiceType::Process,
                    "root".to_string(),
                ))
                .await
                .unwrap();
        }
        {
            let manager = ProcessService::new(
                "svc".into(),
                config.services.get("svc").unwrap().clone(),
                HashMap::new(),
                temp_dir.path().to_string_lossy().into_owned(),
                OutputMode::Captured,
                None,
            );
            orchestrator.services.write().await.insert(
                "svc".to_string(),
                Arc::new(tokio::sync::Mutex::new(Box::new(manager))),
            );
        }

        let orch = Arc::new(orchestrator);
        let starter = {
            let orch = Arc::clone(&orch);
            tokio::spawn(async move { orch.start_service_with_timeout("svc").await })
        };
        // Let the loser lose the race and settle into polling the winner.
        tokio::time::sleep(Duration::from_millis(600)).await;
        // Guard against a vacuous pass: if the starter already returned
        // (e.g. failed before ever losing the race), cancellation would
        // trivially "leave the row alone" without exercising the loser path.
        assert!(
            !starter.is_finished(),
            "starter must still be waiting on the winner when we cancel"
        );
        orch.cancellation_token.cancel();

        let result = starter.await.unwrap();
        assert!(
            matches!(result, Err(Error::Cancelled(_))),
            "the loser must report cancellation, got {:?}",
            result
        );

        // Give any (buggy) cleanup task time to run before checking.
        tokio::time::sleep(Duration::from_millis(200)).await;
        let row = {
            let tracker = orch.state_tracker.read().await;
            tracker.get_service("svc").await
        };
        assert!(
            matches!(row.map(|s| s.status), Some(Status::Starting)),
            "the winner's Starting registration must survive the loser's cancellation"
        );
    }

    /// The failed-start rollback must not touch registrations this
    /// orchestrator's start attempts don't own: a losing invocation whose own
    /// start failed (e.g. a hook error) used to stop_all + clear, destroying
    /// the concurrent winner's live state rows.
    #[tokio::test]
    async fn cleanup_leaves_unowned_registrations_alone() {
        let temp_dir = tempfile::tempdir().unwrap();
        let mut config = Config::default();
        config.services.insert(
            "svc".to_string(),
            crate::config::Service {
                process: Some("sleep 300".to_string()),
                ..Default::default()
            },
        );
        let orchestrator = Orchestrator::new(config, temp_dir.path().to_path_buf())
            .await
            .unwrap();
        {
            let mut tracker = orchestrator.state_tracker.write().await;
            tracker.initialize().await.unwrap();
            // A concurrent winner's registration, mid-start. This
            // orchestrator never records ownership of it.
            tracker
                .register_service(ServiceState::new(
                    "svc".to_string(),
                    ServiceType::Process,
                    "root".to_string(),
                ))
                .await
                .unwrap();
        }

        orchestrator.cleanup_failed_start().await;

        let row = {
            let tracker = orchestrator.state_tracker.read().await;
            tracker.get_service("svc").await
        };
        assert!(
            matches!(row.map(|s| s.status), Some(Status::Starting)),
            "an unowned registration must survive this run's failure cleanup"
        );
    }

    /// An owned service whose manager holds no identity (nothing spawned)
    /// must leave its state row for the registration guard / other owners.
    #[tokio::test]
    async fn cleanup_failed_start_spares_identityless_rows() {
        use crate::service::ProcessService;

        let temp_dir = tempfile::tempdir().unwrap();
        let mut config = Config::default();
        config.services.insert(
            "svc".to_string(),
            crate::config::Service {
                process: Some("sleep 300".to_string()),
                ..Default::default()
            },
        );
        let orchestrator = Orchestrator::new(config.clone(), temp_dir.path().to_path_buf())
            .await
            .unwrap();
        {
            let mut tracker = orchestrator.state_tracker.write().await;
            tracker.initialize().await.unwrap();
            tracker
                .register_service(ServiceState::new(
                    "svc".to_string(),
                    ServiceType::Process,
                    "root".to_string(),
                ))
                .await
                .unwrap();
        }
        {
            let manager = ProcessService::new(
                "svc".into(),
                config.services.get("svc").unwrap().clone(),
                HashMap::new(),
                temp_dir.path().to_string_lossy().into_owned(),
                OutputMode::Captured,
                None,
            );
            orchestrator.services.write().await.insert(
                "svc".to_string(),
                Arc::new(tokio::sync::Mutex::new(Box::new(manager))),
            );
        }
        orchestrator.record_owned("svc");

        orchestrator.cleanup_failed_start().await;

        // The manager never spawned anything (no PID/container), so the row
        // is deliberately NOT deleted by name: in the real flow the
        // registration guard's drop already removed it, and after that the
        // name may belong to another process. Identity-matched removal is
        // covered by the state-layer unregister_service_matching tests.
        let row = {
            let tracker = orchestrator.state_tracker.read().await;
            tracker.get_service("svc").await
        };
        assert!(
            matches!(row.map(|s| s.status), Some(Status::Starting)),
            "an identity-less owned row must be left for the registration guard"
        );
    }

    /// A row stuck in `Starting` (winner hard-killed without cleanup) must
    /// hit the deadline with an actionable error instead of hanging.
    #[tokio::test]
    async fn concurrent_start_loser_times_out_on_stuck_starting_row() {
        let temp_dir = tempfile::tempdir().unwrap();
        let (orchestrator, manager) = orchestrator_with_starting_row(&temp_dir).await;

        let error = orchestrator
            .await_concurrent_start("svc", &manager, Duration::from_millis(600))
            .await
            .expect_err("a permanently-Starting row must not hang the loser");
        assert!(
            matches!(error, Error::ServiceStartFailed(ref name, ref reason)
                if name == "svc" && reason.contains("still marked Starting")),
            "expected the stuck-Starting error, got {:?}",
            error
        );
    }
}
