use crate::config::{Config, ServiceType};
use crate::dependency::{ExternalServiceExpander, Graph};
use crate::error::{Error, Result};
use crate::parameter::Resolver;
use crate::service::{OutputMode, ServiceManager, Status};
use crate::state::{ServiceState, StateTracker};
use tokio_util::sync::CancellationToken;
use tracing::Instrument;
// Using tokio::sync::RwLock for async-aware locking
// Previous implementation used parking_lot::RwLock which required HashMap removal pattern
// to avoid blocking tokio threads across .await points.
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use super::health::SharedHealthCheckerRegistry;

/// Default timeout for service startup operations (2 minutes)
pub const DEFAULT_STARTUP_TIMEOUT: Duration = Duration::from_secs(120);

/// Default timeout for service stop operations (30 seconds)
pub const DEFAULT_STOP_TIMEOUT: Duration = Duration::from_secs(30);

/// Type alias for the shared service registry
pub(super) type ServiceRegistry = HashMap<String, Arc<tokio::sync::Mutex<Box<dyn ServiceManager>>>>;
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
    /// When true, skip port cache and allocate fresh random ports
    randomize_ports: bool,
    /// When set, Docker containers use this ID instead of the work-dir hash.
    /// Used by isolated script execution to give child orchestrators their own
    /// container namespace, preventing collisions with parent containers.
    pub(super) isolation_id: Option<String>,
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
    /// use fed::{Config, Orchestrator};
    /// use fed::service::OutputMode;
    ///
    /// # async fn example() -> Result<(), fed::Error> {
    /// let config = Config::default();
    /// let orchestrator = Orchestrator::builder()
    ///     .config(config)
    ///     .output_mode(OutputMode::Captured)
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
            randomize_ports: false,
            isolation_id: None,
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
            randomize_ports: false,
            isolation_id: None,
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
            randomize_ports: false,
            isolation_id: None,
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

    /// Scope the vault query to the manual-secret names the target script
    /// transitively references. `None` fetches every missing manual secret (the
    /// safe default). Must be set before [`Orchestrator::initialize`], which
    /// runs secret resolution.
    pub fn set_required_secret_names(&mut self, names: Option<std::collections::HashSet<String>>) {
        self.resolver.set_required_names(names);
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
            if isolated {
                if let Some(id) = isolation_id {
                    tracing::debug!("Applying persisted isolation_id: {}", id);
                    self.isolation_id = Some(id);
                }
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
    ) -> Result<()> {
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
    pub async fn start(&self, service_name: &str) -> Result<()> {
        // Check for early cancellation
        if self.cancellation_token.is_cancelled() {
            return Err(Error::Cancelled(service_name.to_string()));
        }

        // Release port listeners just before starting services.
        // This minimizes the TOCTOU race window.
        self.release_port_listeners_once();

        // Get services to start in order
        let deps = self.dep_graph.get_dependencies(service_name);

        // Start dependencies first
        for dep in deps {
            // Check for cancellation before each dependency
            if self.cancellation_token.is_cancelled() {
                return Err(Error::Cancelled(dep.clone()));
            }
            self.start_service_with_timeout(&dep).await?;
        }

        // Start the requested service
        self.start_service_with_timeout(service_name).await
    }

    /// Start a service with timeout and cancellation support.
    ///
    /// A per-service `startup_timeout` (set in the service config) takes
    /// precedence over the orchestrator-wide default.
    async fn start_service_with_timeout(&self, name: &str) -> Result<()> {
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

        tokio::select! {
            biased;

            _ = cancel_token.cancelled() => {
                if !was_active_before {
                    self.stop_interrupted_start(name).await;
                }
                Err(Error::Cancelled(name.to_string()))
            }

            result = tokio::time::timeout(timeout, self.start_service(name)) => {
                match result {
                    Ok(inner_result) => inner_result,
                    Err(_elapsed) => {
                        if !was_active_before {
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
    async fn start_service(&self, name: &str) -> Result<()> {
        self.start_service_impl(name)
            .instrument(tracing::info_span!("start_service", service.name = %name))
            .await
    }

    /// Implementation of start_service (separate to allow instrumentation)
    async fn start_service_impl(&self, name: &str) -> Result<()> {
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

        // Oneshot (`run:`) services take a dedicated run-to-completion path:
        // execute once, gate dependents on completion, re-run every startup.
        if self
            .config
            .services
            .get(name)
            .map(|s| s.service_type() == ServiceType::Oneshot)
            .unwrap_or(false)
        {
            return self.run_oneshot(name, &manager_arc).await;
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
                    return Ok(());
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
        }

        let Some(registration) =
            super::registration::ServiceRegistration::register(&self.state_tracker, service_state)
                .await?
        else {
            return Ok(()); // already registered by another thread
        };

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
            tracker.update_service_status(name, Status::Running).await?;

            // Store PID if available (for process services)
            // Store container ID if available (for docker services)
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
        self.await_healthcheck(name, &manager_arc)
            .instrument(tracing::info_span!("await_healthcheck"))
            .await?;

        Ok(())
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
    pub async fn start_all(&self) -> Result<()> {
        // Check for early cancellation
        if self.cancellation_token.is_cancelled() {
            return Err(Error::Cancelled("start_all".to_string()));
        }

        // Release port listeners just before starting services.
        // This minimizes the TOCTOU race window.
        self.release_port_listeners_once();

        // Get parallel groups
        let groups = self.dep_graph.get_parallel_groups()?;

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

            // Collect any errors
            let errors: Vec<Error> = results.into_iter().filter_map(|r| r.err()).collect();

            // If any service failed, return aggregated errors
            if !errors.is_empty() {
                if errors.len() == 1 {
                    return Err(errors.into_iter().next().expect("errors not empty"));
                } else {
                    return Err(Error::Multiple(errors));
                }
            }
        }

        Ok(())
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
    pub async fn restart_all(&self) -> Result<()> {
        // First, stop all services in reverse dependency order
        self.stop_all().await?;

        // Check for cancellation before starting
        if self.cancellation_token.is_cancelled() {
            return Err(Error::Cancelled("restart_all".to_string()));
        }

        // Then, start all services in dependency order
        self.start_all().await?;

        Ok(())
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
    pub async fn restart(&self, service_name: &str) -> Result<()> {
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

        self.start(service_name).await?;

        // Dependents come back in shallowest-first order (reverse of the
        // stop iteration) so each one's own dependencies are already up.
        for dependent in was_running.iter().rev() {
            if self.cancellation_token.is_cancelled() {
                return Err(Error::Cancelled(dependent.clone()));
            }
            self.start_service_with_timeout(dependent).await?;
        }

        Ok(())
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
        let services = self.services.read().await;
        let mut result = HashMap::new();
        for (name, arc) in services.iter() {
            let manager = arc.lock().await;
            let status = manager.status();

            // For running services, trigger a health check to detect exits
            // The health check is cached (500ms TTL) so this is inexpensive
            if status == Status::Running || status == Status::Healthy {
                // Health check updates status internally if process has died
                let _ = manager.health().await;
            }

            // Get status again after potential health check update
            result.insert(name.clone(), manager.status());
        }
        result
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

        tracing::debug!("Cleanup: stopping all services");
        let _ = self.stop_all().await;
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
        tracing::debug!("Cleanup: complete");
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
            if let Some(svc) = self.config.services.get(name) {
                if let Some(ref image) = svc.image {
                    if !images.contains(image) {
                        images.push(image.clone());
                    }
                }
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
}
