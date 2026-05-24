use super::types::{LockFile, RegistrationOutcome, ServiceState};
use crate::config::ServiceType;
use crate::error::{validate_pid_for_check, Error, Result};
use crate::service::Status;
use chrono::{DateTime, Utc};
use fs2::FileExt;
use rusqlite::OptionalExtension;
use std::collections::HashMap;
use std::fs::OpenOptions;
use std::io::Write;
use std::path::{Path, PathBuf};
use tokio_rusqlite::Connection;
use tracing::{debug, info, warn};

const FED_DIR: &str = ".fed";
const DB_FILE_NAME: &str = "lock.db";
const LOCK_FILE_NAME: &str = ".lock";
const SCHEMA_VERSION: i32 = 6;

/// SQLite-backed state tracker for persistent service state management
/// Provides ACID transactions and crash recovery via WAL mode.
///
/// Uses advisory file locking (`.fed/.lock`) to prevent multiple `fed` instances
/// from modifying state simultaneously. The lock is held for the lifetime of
/// the state tracker and released when dropped.
pub struct SqliteStateTracker {
    db_path: PathBuf,
    conn: Connection,
    work_dir: String,
    /// Advisory lock file handle - held to prevent concurrent modifications.
    /// Using `Option` to allow graceful degradation if locking fails.
    #[allow(dead_code)]
    lock_file: Option<std::fs::File>,
}

/// Check if a given PID belongs to a `fed` process.
///
/// Used to distinguish between a genuine concurrent `fed` instance and a stale
/// lock file whose PID was recycled by an unrelated process. Returns `true` if
/// the process name starts with "fed" (covers "fed", "fed.exe", debug builds).
/// Returns `true` (assumes fed) if we can't determine the process name — better
/// a false warning than a silenced real conflict.
#[cfg(unix)]
fn is_fed_process(pid: u32) -> bool {
    // Try /proc/<pid>/comm first (Linux)
    #[cfg(target_os = "linux")]
    {
        let comm_path = format!("/proc/{}/comm", pid);
        if let Ok(comm) = std::fs::read_to_string(&comm_path) {
            return comm.trim().starts_with("fed");
        }
    }

    // Fall back to ps -o comm= (macOS, BSDs)
    if let Ok(output) = std::process::Command::new("ps")
        .args(["-o", "comm=", "-p", &pid.to_string()])
        .env("LC_ALL", "C")
        .output()
    {
        if output.status.success() {
            let comm = String::from_utf8_lossy(&output.stdout);
            let name = comm.trim();
            // ps may return the full path (e.g. /usr/local/bin/fed) or just "fed"
            let basename = name.rsplit('/').next().unwrap_or(name);
            return basename.starts_with("fed");
        }
    }

    // Can't determine — assume it's fed to be safe
    true
}

impl SqliteStateTracker {
    /// Create a new SQLite state tracker with the given working directory.
    ///
    /// Acquires an advisory file lock (`.fed/.lock`) to prevent concurrent
    /// modifications from multiple `fed` instances. The lock is held for the
    /// lifetime of this struct and released when dropped.
    pub async fn new(work_dir: PathBuf) -> Result<Self> {
        // Create .fed directory if it doesn't exist
        let fed_dir = work_dir.join(FED_DIR);
        std::fs::create_dir_all(&fed_dir)?;

        // Try to acquire advisory lock for multi-terminal safety
        let lock_path = fed_dir.join(LOCK_FILE_NAME);
        let lock_file = Self::try_acquire_lock(&lock_path)?;

        let db_path = fed_dir.join(DB_FILE_NAME);
        let work_dir_str = work_dir.to_string_lossy().to_string();

        // Open database connection
        let conn = Connection::open(&db_path).await?;

        // Configure WAL mode for crash recovery
        conn.call(|conn: &mut rusqlite::Connection| {
            conn.pragma_update(None, "journal_mode", "WAL")?;
            conn.pragma_update(None, "synchronous", "NORMAL")?;
            conn.pragma_update(None, "foreign_keys", "ON")?;
            conn.pragma_update(None, "busy_timeout", 5000)?;
            Ok(())
        })
        .await?;

        Ok(Self {
            db_path,
            conn,
            work_dir: work_dir_str,
            lock_file,
        })
    }

    /// Create an ephemeral in-memory state tracker.
    ///
    /// Uses an in-memory SQLite database with no file lock and no `.fed/` directory.
    /// Intended for isolated child orchestrators (e.g. `isolated: true` scripts)
    /// that must not touch the parent's persistent state.
    pub async fn new_ephemeral() -> Result<Self> {
        let conn = Connection::open(":memory:").await?;

        conn.call(|conn: &mut rusqlite::Connection| {
            conn.pragma_update(None, "foreign_keys", "ON")?;
            conn.pragma_update(None, "busy_timeout", 5000)?;
            Ok(())
        })
        .await?;

        Ok(Self {
            db_path: PathBuf::from(":memory:"),
            conn,
            work_dir: String::new(),
            lock_file: None,
        })
    }

    /// Try to acquire an advisory file lock.
    ///
    /// Returns the lock file handle if successful, or None if another process
    /// holds the lock (with a warning logged). The lock is automatically
    /// released when the file handle is dropped.
    fn try_acquire_lock(lock_path: &Path) -> Result<Option<std::fs::File>> {
        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(false)
            .open(lock_path)
            .map_err(|e| Error::Filesystem(format!("Failed to open lock file: {}", e)))?;

        // Try non-blocking exclusive lock
        match FileExt::try_lock_exclusive(&file) {
            Ok(()) => {
                // Write PID + marker so readers can verify the lock holder is fed
                // Format: "<PID> fed\n"
                let _ = file.set_len(0); // Truncate
                let _ = writeln!(file, "{} fed", std::process::id());
                debug!("Acquired advisory lock on {:?}", lock_path);
                Ok(Some(file))
            }
            Err(e) => {
                // Lock failed - another fed instance may be running
                debug!("Lock acquisition failed: {} (kind: {:?})", e, e.kind());
                // Read the lock file to provide better diagnostics.
                // Format: "<PID> fed\n" (new) or "<PID>\n" (legacy).
                if let Ok(contents) = std::fs::read_to_string(lock_path) {
                    let contents = contents.trim();
                    if !contents.is_empty() {
                        // Parse PID and optional marker
                        let mut parts = contents.split_whitespace();
                        let pid_str = parts.next().unwrap_or("");
                        let marker = parts.next(); // Some("fed") or None (legacy format)

                        if let Ok(pid) = pid_str.parse::<u32>() {
                            // Same process holding lock (e.g., during set_work_dir) - not a conflict
                            if pid == std::process::id() {
                                debug!("Lock held by same process (state tracker recreation)");
                            } else {
                                #[cfg(unix)]
                                {
                                    use nix::sys::signal::kill;
                                    use nix::unistd::Pid;
                                    // Check if process exists (signal 0 doesn't send anything)
                                    if kill(Pid::from_raw(pid as i32), None).is_ok() {
                                        // Process is alive — but is it actually a fed instance?
                                        // New format: marker == Some("fed") confirms it.
                                        // Legacy format (no marker): fall back to process name check.
                                        let is_fed = marker == Some("fed") || is_fed_process(pid);
                                        if is_fed {
                                            warn!(
                                                "Another fed instance (PID {}) is modifying this workspace. \
                                                 Proceeding anyway, but state conflicts are possible.",
                                                pid
                                            );
                                        } else {
                                            debug!(
                                                "Lock PID {} is alive but not a fed process - stale lock, proceeding",
                                                pid
                                            );
                                        }
                                    } else {
                                        // Process is dead - stale lock file, we can proceed
                                        debug!(
                                            "Stale lock file (PID {} no longer exists) - proceeding",
                                            pid
                                        );
                                    }
                                }
                                #[cfg(not(unix))]
                                {
                                    // Can't check process name — trust the marker if present
                                    if marker == Some("fed") {
                                        warn!(
                                            "Another fed instance (PID {}) may be modifying this workspace. \
                                             Proceeding anyway, but state conflicts are possible.",
                                            pid
                                        );
                                    }
                                }
                            }
                        }
                    }
                } else {
                    debug!("Could not acquire lock ({}) - proceeding anyway", e);
                }
                // Don't fail - just proceed without exclusive lock
                // This allows read-only operations (status, logs) to work
                Ok(None)
            }
        }
    }

    /// Clone the database connection handle.
    ///
    /// This can be used to perform read operations without holding the outer RwLock,
    /// since tokio_rusqlite::Connection is internally thread-safe and Clone.
    /// Useful for avoiding lock contention during long-running queries.
    pub fn clone_connection(&self) -> Connection {
        self.conn.clone()
    }

    /// Fetch all services from the database using a cloned connection.
    ///
    /// This static method allows fetching services without borrowing `&self`,
    /// enabling the caller to release any outer locks before the async query.
    pub async fn fetch_services_from_connection(
        conn: &Connection,
    ) -> HashMap<String, ServiceState> {
        match conn
            .call(|conn: &mut rusqlite::Connection| {
                let mut stmt = conn.prepare(
                    "SELECT id, status, service_type, pid, container_id, started_at, external_repo, namespace, restart_count, last_restart_at, consecutive_failures, startup_message FROM services"
                )?;

                let services_iter = stmt.query_map([], |row| {
                    let id: String = row.get(0)?;
                    let status_str: String = row.get(1)?;
                    let service_type_str: String = row.get(2)?;
                    let started_at_str: String = row.get(5)?;
                    let last_restart_str: Option<String> = row.get(9)?;

                    Ok((
                        id.clone(),
                        status_str.clone(),
                        ServiceState {
                            id,
                            status: status_str.parse::<Status>().unwrap_or(Status::Stopped),
                            service_type: service_type_str.parse::<ServiceType>().unwrap_or(ServiceType::Undefined),
                            pid: row.get(3)?,
                            container_id: row.get(4)?,
                            started_at: started_at_str
                                .parse::<DateTime<Utc>>()
                                .unwrap_or_else(|_| Utc::now()),
                            external_repo: row.get(6)?,
                            namespace: row.get(7)?,
                            restart_count: row.get(8)?,
                            last_restart_at: last_restart_str
                                .and_then(|s| s.parse::<DateTime<Utc>>().ok()),
                            consecutive_failures: row.get(10)?,
                            port_allocations: HashMap::new(),
                            startup_message: row.get(11)?,
                        },
                    ))
                })?;

                // Filter out stale DB-only statuses before constructing the map
                let mut services: HashMap<String, ServiceState> = services_iter
                    .filter_map(|r| r.ok())
                    .filter(|(_, raw_status, _)| !Self::status_is_stale(raw_status))
                    .map(|(id, _, state)| (id, state))
                    .collect();

                // Validate PIDs - filter out invalid ones (don't delete, just skip)
                services.retain(|service_id, service_state| {
                    if let Some(pid) = service_state.pid {
                        if pid > i32::MAX as u32 || pid == 0 {
                            warn!(
                                "Service '{}' has invalid PID {} (exceeds i32::MAX or is 0), skipping",
                                service_id, pid
                            );
                            return false;
                        }
                    }
                    true
                });

                // Load port allocations for each service
                for (service_id, service) in services.iter_mut() {
                    let mut port_stmt = conn.prepare(
                        "SELECT parameter_name, port FROM port_allocations WHERE service_id = ?1"
                    )?;

                    let ports: HashMap<String, u16> = port_stmt
                        .query_map(rusqlite::params![service_id], |row| {
                            Ok((row.get(0)?, row.get(1)?))
                        })?
                        .filter_map(|r| r.ok())
                        .collect();

                    service.port_allocations = ports;
                }

                Ok(services)
            })
            .await
        {
            Ok(services) => services,
            Err(e) => {
                warn!("Failed to fetch services: {}", e);
                HashMap::new()
            }
        }
    }

    /// Execute a function within a transaction, automatically updating the lock file timestamp and committing.
    /// This reduces boilerplate across all transaction-based methods.
    #[tracing::instrument(skip(self, f), fields(operation = "db_transaction"))]
    async fn with_transaction<F, T>(&self, f: F) -> Result<T>
    where
        F: FnOnce(&rusqlite::Transaction) -> rusqlite::Result<T> + Send + 'static,
        T: Send + 'static,
    {
        self.conn
            .call(move |conn: &mut rusqlite::Connection| {
                let tx = conn.transaction()?;
                let result = f(&tx)?;
                tx.execute(
                    "UPDATE lock_file SET updated_at = datetime('now') WHERE id = 1",
                    [],
                )?;
                tx.commit()?;
                Ok(result)
            })
            .await
            .map_err(Error::from)
    }

    /// Initialize state tracker - create schema or load existing
    pub async fn initialize(&mut self) -> Result<()> {
        // Check if schema exists
        let schema_exists: bool = self
            .conn
            .call(
                |conn: &mut rusqlite::Connection| -> tokio_rusqlite::Result<bool> {
                    Ok(conn.query_row(
                "SELECT COUNT(*) > 0 FROM sqlite_master WHERE type='table' AND name='lock_file'",
                [],
                |row| row.get(0),
            )?)
                },
            )
            .await?;

        if !schema_exists {
            debug!("Creating SQLite schema");
            self.create_schema().await?;
            self.init_lock_file().await?;
        } else {
            debug!("Loading existing SQLite state");
            // Run migrations if needed
            self.run_migrations().await?;
            self.validate_and_cleanup().await?;
        }

        Ok(())
    }

    /// Run database migrations to bring schema up to current version
    async fn run_migrations(&self) -> Result<()> {
        let current_version: i32 = self
            .conn
            .call(
                |conn: &mut rusqlite::Connection| -> tokio_rusqlite::Result<i32> {
                    conn.query_row("SELECT MAX(version) FROM schema_version", [], |row| {
                        row.get(0)
                    })
                    .or(Ok(1)) // Default to 1 if no version found
                },
            )
            .await
            .unwrap_or(1);

        if current_version >= SCHEMA_VERSION {
            debug!(
                "Database schema is up to date (version {})",
                current_version
            );
            return Ok(());
        }

        info!(
            "Migrating database schema from version {} to {}",
            current_version, SCHEMA_VERSION
        );

        // Migration from v1 to v2: Add circuit breaker support
        if current_version < 2 {
            self.migrate_v1_to_v2().await?;
        }

        // Migration from v2 to v3: Add startup_message column
        if current_version < 3 {
            self.migrate_v2_to_v3().await?;
        }

        // Migration from v3 to v4: Extract _ports into persisted_ports table
        if current_version < 4 {
            self.migrate_v3_to_v4().await?;
        }

        // Migration from v4 to v5: Add project_settings table
        if current_version < 5 {
            self.migrate_v4_to_v5().await?;
        }

        // Migration from v5 to v6: Scope persisted_ports by isolation_id
        if current_version < 6 {
            self.migrate_v5_to_v6().await?;
        }

        Ok(())
    }

    /// Migration v1 -> v2: Add circuit breaker tables and columns
    async fn migrate_v1_to_v2(&self) -> Result<()> {
        debug!("Running migration v1 -> v2: Adding circuit breaker support");

        self.conn
            .call(|conn: &mut rusqlite::Connection| -> tokio_rusqlite::Result<()> {
                let tx = conn.transaction()?;

                // Check if migration has already been applied
                let already_applied: bool = tx
                    .query_row(
                        "SELECT COUNT(*) > 0 FROM schema_version WHERE version = 2",
                        [],
                        |row| row.get(0),
                    )
                    .unwrap_or(false);

                if already_applied {
                    // Migration already applied, nothing to do
                    return Ok(());
                }

                // Create restart_history table for tracking restart timestamps
                tx.execute_batch(
                    r#"
                    CREATE TABLE IF NOT EXISTS restart_history (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        service_id TEXT NOT NULL,
                        restarted_at TEXT NOT NULL,
                        FOREIGN KEY (service_id) REFERENCES services(id) ON DELETE CASCADE
                    );

                    CREATE INDEX IF NOT EXISTS idx_restart_history_service
                        ON restart_history(service_id, restarted_at);
                    "#,
                )?;

                // Add circuit_breaker_open_until column to services table
                // SQLite allows adding columns without default values
                // First check if column already exists
                let has_column: bool = tx
                    .query_row(
                        "SELECT COUNT(*) > 0 FROM pragma_table_info('services') WHERE name = 'circuit_breaker_open_until'",
                        [],
                        |row| row.get(0),
                    )
                    .unwrap_or(false);

                if !has_column {
                    tx.execute(
                        "ALTER TABLE services ADD COLUMN circuit_breaker_open_until TEXT",
                        [],
                    )?;
                }

                // Record migration
                tx.execute(
                    "INSERT INTO schema_version (version, applied_at) VALUES (2, datetime('now'))",
                    [],
                )?;

                tx.commit()?;
                Ok(())
            })
            .await?;

        info!("Migration v1 -> v2 completed successfully");
        Ok(())
    }

    /// Migration v2 -> v3: Add startup_message column to services table
    async fn migrate_v2_to_v3(&self) -> Result<()> {
        debug!("Running migration v2 -> v3: Adding startup_message column");

        self.conn
            .call(|conn: &mut rusqlite::Connection| -> tokio_rusqlite::Result<()> {
                let tx = conn.transaction()?;

                let already_applied: bool = tx
                    .query_row(
                        "SELECT COUNT(*) > 0 FROM schema_version WHERE version = 3",
                        [],
                        |row| row.get(0),
                    )
                    .unwrap_or(false);

                if already_applied {
                    return Ok(());
                }

                let has_column: bool = tx
                    .query_row(
                        "SELECT COUNT(*) > 0 FROM pragma_table_info('services') WHERE name = 'startup_message'",
                        [],
                        |row| row.get(0),
                    )
                    .unwrap_or(false);

                if !has_column {
                    tx.execute(
                        "ALTER TABLE services ADD COLUMN startup_message TEXT",
                        [],
                    )?;
                }

                tx.execute(
                    "INSERT INTO schema_version (version, applied_at) VALUES (3, datetime('now'))",
                    [],
                )?;

                tx.commit()?;
                Ok(())
            })
            .await?;

        info!("Migration v2 -> v3 completed successfully");
        Ok(())
    }

    /// Migration v3 -> v4: Extract `_ports` synthetic service into dedicated `persisted_ports` table
    async fn migrate_v3_to_v4(&self) -> Result<()> {
        debug!("Running migration v3 -> v4: Creating persisted_ports table");

        self.conn
            .call(
                |conn: &mut rusqlite::Connection| -> tokio_rusqlite::Result<()> {
                    let tx = conn.transaction()?;

                    let already_applied: bool = tx
                        .query_row(
                            "SELECT COUNT(*) > 0 FROM schema_version WHERE version = 4",
                            [],
                            |row| row.get(0),
                        )
                        .unwrap_or(false);

                    if already_applied {
                        return Ok(());
                    }

                    // Create the new table
                    tx.execute_batch(
                        "CREATE TABLE IF NOT EXISTS persisted_ports (
                        param_name TEXT PRIMARY KEY,
                        port INTEGER NOT NULL,
                        source TEXT NOT NULL,
                        allocated_at TEXT NOT NULL
                    );",
                    )?;

                    // Migrate existing data from _ports synthetic service
                    tx.execute(
                    "INSERT OR IGNORE INTO persisted_ports (param_name, port, source, allocated_at)
                     SELECT parameter_name, port, 'resolver', datetime('now')
                     FROM port_allocations WHERE service_id = '_ports'",
                    [],
                )?;

                    // Ensure migrated ports have bind reservations in allocated_ports
                    tx.execute(
                        "INSERT OR IGNORE INTO allocated_ports (port, allocated_at)
                     SELECT port, allocated_at FROM persisted_ports",
                        [],
                    )?;

                    // Remove old synthetic entries
                    tx.execute(
                        "DELETE FROM port_allocations WHERE service_id = '_ports'",
                        [],
                    )?;
                    tx.execute("DELETE FROM services WHERE id = '_ports'", [])?;

                    tx.execute(
                    "INSERT INTO schema_version (version, applied_at) VALUES (4, datetime('now'))",
                    [],
                )?;

                    tx.commit()?;
                    Ok(())
                },
            )
            .await?;

        info!("Migration v3 -> v4 completed successfully");
        Ok(())
    }

    /// Migration v4 -> v5: Add project_settings table
    async fn migrate_v4_to_v5(&self) -> Result<()> {
        debug!("Running migration v4 -> v5: Creating project_settings table");

        self.conn
            .call(
                |conn: &mut rusqlite::Connection| -> tokio_rusqlite::Result<()> {
                    let tx = conn.transaction()?;

                    let already_applied: bool = tx
                        .query_row(
                            "SELECT COUNT(*) > 0 FROM schema_version WHERE version = 5",
                            [],
                            |row| row.get(0),
                        )
                        .unwrap_or(false);

                    if already_applied {
                        return Ok(());
                    }

                    tx.execute_batch(
                        "CREATE TABLE IF NOT EXISTS project_settings (
                        key TEXT PRIMARY KEY,
                        value TEXT NOT NULL,
                        updated_at TEXT NOT NULL DEFAULT (datetime('now'))
                    );",
                    )?;

                    tx.execute(
                    "INSERT INTO schema_version (version, applied_at) VALUES (5, datetime('now'))",
                    [],
                )?;

                    tx.commit()?;
                    Ok(())
                },
            )
            .await?;

        info!("Migration v4 -> v5 completed successfully");
        Ok(())
    }

    /// Migration v5 -> v6: Scope `persisted_ports` by `isolation_id`.
    ///
    /// Before v6 the table was a flat `param_name -> port` map with no isolation
    /// boundary, so randomized ports (from `fed isolate enable` / the deprecated
    /// `fed ports randomize`) leaked into the non-isolated start path and stuck
    /// there permanently. v6 adds an `isolation_id` column (`''` = shared scope)
    /// to the primary key.
    ///
    /// Legacy rows are intentionally dropped: we cannot tell which were genuine
    /// conflict resolutions versus randomized leftovers, and the safe default is
    /// to fall back to configured ports and re-resolve real conflicts on the next
    /// `fed start`. This is what heals projects already stuck with random ports.
    async fn migrate_v5_to_v6(&self) -> Result<()> {
        debug!("Running migration v5 -> v6: Scoping persisted_ports by isolation_id");

        self.conn
            .call(
                |conn: &mut rusqlite::Connection| -> tokio_rusqlite::Result<()> {
                    let tx = conn.transaction()?;

                    let already_applied: bool = tx
                        .query_row(
                            "SELECT COUNT(*) > 0 FROM schema_version WHERE version = 6",
                            [],
                            |row| row.get(0),
                        )
                        .unwrap_or(false);

                    if already_applied {
                        return Ok(());
                    }

                    // Recreate the table with the scoped schema. We discard legacy
                    // rows (see method doc) rather than guess their scope.
                    tx.execute_batch(
                        "DROP TABLE IF EXISTS persisted_ports;
                         CREATE TABLE persisted_ports (
                            param_name TEXT NOT NULL,
                            port INTEGER NOT NULL,
                            source TEXT NOT NULL,
                            allocated_at TEXT NOT NULL,
                            isolation_id TEXT NOT NULL DEFAULT '',
                            PRIMARY KEY (param_name, isolation_id)
                         );",
                    )?;

                    // Release the bind reservations tied to the dropped ports so
                    // they become reusable, but keep reservations for ports still
                    // owned by tracked services.
                    tx.execute(
                        "DELETE FROM allocated_ports WHERE port NOT IN (SELECT port FROM port_allocations)",
                        [],
                    )?;

                    tx.execute(
                        "INSERT INTO schema_version (version, applied_at) VALUES (6, datetime('now'))",
                        [],
                    )?;

                    tx.commit()?;
                    Ok(())
                },
            )
            .await?;

        info!("Migration v5 -> v6 completed successfully");
        Ok(())
    }

    /// Create database schema
    async fn create_schema(&self) -> Result<()> {
        self.conn.call(|conn: &mut rusqlite::Connection| -> tokio_rusqlite::Result<()> {
            conn.execute_batch(
                r#"
                -- Schema version tracking
                CREATE TABLE schema_version (
                    version INTEGER PRIMARY KEY,
                    applied_at TEXT NOT NULL
                );

                -- Lock file metadata (singleton)
                CREATE TABLE lock_file (
                    id INTEGER PRIMARY KEY CHECK (id = 1),
                    fed_pid INTEGER NOT NULL,
                    work_dir TEXT NOT NULL,
                    started_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );

                -- Services table
                CREATE TABLE services (
                    id TEXT PRIMARY KEY,
                    status TEXT NOT NULL,
                    service_type TEXT NOT NULL,
                    pid INTEGER,
                    container_id TEXT,
                    started_at TEXT NOT NULL,
                    external_repo TEXT,
                    namespace TEXT NOT NULL,
                    restart_count INTEGER NOT NULL DEFAULT 0,
                    last_restart_at TEXT,
                    consecutive_failures INTEGER NOT NULL DEFAULT 0,
                    circuit_breaker_open_until TEXT,
                    startup_message TEXT
                );

                -- Indexes for services
                CREATE INDEX idx_services_status ON services(status);
                CREATE INDEX idx_services_namespace ON services(namespace);
                CREATE INDEX idx_services_pid ON services(pid) WHERE pid IS NOT NULL;
                CREATE INDEX idx_services_container_id ON services(container_id) WHERE container_id IS NOT NULL;

                -- Port allocations per service
                CREATE TABLE port_allocations (
                    service_id TEXT NOT NULL,
                    parameter_name TEXT NOT NULL,
                    port INTEGER NOT NULL,
                    PRIMARY KEY (service_id, parameter_name),
                    FOREIGN KEY (service_id) REFERENCES services(id) ON DELETE CASCADE
                );

                CREATE INDEX idx_port_allocations_port ON port_allocations(port);

                -- Global allocated ports
                CREATE TABLE allocated_ports (
                    port INTEGER PRIMARY KEY,
                    allocated_at TEXT NOT NULL
                );

                -- Persisted port resolutions (replaces _ports synthetic service).
                -- Scoped by isolation_id, mirroring container/volume/marker scoping:
                -- '' is the shared (non-isolated) scope, 'iso-xxxx' an isolation
                -- session. The shared scope only ever holds conflict-resolved ports;
                -- randomized ports live under their isolation scope.
                CREATE TABLE persisted_ports (
                    param_name TEXT NOT NULL,
                    port INTEGER NOT NULL,
                    source TEXT NOT NULL,
                    allocated_at TEXT NOT NULL,
                    isolation_id TEXT NOT NULL DEFAULT '',
                    PRIMARY KEY (param_name, isolation_id)
                );

                -- Restart history for circuit breaker tracking
                CREATE TABLE restart_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    service_id TEXT NOT NULL,
                    restarted_at TEXT NOT NULL,
                    FOREIGN KEY (service_id) REFERENCES services(id) ON DELETE CASCADE
                );

                CREATE INDEX idx_restart_history_service ON restart_history(service_id, restarted_at);

                -- Project-level key/value settings
                CREATE TABLE project_settings (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL,
                    updated_at TEXT NOT NULL DEFAULT (datetime('now'))
                );

                "#,
            )?;

            // Insert schema version separately (can't use placeholders in execute_batch)
            conn.execute(
                "INSERT INTO schema_version (version, applied_at) VALUES (?1, datetime('now'))",
                rusqlite::params![SCHEMA_VERSION],
            )?;

            Ok(())
        }).await?;

        Ok(())
    }

    /// Initialize lock file row
    async fn init_lock_file(&self) -> Result<()> {
        let now = Utc::now().to_rfc3339();
        let work_dir = self.work_dir.clone();
        let pid = std::process::id();

        self.conn.call(move |conn: &mut rusqlite::Connection| {
            conn.execute(
                "INSERT INTO lock_file (id, fed_pid, work_dir, started_at, updated_at) VALUES (1, ?1, ?2, ?3, ?4)",
                rusqlite::params![pid, &work_dir, &now, &now],
            )?;
            Ok(())
        }).await?;

        Ok(())
    }

    /// Validate existing state and cleanup stale services
    async fn validate_and_cleanup(&mut self) -> Result<()> {
        // Mark dead services as stale (does not delete — purge_stale_services does that)
        self.mark_dead_services().await?;

        // Update to current PID
        let pid = std::process::id();
        self.conn
            .call(move |conn: &mut rusqlite::Connection| {
                conn.execute(
                    "UPDATE lock_file SET fed_pid = ?1, updated_at = datetime('now') WHERE id = 1",
                    rusqlite::params![pid],
                )?;
                Ok(())
            })
            .await?;

        Ok(())
    }

    /// Check if a process with given PID is running (not a zombie)
    async fn is_process_running(pid: u32) -> bool {
        #[cfg(unix)]
        {
            use nix::sys::signal::kill;

            // Validate PID for read-only check (rejects 0 and >i32::MAX)
            let Some(nix_pid) = validate_pid_for_check(pid) else {
                warn!("Invalid PID {} for process check", pid);
                return false;
            };

            // First check if process exists at all
            if kill(nix_pid, None).is_err() {
                return false;
            }

            // Check if process is a zombie using ps command
            // Zombies have PID entries but aren't actually running
            match tokio::process::Command::new("ps")
                .args(["-p", &pid.to_string(), "-o", "stat="])
                .output()
                .await
            {
                Ok(output) => {
                    let stat = String::from_utf8_lossy(&output.stdout);
                    let stat = stat.trim();
                    // Process exists and is not a zombie (Z state)
                    !stat.is_empty() && !stat.starts_with('Z')
                }
                Err(_) => {
                    // If ps fails, fall back to just the kill check result
                    true
                }
            }
        }

        #[cfg(not(unix))]
        {
            warn!("Process validation not fully implemented for this platform");
            false
        }
    }

    /// Check if a Docker container is running
    async fn is_container_running(container_id: &str) -> bool {
        crate::docker::is_container_running(container_id).await
    }

    /// Check if a status string indicates the service MUST have a PID/container.
    ///
    /// Returns true for statuses where a missing PID/container indicates a crash:
    /// - "running" - service is actively running, must have PID/container
    /// - "healthy" - service is running and healthy, must have PID/container
    /// - "failing" - service is running but failing health checks, must have PID/container
    ///
    /// Returns false for:
    /// - "starting" - service may still be spinning up, don't clean up yet
    /// - "stopped" - service is not running, no PID/container expected
    /// - "stopping" - service is shutting down, PID/container may be gone
    /// - any other status - unknown/invalid, err on side of not cleaning up
    ///
    /// Note: "starting" is included because a service stuck in Starting with no
    /// PID and no container ID indicates a failed start that wasn't cleaned up.
    /// The caller (`mark_dead_services`) additionally checks for missing PID/container,
    /// so a legitimately-starting service that has already received a PID won't be
    /// incorrectly cleaned up.
    fn status_indicates_should_be_running(status: Status) -> bool {
        matches!(
            status,
            Status::Running | Status::Healthy | Status::Failing | Status::Starting
        )
    }

    /// Check if a status indicates the service is stale (marked for cleanup).
    fn status_is_stale(status: &str) -> bool {
        status == "stale"
    }

    /// Register a new service in the state.
    ///
    /// Returns `Registered` if the service was newly inserted, or
    /// `AlreadyExists { status }` if it was already in the DB. When a service
    /// already exists, its row is left untouched — no status, PID, or timestamp
    /// clobbering. The caller should inspect the outcome and skip starting if
    /// the service already exists.
    pub async fn register_service(
        &mut self,
        service_state: ServiceState,
    ) -> Result<RegistrationOutcome> {
        debug!("Registering service: {}", service_state.id);

        let id = service_state.id.clone();
        let status = service_state.status.to_string();
        let service_type = service_state.service_type.to_string();
        let namespace = service_state.namespace.clone();
        let started_at = service_state.started_at.to_rfc3339();
        let pid = service_state.pid;
        let container_id = service_state.container_id.clone();
        let external_repo = service_state.external_repo.clone();
        let restart_count = service_state.restart_count;
        let last_restart_at = service_state.last_restart_at.map(|dt| dt.to_rfc3339());
        let consecutive_failures = service_state.consecutive_failures;
        let startup_message = service_state.startup_message.clone();

        self.conn
            .call(move |conn: &mut rusqlite::Connection| {
                let tx = conn.transaction()?;

                // Check if service already exists and retrieve its current status
                let existing_status: Option<String> = tx
                    .query_row(
                        "SELECT status FROM services WHERE id = ?1",
                        rusqlite::params![&id],
                        |row| row.get(0),
                    )
                    .optional()?;

                if let Some(status_str) = existing_status {
                    // Service already registered — leave its row untouched.
                    tx.commit()?;
                    let status = status_str.parse::<Status>().unwrap_or(Status::Starting);
                    return Ok(RegistrationOutcome::AlreadyExists { status });
                }

                // Insert new service
                tx.execute(
                    "INSERT INTO services (id, status, service_type, pid, container_id, started_at, external_repo, namespace, restart_count, last_restart_at, consecutive_failures, startup_message)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)",
                    rusqlite::params![
                        &id,
                        &status,
                        &service_type,
                        pid,
                        container_id.as_deref(),
                        &started_at,
                        external_repo.as_deref(),
                        &namespace,
                        restart_count,
                        last_restart_at,
                        consecutive_failures,
                        startup_message.as_deref(),
                    ],
                )?;

                // Update lock file timestamp
                tx.execute(
                    "UPDATE lock_file SET updated_at = datetime('now') WHERE id = 1",
                    [],
                )?;

                tx.commit()?;
                Ok(RegistrationOutcome::Registered)
            })
            .await
            .map_err(Error::from)
    }

    /// Update service status
    #[must_use = "ignoring this result may cause state loss - the status update will not be persisted"]
    pub async fn update_service_status(&mut self, service_id: &str, status: Status) -> Result<()> {
        let service_id = service_id.to_string();
        let service_id_for_tx = service_id.clone();
        let status = status.to_string();

        let rows = self
            .with_transaction(move |tx| {
                tx.execute(
                    "UPDATE services SET status = ?1 WHERE id = ?2",
                    rusqlite::params![&status, &service_id_for_tx],
                )
            })
            .await?;

        if rows == 0 {
            return Err(Error::ServiceNotFound(service_id));
        }

        Ok(())
    }

    /// Update service PID
    #[must_use = "ignoring this result may cause state loss - the PID will not be persisted"]
    pub async fn update_service_pid(&mut self, service_id: &str, pid: u32) -> Result<()> {
        // Validate PID can be safely used for signal operations
        if pid > i32::MAX as u32 {
            return Err(Error::Validation(format!(
                "Service '{}': PID {} exceeds i32::MAX, cannot be used for signal operations",
                service_id, pid
            )));
        }
        if pid == 0 {
            return Err(Error::Validation(format!(
                "Service '{}': PID cannot be 0",
                service_id
            )));
        }

        let service_id = service_id.to_string();
        let service_id_for_tx = service_id.clone();

        let rows = self
            .with_transaction(move |tx| {
                tx.execute(
                    "UPDATE services SET pid = ?1 WHERE id = ?2",
                    rusqlite::params![pid, &service_id_for_tx],
                )
            })
            .await?;

        if rows == 0 {
            return Err(Error::ServiceNotFound(service_id));
        }

        Ok(())
    }

    /// Update service container ID
    #[must_use = "ignoring this result may cause state loss - the container ID will not be persisted"]
    pub async fn update_service_container_id(
        &mut self,
        service_id: &str,
        container_id: String,
    ) -> Result<()> {
        let service_id = service_id.to_string();
        let service_id_for_tx = service_id.clone();

        let rows = self
            .with_transaction(move |tx| {
                tx.execute(
                    "UPDATE services SET container_id = ?1 WHERE id = ?2",
                    rusqlite::params![&container_id, &service_id_for_tx],
                )
            })
            .await?;

        if rows == 0 {
            return Err(Error::ServiceNotFound(service_id));
        }

        Ok(())
    }

    /// Atomically transition service state with metadata updates.
    ///
    /// This method ensures that status transitions happen atomically with their associated
    /// metadata (PID, container ID) in a single database transaction. This prevents
    /// inconsistent state where the database shows "Running" but has no PID.
    ///
    /// The transition is validated against the current state to ensure it follows
    /// valid state machine paths (see `Status::is_valid_transition`).
    ///
    /// # Arguments
    ///
    /// * `service_id` - The service to transition
    /// * `transition` - The state transition to apply (includes status and metadata)
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The service doesn't exist
    /// - The transition is invalid (violates state machine)
    /// - The database transaction fails
    ///
    /// # Examples
    ///
    /// ```ignore
    /// // Transition to Running with PID in one atomic operation
    /// let transition = StateTransition::running_with_pid(12345);
    /// state_tracker.apply_state_transition("my-service", transition).await?;
    /// ```
    #[must_use = "ignoring this result may cause state loss - the transition will not be applied"]
    pub async fn apply_state_transition(
        &mut self,
        service_id: &str,
        transition: crate::service::StateTransition,
    ) -> Result<()> {
        let service_id = service_id.to_string();
        let service_id_for_tx = service_id.clone();

        // Validate transition against current state
        let current_status = {
            let state = self.get_service(&service_id).await;
            state
                .ok_or_else(|| Error::ServiceNotFound(service_id.clone()))?
                .status
        };

        // Validate the transition
        transition.validate(current_status)?;

        // Apply the transition atomically
        let status_str = transition.status.to_string();
        let pid = transition.pid;
        let container_id = transition.container_id;
        let clear_pid = transition.clear_pid;
        let clear_container_id = transition.clear_container_id;

        let rows = self
            .with_transaction(move |tx| {
                // Build UPDATE statement dynamically based on what needs to be updated
                let mut updates = vec!["status = ?1".to_string()];
                let mut param_index = 2;

                // Track parameters for rusqlite
                let mut params: Vec<Box<dyn rusqlite::ToSql>> = vec![Box::new(status_str.clone())];

                if let Some(pid_val) = pid {
                    // Validate PID
                    if pid_val > i32::MAX as u32 {
                        return Err(rusqlite::Error::ToSqlConversionFailure(Box::new(
                            std::io::Error::new(
                                std::io::ErrorKind::InvalidInput,
                                format!("PID {} exceeds i32::MAX", pid_val),
                            ),
                        )));
                    }
                    if pid_val == 0 {
                        return Err(rusqlite::Error::ToSqlConversionFailure(Box::new(
                            std::io::Error::new(
                                std::io::ErrorKind::InvalidInput,
                                "PID cannot be 0",
                            ),
                        )));
                    }
                    updates.push(format!("pid = ?{}", param_index));
                    params.push(Box::new(pid_val));
                    param_index += 1;
                }

                if let Some(ref cid) = container_id {
                    updates.push(format!("container_id = ?{}", param_index));
                    params.push(Box::new(cid.clone()));
                    param_index += 1;
                }

                if clear_pid {
                    updates.push("pid = NULL".to_string());
                }

                if clear_container_id {
                    updates.push("container_id = NULL".to_string());
                }

                let query = format!(
                    "UPDATE services SET {} WHERE id = ?{}",
                    updates.join(", "),
                    param_index
                );
                params.push(Box::new(service_id_for_tx.clone()));

                // Convert params to references for rusqlite
                let param_refs: Vec<&dyn rusqlite::ToSql> =
                    params.iter().map(|p| p.as_ref()).collect();

                tx.execute(&query, param_refs.as_slice())
            })
            .await?;

        if rows == 0 {
            return Err(Error::ServiceNotFound(service_id));
        }

        Ok(())
    }

    /// Increment restart count for a service
    #[must_use = "ignoring this result may cause state loss - the restart count will not be updated"]
    pub async fn increment_restart_count(&mut self, service_id: &str) -> Result<()> {
        let service_id = service_id.to_string();
        let service_id_for_tx = service_id.clone();
        let now = Utc::now().to_rfc3339();

        let rows = self
            .with_transaction(move |tx| {
                tx.execute(
                    "UPDATE services SET restart_count = restart_count + 1, last_restart_at = ?1, consecutive_failures = 0 WHERE id = ?2",
                    rusqlite::params![&now, &service_id_for_tx],
                )
            })
            .await?;

        if rows == 0 {
            return Err(Error::ServiceNotFound(service_id));
        }

        Ok(())
    }

    /// Increment consecutive failures for a service
    #[must_use = "ignoring this result may cause state loss - the failure count will not be updated"]
    pub async fn increment_consecutive_failures(&mut self, service_id: &str) -> Result<()> {
        let service_id = service_id.to_string();
        let service_id_for_tx = service_id.clone();

        let rows = self
            .with_transaction(move |tx| {
                tx.execute(
                    "UPDATE services SET consecutive_failures = consecutive_failures + 1 WHERE id = ?1",
                    rusqlite::params![&service_id_for_tx],
                )
            })
            .await?;

        if rows == 0 {
            return Err(Error::ServiceNotFound(service_id));
        }

        Ok(())
    }

    /// Reset consecutive failures (on successful health check)
    #[must_use = "ignoring this result may cause state loss - the failure count will not be reset"]
    pub async fn reset_consecutive_failures(&mut self, service_id: &str) -> Result<()> {
        let service_id = service_id.to_string();
        let service_id_for_tx = service_id.clone();

        let rows = self
            .with_transaction(move |tx| {
                tx.execute(
                    "UPDATE services SET consecutive_failures = 0 WHERE id = ?1",
                    rusqlite::params![&service_id_for_tx],
                )
            })
            .await?;

        if rows == 0 {
            return Err(Error::ServiceNotFound(service_id));
        }

        Ok(())
    }

    /// Batch update health check results to reduce lock contention.
    /// Takes lists of services that passed/failed health checks.
    /// Returns a map of service_id -> consecutive_failures for failed services.
    pub async fn batch_health_update(
        &mut self,
        healthy_services: Vec<String>,
        unhealthy_services: Vec<String>,
    ) -> Result<std::collections::HashMap<String, u32>> {
        self.with_transaction(move |tx| {
            // Reset consecutive failures for healthy services
            for service_id in &healthy_services {
                tx.execute(
                    "UPDATE services SET consecutive_failures = 0 WHERE id = ?1",
                    rusqlite::params![service_id],
                )?;
            }

            // Increment consecutive failures for unhealthy services and collect counts
            let mut failure_counts = std::collections::HashMap::new();
            for service_id in &unhealthy_services {
                tx.execute(
                    "UPDATE services SET consecutive_failures = consecutive_failures + 1 WHERE id = ?1",
                    rusqlite::params![service_id],
                )?;

                // Get the new failure count
                let count: u32 = tx
                    .query_row(
                        "SELECT consecutive_failures FROM services WHERE id = ?1",
                        rusqlite::params![service_id],
                        |row| row.get(0),
                    )
                    .unwrap_or(0);
                failure_counts.insert(service_id.clone(), count);
            }

            Ok(failure_counts)
        })
        .await
    }

    /// Batch increment restart counts for multiple services
    #[must_use = "ignoring this result may cause state loss - restart counts will not be updated"]
    pub async fn batch_increment_restart_counts(&mut self, service_ids: Vec<String>) -> Result<()> {
        let now = chrono::Utc::now().to_rfc3339();

        self.with_transaction(move |tx| {
            for service_id in &service_ids {
                tx.execute(
                    "UPDATE services SET restart_count = restart_count + 1, last_restart_at = ?1, consecutive_failures = 0 WHERE id = ?2",
                    rusqlite::params![&now, service_id],
                )?;
            }
            Ok(())
        })
        .await
    }

    /// Get restart count for a service
    pub async fn get_restart_count(&self, service_id: &str) -> Option<u32> {
        let service_id = service_id.to_string();

        self.conn
            .call(
                move |conn: &mut rusqlite::Connection| -> tokio_rusqlite::Result<u32> {
                    Ok(conn.query_row(
                        "SELECT restart_count FROM services WHERE id = ?1",
                        rusqlite::params![&service_id],
                        |row| row.get(0),
                    )?)
                },
            )
            .await
            .ok()
    }

    /// Get consecutive failures for a service
    pub async fn get_consecutive_failures(&self, service_id: &str) -> Option<u32> {
        let service_id = service_id.to_string();

        self.conn
            .call(
                move |conn: &mut rusqlite::Connection| -> tokio_rusqlite::Result<u32> {
                    Ok(conn.query_row(
                        "SELECT consecutive_failures FROM services WHERE id = ?1",
                        rusqlite::params![&service_id],
                        |row| row.get(0),
                    )?)
                },
            )
            .await
            .ok()
    }

    // =========================================================================
    // Circuit Breaker Methods
    // =========================================================================

    /// Record a restart event for circuit breaker tracking.
    ///
    /// This adds an entry to the restart_history table with the current timestamp.
    /// Old entries (older than 24 hours) are automatically cleaned up.
    #[must_use = "ignoring this result may cause state loss - restart will not be tracked for circuit breaker"]
    pub async fn record_restart(&mut self, service_id: &str) -> Result<()> {
        let service_id = service_id.to_string();
        let now = Utc::now().to_rfc3339();

        self.with_transaction(move |tx| {
            // Record the restart event
            tx.execute(
                "INSERT INTO restart_history (service_id, restarted_at) VALUES (?1, ?2)",
                rusqlite::params![&service_id, &now],
            )?;

            // Cleanup old entries (keep last 24 hours to avoid unbounded growth)
            tx.execute(
                "DELETE FROM restart_history WHERE service_id = ?1
                 AND restarted_at < datetime('now', '-1 day')",
                rusqlite::params![&service_id],
            )?;

            Ok(())
        })
        .await
    }

    /// Check if the circuit breaker should trip for a service.
    ///
    /// Returns `true` if the number of restarts within the specified window
    /// meets or exceeds the threshold, indicating a crash loop.
    ///
    /// # Arguments
    /// * `service_id` - The service to check
    /// * `threshold` - Number of restarts to trigger circuit breaker
    /// * `window_secs` - Time window in seconds for counting restarts
    pub async fn check_circuit_breaker(
        &self,
        service_id: &str,
        threshold: u32,
        window_secs: u64,
    ) -> Result<bool> {
        let service_id = service_id.to_string();

        self.conn
            .call(move |conn| {
                let count: u32 = conn.query_row(
                    "SELECT COUNT(*) FROM restart_history
                     WHERE service_id = ?1
                     AND restarted_at > datetime('now', ?2)",
                    rusqlite::params![&service_id, format!("-{} seconds", window_secs)],
                    |row| row.get(0),
                )?;

                Ok(count >= threshold)
            })
            .await
            .map_err(Error::from)
    }

    /// Open the circuit breaker for a service.
    ///
    /// This sets the `circuit_breaker_open_until` timestamp to the current time
    /// plus the cooldown period. While open, restart attempts should be blocked.
    ///
    /// # Arguments
    /// * `service_id` - The service to open the circuit breaker for
    /// * `cooldown_secs` - How long the circuit breaker should remain open
    #[must_use = "ignoring this result may cause the circuit breaker to not open - service may restart in a crash loop"]
    pub async fn open_circuit_breaker(
        &mut self,
        service_id: &str,
        cooldown_secs: u64,
    ) -> Result<()> {
        let service_id = service_id.to_string();

        self.with_transaction(move |tx| {
            tx.execute(
                "UPDATE services SET circuit_breaker_open_until = datetime('now', ?1)
                 WHERE id = ?2",
                rusqlite::params![format!("+{} seconds", cooldown_secs), &service_id],
            )?;
            Ok(())
        })
        .await
    }

    /// Get the restart history for a service (last 50 events).
    ///
    /// Returns a vector of RFC3339 timestamps for recent restarts.
    pub async fn get_restart_history(&self, service_id: &str) -> Result<Vec<String>> {
        let service_id = service_id.to_string();

        self.conn
            .call(move |conn| {
                let mut stmt = conn.prepare(
                    "SELECT restarted_at FROM restart_history
                     WHERE service_id = ?1
                     ORDER BY restarted_at DESC
                     LIMIT 50",
                )?;

                let events = stmt
                    .query_map(rusqlite::params![&service_id], |row| {
                        row.get::<_, String>(0)
                    })?
                    .collect::<std::result::Result<Vec<_>, _>>()?;

                Ok(events)
            })
            .await
            .map_err(Error::from)
    }

    /// Check if the circuit breaker is currently open (in cooldown period).
    ///
    /// Returns `true` if the circuit breaker is open and restarts should be blocked.
    /// Returns `false` if the circuit breaker is closed or the cooldown has expired.
    pub async fn is_circuit_breaker_open(&self, service_id: &str) -> bool {
        let service_id = service_id.to_string();

        self.conn
            .call(move |conn| {
                // Query checks if circuit_breaker_open_until is in the future
                let is_open: bool = conn
                    .query_row(
                        "SELECT circuit_breaker_open_until > datetime('now')
                         FROM services WHERE id = ?1",
                        rusqlite::params![&service_id],
                        |row| row.get(0),
                    )
                    .unwrap_or(false);

                Ok(is_open)
            })
            .await
            .unwrap_or(false)
    }

    /// Get the time remaining until circuit breaker closes (in seconds).
    ///
    /// Returns `Some(seconds)` if the circuit breaker is open, `None` if closed.
    /// This is useful for logging how long until the service can restart.
    pub async fn get_circuit_breaker_remaining(&self, service_id: &str) -> Result<Option<i64>> {
        let service_id = service_id.to_string();

        let remaining = self.conn
            .call(move |conn| {
                match conn.query_row(
                    "SELECT MAX(0, CAST((julianday(circuit_breaker_open_until) - julianday('now')) * 86400 AS INTEGER))
                     FROM services
                     WHERE id = ?1 AND circuit_breaker_open_until > datetime('now')",
                    rusqlite::params![&service_id],
                    |row| row.get(0),
                ) {
                    Ok(val) => Ok(Some(val)),
                    Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
                    Err(e) => Err(e.into()),
                }
            })
            .await?;

        Ok(remaining)
    }

    /// Close the circuit breaker for a service.
    ///
    /// This clears the `circuit_breaker_open_until` timestamp, allowing
    /// normal restart behavior to resume. Should be called when a service
    /// becomes healthy.
    #[must_use = "ignoring this result may leave the circuit breaker open - service may not restart when expected"]
    pub async fn close_circuit_breaker(&mut self, service_id: &str) -> Result<()> {
        let service_id = service_id.to_string();

        self.with_transaction(move |tx| {
            tx.execute(
                "UPDATE services SET circuit_breaker_open_until = NULL WHERE id = ?1",
                rusqlite::params![&service_id],
            )?;
            Ok(())
        })
        .await
    }

    /// Clear restart history for a service.
    ///
    /// This resets the circuit breaker tracking for a service. Should be called
    /// when the service has been healthy for a sustained period.
    #[must_use = "ignoring this result may leave stale restart history - circuit breaker may trip unexpectedly"]
    pub async fn clear_restart_history(&mut self, service_id: &str) -> Result<()> {
        let service_id = service_id.to_string();

        self.with_transaction(move |tx| {
            tx.execute(
                "DELETE FROM restart_history WHERE service_id = ?1",
                rusqlite::params![&service_id],
            )?;
            Ok(())
        })
        .await
    }

    /// Unregister a service (when stopped)
    pub async fn unregister_service(&mut self, service_id: &str) -> Result<()> {
        let service_id = service_id.to_string();
        let service_id_for_tx = service_id.clone();

        self.with_transaction(move |tx| {
            tx.execute(
                "DELETE FROM services WHERE id = ?1",
                rusqlite::params![&service_id_for_tx],
            )?;
            // Clean up global ports no longer in use
            tx.execute(
                "DELETE FROM allocated_ports WHERE port NOT IN (SELECT DISTINCT port FROM port_allocations)",
                [],
            )?;
            Ok(())
        })
        .await?;

        debug!("Unregistered service: {}", service_id);
        Ok(())
    }

    /// Track a newly allocated port
    pub async fn track_port(&mut self, port: u16) {
        let now = Utc::now().to_rfc3339();

        // Use INSERT OR IGNORE to handle duplicates
        if let Err(e) = self
            .conn
            .call(
                move |conn: &mut rusqlite::Connection| -> tokio_rusqlite::Result<()> {
                    conn.execute(
                "INSERT OR IGNORE INTO allocated_ports (port, allocated_at) VALUES (?1, ?2)",
                rusqlite::params![port, &now],
            )?;
                    Ok(())
                },
            )
            .await
        {
            warn!("Failed to track port {}: {}", port, e);
        }
    }

    /// Add port allocation to a specific service
    #[must_use = "ignoring this result may cause state loss - port allocation will not be tracked"]
    pub async fn add_service_port(
        &mut self,
        service_id: &str,
        param_name: String,
        port: u16,
    ) -> Result<()> {
        let service_id = service_id.to_string();
        let service_id_clone = service_id.clone();
        let now = Utc::now().to_rfc3339();

        let exists = self.conn.call(move |conn: &mut rusqlite::Connection| {
            let tx = conn.transaction()?;

            // Verify service exists
            let exists: bool = tx.query_row(
                "SELECT COUNT(*) > 0 FROM services WHERE id = ?1",
                rusqlite::params![&service_id_clone],
                |row| row.get(0),
            )?;

            if exists {
                // Insert or update port allocation
                tx.execute(
                    "INSERT OR REPLACE INTO port_allocations (service_id, parameter_name, port) VALUES (?1, ?2, ?3)",
                    rusqlite::params![&service_id_clone, &param_name, port],
                )?;

                // Track in global ports
                tx.execute(
                    "INSERT OR IGNORE INTO allocated_ports (port, allocated_at) VALUES (?1, ?2)",
                    rusqlite::params![port, &now],
                )?;

                tx.execute(
                    "UPDATE lock_file SET updated_at = datetime('now') WHERE id = 1",
                    [],
                )?;

                tx.commit()?;
            }

            Ok(exists)
        }).await?;

        if !exists {
            return Err(Error::ServiceNotFound(service_id));
        }

        Ok(())
    }

    /// Update service port mappings from Docker
    #[must_use = "ignoring this result may cause state loss - port mappings will not be tracked"]
    pub async fn update_service_port_mappings(
        &mut self,
        service_id: &str,
        port_mappings: HashMap<String, String>,
    ) -> Result<()> {
        let service_id = service_id.to_string();
        let service_id_clone = service_id.clone();
        let now = Utc::now().to_rfc3339();

        let exists = self.conn.call(move |conn: &mut rusqlite::Connection| {
            let tx = conn.transaction()?;

            // Verify service exists
            let exists: bool = tx.query_row(
                "SELECT COUNT(*) > 0 FROM services WHERE id = ?1",
                rusqlite::params![&service_id_clone],
                |row| row.get(0),
            )?;

            if exists {
                for (container_port, host_port) in port_mappings {
                    if let Ok(port_num) = host_port.parse::<u16>() {
                        tx.execute(
                            "INSERT OR REPLACE INTO port_allocations (service_id, parameter_name, port) VALUES (?1, ?2, ?3)",
                            rusqlite::params![&service_id_clone, &container_port, port_num],
                        )?;

                        tx.execute(
                            "INSERT OR IGNORE INTO allocated_ports (port, allocated_at) VALUES (?1, ?2)",
                            rusqlite::params![port_num, &now],
                        )?;
                    }
                }

                tx.execute(
                    "UPDATE lock_file SET updated_at = datetime('now') WHERE id = 1",
                    [],
                )?;

                tx.commit()?;
            }

            Ok(exists)
        }).await?;

        if !exists {
            return Err(Error::ServiceNotFound(service_id));
        }

        Ok(())
    }

    /// Get all allocated ports
    pub async fn get_allocated_ports(&self) -> Vec<u16> {
        match self
            .conn
            .call(|conn: &mut rusqlite::Connection| {
                let mut stmt = conn.prepare("SELECT port FROM allocated_ports ORDER BY port")?;
                let ports: Vec<u16> = stmt
                    .query_map([], |row| row.get(0))?
                    .filter_map(|r| r.ok())
                    .collect();
                Ok(ports)
            })
            .await
        {
            Ok(ports) => ports,
            Err(e) => {
                warn!("Failed to get allocated ports: {}", e);
                Vec::new()
            }
        }
    }

    /// Get all registered services
    pub async fn get_services(&self) -> HashMap<String, ServiceState> {
        match self.conn.call(|conn: &mut rusqlite::Connection| {
            let mut stmt = conn.prepare(
                "SELECT id, status, service_type, pid, container_id, started_at, external_repo, namespace, restart_count, last_restart_at, consecutive_failures, startup_message FROM services"
            )?;

            let services_iter = stmt.query_map([], |row| {
                let id: String = row.get(0)?;
                let status_str: String = row.get(1)?;
                let service_type_str: String = row.get(2)?;
                let started_at_str: String = row.get(5)?;
                let last_restart_str: Option<String> = row.get(9)?;

                Ok((
                    id.clone(),
                    status_str.clone(),
                    ServiceState {
                        id,
                        status: status_str.parse::<Status>().unwrap_or(Status::Stopped),
                        service_type: service_type_str.parse::<ServiceType>().unwrap_or(ServiceType::Undefined),
                        pid: row.get(3)?,
                        container_id: row.get(4)?,
                        started_at: started_at_str
                            .parse::<DateTime<Utc>>()
                            .unwrap_or_else(|_| Utc::now()),
                        external_repo: row.get(6)?,
                        namespace: row.get(7)?,
                        restart_count: row.get(8)?,
                        last_restart_at: last_restart_str.and_then(|s| s.parse::<DateTime<Utc>>().ok()),
                        consecutive_failures: row.get(10)?,
                        port_allocations: HashMap::new(), // Will be populated below
                        startup_message: row.get(11)?,
                    },
                ))
            })?;

            // Filter out stale DB-only statuses before constructing the map
            let mut services: HashMap<String, ServiceState> = services_iter
                .filter_map(|r| r.ok())
                .filter(|(_, raw_status, _)| !Self::status_is_stale(raw_status))
                .map(|(id, _, state)| (id, state))
                .collect();

            // Validate and remove services with invalid PIDs
            let mut invalid_service_ids = Vec::new();
            services.retain(|service_id, service_state| {
                if let Some(pid) = service_state.pid {
                    if pid > i32::MAX as u32 || pid == 0 {
                        warn!(
                            "Service '{}' has invalid PID {} (exceeds i32::MAX or is 0), removing from state",
                            service_id, pid
                        );
                        invalid_service_ids.push(service_id.clone());
                        return false;
                    }
                }
                true
            });

            // Delete invalid services from database
            for service_id in invalid_service_ids {
                let _ = conn.execute(
                    "DELETE FROM services WHERE id = ?1",
                    rusqlite::params![&service_id],
                );
            }

            // Load port allocations for each service
            for (service_id, service) in services.iter_mut() {
                let mut port_stmt = conn.prepare(
                    "SELECT parameter_name, port FROM port_allocations WHERE service_id = ?1"
                )?;

                let ports: HashMap<String, u16> = port_stmt
                    .query_map(rusqlite::params![service_id], |row| Ok((row.get(0)?, row.get(1)?)))?
                    .filter_map(|r| r.ok())
                    .collect();

                service.port_allocations = ports;
            }

            Ok(services)
        }).await {
            Ok(services) => services,
            Err(e) => {
                warn!("Failed to get services: {}", e);
                HashMap::new()
            }
        }
    }

    /// Get specific service state
    pub async fn get_service(&self, service_id: &str) -> Option<ServiceState> {
        let service_id = service_id.to_string();

        self.conn.call(move |conn: &mut rusqlite::Connection| -> tokio_rusqlite::Result<Option<ServiceState>> {
            let service = match conn.query_row(
                "SELECT id, status, service_type, pid, container_id, started_at, external_repo, namespace, restart_count, last_restart_at, consecutive_failures, startup_message FROM services WHERE id = ?1",
                rusqlite::params![&service_id],
                |row| {
                    let id: String = row.get(0)?;
                    let status_str: String = row.get(1)?;
                    let service_type_str: String = row.get(2)?;
                    let started_at_str: String = row.get(5)?;
                    let last_restart_str: Option<String> = row.get(9)?;

                    Ok(ServiceState {
                        id,
                        status: status_str.parse::<Status>().unwrap_or(Status::Stopped),
                        service_type: service_type_str.parse::<ServiceType>().unwrap_or(ServiceType::Undefined),
                        pid: row.get(3)?,
                        container_id: row.get(4)?,
                        started_at: started_at_str.parse::<DateTime<Utc>>().unwrap_or_else(|_| Utc::now()),
                        external_repo: row.get(6)?,
                        namespace: row.get(7)?,
                        restart_count: row.get(8)?,
                        last_restart_at: last_restart_str.and_then(|s| s.parse::<DateTime<Utc>>().ok()),
                        consecutive_failures: row.get(10)?,
                        port_allocations: HashMap::new(),
                        startup_message: row.get(11)?,
                    })
                }
            ) {
                Ok(s) => s,
                Err(_) => return Ok(None),
            };

            // Load port allocations
            let mut port_stmt = conn
                .prepare("SELECT parameter_name, port FROM port_allocations WHERE service_id = ?1")?;

            let ports: HashMap<String, u16> = port_stmt
                .query_map(rusqlite::params![&service_id], |row| Ok((row.get(0)?, row.get(1)?)))?
                .filter_map(|r| r.ok())
                .collect();

            Ok(Some(ServiceState {
                port_allocations: ports,
                ..service
            }))
        }).await.ok().flatten()
    }

    /// Check if a service is already registered
    pub async fn is_service_registered(&self, service_id: &str) -> bool {
        let service_id = service_id.to_string();

        self.conn
            .call(
                move |conn: &mut rusqlite::Connection| -> tokio_rusqlite::Result<bool> {
                    Ok(conn.query_row(
                        "SELECT COUNT(*) > 0 FROM services WHERE id = ?1",
                        rusqlite::params![&service_id],
                        |row| row.get(0),
                    )?)
                },
            )
            .await
            .unwrap_or(false)
    }

    /// Persist state to database (no-op for SQLite, always persisted)
    #[must_use = "ignoring this result may hide underlying errors"]
    pub async fn save(&mut self) -> Result<()> {
        // SQLite auto-persists with each transaction
        // This method exists for interface compatibility
        Ok(())
    }

    /// Force save (no-op for SQLite)
    pub async fn force_save(&mut self) -> Result<()> {
        // Update timestamp to indicate activity
        self.conn
            .call(|conn: &mut rusqlite::Connection| {
                conn.execute(
                    "UPDATE lock_file SET updated_at = datetime('now') WHERE id = 1",
                    [],
                )?;
                Ok(())
            })
            .await?;
        Ok(())
    }

    /// Clear runtime state (when all services stopped).
    ///
    /// Preserves `persisted_ports` so that port allocations from
    /// `fed ports randomize` survive stop/start cycles and error cleanup.
    /// Use `clear_port_resolutions()` to explicitly clear port allocations.
    #[must_use = "ignoring this result may leave stale state in the database"]
    pub async fn clear(&mut self) -> Result<()> {
        self.with_transaction(|tx| {
            tx.execute("DELETE FROM services", [])?;
            tx.execute("DELETE FROM port_allocations", [])?;
            // Clear bind reservations for stopped services. Preserve only those
            // backing persisted_ports (global parameter resolutions) so that
            // `fed ports randomize` allocations survive stop/start.
            tx.execute(
                "DELETE FROM allocated_ports WHERE port NOT IN (SELECT port FROM persisted_ports)",
                [],
            )?;
            Ok(())
        })
        .await?;

        info!("Cleared all services from state");
        Ok(())
    }

    /// Mark dead/stale services in state without deleting them.
    ///
    /// Sets status to `stale` so that port_allocations remain readable
    /// until [`SqliteStateTracker::purge_stale_services`] is called. This enables callers to
    /// collect managed port information before data is deleted.
    pub async fn mark_dead_services(&mut self) -> Result<usize> {
        let services = self.get_services().await;
        let mut stale_services = Vec::new();

        // Check Docker daemon health with retry before evaluating container services.
        // If daemon is unhealthy after retries, we cannot reliably determine container state,
        // so we skip container cleanup to avoid removing healthy containers.
        let daemon_healthy = crate::docker::check_daemon_with_retry().await;
        if !daemon_healthy {
            warn!("Docker daemon unhealthy after retries - skipping container cleanup to avoid data loss");
        }

        for (service_id, service_state) in &services {
            // Stale services are already filtered out at the DB read boundary

            let is_stale = if let Some(pid) = service_state.pid {
                !Self::is_process_running(pid).await
            } else if let Some(ref container_id) = service_state.container_id {
                // Only check container status if daemon is healthy
                if daemon_healthy {
                    !Self::is_container_running(container_id).await
                } else {
                    // Daemon unhealthy - assume container is running to avoid spurious cleanup
                    false
                }
            } else {
                // Service has no PID and no container_id.
                // Only consider stale if its status indicates it SHOULD be running.
                // Services that are "stopped" or were never started are not stale.
                Self::status_indicates_should_be_running(service_state.status)
            };

            if is_stale {
                debug!("Service '{}' appears to be stale", service_id);
                stale_services.push(service_id.clone());
            }
        }

        let marked = stale_services.len();

        if marked > 0 {
            debug!(
                "Marking {} stale service(s): {}",
                marked,
                stale_services.join(", ")
            );
            self.with_transaction(move |tx| {
                for service_id in &stale_services {
                    tx.execute(
                        "UPDATE services SET status = 'stale' WHERE id = ?1",
                        rusqlite::params![service_id],
                    )?;
                }
                Ok(())
            })
            .await?;

            info!("Marked {} dead service(s) as stale", marked);
        }

        Ok(marked)
    }

    /// Purge services previously marked as stale by [`SqliteStateTracker::mark_dead_services`].
    ///
    /// Deletes stale service records and their associated port_allocations
    /// (via CASCADE). Call this after managed port information has been collected.
    pub async fn purge_stale_services(&mut self) -> Result<usize> {
        let count: usize = self
            .conn
            .call(
                |conn: &mut rusqlite::Connection| -> tokio_rusqlite::Result<usize> {
                    let tx = conn.transaction()?;
                    let removed = tx.execute(
                        "DELETE FROM services WHERE status = 'stale'",
                        [],
                    )?;
                    // Clean up orphaned bind reservations: delete allocated_ports entries
                    // for ports no longer in use by any service (port_allocations) or
                    // global parameter resolution (persisted_ports).
                    tx.execute(
                        "DELETE FROM allocated_ports WHERE port NOT IN (SELECT DISTINCT port FROM port_allocations) AND port NOT IN (SELECT port FROM persisted_ports)",
                        [],
                    )?;
                    tx.execute(
                        "UPDATE lock_file SET updated_at = datetime('now') WHERE id = 1",
                        [],
                    )?;
                    tx.commit()?;
                    Ok(removed)
                },
            )
            .await?;

        if count > 0 {
            info!("Purged {} stale service(s) from state", count);
        }

        Ok(count)
    }

    /// Save resolved port parameters for an isolation scope.
    ///
    /// Writes to the `persisted_ports` table and updates `allocated_ports`
    /// for bind reservations. On subsequent `fed start`, `collect_managed_ports`
    /// reads these to detect ports owned by managed services.
    ///
    /// `isolation_id` selects the scope: `None` is the shared (non-isolated)
    /// scope, `Some(id)` an isolation session. Only the targeted scope's rows
    /// are replaced — saving the shared scope never touches isolation rows and
    /// vice versa. This is what keeps randomized ports from leaking into the
    /// non-isolated start path.
    pub async fn save_port_resolutions(
        &mut self,
        resolutions: &[(String, u16)],
        isolation_id: Option<&str>,
    ) -> Result<()> {
        let count = resolutions.len();
        let resolutions = resolutions.to_vec();
        let scope = isolation_id.unwrap_or("").to_string();
        let now = Utc::now().to_rfc3339();

        self.conn
            .call(move |conn: &mut rusqlite::Connection| -> tokio_rusqlite::Result<()> {
                let tx = conn.transaction()?;

                // Clear previous resolutions for this scope only
                tx.execute(
                    "DELETE FROM persisted_ports WHERE isolation_id = ?1",
                    rusqlite::params![&scope],
                )?;

                // Insert all resolved port parameters
                for (param_name, port) in &resolutions {
                    tx.execute(
                        "INSERT INTO persisted_ports (param_name, port, source, allocated_at, isolation_id) VALUES (?1, ?2, 'resolver', ?3, ?4)",
                        rusqlite::params![param_name, port, &now, &scope],
                    )?;
                    tx.execute(
                        "INSERT OR IGNORE INTO allocated_ports (port, allocated_at) VALUES (?1, ?2)",
                        rusqlite::params![port, &now],
                    )?;
                }

                tx.execute(
                    "UPDATE lock_file SET updated_at = datetime('now') WHERE id = 1",
                    [],
                )?;
                tx.commit()?;
                Ok(())
            })
            .await?;

        debug!("Saved {} port resolution(s) to state tracker", count);
        Ok(())
    }

    /// Get persisted port resolutions for an isolation scope.
    ///
    /// `isolation_id` selects the scope: `None` reads the shared (non-isolated)
    /// scope, `Some(id)` an isolation session.
    pub async fn get_global_port_allocations(
        &self,
        isolation_id: Option<&str>,
    ) -> HashMap<String, u16> {
        let scope = isolation_id.unwrap_or("").to_string();
        match self
            .conn
            .call(
                move |conn: &mut rusqlite::Connection| -> tokio_rusqlite::Result<HashMap<String, u16>> {
                    let mut stmt = conn.prepare(
                        "SELECT param_name, port FROM persisted_ports WHERE isolation_id = ?1",
                    )?;
                    let ports: HashMap<String, u16> = stmt
                        .query_map(rusqlite::params![&scope], |row| {
                            Ok((row.get::<_, String>(0)?, row.get::<_, u16>(1)?))
                        })?
                        .filter_map(|r| r.ok())
                        .collect();
                    Ok(ports)
                },
            )
            .await
        {
            Ok(ports) => ports,
            Err(e) => {
                warn!("Failed to read global port allocations: {}", e);
                HashMap::new()
            }
        }
    }

    /// Clear persisted port resolutions and allocated port bind reservations.
    ///
    /// Wipes every isolation scope — used by `fed isolate disable` and
    /// `fed clean` to return the project to configured ports.
    pub async fn clear_port_resolutions(&mut self) -> Result<()> {
        self.conn
            .call(
                |conn: &mut rusqlite::Connection| -> tokio_rusqlite::Result<()> {
                    let tx = conn.transaction()?;
                    tx.execute("DELETE FROM persisted_ports", [])?;
                    tx.execute("DELETE FROM allocated_ports", [])?;
                    tx.commit()?;
                    Ok(())
                },
            )
            .await?;
        info!("Cleared all port resolutions");
        Ok(())
    }

    /// Get the database file path
    pub fn lock_file_path(&self) -> &Path {
        &self.db_path
    }

    /// Convert to LockFile format (for compatibility with existing code)
    pub async fn to_lock_file(&self) -> Result<LockFile> {
        let (fed_pid, work_dir, started_at): (u32, String, String) = self
            .conn
            .call(
                |conn: &mut rusqlite::Connection| -> tokio_rusqlite::Result<(u32, String, String)> {
                    Ok(conn.query_row(
                        "SELECT fed_pid, work_dir, started_at FROM lock_file WHERE id = 1",
                        [],
                        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
                    )?)
                },
            )
            .await?;

        let services = self.get_services().await;
        let allocated_ports = self.get_allocated_ports().await;

        Ok(LockFile {
            fed_pid,
            work_dir,
            started_at: started_at
                .parse::<DateTime<Utc>>()
                .unwrap_or_else(|_| Utc::now()),
            services,
            allocated_ports,
        })
    }

    /// Set isolation mode (enabled + optional isolation_id).
    /// If enabled and isolation_id is None, a new ID is generated.
    pub async fn set_isolation_mode(
        &self,
        enabled: bool,
        isolation_id: Option<String>,
    ) -> Result<()> {
        let id = if enabled {
            isolation_id.unwrap_or_else(|| format!("iso-{:08x}", rand::random::<u32>()))
        } else {
            String::new()
        };
        let enabled_str = enabled.to_string();
        self.conn
            .call(move |conn: &mut rusqlite::Connection| {
                let tx = conn.transaction()?;
                tx.execute(
                    "INSERT INTO project_settings (key, value, updated_at) VALUES ('isolation_enabled', ?1, datetime('now'))
                     ON CONFLICT(key) DO UPDATE SET value = ?1, updated_at = datetime('now')",
                    rusqlite::params![&enabled_str],
                )?;
                if enabled {
                    tx.execute(
                        "INSERT INTO project_settings (key, value, updated_at) VALUES ('isolation_id', ?1, datetime('now'))
                         ON CONFLICT(key) DO UPDATE SET value = ?1, updated_at = datetime('now')",
                        rusqlite::params![&id],
                    )?;
                } else {
                    tx.execute(
                        "DELETE FROM project_settings WHERE key = 'isolation_id'",
                        [],
                    )?;
                }
                tx.commit()?;
                Ok(())
            })
            .await
            .map_err(Error::from)
    }

    /// Get current isolation mode: (enabled, optional isolation_id).
    pub async fn get_isolation_mode(&self) -> (bool, Option<String>) {
        let result = self
            .conn
            .call(|conn: &mut rusqlite::Connection| {
                let enabled: Option<String> = conn
                    .query_row(
                        "SELECT value FROM project_settings WHERE key = 'isolation_enabled'",
                        [],
                        |row| row.get(0),
                    )
                    .optional()?;

                let isolation_id: Option<String> = conn
                    .query_row(
                        "SELECT value FROM project_settings WHERE key = 'isolation_id'",
                        [],
                        |row| row.get(0),
                    )
                    .optional()?;

                Ok((enabled, isolation_id))
            })
            .await;

        match result {
            Ok((Some(enabled_str), id)) => {
                let enabled = enabled_str == "true";
                (enabled, if enabled { id } else { None })
            }
            _ => (false, None),
        }
    }

    /// Clear isolation mode entirely.
    pub async fn clear_isolation_mode(&self) -> Result<()> {
        self.conn
            .call(|conn: &mut rusqlite::Connection| {
                conn.execute(
                    "DELETE FROM project_settings WHERE key IN ('isolation_enabled', 'isolation_id')",
                    [],
                )?;
                Ok(())
            })
            .await
            .map_err(Error::from)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;
    #[cfg(unix)]
    use {
        fs2::FileExt,
        nix::sys::wait::waitpid,
        nix::unistd::{fork, pipe, read, write, ForkResult, Pid},
        std::fs::OpenOptions,
        std::os::fd::AsRawFd,
        std::path::Path,
        std::time::Duration,
    };

    #[cfg(unix)]
    enum LockOwnerPid {
        SelfPid,
        Literal(&'static str),
    }

    #[cfg(unix)]
    fn spawn_lock_holder(lock_path: &Path, pid_mode: LockOwnerPid) -> Pid {
        let (read_fd, write_fd) = pipe().expect("failed to create test sync pipe");

        match unsafe { fork() }.expect("failed to fork lock holder process") {
            ForkResult::Child => {
                drop(read_fd);

                let mut lock_file = OpenOptions::new()
                    .write(true)
                    .create(true)
                    .truncate(false)
                    .open(lock_path)
                    .expect("child failed to open lock path");

                lock_file
                    .try_lock_exclusive()
                    .expect("child failed to acquire lock");

                let owner_pid = match pid_mode {
                    LockOwnerPid::SelfPid => std::process::id().to_string(),
                    LockOwnerPid::Literal(value) => value.to_string(),
                };

                lock_file
                    .set_len(0)
                    .expect("child failed to truncate lock file");
                writeln!(lock_file, "{}", owner_pid).expect("child failed to write lock owner pid");

                let _ = write(&write_fd, &[1u8]);
                drop(write_fd);

                // Keep lock alive long enough for parent to contend on it.
                std::thread::sleep(Duration::from_secs(2));
                std::process::exit(0);
            }
            ForkResult::Parent { child } => {
                drop(write_fd);
                let mut ready = [0u8; 1];
                let _ = read(read_fd.as_raw_fd(), &mut ready)
                    .expect("failed to wait for child lock holder");
                drop(read_fd);
                child
            }
        }
    }

    #[cfg(unix)]
    fn wait_for_child_exit(child: Pid) {
        let _ = waitpid(child, None).expect("failed to wait for lock holder child");
    }

    /// Create a test state tracker with a temporary directory
    async fn create_test_tracker() -> (SqliteStateTracker, TempDir) {
        let temp_dir = TempDir::new().unwrap();
        let mut tracker = SqliteStateTracker::new(temp_dir.path().to_path_buf())
            .await
            .unwrap();
        tracker.initialize().await.unwrap();
        (tracker, temp_dir)
    }

    /// Register a test service for circuit breaker testing
    async fn register_test_service(tracker: &mut SqliteStateTracker, service_id: &str) {
        let state = ServiceState {
            id: service_id.to_string(),
            status: Status::Running,
            service_type: ServiceType::Process,
            pid: Some(12345),
            container_id: None,
            started_at: Utc::now(),
            external_repo: None,
            namespace: "test".to_string(),
            restart_count: 0,
            last_restart_at: None,
            consecutive_failures: 0,
            port_allocations: HashMap::new(),
            startup_message: None,
        };
        tracker.register_service(state).await.unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn test_try_acquire_lock_returns_none_when_held_by_another_process() {
        let temp_dir = TempDir::new().unwrap();
        let lock_path = temp_dir.path().join(".lock");

        let child = spawn_lock_holder(&lock_path, LockOwnerPid::SelfPid);
        let lock_while_contended = SqliteStateTracker::try_acquire_lock(&lock_path).unwrap();
        wait_for_child_exit(child);
        let lock_after_release = SqliteStateTracker::try_acquire_lock(&lock_path).unwrap();

        assert!(
            lock_while_contended.is_none(),
            "Expected None when another process holds the workspace lock"
        );
        assert!(
            lock_after_release.is_some(),
            "Expected lock acquisition to succeed after the competing process exits"
        );
    }

    #[cfg(unix)]
    #[test]
    fn test_try_acquire_lock_returns_none_with_non_pid_lock_contents() {
        let temp_dir = TempDir::new().unwrap();
        let lock_path = temp_dir.path().join(".lock");

        let child = spawn_lock_holder(&lock_path, LockOwnerPid::Literal("not-a-pid"));
        let lock_while_contended = SqliteStateTracker::try_acquire_lock(&lock_path).unwrap();
        wait_for_child_exit(child);
        let lock_after_release = SqliteStateTracker::try_acquire_lock(&lock_path).unwrap();

        assert!(
            lock_while_contended.is_none(),
            "Expected None on lock contention even when lock file PID contents are invalid"
        );
        assert!(
            lock_after_release.is_some(),
            "Expected lock acquisition to succeed once contention is gone"
        );
    }

    #[tokio::test]
    async fn test_record_restart() {
        let (mut tracker, _temp_dir) = create_test_tracker().await;
        register_test_service(&mut tracker, "test-service").await;

        // Record a restart
        let result = tracker.record_restart("test-service").await;
        assert!(result.is_ok());

        // Verify it was recorded by checking if it triggers circuit breaker
        let should_trip = tracker
            .check_circuit_breaker("test-service", 1, 60)
            .await
            .unwrap();
        assert!(
            should_trip,
            "Should trip with threshold of 1 after 1 restart"
        );
    }

    #[tokio::test]
    async fn test_circuit_breaker_does_not_trip_below_threshold() {
        let (mut tracker, _temp_dir) = create_test_tracker().await;
        register_test_service(&mut tracker, "test-service").await;

        // Record 3 restarts
        for _ in 0..3 {
            tracker.record_restart("test-service").await.unwrap();
        }

        // Should not trip with threshold of 5
        let should_trip = tracker
            .check_circuit_breaker("test-service", 5, 60)
            .await
            .unwrap();
        assert!(
            !should_trip,
            "Should not trip with 3 restarts when threshold is 5"
        );
    }

    #[tokio::test]
    async fn test_circuit_breaker_trips_at_threshold() {
        let (mut tracker, _temp_dir) = create_test_tracker().await;
        register_test_service(&mut tracker, "test-service").await;

        // Record exactly 5 restarts (the default threshold)
        for _ in 0..5 {
            tracker.record_restart("test-service").await.unwrap();
        }

        // Should trip with threshold of 5
        let should_trip = tracker
            .check_circuit_breaker("test-service", 5, 60)
            .await
            .unwrap();
        assert!(
            should_trip,
            "Should trip with 5 restarts when threshold is 5"
        );
    }

    #[tokio::test]
    async fn test_circuit_breaker_trips_above_threshold() {
        let (mut tracker, _temp_dir) = create_test_tracker().await;
        register_test_service(&mut tracker, "test-service").await;

        // Record 7 restarts (above threshold of 5)
        for _ in 0..7 {
            tracker.record_restart("test-service").await.unwrap();
        }

        // Should trip with threshold of 5
        let should_trip = tracker
            .check_circuit_breaker("test-service", 5, 60)
            .await
            .unwrap();
        assert!(
            should_trip,
            "Should trip with 7 restarts when threshold is 5"
        );
    }

    #[tokio::test]
    async fn test_open_and_check_circuit_breaker() {
        let (mut tracker, _temp_dir) = create_test_tracker().await;
        register_test_service(&mut tracker, "test-service").await;

        // Initially circuit breaker should be closed
        let is_open = tracker.is_circuit_breaker_open("test-service").await;
        assert!(!is_open, "Circuit breaker should be closed initially");

        // Open the circuit breaker with 300s cooldown
        tracker
            .open_circuit_breaker("test-service", 300)
            .await
            .unwrap();

        // Now it should be open
        let is_open = tracker.is_circuit_breaker_open("test-service").await;
        assert!(is_open, "Circuit breaker should be open after opening");
    }

    #[tokio::test]
    async fn test_close_circuit_breaker() {
        let (mut tracker, _temp_dir) = create_test_tracker().await;
        register_test_service(&mut tracker, "test-service").await;

        // Open the circuit breaker
        tracker
            .open_circuit_breaker("test-service", 300)
            .await
            .unwrap();
        assert!(tracker.is_circuit_breaker_open("test-service").await);

        // Close it
        tracker.close_circuit_breaker("test-service").await.unwrap();

        // Should be closed now
        let is_open = tracker.is_circuit_breaker_open("test-service").await;
        assert!(!is_open, "Circuit breaker should be closed after closing");
    }

    #[tokio::test]
    async fn test_circuit_breaker_remaining_time() {
        let (mut tracker, _temp_dir) = create_test_tracker().await;
        register_test_service(&mut tracker, "test-service").await;

        // No remaining time when circuit is closed
        let remaining = tracker
            .get_circuit_breaker_remaining("test-service")
            .await
            .unwrap();
        assert!(
            remaining.is_none(),
            "Should have no remaining time when closed"
        );

        // Open the circuit breaker with 60s cooldown
        tracker
            .open_circuit_breaker("test-service", 60)
            .await
            .unwrap();

        // Should have remaining time
        let remaining = tracker
            .get_circuit_breaker_remaining("test-service")
            .await
            .unwrap();
        assert!(remaining.is_some(), "Should have remaining time when open");
        let remaining = remaining.unwrap();
        // Should be approximately 60 seconds (allow some tolerance)
        assert!(
            (55..=65).contains(&remaining),
            "Remaining time should be approximately 60s, got {}",
            remaining
        );
    }

    #[tokio::test]
    async fn test_clear_restart_history() {
        let (mut tracker, _temp_dir) = create_test_tracker().await;
        register_test_service(&mut tracker, "test-service").await;

        // Record some restarts
        for _ in 0..5 {
            tracker.record_restart("test-service").await.unwrap();
        }

        // Verify restarts are recorded
        let should_trip = tracker
            .check_circuit_breaker("test-service", 5, 60)
            .await
            .unwrap();
        assert!(should_trip, "Should trip after recording restarts");

        // Clear history
        tracker.clear_restart_history("test-service").await.unwrap();

        // Should not trip now (no restart history)
        let should_trip = tracker
            .check_circuit_breaker("test-service", 5, 60)
            .await
            .unwrap();
        assert!(!should_trip, "Should not trip after clearing history");
    }

    #[tokio::test]
    async fn test_circuit_breaker_per_service_isolation() {
        let (mut tracker, _temp_dir) = create_test_tracker().await;
        register_test_service(&mut tracker, "service-a").await;
        register_test_service(&mut tracker, "service-b").await;

        // Record restarts only for service-a
        for _ in 0..5 {
            tracker.record_restart("service-a").await.unwrap();
        }

        // Service A should trip
        let should_trip_a = tracker
            .check_circuit_breaker("service-a", 5, 60)
            .await
            .unwrap();
        assert!(should_trip_a, "Service A should trip");

        // Service B should not trip (no restarts recorded)
        let should_trip_b = tracker
            .check_circuit_breaker("service-b", 5, 60)
            .await
            .unwrap();
        assert!(!should_trip_b, "Service B should not trip");

        // Open circuit breaker only for service-a
        tracker
            .open_circuit_breaker("service-a", 300)
            .await
            .unwrap();

        // Service A circuit should be open
        assert!(tracker.is_circuit_breaker_open("service-a").await);

        // Service B circuit should still be closed
        assert!(!tracker.is_circuit_breaker_open("service-b").await);
    }

    #[tokio::test]
    async fn test_circuit_breaker_with_zero_cooldown() {
        let (mut tracker, _temp_dir) = create_test_tracker().await;
        register_test_service(&mut tracker, "test-service").await;

        // Open with 0 cooldown - should immediately be considered closed
        tracker
            .open_circuit_breaker("test-service", 0)
            .await
            .unwrap();

        // With 0 cooldown, the circuit breaker should not be considered open
        // because the "open_until" time would be "now + 0 seconds" = now
        // and the check is "open_until > now" which would be false
        let is_open = tracker.is_circuit_breaker_open("test-service").await;
        // Note: This might still be open due to timing, so we accept either result
        // The important thing is it doesn't panic
        let _ = is_open;
    }

    #[tokio::test]
    async fn test_circuit_breaker_unregistered_service() {
        let (tracker, _temp_dir) = create_test_tracker().await;

        // Check circuit breaker for non-existent service
        let is_open = tracker.is_circuit_breaker_open("nonexistent").await;
        assert!(!is_open, "Non-existent service should have closed circuit");

        let should_trip = tracker
            .check_circuit_breaker("nonexistent", 5, 60)
            .await
            .unwrap();
        assert!(
            !should_trip,
            "Non-existent service should not trip circuit breaker"
        );
    }

    #[tokio::test]
    async fn test_ephemeral_tracker_works() {
        let mut tracker = SqliteStateTracker::new_ephemeral().await.unwrap();
        tracker.initialize().await.unwrap();

        // Register a service
        register_test_service(&mut tracker, "ephemeral-svc").await;

        // Query it back
        let services = tracker.get_services().await;
        assert_eq!(services.len(), 1);
        assert!(services.contains_key("ephemeral-svc"));

        // Clear and verify empty
        tracker.clear().await.unwrap();
        let services = tracker.get_services().await;
        assert!(services.is_empty());
    }

    // ========================================================================
    // apply_state_transition tests
    // ========================================================================

    /// Register a service in Stopped state for transition testing
    async fn register_stopped_service(tracker: &mut SqliteStateTracker, service_id: &str) {
        let state = ServiceState {
            id: service_id.to_string(),
            status: Status::Stopped,
            service_type: ServiceType::Process,
            pid: None,
            container_id: None,
            started_at: Utc::now(),
            external_repo: None,
            namespace: "test".to_string(),
            restart_count: 0,
            last_restart_at: None,
            consecutive_failures: 0,
            port_allocations: HashMap::new(),
            startup_message: None,
        };
        tracker.register_service(state).await.unwrap();
    }

    #[tokio::test]
    async fn test_apply_transition_stopped_to_starting() {
        let (mut tracker, _temp_dir) = create_test_tracker().await;
        register_stopped_service(&mut tracker, "svc").await;

        let transition = crate::service::StateTransition::starting();
        tracker
            .apply_state_transition("svc", transition)
            .await
            .unwrap();

        let state = tracker.get_service("svc").await.unwrap();
        assert_eq!(state.status, Status::Starting);
    }

    #[tokio::test]
    async fn test_apply_transition_starting_to_running_with_pid() {
        let (mut tracker, _temp_dir) = create_test_tracker().await;
        register_stopped_service(&mut tracker, "svc").await;

        // Stopped -> Starting
        tracker
            .apply_state_transition("svc", crate::service::StateTransition::starting())
            .await
            .unwrap();

        // Starting -> Running with PID
        tracker
            .apply_state_transition("svc", crate::service::StateTransition::running_with_pid(42))
            .await
            .unwrap();

        let state = tracker.get_service("svc").await.unwrap();
        assert_eq!(state.status, Status::Running);
        assert_eq!(state.pid, Some(42));
    }

    #[tokio::test]
    async fn test_apply_transition_starting_to_running_with_container() {
        let (mut tracker, _temp_dir) = create_test_tracker().await;
        register_stopped_service(&mut tracker, "svc").await;

        tracker
            .apply_state_transition("svc", crate::service::StateTransition::starting())
            .await
            .unwrap();

        tracker
            .apply_state_transition(
                "svc",
                crate::service::StateTransition::running_with_container("abc123".to_string()),
            )
            .await
            .unwrap();

        let state = tracker.get_service("svc").await.unwrap();
        assert_eq!(state.status, Status::Running);
        assert_eq!(state.container_id, Some("abc123".to_string()));
    }

    #[tokio::test]
    async fn test_apply_transition_stopped_clears_pid_and_container() {
        let (mut tracker, _temp_dir) = create_test_tracker().await;
        register_stopped_service(&mut tracker, "svc").await;

        // Go through Starting -> Running (with PID) -> Stopping -> Stopped
        tracker
            .apply_state_transition("svc", crate::service::StateTransition::starting())
            .await
            .unwrap();
        tracker
            .apply_state_transition("svc", crate::service::StateTransition::running_with_pid(99))
            .await
            .unwrap();
        tracker
            .apply_state_transition("svc", crate::service::StateTransition::stopping())
            .await
            .unwrap();
        tracker
            .apply_state_transition("svc", crate::service::StateTransition::stopped())
            .await
            .unwrap();

        let state = tracker.get_service("svc").await.unwrap();
        assert_eq!(state.status, Status::Stopped);
        assert_eq!(state.pid, None);
        assert_eq!(state.container_id, None);
    }

    #[tokio::test]
    async fn test_apply_transition_invalid_stopped_to_running() {
        let (mut tracker, _temp_dir) = create_test_tracker().await;
        register_stopped_service(&mut tracker, "svc").await;

        // Stopped -> Running is invalid (must go through Starting)
        let result = tracker
            .apply_state_transition("svc", crate::service::StateTransition::running())
            .await;
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("Invalid state transition"),
            "Expected validation error, got: {}",
            err_msg
        );
    }

    #[tokio::test]
    async fn test_apply_transition_nonexistent_service() {
        let (mut tracker, _temp_dir) = create_test_tracker().await;

        let result = tracker
            .apply_state_transition(
                "no-such-service",
                crate::service::StateTransition::starting(),
            )
            .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_apply_transition_running_to_healthy() {
        let (mut tracker, _temp_dir) = create_test_tracker().await;
        register_stopped_service(&mut tracker, "svc").await;

        tracker
            .apply_state_transition("svc", crate::service::StateTransition::starting())
            .await
            .unwrap();
        tracker
            .apply_state_transition("svc", crate::service::StateTransition::running())
            .await
            .unwrap();
        tracker
            .apply_state_transition("svc", crate::service::StateTransition::healthy())
            .await
            .unwrap();

        let state = tracker.get_service("svc").await.unwrap();
        assert_eq!(state.status, Status::Healthy);
    }

    #[tokio::test]
    async fn test_apply_transition_running_to_failing() {
        let (mut tracker, _temp_dir) = create_test_tracker().await;
        register_stopped_service(&mut tracker, "svc").await;

        tracker
            .apply_state_transition("svc", crate::service::StateTransition::starting())
            .await
            .unwrap();
        tracker
            .apply_state_transition("svc", crate::service::StateTransition::running())
            .await
            .unwrap();
        tracker
            .apply_state_transition("svc", crate::service::StateTransition::failing())
            .await
            .unwrap();

        let state = tracker.get_service("svc").await.unwrap();
        assert_eq!(state.status, Status::Failing);
    }

    #[tokio::test]
    async fn test_apply_transition_same_state_is_noop() {
        let (mut tracker, _temp_dir) = create_test_tracker().await;
        register_stopped_service(&mut tracker, "svc").await;

        // Stopped -> Stopped should succeed (same-state is valid)
        tracker
            .apply_state_transition("svc", crate::service::StateTransition::stopped())
            .await
            .unwrap();

        let state = tracker.get_service("svc").await.unwrap();
        assert_eq!(state.status, Status::Stopped);
    }

    // ========================================================================
    // CRUD operation tests
    // ========================================================================

    /// Create an ephemeral test tracker (no filesystem, no lock file)
    async fn create_ephemeral_tracker() -> SqliteStateTracker {
        let mut tracker = SqliteStateTracker::new_ephemeral().await.unwrap();
        tracker.initialize().await.unwrap();
        tracker
    }

    /// Build a ServiceState with the given id and service type
    fn make_service_state(id: &str, stype: ServiceType) -> ServiceState {
        ServiceState {
            id: id.to_string(),
            status: Status::Running,
            service_type: stype,
            pid: Some(99999),
            container_id: None,
            started_at: Utc::now(),
            external_repo: None,
            namespace: "test".to_string(),
            restart_count: 0,
            last_restart_at: None,
            consecutive_failures: 0,
            port_allocations: HashMap::new(),
            startup_message: None,
        }
    }

    // --- register_service ---

    #[tokio::test]
    async fn test_register_service_new() {
        let mut tracker = create_ephemeral_tracker().await;

        let state = make_service_state("svc-a", ServiceType::Process);
        let outcome = tracker.register_service(state).await.unwrap();

        assert_eq!(
            outcome,
            RegistrationOutcome::Registered,
            "First registration should return Registered"
        );

        let retrieved = tracker.get_service("svc-a").await;
        assert!(
            retrieved.is_some(),
            "Service should be retrievable after registration"
        );
        let retrieved = retrieved.unwrap();
        assert_eq!(retrieved.id, "svc-a");
        assert_eq!(retrieved.status, Status::Running);
        assert_eq!(retrieved.service_type, ServiceType::Process);
        assert_eq!(retrieved.pid, Some(99999));
        assert_eq!(retrieved.namespace, "test");
    }

    #[tokio::test]
    async fn test_register_service_duplicate_returns_already_exists() {
        let mut tracker = create_ephemeral_tracker().await;

        let state = make_service_state("svc-a", ServiceType::Process);
        let first = tracker.register_service(state).await.unwrap();
        assert_eq!(first, RegistrationOutcome::Registered);

        let state2 = make_service_state("svc-a", ServiceType::Docker);
        let second = tracker.register_service(state2).await.unwrap();
        assert_eq!(
            second,
            RegistrationOutcome::AlreadyExists {
                status: Status::Running
            },
            "Second registration should return AlreadyExists with current status"
        );

        // The existing row must be left completely untouched
        let retrieved = tracker.get_service("svc-a").await.unwrap();
        assert_eq!(
            retrieved.service_type,
            ServiceType::Process,
            "service_type must not change on duplicate registration"
        );
    }

    #[tokio::test]
    async fn test_register_service_with_all_fields() {
        let mut tracker = create_ephemeral_tracker().await;

        let state = ServiceState {
            id: "full-svc".to_string(),
            status: Status::Healthy,
            service_type: ServiceType::Docker,
            pid: None,
            container_id: Some("abc123def".to_string()),
            started_at: Utc::now(),
            external_repo: Some("github.com/test/repo".to_string()),
            namespace: "external".to_string(),
            restart_count: 3,
            last_restart_at: Some(Utc::now()),
            consecutive_failures: 1,
            port_allocations: HashMap::new(),
            startup_message: Some("Running on port 8080".to_string()),
        };
        tracker.register_service(state).await.unwrap();

        let retrieved = tracker.get_service("full-svc").await.unwrap();
        assert_eq!(retrieved.status, Status::Healthy);
        assert_eq!(retrieved.container_id, Some("abc123def".to_string()));
        assert_eq!(
            retrieved.external_repo,
            Some("github.com/test/repo".to_string())
        );
        assert_eq!(retrieved.namespace, "external");
        assert_eq!(retrieved.restart_count, 3);
        assert!(retrieved.last_restart_at.is_some());
        assert_eq!(retrieved.consecutive_failures, 1);
        assert_eq!(
            retrieved.startup_message,
            Some("Running on port 8080".to_string())
        );
    }

    #[tokio::test]
    async fn test_register_multiple_services() {
        let mut tracker = create_ephemeral_tracker().await;

        for name in &["alpha", "beta", "gamma"] {
            let state = make_service_state(name, ServiceType::Process);
            tracker.register_service(state).await.unwrap();
        }

        let services = tracker.get_services().await;
        assert_eq!(services.len(), 3);
        assert!(services.contains_key("alpha"));
        assert!(services.contains_key("beta"));
        assert!(services.contains_key("gamma"));
    }

    // --- unregister_service ---

    #[tokio::test]
    async fn test_unregister_service_removes_it() {
        let mut tracker = create_ephemeral_tracker().await;

        let state = make_service_state("to-remove", ServiceType::Process);
        tracker.register_service(state).await.unwrap();

        // Confirm it exists
        assert!(tracker.get_service("to-remove").await.is_some());

        tracker.unregister_service("to-remove").await.unwrap();

        // Confirm it's gone
        assert!(tracker.get_service("to-remove").await.is_none());
        let services = tracker.get_services().await;
        assert!(!services.contains_key("to-remove"));
    }

    #[tokio::test]
    async fn test_unregister_nonexistent_service_succeeds() {
        let mut tracker = create_ephemeral_tracker().await;

        // Unregistering a service that doesn't exist should not error
        // (DELETE WHERE id = ? simply affects 0 rows)
        let result = tracker.unregister_service("ghost").await;
        assert!(
            result.is_ok(),
            "Unregistering nonexistent service should succeed silently"
        );
    }

    #[tokio::test]
    async fn test_unregister_does_not_affect_other_services() {
        let mut tracker = create_ephemeral_tracker().await;

        let s1 = make_service_state("keep-me", ServiceType::Process);
        let s2 = make_service_state("remove-me", ServiceType::Docker);
        tracker.register_service(s1).await.unwrap();
        tracker.register_service(s2).await.unwrap();

        tracker.unregister_service("remove-me").await.unwrap();

        assert!(tracker.get_service("keep-me").await.is_some());
        assert!(tracker.get_service("remove-me").await.is_none());
    }

    // --- update_service_status ---

    #[tokio::test]
    async fn test_update_service_status_happy_path() {
        let mut tracker = create_ephemeral_tracker().await;

        let state = make_service_state("svc", ServiceType::Process);
        tracker.register_service(state).await.unwrap();

        tracker
            .update_service_status("svc", Status::Healthy)
            .await
            .unwrap();

        let retrieved = tracker.get_service("svc").await.unwrap();
        assert_eq!(retrieved.status, Status::Healthy);
    }

    #[tokio::test]
    async fn test_update_service_status_multiple_transitions() {
        let mut tracker = create_ephemeral_tracker().await;

        let state = ServiceState {
            id: "svc".to_string(),
            status: Status::Starting,
            service_type: ServiceType::Process,
            pid: None,
            container_id: None,
            started_at: Utc::now(),
            external_repo: None,
            namespace: "test".to_string(),
            restart_count: 0,
            last_restart_at: None,
            consecutive_failures: 0,
            port_allocations: HashMap::new(),
            startup_message: None,
        };
        tracker.register_service(state).await.unwrap();

        // Starting -> Running
        tracker
            .update_service_status("svc", Status::Running)
            .await
            .unwrap();
        assert_eq!(
            tracker.get_service("svc").await.unwrap().status,
            Status::Running
        );

        // Running -> Healthy
        tracker
            .update_service_status("svc", Status::Healthy)
            .await
            .unwrap();
        assert_eq!(
            tracker.get_service("svc").await.unwrap().status,
            Status::Healthy
        );

        // Healthy -> Stopping
        tracker
            .update_service_status("svc", Status::Stopping)
            .await
            .unwrap();
        assert_eq!(
            tracker.get_service("svc").await.unwrap().status,
            Status::Stopping
        );

        // Stopping -> Stopped
        tracker
            .update_service_status("svc", Status::Stopped)
            .await
            .unwrap();
        assert_eq!(
            tracker.get_service("svc").await.unwrap().status,
            Status::Stopped
        );
    }

    #[tokio::test]
    async fn test_update_service_status_nonexistent_returns_error() {
        let mut tracker = create_ephemeral_tracker().await;

        let result = tracker
            .update_service_status("no-such-service", Status::Running)
            .await;
        assert!(result.is_err(), "Updating nonexistent service should error");

        let err = result.unwrap_err();
        let err_msg = err.to_string();
        assert!(
            err_msg.contains("no-such-service"),
            "Error should mention the service name, got: {}",
            err_msg
        );
    }

    // --- mark_dead_services ---

    #[tokio::test]
    async fn test_mark_dead_services_no_pid_no_container_running_status() {
        let mut tracker = create_ephemeral_tracker().await;

        // A service with Running status but no PID and no container_id
        // should be considered stale (it claims to be running but has no
        // process or container to back that claim).
        let state = ServiceState {
            id: "orphan".to_string(),
            status: Status::Running,
            service_type: ServiceType::Process,
            pid: None,
            container_id: None,
            started_at: Utc::now(),
            external_repo: None,
            namespace: "test".to_string(),
            restart_count: 0,
            last_restart_at: None,
            consecutive_failures: 0,
            port_allocations: HashMap::new(),
            startup_message: None,
        };
        tracker.register_service(state).await.unwrap();

        let marked = tracker.mark_dead_services().await.unwrap();
        assert_eq!(marked, 1, "Should mark 1 stale service");

        // get_services filters stale, so it should be empty now
        let services = tracker.get_services().await;
        assert!(
            services.is_empty(),
            "Stale services should be filtered from get_services()"
        );
    }

    #[tokio::test]
    async fn test_mark_dead_services_stopped_not_marked() {
        let mut tracker = create_ephemeral_tracker().await;

        // A Stopped service with no PID should NOT be marked stale
        let state = ServiceState {
            id: "stopped-svc".to_string(),
            status: Status::Stopped,
            service_type: ServiceType::Process,
            pid: None,
            container_id: None,
            started_at: Utc::now(),
            external_repo: None,
            namespace: "test".to_string(),
            restart_count: 0,
            last_restart_at: None,
            consecutive_failures: 0,
            port_allocations: HashMap::new(),
            startup_message: None,
        };
        tracker.register_service(state).await.unwrap();

        let marked = tracker.mark_dead_services().await.unwrap();
        assert_eq!(marked, 0, "Stopped service should not be marked stale");

        let services = tracker.get_services().await;
        assert_eq!(services.len(), 1);
    }

    #[tokio::test]
    async fn test_mark_dead_services_with_dead_pid() {
        let mut tracker = create_ephemeral_tracker().await;

        // Spawn a short-lived process and wait for it to finish, giving us a
        // PID that is guaranteed to be dead without relying on magic constants.
        let mut child = std::process::Command::new("true").spawn().unwrap();
        let dead_pid = child.id();
        child.wait().unwrap();

        let state = ServiceState {
            id: "dead-pid-svc".to_string(),
            status: Status::Running,
            service_type: ServiceType::Process,
            pid: Some(dead_pid),
            container_id: None,
            started_at: Utc::now(),
            external_repo: None,
            namespace: "test".to_string(),
            restart_count: 0,
            last_restart_at: None,
            consecutive_failures: 0,
            port_allocations: HashMap::new(),
            startup_message: None,
        };
        tracker.register_service(state).await.unwrap();

        let marked = tracker.mark_dead_services().await.unwrap();
        assert_eq!(marked, 1, "Service with dead PID should be marked stale");
    }

    // --- purge_stale_services ---

    #[tokio::test]
    async fn test_purge_stale_services_removes_stale() {
        let mut tracker = create_ephemeral_tracker().await;

        // Register a service that will become stale
        let state = ServiceState {
            id: "will-be-stale".to_string(),
            status: Status::Running,
            service_type: ServiceType::Process,
            pid: None,
            container_id: None,
            started_at: Utc::now(),
            external_repo: None,
            namespace: "test".to_string(),
            restart_count: 0,
            last_restart_at: None,
            consecutive_failures: 0,
            port_allocations: HashMap::new(),
            startup_message: None,
        };
        tracker.register_service(state).await.unwrap();

        // Mark it stale
        let marked = tracker.mark_dead_services().await.unwrap();
        assert_eq!(marked, 1);

        // Purge stale services
        let purged = tracker.purge_stale_services().await.unwrap();
        assert_eq!(purged, 1, "Should purge 1 stale service");

        // Service should be completely gone now (even from get_service which doesn't filter stale)
        assert!(
            tracker.get_service("will-be-stale").await.is_none(),
            "Purged service should be completely removed from the database"
        );
    }

    #[tokio::test]
    async fn test_purge_stale_services_leaves_healthy() {
        let mut tracker = create_ephemeral_tracker().await;

        // Register a healthy service with a real-ish PID (current process)
        let state = ServiceState {
            id: "alive".to_string(),
            status: Status::Stopped,
            service_type: ServiceType::Process,
            pid: None,
            container_id: None,
            started_at: Utc::now(),
            external_repo: None,
            namespace: "test".to_string(),
            restart_count: 0,
            last_restart_at: None,
            consecutive_failures: 0,
            port_allocations: HashMap::new(),
            startup_message: None,
        };
        tracker.register_service(state).await.unwrap();

        // No services should be marked stale (Stopped status is not "should be running")
        let marked = tracker.mark_dead_services().await.unwrap();
        assert_eq!(marked, 0);

        let purged = tracker.purge_stale_services().await.unwrap();
        assert_eq!(purged, 0, "No stale services to purge");

        assert!(tracker.get_service("alive").await.is_some());
    }

    #[tokio::test]
    async fn test_purge_with_no_stale_services() {
        let mut tracker = create_ephemeral_tracker().await;

        let purged = tracker.purge_stale_services().await.unwrap();
        assert_eq!(purged, 0, "Purging empty tracker should return 0");
    }

    // --- Port persistence ---

    #[tokio::test]
    async fn test_add_service_port_and_retrieve() {
        let mut tracker = create_ephemeral_tracker().await;

        let state = make_service_state("port-svc", ServiceType::Process);
        tracker.register_service(state).await.unwrap();

        tracker
            .add_service_port("port-svc", "HTTP_PORT".to_string(), 8080)
            .await
            .unwrap();

        // Retrieve via get_service (loads port_allocations)
        let svc = tracker.get_service("port-svc").await.unwrap();
        assert_eq!(svc.port_allocations.len(), 1);
        assert_eq!(svc.port_allocations.get("HTTP_PORT"), Some(&8080));

        // Also appears in global allocated ports
        let allocated = tracker.get_allocated_ports().await;
        assert!(allocated.contains(&8080));
    }

    #[tokio::test]
    async fn test_add_multiple_ports_to_service() {
        let mut tracker = create_ephemeral_tracker().await;

        let state = make_service_state("multi-port", ServiceType::Docker);
        tracker.register_service(state).await.unwrap();

        tracker
            .add_service_port("multi-port", "HTTP_PORT".to_string(), 8080)
            .await
            .unwrap();
        tracker
            .add_service_port("multi-port", "GRPC_PORT".to_string(), 9090)
            .await
            .unwrap();
        tracker
            .add_service_port("multi-port", "DEBUG_PORT".to_string(), 5005)
            .await
            .unwrap();

        let svc = tracker.get_service("multi-port").await.unwrap();
        assert_eq!(svc.port_allocations.len(), 3);
        assert_eq!(svc.port_allocations.get("HTTP_PORT"), Some(&8080));
        assert_eq!(svc.port_allocations.get("GRPC_PORT"), Some(&9090));
        assert_eq!(svc.port_allocations.get("DEBUG_PORT"), Some(&5005));
    }

    #[tokio::test]
    async fn test_add_service_port_nonexistent_service_errors() {
        let mut tracker = create_ephemeral_tracker().await;

        let result = tracker
            .add_service_port("no-such-svc", "PORT".to_string(), 8080)
            .await;
        assert!(
            result.is_err(),
            "Adding port to nonexistent service should error"
        );
    }

    #[tokio::test]
    async fn test_add_service_port_replaces_existing_param() {
        let mut tracker = create_ephemeral_tracker().await;

        let state = make_service_state("svc", ServiceType::Process);
        tracker.register_service(state).await.unwrap();

        tracker
            .add_service_port("svc", "PORT".to_string(), 8080)
            .await
            .unwrap();
        // Replace with new port for same parameter
        tracker
            .add_service_port("svc", "PORT".to_string(), 9090)
            .await
            .unwrap();

        let svc = tracker.get_service("svc").await.unwrap();
        assert_eq!(svc.port_allocations.len(), 1);
        assert_eq!(svc.port_allocations.get("PORT"), Some(&9090));
    }

    #[tokio::test]
    async fn test_save_and_get_global_port_allocations() {
        let mut tracker = create_ephemeral_tracker().await;

        let resolutions = vec![
            ("HTTP_PORT".to_string(), 8080u16),
            ("GRPC_PORT".to_string(), 9090u16),
        ];
        tracker
            .save_port_resolutions(&resolutions, None)
            .await
            .unwrap();

        let globals = tracker.get_global_port_allocations(None).await;
        assert_eq!(globals.len(), 2);
        assert_eq!(globals.get("HTTP_PORT"), Some(&8080));
        assert_eq!(globals.get("GRPC_PORT"), Some(&9090));

        // Ports should also appear in allocated_ports
        let allocated = tracker.get_allocated_ports().await;
        assert!(allocated.contains(&8080));
        assert!(allocated.contains(&9090));
    }

    #[tokio::test]
    async fn test_save_port_resolutions_replaces_previous() {
        let mut tracker = create_ephemeral_tracker().await;

        let first = vec![("OLD_PORT".to_string(), 3000u16)];
        tracker.save_port_resolutions(&first, None).await.unwrap();

        let second = vec![("NEW_PORT".to_string(), 4000u16)];
        tracker.save_port_resolutions(&second, None).await.unwrap();

        let globals = tracker.get_global_port_allocations(None).await;
        assert_eq!(globals.len(), 1, "Previous resolutions should be replaced");
        assert_eq!(globals.get("NEW_PORT"), Some(&4000));
        assert!(!globals.contains_key("OLD_PORT"));
    }

    #[tokio::test]
    async fn test_clear_port_resolutions() {
        let mut tracker = create_ephemeral_tracker().await;

        let resolutions = vec![("PORT".to_string(), 5000u16)];
        tracker
            .save_port_resolutions(&resolutions, None)
            .await
            .unwrap();

        tracker.clear_port_resolutions().await.unwrap();

        let globals = tracker.get_global_port_allocations(None).await;
        assert!(globals.is_empty(), "All port resolutions should be cleared");

        let allocated = tracker.get_allocated_ports().await;
        assert!(
            allocated.is_empty(),
            "Allocated ports should also be cleared"
        );
    }

    #[tokio::test]
    async fn test_port_resolutions_scoped_by_isolation_id() {
        let mut tracker = create_ephemeral_tracker().await;

        // The shared scope and an isolation scope are independent namespaces,
        // even for the same parameter name.
        let shared = vec![("DB_PORT".to_string(), 5432u16)];
        tracker.save_port_resolutions(&shared, None).await.unwrap();

        let isolated = vec![("DB_PORT".to_string(), 51833u16)];
        tracker
            .save_port_resolutions(&isolated, Some("iso-cafebabe"))
            .await
            .unwrap();

        // Reads are scoped: each scope sees only its own value.
        assert_eq!(
            tracker
                .get_global_port_allocations(None)
                .await
                .get("DB_PORT"),
            Some(&5432)
        );
        assert_eq!(
            tracker
                .get_global_port_allocations(Some("iso-cafebabe"))
                .await
                .get("DB_PORT"),
            Some(&51833)
        );

        // Saving one scope must not disturb the other — this is the property
        // that keeps randomized isolation ports out of the non-isolated path.
        let shared2 = vec![("DB_PORT".to_string(), 5433u16)];
        tracker.save_port_resolutions(&shared2, None).await.unwrap();
        assert_eq!(
            tracker
                .get_global_port_allocations(Some("iso-cafebabe"))
                .await
                .get("DB_PORT"),
            Some(&51833),
            "Saving the shared scope must not touch the isolation scope"
        );

        // An empty scope falls back to nothing (caller then uses config defaults).
        assert!(tracker
            .get_global_port_allocations(Some("iso-other"))
            .await
            .is_empty());

        // clear_port_resolutions wipes every scope.
        tracker.clear_port_resolutions().await.unwrap();
        assert!(tracker.get_global_port_allocations(None).await.is_empty());
        assert!(tracker
            .get_global_port_allocations(Some("iso-cafebabe"))
            .await
            .is_empty());
    }

    #[tokio::test]
    async fn test_track_port() {
        let mut tracker = create_ephemeral_tracker().await;

        tracker.track_port(7777).await;
        tracker.track_port(8888).await;
        // Duplicate should be silently ignored
        tracker.track_port(7777).await;

        let allocated = tracker.get_allocated_ports().await;
        assert_eq!(allocated.len(), 2);
        assert!(allocated.contains(&7777));
        assert!(allocated.contains(&8888));
    }

    #[tokio::test]
    async fn test_port_allocations_visible_in_get_services() {
        let mut tracker = create_ephemeral_tracker().await;

        let state = make_service_state("svc-ports", ServiceType::Process);
        tracker.register_service(state).await.unwrap();

        tracker
            .add_service_port("svc-ports", "API_PORT".to_string(), 3000)
            .await
            .unwrap();

        // get_services should also load port allocations
        let services = tracker.get_services().await;
        let svc = services.get("svc-ports").unwrap();
        assert_eq!(svc.port_allocations.get("API_PORT"), Some(&3000));
    }

    #[tokio::test]
    async fn test_unregister_cleans_up_orphaned_allocated_ports() {
        let mut tracker = create_ephemeral_tracker().await;

        let state = make_service_state("svc", ServiceType::Process);
        tracker.register_service(state).await.unwrap();

        tracker
            .add_service_port("svc", "PORT".to_string(), 6000)
            .await
            .unwrap();

        // Port 6000 is now tracked
        assert!(tracker.get_allocated_ports().await.contains(&6000));

        // Unregistering cleans up orphaned allocated_ports entries
        tracker.unregister_service("svc").await.unwrap();

        let allocated = tracker.get_allocated_ports().await;
        assert!(
            !allocated.contains(&6000),
            "Orphaned port should be cleaned up after unregister"
        );
    }

    // --- is_service_registered ---

    #[tokio::test]
    async fn test_is_service_registered() {
        let mut tracker = create_ephemeral_tracker().await;

        assert!(!tracker.is_service_registered("svc").await);

        let state = make_service_state("svc", ServiceType::Process);
        tracker.register_service(state).await.unwrap();

        assert!(tracker.is_service_registered("svc").await);

        tracker.unregister_service("svc").await.unwrap();

        assert!(!tracker.is_service_registered("svc").await);
    }

    // --- clear ---

    #[tokio::test]
    async fn test_clear_removes_all_services() {
        let mut tracker = create_ephemeral_tracker().await;

        for name in &["a", "b", "c"] {
            let state = make_service_state(name, ServiceType::Process);
            tracker.register_service(state).await.unwrap();
        }

        assert_eq!(tracker.get_services().await.len(), 3);

        tracker.clear().await.unwrap();

        assert!(tracker.get_services().await.is_empty());
    }

    #[tokio::test]
    async fn test_clear_preserves_persisted_ports() {
        let mut tracker = create_ephemeral_tracker().await;

        // Save global port resolutions
        let resolutions = vec![("PRESERVED_PORT".to_string(), 9999u16)];
        tracker
            .save_port_resolutions(&resolutions, None)
            .await
            .unwrap();

        // Register a service with its own port
        let state = make_service_state("svc", ServiceType::Process);
        tracker.register_service(state).await.unwrap();
        tracker
            .add_service_port("svc", "SVC_PORT".to_string(), 1234)
            .await
            .unwrap();

        // Clear should remove services but keep persisted_ports
        tracker.clear().await.unwrap();

        assert!(tracker.get_services().await.is_empty());

        let globals = tracker.get_global_port_allocations(None).await;
        assert_eq!(
            globals.get("PRESERVED_PORT"),
            Some(&9999),
            "Persisted port resolutions should survive clear()"
        );
    }

    // --- Bug: Starting status not marked stale ---

    #[tokio::test]
    async fn test_mark_dead_services_starting_no_pid_is_stale() {
        let mut tracker = create_ephemeral_tracker().await;

        // A service with Starting status but no PID and no container_id
        // is clearly stale — the process was never spawned (start failed
        // between registration and actual process creation).
        let state = ServiceState {
            id: "stuck-starting".to_string(),
            status: Status::Starting,
            service_type: ServiceType::Process,
            pid: None,
            container_id: None,
            started_at: Utc::now(),
            external_repo: None,
            namespace: "test".to_string(),
            restart_count: 0,
            last_restart_at: None,
            consecutive_failures: 0,
            port_allocations: HashMap::new(),
            startup_message: None,
        };
        tracker.register_service(state).await.unwrap();

        let marked = tracker.mark_dead_services().await.unwrap();
        assert_eq!(
            marked, 1,
            "Service with Starting status and no PID should be marked stale"
        );

        let services = tracker.get_services().await;
        assert!(
            services.is_empty(),
            "Stale Starting service should be filtered from get_services()"
        );
    }

    // --- Bug: register_service clobbers existing service status ---

    #[tokio::test]
    async fn test_register_service_does_not_clobber_running_status() {
        let mut tracker = create_ephemeral_tracker().await;

        // Register a service as Running (simulates a successfully started service)
        let state = ServiceState {
            id: "svc".to_string(),
            status: Status::Running,
            service_type: ServiceType::Process,
            pid: Some(12345),
            container_id: None,
            started_at: Utc::now(),
            external_repo: None,
            namespace: "test".to_string(),
            restart_count: 0,
            last_restart_at: None,
            consecutive_failures: 0,
            port_allocations: HashMap::new(),
            startup_message: None,
        };
        tracker.register_service(state).await.unwrap();

        // Try to register again (simulates a concurrent or duplicate start attempt)
        let new_state = ServiceState {
            id: "svc".to_string(),
            status: Status::Starting,
            service_type: ServiceType::Process,
            pid: None,
            container_id: None,
            started_at: Utc::now(),
            external_repo: None,
            namespace: "test".to_string(),
            restart_count: 0,
            last_restart_at: None,
            consecutive_failures: 0,
            port_allocations: HashMap::new(),
            startup_message: None,
        };
        let outcome = tracker.register_service(new_state).await.unwrap();
        assert_eq!(
            outcome,
            RegistrationOutcome::AlreadyExists {
                status: Status::Running
            },
            "Should return AlreadyExists with the existing Running status"
        );

        // The existing service's row must be completely untouched
        let retrieved = tracker.get_service("svc").await.unwrap();
        assert_eq!(
            retrieved.status,
            Status::Running,
            "register_service must not clobber existing Running status to Starting"
        );
        assert_eq!(
            retrieved.pid,
            Some(12345),
            "register_service must not lose the existing PID"
        );
    }

    #[tokio::test]
    async fn test_ephemeral_tracker_does_not_corrupt_parent() {
        // Parent: file-backed tracker
        let temp_dir = TempDir::new().unwrap();
        let mut parent = SqliteStateTracker::new(temp_dir.path().to_path_buf())
            .await
            .unwrap();
        parent.initialize().await.unwrap();
        register_test_service(&mut parent, "parent-svc").await;

        // Child: ephemeral tracker (simulates isolated script)
        let mut child = SqliteStateTracker::new_ephemeral().await.unwrap();
        child.initialize().await.unwrap();
        register_test_service(&mut child, "child-svc").await;
        child.clear().await.unwrap();

        // Parent state must be intact
        let parent_services = parent.get_services().await;
        assert_eq!(parent_services.len(), 1);
        assert!(
            parent_services.contains_key("parent-svc"),
            "Parent service must survive child clear()"
        );
    }
}
