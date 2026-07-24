use super::types::{DesiredState, LockFile, RegistrationOutcome, ServiceState};
use crate::config::ServiceType;
use crate::error::{Error, Result, validate_pid_for_check};
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

const DB_FILE_NAME: &str = "lock.db";
const LOCK_FILE_NAME: &str = ".lock";
const SCHEMA_VERSION: i32 = 9;

mod isolation;
mod migrations;
mod ports;
mod restart_history;
mod service_crud;

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
        && output.status.success()
    {
        let comm = String::from_utf8_lossy(&output.stdout);
        let name = comm.trim();
        // ps may return the full path (e.g. /usr/local/bin/fed) or just "fed"
        let basename = name.rsplit('/').next().unwrap_or(name);
        return basename.starts_with("fed");
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
        // Create .fed directory (with its self-managed .gitignore) if needed
        let fed_dir = crate::fed_dir::ensure_fed_dir(&work_dir)?;

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

    /// Create an unlocked state tracker for the supervisor.
    ///
    /// Points at the same on-disk `lock.db` as [`SqliteStateTracker::new`]
    /// (same WAL/pragma setup) but **skips `try_acquire_lock` entirely** — no
    /// `.fed/.lock` file handle is held. A supervisor that holds `.fed/.lock`
    /// for its entire (potentially hours/days) lifetime would push every
    /// short-lived `fed` CLI invocation in the same directory into the
    /// degraded "another fed instance ... proceeding anyway" path forever
    /// (`try_acquire_lock`'s fallback below). The supervisor coordinates with
    /// those short-lived invocations purely through SQLite's own WAL
    /// concurrency instead — already the actual data-safety mechanism;
    /// `.fed/.lock` has always been a courtesy warning layer on top. The
    /// supervisor's own single-instance enforcement is the separate
    /// `.fed/supervisor.lock` file (see `07-supervisor.md` Design §1),
    /// unrelated to this tracker.
    ///
    /// Unlike [`SqliteStateTracker::new_ephemeral`] (in-memory, no `.fed/`
    /// directory at all — for isolated child orchestrators that must not
    /// touch the parent's persistent state), this variant must still point
    /// at the real on-disk `lock.db` so it observes the same state every
    /// other `fed` invocation in the directory does.
    pub async fn new_for_supervisor(work_dir: PathBuf) -> Result<Self> {
        let fed_dir = crate::fed_dir::ensure_fed_dir(&work_dir)?;
        let db_path = fed_dir.join(DB_FILE_NAME);
        let work_dir_str = work_dir.to_string_lossy().to_string();

        let conn = Connection::open(&db_path).await?;

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
            lock_file: None,
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
                    "SELECT id, status, service_type, pid, container_id, started_at, external_repo, namespace, restart_count, last_restart_at, consecutive_failures, startup_message, desired_state, native_restart_enabled FROM services"
                )?;

                let services_iter = stmt.query_map([], |row| {
                    let id: String = row.get(0)?;
                    let status_str: String = row.get(1)?;
                    let service_type_str: String = row.get(2)?;
                    let started_at_str: String = row.get(5)?;
                    let last_restart_str: Option<String> = row.get(9)?;
                    let desired_state_str: String = row.get(12)?;
                    let native_restart_enabled: bool = row.get(13)?;

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
                            desired_state: desired_state_str
                                .parse::<DesiredState>()
                                .unwrap_or(DesiredState::Running),
                            native_restart_enabled,
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
                    if let Some(pid) = service_state.pid
                        && (pid > i32::MAX as u32 || pid == 0) {
                            warn!(
                                "Service '{}' has invalid PID {} (exceeds i32::MAX or is 0), skipping",
                                service_id, pid
                            );
                            return false;
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
        self.initialize_for_supervisor().await?;
        Ok(())
    }

    /// Same as [`SqliteStateTracker::initialize`], but returns the ids
    /// [`SqliteStateTracker::mark_dead_services`] just staled on this pass
    /// instead of discarding them.
    ///
    /// Plain `initialize()` (above) can't expose this — the real call chain
    /// is `initialize()` -> `validate_and_cleanup()` -> `mark_dead_services()`,
    /// and until now the outer two layers both returned plain `Result<()>`,
    /// discarding the innermost function's `Vec<String>`. The supervisor
    /// attach path (`Orchestrator::initialize_supervisor`,
    /// `07-supervisor.md` Design §1) needs exactly this: which rows just
    /// went stale, before they become invisible to `get_services()`, so it
    /// can immediately re-derive whether each one should be restarted
    /// (crashed while unsupervised, `desired_state == 'running'`) or left
    /// alone (`desired_state == 'stopped'`).
    ///
    /// A brand-new `.fed/` directory has nothing to stale — this returns an
    /// empty vec on the fresh-schema branch, matching `initialize()`'s own
    /// no-special-casing behavior for that case.
    pub async fn initialize_for_supervisor(&mut self) -> Result<Vec<String>> {
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
            Ok(Vec::new())
        } else {
            debug!("Loading existing SQLite state");
            // Run migrations if needed
            self.run_migrations().await?;
            self.validate_and_cleanup().await
        }
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

    /// Validate existing state and cleanup stale services.
    ///
    /// Returns the ids [`SqliteStateTracker::mark_dead_services`] just
    /// staled on this pass (see [`SqliteStateTracker::initialize_for_supervisor`]
    /// for why this needs to propagate all the way up); `initialize()`
    /// itself discards it.
    async fn validate_and_cleanup(&mut self) -> Result<Vec<String>> {
        // Mark dead services as stale (does not delete — purge_stale_services does that)
        let newly_stale = self.mark_dead_services().await?;

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

        Ok(newly_stale)
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;
    #[cfg(unix)]
    use {
        fs2::FileExt,
        nix::sys::wait::waitpid,
        nix::unistd::{ForkResult, Pid, fork, pipe, read, write},
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

    pub(crate) mod test_support {
        use super::super::*;
        use tempfile::TempDir;

        /// Create a test state tracker with a temporary directory
        pub(crate) async fn create_test_tracker() -> (SqliteStateTracker, TempDir) {
            let temp_dir = TempDir::new().unwrap();
            let mut tracker = SqliteStateTracker::new(temp_dir.path().to_path_buf())
                .await
                .unwrap();
            tracker.initialize().await.unwrap();
            (tracker, temp_dir)
        }

        /// Register a test service for circuit breaker testing
        pub(crate) async fn register_test_service(
            tracker: &mut SqliteStateTracker,
            service_id: &str,
        ) {
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
                desired_state: DesiredState::Running,
                native_restart_enabled: false,
            };
            tracker.register_service(state).await.unwrap();
        }

        /// Register a service in Stopped state for transition testing
        pub(crate) async fn register_stopped_service(
            tracker: &mut SqliteStateTracker,
            service_id: &str,
        ) {
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
                desired_state: DesiredState::Running,
                native_restart_enabled: false,
            };
            tracker.register_service(state).await.unwrap();
        }

        /// Create an ephemeral test tracker (no filesystem, no lock file)
        pub(crate) async fn create_ephemeral_tracker() -> SqliteStateTracker {
            let mut tracker = SqliteStateTracker::new_ephemeral().await.unwrap();
            tracker.initialize().await.unwrap();
            tracker
        }

        /// Build a ServiceState with the given id and service type
        pub(crate) fn make_service_state(id: &str, stype: ServiceType) -> ServiceState {
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
                desired_state: DesiredState::Running,
                native_restart_enabled: false,
            }
        }
    }
    use test_support::*;

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

    // --- new_for_supervisor: unlocked concurrent access (07-supervisor.md Design §1) ---

    /// A `new_for_supervisor` tracker must never hold `.fed/.lock` — a normal,
    /// locked `SqliteStateTracker::new` on the *same* directory must be able
    /// to acquire the advisory lock cleanly while the supervisor tracker is
    /// alive and initialized, with no "another fed instance ... proceeding
    /// anyway" degraded path triggered. This is the direct regression test
    /// for hole #4's locking half: a supervisor built via the normal `new()`
    /// constructor would hold `.fed/.lock` for its entire lifetime, pushing
    /// every subsequent `fed` invocation into the degraded path forever.
    #[tokio::test]
    async fn test_supervisor_tracker_does_not_hold_advisory_lock() {
        let temp_dir = TempDir::new().unwrap();
        let lock_path = temp_dir.path().join(LOCK_FILE_NAME);

        // Bring up the "supervisor" first, exactly as initialize_supervisor
        // would, and leave it alive for the rest of the test.
        let mut supervisor = SqliteStateTracker::new_for_supervisor(temp_dir.path().to_path_buf())
            .await
            .unwrap();
        supervisor.initialize_for_supervisor().await.unwrap();
        assert!(
            supervisor.lock_file.is_none(),
            "supervisor tracker must never hold a lock file handle"
        );

        // The supervisor's own construction/initialization must never touch
        // .fed/.lock at all — it doesn't call try_acquire_lock, so the file
        // shouldn't even exist yet.
        assert!(
            !lock_path.exists(),
            "supervisor tracker must never create or write .fed/.lock"
        );

        // A short-lived `fed status`-shaped invocation, using the real
        // locked constructor, must acquire the advisory lock cleanly.
        let normal = SqliteStateTracker::new(temp_dir.path().to_path_buf())
            .await
            .unwrap();
        assert!(
            normal.lock_file.is_some(),
            "a normal tracker must still acquire .fed/.lock when nothing else holds it \
             (the supervisor must not be silently holding it)"
        );

        // Concurrent access from both trackers must not corrupt state:
        // register through the supervisor tracker, read back through the
        // normal one, and vice versa.
        register_test_service(&mut supervisor, "supervised-svc").await;
        let seen_by_normal = normal.get_services().await;
        assert!(
            seen_by_normal.contains_key("supervised-svc"),
            "the normal tracker must see writes made through the unlocked supervisor tracker"
        );
    }

    /// Several `fed status`/`fed logs`-shaped invocations run in a loop while
    /// a supervisor tracker is alive must all acquire the advisory lock
    /// without warning about a competing instance — repeats the single-shot
    /// check above across multiple sequential acquisitions, matching the
    /// plan's "lock non-monopolization" test description.
    #[tokio::test]
    async fn test_supervisor_tracker_allows_many_sequential_normal_trackers() {
        let temp_dir = TempDir::new().unwrap();

        let mut supervisor = SqliteStateTracker::new_for_supervisor(temp_dir.path().to_path_buf())
            .await
            .unwrap();
        supervisor.initialize_for_supervisor().await.unwrap();

        for i in 0..5 {
            // The previous iteration's lock releases on drop, but under a
            // loaded parallel test run that release can land a beat late.
            // Retry instead of asserting on drop timing. The window is
            // deliberately generous (10s): 2s was observed to flake on
            // loaded GitHub Actions macOS runners.
            let mut acquired = false;
            for _ in 0..100 {
                let normal = SqliteStateTracker::new(temp_dir.path().to_path_buf())
                    .await
                    .unwrap();
                if normal.lock_file.is_some() {
                    acquired = true;
                    break;
                }
                drop(normal);
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            }
            assert!(
                acquired,
                "invocation {} should acquire .fed/.lock while the supervisor is alive",
                i
            );
        }
    }
}
