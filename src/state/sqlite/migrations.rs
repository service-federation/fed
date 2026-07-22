// Split from sqlite.rs (see git history before this commit for pre-split blame).
use super::*;

impl SqliteStateTracker {
    /// Run database migrations to bring schema up to current version
    pub(super) async fn run_migrations(&self) -> Result<()> {
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

        // Migration from v6 to v7: Add desired_state column
        if current_version < 7 {
            self.migrate_v6_to_v7().await?;
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

    /// Migration v6 -> v7: Add `desired_state` column to `services`.
    ///
    /// Persists whether a service is *meant* to be running, independent of
    /// `status` (last-observed reality). Every stop path writes `'stopped'`
    /// here before sending any kill signal; registration defaults new rows to
    /// `'running'`. See `07-supervisor.md` Design §1 — this is the
    /// cross-process signal a future restart-policy supervisor consults
    /// instead of an in-process manager object it never touched.
    async fn migrate_v6_to_v7(&self) -> Result<()> {
        debug!("Running migration v6 -> v7: Adding desired_state column");

        self.conn
            .call(|conn: &mut rusqlite::Connection| -> tokio_rusqlite::Result<()> {
                let tx = conn.transaction()?;

                let already_applied: bool = tx
                    .query_row(
                        "SELECT COUNT(*) > 0 FROM schema_version WHERE version = 7",
                        [],
                        |row| row.get(0),
                    )
                    .unwrap_or(false);

                if already_applied {
                    return Ok(());
                }

                let has_column: bool = tx
                    .query_row(
                        "SELECT COUNT(*) > 0 FROM pragma_table_info('services') WHERE name = 'desired_state'",
                        [],
                        |row| row.get(0),
                    )
                    .unwrap_or(false);

                if !has_column {
                    tx.execute(
                        "ALTER TABLE services ADD COLUMN desired_state TEXT NOT NULL DEFAULT 'running'",
                        [],
                    )?;
                }

                tx.execute(
                    "INSERT INTO schema_version (version, applied_at) VALUES (7, datetime('now'))",
                    [],
                )?;

                tx.commit()?;
                Ok(())
            })
            .await?;

        info!("Migration v6 -> v7 completed successfully");
        Ok(())
    }

    /// Create database schema
    pub(super) async fn create_schema(&self) -> Result<()> {
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
                    startup_message TEXT,
                    desired_state TEXT NOT NULL DEFAULT 'running'
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
}

#[cfg(test)]
mod desired_state_migration_tests {
    use super::*;
    use tempfile::TempDir;

    /// Hand-write a `lock.db` matching the exact v6 schema (pre-`desired_state`,
    /// see the pre-migration `create_schema` text in git history) so migrating
    /// it forward exercises the real ALTER TABLE path rather than a
    /// freshly-created (already up to date) database.
    fn write_legacy_v6_db(fed_dir: &std::path::Path, service_id: &str, status: &str) {
        std::fs::create_dir_all(fed_dir).unwrap();
        let db_path = fed_dir.join("lock.db");
        let conn = rusqlite::Connection::open(&db_path).unwrap();

        conn.execute_batch(
            r#"
            CREATE TABLE schema_version (
                version INTEGER PRIMARY KEY,
                applied_at TEXT NOT NULL
            );

            CREATE TABLE lock_file (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                fed_pid INTEGER NOT NULL,
                work_dir TEXT NOT NULL,
                started_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

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

            CREATE TABLE port_allocations (
                service_id TEXT NOT NULL,
                parameter_name TEXT NOT NULL,
                port INTEGER NOT NULL,
                PRIMARY KEY (service_id, parameter_name)
            );

            CREATE TABLE allocated_ports (
                port INTEGER PRIMARY KEY,
                allocated_at TEXT NOT NULL
            );

            CREATE TABLE persisted_ports (
                param_name TEXT NOT NULL,
                port INTEGER NOT NULL,
                source TEXT NOT NULL,
                allocated_at TEXT NOT NULL,
                isolation_id TEXT NOT NULL DEFAULT '',
                PRIMARY KEY (param_name, isolation_id)
            );

            CREATE TABLE restart_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                service_id TEXT NOT NULL,
                restarted_at TEXT NOT NULL
            );

            CREATE TABLE project_settings (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL,
                updated_at TEXT NOT NULL DEFAULT (datetime('now'))
            );
            "#,
        )
        .unwrap();

        conn.execute(
            "INSERT INTO schema_version (version, applied_at) VALUES (6, datetime('now'))",
            [],
        )
        .unwrap();

        conn.execute(
            "INSERT INTO lock_file (id, fed_pid, work_dir, started_at, updated_at) VALUES (1, 999999, 'legacy', datetime('now'), datetime('now'))",
            [],
        )
        .unwrap();

        conn.execute(
            "INSERT INTO services (id, status, service_type, pid, container_id, started_at, external_repo, namespace, restart_count, last_restart_at, consecutive_failures, circuit_breaker_open_until, startup_message)
             VALUES (?1, ?2, 'process', NULL, NULL, datetime('now'), NULL, 'root', 0, NULL, 0, NULL, NULL)",
            rusqlite::params![service_id, status],
        )
        .unwrap();
    }

    #[tokio::test]
    async fn migrate_v6_to_v7_adds_desired_state_column_defaulting_to_running() {
        let temp_dir = TempDir::new().unwrap();
        let fed_dir = temp_dir.path().join(".fed");
        write_legacy_v6_db(&fed_dir, "legacy-svc", "stopped");

        let mut tracker = SqliteStateTracker::new(temp_dir.path().to_path_buf())
            .await
            .unwrap();
        tracker
            .initialize()
            .await
            .expect("migrating a v6 db to v7 should succeed");

        // The pre-existing row must have gained the new column with the
        // documented default, without anything explicitly writing it.
        let state = tracker
            .get_service("legacy-svc")
            .await
            .expect("legacy row should survive the migration");
        assert_eq!(
            state.desired_state,
            DesiredState::Running,
            "column added by ALTER TABLE ... DEFAULT 'running' should backfill existing rows"
        );

        // Schema version must have advanced all the way to current.
        let conn = rusqlite::Connection::open(fed_dir.join("lock.db")).unwrap();
        let version: i32 = conn
            .query_row("SELECT MAX(version) FROM schema_version", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(version, SCHEMA_VERSION);
        assert_eq!(version, 7);
    }

    #[tokio::test]
    async fn migrate_v6_to_v7_is_idempotent_on_rerun() {
        let temp_dir = TempDir::new().unwrap();
        let fed_dir = temp_dir.path().join(".fed");
        write_legacy_v6_db(&fed_dir, "legacy-svc", "running");

        // First migration.
        {
            let mut tracker = SqliteStateTracker::new(temp_dir.path().to_path_buf())
                .await
                .unwrap();
            tracker.initialize().await.unwrap();
        }

        // Re-opening (simulating a second `fed` invocation against an
        // already-migrated db) must not error on the already-applied guard
        // or the already-existing column.
        let mut tracker = SqliteStateTracker::new(temp_dir.path().to_path_buf())
            .await
            .unwrap();
        tracker
            .initialize()
            .await
            .expect("re-running migrations against an already-migrated db should be a no-op");

        let state = tracker.get_service("legacy-svc").await.unwrap();
        assert_eq!(state.desired_state, DesiredState::Running);
    }

    /// A brand-new `.fed/` directory (the `create_schema` path, not the
    /// migration path) must get `desired_state` directly — the "two touch
    /// points" note in `07-supervisor.md` Design §1: forgetting to also add
    /// the column to `create_schema` would leave fresh projects one
    /// migration behind their own schema version.
    #[tokio::test]
    async fn fresh_database_has_desired_state_column_without_migrating() {
        let temp_dir = TempDir::new().unwrap();
        let mut tracker = SqliteStateTracker::new(temp_dir.path().to_path_buf())
            .await
            .unwrap();
        tracker.initialize().await.unwrap();

        let state = crate::state::ServiceState::new(
            "fresh-svc".to_string(),
            crate::config::ServiceType::Process,
            "root".to_string(),
        );
        tracker.register_service(state).await.unwrap();

        let retrieved = tracker.get_service("fresh-svc").await.unwrap();
        assert_eq!(retrieved.desired_state, DesiredState::Running);

        let conn = rusqlite::Connection::open(temp_dir.path().join(".fed/lock.db")).unwrap();
        let version: i32 = conn
            .query_row("SELECT MAX(version) FROM schema_version", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(version, SCHEMA_VERSION);
    }
}
