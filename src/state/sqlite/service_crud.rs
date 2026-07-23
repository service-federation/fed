// Split from sqlite.rs (see git history before this commit for pre-split blame).
use super::*;

impl SqliteStateTracker {
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
    pub(super) fn status_is_stale(status: &str) -> bool {
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
        let desired_state = service_state.desired_state.to_string();
        let native_restart_enabled = service_state.native_restart_enabled;

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
                    // Service already registered — leave its row (including
                    // desired_state) untouched.
                    tx.commit()?;
                    let status = status_str.parse::<Status>().unwrap_or(Status::Starting);
                    return Ok(RegistrationOutcome::AlreadyExists { status });
                }

                // Insert new service. desired_state is written explicitly
                // (defaulting to 'running' via ServiceState::new) rather than
                // relying solely on the column's SQL DEFAULT, so a fresh
                // registration always marks intent as running.
                // native_restart_enabled is likewise written explicitly at
                // registration time — captured once per fresh row, same as
                // startup_message, since an already-registered row is left
                // untouched above.
                tx.execute(
                    "INSERT INTO services (id, status, service_type, pid, container_id, started_at, external_repo, namespace, restart_count, last_restart_at, consecutive_failures, startup_message, desired_state, native_restart_enabled)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14)",
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
                        &desired_state,
                        native_restart_enabled,
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

    /// Set `status` only if the row's current status is one of
    /// `allowed_from`, atomically (single SQL UPDATE, so it holds across
    /// processes too). Returns whether the transition applied.
    ///
    /// This exists for writers that observed a status earlier and must not
    /// clobber a transition that happened since — e.g. the startup health
    /// wait writing `Healthy` must not overwrite a concurrent `fed stop`'s
    /// `Stopping`/`Stopped`.
    pub async fn try_transition_service_status(
        &mut self,
        service_id: &str,
        allowed_from: &[Status],
        to: Status,
    ) -> Result<bool> {
        let service_id = service_id.to_string();
        let to = to.to_string();
        let from: Vec<String> = allowed_from.iter().map(|s| s.to_string()).collect();

        let rows = self
            .with_transaction(move |tx| {
                let placeholders = (0..from.len())
                    .map(|i| format!("?{}", i + 3))
                    .collect::<Vec<_>>()
                    .join(", ");
                let sql = format!(
                    "UPDATE services SET status = ?1 WHERE id = ?2 AND status IN ({})",
                    placeholders
                );
                let mut params: Vec<&dyn rusqlite::ToSql> = vec![&to, &service_id];
                for status in &from {
                    params.push(status);
                }
                tx.execute(&sql, params.as_slice())
            })
            .await?;

        Ok(rows > 0)
    }

    /// Set a single service's persisted desired state (running/stopped).
    ///
    /// This is the intent signal every stop path must write **before** any
    /// kill signal is sent — see `07-supervisor.md` Design §1. Unlike
    /// `status`, this is never overwritten by health-check observations; it
    /// only changes on an explicit stop or (re-)registration.
    #[must_use = "ignoring this result may cause state loss - the desired_state update will not be persisted"]
    pub async fn set_desired_state(
        &mut self,
        service_id: &str,
        desired_state: DesiredState,
    ) -> Result<()> {
        let service_id = service_id.to_string();
        let service_id_for_tx = service_id.clone();
        let desired_state = desired_state.to_string();

        let rows = self
            .with_transaction(move |tx| {
                tx.execute(
                    "UPDATE services SET desired_state = ?1 WHERE id = ?2",
                    rusqlite::params![&desired_state, &service_id_for_tx],
                )
            })
            .await?;

        if rows == 0 {
            return Err(Error::ServiceNotFound(service_id));
        }

        Ok(())
    }

    /// Set the persisted desired state for every currently-registered
    /// service in a single transaction.
    ///
    /// Used by whole-project `fed stop` to quiesce every service's intent to
    /// `Stopped` in one batch *before* any kill signal goes out — closing the
    /// race window where a per-service interleaved write-then-kill loop could
    /// leave some rows still `Running` while their processes are already
    /// being torn down (`07-supervisor.md` Design §1).
    #[must_use = "ignoring this result may cause state loss - the desired_state update will not be persisted"]
    pub async fn set_all_desired_state(&mut self, desired_state: DesiredState) -> Result<()> {
        let desired_state = desired_state.to_string();

        self.with_transaction(move |tx| {
            tx.execute(
                "UPDATE services SET desired_state = ?1",
                rusqlite::params![&desired_state],
            )
        })
        .await?;

        Ok(())
    }

    /// Whether a service's persisted `desired_state` is `Running`.
    ///
    /// This is the signal the supervisor consults **instead of** any
    /// in-process manager's `Status` before ever attempting a restart
    /// (`07-supervisor.md` Design §1) — a separate `fed stop` process never
    /// touches the supervisor's manager objects, but it does write this
    /// column, so gating on it (rather than `manager.status()`) is what
    /// makes `fed stop` reliably prevent resurrection across processes.
    ///
    /// Returns `false` if the row doesn't exist. A service with no
    /// persisted row has nothing for the supervisor to protect, so
    /// "unknown" and "not desired-running" collapse to the same safe
    /// answer here — unlike most other lookups in this module, there is no
    /// case where a caller needs to distinguish "missing" from "stopped".
    pub async fn is_desired_running(&self, service_id: &str) -> bool {
        let service_id = service_id.to_string();

        self.conn
            .call(
                move |conn: &mut rusqlite::Connection| -> tokio_rusqlite::Result<Option<String>> {
                    Ok(conn
                        .query_row(
                            "SELECT desired_state FROM services WHERE id = ?1",
                            rusqlite::params![&service_id],
                            |row| row.get(0),
                        )
                        .optional()?)
                },
            )
            .await
            .ok()
            .flatten()
            .map(|s| {
                s.parse::<DesiredState>().unwrap_or(DesiredState::Running) == DesiredState::Running
            })
            .unwrap_or(false)
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

        // A new PID means a new process: refresh started_at so PID-reuse
        // guards (validate_pid_start_time) compare against the right epoch.
        let started_at = Utc::now().to_rfc3339();
        let rows = self
            .with_transaction(move |tx| {
                tx.execute(
                    "UPDATE services SET pid = ?1, started_at = ?2 WHERE id = ?3",
                    rusqlite::params![pid, &started_at, &service_id_for_tx],
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

    /// Get all registered services
    pub async fn get_services(&self) -> HashMap<String, ServiceState> {
        match self.conn.call(|conn: &mut rusqlite::Connection| {
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
                        last_restart_at: last_restart_str.and_then(|s| s.parse::<DateTime<Utc>>().ok()),
                        consecutive_failures: row.get(10)?,
                        port_allocations: HashMap::new(), // Will be populated below
                        startup_message: row.get(11)?,
                        desired_state: desired_state_str.parse::<DesiredState>().unwrap_or(DesiredState::Running),
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

            // Validate and remove services with invalid PIDs
            let mut invalid_service_ids = Vec::new();
            services.retain(|service_id, service_state| {
                if let Some(pid) = service_state.pid
                    && (pid > i32::MAX as u32 || pid == 0) {
                        warn!(
                            "Service '{}' has invalid PID {} (exceeds i32::MAX or is 0), removing from state",
                            service_id, pid
                        );
                        invalid_service_ids.push(service_id.clone());
                        return false;
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
                // A fresh directory has a state DB with no tables yet — that's
                // legitimately empty state, not a condition to warn about.
                if e.to_string().contains("no such table") {
                    tracing::debug!("State DB has no tables yet; treating as empty");
                } else {
                    warn!("Failed to get services: {}", e);
                }
                HashMap::new()
            }
        }
    }

    /// Get specific service state
    pub async fn get_service(&self, service_id: &str) -> Option<ServiceState> {
        let service_id = service_id.to_string();

        self.conn.call(move |conn: &mut rusqlite::Connection| -> tokio_rusqlite::Result<Option<ServiceState>> {
            let service = match conn.query_row(
                "SELECT id, status, service_type, pid, container_id, started_at, external_repo, namespace, restart_count, last_restart_at, consecutive_failures, startup_message, desired_state, native_restart_enabled FROM services WHERE id = ?1",
                rusqlite::params![&service_id],
                |row| {
                    let id: String = row.get(0)?;
                    let status_str: String = row.get(1)?;
                    let service_type_str: String = row.get(2)?;
                    let started_at_str: String = row.get(5)?;
                    let last_restart_str: Option<String> = row.get(9)?;
                    let desired_state_str: String = row.get(12)?;
                    let native_restart_enabled: bool = row.get(13)?;

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
                        desired_state: desired_state_str.parse::<DesiredState>().unwrap_or(DesiredState::Running),
                        native_restart_enabled,
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

    /// Consecutive failed liveness checks a native-restart-enabled service
    /// (`ServiceState::native_restart_enabled`) tolerates before being marked
    /// `'stale'` — `07-supervisor.md` Design §3's state-reconciliation gap
    /// mitigation. Docker's own restart backoff after a container-exit event
    /// is brief; this spans the same order of magnitude rather than an
    /// arbitrary long timeout, so a service genuinely gone (not just
    /// mid-backoff) is still caught within a few `fed` invocations.
    const STALE_GRACE_THRESHOLD: u32 = 3;

    /// Mark dead/stale services in state without deleting them.
    ///
    /// Sets status to `stale` so that port_allocations remain readable
    /// until [`SqliteStateTracker::purge_stale_services`] is called. This enables callers to
    /// collect managed port information before data is deleted.
    ///
    /// Returns the ids of the services just marked stale (not the ids of
    /// services that were already stale from a previous pass) — needed by
    /// the supervisor attach path (`Orchestrator::initialize_supervisor`,
    /// `07-supervisor.md` Design §1) to notice a crashed, restart-worthy
    /// service *before* it becomes permanently invisible to `get_services()`.
    /// Plain `initialize()` discards this value (see `validate_and_cleanup`).
    ///
    /// Services with `native_restart_enabled` (Docker services with
    /// `restart: always` — see
    /// `crate::config::Service::docker_native_restart_enabled`) get a short
    /// grace period instead of one-shot staleness (Design §3): a concurrent
    /// `fed` command's liveness check can catch the container mid-backoff
    /// while Docker's own `--restart unless-stopped` is bringing it back up,
    /// and a single such snapshot marking the row `'stale'` would filter it
    /// out of `get_services()` permanently, even after Docker's restart
    /// succeeds a moment later. Everything else (process services, and
    /// Docker services without native restart) keeps today's one-shot
    /// check unchanged — they have no external process racing to revive
    /// them on its own.
    pub async fn mark_dead_services(&mut self) -> Result<Vec<String>> {
        let services = self.get_services().await;
        let mut stale_services = Vec::new();
        // Native-restart services whose liveness check just passed: reset
        // their grace counter so a future blip starts counting fresh.
        let mut grace_reset: Vec<String> = Vec::new();
        // Native-restart services whose liveness check just failed: bump
        // the counter and only stale once the threshold is exceeded.
        let mut grace_hit: Vec<String> = Vec::new();

        // Check Docker daemon health with retry before evaluating container services.
        // If daemon is unhealthy after retries, we cannot reliably determine container state,
        // so we skip container cleanup to avoid removing healthy containers.
        let daemon_healthy = crate::docker::check_daemon_with_retry().await;
        if !daemon_healthy {
            warn!(
                "Docker daemon unhealthy after retries - skipping container cleanup to avoid data loss"
            );
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
            } else if service_state.status == Status::Starting {
                // No PID/container plus `Starting` is what a live concurrent
                // start looks like while it runs install/migrate hooks —
                // don't mark it stale until the grace window has passed
                // (see `STARTING_STALE_GRACE` docs).
                Utc::now()
                    .signed_duration_since(service_state.started_at)
                    .to_std()
                    .is_ok_and(|age| age >= crate::state::STARTING_STALE_GRACE)
            } else {
                // Service has no PID and no container_id.
                // Only consider stale if its status indicates it SHOULD be running.
                // Services that are "stopped" or were never started are not stale.
                Self::status_indicates_should_be_running(service_state.status)
            };

            if is_stale {
                if service_state.native_restart_enabled {
                    grace_hit.push(service_id.clone());
                } else {
                    debug!("Service '{}' appears to be stale", service_id);
                    stale_services.push(service_id.clone());
                }
            } else if service_state.native_restart_enabled {
                grace_reset.push(service_id.clone());
            }
        }

        if !grace_reset.is_empty() {
            self.reset_stale_grace_counts(grace_reset).await?;
        }

        for service_id in grace_hit {
            let failures = self.increment_stale_grace_count(&service_id).await?;
            if failures >= Self::STALE_GRACE_THRESHOLD {
                debug!(
                    "Native-restart service '{}' failed its liveness check {} times in a row \
                     (>= grace threshold {}); marking stale",
                    service_id,
                    failures,
                    Self::STALE_GRACE_THRESHOLD
                );
                stale_services.push(service_id);
            } else {
                debug!(
                    "Native-restart service '{}' failed its liveness check ({}/{}); within \
                     Docker's own restart-backoff grace period, not marking stale yet",
                    service_id,
                    failures,
                    Self::STALE_GRACE_THRESHOLD
                );
            }
        }

        let marked = stale_services.len();

        if marked > 0 {
            debug!(
                "Marking {} stale service(s): {}",
                marked,
                stale_services.join(", ")
            );
            let ids_for_tx = stale_services.clone();
            self.with_transaction(move |tx| {
                for service_id in &ids_for_tx {
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

        Ok(stale_services)
    }

    /// Bump a native-restart-enabled service's consecutive-liveness-check-
    /// failure counter and return the new value. See
    /// [`SqliteStateTracker::mark_dead_services`]'s `STALE_GRACE_THRESHOLD`.
    async fn increment_stale_grace_count(&mut self, service_id: &str) -> Result<u32> {
        let service_id = service_id.to_string();
        self.with_transaction(move |tx| {
            tx.execute(
                "UPDATE services SET stale_grace_count = stale_grace_count + 1 WHERE id = ?1",
                rusqlite::params![&service_id],
            )?;
            tx.query_row(
                "SELECT stale_grace_count FROM services WHERE id = ?1",
                rusqlite::params![&service_id],
                |row| row.get(0),
            )
        })
        .await
    }

    /// Reset the stale-grace counter to 0 for every given service — called
    /// when a native-restart-enabled service's liveness check passes again,
    /// so a later blip starts counting fresh rather than inheriting a stale
    /// partial count from an unrelated earlier failure.
    async fn reset_stale_grace_counts(&mut self, service_ids: Vec<String>) -> Result<()> {
        self.with_transaction(move |tx| {
            for service_id in &service_ids {
                tx.execute(
                    "UPDATE services SET stale_grace_count = 0 WHERE id = ?1",
                    rusqlite::params![service_id],
                )?;
            }
            Ok(())
        })
        .await
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::sqlite::tests::test_support::*;

    // ========================================================================
    // apply_state_transition tests
    // ========================================================================

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
            desired_state: DesiredState::Running,
            native_restart_enabled: false,
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
            desired_state: DesiredState::Running,
            native_restart_enabled: false,
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
            desired_state: DesiredState::Running,
            native_restart_enabled: false,
        };
        tracker.register_service(state).await.unwrap();

        let marked = tracker.mark_dead_services().await.unwrap();
        assert_eq!(marked.len(), 1, "Should mark 1 stale service");

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
            desired_state: DesiredState::Running,
            native_restart_enabled: false,
        };
        tracker.register_service(state).await.unwrap();

        let marked = tracker.mark_dead_services().await.unwrap();
        assert_eq!(
            marked.len(),
            0,
            "Stopped service should not be marked stale"
        );

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
            desired_state: DesiredState::Running,
            native_restart_enabled: false,
        };
        tracker.register_service(state).await.unwrap();

        let marked = tracker.mark_dead_services().await.unwrap();
        assert_eq!(
            marked.len(),
            1,
            "Service with dead PID should be marked stale"
        );
    }

    /// The supervisor attach path (`Orchestrator::initialize_supervisor`)
    /// needs the *ids* of services just marked stale, not just a count —
    /// `07-supervisor.md` Design §1's `initialize_for_supervisor()` hop
    /// depends on this to know which specific rows just went stale.
    #[tokio::test]
    async fn test_mark_dead_services_returns_stale_ids() {
        let mut tracker = create_ephemeral_tracker().await;

        // One dead service (Running status, no PID/container to back it —
        // mark_dead_services treats this as a crash), one legitimately-
        // stopped (not stale) service.
        let dead = ServiceState {
            id: "dead-one".to_string(),
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
            desired_state: DesiredState::Running,
            native_restart_enabled: false,
        };
        tracker.register_service(dead).await.unwrap();
        register_stopped_service(&mut tracker, "stopped-one").await;

        let marked = tracker.mark_dead_services().await.unwrap();
        assert_eq!(
            marked,
            vec!["dead-one".to_string()],
            "Should return exactly the id of the service that just went stale"
        );

        // A second call with nothing new to mark returns an empty vec, not
        // the previously-stale id again (mark_dead_services only sees
        // already-filtered, non-stale rows via get_services()).
        let marked_again = tracker.mark_dead_services().await.unwrap();
        assert!(
            marked_again.is_empty(),
            "Second pass should find nothing new to mark stale"
        );
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
            desired_state: DesiredState::Running,
            native_restart_enabled: false,
        };
        tracker.register_service(state).await.unwrap();

        // Mark it stale
        let marked = tracker.mark_dead_services().await.unwrap();
        assert_eq!(marked.len(), 1);

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
            desired_state: DesiredState::Running,
            native_restart_enabled: false,
        };
        tracker.register_service(state).await.unwrap();

        // No services should be marked stale (Stopped status is not "should be running")
        let marked = tracker.mark_dead_services().await.unwrap();
        assert_eq!(marked.len(), 0);

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

    // --- Starting rows: stale only after the grace window ---

    fn starting_row_no_pid(id: &str, started_at: chrono::DateTime<Utc>) -> ServiceState {
        ServiceState {
            id: id.to_string(),
            status: Status::Starting,
            service_type: ServiceType::Process,
            pid: None,
            container_id: None,
            started_at,
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

    /// A fresh `Starting` row with no PID is what a live concurrent start
    /// looks like while it runs install/migrate hooks — another `fed`
    /// process initializing must NOT mark it stale, or the loser of a
    /// registration race would misread the winner as failed.
    #[tokio::test]
    async fn test_mark_dead_services_young_starting_no_pid_kept() {
        let mut tracker = create_ephemeral_tracker().await;
        tracker
            .register_service(starting_row_no_pid("mid-start", Utc::now()))
            .await
            .unwrap();

        let marked = tracker.mark_dead_services().await.unwrap();
        assert!(
            marked.is_empty(),
            "a Starting row inside the grace window must be left alone"
        );
        assert!(
            tracker.get_services().await.contains_key("mid-start"),
            "the live concurrent start's row must survive the sweep"
        );
    }

    /// A `Starting` row older than the grace window is a starter that died
    /// without cleanup — that one IS stale.
    #[tokio::test]
    async fn test_mark_dead_services_old_starting_no_pid_is_stale() {
        let mut tracker = create_ephemeral_tracker().await;
        let stale_age = chrono::Duration::from_std(crate::state::STARTING_STALE_GRACE).unwrap()
            + chrono::Duration::seconds(1);
        tracker
            .register_service(starting_row_no_pid(
                "stuck-starting",
                Utc::now() - stale_age,
            ))
            .await
            .unwrap();

        let marked = tracker.mark_dead_services().await.unwrap();
        assert_eq!(
            marked.len(),
            1,
            "Service stuck in Starting past the grace window should be marked stale"
        );

        let services = tracker.get_services().await;
        assert!(
            services.is_empty(),
            "Stale Starting service should be filtered from get_services()"
        );
    }

    /// `try_transition_service_status` applies only from an allowed status,
    /// atomically, and reports whether it did.
    #[tokio::test]
    async fn test_try_transition_service_status() {
        let mut tracker = create_ephemeral_tracker().await;
        tracker
            .register_service(starting_row_no_pid("svc", Utc::now()))
            .await
            .unwrap();

        // Current status (Starting) not in allowed_from → not applied.
        let applied = tracker
            .try_transition_service_status("svc", &[Status::Running], Status::Healthy)
            .await
            .unwrap();
        assert!(
            !applied,
            "transition from a disallowed status must not apply"
        );
        assert_eq!(
            tracker.get_service("svc").await.unwrap().status,
            Status::Starting,
            "status must be untouched after a refused transition"
        );

        // Allowed → applied.
        tracker
            .update_service_status("svc", Status::Running)
            .await
            .unwrap();
        let applied = tracker
            .try_transition_service_status(
                "svc",
                &[Status::Running, Status::Failing],
                Status::Healthy,
            )
            .await
            .unwrap();
        assert!(applied);
        assert_eq!(
            tracker.get_service("svc").await.unwrap().status,
            Status::Healthy
        );

        // Missing row → not applied, not an error.
        let applied = tracker
            .try_transition_service_status("ghost", &[Status::Running], Status::Healthy)
            .await
            .unwrap();
        assert!(!applied);
    }

    // --- 07-supervisor.md Design §3: native-restart stale-grace period ---
    //
    // These exercise the grace-period *mechanism* directly against a
    // Process-backed row with `native_restart_enabled` forced to `true` —
    // the flag is decoupled from actual service type in
    // `mark_dead_services`'s logic, so this covers the counter behavior
    // without needing a real Docker daemon (the docker-gated integration
    // test in `tests/docker_service_test.rs` covers the real container
    // case end-to-end).

    #[tokio::test]
    async fn test_mark_dead_services_native_restart_gets_grace_period_not_immediate_stale() {
        let mut tracker = create_ephemeral_tracker().await;

        let mut child = std::process::Command::new("true").spawn().unwrap();
        let dead_pid = child.id();
        child.wait().unwrap();

        let state = ServiceState {
            id: "native-restart-svc".to_string(),
            status: Status::Running,
            service_type: ServiceType::Docker,
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
            desired_state: DesiredState::Running,
            native_restart_enabled: true,
        };
        tracker.register_service(state).await.unwrap();

        // First two failed liveness checks: within the grace threshold
        // (STALE_GRACE_THRESHOLD = 3), so the row must stay visible.
        for attempt in 1..SqliteStateTracker::STALE_GRACE_THRESHOLD {
            let marked = tracker.mark_dead_services().await.unwrap();
            assert!(
                marked.is_empty(),
                "attempt {attempt}: native-restart service should not be marked stale \
                 within its grace period"
            );
            assert_eq!(
                tracker.get_services().await.len(),
                1,
                "attempt {attempt}: service must remain visible during its grace period"
            );
        }

        // The threshold-th consecutive failure marks it stale.
        let marked = tracker.mark_dead_services().await.unwrap();
        assert_eq!(
            marked,
            vec!["native-restart-svc".to_string()],
            "service should be marked stale once its failures reach the grace threshold"
        );
        assert!(tracker.get_services().await.is_empty());
    }

    #[tokio::test]
    async fn test_mark_dead_services_native_restart_grace_count_resets_on_recovery() {
        let mut tracker = create_ephemeral_tracker().await;

        let mut child = std::process::Command::new("true").spawn().unwrap();
        let dead_pid = child.id();
        child.wait().unwrap();
        let live_pid = std::process::id();

        let state = ServiceState {
            id: "native-restart-svc".to_string(),
            status: Status::Running,
            service_type: ServiceType::Docker,
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
            desired_state: DesiredState::Running,
            native_restart_enabled: true,
        };
        tracker.register_service(state).await.unwrap();

        // One failed check (grace count -> 1), well under the threshold.
        let marked = tracker.mark_dead_services().await.unwrap();
        assert!(marked.is_empty());

        // Docker's own restart brings it back — a subsequent liveness check
        // observes a live PID and must reset the grace counter, not just
        // leave it short of the threshold.
        tracker
            .update_service_pid("native-restart-svc", live_pid)
            .await
            .unwrap();
        let marked = tracker.mark_dead_services().await.unwrap();
        assert!(marked.is_empty(), "a healthy check must not stale the row");

        // Simulate it dying again — if the counter had NOT been reset, this
        // would only need 2 more failures (1 already banked + 2 = 3) to hit
        // the threshold. Prove it actually needs a fresh 3 by checking the
        // row survives exactly `STALE_GRACE_THRESHOLD - 1` more failures.
        tracker
            .update_service_pid("native-restart-svc", dead_pid)
            .await
            .unwrap();
        for attempt in 1..SqliteStateTracker::STALE_GRACE_THRESHOLD {
            let marked = tracker.mark_dead_services().await.unwrap();
            assert!(
                marked.is_empty(),
                "attempt {attempt} after reset: should still be within a fresh grace period"
            );
        }
        let marked = tracker.mark_dead_services().await.unwrap();
        assert_eq!(
            marked,
            vec!["native-restart-svc".to_string()],
            "a fresh run of failures (post-reset) should still take the full threshold to stale"
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
            desired_state: DesiredState::Running,
            native_restart_enabled: false,
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
            desired_state: DesiredState::Running,
            native_restart_enabled: false,
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

    // ========================================================================
    // desired_state round-trip tests (07-supervisor.md Design §1, Phase 1)
    // ========================================================================

    #[tokio::test]
    async fn test_register_service_defaults_desired_state_running() {
        let mut tracker = create_ephemeral_tracker().await;

        let state = make_service_state("svc", ServiceType::Process);
        tracker.register_service(state).await.unwrap();

        let retrieved = tracker.get_service("svc").await.unwrap();
        assert_eq!(
            retrieved.desired_state,
            DesiredState::Running,
            "a freshly registered row should default to desired_state='running'"
        );
    }

    #[tokio::test]
    async fn test_register_service_already_exists_leaves_desired_state_untouched() {
        let mut tracker = create_ephemeral_tracker().await;

        let state = make_service_state("svc", ServiceType::Process);
        tracker.register_service(state).await.unwrap();

        // Simulate a prior stop having written 'stopped' for this row.
        tracker
            .set_desired_state("svc", DesiredState::Stopped)
            .await
            .unwrap();

        // A second registration attempt (e.g. a racing start) must leave the
        // row — including desired_state — completely untouched.
        let state2 = make_service_state("svc", ServiceType::Process);
        let outcome = tracker.register_service(state2).await.unwrap();
        assert_eq!(
            outcome,
            RegistrationOutcome::AlreadyExists {
                status: Status::Running
            }
        );

        let retrieved = tracker.get_service("svc").await.unwrap();
        assert_eq!(
            retrieved.desired_state,
            DesiredState::Stopped,
            "register_service must not clobber an existing row's desired_state"
        );
    }

    #[tokio::test]
    async fn test_set_desired_state_round_trip() {
        let mut tracker = create_ephemeral_tracker().await;

        let state = make_service_state("svc", ServiceType::Process);
        tracker.register_service(state).await.unwrap();
        assert_eq!(
            tracker.get_service("svc").await.unwrap().desired_state,
            DesiredState::Running
        );

        tracker
            .set_desired_state("svc", DesiredState::Stopped)
            .await
            .unwrap();
        assert_eq!(
            tracker.get_service("svc").await.unwrap().desired_state,
            DesiredState::Stopped
        );

        // get_services() (the bulk read site) must agree with get_service()
        // (the single-row read site) — both are separate SELECT/mapping
        // sites that must stay in sync.
        let services = tracker.get_services().await;
        assert_eq!(
            services.get("svc").unwrap().desired_state,
            DesiredState::Stopped
        );

        tracker
            .set_desired_state("svc", DesiredState::Running)
            .await
            .unwrap();
        assert_eq!(
            tracker.get_service("svc").await.unwrap().desired_state,
            DesiredState::Running
        );
    }

    #[tokio::test]
    async fn test_set_desired_state_nonexistent_service_errors() {
        let mut tracker = create_ephemeral_tracker().await;

        let result = tracker
            .set_desired_state("no-such-service", DesiredState::Stopped)
            .await;
        assert!(
            result.is_err(),
            "setting desired_state on a missing row should error"
        );
    }

    #[tokio::test]
    async fn test_set_all_desired_state_batches_every_row() {
        let mut tracker = create_ephemeral_tracker().await;

        for name in &["alpha", "beta", "gamma"] {
            let state = make_service_state(name, ServiceType::Process);
            tracker.register_service(state).await.unwrap();
        }

        tracker
            .set_all_desired_state(DesiredState::Stopped)
            .await
            .unwrap();

        let services = tracker.get_services().await;
        assert_eq!(services.len(), 3);
        for name in &["alpha", "beta", "gamma"] {
            assert_eq!(
                services.get(*name).unwrap().desired_state,
                DesiredState::Stopped,
                "service '{}' should have been included in the batch write",
                name
            );
        }

        tracker
            .set_all_desired_state(DesiredState::Running)
            .await
            .unwrap();
        let services = tracker.get_services().await;
        for name in &["alpha", "beta", "gamma"] {
            assert_eq!(
                services.get(*name).unwrap().desired_state,
                DesiredState::Running
            );
        }
    }

    #[tokio::test]
    async fn test_set_all_desired_state_on_empty_db_is_a_noop() {
        let mut tracker = create_ephemeral_tracker().await;

        // Must not error even though there are no rows to update.
        tracker
            .set_all_desired_state(DesiredState::Stopped)
            .await
            .unwrap();
    }
}
