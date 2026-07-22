// Split from sqlite.rs (see git history before this commit for pre-split blame).
use super::*;

impl SqliteStateTracker {
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::sqlite::tests::test_support::*;

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
        assert!(
            tracker
                .get_global_port_allocations(Some("iso-other"))
                .await
                .is_empty()
        );

        // clear_port_resolutions wipes every scope.
        tracker.clear_port_resolutions().await.unwrap();
        assert!(tracker.get_global_port_allocations(None).await.is_empty());
        assert!(
            tracker
                .get_global_port_allocations(Some("iso-cafebabe"))
                .await
                .is_empty()
        );
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
}
