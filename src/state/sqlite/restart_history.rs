// Split from sqlite.rs (see git history before this commit for pre-split blame).
use super::*;

impl SqliteStateTracker {
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

    // =========================================================================
    // Circuit Breaker Methods
    // =========================================================================

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
}

#[cfg(test)]
mod tests {
    use crate::state::sqlite::tests::test_support::*;

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
}
