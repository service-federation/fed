use crate::error::{Error, Result};
use crate::state::{RegistrationOutcome, ServiceState, StateTracker};
use std::sync::Arc;
use tokio::sync::RwLock;

/// RAII guard that unregisters a service from the state tracker on drop
/// unless explicitly committed (i.e., the service started successfully).
///
/// This eliminates manual `unregister_service` calls across async error paths
/// in `start_service_impl`. Every `?` between `register()` and `commit()` is
/// automatically covered — if the service fails to start, it gets cleaned up.
///
/// # Drop behavior
///
/// If `commit()` was not called, Drop spawns a background task to unregister
/// the service. This is best-effort: if the tokio runtime is shutting down,
/// the task may not run, but process exit cleans up everything anyway.
pub(super) struct ServiceRegistration {
    state_tracker: Arc<RwLock<StateTracker>>,
    service_name: String,
    /// Present only while the guard is in the cancellation-sensitive handoff
    /// from the detached registration task to its caller. Once delivered,
    /// later startup writes may refresh `started_at`, so normal Drop cleanup
    /// returns to the established delete-by-name behavior.
    registration_started_at: Option<String>,
    committed: bool,
}

impl ServiceRegistration {
    /// Attempt to register a service in the state tracker.
    ///
    /// Returns:
    /// - `Ok(Some(guard))` if newly registered — caller must start the service
    /// - `Ok(None)` if already registered — caller should skip
    /// - `Err` on database failure
    pub async fn register(
        state_tracker: &Arc<RwLock<StateTracker>>,
        state: ServiceState,
    ) -> Result<Option<Self>> {
        let tracker = Arc::clone(state_tracker);
        let registration_started_at = state.started_at.to_rfc3339();
        let (result_tx, result_rx) = tokio::sync::oneshot::channel();

        // Shield the database transaction from caller cancellation. Once
        // tokio-rusqlite queues a closure, dropping its future does not cancel
        // the SQLite work. Running registration in a detached task ensures it
        // reaches an ownership decision. If the caller disappears after our
        // INSERT commits, delete the row only if its original timestamp still
        // matches this attempt; if another attempt already owns or replaced the
        // row, leave it untouched.
        let _registration_task = tokio::spawn(async move {
            let name = state.id.clone();
            let result = match tracker.write().await.register_service(state).await {
                Ok(RegistrationOutcome::Registered) => Ok(Some(Self {
                    state_tracker: Arc::clone(&tracker),
                    service_name: name,
                    registration_started_at: Some(registration_started_at),
                    committed: false,
                })),
                Ok(RegistrationOutcome::AlreadyExists { status }) => {
                    tracing::debug!(
                        "Service '{}' already registered (status: {}), skipping start",
                        name,
                        status
                    );
                    Ok(None)
                }
                Err(error) => Err(error),
            };
            // If the receiver was cancelled before or just after this send,
            // dropping the undelivered guard invokes timestamp-matched cleanup.
            let _ = result_tx.send(result);
        });

        let mut registration = result_rx
            .await
            .map_err(|error| Error::Database(tokio_rusqlite::Error::Other(Box::new(error))))??;
        if let Some(guard) = registration.as_mut() {
            // No await occurs between receipt and this handoff, so cancellation
            // cannot observe the guard after its conditional identity is cleared.
            guard.registration_started_at = None;
        }
        Ok(registration)
    }

    /// Mark the registration as successful — Drop will no longer unregister.
    ///
    /// Call this after the service has been started and its state updated to
    /// Running. Once committed, the service lives until explicitly stopped.
    pub fn commit(mut self) {
        self.committed = true;
    }
}

impl Drop for ServiceRegistration {
    fn drop(&mut self) {
        if !self.committed {
            let tracker = Arc::clone(&self.state_tracker);
            let name = std::mem::take(&mut self.service_name);
            let registration_started_at = self.registration_started_at.take();
            tracing::debug!(
                "ServiceRegistration guard dropping uncommitted '{}' — spawning cleanup",
                name
            );
            tokio::spawn(async move {
                let result = if let Some(started_at) = registration_started_at {
                    tracker
                        .write()
                        .await
                        .unregister_service_started_at(&name, &started_at)
                        .await
                } else {
                    tracker.write().await.unregister_service(&name).await
                };
                if let Err(e) = result {
                    tracing::warn!(
                        "Failed to unregister '{}' during guard cleanup: {}",
                        name,
                        e
                    );
                }
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ServiceType;
    use crate::state::ServiceState;

    #[tokio::test]
    async fn test_commit_prevents_unregister() {
        let tracker = Arc::new(RwLock::new(StateTracker::new_ephemeral().await.unwrap()));
        {
            let mut t = tracker.write().await;
            t.initialize().await.unwrap();
        }

        let state = ServiceState::new("svc".into(), ServiceType::Process, "test".into());
        let guard = ServiceRegistration::register(&tracker, state)
            .await
            .unwrap()
            .expect("should be Registered");

        guard.commit();

        // Give any hypothetical spawn a chance to run
        tokio::task::yield_now().await;

        // Service should still be registered
        let t = tracker.read().await;
        assert!(t.get_service("svc").await.is_some());
    }

    #[tokio::test]
    async fn test_drop_without_commit_unregisters() {
        let tracker = Arc::new(RwLock::new(StateTracker::new_ephemeral().await.unwrap()));
        {
            let mut t = tracker.write().await;
            t.initialize().await.unwrap();
        }

        let state = ServiceState::new("svc".into(), ServiceType::Process, "test".into());
        let guard = ServiceRegistration::register(&tracker, state)
            .await
            .unwrap()
            .expect("should be Registered");

        // Drop without commit
        drop(guard);

        // Let the spawned cleanup task run
        tokio::task::yield_now().await;
        // Extra yield to be safe — the spawn needs to acquire the write lock
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;

        let t = tracker.read().await;
        assert!(
            t.get_service("svc").await.is_none(),
            "Service should be unregistered after guard dropped without commit"
        );
    }

    #[tokio::test]
    async fn test_already_exists_returns_none() {
        let tracker = Arc::new(RwLock::new(StateTracker::new_ephemeral().await.unwrap()));
        {
            let mut t = tracker.write().await;
            t.initialize().await.unwrap();
        }

        let state = ServiceState::new("svc".into(), ServiceType::Process, "test".into());
        let guard = ServiceRegistration::register(&tracker, state)
            .await
            .unwrap()
            .expect("first should be Registered");
        guard.commit();

        // Second registration should return None
        let state2 = ServiceState::new("svc".into(), ServiceType::Docker, "test".into());
        let result = ServiceRegistration::register(&tracker, state2)
            .await
            .unwrap();
        assert!(
            result.is_none(),
            "Duplicate registration should return None"
        );
    }

    #[tokio::test]
    async fn cancelled_registration_cleans_up_committed_row() {
        let tracker = Arc::new(RwLock::new(StateTracker::new_ephemeral().await.unwrap()));
        {
            let mut t = tracker.write().await;
            t.initialize().await.unwrap();
        }

        // Occupy tokio-rusqlite's worker so registration can enqueue its
        // transaction but cannot complete before we cancel its caller.
        let conn = tracker.read().await.clone_connection();
        let blocker_conn = conn.clone();
        let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = std::sync::mpsc::sync_channel(0);
        let blocker = tokio::spawn(async move {
            blocker_conn
                .call(move |_| {
                    entered_tx.send(()).unwrap();
                    release_rx.recv().unwrap();
                    Ok(())
                })
                .await
                .unwrap();
        });
        tokio::time::timeout(std::time::Duration::from_secs(1), entered_rx)
            .await
            .expect("database blocker should start")
            .expect("database blocker should signal");

        let register_tracker = Arc::clone(&tracker);
        let registration = tokio::spawn(async move {
            ServiceRegistration::register(
                &register_tracker,
                ServiceState::new("svc".into(), ServiceType::Process, "test".into()),
            )
            .await
        });

        // The registration task holds the tracker write lock only after it has
        // sent its transaction to the blocked database worker.
        let queued = tokio::time::timeout(std::time::Duration::from_secs(1), async {
            loop {
                if tracker.try_read().is_err() {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await;
        assert!(queued.is_ok(), "registration transaction should be queued");
        registration.abort();
        let cancelled = matches!(registration.await, Err(error) if error.is_cancelled());
        assert!(cancelled, "registration caller should be cancelled");

        // Queue an observation behind registration but ahead of its eventual
        // guard cleanup. This proves the cancelled transaction really commits.
        let observer_conn = conn.clone();
        let observed = tokio::spawn(async move {
            observer_conn
                .call(|conn| {
                    Ok(conn.query_row(
                        "SELECT COUNT(*) FROM services WHERE id = 'svc'",
                        [],
                        |row| row.get::<_, i64>(0),
                    )?)
                })
                .await
                .unwrap()
        });

        release_tx.send(()).unwrap();
        blocker.await.unwrap();
        assert_eq!(
            observed.await.unwrap(),
            1,
            "registration should commit after its caller is cancelled"
        );

        for _ in 0..100 {
            let row = tracker.read().await.get_service("svc").await;
            if row.is_none() {
                return;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        panic!("cancelled registration left a committed Starting row");
    }
}
