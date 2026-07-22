//! Health check registration and awaiting for services.
//!
//! This module contains the `HealthCheckRunner` struct which encapsulates
//! health checker creation, registration, and polling logic that was previously
//! part of the main Orchestrator. Extracting these operations improves separation
//! of concerns and keeps the orchestrator core focused on service coordination.

use crate::config::HealthCheckType;
use crate::error::{Error, Result};
use crate::healthcheck::{CommandChecker, DockerCommandChecker, HealthChecker, HttpChecker};
use crate::service::{ServiceManager, Status};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use super::core::Orchestrator;

/// Type alias for the health checker registry.
/// Uses `Arc` so checkers can be cloned out without holding the read lock.
pub(super) type HealthCheckerRegistry = HashMap<String, Arc<dyn HealthChecker>>;
/// Type alias for the shared health checker registry
pub(super) type SharedHealthCheckerRegistry = Arc<tokio::sync::RwLock<HealthCheckerRegistry>>;

/// Short-lived helper for health check operations.
///
/// Constructed on-demand from an `Orchestrator` reference. Health-check methods
/// on `Orchestrator` delegate here after constructing a `HealthCheckRunner`.
pub(super) struct HealthCheckRunner<'a> {
    orchestrator: &'a Orchestrator,
}

impl<'a> HealthCheckRunner<'a> {
    pub fn new(orchestrator: &'a Orchestrator) -> Self {
        Self { orchestrator }
    }

    /// Create health checkers for all configured services and register them.
    pub async fn create_health_checkers(&self) {
        for (name, service) in &self.orchestrator.config.services {
            if let Some(ref healthcheck) = service.healthcheck {
                // Use configured timeout or default (5 seconds)
                let timeout = healthcheck.get_timeout();

                let checker: Arc<dyn HealthChecker> = match healthcheck.health_check_type() {
                    HealthCheckType::Http => {
                        if let Some(url) = healthcheck.get_http_url() {
                            // Use shared HTTP client to prevent file descriptor exhaustion
                            // when running many services with HTTP health checks
                            match HttpChecker::with_shared_client(url.to_string(), timeout) {
                                Ok(checker) => Arc::new(checker),
                                Err(e) => {
                                    tracing::warn!(
                                        "Skipping invalid healthcheck URL for service '{}': {}",
                                        name,
                                        e
                                    );
                                    continue;
                                }
                            }
                        } else {
                            continue;
                        }
                    }
                    HealthCheckType::Command => {
                        if let Some(cmd) = healthcheck.get_command() {
                            // Docker services: run healthcheck inside container
                            // Process/Gradle services: run healthcheck on host
                            if service.image.is_some() {
                                // Docker service - use docker exec
                                let session_id = self.orchestrator.isolation_id.clone();
                                let container_name = crate::service::docker_container_name(
                                    name,
                                    session_id.as_deref(),
                                    &self.orchestrator.work_dir,
                                );
                                Arc::new(DockerCommandChecker::new(
                                    container_name,
                                    cmd.to_string(),
                                    timeout,
                                ))
                            } else {
                                // Process/Gradle service - run on host
                                Arc::new(CommandChecker::new(
                                    "bash".to_string(),
                                    vec!["-c".to_string(), cmd.to_string()],
                                    timeout,
                                ))
                            }
                        } else {
                            continue;
                        }
                    }
                    HealthCheckType::None => continue,
                };

                self.orchestrator
                    .health_checkers
                    .write()
                    .await
                    .insert(name.clone(), checker);
            }
        }
    }

    /// Fail fast if a service's healthcheck already passes BEFORE the service
    /// is started: whatever is answering it is not ours, and once our process
    /// starts we could never tell the two apart. Without this, a leftover
    /// process on the service's port makes the healthcheck pass instantly
    /// while the actual service crashes (e.g. dev servers that refuse to
    /// start because "another server is already running").
    ///
    /// Docker command healthchecks run inside the not-yet-created container,
    /// so they can only error here — that is treated as "nothing listening".
    pub async fn preflight_foreign_listener(&self, name: &str) -> Result<()> {
        let checker = {
            let health_checkers = self.orchestrator.health_checkers.read().await;
            match health_checkers.get(name) {
                Some(c) => Arc::clone(c),
                None => return Ok(()),
            }
        };

        if let Ok(true) = checker.check().await {
            return Err(Error::ServiceStartFailed(
                name.to_string(),
                format!(
                    "The healthcheck for '{}' already passes before the service was \
                     started — another process is already serving it. Starting anyway \
                     would make the healthcheck meaningless (it can't tell the two \
                     apart). Stop the other process first. A common cause is a dev \
                     server that daemonized and outlived a previous run.",
                    name
                ),
            ));
        }
        Ok(())
    }

    /// Wait for a service to become healthy (used by script dependencies).
    /// Returns Ok(()) when healthy, or Err after timeout.
    pub async fn wait_for_healthy(&self, service_name: &str, timeout: Duration) -> Result<()> {
        let checker = {
            let health_checkers = self.orchestrator.health_checkers.read().await;
            match health_checkers.get(service_name) {
                Some(c) => Arc::clone(c),
                None => {
                    // No healthcheck configured - consider it healthy after a brief moment
                    tokio::time::sleep(Duration::from_millis(500)).await;
                    return Ok(());
                }
            }
        };

        let start = std::time::Instant::now();
        let check_interval = Duration::from_millis(500);

        loop {
            if start.elapsed() > timeout {
                return Err(Error::HealthCheckFailed(
                    service_name.to_string(),
                    format!("did not become healthy within {:?}", timeout),
                ));
            }

            match checker.check().await {
                Ok(true) => {
                    tracing::debug!("Service '{}' is healthy", service_name);
                    return Ok(());
                }
                Ok(false) => {
                    tracing::debug!("Service '{}' not healthy yet, waiting...", service_name);
                }
                Err(e) => {
                    tracing::debug!(
                        "Service '{}' health check failed: {}, retrying...",
                        service_name,
                        e
                    );
                }
            }

            tokio::time::sleep(check_interval).await;
        }
    }

    /// Await a service's healthcheck during startup.
    ///
    /// If the service has a registered healthcheck, polls it until healthy or timeout.
    /// Also monitors process/container liveness to detect early crashes without
    /// waiting for the full timeout. If no healthcheck is registered, returns immediately.
    ///
    /// Respects the orchestrator's cancellation token for responsive Ctrl-C handling.
    pub async fn await_healthcheck(
        &self,
        name: &str,
        manager_arc: &Arc<tokio::sync::Mutex<Box<dyn ServiceManager>>>,
    ) -> Result<()> {
        // Clone the Arc and drop the read lock immediately
        let checker = {
            let health_checkers = self.orchestrator.health_checkers.read().await;
            match health_checkers.get(name) {
                Some(c) => Arc::clone(c),
                None => return Ok(()), // No healthcheck configured -- nothing to wait for
            }
        };

        let timeout = checker.timeout();
        let start = std::time::Instant::now();
        let check_interval = Duration::from_millis(500);

        tracing::info!(
            "Waiting for healthcheck on '{}' (timeout: {:?})",
            name,
            timeout
        );

        loop {
            if start.elapsed() >= timeout {
                tracing::warn!(
                    "Service '{}' did not become healthy within {:?}",
                    name,
                    timeout
                );
                // Don't fail the start -- the service process is running, just not
                // healthy yet. The TUI/status command will show the accurate state.
                return Ok(());
            }

            // Respond to Ctrl-C promptly instead of waiting for timeout
            if self.orchestrator.cancellation_token.is_cancelled() {
                tracing::debug!("Healthcheck wait for '{}' cancelled", name);
                return Err(Error::Cancelled(name.to_string()));
            }

            // Check if the service died while we were waiting.
            // Read PID/container info under manager lock, then drop it before
            // acquiring state_tracker write lock (preserves lock ordering:
            // state_tracker -> manager, never the reverse).
            let (pid, container_id) = {
                let manager = manager_arc.lock().await;
                (manager.get_pid(), manager.get_container_id())
            };
            // manager lock released here

            // Process/Gradle services: check PID liveness via signal 0
            if let Some(pid) = pid {
                let nix_pid = nix::unistd::Pid::from_raw(pid as i32);
                if nix::sys::signal::kill(nix_pid, None).is_err() {
                    tracing::warn!(
                        "Service '{}' (PID {}) died during healthcheck wait",
                        name,
                        pid
                    );
                    let mut tracker = self.orchestrator.state_tracker.write().await;
                    tracker.update_service_status(name, Status::Stopped).await?;
                    tracker.save().await?;
                    return Err(Error::ServiceStartFailed(
                        name.to_string(),
                        format!("Service '{}' crashed during healthcheck wait", name),
                    ));
                }
            }

            // Docker services: check container is still running
            if let Some(ref container_id) = container_id {
                let client = crate::docker::DockerClient::new();
                let is_running = client.is_alive(container_id, Duration::from_secs(5)).await;
                if !is_running {
                    tracing::warn!(
                        "Service '{}' container {} stopped during healthcheck wait",
                        name,
                        container_id
                    );
                    let mut tracker = self.orchestrator.state_tracker.write().await;
                    tracker.update_service_status(name, Status::Stopped).await?;
                    tracker.save().await?;
                    return Err(Error::ServiceStartFailed(
                        name.to_string(),
                        format!(
                            "Service '{}' container stopped during healthcheck wait",
                            name
                        ),
                    ));
                }
            }

            // Poll the healthcheck
            match checker.check().await {
                Ok(true) => {
                    // The healthcheck endpoint responding does not prove OUR process
                    // is serving it: a leftover process on the same port answers too
                    // (e.g. `astro dev` daemonizes-and-exits under agent environments,
                    // leaving a foreign listener). Re-verify liveness before declaring
                    // the service healthy.
                    if let Some(pid) = pid {
                        let nix_pid = nix::unistd::Pid::from_raw(pid as i32);
                        if nix::sys::signal::kill(nix_pid, None).is_err() {
                            let mut tracker = self.orchestrator.state_tracker.write().await;
                            tracker.update_service_status(name, Status::Stopped).await?;
                            tracker.save().await?;
                            return Err(Error::ServiceStartFailed(
                                name.to_string(),
                                format!(
                                    "Service '{}' exited, but its healthcheck still passes. \
                                     Another process is likely already listening on the \
                                     healthcheck port, or the command daemonizes and exits \
                                     (some dev servers background themselves in agent \
                                     environments). Stop the other process, or make the \
                                     command stay in the foreground.",
                                    name
                                ),
                            ));
                        }
                    }
                    tracing::info!("Service '{}' is healthy", name);
                    let mut tracker = self.orchestrator.state_tracker.write().await;
                    tracker.update_service_status(name, Status::Healthy).await?;
                    tracker.save().await?;
                    return Ok(());
                }
                Ok(false) => {
                    tracing::debug!("Service '{}' not healthy yet, waiting...", name);
                }
                Err(e) => {
                    tracing::debug!("Service '{}' health check error: {}, retrying...", name, e);
                }
            }

            tokio::time::sleep(check_interval).await;
        }
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::disallowed_methods)]

    use super::*;
    use crate::config::Config;
    use async_trait::async_trait;

    struct AlwaysUnhealthy {
        timeout: Duration,
    }

    #[async_trait]
    impl HealthChecker for AlwaysUnhealthy {
        async fn check(&self) -> Result<bool> {
            Ok(false)
        }

        fn timeout(&self) -> Duration {
            self.timeout
        }
    }

    struct LiveManager;

    #[async_trait]
    impl ServiceManager for LiveManager {
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
            Ok(false)
        }

        fn status(&self) -> Status {
            Status::Running
        }

        fn name(&self) -> &str {
            "service"
        }

        fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
            self
        }
    }

    #[tokio::test]
    async fn script_readiness_timeout_is_an_error() {
        let temp = tempfile::tempdir().unwrap();
        let orchestrator =
            Orchestrator::new_ephemeral(Config::default(), temp.path().to_path_buf())
                .await
                .unwrap();
        orchestrator.health_checkers.write().await.insert(
            "service".to_string(),
            Arc::new(AlwaysUnhealthy {
                timeout: Duration::from_millis(1),
            }),
        );

        let error = HealthCheckRunner::new(&orchestrator)
            .wait_for_healthy("service", Duration::from_millis(1))
            .await
            .expect_err("script dependencies must not run after a readiness timeout");

        assert!(matches!(error, Error::HealthCheckFailed(ref name, _) if name == "service"));
    }

    #[tokio::test]
    async fn startup_health_timeout_warns_but_does_not_fail_startup() {
        let temp = tempfile::tempdir().unwrap();
        let orchestrator =
            Orchestrator::new_ephemeral(Config::default(), temp.path().to_path_buf())
                .await
                .unwrap();
        orchestrator.health_checkers.write().await.insert(
            "service".to_string(),
            Arc::new(AlwaysUnhealthy {
                timeout: Duration::ZERO,
            }),
        );
        let manager: Arc<tokio::sync::Mutex<Box<dyn ServiceManager>>> =
            Arc::new(tokio::sync::Mutex::new(Box::new(LiveManager)));

        HealthCheckRunner::new(&orchestrator)
            .await_healthcheck("service", &manager)
            .await
            .expect("a live service missing its startup health timeout remains a warning");
    }
}
