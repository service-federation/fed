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

/// Outcome of the startup health wait for a single service.
///
/// A healthcheck timeout during `fed start` is non-fatal — the process is
/// alive and dependents may proceed — but callers must still be able to tell
/// "verified healthy" from "started, health never confirmed". This type
/// carries that distinction as data instead of a log line.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StartHealth {
    /// The configured healthcheck passed before startup returned. The
    /// timeout is evaluated between polling attempts, so a check already in
    /// flight at the deadline may still complete and count as healthy.
    Healthy,
    /// The configured healthcheck did not pass within its timeout; the
    /// process/container was still alive, so startup continued.
    TimedOut {
        /// The configured healthcheck timeout that elapsed.
        timeout: Duration,
    },
    /// Nothing was verified this call: no healthcheck is configured, the
    /// service was already running, or the node is a hook-only oneshot.
    Unchecked,
}

/// Per-service [`StartHealth`] outcomes collected by a start call, in start
/// order.
#[derive(Debug, Clone, Default)]
pub struct StartOutcome {
    health: Vec<(String, StartHealth)>,
}

impl StartOutcome {
    /// Record `health` for `service`. The latest real observation
    /// (`Healthy`/`TimedOut`) wins — a service restarted twice in one
    /// command must report its final wait, not a stale earlier one. Only a
    /// later `Unchecked` (a deduplicated no-op start) never downgrades a
    /// real observation.
    pub fn record(&mut self, service: &str, health: StartHealth) {
        match self.health.iter_mut().find(|(name, _)| name == service) {
            Some((_, existing)) => {
                if health != StartHealth::Unchecked {
                    *existing = health;
                }
            }
            None => self.health.push((service.to_string(), health)),
        }
    }

    /// Fold another outcome (e.g. from a dependency's start call) into this one.
    pub fn merge(&mut self, other: StartOutcome) {
        for (name, health) in other.health {
            self.record(&name, health);
        }
    }

    /// The recorded outcome for `service`, if it was part of this start.
    pub fn get(&self, service: &str) -> Option<StartHealth> {
        self.health
            .iter()
            .find(|(name, _)| name == service)
            .map(|(_, health)| *health)
    }

    /// Services whose configured healthcheck did not pass during startup,
    /// with the timeout that elapsed, in start order.
    pub fn warnings(&self) -> impl Iterator<Item = (&str, Duration)> {
        self.health
            .iter()
            .filter_map(|(name, health)| match health {
                StartHealth::TimedOut { timeout } => Some((name.as_str(), *timeout)),
                _ => None,
            })
    }

    /// True if any service's healthcheck timed out during startup.
    pub fn has_warnings(&self) -> bool {
        self.warnings().next().is_some()
    }
}

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

        // Same visibility rule as await_healthcheck: without a pending
        // progress line (script deps run without one), this wait would
        // otherwise be a silent pause up to the full timeout.
        let has_progress_line = crate::progress::has_pending();
        if has_progress_line {
            tracing::debug!(
                "Waiting for healthcheck on '{}' (timeout: {:?})",
                service_name,
                timeout
            );
        } else {
            tracing::info!(
                "Waiting for healthcheck on '{}' (timeout: {:?})",
                service_name,
                timeout
            );
        }

        loop {
            if start.elapsed() > timeout {
                return Err(Error::HealthCheckFailed(
                    service_name.to_string(),
                    format!("did not become healthy within {:?}", timeout),
                ));
            }

            match checker.check().await {
                Ok(true) => {
                    if has_progress_line {
                        tracing::debug!("Service '{}' is healthy", service_name);
                    } else {
                        tracing::info!("Service '{}' is healthy", service_name);
                    }
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
    ) -> Result<StartHealth> {
        // Clone the Arc and drop the read lock immediately
        let checker = {
            let health_checkers = self.orchestrator.health_checkers.read().await;
            match health_checkers.get(name) {
                Some(c) => Arc::clone(c),
                // No healthcheck configured -- nothing to wait for
                None => return Ok(StartHealth::Unchecked),
            }
        };

        let timeout = checker.timeout();
        let start = std::time::Instant::now();
        let check_interval = Duration::from_millis(500);

        // Callers with an in-place progress line (fed start) get live detail
        // attached to it; everyone else (restart, script deps, non-TTY) keeps
        // the plain log lines so long waits aren't silent.
        let has_progress_line = crate::progress::has_pending();
        if has_progress_line {
            tracing::debug!(
                "Waiting for healthcheck on '{}' (timeout: {:?})",
                name,
                timeout
            );
        } else {
            tracing::info!(
                "Waiting for healthcheck on '{}' (timeout: {:?})",
                name,
                timeout
            );
        }

        loop {
            // Live detail on the in-place progress line (no-op if none is up).
            // Named, because concurrent starts share one pending line.
            crate::progress::set_detail(&format!(
                "{}: healthcheck {}s/{}s",
                name,
                start.elapsed().as_secs(),
                timeout.as_secs()
            ));

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

            // Deadline check AFTER the liveness checks above: a process that
            // died during the final poll sleep must surface as a fatal start
            // error, never be misreported as a non-fatal timeout warning.
            if start.elapsed() >= timeout {
                if has_progress_line {
                    // fed start owns the reporting (⚠ outcome line + summary)
                    tracing::debug!(
                        "Service '{}' did not become healthy within {:?}",
                        name,
                        timeout
                    );
                } else {
                    tracing::warn!(
                        "Service '{}' did not become healthy within {:?}",
                        name,
                        timeout
                    );
                }
                // Don't fail the start -- the service process is running, just
                // not healthy yet. Return the timeout as data so the command
                // layer can surface it instead of claiming full success.
                return Ok(StartHealth::TimedOut { timeout });
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
                    if has_progress_line {
                        tracing::debug!("Service '{}' is healthy", name);
                    } else {
                        tracing::info!("Service '{}' is healthy", name);
                    }
                    let mut tracker = self.orchestrator.state_tracker.write().await;
                    tracker.update_service_status(name, Status::Healthy).await?;
                    tracker.save().await?;
                    return Ok(StartHealth::Healthy);
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

        let outcome = HealthCheckRunner::new(&orchestrator)
            .await_healthcheck("service", &manager)
            .await
            .expect("a live service missing its startup health timeout remains a warning");
        assert_eq!(
            outcome,
            StartHealth::TimedOut {
                timeout: Duration::ZERO
            },
            "the timeout must surface as structured data, not just a log line"
        );
    }

    struct DeadPidManager {
        pid: u32,
    }

    #[async_trait]
    impl ServiceManager for DeadPidManager {
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

        fn get_pid(&self) -> Option<u32> {
            Some(self.pid)
        }

        fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
            self
        }
    }

    /// A process that died right before the health deadline must fail the
    /// start, not be misreported as a non-fatal timeout warning: the liveness
    /// check runs before the deadline check.
    #[tokio::test]
    async fn process_death_at_health_deadline_is_fatal_not_a_timeout_warning() {
        // Reap a real child so its PID is known-dead.
        let mut child = std::process::Command::new("true")
            .spawn()
            .expect("spawn true");
        let pid = child.id();
        child.wait().expect("reap child");

        let temp = tempfile::tempdir().unwrap();
        let orchestrator =
            Orchestrator::new_ephemeral(Config::default(), temp.path().to_path_buf())
                .await
                .unwrap();
        {
            let mut tracker = orchestrator.state_tracker.write().await;
            tracker.initialize().await.unwrap();
            tracker
                .register_service(crate::state::ServiceState::new(
                    "service".to_string(),
                    crate::config::ServiceType::Process,
                    String::new(),
                ))
                .await
                .unwrap();
        }
        orchestrator.health_checkers.write().await.insert(
            "service".to_string(),
            Arc::new(AlwaysUnhealthy {
                timeout: Duration::ZERO,
            }),
        );
        let manager: Arc<tokio::sync::Mutex<Box<dyn ServiceManager>>> =
            Arc::new(tokio::sync::Mutex::new(Box::new(DeadPidManager { pid })));

        let error = HealthCheckRunner::new(&orchestrator)
            .await_healthcheck("service", &manager)
            .await
            .expect_err("a dead process must fail the start even at the deadline");
        assert!(
            matches!(error, Error::ServiceStartFailed(ref name, _) if name == "service"),
            "expected ServiceStartFailed, got {:?}",
            error
        );
    }

    #[test]
    fn start_outcome_real_observation_replaces_unchecked_and_never_downgrades() {
        let mut outcome = StartOutcome::default();
        outcome.record("api", StartHealth::Unchecked);
        outcome.record(
            "api",
            StartHealth::TimedOut {
                timeout: Duration::from_secs(5),
            },
        );
        // A later deduplicated start (already running) reports Unchecked —
        // it must not erase the recorded timeout.
        outcome.record("api", StartHealth::Unchecked);

        assert_eq!(
            outcome.get("api"),
            Some(StartHealth::TimedOut {
                timeout: Duration::from_secs(5)
            })
        );
        assert!(outcome.has_warnings());
        let warnings: Vec<_> = outcome.warnings().collect();
        assert_eq!(warnings, vec![("api", Duration::from_secs(5))]);
    }

    /// The latest real observation wins: a service restarted twice in one
    /// command reports its final health wait, in either direction.
    #[test]
    fn start_outcome_latest_real_observation_wins() {
        let timed_out = StartHealth::TimedOut {
            timeout: Duration::from_secs(5),
        };

        let mut outcome = StartOutcome::default();
        outcome.record("api", StartHealth::Healthy);
        outcome.record("api", timed_out);
        assert_eq!(
            outcome.get("api"),
            Some(timed_out),
            "a later timeout must not be masked by an earlier Healthy"
        );

        let mut outcome = StartOutcome::default();
        outcome.record("api", timed_out);
        outcome.record("api", StartHealth::Healthy);
        assert_eq!(
            outcome.get("api"),
            Some(StartHealth::Healthy),
            "a later healthy wait must clear an earlier stale warning"
        );
        assert!(!outcome.has_warnings());
    }
}
