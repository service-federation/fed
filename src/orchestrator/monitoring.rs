//! Health monitoring and automatic restart functionality for services.
//!
//! This module handles:
//! - Periodic health checks for running services
//! - Automatic restart with exponential backoff
//! - Panic-safe monitoring loop
//!
//! # Lock ordering (see `lock_order.rs`)
//!
//! This module acquires `services`, `state_tracker`, and individual service
//! mutexes. To prevent deadlocks, never hold a service mutex while acquiring
//! `state_tracker`. Instead: scope the mutex, release it, then acquire
//! `state_tracker` separately.
//!
//! # Architecture
//!
//! The monitoring system is decomposed into small, composable functions:
//! - [`check_all_services`]: Concurrently checks health of all running services
//! - [`classify_health_results`]: Separates healthy from unhealthy services
//! - [`should_restart_service`]: Pure function determining restart eligibility
//! - [`restart_single_service`]: Handles restart with backoff for one service
//! - [`execute_health_check_cycle`]: Orchestrates a complete health check cycle

use crate::config::{Config, RestartPolicy};
use crate::error::{Error, Result};
use crate::service::{ServiceManager, Status};
use crate::state::StateTracker;
use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Mutex, RwLock};
use tokio_util::sync::CancellationToken;

use super::Orchestrator;

/// Type alias for a single service manager entry
type ServiceEntry = Arc<Mutex<Box<dyn ServiceManager>>>;

/// Type alias for the services map
type ServicesMap = Arc<RwLock<HashMap<String, ServiceEntry>>>;

/// Type alias for the state tracker
type StateTrackerRef = Arc<RwLock<StateTracker>>;

/// Result of a health check for a single service
struct HealthCheckResult {
    name: String,
    manager: Arc<Mutex<Box<dyn ServiceManager>>>,
    is_healthy: bool,
}

/// Calculate backoff delay for restart attempts with jitter
/// Uses capped exponential backoff with jitter: 1s, 2s, 4s, 8s, 16s, 32s, 60s (max)
/// Jitter prevents thundering herd when multiple services fail simultaneously
///
/// # Backoff Sequence (base delay ± 50% jitter)
/// - Failure 1: 1s ± 0.5s
/// - Failure 2: 2s ± 1s
/// - Failure 3: 4s ± 2s
/// - Failure 4: 8s ± 4s
/// - Failure 5: 16s ± 8s
/// - Failure 6: 32s ± 16s
/// - Failure 7+: 60s ± 30s (capped)
///
/// # Arguments
/// * `consecutive_failures` - Number of consecutive health check failures (1-based)
///
/// # Returns
/// Duration to wait before next restart attempt with random jitter applied
pub(super) fn calculate_backoff_delay(consecutive_failures: u32) -> Duration {
    if consecutive_failures == 0 {
        return Duration::from_secs(0);
    }

    // Start at 1 second, double each time, cap at 60 seconds
    // 2^0 = 1, 2^1 = 2, 2^2 = 4, 2^3 = 8, 2^4 = 16, 2^5 = 32, 2^6 = 64 (capped at 60)
    let exponent = consecutive_failures.saturating_sub(1).min(6);
    let base_delay_secs = 2u64.pow(exponent).min(60);

    // Add jitter: ±50% of the delay to prevent thundering herd
    // Jitter range: [base_delay * 0.5, base_delay * 1.5]
    use rand::Rng;
    let mut rng = rand::thread_rng();
    let jitter_range = (base_delay_secs as f64 * 0.5) as u64;
    let min_delay = base_delay_secs.saturating_sub(jitter_range);
    let max_delay = base_delay_secs.saturating_add(jitter_range);

    let delay_with_jitter = if min_delay < max_delay {
        rng.gen_range(min_delay..=max_delay)
    } else {
        base_delay_secs
    };

    Duration::from_secs(delay_with_jitter)
}

/// Check all services concurrently and return health results.
///
/// Only checks services that are in Running, Healthy, or Failing state.
/// Uses concurrent futures to check all services in parallel.
///
/// # Lock Optimization
///
/// Acquires the services read lock ONCE, collects all service Arcs, then
/// drops the lock before performing health checks. This minimizes lock
/// contention when many services are being monitored or when start/stop
/// operations are concurrent with health checking.
///
/// Previously: N lock acquisitions (one per service)
/// Now: 1 lock acquisition for all services
async fn check_all_services(services: &ServicesMap) -> Vec<HealthCheckResult> {
    // Acquire read lock once and collect all service entries
    #[allow(clippy::type_complexity)]
    let service_entries: Vec<(String, Arc<Mutex<Box<dyn ServiceManager>>>)> = {
        let svcs = services.read().await;
        svcs.iter()
            .map(|(name, manager)| (name.clone(), Arc::clone(manager)))
            .collect()
    }; // Read lock dropped here

    let mut health_check_tasks = Vec::new();

    for (service_name, manager_arc) in service_entries {
        // Check if we should monitor this service
        let should_check = {
            let manager = manager_arc.lock().await;
            let status = manager.status();
            matches!(status, Status::Running | Status::Healthy | Status::Failing)
        };

        if !should_check {
            continue;
        }

        // Create concurrent health check task
        let manager_arc_clone = Arc::clone(&manager_arc);
        health_check_tasks.push(async move {
            let is_healthy = {
                let manager = manager_arc_clone.lock().await;
                manager.health().await.unwrap_or(false)
            };
            HealthCheckResult {
                name: service_name,
                manager: manager_arc,
                is_healthy,
            }
        });
    }

    // Run all health checks concurrently
    futures::future::join_all(health_check_tasks).await
}

/// Classify health results into healthy and unhealthy services.
///
/// Returns:
/// - `healthy_names`: Vec of service names that passed health check
/// - `unhealthy`: Vec of (name, manager) for services that failed
fn classify_health_results(
    results: Vec<HealthCheckResult>,
) -> (Vec<String>, Vec<(String, ServiceEntry)>) {
    let mut healthy_names = Vec::new();
    let mut unhealthy = Vec::new();

    for result in results {
        if result.is_healthy {
            healthy_names.push(result.name);
        } else {
            tracing::warn!("Service '{}' failed health check", result.name);
            unhealthy.push((result.name, result.manager));
        }
    }

    (healthy_names, unhealthy)
}

/// Determine if a service should be restarted based on restart policy.
///
/// This is a pure function for easy testing.
///
/// # Arguments
/// * `restart_policy` - The configured restart policy for the service
/// * `consecutive_failures` - Number of consecutive failures (1-indexed)
///
/// # Returns
/// `true` if the service should be restarted
pub(super) fn should_restart_service(
    restart_policy: &RestartPolicy,
    consecutive_failures: u32,
) -> bool {
    // Note on restart semantics:
    // - max_retries = number of restart attempts allowed
    // - consecutive_failures is 1-indexed (first failure = 1)
    // - Example: max_retries=3 allows failures 1,2,3 to restart
    //   but failure 4 will not restart (3 restart attempts made)
    match restart_policy {
        RestartPolicy::No => false,
        RestartPolicy::Always => true,
        RestartPolicy::OnFailure { max_retries } => {
            if let Some(max) = max_retries {
                consecutive_failures <= *max
            } else {
                true
            }
        }
    }
}

/// Restart a single service with backoff delay.
///
/// # Returns
/// `true` if restart was successful
async fn restart_single_service(
    name: &str,
    manager_arc: &ServiceEntry,
    consecutive_failures: u32,
    state_tracker: &StateTrackerRef,
    cancel_token: &CancellationToken,
) -> bool {
    // Calculate and apply backoff delay. The backoff can be up to ~90s, so it
    // must be cancellable: cleanup() only waits a few seconds for this task,
    // and restarting after shutdown would spawn a process nothing tracks.
    let delay = calculate_backoff_delay(consecutive_failures);
    if delay.as_secs() > 0 {
        tracing::info!(
            "Waiting {}s before restarting '{}' (attempt {})",
            delay.as_secs(),
            name,
            consecutive_failures
        );
        tokio::select! {
            _ = cancel_token.cancelled() => {
                tracing::info!("Restart of '{}' cancelled during backoff", name);
                return false;
            }
            _ = tokio::time::sleep(delay) => {}
        }
    }

    // Lock and restart the service
    let start_result = {
        let mut manager = manager_arc.lock().await;

        if cancel_token.is_cancelled() {
            tracing::info!("Restart of '{}' cancelled", name);
            return false;
        }

        // The user may have stopped the service while we were backing off —
        // resurrecting it here would leave a running process with no state
        // entry (it was unregistered by the stop).
        if matches!(manager.status(), Status::Stopped | Status::Stopping) {
            tracing::info!(
                "Skipping restart of '{}': service was stopped during backoff",
                name
            );
            return false;
        }

        tracing::info!("Restarting service '{}'", name);

        // Stop
        if let Err(e) = manager.stop().await {
            tracing::error!("Failed to stop service '{}': {}", name, e);
        }

        // Brief delay to ensure clean shutdown
        tokio::time::sleep(Duration::from_millis(500)).await;

        // Start
        manager.start().await
    }; // manager lock released

    match start_result {
        Ok(_) => {
            tracing::info!("Successfully restarted service '{}'", name);

            // Record the new PID/container ID — the old ones are dead, and a
            // future fed process restoring from the DB would otherwise treat
            // the restarted service as stale (or adopt a reused PID).
            // LOCK ORDER: state_tracker before service mutex (see lock_order.rs).
            let mut tracker = state_tracker.write().await;
            {
                let manager = manager_arc.lock().await;
                if let Some(pid) = manager.get_pid() {
                    if let Err(e) = tracker.update_service_pid(name, pid).await {
                        tracing::warn!("Failed to update PID for restarted '{}': {}", name, e);
                    }
                }
                if let Some(container_id) = manager.get_container_id() {
                    if let Err(e) = tracker
                        .update_service_container_id(name, container_id)
                        .await
                    {
                        tracing::warn!(
                            "Failed to update container ID for restarted '{}': {}",
                            name,
                            e
                        );
                    }
                }
            }
            if let Err(e) = tracker.save().await {
                tracing::warn!("Failed to save state after restarting '{}': {}", name, e);
            }
            true
        }
        Err(e) => {
            tracing::error!("Failed to restart service '{}': {}", name, e);
            false
        }
    }
}

/// Execute a complete health check cycle.
///
/// This function orchestrates:
/// 1. Concurrent health checks for all services
/// 2. Classification of results
/// 3. State tracker updates
/// 4. Circuit breaker checks and updates
/// 5. Restart handling for unhealthy services
/// 6. Dependency health propagation (if configured)
///
/// Note: Dead service cleanup is NOT done here - it runs on startup only.
/// This keeps the hot loop fast and predictable.
async fn execute_health_check_cycle(
    services: &ServicesMap,
    state_tracker: &StateTrackerRef,
    config: &Config,
    cancel_token: &CancellationToken,
) {
    // Check all services concurrently
    let health_results = check_all_services(services).await;

    // Classify results
    let (healthy_names, unhealthy) = classify_health_results(health_results);

    // Extract unhealthy names for batch update and dependency propagation
    let unhealthy_names: Vec<String> = unhealthy.iter().map(|(n, _)| n.clone()).collect();

    // Batch update health status with single lock acquisition
    let failure_counts = {
        let mut tracker = state_tracker.write().await;
        tracker
            .batch_health_update(healthy_names.clone(), unhealthy_names.clone())
            .await
            .unwrap_or_default()
    };

    // Close circuit breakers and clear restart history for healthy services
    if !healthy_names.is_empty() {
        let mut tracker = state_tracker.write().await;
        for service_name in &healthy_names {
            // Close circuit breaker when service becomes healthy
            if let Err(e) = tracker.close_circuit_breaker(service_name).await {
                tracing::warn!(
                    "Failed to close circuit breaker for '{}': {}",
                    service_name,
                    e
                );
            }
            // Clear restart history to reset the crash loop counter
            if let Err(e) = tracker.clear_restart_history(service_name).await {
                tracing::warn!(
                    "Failed to clear restart history for '{}': {}",
                    service_name,
                    e
                );
            }
        }
    }

    // Process restarts for unhealthy services
    let mut successful_restarts = Vec::new();

    for (service_name, manager_arc) in unhealthy {
        let consecutive_failures = failure_counts.get(&service_name).copied().unwrap_or(1);

        // Get service config for circuit breaker and restart settings
        let service_config = config.services.get(&service_name);
        let circuit_breaker = service_config
            .and_then(|s| s.circuit_breaker.clone())
            .unwrap_or_default();

        // Check if circuit breaker is open (in cooldown)
        let is_circuit_open = {
            let tracker = state_tracker.read().await;
            tracker.is_circuit_breaker_open(&service_name).await
        };

        if is_circuit_open {
            // Get remaining cooldown time for better logging
            let remaining = {
                let tracker = state_tracker.read().await;
                match tracker.get_circuit_breaker_remaining(&service_name).await {
                    Ok(Some(secs)) => secs,
                    Ok(None) => 0,
                    Err(e) => {
                        tracing::warn!(
                            "Failed to read circuit breaker remaining for '{}': {}",
                            service_name,
                            e
                        );
                        0
                    }
                }
            };

            tracing::warn!(
                "Service '{}' circuit breaker is open (crash loop detected) - \
                 skipping restart, {}s remaining in cooldown",
                service_name,
                remaining
            );
            continue;
        }

        // Get restart policy
        let restart_policy = service_config
            .and_then(|s| s.restart.clone())
            .unwrap_or(RestartPolicy::No);

        if should_restart_service(&restart_policy, consecutive_failures) {
            // Record restart attempt BEFORE restarting for circuit breaker tracking
            {
                let mut tracker = state_tracker.write().await;
                if let Err(e) = tracker.record_restart(&service_name).await {
                    tracing::warn!("Failed to record restart for '{}': {}", service_name, e);
                }
            }

            // Check if we should trip the circuit breaker
            let should_trip = {
                let tracker = state_tracker.read().await;
                tracker
                    .check_circuit_breaker(
                        &service_name,
                        circuit_breaker.restart_threshold,
                        circuit_breaker.window_secs,
                    )
                    .await
                    .unwrap_or(false)
            };

            if should_trip {
                // Trip the circuit breaker
                {
                    let mut tracker = state_tracker.write().await;
                    if let Err(e) = tracker
                        .open_circuit_breaker(&service_name, circuit_breaker.cooldown_secs)
                        .await
                    {
                        tracing::warn!(
                            "Failed to open circuit breaker for '{}': {}",
                            service_name,
                            e
                        );
                    }
                }

                tracing::error!(
                    "Service '{}' crash loop detected ({} restarts in {}s) - \
                     circuit breaker opened for {}s. Check logs with: fed logs {}",
                    service_name,
                    circuit_breaker.restart_threshold,
                    circuit_breaker.window_secs,
                    circuit_breaker.cooldown_secs,
                    service_name
                );
                continue;
            }

            if restart_single_service(
                &service_name,
                &manager_arc,
                consecutive_failures,
                state_tracker,
                cancel_token,
            )
            .await
            {
                successful_restarts.push(service_name);
            }
        } else {
            tracing::warn!(
                "Service '{}' exceeded restart limit (failures: {})",
                service_name,
                consecutive_failures
            );
        }
    }

    // Batch update restart counts for successful restarts
    if !successful_restarts.is_empty() {
        let mut tracker = state_tracker.write().await;
        if let Err(e) = tracker
            .batch_increment_restart_counts(successful_restarts)
            .await
        {
            tracing::warn!("Failed to batch increment restart counts: {}", e);
        }
    }

    // Handle dependency health propagation if there are unhealthy services
    if !unhealthy_names.is_empty() {
        handle_dependency_health_propagation(&unhealthy_names, services, state_tracker, config)
            .await;
    }
}

/// Handle dependency health propagation.
///
/// When a service becomes unhealthy, check all services that depend on it
/// and apply their configured on_failure policies:
/// - Stop: Stop the dependent service
/// - Restart: Restart the dependent service
/// - Ignore: Do nothing
///
/// This is called after health checks complete to propagate dependency failures.
async fn handle_dependency_health_propagation(
    unhealthy_services: &[String],
    services: &ServicesMap,
    state_tracker: &StateTrackerRef,
    config: &Config,
) {
    use crate::config::DependencyFailurePolicy;

    // Build reverse dependency map: dependency -> dependents
    let mut reverse_deps: HashMap<String, Vec<(String, DependencyFailurePolicy)>> = HashMap::new();

    for (service_name, service_config) in &config.services {
        for depends_on in &service_config.depends_on {
            let dep_name = depends_on.service_name();
            let policy = depends_on.failure_policy();
            reverse_deps
                .entry(dep_name.to_string())
                .or_default()
                .push((service_name.clone(), policy));
        }
    }

    // For each unhealthy service, check its dependents
    for failed_service in unhealthy_services {
        if let Some(dependents) = reverse_deps.get(failed_service) {
            for (dependent_name, policy) in dependents {
                // Check if dependent is currently running
                let dependent_status = {
                    let tracker = state_tracker.read().await;
                    tracker.get_service(dependent_name).await.map(|s| s.status)
                };

                let is_running = matches!(
                    dependent_status,
                    Some(status) if status == Status::Running || status == Status::Healthy
                );

                if !is_running {
                    continue;
                }

                match policy {
                    DependencyFailurePolicy::Stop => {
                        tracing::warn!(
                            "Dependency '{}' failed - stopping dependent service '{}'",
                            failed_service,
                            dependent_name
                        );

                        // Find the service manager and stop it
                        let manager_opt = {
                            let svcs = services.read().await;
                            svcs.get(dependent_name).map(Arc::clone)
                        };

                        if let Some(manager_arc) = manager_opt {
                            // LOCK ORDER: scope manager mutex, release it,
                            // then acquire state_tracker separately.
                            let stop_result = {
                                let mut manager = manager_arc.lock().await;
                                manager.stop().await
                            };
                            // manager lock released

                            if let Err(e) = &stop_result {
                                tracing::error!(
                                    "Failed to stop dependent service '{}': {}",
                                    dependent_name,
                                    e,
                                );
                            } else {
                                let mut tracker = state_tracker.write().await;
                                let _ = tracker
                                    .update_service_status(dependent_name, Status::Stopped)
                                    .await;
                            }
                        }
                    }
                    DependencyFailurePolicy::Restart => {
                        tracing::warn!(
                            "Dependency '{}' failed - restarting dependent service '{}'",
                            failed_service,
                            dependent_name
                        );

                        // Find the service manager and restart it
                        let manager_opt = {
                            let svcs = services.read().await;
                            svcs.get(dependent_name).map(Arc::clone)
                        };

                        if let Some(manager_arc) = manager_opt {
                            // LOCK ORDER: scope manager mutex, release it,
                            // then acquire state_tracker separately.
                            let stop_result = {
                                let mut manager = manager_arc.lock().await;
                                manager.stop().await
                            };
                            // manager lock released

                            if let Err(e) = &stop_result {
                                tracing::error!(
                                    "Failed to stop dependent service '{}' for restart: {}",
                                    dependent_name,
                                    e,
                                );
                                continue;
                            }

                            // Update state to Stopped
                            {
                                let mut tracker = state_tracker.write().await;
                                let _ = tracker
                                    .update_service_status(dependent_name, Status::Stopped)
                                    .await;
                            }

                            // LOCK ORDER: same pattern — scope mutex, then
                            // acquire state_tracker.
                            let start_result = {
                                let mut manager = manager_arc.lock().await;
                                manager.start().await
                            };
                            // manager lock released

                            match start_result {
                                Ok(_) => {
                                    let mut tracker = state_tracker.write().await;
                                    let _ = tracker
                                        .update_service_status(dependent_name, Status::Starting)
                                        .await;
                                }
                                Err(e) => {
                                    tracing::error!(
                                        "Failed to restart dependent service '{}': {}",
                                        dependent_name,
                                        e
                                    );
                                }
                            }
                        }
                    }
                    DependencyFailurePolicy::Ignore => {
                        // Do nothing - dependent continues running despite dependency failure
                        tracing::debug!(
                            "Dependency '{}' failed but dependent service '{}' configured to ignore",
                            failed_service,
                            dependent_name
                        );
                    }
                }
            }
        }
    }
}

/// Add small random jitter to health check interval.
///
/// Prevents exact synchronization between multiple fed instances.
/// Uses ±500ms (±10% of 5 second interval) for predictable timing.
async fn apply_health_check_jitter() {
    use rand::Rng;
    let jitter_ms = {
        let mut rng = rand::thread_rng();
        rng.gen_range(0..=500) // ±500ms max jitter
    };
    tokio::time::sleep(Duration::from_millis(jitter_ms)).await;
}

/// Run the monitoring loop with panic recovery.
///
/// Wraps each health check cycle in panic handling to prevent
/// silent monitoring death from unexpected panics.
async fn run_monitoring_loop(
    services: ServicesMap,
    state_tracker: StateTrackerRef,
    config: Config,
    cancel_token: CancellationToken,
    startup_complete: Arc<std::sync::atomic::AtomicBool>,
) {
    use futures::FutureExt;
    use std::panic::AssertUnwindSafe;

    // Health checks run every 5 seconds. Combined with up to ±500ms jitter,
    // cancellation may take up to ~5.5s to take effect (worst case: check just started).
    let mut interval = tokio::time::interval(Duration::from_secs(5));

    loop {
        tokio::select! {
            _ = cancel_token.cancelled() => {
                tracing::debug!("Monitoring loop shutting down");
                break;
            }
            _ = interval.tick() => {
                apply_health_check_jitter().await;

                // Skip all monitoring during startup to prevent race conditions
                if !startup_complete.load(Ordering::SeqCst) {
                    continue;
                }

                // Clone references for the panic-safe closure
                let services_clone = Arc::clone(&services);
                let state_tracker_clone = Arc::clone(&state_tracker);
                let config_clone = config.clone();

                let cancel_token_clone = cancel_token.clone();
                let health_check_result = AssertUnwindSafe(async {
                    execute_health_check_cycle(
                        &services_clone,
                        &state_tracker_clone,
                        &config_clone,
                        &cancel_token_clone,
                    )
                    .await;
                })
                .catch_unwind()
                .await;

                // Log any panics but continue monitoring
                if let Err(panic_info) = health_check_result {
                    let panic_msg = if let Some(s) = panic_info.downcast_ref::<&str>() {
                        s.to_string()
                    } else if let Some(s) = panic_info.downcast_ref::<String>() {
                        s.clone()
                    } else {
                        "Unknown panic".to_string()
                    };
                    tracing::error!(
                        "Monitoring health check cycle panicked: {}. Continuing monitoring...",
                        panic_msg
                    );
                }
            }
        }
    }
}

impl Orchestrator {
    /// Check if a service is healthy
    pub async fn check_health(&self, service_name: &str) -> Result<bool> {
        // Get Arc clone of manager
        let manager_arc = {
            let services = self.services.read().await;
            if let Some(arc) = services.get(service_name) {
                Arc::clone(arc)
            } else {
                return Err(Error::ServiceNotFound(service_name.to_string()));
            }
        };

        // First check if service is running
        {
            let manager = manager_arc.lock().await;
            let status = manager.status();
            if status != Status::Running && status != Status::Healthy {
                return Ok(false);
            }
        }

        // Use health checker if available
        if let Some(checker) = self.health_checkers.read().await.get(service_name) {
            return checker.check().await;
        }

        // Fall back to service's own health check
        {
            let manager = manager_arc.lock().await;
            manager.health().await
        }
    }

    /// Start monitoring services in the background
    /// Checks health periodically and restarts failed services according to restart policy
    pub(super) async fn start_monitoring(&self) {
        if self.output_mode.is_file() {
            // Skip monitoring in File mode (background/detached services)
            return;
        }

        let services = Arc::clone(&self.services);
        let config = self.config.clone();
        let state_tracker = Arc::clone(&self.state_tracker);
        let cancel_token = self.child_token();
        let startup_complete = Arc::clone(&self.startup_complete);

        let handle = tokio::spawn(run_monitoring_loop(
            services,
            state_tracker,
            config,
            cancel_token,
            startup_complete,
        ));

        // Store the handle so we can await it during cleanup
        let mut task = self.monitoring_task.lock().await;
        *task = Some(handle);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_should_restart_no_policy() {
        assert!(!should_restart_service(&RestartPolicy::No, 1));
        assert!(!should_restart_service(&RestartPolicy::No, 5));
    }

    #[test]
    fn test_should_restart_always_policy() {
        assert!(should_restart_service(&RestartPolicy::Always, 1));
        assert!(should_restart_service(&RestartPolicy::Always, 100));
    }

    #[test]
    fn test_should_restart_on_failure_with_limit() {
        let policy = RestartPolicy::OnFailure {
            max_retries: Some(3),
        };
        assert!(should_restart_service(&policy, 1));
        assert!(should_restart_service(&policy, 2));
        assert!(should_restart_service(&policy, 3));
        assert!(!should_restart_service(&policy, 4));
        assert!(!should_restart_service(&policy, 10));
    }

    #[test]
    fn test_should_restart_on_failure_unlimited() {
        let policy = RestartPolicy::OnFailure { max_retries: None };
        assert!(should_restart_service(&policy, 1));
        assert!(should_restart_service(&policy, 100));
        assert!(should_restart_service(&policy, 1000));
    }

    #[test]
    fn test_calculate_backoff_zero_failures() {
        let delay = calculate_backoff_delay(0);
        assert_eq!(delay, Duration::from_secs(0));
    }

    #[test]
    fn test_calculate_backoff_exponential() {
        // Test multiple times to account for jitter
        for _ in 0..10 {
            let delay1 = calculate_backoff_delay(1);
            assert!(delay1.as_secs() <= 2);

            let delay2 = calculate_backoff_delay(2);
            assert!(delay2.as_secs() >= 1 && delay2.as_secs() <= 3);

            let delay3 = calculate_backoff_delay(3);
            assert!(delay3.as_secs() >= 2 && delay3.as_secs() <= 6);
        }
    }

    #[test]
    fn test_calculate_backoff_capped() {
        // Test that backoff is capped at ~60 seconds (with jitter: 30-90)
        for _ in 0..10 {
            let delay = calculate_backoff_delay(10);
            assert!(delay.as_secs() <= 90);
        }
    }
}
