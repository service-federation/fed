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

use crate::config::{Config, DependencyFailurePolicy, RestartPolicy};
use crate::error::{Error, Result};
use crate::service::{ServiceManager, Status};
use crate::state::StateTracker;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::Ordering;
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
/// # Scope
///
/// `scope`, if `Some`, restricts health-checking to services whose name is
/// in the set — everything else is skipped entirely (not even a liveness
/// check), matching the supervisor's filtered-monitoring policy
/// (`07-supervisor.md` Design §2; see [`supervised_service_names`] for the
/// scope formula). `None` preserves today's behavior exactly: every
/// Running/Healthy/Failing service is checked, unfiltered — this is what
/// `--watch`/`fed tui` (via [`Orchestrator::start_monitoring`]) always pass,
/// so their behavior is bit-identical to before this parameter existed.
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
async fn check_all_services(
    services: &ServicesMap,
    scope: Option<&HashSet<String>>,
) -> Vec<HealthCheckResult> {
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
        // Out-of-scope services are skipped entirely — not even a liveness
        // check — so monitoring a large project costs nothing for services
        // nobody asked to have supervised.
        if let Some(scope) = scope
            && !scope.contains(&service_name)
        {
            continue;
        }

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

/// Compute the supervisor's health-check scope: the union of every service
/// with a restart policy plus every dependency target whose failure someone
/// has explicitly opted into caring about.
///
/// `restart:` and `depends_on: ...on_failure:` are two separate,
/// already-shipped, orthogonal features — a service can have `restart: no`
/// and still configure `on_failure: restart` against one of its
/// dependencies, wanting to come back specifically when that dependency
/// fails, with no general self-healing otherwise. Narrowing scope to "only
/// services with `restart != No`" would silently stop firing `on_failure`
/// handling for any dependency chain where the *failing* link has
/// `restart: no` — under plain `fed start`, since that link would no longer
/// be health-checked at all, its failure would never be detected, so
/// nothing downstream would ever be told about it.
///
/// ```text
/// scope = { s : s.restart != No }
///       ∪ { d : d is a depends_on target of some s where either
///               s.restart != No, or
///               s's failure_policy for that dependency is not Ignore }
/// ```
///
/// In words: check the health of every service that either wants its *own*
/// crashes healed, or whose failure someone else has explicitly said they
/// care about via `on_failure`. This is less obvious than "just
/// restart-policy services" and is a case a future maintainer could
/// plausibly "simplify" back into the parity gap above — don't.
///
/// `handle_dependency_health_propagation` itself is unaffected by this
/// function and continues to act on the full dependent set regardless of a
/// dependent's own restart policy — narrowing *that* too would break the
/// existing, shipped `on_failure` feature for services with `restart: no`.
///
/// Recomputed once per supervisor tick (cheap; called fresh from `config`
/// each cycle) so a later `fed start <new-service>` in the same directory is
/// picked up without restarting the supervisor.
///
/// Re-exported as `crate::orchestrator::supervised_service_names` (see
/// `orchestrator/mod.rs`) so `fed status --json`'s per-service
/// `supervised_by` field (`07-supervisor.md` Design §4) can compute the same
/// scope the supervisor itself uses, without status.rs reimplementing the
/// union formula.
pub fn supervised_service_names(config: &Config) -> HashSet<String> {
    let mut scope: HashSet<String> = HashSet::new();

    for (name, service) in &config.services {
        let restart_enabled = !matches!(
            service.restart.clone().unwrap_or(RestartPolicy::No),
            RestartPolicy::No
        );
        if restart_enabled {
            scope.insert(name.clone());
        }

        for depends_on in &service.depends_on {
            let someone_cares = restart_enabled
                || !matches!(depends_on.failure_policy(), DependencyFailurePolicy::Ignore);
            if someone_cares {
                scope.insert(depends_on.service_name().to_string());
            }
        }
    }

    scope
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
                if let Some(pid) = manager.get_pid()
                    && let Err(e) = tracker.update_service_pid(name, pid).await
                {
                    tracing::warn!("Failed to update PID for restarted '{}': {}", name, e);
                }
                if let Some(container_id) = manager.get_container_id()
                    && let Err(e) = tracker
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
///
/// `scope` is forwarded verbatim to [`check_all_services`] — see that
/// function's doc comment for the `None`-means-unfiltered contract.
async fn execute_health_check_cycle(
    services: &ServicesMap,
    state_tracker: &StateTrackerRef,
    config: &Config,
    cancel_token: &CancellationToken,
    scope: Option<&HashSet<String>>,
) {
    // Check all services concurrently
    let health_results = check_all_services(services, scope).await;

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
        // A service the user explicitly stopped (any of the three `fed
        // stop` paths — whole-project, per-service, or the config-can't-
        // load fallback) must never be treated as crash-looping. This is
        // the persisted, cross-process signal (`07-supervisor.md` Design
        // §1): a separate `fed stop` invocation never touches this
        // process's manager objects, but it does write `desired_state`, so
        // gating here — rather than only on `restart_single_service`'s own
        // same-process `manager.status()` check below — is what makes
        // `fed stop` reliably prevent resurrection across processes (the
        // supervisor's whole reason for existing). Skipped entirely,
        // before any circuit-breaker/restart-history accounting, so a
        // stopped service is never counted as a crash.
        let desired_running = {
            let tracker = state_tracker.read().await;
            tracker.is_desired_running(&service_name).await
        };
        if !desired_running {
            tracing::debug!(
                "Service '{}' is desired_state=stopped — skipping restart/circuit-breaker accounting",
                service_name
            );
            continue;
        }

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
///
/// `filter_scope`: when `false`, every cycle passes `None` to
/// [`execute_health_check_cycle`] — bit-identical to the pre-existing,
/// unfiltered `--watch`/`tui` behavior. When `true`, [`supervised_service_names`]
/// is recomputed fresh from `config` on *every* tick (cheap — a HashMap scan)
/// rather than once at spawn time, so a later `fed start <new-service>` in
/// the same directory would be picked up without restarting the supervisor,
/// if `config` were ever live-reloaded (not yet implemented — but this keeps
/// the loop shaped so that adding it later doesn't require touching this
/// function again).
async fn run_monitoring_loop(
    services: ServicesMap,
    state_tracker: StateTrackerRef,
    config: Config,
    cancel_token: CancellationToken,
    startup_complete: Arc<std::sync::atomic::AtomicBool>,
    filter_scope: bool,
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

                // Recomputed fresh each tick, not cached across ticks — see
                // this function's doc comment.
                let scope = if filter_scope {
                    Some(supervised_service_names(&config_clone))
                } else {
                    None
                };

                let cancel_token_clone = cancel_token.clone();
                let health_check_result = AssertUnwindSafe(async {
                    execute_health_check_cycle(
                        &services_clone,
                        &state_tracker_clone,
                        &config_clone,
                        &cancel_token_clone,
                        scope.as_ref(),
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

        self.spawn_monitoring_loop(false).await;
    }

    /// Start monitoring unconditionally, regardless of output mode, scoped to
    /// [`supervised_service_names`] rather than every service.
    ///
    /// Used exclusively by [`Orchestrator::initialize_supervisor`]
    /// (`07-supervisor.md` Design §1/§2/§6): a supervisor exists precisely to
    /// watch backgrounded (`OutputMode::File`) services, so the
    /// output-mode skip in [`Orchestrator::start_monitoring`] above must not
    /// apply, and its health-check scope must be the restart-policy/
    /// dependency union rather than "every service" (the cost argument for
    /// scoping only matters once monitoring actually runs unconditionally).
    pub(super) async fn start_monitoring_for_supervisor(&self) {
        self.spawn_monitoring_loop(true).await;
    }

    /// Shared spawn logic for [`Orchestrator::start_monitoring`] and
    /// [`Orchestrator::start_monitoring_for_supervisor`] — the only
    /// difference between the two call sites is whether the loop filters its
    /// health-check scope.
    async fn spawn_monitoring_loop(&self, filter_scope: bool) {
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
            filter_scope,
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

    // --- supervised_service_names (07-supervisor.md Design §2 union formula) ---

    fn service_with_restart(restart: Option<RestartPolicy>) -> crate::config::Service {
        crate::config::Service {
            process: Some("sleep 300".to_string()),
            restart,
            ..Default::default()
        }
    }

    #[test]
    fn test_supervised_service_names_restart_policy_included() {
        let mut config = Config::default();
        config.services.insert(
            "always".to_string(),
            service_with_restart(Some(RestartPolicy::Always)),
        );
        config.services.insert(
            "on-failure".to_string(),
            service_with_restart(Some(RestartPolicy::OnFailure {
                max_retries: Some(3),
            })),
        );
        config.services.insert(
            "no-restart".to_string(),
            service_with_restart(Some(RestartPolicy::No)),
        );
        config
            .services
            .insert("unset".to_string(), service_with_restart(None));

        let scope = supervised_service_names(&config);

        assert!(scope.contains("always"));
        assert!(scope.contains("on-failure"));
        assert!(
            !scope.contains("no-restart"),
            "restart: no must not be in scope on its own"
        );
        assert!(
            !scope.contains("unset"),
            "unset restart (defaults to No) must not be in scope on its own"
        );
    }

    #[test]
    fn test_supervised_service_names_union_includes_on_failure_restart_dependency() {
        use crate::config::{DependencyFailurePolicy, DependsOn};

        // `dependent` has restart: no, but explicitly opts in to
        // on_failure: restart against `dependency` — dependency must be in
        // scope even though dependent itself is not, and even though
        // dependency's own restart policy is `no`. This is the direct test
        // for the union formula, not just "restart != No".
        let mut config = Config::default();
        config.services.insert(
            "dependency".to_string(),
            service_with_restart(Some(RestartPolicy::No)),
        );
        let mut dependent = service_with_restart(Some(RestartPolicy::No));
        dependent.depends_on = vec![DependsOn::Structured {
            service: "dependency".to_string(),
            on_failure: DependencyFailurePolicy::Restart,
        }];
        config.services.insert("dependent".to_string(), dependent);

        let scope = supervised_service_names(&config);

        assert!(
            scope.contains("dependency"),
            "dependency must be in scope: dependent explicitly cares about its failure"
        );
        assert!(
            !scope.contains("dependent"),
            "dependent itself has restart: no and isn't anyone's on_failure target"
        );
    }

    #[test]
    fn test_supervised_service_names_ignore_policy_excluded() {
        use crate::config::{DependencyFailurePolicy, DependsOn};

        // `dependent` explicitly ignores dependency's failures and has no
        // restart policy of its own — dependency must NOT be pulled into
        // scope just because a depends_on edge exists.
        let mut config = Config::default();
        config.services.insert(
            "dependency".to_string(),
            service_with_restart(Some(RestartPolicy::No)),
        );
        let mut dependent = service_with_restart(Some(RestartPolicy::No));
        dependent.depends_on = vec![DependsOn::Structured {
            service: "dependency".to_string(),
            on_failure: DependencyFailurePolicy::Ignore,
        }];
        config.services.insert("dependent".to_string(), dependent);

        let scope = supervised_service_names(&config);

        assert!(
            !scope.contains("dependency"),
            "an Ignore-policy dependency with no restart policy of its own must stay out of scope"
        );
    }

    #[test]
    fn test_supervised_service_names_default_depends_on_policy_counts_as_caring() {
        use crate::config::DependsOn;

        // Plain `depends_on: [dependency]` (no explicit on_failure) defaults
        // to DependencyFailurePolicy::Stop, not Ignore — only Ignore is
        // excluded from the union per Design §2, so this must still pull
        // `dependency` into scope.
        let mut config = Config::default();
        config.services.insert(
            "dependency".to_string(),
            service_with_restart(Some(RestartPolicy::No)),
        );
        let mut dependent = service_with_restart(Some(RestartPolicy::No));
        dependent.depends_on = vec![DependsOn::Simple("dependency".to_string())];
        config.services.insert("dependent".to_string(), dependent);

        let scope = supervised_service_names(&config);
        assert!(
            scope.contains("dependency"),
            "default on_failure policy is Stop (not Ignore), so it counts as \
             'someone cares about this dependency's failure' per the union formula"
        );
    }

    #[test]
    fn test_supervised_service_names_restart_dependent_pulls_in_dependency_regardless_of_edge_policy()
     {
        use crate::config::{DependencyFailurePolicy, DependsOn};

        // `dependent` has restart: always — its OWN restart policy makes it
        // care about everything it depends on (the union formula's first
        // disjunct), even if the depends_on edge's own failure policy is
        // Ignore.
        let mut config = Config::default();
        config.services.insert(
            "dependency".to_string(),
            service_with_restart(Some(RestartPolicy::No)),
        );
        let mut dependent = service_with_restart(Some(RestartPolicy::Always));
        dependent.depends_on = vec![DependsOn::Structured {
            service: "dependency".to_string(),
            on_failure: DependencyFailurePolicy::Ignore,
        }];
        config.services.insert("dependent".to_string(), dependent);

        let scope = supervised_service_names(&config);
        assert!(scope.contains("dependent"));
        assert!(
            scope.contains("dependency"),
            "dependent's own restart:always makes it care about dependency \
             regardless of the on_failure policy on that edge"
        );
    }

    // --- check_all_services scope filtering ---

    /// Minimal `ServiceManager` test double — only `status()`/`health()` are
    /// exercised by `check_all_services`.
    struct FakeManager {
        status: Status,
        healthy: bool,
    }

    #[async_trait::async_trait]
    impl ServiceManager for FakeManager {
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
            Ok(self.healthy)
        }
        fn status(&self) -> Status {
            self.status
        }
        fn name(&self) -> &str {
            "fake"
        }
        fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
            self
        }
    }

    fn fake_entry(status: Status, healthy: bool) -> ServiceEntry {
        Arc::new(Mutex::new(
            Box::new(FakeManager { status, healthy }) as Box<dyn ServiceManager>
        ))
    }

    /// `scope: None` must remain bit-identical to the pre-scope behavior:
    /// every Running/Healthy/Failing service gets checked.
    #[tokio::test]
    async fn test_check_all_services_scope_none_checks_everything() {
        let services: ServicesMap = Arc::new(RwLock::new(HashMap::from([
            ("a".to_string(), fake_entry(Status::Running, true)),
            ("b".to_string(), fake_entry(Status::Running, false)),
        ])));

        let results = check_all_services(&services, None).await;
        let names: HashSet<String> = results.iter().map(|r| r.name.clone()).collect();
        assert_eq!(names, HashSet::from(["a".to_string(), "b".to_string()]));
    }

    /// A service outside `scope` must not even get a liveness check — not
    /// just be excluded from restart consideration afterward.
    #[tokio::test]
    async fn test_check_all_services_scope_filters_out_of_scope_services() {
        let services: ServicesMap = Arc::new(RwLock::new(HashMap::from([
            ("supervised".to_string(), fake_entry(Status::Running, true)),
            (
                "unsupervised".to_string(),
                fake_entry(Status::Running, false),
            ),
        ])));

        let scope: HashSet<String> = HashSet::from(["supervised".to_string()]);
        let results = check_all_services(&services, Some(&scope)).await;

        assert_eq!(
            results.len(),
            1,
            "only the in-scope service should be checked"
        );
        assert_eq!(results[0].name, "supervised");
    }

    /// A service that's in scope but not in a checkable status
    /// (Running/Healthy/Failing) must still be skipped — scope narrows the
    /// candidate set, it doesn't override the existing status gate.
    #[tokio::test]
    async fn test_check_all_services_scope_still_respects_status_gate() {
        let services: ServicesMap = Arc::new(RwLock::new(HashMap::from([(
            "stopped-but-in-scope".to_string(),
            fake_entry(Status::Stopped, true),
        )])));

        let scope: HashSet<String> = HashSet::from(["stopped-but-in-scope".to_string()]);
        let results = check_all_services(&services, Some(&scope)).await;

        assert!(
            results.is_empty(),
            "in-scope but Stopped services must still be skipped, same as unfiltered behavior"
        );
    }

    /// Empty scope means nothing gets checked, even if everything is
    /// Running — the degenerate case of the union formula (no service has a
    /// restart policy and nothing depends on anything with non-Ignore
    /// failure handling).
    #[tokio::test]
    async fn test_check_all_services_empty_scope_checks_nothing() {
        let services: ServicesMap = Arc::new(RwLock::new(HashMap::from([(
            "a".to_string(),
            fake_entry(Status::Running, true),
        )])));

        let scope: HashSet<String> = HashSet::new();
        let results = check_all_services(&services, Some(&scope)).await;

        assert!(results.is_empty());
    }
}
