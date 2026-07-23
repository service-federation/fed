//! Docker utilities for container management.
//!
//! Provides shared functions for interacting with Docker containers,
//! consolidating duplicate implementations across the codebase.

pub mod client;
pub mod error;

pub use client::DockerClient;
pub use error::DockerError;

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

/// Cache duration for Docker daemon health status.
/// We cache for 5 seconds to avoid hammering `docker info` on every health check cycle.
const DAEMON_HEALTH_CACHE_DURATION: Duration = Duration::from_secs(5);

/// Cached daemon health status (atomic for lock-free access).
static DAEMON_HEALTHY: AtomicBool = AtomicBool::new(true);

/// Timestamp (as nanos since an arbitrary epoch) when daemon health was last checked.
/// Using AtomicU64 to store elapsed nanos from a reference point.
static DAEMON_HEALTH_CHECKED_AT: AtomicU64 = AtomicU64::new(0);

/// Reference instant for computing elapsed time (initialized on first use).
/// This is a workaround since Instant cannot be stored in a static directly.
static REFERENCE_INSTANT: std::sync::OnceLock<Instant> = std::sync::OnceLock::new();

fn get_reference_instant() -> &'static Instant {
    REFERENCE_INSTANT.get_or_init(Instant::now)
}

fn elapsed_nanos() -> u64 {
    get_reference_instant().elapsed().as_nanos() as u64
}

/// Check if the Docker daemon is healthy and responsive.
///
/// Uses `docker info` to verify the daemon is running. Results are cached
/// for 5 seconds to avoid excessive subprocess spawning during health check
/// cycles with many services.
///
/// # Returns
/// - `true` if daemon is healthy (or cache indicates healthy)
/// - `false` if daemon is unhealthy or unreachable
///
/// # Use Case
/// Call this before attributing container health check failures to containers.
/// If the daemon is down, individual container checks will fail even for
/// healthy containers - we should not trigger restarts in this case.
pub async fn is_daemon_healthy() -> bool {
    fn cache_lookup() -> Option<bool> {
        let last_checked = DAEMON_HEALTH_CHECKED_AT.load(Ordering::Relaxed);
        let now = elapsed_nanos();
        let cache_valid = last_checked > 0
            && now.saturating_sub(last_checked) < DAEMON_HEALTH_CACHE_DURATION.as_nanos() as u64;
        cache_valid.then(|| DAEMON_HEALTHY.load(Ordering::Relaxed))
    }

    if let Some(cached) = cache_lookup() {
        return cached;
    }

    // Single-flight the probe: concurrent health checks (all services at
    // once) must not each spawn `docker info` when the cache expires.
    static PROBE_GATE: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());
    let _gate = PROBE_GATE.lock().await;

    // Someone else may have refreshed the cache while we waited
    if let Some(cached) = cache_lookup() {
        return cached;
    }

    let healthy = check_daemon_health_uncached().await;

    // Update cache
    DAEMON_HEALTHY.store(healthy, Ordering::Relaxed);
    DAEMON_HEALTH_CHECKED_AT.store(elapsed_nanos(), Ordering::Relaxed);

    healthy
}

/// Perform an uncached Docker daemon health check.
///
/// Uses `docker info` with a short timeout. This command is lightweight
/// and reliable for checking daemon responsiveness.
async fn check_daemon_health_uncached() -> bool {
    DockerClient::new()
        .daemon_healthy(Duration::from_secs(2))
        .await
}

/// Check Docker daemon health with retry and exponential backoff.
///
/// This is useful when the daemon may be temporarily unavailable (during startup,
/// restart, or resource contention). A single check creates false negatives that
/// prevent cleanup or cause spurious failures.
///
/// # Parameters
/// - `max_attempts`: Maximum number of retry attempts (default: 3)
/// - `total_timeout`: Maximum total time to spend retrying (default: 10 seconds)
///
/// # Returns
/// - `true` if daemon becomes healthy within the timeout
/// - `false` if daemon remains unhealthy after all retries
///
/// # Retry Strategy
/// Exponential backoff: 100ms, 200ms, 400ms, 800ms, 1600ms...
pub async fn check_daemon_with_retry() -> bool {
    check_daemon_with_retry_custom(3, Duration::from_secs(10)).await
}

/// Check Docker daemon health with custom retry parameters.
///
/// This allows fine-tuning the retry behavior for different use cases.
/// For normal operations, use `check_daemon_with_retry()` instead.
pub async fn check_daemon_with_retry_custom(max_attempts: u32, total_timeout: Duration) -> bool {
    use tokio::time::{sleep, timeout};

    let start = std::time::Instant::now();
    let mut delay = Duration::from_millis(100);

    for attempt in 1..=max_attempts {
        // Check if we've exceeded total timeout
        if start.elapsed() >= total_timeout {
            tracing::debug!(
                "Docker daemon health check timeout after {:?}",
                start.elapsed()
            );
            return false;
        }

        // Try to check daemon health
        let remaining = total_timeout.saturating_sub(start.elapsed());
        let check = timeout(remaining, check_daemon_health_uncached()).await;

        match check {
            Ok(true) => {
                if attempt > 1 {
                    tracing::info!(
                        "Docker daemon became healthy after {} attempts ({:?})",
                        attempt,
                        start.elapsed()
                    );
                }
                return true;
            }
            Ok(false) | Err(_) => {
                if attempt < max_attempts {
                    tracing::debug!(
                        "Docker daemon health check attempt {}/{} failed, retrying in {:?}",
                        attempt,
                        max_attempts,
                        delay
                    );
                    sleep(delay).await;
                    // Exponential backoff, capped at 2 seconds
                    delay = (delay * 2).min(Duration::from_secs(2));
                } else {
                    tracing::warn!(
                        "Docker daemon unhealthy after {} attempts ({:?})",
                        max_attempts,
                        start.elapsed()
                    );
                }
            }
        }
    }

    false
}

/// Synchronous version of daemon health check.
///
/// Note: This does NOT use the cache since it's typically called from
/// contexts where we want a fresh check. For cached behavior, use
/// the async version.
pub fn is_daemon_healthy_sync() -> bool {
    DockerClient::new().daemon_healthy_sync()
}

/// Check if a Docker container is running (synchronous version).
///
/// Uses `docker inspect` to check the container's running state.
/// Returns `false` if the container doesn't exist or Docker is unavailable.
///
/// Use this in synchronous contexts where async is not available.
pub fn is_container_running_sync(container_id: &str) -> bool {
    DockerClient::new().is_running_sync(container_id)
}

/// Check if a Docker container is running (asynchronous version).
///
/// Uses `docker inspect` to check the container's running state.
/// Returns `false` if the container doesn't exist or Docker is unavailable.
///
/// Use this in async contexts where non-blocking I/O is preferred.
pub async fn is_container_running(container_id: &str) -> bool {
    DockerClient::new()
        .is_running(container_id, Duration::from_secs(5))
        .await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_nonexistent_container_sync() {
        // A random container ID should not exist
        assert!(!is_container_running_sync("nonexistent-container-12345"));
    }

    #[tokio::test]
    async fn test_nonexistent_container_async() {
        // A random container ID should not exist
        assert!(!is_container_running("nonexistent-container-12345").await);
    }

    #[test]
    fn test_daemon_health_sync() {
        // This test verifies the sync daemon check works.
        // On a machine with Docker installed, this should return true.
        // On a machine without Docker, this should return false.
        // Either way, it should not panic.
        let _healthy = is_daemon_healthy_sync();
    }

    #[tokio::test]
    async fn test_daemon_health_async() {
        // This test verifies the async daemon check works with caching.
        // First call populates the cache.
        let healthy1 = is_daemon_healthy().await;

        // Second call should use cache (same result, but faster)
        let healthy2 = is_daemon_healthy().await;

        // Results should be consistent
        assert_eq!(healthy1, healthy2);
    }

    #[test]
    fn test_elapsed_nanos_increases() {
        // Verify our time tracking mechanism works
        let t1 = elapsed_nanos();
        std::thread::sleep(std::time::Duration::from_millis(10));
        let t2 = elapsed_nanos();
        assert!(t2 > t1, "elapsed_nanos should increase over time");
    }

    #[tokio::test]
    async fn test_daemon_retry_respects_timeout() {
        // Test that retry logic respects total timeout
        let start = std::time::Instant::now();

        // Use a very short timeout to ensure test completes quickly
        // Even if Docker daemon is healthy, this tests the timeout mechanism
        let _result = check_daemon_with_retry_custom(10, Duration::from_millis(50)).await;

        let elapsed = start.elapsed();

        // Should not exceed timeout by a large margin. On CI runners under heavy load,
        // spawning `docker info` and scheduling delays can add significant overhead beyond
        // the configured timeout. We use a generous 2s bound to avoid flaky failures while
        // still catching cases where the timeout is completely ignored.
        assert!(
            elapsed < Duration::from_secs(2),
            "Retry should respect timeout, but took {:?}",
            elapsed
        );
    }
}
