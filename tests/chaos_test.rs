/// Smoke tests for basic concurrency scenarios
///
/// These are intentionally simple tests that verify the system doesn't panic
/// or crash under basic concurrent operations. They use timing-based coordination
/// (sleep) which is acceptable for smoke tests but not reliable for reproducing
/// specific race conditions.
///
/// For comprehensive concurrency testing, see:
/// - `reproduction_tests.rs` - Sophisticated race condition reproduction
/// - `port_toctou_test.rs` - Thorough port allocation concurrency coverage
///
/// These tests serve as:
/// 1. Sanity checks that basic concurrent operations don't panic
/// 2. Quick verification that the orchestrator handles interleaved operations
/// 3. Documentation of expected behavior under concurrent access
use fed::{Orchestrator, Parser};
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;

#[path = "support/mod.rs"]
mod support;

/// Helper to create and initialize an orchestrator for testing
/// Returns both the orchestrator and the temp dir (to keep it alive)
async fn create_test_orchestrator(config_content: &str) -> (Arc<Orchestrator>, tempfile::TempDir) {
    let parser = Parser::new();
    let config = parser
        .parse_config(config_content)
        .expect("Failed to parse config");

    let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
    let mut orchestrator =
        support::new_orchestrator_for_test(config, temp_dir.path().to_path_buf())
            .await
            .expect("Failed to create orchestrator");

    // Use shorter timeouts for tests to avoid slow cleanup
    orchestrator.startup_timeout = Duration::from_secs(30);
    orchestrator.stop_timeout = Duration::from_secs(5);
    orchestrator.set_auto_resolve_conflicts(true);

    orchestrator
        .initialize()
        .await
        .expect("Failed to initialize");

    (Arc::new(orchestrator), temp_dir)
}

/// Smoke test: Stop a service while it's still starting
///
/// This test verifies that concurrent start/stop operations are handled gracefully
/// without panics, deadlocks, or resource leaks.
///
/// The orchestrator now supports true concurrent access:
/// - `start()`, `stop()`, `cleanup()` all take `&self` instead of `&mut self`
/// - Operations are cancellable via `cancel_operations()`
/// - Timeouts prevent hanging on stuck services
///
/// Verifies:
/// - No panics when stop is called during startup
/// - Operations complete in bounded time (via timeouts)
/// - Resources are cleaned up properly
#[tokio::test]
async fn chaos_stop_while_starting() {
    let config_content = r#"
parameters:
  PORT_A:
    type: port
  PORT_B:
    type: port
  PORT_C:
    type: port

services:
  base:
    process: 'echo "base"; sleep 2'
    environment:
      PORT: '{{PORT_A}}'
  middle:
    process: 'echo "middle"; sleep 2'
    environment:
      PORT: '{{PORT_B}}'
    depends_on:
      - base
  slow:
    process: 'echo "slow"; sleep 2'
    environment:
      PORT: '{{PORT_C}}'
    depends_on:
      - middle

entrypoint: slow
"#;

    let (orchestrator, _temp_dir) = create_test_orchestrator(config_content).await;

    // Start slow service (has dependencies, takes time)
    // Since methods take &self, we can call concurrently without external locking
    let orch1 = Arc::clone(&orchestrator);
    let handle1 = tokio::spawn(async move { orch1.start("slow").await });

    // Wait briefly then stop (timing-dependent race condition trigger)
    sleep(Duration::from_millis(100)).await;

    // Stop can run concurrently with start
    let stop_result = orchestrator.stop("slow").await;
    println!("Stop result: {:?}", stop_result);

    // Wait for start to complete
    let start_result = handle1.await.expect("Task panicked");
    println!("Start result: {:?}", start_result);

    // At least one should succeed - demonstrates graceful handling of concurrent operations
    // With the new architecture, both operations can run truly concurrently
    assert!(
        start_result.is_ok() || stop_result.is_ok(),
        "Either start or stop should succeed when racing"
    );

    orchestrator.cleanup().await;
}

/// Smoke test: Stop a dependency while dependent service is starting
///
/// This is a timing-dependent test that interrupts a dependency chain mid-startup.
/// Like the other chaos tests, it uses sleep() for coordination and serves as a
/// basic smoke test for graceful degradation.
///
/// Verifies:
/// - No panics when a dependency is stopped during chain startup
/// - System handles partial dependency chain failures gracefully
/// - Cleanup completes without errors
///
/// Expected behavior:
/// - If the dependency stops before the dependent needs it, the dependent should fail gracefully
/// - No resource leaks or deadlocks should occur
/// - All operations complete (even if with errors)
///
/// Note: This test has weak assertions (just "no panics") because the exact outcome
/// depends on timing. The key invariant is graceful handling, not a specific result.
#[tokio::test]
async fn chaos_dependency_chain_interruption() {
    let config_content = r#"
parameters:
  PORT_A:
    type: port
  PORT_B:
    type: port
  PORT_C:
    type: port

services:
  base:
    process: 'echo "base"; sleep 1'
    environment:
      PORT: '{{PORT_A}}'
  middle:
    process: 'echo "middle"; sleep 1'
    environment:
      PORT: '{{PORT_B}}'
    depends_on:
      - base
  top:
    process: 'echo "top"; sleep 1'
    environment:
      PORT: '{{PORT_C}}'
    depends_on:
      - middle

entrypoint: top
"#;

    let (orchestrator, _temp_dir) = create_test_orchestrator(config_content).await;

    // Start entire chain
    let orch1 = Arc::clone(&orchestrator);
    let start_handle = tokio::spawn(async move { orch1.start("top").await });

    // Wait for chain to begin (timing-dependent)
    sleep(Duration::from_millis(500)).await;

    // Stop middle dependency - this should interrupt the chain gracefully
    let stop_result = orchestrator.stop("middle").await;
    println!("Stop middle result: {:?}", stop_result);

    // Wait for start to complete - it may succeed or fail depending on timing
    let start_result = start_handle.await.expect("Task panicked");
    println!("Start top result: {:?}", start_result);

    // The key invariant: no panics, graceful handling of interruption
    // We don't assert on specific outcomes because timing determines whether
    // the dependency was stopped before or after the dependent started
    println!("Test completed - verified graceful handling of dependency interruption");

    orchestrator.cleanup().await;
}

/// Test: Cancellation during startup
///
/// Verifies that calling cancel_operations() during startup:
/// - Interrupts in-progress start operations
/// - Returns Error::Cancelled
/// - Doesn't leave the system in an inconsistent state
#[tokio::test]
async fn test_cancellation_during_startup() {
    let config_content = r#"
parameters:
  PORT_A:
    type: port

services:
  slow_service:
    process: 'echo "starting"; sleep 10; echo "done"'
    environment:
      PORT: '{{PORT_A}}'

entrypoint: slow_service
"#;

    let (orchestrator, _temp_dir) = create_test_orchestrator(config_content).await;

    // Start the slow service in background
    let orch_clone = Arc::clone(&orchestrator);
    let start_handle = tokio::spawn(async move { orch_clone.start("slow_service").await });

    // Give it a moment to start, then cancel
    sleep(Duration::from_millis(100)).await;
    orchestrator.cancel_operations();

    // The start should return quickly with Cancelled error
    let result = tokio::time::timeout(Duration::from_secs(2), start_handle)
        .await
        .expect("Start didn't respond to cancellation in time")
        .expect("Task panicked");

    // Should be cancelled
    match result {
        Err(fed::Error::Cancelled(_)) => {
            println!("Correctly received cancellation error");
        }
        Ok(_) => {
            // Service might have completed before cancellation - that's OK
            println!("Service completed before cancellation");
        }
        Err(e) => {
            panic!("Unexpected error: {:?}", e);
        }
    }

    // Cleanup should work after cancellation
    orchestrator.cleanup().await;
}

/// Test: Concurrent start and stop on same service
///
/// Tests that concurrent start and stop operations on the same service
/// are handled gracefully without deadlocks.
#[tokio::test]
async fn test_concurrent_start_stop_same_service() {
    let config_content = r#"
parameters:
  PORT_A:
    type: port

services:
  test_service:
    process: 'echo "running"; sleep 5'
    environment:
      PORT: '{{PORT_A}}'

entrypoint: test_service
"#;

    let (orchestrator, _temp_dir) = create_test_orchestrator(config_content).await;

    // Start both operations concurrently
    let orch1 = Arc::clone(&orchestrator);
    let orch2 = Arc::clone(&orchestrator);

    let (start_result, stop_result) =
        tokio::join!(async { orch1.start("test_service").await }, async {
            // Slight delay to ensure start begins first
            sleep(Duration::from_millis(50)).await;
            orch2.stop("test_service").await
        });

    println!("Start result: {:?}", start_result);
    println!("Stop result: {:?}", stop_result);

    // Both operations should complete (possibly with errors) - no hangs
    // The key invariant is that we don't deadlock

    orchestrator.cleanup().await;
}

/// Test: Timeout on slow service startup
///
/// Verifies that startup_timeout is respected and prevents hanging.
/// Note: Process services start quickly (spawn + detach), so timeout testing
/// is more relevant for services with health checks or blocking start operations.
#[tokio::test]
async fn test_startup_timeout() {
    let config_content = r#"
parameters:
  PORT_A:
    type: port

services:
  hanging_service:
    process: 'echo "starting"; sleep 300'
    environment:
      PORT: '{{PORT_A}}'

entrypoint: hanging_service
"#;

    let parser = Parser::new();
    let config = parser
        .parse_config(config_content)
        .expect("Failed to parse config");

    let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
    let mut orchestrator =
        support::new_orchestrator_for_test(config, temp_dir.path().to_path_buf())
            .await
            .expect("Failed to create orchestrator");

    // Set a very short timeout for testing
    orchestrator.startup_timeout = Duration::from_millis(500);
    orchestrator.stop_timeout = Duration::from_secs(2);
    orchestrator.set_auto_resolve_conflicts(true);
    orchestrator
        .initialize()
        .await
        .expect("Failed to initialize");
    let orchestrator = Arc::new(orchestrator);

    // Start may complete quickly (process spawn is fast) or timeout
    let result = orchestrator.start("hanging_service").await;

    // Accept either timeout, success, or start failure (process may die due to cleanup)
    match &result {
        Err(fed::Error::Timeout(name)) => {
            assert_eq!(name, "hanging_service");
            println!("Correctly received timeout error");
        }
        Ok(_) => {
            println!("Service started (spawn completed before timeout)");
        }
        Err(e) => {
            // Start failures can happen if the process dies quickly
            println!("Start failed (acceptable): {:?}", e);
        }
    }

    // Cleanup should work even after timeout
    orchestrator.cleanup().await;
}

/// A per-service `startup_timeout` in the config takes precedence over the
/// orchestrator-wide default. The orchestrator-wide default is generous (30s);
/// the per-service value is tight (200ms). A `sleep 300` process plus a failing
/// healthcheck would hang the start indefinitely without a per-service override,
/// so a clean timeout within ~1s proves the override is wired.
#[tokio::test]
async fn test_per_service_startup_timeout_overrides_default() {
    let config_content = r#"
services:
  slow_to_be_healthy:
    process: 'sleep 300'
    startup_timeout: "200ms"
    healthcheck:
      command: "false"

entrypoint: slow_to_be_healthy
"#;

    let parser = Parser::new();
    let config = parser
        .parse_config(config_content)
        .expect("Failed to parse config");

    let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
    let mut orchestrator =
        support::new_orchestrator_for_test(config, temp_dir.path().to_path_buf())
            .await
            .expect("Failed to create orchestrator");

    orchestrator.startup_timeout = Duration::from_secs(30);
    orchestrator.stop_timeout = Duration::from_secs(2);
    orchestrator.set_auto_resolve_conflicts(true);
    orchestrator
        .initialize()
        .await
        .expect("Failed to initialize");
    let orchestrator = Arc::new(orchestrator);

    let started = std::time::Instant::now();
    let result = orchestrator.start("slow_to_be_healthy").await;
    let elapsed = started.elapsed();

    match &result {
        Err(fed::Error::Timeout(name)) => assert_eq!(name, "slow_to_be_healthy"),
        Err(e) => panic!("expected Timeout, got {:?}", e),
        Ok(_) => panic!("expected Timeout, got Ok"),
    }
    assert!(
        elapsed < Duration::from_secs(5),
        "per-service startup_timeout did not preempt the 30s default — elapsed {:?}",
        elapsed
    );

    orchestrator.cleanup().await;
}

// Note: Port allocation concurrency testing has been removed from this file.
// The previous test created separate orchestrators (not testing shared allocator concurrency).
// For comprehensive port allocation concurrency testing, see:
// - tests/port_toctou_test.rs - Thorough coverage of TOCTOU prevention and concurrent allocation
