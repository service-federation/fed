/// Integration tests for circuit breaker edge cases
///
/// Tests the full lifecycle of circuit breaker behavior with real services:
/// - Service restarts N times before circuit opens
/// - Circuit stays open for cooldown period
/// - Circuit closes after successful health check
/// - Restart history is cleaned up after 24h
use fed::config::ServiceType;
use fed::state::StateTracker;
use std::time::Duration;
use tempfile::TempDir;
use tokio::time::sleep;

/// Helper to create a temp directory for tests
fn create_test_dir() -> TempDir {
    tempfile::tempdir().expect("Failed to create temp dir")
}

#[tokio::test]
async fn test_circuit_breaker_opens_after_threshold() {
    let temp_dir = create_test_dir();
    let mut tracker = StateTracker::new(temp_dir.path().to_path_buf())
        .await
        .expect("Failed to create tracker");
    tracker.initialize().await.expect("Failed to initialize");

    // Register a service
    let service_state = fed::state::ServiceState::new(
        "crash-service".to_string(),
        ServiceType::Process,
        "default".to_string(),
    );
    tracker.register_service(service_state).await.unwrap();

    // Configure circuit breaker: 3 restarts in 10 seconds
    let threshold = 3u32;
    let window_secs = 10u64;

    // Record restarts - should NOT trip circuit breaker yet
    for i in 0..threshold - 1 {
        tracker
            .record_restart("crash-service")
            .await
            .unwrap_or_else(|_| panic!("Failed to record restart {}", i));
    }

    // Circuit breaker should NOT be open yet
    let should_trip = tracker
        .check_circuit_breaker("crash-service", threshold, window_secs)
        .await
        .expect("Failed to check circuit breaker");
    assert!(
        !should_trip,
        "Circuit breaker should not trip before threshold"
    );

    // Record one more restart - this should trip the circuit breaker
    tracker
        .record_restart("crash-service")
        .await
        .expect("Failed to record final restart");

    // Circuit breaker should be triggered now
    let should_trip = tracker
        .check_circuit_breaker("crash-service", threshold, window_secs)
        .await
        .expect("Failed to check circuit breaker");
    assert!(
        should_trip,
        "Circuit breaker should trip after {} restarts",
        threshold
    );
}

#[tokio::test]
async fn test_circuit_breaker_cooldown_period() {
    let temp_dir = create_test_dir();
    let mut tracker = StateTracker::new(temp_dir.path().to_path_buf())
        .await
        .expect("Failed to create tracker");
    tracker.initialize().await.expect("Failed to initialize");

    // Register a service
    let service_state = fed::state::ServiceState::new(
        "cooldown-service".to_string(),
        ServiceType::Process,
        "default".to_string(),
    );
    tracker.register_service(service_state).await.unwrap();

    // Open circuit breaker with a short cooldown for testing (3 seconds)
    let cooldown_secs = 3u64;
    let opened_at = std::time::Instant::now();
    tracker
        .open_circuit_breaker("cooldown-service", cooldown_secs)
        .await
        .expect("Failed to open circuit breaker");

    // Circuit breaker should be open
    assert!(
        tracker.is_circuit_breaker_open("cooldown-service").await,
        "Circuit breaker should be open immediately after opening"
    );

    // Wait a third of the cooldown period
    sleep(Duration::from_secs(1)).await;

    // Only assert mid-window state if this runner actually observed the
    // window: a loaded CI machine can stretch a 1s sleep past the whole
    // cooldown, which is a scheduling artifact, not a breaker bug.
    if opened_at.elapsed() < Duration::from_secs(cooldown_secs) {
        assert!(
            tracker.is_circuit_breaker_open("cooldown-service").await,
            "Circuit breaker should still be open during cooldown"
        );
    }

    // Wait for cooldown to expire (plus a small buffer)
    sleep(Duration::from_millis(2500)).await;

    // Circuit breaker should now be closed
    assert!(
        !tracker.is_circuit_breaker_open("cooldown-service").await,
        "Circuit breaker should close after cooldown expires"
    );
}

#[tokio::test]
async fn test_circuit_breaker_window_sliding() {
    let temp_dir = create_test_dir();
    let mut tracker = StateTracker::new(temp_dir.path().to_path_buf())
        .await
        .expect("Failed to create tracker");
    tracker.initialize().await.expect("Failed to initialize");

    // Register a service
    let service_state = fed::state::ServiceState::new(
        "window-service".to_string(),
        ServiceType::Process,
        "default".to_string(),
    );
    tracker.register_service(service_state).await.unwrap();

    // Configure circuit breaker: 3 restarts in 2 seconds
    let threshold = 3u32;
    let window_secs = 2u64;

    // Record 2 restarts
    for _ in 0..2 {
        tracker
            .record_restart("window-service")
            .await
            .expect("Failed to record restart");
    }

    // Wait for window to expire
    sleep(Duration::from_millis(2100)).await;

    // Old restarts should be outside the window - circuit breaker should not trip
    let should_trip = tracker
        .check_circuit_breaker("window-service", threshold, window_secs)
        .await
        .expect("Failed to check circuit breaker");
    assert!(
        !should_trip,
        "Circuit breaker should not trip when old restarts are outside window"
    );

    // Record 3 new restarts within the window
    for _ in 0..3 {
        tracker
            .record_restart("window-service")
            .await
            .expect("Failed to record restart");
    }

    // Circuit breaker should trip now (3 restarts within 2 seconds)
    let should_trip = tracker
        .check_circuit_breaker("window-service", threshold, window_secs)
        .await
        .expect("Failed to check circuit breaker");
    assert!(
        should_trip,
        "Circuit breaker should trip with 3 restarts in window"
    );
}

#[tokio::test]
async fn test_restart_history_retrieval() {
    let temp_dir = create_test_dir();
    let mut tracker = StateTracker::new(temp_dir.path().to_path_buf())
        .await
        .expect("Failed to create tracker");
    tracker.initialize().await.expect("Failed to initialize");

    // Register a service
    let service_state = fed::state::ServiceState::new(
        "history-service".to_string(),
        ServiceType::Process,
        "default".to_string(),
    );
    tracker.register_service(service_state).await.unwrap();

    // Record several restarts
    let num_restarts = 5;
    for _ in 0..num_restarts {
        tracker
            .record_restart("history-service")
            .await
            .expect("Failed to record restart");
        sleep(Duration::from_millis(10)).await; // Small delay to ensure distinct timestamps
    }

    // Retrieve restart history
    let history = tracker
        .get_restart_history("history-service")
        .await
        .expect("Failed to get restart history");

    // Should have all restarts recorded
    assert_eq!(
        history.len(),
        num_restarts,
        "Should have {} restart events in history",
        num_restarts
    );

    // Timestamps should be in descending order (most recent first)
    for i in 0..history.len() - 1 {
        assert!(
            history[i] >= history[i + 1],
            "Restart history should be in descending order"
        );
    }
}

#[tokio::test]
async fn test_circuit_breaker_multiple_services() {
    let temp_dir = create_test_dir();
    let mut tracker = StateTracker::new(temp_dir.path().to_path_buf())
        .await
        .expect("Failed to create tracker");
    tracker.initialize().await.expect("Failed to initialize");

    // Register multiple services
    for i in 1..=3 {
        let service_state = fed::state::ServiceState::new(
            format!("service-{}", i),
            ServiceType::Process,
            "default".to_string(),
        );
        tracker.register_service(service_state).await.unwrap();
    }

    // Configure circuit breaker
    let threshold = 3u32;
    let window_secs = 10u64;

    // Trip circuit breaker for service-1
    for _ in 0..threshold {
        tracker
            .record_restart("service-1")
            .await
            .expect("Failed to record restart");
    }

    // Trip circuit breaker for service-3
    for _ in 0..threshold {
        tracker
            .record_restart("service-3")
            .await
            .expect("Failed to record restart");
    }

    // Check circuit breaker state for each service
    let service1_trip = tracker
        .check_circuit_breaker("service-1", threshold, window_secs)
        .await
        .expect("Failed to check service-1");
    let service2_trip = tracker
        .check_circuit_breaker("service-2", threshold, window_secs)
        .await
        .expect("Failed to check service-2");
    let service3_trip = tracker
        .check_circuit_breaker("service-3", threshold, window_secs)
        .await
        .expect("Failed to check service-3");

    assert!(service1_trip, "service-1 circuit breaker should be tripped");
    assert!(
        !service2_trip,
        "service-2 circuit breaker should NOT be tripped"
    );
    assert!(service3_trip, "service-3 circuit breaker should be tripped");
}

#[tokio::test]
async fn test_circuit_breaker_reset_on_unregister() {
    let temp_dir = create_test_dir();
    let mut tracker = StateTracker::new(temp_dir.path().to_path_buf())
        .await
        .expect("Failed to create tracker");
    tracker.initialize().await.expect("Failed to initialize");

    // Register a service
    let service_state = fed::state::ServiceState::new(
        "reset-service".to_string(),
        ServiceType::Process,
        "default".to_string(),
    );
    tracker.register_service(service_state).await.unwrap();

    // Record restarts and open circuit breaker
    for _ in 0..3 {
        tracker
            .record_restart("reset-service")
            .await
            .expect("Failed to record restart");
    }
    tracker
        .open_circuit_breaker("reset-service", 60)
        .await
        .expect("Failed to open circuit breaker");

    // Verify circuit breaker is open
    assert!(
        tracker.is_circuit_breaker_open("reset-service").await,
        "Circuit breaker should be open before unregister"
    );

    // Unregister the service (simulates service being stopped)
    tracker.unregister_service("reset-service").await.unwrap();

    // Register the same service again (simulates service being started fresh)
    let service_state = fed::state::ServiceState::new(
        "reset-service".to_string(),
        ServiceType::Process,
        "default".to_string(),
    );
    tracker.register_service(service_state).await.unwrap();

    // Circuit breaker should be closed for the new registration
    assert!(
        !tracker.is_circuit_breaker_open("reset-service").await,
        "Circuit breaker should be closed after re-registration"
    );

    // Restart history should be empty for the new registration
    let history = tracker
        .get_restart_history("reset-service")
        .await
        .expect("Failed to get restart history");
    assert!(
        history.is_empty(),
        "Restart history should be empty after re-registration"
    );
}

#[tokio::test]
async fn test_consecutive_failures_increment_and_reset() {
    let temp_dir = create_test_dir();
    let mut tracker = StateTracker::new(temp_dir.path().to_path_buf())
        .await
        .expect("Failed to create tracker");
    tracker.initialize().await.expect("Failed to initialize");

    // Register a service
    let service_state = fed::state::ServiceState::new(
        "failure-service".to_string(),
        ServiceType::Process,
        "default".to_string(),
    );
    tracker.register_service(service_state).await.unwrap();

    // Increment consecutive failures
    for i in 1..=5 {
        tracker
            .increment_consecutive_failures("failure-service")
            .await
            .expect("Failed to increment failures");

        let failures = tracker
            .get_consecutive_failures("failure-service")
            .await
            .expect("Failed to get failures");
        assert_eq!(failures, i, "Should have {} consecutive failures", i);
    }

    // Reset consecutive failures (simulates successful health check)
    tracker
        .reset_consecutive_failures("failure-service")
        .await
        .expect("Failed to reset failures");

    let failures = tracker
        .get_consecutive_failures("failure-service")
        .await
        .expect("Failed to get failures");
    assert_eq!(failures, 0, "Consecutive failures should be 0 after reset");
}

#[tokio::test]
async fn test_circuit_breaker_time_remaining() {
    let temp_dir = create_test_dir();
    let mut tracker = StateTracker::new(temp_dir.path().to_path_buf())
        .await
        .expect("Failed to create tracker");
    tracker.initialize().await.expect("Failed to initialize");

    // Register a service
    let service_state = fed::state::ServiceState::new(
        "timer-service".to_string(),
        ServiceType::Process,
        "default".to_string(),
    );
    tracker.register_service(service_state).await.unwrap();

    // Open circuit breaker with 5 second cooldown
    let cooldown_secs = 5u64;
    tracker
        .open_circuit_breaker("timer-service", cooldown_secs)
        .await
        .expect("Failed to open circuit breaker");

    // Get time remaining - should be approximately 5 seconds (with tolerance for execution time)
    let remaining: Option<i64> = tracker
        .get_circuit_breaker_remaining("timer-service")
        .await
        .unwrap();
    match remaining {
        Some(secs) => {
            assert!(
                (4..=5).contains(&secs),
                "Time remaining should be approximately {} seconds, got {}",
                cooldown_secs,
                secs
            );
        }
        None => panic!("Circuit breaker should be open with time remaining"),
    }

    // Wait for cooldown to expire
    sleep(Duration::from_millis(5100)).await;

    // Time remaining should be None (circuit breaker closed)
    let remaining: Option<i64> = tracker
        .get_circuit_breaker_remaining("timer-service")
        .await
        .unwrap();
    assert!(
        remaining.is_none(),
        "Time remaining should be None after cooldown expires"
    );
}
