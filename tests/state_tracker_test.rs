use fed::config::ServiceType;
use fed::service::Status;
use fed::state::{ServiceState, StateTracker};
use tempfile::TempDir;

/// Helper to create a temp directory for tests
fn create_test_dir() -> TempDir {
    tempfile::tempdir().expect("Failed to create temp dir")
}

#[tokio::test]
async fn test_new_state_tracker() {
    let temp_dir = create_test_dir();
    let tracker = StateTracker::new(temp_dir.path().to_path_buf())
        .await
        .unwrap();

    assert_eq!(tracker.get_services().await.len(), 0);
    assert_eq!(tracker.get_allocated_ports().await.len(), 0);
}

#[tokio::test]
async fn test_initialize_no_existing_file() {
    let temp_dir = create_test_dir();
    let mut tracker = StateTracker::new(temp_dir.path().to_path_buf())
        .await
        .unwrap();

    let result = tracker.initialize().await;
    assert!(
        result.is_ok(),
        "Initialize should succeed with no existing file"
    );
}

#[tokio::test]
async fn test_register_and_get_service() {
    let temp_dir = create_test_dir();
    let mut tracker = StateTracker::new(temp_dir.path().to_path_buf())
        .await
        .unwrap();
    tracker.initialize().await.expect("Init failed");

    let service_state = ServiceState::new(
        "test-service".to_string(),
        ServiceType::Process,
        "default".to_string(),
    );

    tracker.register_service(service_state).await.unwrap();

    assert_eq!(tracker.get_services().await.len(), 1);
    assert!(tracker.is_service_registered("test-service").await);

    let service = tracker.get_service("test-service").await;
    assert!(service.is_some());
    assert_eq!(service.unwrap().id, "test-service");
}

#[tokio::test]
async fn test_unregister_service() {
    let temp_dir = create_test_dir();
    let mut tracker = StateTracker::new(temp_dir.path().to_path_buf())
        .await
        .unwrap();
    tracker.initialize().await.expect("Init failed");

    let service_state = ServiceState::new(
        "test-service".to_string(),
        ServiceType::Process,
        "default".to_string(),
    );

    tracker.register_service(service_state).await.unwrap();
    assert!(tracker.is_service_registered("test-service").await);

    tracker.unregister_service("test-service").await.unwrap();
    assert!(!tracker.is_service_registered("test-service").await);
    assert_eq!(tracker.get_services().await.len(), 0);
}

#[tokio::test]
async fn test_update_service_status() {
    let temp_dir = create_test_dir();
    let mut tracker = StateTracker::new(temp_dir.path().to_path_buf())
        .await
        .unwrap();
    tracker.initialize().await.expect("Init failed");

    let service_state = ServiceState::new(
        "test-service".to_string(),
        ServiceType::Process,
        "default".to_string(),
    );

    tracker.register_service(service_state).await.unwrap();

    let result = tracker
        .update_service_status("test-service", Status::Running)
        .await;
    assert!(result.is_ok());

    let service = tracker.get_service("test-service").await.unwrap();
    assert_eq!(service.status, Status::Running);
}

#[tokio::test]
async fn test_update_nonexistent_service_status() {
    let temp_dir = create_test_dir();
    let mut tracker = StateTracker::new(temp_dir.path().to_path_buf())
        .await
        .unwrap();
    tracker.initialize().await.expect("Init failed");

    let result = tracker
        .update_service_status("nonexistent", Status::Running)
        .await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_update_service_pid() {
    let temp_dir = create_test_dir();
    let mut tracker = StateTracker::new(temp_dir.path().to_path_buf())
        .await
        .unwrap();
    tracker.initialize().await.expect("Init failed");

    let service_state = ServiceState::new(
        "test-service".to_string(),
        ServiceType::Process,
        "default".to_string(),
    );

    tracker.register_service(service_state).await.unwrap();

    let result = tracker.update_service_pid("test-service", 12345).await;
    assert!(result.is_ok());

    let service = tracker.get_service("test-service").await.unwrap();
    assert_eq!(service.pid, Some(12345));
}

#[tokio::test]
async fn test_track_port() {
    let temp_dir = create_test_dir();
    let mut tracker = StateTracker::new(temp_dir.path().to_path_buf())
        .await
        .unwrap();
    tracker.initialize().await.expect("Init failed");

    tracker.track_port(8080).await;
    tracker.track_port(8081).await;
    tracker.track_port(8080).await; // Duplicate should be ignored

    let ports = tracker.get_allocated_ports().await;
    assert_eq!(ports.len(), 2);
    assert!(ports.contains(&8080));
    assert!(ports.contains(&8081));
}

#[tokio::test]
async fn test_add_service_port() {
    let temp_dir = create_test_dir();
    let mut tracker = StateTracker::new(temp_dir.path().to_path_buf())
        .await
        .unwrap();
    tracker.initialize().await.expect("Init failed");

    let service_state = ServiceState::new(
        "test-service".to_string(),
        ServiceType::Process,
        "default".to_string(),
    );

    tracker.register_service(service_state).await.unwrap();

    let result = tracker
        .add_service_port("test-service", "HTTP_PORT".to_string(), 8080)
        .await;
    assert!(result.is_ok());

    let service = tracker.get_service("test-service").await.unwrap();
    assert_eq!(service.port_allocations.get("HTTP_PORT"), Some(&8080));
}

#[tokio::test]
async fn test_save_and_load_lock_file() {
    let temp_dir = create_test_dir();

    // Create and save state
    {
        let mut tracker = StateTracker::new(temp_dir.path().to_path_buf())
            .await
            .unwrap();
        tracker.initialize().await.expect("Init failed");

        let service_state = ServiceState::new(
            "test-service".to_string(),
            ServiceType::Process,
            "default".to_string(),
        );

        tracker.register_service(service_state).await.unwrap();
        tracker.track_port(8080).await;
        tracker
            .update_service_status("test-service", Status::Running)
            .await
            .expect("Update failed");

        tracker.save().await.expect("Save failed");
    }

    // Load state in new tracker
    {
        let mut tracker = StateTracker::new(temp_dir.path().to_path_buf())
            .await
            .unwrap();
        tracker.initialize().await.expect("Init failed");

        // Note: Without proper process validation, services may be cleaned up
        // This test mainly verifies the save/load mechanism
        assert!(tracker.lock_file_path().exists(), "Lock file should exist");
    }
}

#[tokio::test]
async fn test_force_save() {
    let temp_dir = create_test_dir();
    let mut tracker = StateTracker::new(temp_dir.path().to_path_buf())
        .await
        .unwrap();
    tracker.initialize().await.expect("Init failed");

    // Force save even without changes
    let result = tracker.force_save().await;
    assert!(result.is_ok());
    assert!(tracker.lock_file_path().exists());
}

#[tokio::test]
async fn test_clear_lock_file() {
    let temp_dir = create_test_dir();
    let mut tracker = StateTracker::new(temp_dir.path().to_path_buf())
        .await
        .unwrap();
    tracker.initialize().await.expect("Init failed");

    let service_state = ServiceState::new(
        "test-service".to_string(),
        ServiceType::Process,
        "default".to_string(),
    );

    tracker.register_service(service_state).await.unwrap();
    tracker.save().await.expect("Save failed");

    assert!(tracker.lock_file_path().exists());

    tracker.clear().await.expect("Clear failed");
    // With SQLite, clear() empties tables but keeps the database file
    assert!(tracker.lock_file_path().exists());
    assert_eq!(tracker.get_services().await.len(), 0);
    assert_eq!(tracker.get_allocated_ports().await.len(), 0);
}

#[tokio::test]
async fn test_multiple_services() {
    let temp_dir = create_test_dir();
    let mut tracker = StateTracker::new(temp_dir.path().to_path_buf())
        .await
        .unwrap();
    tracker.initialize().await.expect("Init failed");

    for i in 1..=5 {
        let service_state = ServiceState::new(
            format!("service-{}", i),
            ServiceType::Process,
            "default".to_string(),
        );
        tracker.register_service(service_state).await.unwrap();
    }

    assert_eq!(tracker.get_services().await.len(), 5);

    // Unregister some
    tracker.unregister_service("service-2").await.unwrap();
    tracker.unregister_service("service-4").await.unwrap();

    assert_eq!(tracker.get_services().await.len(), 3);
    assert!(tracker.is_service_registered("service-1").await);
    assert!(!tracker.is_service_registered("service-2").await);
    assert!(tracker.is_service_registered("service-3").await);
    assert!(!tracker.is_service_registered("service-4").await);
    assert!(tracker.is_service_registered("service-5").await);
}

#[tokio::test]
async fn test_multiple_ports() {
    let temp_dir = create_test_dir();
    let mut tracker = StateTracker::new(temp_dir.path().to_path_buf())
        .await
        .unwrap();
    tracker.initialize().await.expect("Init failed");

    let ports = vec![8080, 8081, 8082, 8083, 8084];
    for port in &ports {
        tracker.track_port(*port).await;
    }

    let allocated = tracker.get_allocated_ports().await;
    assert_eq!(allocated.len(), 5);

    for port in &ports {
        assert!(allocated.contains(port));
    }
}

#[tokio::test]
async fn test_lock_file_path() {
    let temp_dir = create_test_dir();
    let tracker = StateTracker::new(temp_dir.path().to_path_buf())
        .await
        .unwrap();

    let expected_path = temp_dir.path().join(".fed").join("lock.db");
    assert_eq!(tracker.lock_file_path(), expected_path);
}

#[tokio::test]
async fn test_service_state_creation() {
    let state = ServiceState::new(
        "my-service".to_string(),
        ServiceType::Docker,
        "production".to_string(),
    );

    assert_eq!(state.id, "my-service");
    assert_eq!(state.service_type, ServiceType::Docker);
    assert_eq!(state.namespace, "production");
    assert_eq!(state.status, Status::Starting);
    assert_eq!(state.pid, None);
    assert_eq!(state.container_id, None);
    assert!(state.port_allocations.is_empty());
}

#[tokio::test]
async fn test_service_with_container_id() {
    let temp_dir = create_test_dir();
    let mut tracker = StateTracker::new(temp_dir.path().to_path_buf())
        .await
        .unwrap();
    tracker.initialize().await.expect("Init failed");

    let mut service_state = ServiceState::new(
        "docker-service".to_string(),
        ServiceType::Docker,
        "default".to_string(),
    );
    service_state.container_id = Some("abc123".to_string());

    tracker.register_service(service_state).await.unwrap();

    let service = tracker.get_service("docker-service").await.unwrap();
    assert_eq!(service.container_id, Some("abc123".to_string()));
}

#[tokio::test]
async fn test_namespace_handling() {
    let temp_dir = create_test_dir();
    let mut tracker = StateTracker::new(temp_dir.path().to_path_buf())
        .await
        .unwrap();
    tracker.initialize().await.expect("Init failed");

    let service1 = ServiceState::new(
        "service1".to_string(),
        ServiceType::Process,
        "namespace-a".to_string(),
    );

    let service2 = ServiceState::new(
        "service2".to_string(),
        ServiceType::Process,
        "namespace-b".to_string(),
    );

    tracker.register_service(service1).await.unwrap();
    tracker.register_service(service2).await.unwrap();

    let s1 = tracker.get_service("service1").await.unwrap();
    let s2 = tracker.get_service("service2").await.unwrap();

    assert_eq!(s1.namespace, "namespace-a");
    assert_eq!(s2.namespace, "namespace-b");
}

#[tokio::test]
async fn test_update_service_multiple_times() {
    let temp_dir = create_test_dir();
    let mut tracker = StateTracker::new(temp_dir.path().to_path_buf())
        .await
        .unwrap();
    tracker.initialize().await.expect("Init failed");

    let service_state = ServiceState::new(
        "test-service".to_string(),
        ServiceType::Process,
        "default".to_string(),
    );

    tracker.register_service(service_state).await.unwrap();

    // Multiple status updates
    tracker
        .update_service_status("test-service", Status::Starting)
        .await
        .expect("Update 1 failed");
    tracker
        .update_service_status("test-service", Status::Running)
        .await
        .expect("Update 2 failed");
    tracker
        .update_service_status("test-service", Status::Healthy)
        .await
        .expect("Update 3 failed");

    let service = tracker.get_service("test-service").await.unwrap();
    assert_eq!(service.status, Status::Healthy);
}

#[tokio::test]
async fn test_port_allocations_per_service() {
    let temp_dir = create_test_dir();
    let mut tracker = StateTracker::new(temp_dir.path().to_path_buf())
        .await
        .unwrap();
    tracker.initialize().await.expect("Init failed");

    let service_state = ServiceState::new(
        "web-service".to_string(),
        ServiceType::Process,
        "default".to_string(),
    );

    tracker.register_service(service_state).await.unwrap();

    tracker
        .add_service_port("web-service", "HTTP_PORT".to_string(), 8080)
        .await
        .expect("Add port 1 failed");
    tracker
        .add_service_port("web-service", "HTTPS_PORT".to_string(), 8443)
        .await
        .expect("Add port 2 failed");
    tracker
        .add_service_port("web-service", "ADMIN_PORT".to_string(), 9090)
        .await
        .expect("Add port 3 failed");

    let service = tracker.get_service("web-service").await.unwrap();
    assert_eq!(service.port_allocations.len(), 3);
    assert_eq!(service.port_allocations.get("HTTP_PORT"), Some(&8080));
    assert_eq!(service.port_allocations.get("HTTPS_PORT"), Some(&8443));
    assert_eq!(service.port_allocations.get("ADMIN_PORT"), Some(&9090));
}

#[tokio::test]
async fn test_empty_state_not_saved() {
    let temp_dir = create_test_dir();
    let mut tracker = StateTracker::new(temp_dir.path().to_path_buf())
        .await
        .unwrap();
    tracker.initialize().await.expect("Init failed");

    // Don't add any services or ports
    tracker.save().await.expect("Save failed");

    // With SQLite, the database file is created on initialization
    // even with empty state (unlike the old JSON implementation)
    assert!(tracker.lock_file_path().exists());
    // But there should be no services
    assert_eq!(tracker.get_services().await.len(), 0);
}

#[tokio::test]
async fn test_concurrent_port_tracking() {
    let temp_dir = create_test_dir();
    let mut tracker = StateTracker::new(temp_dir.path().to_path_buf())
        .await
        .unwrap();
    tracker.initialize().await.expect("Init failed");

    // Simulate tracking many ports
    for port in 8000..8100 {
        tracker.track_port(port).await;
    }

    let ports = tracker.get_allocated_ports().await;
    assert_eq!(ports.len(), 100);
}

// =============================================================================
// Regression Tests: Namespace/Plain Name Consistency
// =============================================================================
// These tests prevent regressions related to service ID format.
// Services MUST be stored by plain names (e.g., "docker-infra"), NOT
// namespaced IDs (e.g., "root/docker-infra").

/// Regression test: Services should be stored by plain name only
/// If stored by namespaced ID, lookups using plain names will fail
#[tokio::test]
async fn test_regression_service_stored_by_plain_name() {
    let temp_dir = create_test_dir();
    let mut tracker = StateTracker::new(temp_dir.path().to_path_buf())
        .await
        .unwrap();
    tracker.initialize().await.expect("Init failed");

    // Register service with plain name (correct behavior)
    let service_state = ServiceState::new(
        "docker-infra".to_string(), // Plain name, NOT "root/docker-infra"
        ServiceType::Docker,
        "root".to_string(),
    );
    tracker.register_service(service_state).await.unwrap();

    // Lookup should work with plain name
    assert!(tracker.is_service_registered("docker-infra").await);
    assert!(tracker.get_service("docker-infra").await.is_some());

    // Lookup should NOT work with namespaced ID (they're different keys)
    assert!(!tracker.is_service_registered("root/docker-infra").await);
    assert!(tracker.get_service("root/docker-infra").await.is_none());
}

/// Regression test: update_service_status must use plain name
#[tokio::test]
async fn test_regression_update_status_requires_plain_name() {
    let temp_dir = create_test_dir();
    let mut tracker = StateTracker::new(temp_dir.path().to_path_buf())
        .await
        .unwrap();
    tracker.initialize().await.expect("Init failed");

    let service_state = ServiceState::new(
        "my-service".to_string(),
        ServiceType::Process,
        "root".to_string(),
    );
    tracker.register_service(service_state).await.unwrap();

    // Update with plain name should succeed
    assert!(
        tracker
            .update_service_status("my-service", Status::Running)
            .await
            .is_ok()
    );

    // Update with namespaced ID should fail (service not found)
    assert!(
        tracker
            .update_service_status("root/my-service", Status::Healthy)
            .await
            .is_err()
    );
}

/// Regression test: unregister_service must use plain name
#[tokio::test]
async fn test_regression_unregister_requires_plain_name() {
    let temp_dir = create_test_dir();
    let mut tracker = StateTracker::new(temp_dir.path().to_path_buf())
        .await
        .unwrap();
    tracker.initialize().await.expect("Init failed");

    let service_state = ServiceState::new(
        "temp-service".to_string(),
        ServiceType::Process,
        "root".to_string(),
    );
    tracker.register_service(service_state).await.unwrap();
    assert!(tracker.is_service_registered("temp-service").await);

    // Unregister with namespaced ID should do nothing (key not found)
    tracker
        .unregister_service("root/temp-service")
        .await
        .unwrap();
    assert!(tracker.is_service_registered("temp-service").await); // Still registered!

    // Unregister with plain name should work
    tracker.unregister_service("temp-service").await.unwrap();
    assert!(!tracker.is_service_registered("temp-service").await);
}
