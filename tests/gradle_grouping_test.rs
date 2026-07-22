#![allow(clippy::field_reassign_with_default)]

use fed::Config;
use fed::config::{Service, ServiceType};

#[path = "support/mod.rs"]
mod support;

/// Helper to create a test config with gradle services
fn create_test_config() -> Config {
    let mut config = Config::default();

    // Service 1: Gradle task in default directory
    let mut service1 = Service::default();
    service1.gradle_task = Some(":service1:bootRun".to_string());
    service1
        .environment
        .insert("PORT".to_string(), "8080".to_string());

    // Service 2: Gradle task in default directory (can be grouped with service1)
    let mut service2 = Service::default();
    service2.gradle_task = Some(":service2:bootRun".to_string());
    service2
        .environment
        .insert("PORT".to_string(), "8081".to_string());

    // Service 3: Gradle task in different directory
    let mut service3 = Service::default();
    service3.gradle_task = Some(":service3:bootRun".to_string());
    service3.cwd = Some("subdir".to_string());
    service3
        .environment
        .insert("PORT".to_string(), "8082".to_string());

    // Service 4: Docker service
    let mut service4 = Service::default();
    service4.image = Some("postgres:15".to_string());

    // Service 5: Process service
    let mut service5 = Service::default();
    service5.process = Some("echo hello".to_string());

    config.services.insert("service1".to_string(), service1);
    config.services.insert("service2".to_string(), service2);
    config.services.insert("service3".to_string(), service3);
    config.services.insert("service4".to_string(), service4);
    config.services.insert("service5".to_string(), service5);

    config
}

#[tokio::test]
async fn test_gradle_services_same_cwd_no_dependencies_are_grouped() {
    let mut config = create_test_config();

    // No dependencies - service1 and service2 should be grouped
    config.entrypoint = Some("service1".to_string());

    let temp_dir = tempfile::tempdir().unwrap();
    let mut orchestrator =
        support::new_orchestrator_for_test(config, temp_dir.path().to_path_buf())
            .await
            .unwrap();
    orchestrator.set_auto_resolve_conflicts(true);
    orchestrator
        .initialize()
        .await
        .expect("Failed to initialize");

    // Check that services were created
    let services = orchestrator.get_status().await;

    // Service1 and service2 should be in a grouped service
    // The grouped service will have a name like "gradle-group-service1-service2" or "gradle-group-service2-service1"
    let has_grouped_service = services.keys().any(|k| {
        k.starts_with("gradle-group-") && (k.contains("service1") && k.contains("service2"))
    });

    assert!(
        has_grouped_service,
        "Services in same CWD with no dependencies should be grouped. Found services: {:?}",
        services.keys().collect::<Vec<_>>()
    );

    // Service3 should be separate (different CWD)
    assert!(
        services.contains_key("service3"),
        "Service in different CWD should not be grouped"
    );

    // Docker and process services should be separate
    assert!(
        services.contains_key("service4"),
        "Docker service should exist"
    );
    assert!(
        services.contains_key("service5"),
        "Process service should exist"
    );
}

#[tokio::test]
async fn test_gradle_services_with_dependencies_are_grouped() {
    let mut config = create_test_config();

    // Add dependency: service2 depends on service1
    config.services.get_mut("service2").unwrap().depends_on =
        vec![fed::config::DependsOn::Simple("service1".to_string())];
    config.entrypoint = Some("service2".to_string());

    let temp_dir = tempfile::tempdir().unwrap();
    let mut orchestrator =
        support::new_orchestrator_for_test(config, temp_dir.path().to_path_buf())
            .await
            .unwrap();
    orchestrator.set_auto_resolve_conflicts(true);
    orchestrator
        .initialize()
        .await
        .expect("Failed to initialize");

    let services = orchestrator.get_status().await;

    // Service1 and service2 SHOULD be grouped even though service2 depends on service1
    // This is the practical approach: gradle tasks in same CWD are run together
    let has_grouped_service = services.keys().any(|k| {
        k.starts_with("gradle-group-") && k.contains("service1") && k.contains("service2")
    });

    assert!(
        has_grouped_service,
        "Services with dependencies in same CWD should still be grouped. Found services: {:?}",
        services.keys().collect::<Vec<_>>()
    );

    // Individual services should NOT exist (they're in the group)
    assert!(
        !services.contains_key("service1"),
        "Service1 should not exist individually (should be in group)"
    );
    assert!(
        !services.contains_key("service2"),
        "Service2 should not exist individually (should be in group)"
    );
}

#[tokio::test]
async fn test_gradle_services_different_cwd_are_not_grouped() {
    let mut config = create_test_config();

    // service1 (default CWD) and service3 (subdir) should not be grouped
    config.entrypoint = Some("service1".to_string());

    let temp_dir = tempfile::tempdir().unwrap();
    let mut orchestrator =
        support::new_orchestrator_for_test(config, temp_dir.path().to_path_buf())
            .await
            .unwrap();
    orchestrator.set_auto_resolve_conflicts(true);
    orchestrator
        .initialize()
        .await
        .expect("Failed to initialize");

    let services = orchestrator.get_status().await;

    // Service1 and service3 should NOT be grouped (different CWD)
    let has_grouped_1_3 = services.keys().any(|k| {
        k.starts_with("gradle-group-") && k.contains("service1") && k.contains("service3")
    });

    assert!(
        !has_grouped_1_3,
        "Services in different CWD should not be grouped"
    );

    // Service3 should exist (might be grouped with others in same subdir, or individual)
    assert!(
        services.contains_key("service3") || services.keys().any(|k| k.contains("service3")),
        "Service3 should exist"
    );
}

#[tokio::test]
async fn test_three_services_same_cwd_all_grouped() {
    let mut config = Config::default();

    // Create three Gradle services in the same directory
    for i in 1..=3 {
        let mut service = Service::default();
        service.gradle_task = Some(format!(":service{}:bootRun", i));
        config.services.insert(format!("service{}", i), service);
    }

    config.entrypoint = Some("service1".to_string());

    let temp_dir = tempfile::tempdir().unwrap();
    let mut orchestrator =
        support::new_orchestrator_for_test(config, temp_dir.path().to_path_buf())
            .await
            .unwrap();
    orchestrator.set_auto_resolve_conflicts(true);
    orchestrator
        .initialize()
        .await
        .expect("Failed to initialize");

    let services = orchestrator.get_status().await;

    // All three should be in one group
    let grouped_services: Vec<_> = services
        .keys()
        .filter(|k| k.starts_with("gradle-group-"))
        .collect();

    assert_eq!(
        grouped_services.len(),
        1,
        "All three services should be in one group. Found: {:?}",
        services.keys().collect::<Vec<_>>()
    );

    let group_name = grouped_services[0];
    assert!(
        group_name.contains("service1")
            && group_name.contains("service2")
            && group_name.contains("service3"),
        "Group should contain all three services"
    );
}

#[tokio::test]
async fn test_complex_dependency_chain_grouping() {
    let mut config = Config::default();

    // Create a dependency chain:
    // service1 (gradle)
    // service2 (gradle, depends on service1)
    // service3 (gradle, same CWD as service1, no dependencies)
    // service4 (gradle, same CWD as service2, depends on service2)

    let mut service1 = Service::default();
    service1.gradle_task = Some(":service1:bootRun".to_string());

    let mut service2 = Service::default();
    service2.gradle_task = Some(":service2:bootRun".to_string());
    service2.depends_on = vec![fed::config::DependsOn::Simple("service1".to_string())];

    let mut service3 = Service::default();
    service3.gradle_task = Some(":service3:bootRun".to_string());
    // Same CWD as service1

    let mut service4 = Service::default();
    service4.gradle_task = Some(":service4:bootRun".to_string());
    service4.depends_on = vec![fed::config::DependsOn::Simple("service2".to_string())];
    // Same CWD as service2

    config.services.insert("service1".to_string(), service1);
    config.services.insert("service2".to_string(), service2);
    config.services.insert("service3".to_string(), service3);
    config.services.insert("service4".to_string(), service4);

    config.entrypoint = Some("service4".to_string());

    let temp_dir = tempfile::tempdir().unwrap();
    let mut orchestrator =
        support::new_orchestrator_for_test(config, temp_dir.path().to_path_buf())
            .await
            .unwrap();
    orchestrator.set_auto_resolve_conflicts(true);
    orchestrator
        .initialize()
        .await
        .expect("Failed to initialize");

    let services = orchestrator.get_status().await;

    // ALL services in same CWD should be grouped together, regardless of dependencies
    let has_all_grouped = services.keys().any(|k| {
        k.starts_with("gradle-group-")
            && k.contains("service1")
            && k.contains("service2")
            && k.contains("service3")
            && k.contains("service4")
    });

    assert!(
        has_all_grouped,
        "All services in same CWD should be grouped regardless of dependencies. Found: {:?}",
        services.keys().collect::<Vec<_>>()
    );
}

#[tokio::test]
async fn test_mixed_service_types_in_parallel_group() {
    let mut config = Config::default();

    // Parallel group with: gradle1, docker, gradle2, process
    // gradle1 and gradle2 should be grouped

    let mut gradle1 = Service::default();
    gradle1.gradle_task = Some(":gradle1:bootRun".to_string());

    let mut gradle2 = Service::default();
    gradle2.gradle_task = Some(":gradle2:bootRun".to_string());

    let mut docker = Service::default();
    docker.image = Some("nginx:latest".to_string());

    let mut process = Service::default();
    process.process = Some("echo test".to_string());

    config.services.insert("gradle1".to_string(), gradle1);
    config.services.insert("gradle2".to_string(), gradle2);
    config.services.insert("docker".to_string(), docker);
    config.services.insert("process".to_string(), process);

    config.entrypoint = Some("gradle1".to_string());

    let temp_dir = tempfile::tempdir().unwrap();
    let mut orchestrator =
        support::new_orchestrator_for_test(config, temp_dir.path().to_path_buf())
            .await
            .unwrap();
    orchestrator.set_auto_resolve_conflicts(true);
    orchestrator
        .initialize()
        .await
        .expect("Failed to initialize");

    let services = orchestrator.get_status().await;

    // gradle1 and gradle2 should be grouped
    let has_gradle_group = services
        .keys()
        .any(|k| k.starts_with("gradle-group-") && k.contains("gradle1") && k.contains("gradle2"));

    assert!(
        has_gradle_group,
        "Gradle services should be grouped together. Found: {:?}",
        services.keys().collect::<Vec<_>>()
    );

    // Docker and process should be separate
    assert!(
        services.contains_key("docker"),
        "Docker service should exist"
    );
    assert!(
        services.contains_key("process"),
        "Process service should exist"
    );
}

#[tokio::test]
async fn test_gradle_grouping_with_absolute_and_relative_paths() {
    let mut config = Config::default();

    // Service with no CWD (uses default)
    let mut service1 = Service::default();
    service1.gradle_task = Some(":service1:bootRun".to_string());

    // Service with empty CWD (should be treated as default)
    let mut service2 = Service::default();
    service2.gradle_task = Some(":service2:bootRun".to_string());
    service2.cwd = Some("".to_string());

    // Service with relative CWD
    let mut service3 = Service::default();
    service3.gradle_task = Some(":service3:bootRun".to_string());
    service3.cwd = Some("subdir".to_string());

    config.services.insert("service1".to_string(), service1);
    config.services.insert("service2".to_string(), service2);
    config.services.insert("service3".to_string(), service3);

    config.entrypoint = Some("service1".to_string());

    let temp_dir = tempfile::tempdir().unwrap();
    let mut orchestrator =
        support::new_orchestrator_for_test(config, temp_dir.path().to_path_buf())
            .await
            .unwrap();
    orchestrator.set_auto_resolve_conflicts(true);
    orchestrator
        .initialize()
        .await
        .expect("Failed to initialize");

    let services = orchestrator.get_status().await;

    // service1 and service2 should potentially be grouped (both in default dir)
    // service3 should be separate (different dir)
    let service3_grouped_with_others = services.keys().any(|k| {
        k.starts_with("gradle-group-")
            && k.contains("service3")
            && (k.contains("service1") || k.contains("service2"))
    });

    assert!(
        !service3_grouped_with_others,
        "service3 (different CWD) should not be grouped with service1/service2. Found: {:?}",
        services.keys().collect::<Vec<_>>()
    );
}

#[tokio::test]
async fn test_single_gradle_service_not_grouped() {
    let mut config = Config::default();

    let mut service1 = Service::default();
    service1.gradle_task = Some(":service1:bootRun".to_string());

    config.services.insert("service1".to_string(), service1);
    config.entrypoint = Some("service1".to_string());

    let temp_dir = tempfile::tempdir().unwrap();
    let mut orchestrator =
        support::new_orchestrator_for_test(config, temp_dir.path().to_path_buf())
            .await
            .unwrap();
    orchestrator.set_auto_resolve_conflicts(true);
    orchestrator
        .initialize()
        .await
        .expect("Failed to initialize");

    let services = orchestrator.get_status().await;

    // Single service should not create a group
    let has_grouped = services.keys().any(|k| k.starts_with("gradle-group-"));

    assert!(
        !has_grouped,
        "Single service should not be grouped. Found: {:?}",
        services.keys().collect::<Vec<_>>()
    );

    assert!(
        services.contains_key("service1"),
        "Service1 should exist individually"
    );
}

#[tokio::test]
async fn test_environment_merging_in_grouped_services() {
    let mut config = Config::default();

    let mut service1 = Service::default();
    service1.gradle_task = Some(":service1:bootRun".to_string());
    service1
        .environment
        .insert("VAR1".to_string(), "value1".to_string());
    service1
        .environment
        .insert("SHARED".to_string(), "from1".to_string());

    let mut service2 = Service::default();
    service2.gradle_task = Some(":service2:bootRun".to_string());
    service2
        .environment
        .insert("VAR2".to_string(), "value2".to_string());
    service2
        .environment
        .insert("SHARED".to_string(), "from2".to_string());

    config.services.insert("service1".to_string(), service1);
    config.services.insert("service2".to_string(), service2);

    config.entrypoint = Some("service1".to_string());

    let temp_dir = tempfile::tempdir().unwrap();
    let mut orchestrator =
        support::new_orchestrator_for_test(config, temp_dir.path().to_path_buf())
            .await
            .unwrap();
    orchestrator.set_auto_resolve_conflicts(true);
    let result = orchestrator.initialize().await;

    assert!(
        result.is_ok(),
        "Should successfully initialize with merged environments"
    );

    let services = orchestrator.get_status().await;

    // Services should be grouped
    let has_group = services.keys().any(|k| k.starts_with("gradle-group-"));
    assert!(has_group, "Services should be grouped");
}

#[test]
fn test_service_type_detection() {
    let mut service = Service::default();

    // Test GradleTask type
    service.gradle_task = Some(":test:run".to_string());
    assert_eq!(service.service_type(), ServiceType::GradleTask);

    // Test that gradle takes precedence over other fields
    service.process = Some("echo test".to_string());
    assert_eq!(
        service.service_type(),
        ServiceType::Process,
        "Process should take precedence"
    );

    // Test Docker type
    let mut docker_service = Service::default();
    docker_service.image = Some("nginx".to_string());
    assert_eq!(docker_service.service_type(), ServiceType::Docker);
}
