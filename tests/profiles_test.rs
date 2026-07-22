use fed::Parser;

#[path = "support/mod.rs"]
mod support;

#[test]
fn test_profiles_parsing() {
    let parser = Parser::new();
    let config = parser
        .load_config("tests/fixtures/profiles.yaml")
        .expect("Failed to load profiles config");

    config.validate().expect("Config validation failed");

    // Check profiles field exists on services
    assert_eq!(
        config.services["monitoring"].profiles,
        vec!["production", "staging"]
    );
    assert_eq!(
        config.services["development-tools"].profiles,
        vec!["development"]
    );
    assert_eq!(
        config.services["experimental-metrics"].profiles,
        vec!["experimental"]
    );
    assert!(config.services["api"].profiles.is_empty());
    assert_eq!(
        config.services["analytics"].profiles,
        vec!["production", "staging", "analytics"]
    );
}

#[tokio::test]
async fn test_no_profiles_active_starts_only_profileless_services() {
    let parser = Parser::new();
    let config = parser
        .load_config("tests/fixtures/profiles.yaml")
        .expect("Failed to load config");

    // Create orchestrator with no active profiles
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

    // Get status - only api should be in the orchestrator
    let status = orchestrator.get_status().await;

    // Only service without profiles should be present
    assert!(status.contains_key("api"), "api should be present");
    assert!(
        !status.contains_key("monitoring"),
        "monitoring should not be present"
    );
    assert!(
        !status.contains_key("development-tools"),
        "development-tools should not be present"
    );
    assert!(
        !status.contains_key("experimental-metrics"),
        "experimental-metrics should not be present"
    );
    assert!(
        !status.contains_key("analytics"),
        "analytics should not be present"
    );
}

#[tokio::test]
async fn test_production_profile_filters_services() {
    let parser = Parser::new();
    let config = parser
        .load_config("tests/fixtures/profiles.yaml")
        .expect("Failed to load config");

    // Create orchestrator with production profile
    let temp_dir = tempfile::tempdir().unwrap();
    let mut orchestrator =
        support::new_orchestrator_for_test(config, temp_dir.path().to_path_buf())
            .await
            .unwrap()
            .with_profiles(vec!["production".to_string()]);
    orchestrator.set_auto_resolve_conflicts(true);
    orchestrator
        .initialize()
        .await
        .expect("Failed to initialize");

    let status = orchestrator.get_status().await;

    // Should have: api (no profile), monitoring (production), analytics (production), reporting (production)
    assert!(status.contains_key("api"), "api should be present");
    assert!(
        status.contains_key("monitoring"),
        "monitoring should be present"
    );
    assert!(
        status.contains_key("analytics"),
        "analytics should be present"
    );
    assert!(
        status.contains_key("reporting"),
        "reporting should be present"
    );

    // Should NOT have: development-tools (development), experimental-metrics (experimental)
    assert!(
        !status.contains_key("development-tools"),
        "development-tools should not be present"
    );
    assert!(
        !status.contains_key("experimental-metrics"),
        "experimental-metrics should not be present"
    );
    assert!(
        !status.contains_key("debugger"),
        "debugger should not be present"
    );
}

#[tokio::test]
async fn test_development_profile_filters_services() {
    let parser = Parser::new();
    let config = parser
        .load_config("tests/fixtures/profiles.yaml")
        .expect("Failed to load config");

    // Create orchestrator with development profile
    let temp_dir = tempfile::tempdir().unwrap();
    let mut orchestrator =
        support::new_orchestrator_for_test(config, temp_dir.path().to_path_buf())
            .await
            .unwrap()
            .with_profiles(vec!["development".to_string()]);
    orchestrator.set_auto_resolve_conflicts(true);
    orchestrator
        .initialize()
        .await
        .expect("Failed to initialize");

    let status = orchestrator.get_status().await;

    // Should have: api (no profile), development-tools (development), debugger (development)
    assert!(status.contains_key("api"), "api should be present");
    assert!(
        status.contains_key("development-tools"),
        "development-tools should be present"
    );
    assert!(
        status.contains_key("debugger"),
        "debugger should be present"
    );

    // Should NOT have: monitoring, experimental-metrics, analytics, reporting
    assert!(
        !status.contains_key("monitoring"),
        "monitoring should not be present"
    );
    assert!(
        !status.contains_key("experimental-metrics"),
        "experimental-metrics should not be present"
    );
    assert!(
        !status.contains_key("analytics"),
        "analytics should not be present"
    );
    assert!(
        !status.contains_key("reporting"),
        "reporting should not be present"
    );
}

#[tokio::test]
async fn test_multiple_profiles() {
    let parser = Parser::new();
    let config = parser
        .load_config("tests/fixtures/profiles.yaml")
        .expect("Failed to load config");

    // Create orchestrator with production AND experimental profiles
    let temp_dir = tempfile::tempdir().unwrap();
    let mut orchestrator =
        support::new_orchestrator_for_test(config, temp_dir.path().to_path_buf())
            .await
            .unwrap()
            .with_profiles(vec!["production".to_string(), "experimental".to_string()]);
    orchestrator.set_auto_resolve_conflicts(true);
    orchestrator
        .initialize()
        .await
        .expect("Failed to initialize");

    let status = orchestrator.get_status().await;

    // Should have production services AND experimental services
    assert!(status.contains_key("api"), "api should be present");
    assert!(
        status.contains_key("monitoring"),
        "monitoring should be present"
    );
    assert!(
        status.contains_key("analytics"),
        "analytics should be present"
    );
    assert!(
        status.contains_key("reporting"),
        "reporting should be present"
    );
    assert!(
        status.contains_key("experimental-metrics"),
        "experimental-metrics should be present"
    );

    // Should NOT have development services
    assert!(
        !status.contains_key("development-tools"),
        "development-tools should not be present"
    );
    assert!(
        !status.contains_key("debugger"),
        "debugger should not be present"
    );
}

#[tokio::test]
async fn test_staging_profile() {
    let parser = Parser::new();
    let config = parser
        .load_config("tests/fixtures/profiles.yaml")
        .expect("Failed to load config");

    // Create orchestrator with staging profile
    let temp_dir = tempfile::tempdir().unwrap();
    let mut orchestrator =
        support::new_orchestrator_for_test(config, temp_dir.path().to_path_buf())
            .await
            .unwrap()
            .with_profiles(vec!["staging".to_string()]);
    orchestrator.set_auto_resolve_conflicts(true);
    orchestrator
        .initialize()
        .await
        .expect("Failed to initialize");

    let status = orchestrator.get_status().await;

    // monitoring and analytics both have "staging" profile
    assert!(status.contains_key("api"), "api should be present");
    assert!(
        status.contains_key("monitoring"),
        "monitoring should be present"
    );
    assert!(
        status.contains_key("analytics"),
        "analytics should be present"
    );

    // reporting depends on analytics (which is included), but reporting itself only has production profile
    assert!(
        !status.contains_key("reporting"),
        "reporting should not be present (only has production profile)"
    );
}

#[tokio::test]
async fn test_analytics_profile() {
    let parser = Parser::new();
    let config = parser
        .load_config("tests/fixtures/profiles.yaml")
        .expect("Failed to load config");

    // Create orchestrator with analytics profile (testing OR logic)
    let temp_dir = tempfile::tempdir().unwrap();
    let mut orchestrator =
        support::new_orchestrator_for_test(config, temp_dir.path().to_path_buf())
            .await
            .unwrap()
            .with_profiles(vec!["analytics".to_string()]);
    orchestrator.set_auto_resolve_conflicts(true);
    orchestrator
        .initialize()
        .await
        .expect("Failed to initialize");

    let status = orchestrator.get_status().await;

    // analytics service has ["production", "staging", "analytics"] so it should match
    assert!(status.contains_key("api"), "api should be present");
    assert!(
        status.contains_key("analytics"),
        "analytics should be present"
    );

    // Other services should not be present
    assert!(
        !status.contains_key("monitoring"),
        "monitoring should not be present"
    );
    assert!(
        !status.contains_key("reporting"),
        "reporting should not be present"
    );
}

#[tokio::test]
async fn test_dependency_filtering() {
    let parser = Parser::new();
    let config = parser
        .load_config("tests/fixtures/profiles.yaml")
        .expect("Failed to load config");

    // reporting depends on analytics and monitoring
    // All three have production profile, so all should be included
    let temp_dir = tempfile::tempdir().unwrap();
    let mut orchestrator =
        support::new_orchestrator_for_test(config, temp_dir.path().to_path_buf())
            .await
            .unwrap()
            .with_profiles(vec!["production".to_string()]);
    orchestrator.set_auto_resolve_conflicts(true);
    orchestrator
        .initialize()
        .await
        .expect("Failed to initialize");

    let status = orchestrator.get_status().await;

    assert!(
        status.contains_key("reporting"),
        "reporting should be present"
    );
    assert!(
        status.contains_key("analytics"),
        "analytics (dependency) should be present"
    );
    assert!(
        status.contains_key("monitoring"),
        "monitoring (dependency) should be present"
    );
}

#[test]
fn test_service_with_empty_profiles_always_starts() {
    let parser = Parser::new();
    let config = parser
        .load_config("tests/fixtures/profiles.yaml")
        .expect("Failed to load config");

    // api service has no profiles, so it should always be startable
    assert!(config.services["api"].profiles.is_empty());
}

#[tokio::test]
async fn test_profile_case_sensitivity() {
    let parser = Parser::new();
    let config = parser
        .load_config("tests/fixtures/profiles.yaml")
        .expect("Failed to load config");

    // Profiles are case-sensitive, so "Production" should NOT match "production"
    let temp_dir = tempfile::tempdir().unwrap();
    let mut orchestrator =
        support::new_orchestrator_for_test(config, temp_dir.path().to_path_buf())
            .await
            .unwrap()
            .with_profiles(vec!["Production".to_string()]); // Note: capital P
    orchestrator.set_auto_resolve_conflicts(true);
    orchestrator
        .initialize()
        .await
        .expect("Failed to initialize");

    let status = orchestrator.get_status().await;

    // Only api should be present (no profile), monitoring should NOT be present
    assert!(status.contains_key("api"), "api should be present");
    assert!(
        !status.contains_key("monitoring"),
        "monitoring should not be present (case mismatch)"
    );
}

#[tokio::test]
async fn test_nonexistent_profile() {
    let parser = Parser::new();
    let config = parser
        .load_config("tests/fixtures/profiles.yaml")
        .expect("Failed to load config");

    // Use a profile that doesn't exist in any service
    let temp_dir = tempfile::tempdir().unwrap();
    let mut orchestrator =
        support::new_orchestrator_for_test(config, temp_dir.path().to_path_buf())
            .await
            .unwrap()
            .with_profiles(vec!["nonexistent".to_string()]);
    orchestrator.set_auto_resolve_conflicts(true);
    orchestrator
        .initialize()
        .await
        .expect("Failed to initialize");

    let status = orchestrator.get_status().await;

    // Only api should be present (no profile requirement)
    assert!(status.contains_key("api"), "api should be present");
    assert_eq!(status.len(), 1, "Only api should be present");
}
