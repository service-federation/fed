use fed::Parser;
use fed::config::ServiceType;

#[test]
fn test_parse_gradle_example() {
    let parser = Parser::new();
    let config = parser
        .load_config("examples/gradle-example.yaml")
        .expect("Failed to load gradle example");

    // Validate the config
    config.validate().expect("Config validation failed");

    // Check services
    assert_eq!(config.services.len(), 2);
    assert!(config.services.contains_key("auth-service"));
    assert!(config.services.contains_key("user-service"));

    // Check entrypoint
    assert_eq!(config.entrypoint, Some("user-service".to_string()));

    // Check parameters
    assert!(config.parameters.contains_key("AUTH_SERVICE_PORT"));
    assert!(config.parameters.contains_key("USER_SERVICE_PORT"));
    assert!(config.parameters.contains_key("LOG_LEVEL"));

    // Check gradle task services
    let auth_service = &config.services["auth-service"];
    assert_eq!(auth_service.service_type(), ServiceType::GradleTask);
    assert_eq!(
        auth_service.gradle_task.as_ref().unwrap(),
        ":auth-service:bootRun"
    );
    assert_eq!(auth_service.cwd, None);

    let user_service = &config.services["user-service"];
    assert_eq!(user_service.service_type(), ServiceType::GradleTask);
    assert_eq!(
        user_service.gradle_task.as_ref().unwrap(),
        ":user-service:bootRun"
    );
    assert_eq!(user_service.cwd, Some("services/user".to_string()));

    // Check dependencies
    assert_eq!(user_service.depends_on.len(), 1);
    assert_eq!(user_service.depends_on[0].service_name(), "auth-service");

    // Check healthchecks
    assert!(auth_service.healthcheck.is_some());
    assert!(user_service.healthcheck.is_some());

    // Check environment variables
    assert!(auth_service.environment.contains_key("SERVER_PORT"));
    assert!(auth_service.environment.contains_key("LOG_LEVEL"));
    assert!(user_service.environment.contains_key("AUTH_SERVICE_URL"));
}

#[test]
fn test_gradle_task_environment_resolution() {
    // Prevent interactive port conflict prompt from blocking the test
    // when a default port happens to be occupied on the host machine
    std::env::set_var("FED_NON_INTERACTIVE", "1");

    let parser = Parser::new();
    let mut config = parser
        .load_config("examples/gradle-example.yaml")
        .expect("Failed to load gradle example");

    // Validate
    config.validate().expect("Config validation failed");

    // Create resolver and resolve parameters
    let mut resolver = fed::parameter::Resolver::new();
    resolver
        .resolve_parameters(&mut config)
        .expect("Failed to resolve parameters");

    let resolved_config = resolver
        .resolve_config(&config)
        .expect("Failed to resolve config");

    // Check that environment variables have been resolved
    let auth_service = &resolved_config.services["auth-service"];
    let port = &auth_service.environment["SERVER_PORT"];

    // Port should be resolved (either 8080 or a random port if 8080 is in use)
    assert!(
        !port.contains("{{"),
        "Port should be resolved, got: {}",
        port
    );

    let port_num: u16 = port.parse().expect("Port should be a number");
    assert!(port_num > 0);

    // Check LOG_LEVEL is resolved
    let log_level = &auth_service.environment["LOG_LEVEL"];
    assert_eq!(log_level, "INFO");

    // Check user service
    let user_service = &resolved_config.services["user-service"];
    let auth_url = &user_service.environment["AUTH_SERVICE_URL"];
    assert!(
        auth_url.starts_with("http://localhost:"),
        "AUTH_SERVICE_URL should be resolved"
    );
    assert!(!auth_url.contains("{{"));
}
