use fed::config::{Config, Environment, Parameter};
use fed::parameter::Resolver;
use serde_yaml::Value;

#[test]
fn test_environment_specific_values() {
    let mut resolver = Resolver::with_environment(Environment::Production);
    let mut config = Config::default();

    // Create a parameter with environment-specific values
    let param = Parameter {
        default: Some(Value::String("dev-value".to_string())),
        staging: Some(Value::String("staging-value".to_string())),
        production: Some(Value::String("prod-value".to_string())),
        ..Default::default()
    };

    config.parameters.insert("TEST_VAR".to_string(), param);

    resolver.resolve_parameters(&mut config).unwrap();

    let resolved = resolver.get_resolved_parameters();
    assert_eq!(resolved.get("TEST_VAR").unwrap(), "prod-value");
}

#[test]
fn test_environment_fallback_to_default() {
    let mut resolver = Resolver::with_environment(Environment::Staging);
    let mut config = Config::default();

    // Parameter with default but no staging-specific value
    let param = Parameter {
        default: Some(Value::String("default-value".to_string())),
        production: Some(Value::String("prod-value".to_string())),
        ..Default::default()
    };

    config.parameters.insert("TEST_VAR".to_string(), param);

    resolver.resolve_parameters(&mut config).unwrap();

    let resolved = resolver.get_resolved_parameters();
    assert_eq!(resolved.get("TEST_VAR").unwrap(), "default-value");
}

#[test]
fn test_development_alias() {
    let mut resolver = Resolver::with_environment(Environment::Development);
    let mut config = Config::default();

    // Parameter with "develop" (alias) instead of "development"
    let param = Parameter {
        default: Some(Value::String("default-value".to_string())),
        develop: Some(Value::String("develop-value".to_string())),
        ..Default::default()
    };

    config.parameters.insert("TEST_VAR".to_string(), param);

    resolver.resolve_parameters(&mut config).unwrap();

    let resolved = resolver.get_resolved_parameters();
    assert_eq!(resolved.get("TEST_VAR").unwrap(), "develop-value");
}

#[test]
fn test_development_precedence() {
    let mut resolver = Resolver::with_environment(Environment::Development);
    let mut config = Config::default();

    // Parameter with both "development" and "develop" - "development" takes precedence
    let param = Parameter {
        default: Some(Value::String("default-value".to_string())),
        development: Some(Value::String("development-value".to_string())),
        develop: Some(Value::String("develop-value".to_string())),
        ..Default::default()
    };

    config.parameters.insert("TEST_VAR".to_string(), param);

    resolver.resolve_parameters(&mut config).unwrap();

    let resolved = resolver.get_resolved_parameters();
    assert_eq!(resolved.get("TEST_VAR").unwrap(), "development-value");
}

#[test]
fn test_basic_parameter_resolution() {
    let mut resolver = Resolver::new();
    let mut config = Config::default();

    let param = Parameter {
        default: Some(Value::String("param-value".to_string())),
        ..Default::default()
    };

    config.parameters.insert("TEST_VAR".to_string(), param);

    resolver.resolve_parameters(&mut config).unwrap();

    let resolved = resolver.get_resolved_parameters();
    assert_eq!(resolved.get("TEST_VAR").unwrap(), "param-value");
}

#[test]
fn test_port_type_with_environment() {
    let mut resolver = Resolver::with_environment(Environment::Production);
    let mut config = Config::default();

    // Find an available port
    let test_listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let available_port = test_listener.local_addr().unwrap().port();
    drop(test_listener);

    // Port parameter with environment-specific default
    let param = Parameter {
        param_type: Some("port".to_string()),
        default: Some(Value::Number(8000.into())),
        production: Some(Value::Number(available_port.into())),
        ..Default::default()
    };

    config.parameters.insert("API_PORT".to_string(), param);

    resolver.resolve_parameters(&mut config).unwrap();

    let resolved = resolver.get_resolved_parameters();
    let port: u16 = resolved.get("API_PORT").unwrap().parse().unwrap();

    // Should use production-specific port
    assert_eq!(port, available_port);
}

#[test]
fn test_template_resolution_with_environment() {
    let mut resolver = Resolver::with_environment(Environment::Staging);
    let mut config = Config::default();

    // Base parameter
    let base_param = Parameter {
        default: Some(Value::String("dev-db".to_string())),
        staging: Some(Value::String("staging-db".to_string())),
        production: Some(Value::String("prod-db".to_string())),
        ..Default::default()
    };

    // Parameter that references base
    let url_param = Parameter {
        default: Some(Value::String("postgres://{{DB_NAME}}".to_string())),
        ..Default::default()
    };

    config.parameters.insert("DB_NAME".to_string(), base_param);
    config.parameters.insert("DB_URL".to_string(), url_param);

    resolver.resolve_parameters(&mut config).unwrap();

    let resolved = resolver.get_resolved_parameters();
    assert_eq!(resolved.get("DB_NAME").unwrap(), "staging-db");
    assert_eq!(resolved.get("DB_URL").unwrap(), "postgres://staging-db");
}

#[test]
fn test_complex_multi_environment_config() {
    let mut resolver = Resolver::with_environment(Environment::Production);
    let mut config = Config::default();

    // DEBUG_MODE: true in dev, false in production
    let debug_param = Parameter {
        default: Some(Value::String("true".to_string())),
        production: Some(Value::String("false".to_string())),
        ..Default::default()
    };

    // REPLICA_COUNT: 1 in dev, 2 in staging, 5 in production
    let replica_param = Parameter {
        default: Some(Value::Number(1.into())),
        staging: Some(Value::Number(2.into())),
        production: Some(Value::Number(5.into())),
        ..Default::default()
    };

    // JWT_SECRET: static in dev, different secret in production
    let secret_param = Parameter {
        default: Some(Value::String("dev-secret".to_string())),
        production: Some(Value::String("prod-secret-from-vault".to_string())),
        ..Default::default()
    };

    config
        .parameters
        .insert("DEBUG_MODE".to_string(), debug_param);
    config
        .parameters
        .insert("REPLICA_COUNT".to_string(), replica_param);
    config
        .parameters
        .insert("JWT_SECRET".to_string(), secret_param);

    resolver.resolve_parameters(&mut config).unwrap();

    let resolved = resolver.get_resolved_parameters();
    assert_eq!(resolved.get("DEBUG_MODE").unwrap(), "false");
    assert_eq!(resolved.get("REPLICA_COUNT").unwrap(), "5");
    assert_eq!(
        resolved.get("JWT_SECRET").unwrap(),
        "prod-secret-from-vault"
    );
}

/// The `variables:` top-level key was an alias for `parameters:`, removed in
/// 4.0. A config still using it must fail validation with a migration message
/// rather than being silently ignored.
#[test]
fn test_legacy_variables_key_rejected() {
    let yaml = r#"
variables:
  API_PORT:
    type: port
    default: 8080
services:
  api:
    process: "echo hi"
"#;
    let config: Config = serde_yaml::from_str(yaml).unwrap();
    let err = config.validate().unwrap_err().to_string();
    assert!(
        err.contains("`variables:`") && err.contains("`parameters:`"),
        "expected a migration message pointing variables -> parameters, got: {err}"
    );
}

/// An empty/absent `variables:` key must not trip the migration error.
#[test]
fn test_no_variables_key_is_fine() {
    let yaml = r#"
parameters:
  API_PORT:
    type: port
    default: 8080
services:
  api:
    process: "echo hi"
    startup_message: "http://localhost:{{API_PORT}}"
entrypoint: api
"#;
    let config: Config = serde_yaml::from_str(yaml).unwrap();
    assert!(config.validate().is_ok());
}

#[test]
fn test_environment_from_string() {
    assert_eq!(
        "development".parse::<Environment>().unwrap(),
        Environment::Development
    );
    assert_eq!(
        "develop".parse::<Environment>().unwrap(),
        Environment::Development
    );
    assert_eq!(
        "staging".parse::<Environment>().unwrap(),
        Environment::Staging
    );
    assert_eq!(
        "production".parse::<Environment>().unwrap(),
        Environment::Production
    );

    // Case insensitive
    assert_eq!(
        "PRODUCTION".parse::<Environment>().unwrap(),
        Environment::Production
    );

    // Invalid
    assert!("invalid".parse::<Environment>().is_err());
}
