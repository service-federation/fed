use fed::config::{Config, Environment, Parameter};
use fed::parameter::Resolver;
use serde_yaml::Value;

#[test]
fn test_environment_specific_values() {
    let mut resolver = Resolver::with_environment(Environment::Production);
    let mut config = Config::default();

    // Create a variable with environment-specific values
    let param = Parameter {
        param_type: None,
        default: Some(Value::String("dev-value".to_string())),
        either: vec![],
        development: None,
        develop: None,
        staging: Some(Value::String("staging-value".to_string())),
        production: Some(Value::String("prod-value".to_string())),
        source: None,
        description: None,
        optional: None,
        generate: None,
        value: None,
    };

    config.variables.insert("TEST_VAR".to_string(), param);

    resolver.resolve_parameters(&mut config).unwrap();

    let resolved = resolver.get_resolved_parameters();
    assert_eq!(resolved.get("TEST_VAR").unwrap(), "prod-value");
}

#[test]
fn test_environment_fallback_to_default() {
    let mut resolver = Resolver::with_environment(Environment::Staging);
    let mut config = Config::default();

    // Variable with default but no staging-specific value
    let param = Parameter {
        param_type: None,
        default: Some(Value::String("default-value".to_string())),
        either: vec![],
        development: None,
        develop: None,
        staging: None,
        production: Some(Value::String("prod-value".to_string())),
        source: None,
        description: None,
        optional: None,
        generate: None,
        value: None,
    };

    config.variables.insert("TEST_VAR".to_string(), param);

    resolver.resolve_parameters(&mut config).unwrap();

    let resolved = resolver.get_resolved_parameters();
    assert_eq!(resolved.get("TEST_VAR").unwrap(), "default-value");
}

#[test]
fn test_development_alias() {
    let mut resolver = Resolver::with_environment(Environment::Development);
    let mut config = Config::default();

    // Variable with "develop" (alias) instead of "development"
    let param = Parameter {
        param_type: None,
        default: Some(Value::String("default-value".to_string())),
        either: vec![],
        development: None,
        develop: Some(Value::String("develop-value".to_string())),
        staging: None,
        production: None,
        source: None,
        description: None,
        optional: None,
        generate: None,
        value: None,
    };

    config.variables.insert("TEST_VAR".to_string(), param);

    resolver.resolve_parameters(&mut config).unwrap();

    let resolved = resolver.get_resolved_parameters();
    assert_eq!(resolved.get("TEST_VAR").unwrap(), "develop-value");
}

#[test]
fn test_development_precedence() {
    let mut resolver = Resolver::with_environment(Environment::Development);
    let mut config = Config::default();

    // Variable with both "development" and "develop" - "development" takes precedence
    let param = Parameter {
        param_type: None,
        default: Some(Value::String("default-value".to_string())),
        either: vec![],
        development: Some(Value::String("development-value".to_string())),
        develop: Some(Value::String("develop-value".to_string())),
        staging: None,
        production: None,
        source: None,
        description: None,
        optional: None,
        generate: None,
        value: None,
    };

    config.variables.insert("TEST_VAR".to_string(), param);

    resolver.resolve_parameters(&mut config).unwrap();

    let resolved = resolver.get_resolved_parameters();
    assert_eq!(resolved.get("TEST_VAR").unwrap(), "development-value");
}

#[test]
fn test_variables_take_precedence_over_parameters() {
    let mut resolver = Resolver::with_environment(Environment::Production);
    let mut config = Config::default();

    // Add both parameters and variables
    let param_param = Parameter {
        param_type: None,
        default: Some(Value::String("param-value".to_string())),
        either: vec![],
        development: None,
        develop: None,
        staging: None,
        production: None,
        source: None,
        description: None,
        optional: None,
        generate: None,
        value: None,
    };

    let var_param = Parameter {
        param_type: None,
        default: Some(Value::String("var-value".to_string())),
        either: vec![],
        development: None,
        develop: None,
        staging: None,
        production: Some(Value::String("var-prod-value".to_string())),
        source: None,
        description: None,
        optional: None,
        generate: None,
        value: None,
    };

    config
        .parameters
        .insert("TEST_VAR".to_string(), param_param);
    config.variables.insert("TEST_VAR".to_string(), var_param);

    resolver.resolve_parameters(&mut config).unwrap();

    let resolved = resolver.get_resolved_parameters();
    // variables should take precedence
    assert_eq!(resolved.get("TEST_VAR").unwrap(), "var-prod-value");
}

#[test]
fn test_backward_compatibility_with_parameters() {
    let mut resolver = Resolver::new();
    let mut config = Config::default();

    // Only parameters field is used (backward compatibility)
    let param = Parameter {
        param_type: None,
        default: Some(Value::String("param-value".to_string())),
        either: vec![],
        development: None,
        develop: None,
        staging: None,
        production: None,
        source: None,
        description: None,
        optional: None,
        generate: None,
        value: None,
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
        either: vec![],
        development: None,
        develop: None,
        staging: None,
        production: Some(Value::Number(available_port.into())),
        source: None,
        description: None,
        optional: None,
        generate: None,
        value: None,
    };

    config.variables.insert("API_PORT".to_string(), param);

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

    // Base variable
    let base_param = Parameter {
        param_type: None,
        default: Some(Value::String("dev-db".to_string())),
        either: vec![],
        development: None,
        develop: None,
        staging: Some(Value::String("staging-db".to_string())),
        production: Some(Value::String("prod-db".to_string())),
        source: None,
        description: None,
        optional: None,
        generate: None,
        value: None,
    };

    // Variable that references base
    let url_param = Parameter {
        param_type: None,
        default: Some(Value::String("postgres://{{DB_NAME}}".to_string())),
        either: vec![],
        development: None,
        develop: None,
        staging: None,
        production: None,
        source: None,
        description: None,
        optional: None,
        generate: None,
        value: None,
    };

    config.variables.insert("DB_NAME".to_string(), base_param);
    config.variables.insert("DB_URL".to_string(), url_param);

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
        param_type: None,
        default: Some(Value::String("true".to_string())),
        either: vec![],
        development: None,
        develop: None,
        staging: None,
        production: Some(Value::String("false".to_string())),
        source: None,
        description: None,
        optional: None,
        generate: None,
        value: None,
    };

    // REPLICA_COUNT: 1 in dev, 2 in staging, 5 in production
    let replica_param = Parameter {
        param_type: None,
        default: Some(Value::Number(1.into())),
        either: vec![],
        development: None,
        develop: None,
        staging: Some(Value::Number(2.into())),
        production: Some(Value::Number(5.into())),
        source: None,
        description: None,
        optional: None,
        generate: None,
        value: None,
    };

    // JWT_SECRET: static in dev, different secret in production
    let secret_param = Parameter {
        param_type: None,
        default: Some(Value::String("dev-secret".to_string())),
        either: vec![],
        development: None,
        develop: None,
        staging: None,
        production: Some(Value::String("prod-secret-from-vault".to_string())),
        source: None,
        description: None,
        optional: None,
        generate: None,
        value: None,
    };

    config
        .variables
        .insert("DEBUG_MODE".to_string(), debug_param);
    config
        .variables
        .insert("REPLICA_COUNT".to_string(), replica_param);
    config
        .variables
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

#[test]
fn test_get_effective_parameters() {
    let mut config = Config::default();

    // Add parameters
    let param1 = Parameter {
        param_type: None,
        default: Some(Value::String("param-value".to_string())),
        either: vec![],
        development: None,
        develop: None,
        staging: None,
        production: None,
        source: None,
        description: None,
        optional: None,
        generate: None,
        value: None,
    };
    config.parameters.insert("PARAM1".to_string(), param1);

    // Add variables
    let var1 = Parameter {
        param_type: None,
        default: Some(Value::String("var-value".to_string())),
        either: vec![],
        development: None,
        develop: None,
        staging: None,
        production: None,
        source: None,
        description: None,
        optional: None,
        generate: None,
        value: None,
    };
    config.variables.insert("VAR1".to_string(), var1);

    // When variables are present, they should be returned
    let effective = config.get_effective_parameters();
    assert!(effective.contains_key("VAR1"));
    assert!(!effective.contains_key("PARAM1"));

    // When variables are empty, parameters should be returned
    config.variables.clear();
    let effective = config.get_effective_parameters();
    assert!(effective.contains_key("PARAM1"));
    assert!(!effective.contains_key("VAR1"));
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
