use fed::config::{Config, Parameter};
use fed::parameter::Resolver;
use serde_yaml::Value;

mod support;

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
fn explicit_value_overrides_templated_default_and_reaches_service() {
    let mut config = support::parse_checked(
        r#"
parameters:
  HOST:
    default: localhost
  ENDPOINT:
    default: "http://{{HOST}}:3000"
  DESCRIPTION:
    default: "{{ENDPOINT}}/description"
services:
  app:
    process: "echo ready"
    environment:
      ENDPOINT: "{{ENDPOINT}}"
      DESCRIPTION: "{{DESCRIPTION}}"
"#,
    );
    config.parameters.get_mut("ENDPOINT").unwrap().value = Some("https://example.test".into());
    let mut resolver = Resolver::new();
    resolver.resolve_parameters(&mut config).unwrap();
    let resolved = resolver.resolve_config(&config).unwrap();

    assert_eq!(
        resolved.services["app"].environment["ENDPOINT"],
        "https://example.test"
    );
    assert_eq!(
        resolved.services["app"].environment["DESCRIPTION"],
        "https://example.test/description"
    );
}

#[test]
fn env_file_value_overrides_templated_default() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join(".env"), "ENDPOINT=https://example.test\n").unwrap();
    let mut config = support::parse_checked(
        r#"
env_file: [.env]
parameters:
  HOST:
    default: localhost
  ENDPOINT:
    default: "http://{{HOST}}:3000"
services:
  app:
    process: "echo ready"
    environment:
      ENDPOINT: "{{ENDPOINT}}"
"#,
    );
    let mut resolver = Resolver::new();
    resolver.set_work_dir(dir.path());
    resolver.resolve_parameters(&mut config).unwrap();
    let resolved = resolver.resolve_config(&config).unwrap();

    assert_eq!(
        resolved.services["app"].environment["ENDPOINT"],
        "https://example.test"
    );
}

#[test]
fn env_file_literal_value_does_not_validate_unused_template_default() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join(".env"), "TEXT='literal {{UNEXPANDED}}'\n").unwrap();
    let mut config = support::parse_checked(
        r#"
env_file: [.env]
parameters:
  TEXT:
    default: "{{MISSING}}"
services:
  app:
    process: "echo ready"
    environment:
      TEXT: "{{TEXT}}"
"#,
    );
    let mut resolver = Resolver::new();
    resolver.set_work_dir(dir.path());
    resolver.resolve_parameters(&mut config).unwrap();
    let resolved = resolver.resolve_config(&config).unwrap();

    assert_eq!(
        resolved.services["app"].environment["TEXT"],
        "literal {{UNEXPANDED}}"
    );
}
