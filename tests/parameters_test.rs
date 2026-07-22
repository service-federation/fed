use fed::config::{Config, Parameter};
use fed::parameter::Resolver;
use serde_yaml::Value;

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
