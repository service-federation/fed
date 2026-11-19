mod support;

use fed::parameter::Resolver;

#[test]
fn command_healthchecks_treat_parameter_values_as_literal_shell_arguments() {
    let fixtures = [
        r#"
parameters:
  MESSAGE:
    default: safe
services:
  app:
    process: "true"
    environment:
      RAW_MESSAGE: "{{MESSAGE}}"
    healthcheck: "printf '%s' {{MESSAGE}}"
"#,
        r#"
parameters:
  MESSAGE:
    default: safe
services:
  app:
    process: "true"
    environment:
      RAW_MESSAGE: "{{MESSAGE}}"
    healthcheck:
      command: "printf '%s' {{MESSAGE}}"
      timeout: 2s
"#,
    ];
    let dir = tempfile::tempdir().unwrap();
    let marker = dir.path().join("injected");
    let payload = format!(
        "$(printf injected > {}); literal 'quotes'",
        marker.display()
    );

    for yaml in fixtures {
        let base = support::parse_checked(yaml);
        for command in [
            "printf '%s' {{MESSAGE}}",
            "printf '%s' '{{MESSAGE}}'",
            "printf '%s' \"{{MESSAGE}}\"",
        ] {
            let mut config = base.clone();
            config.parameters.get_mut("MESSAGE").unwrap().value = Some(payload.clone());
            match config
                .services
                .get_mut("app")
                .unwrap()
                .healthcheck
                .as_mut()
                .unwrap()
            {
                fed::config::HealthCheck::Command(value) => *value = command.to_string(),
                fed::config::HealthCheck::CommandMap { command: value, .. } => {
                    *value = command.to_string()
                }
                _ => unreachable!(),
            }
            let mut resolver = Resolver::new();
            resolver.set_work_dir(dir.path());
            resolver.resolve_parameters(&mut config).unwrap();
            let resolved = resolver.resolve_config(&config).unwrap();
            let service = resolved.services.get("app").unwrap();
            let command = service.healthcheck.as_ref().unwrap().get_command().unwrap();
            let output = std::process::Command::new("bash")
                .args(["-c", command])
                .output()
                .unwrap();
            assert!(output.status.success());
            assert_eq!(String::from_utf8(output.stdout).unwrap(), payload);
            assert!(!marker.exists(), "healthcheck executed parameter contents");
            assert_eq!(service.environment.get("RAW_MESSAGE"), Some(&payload));
        }
    }
}

#[test]
fn http_healthchecks_keep_parameter_values_unescaped() {
    let mut config = support::parse_checked(
        r#"
parameters:
  URL:
    default: "https://example.invalid/health?token=a&ready=1"
services:
  app:
    process: "true"
    healthcheck:
      http_get: "{{URL}}"
"#,
    );
    let mut resolver = Resolver::new();
    resolver.resolve_parameters(&mut config).unwrap();
    let resolved = resolver.resolve_config(&config).unwrap();
    assert_eq!(
        resolved.services["app"]
            .healthcheck
            .as_ref()
            .unwrap()
            .get_http_url(),
        Some("https://example.invalid/health?token=a&ready=1")
    );
}
