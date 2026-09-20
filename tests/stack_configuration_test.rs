//! End-to-end configuration and CLI coverage for shared stack configuration.
use fed::config::ServiceType;
use fed::{Config, Orchestrator, OutputMode, Parser, RunContext, StartHealth};
use std::process::Command;
use std::time::Duration;

#[path = "support/mod.rs"]
mod support;

fn load(yaml: &str) -> Config {
    support::parse_checked(yaml);
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("fed.yaml");
    std::fs::write(&path, yaml).unwrap();
    Parser::new().load_config(path).unwrap()
}

const DEFAULTS: &str = r#"
defaults:
  depends_on: [storage]
  startup_timeout: 20s
  healthcheck_timeout: 4s
  environment: { SHARED: fallback }
templates:
  worker-base:
    process: sleep 30
    startup_timeout: 12s
    environment: { SHARED: template }
  detached-base:
    process: sleep 30
    depends_on: []
services:
  storage: { process: 'sleep 30' }
  plain: { process: 'sleep 30', startup_timeout: 8s }
  inherited: { extends: worker-base }
  detached: { extends: detached-base }
  opted-out: { process: 'sleep 30', depends_on: [] }
  catalog:
    healthcheck: { command: 'true' }
    variants:
      go: { extends: worker-base, cwd: . }
      java: { process: 'sleep 30' }
    default_variant: go
"#;

#[test]
fn defaults_templates_variants_and_explicit_empty_have_consistent_precedence() {
    let mut config = load(DEFAULTS);
    config.validate().unwrap();
    fed::config::variants::resolve_variants(&mut config, &Default::default(), &[]).unwrap();
    let svc = &config.services;
    assert_eq!(svc["plain"].startup_timeout.as_deref(), Some("8s"));
    assert_eq!(svc["inherited"].startup_timeout.as_deref(), Some("12s"));
    assert_eq!(svc["inherited"].environment["SHARED"], "template");
    assert_eq!(svc["catalog"].process.as_deref(), Some("sleep 30"));
    assert_eq!(svc["catalog"].cwd.as_deref(), Some("."));
    assert_eq!(svc["catalog"].startup_timeout.as_deref(), Some("12s"));
    assert_eq!(
        svc["catalog"].effective_start_period(),
        Some(Duration::from_secs(4))
    );
    for name in ["plain", "inherited", "catalog"] {
        assert_eq!(svc[name].depends_on[0].service_name(), "storage");
    }
    for name in ["storage", "detached", "opted-out"] {
        assert!(svc[name].depends_on.is_empty(), "{name}");
    }
    assert!(svc["storage"].startup_timeout.is_none());
    assert!(svc["storage"].environment.is_empty());
    assert_eq!(
        config.templates["worker-base"]
            .healthcheck_start_period
            .as_deref(),
        Some("4s")
    );
}

#[test]
fn unknown_variant_template_names_the_location() {
    let dir = tempfile::tempdir().unwrap();
    let yaml = r#"
services:
  catalog:
    variants:
      go: {extends: absent}
"#;
    support::parse_checked(yaml);
    let path = dir.path().join("fed.yaml");
    std::fs::write(&path, yaml).unwrap();
    let error = Parser::new().load_config(path).unwrap_err().to_string();
    assert!(error.contains("Template 'absent' not found"), "{error}");
    assert!(
        error.contains("variant 'go' of service 'catalog'"),
        "{error}"
    );
}

const STACK: &str = r#"
entrypoint: '*'
services:
  amber: { process: 'sleep 30' }
  birch: { process: 'sleep 30' }
  optional: { process: 'sleep 30', profiles: [extra] }
"#;

fn cli(yaml: &str, args: &[&str]) -> std::process::Output {
    support::parse_checked(yaml);
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("fed.yaml"), yaml).unwrap();
    Command::new(env!("CARGO_BIN_EXE_fed"))
        .current_dir(dir.path())
        .args(args)
        .output()
        .unwrap()
}

#[test]
fn all_and_wildcard_start_the_filtered_graph() {
    for (yaml, args) in [
        (STACK, vec!["start", "--dry-run"]),
        (STACK, vec!["start", "--all", "--dry-run"]),
        (
            STACK,
            vec!["--profile", "extra", "start", "--all", "--dry-run"],
        ),
    ] {
        let output = cli(yaml, &args);
        let text = String::from_utf8_lossy(&output.stdout);
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        let selected = text.split("Start order:").next().unwrap();
        assert!(
            selected.contains("amber") && selected.contains("birch"),
            "{text}"
        );
        assert_eq!(
            selected.contains("optional"),
            args.contains(&"extra"),
            "{text}"
        );
    }
}

#[test]
fn all_rejects_ambiguous_selection_and_wildcard_foreground() {
    for args in [
        vec!["start", "-i"],
        vec!["start", "--all", "amber"],
        vec!["restart", "--all", "amber"],
    ] {
        let output = cli(STACK, &args);
        assert!(!output.status.success());
        if args == ["start", "-i"] {
            assert!(String::from_utf8_lossy(&output.stderr).contains("Pick one"));
        }
    }
}

#[test]
fn health_timing_alias_and_invalid_settings() {
    for field in ["timeout", "start_period"] {
        let config = load(&format!(
            r#"
services:
  amber:
    process: sleep 30
    healthcheck:
      command: 'true'
      {field}: 2s
      interval: 20ms
      probe_timeout: 10ms
      retries: 2
"#
        ));
        config.validate().unwrap();
        let health = config.services["amber"].healthcheck.as_ref().unwrap();
        assert_eq!(health.get_start_period(), Duration::from_secs(2));
        assert_eq!(health.get_interval(), Duration::from_millis(20));
        assert_eq!(
            health.get_probe_timeout(health.get_start_period()),
            Duration::from_millis(10)
        );
        assert_eq!(health.get_retries(), 2);
    }
    for timing in [
        "timeout: 2s, start_period: 3s",
        "interval: 0ms",
        "probe_timeout: invalid",
        "retries: 0",
    ] {
        let config = load(&format!(
            r#"
services:
  amber:
    process: sleep 30
    healthcheck: {{ command: 'true', {timing} }}
"#
        ));
        assert!(config.validate().is_err(), "{timing}");
    }
}

#[tokio::test]
async fn grouping_nodes_start_dependencies_and_probe_timeouts_are_nonfatal() {
    let config = load(
        r#"
services:
  amber:
    process: sleep 30
    healthcheck:
      command: 'exec sleep 2'
      start_period: 100ms
      probe_timeout: 20ms
      interval: 10ms
  bundle: { depends_on: [amber] }
"#,
    );
    config.validate().unwrap();
    assert_eq!(
        config.services["bundle"].service_type(),
        ServiceType::Oneshot
    );
    let dir = tempfile::tempdir().unwrap();
    let orch = Orchestrator::builder()
        .config(config)
        .work_dir(dir.path().into())
        .run_context(RunContext {
            output_mode: OutputMode::File,
            ..Default::default()
        })
        .build()
        .await
        .unwrap();
    let outcome = orch.start("bundle").await.unwrap();
    let statuses = orch.get_status().await;
    orch.stop_all().await.unwrap();
    assert!(matches!(
        outcome.get("amber"),
        Some(StartHealth::TimedOut { .. })
    ));
    assert_eq!(statuses["bundle"], fed::Status::Completed);
    assert_eq!(statuses["amber"], fed::Status::Running);
}

#[tokio::test]
async fn builder_inherits_cli_variant_choices() {
    let config = load(
        r#"
services:
  amber:
    default_variant: java
    variants:
      java: { process: 'sleep 30' }
      go: { process: 'sleep 40' }
"#,
    );
    let dir = tempfile::tempdir().unwrap();
    let context = RunContext {
        variants: vec!["amber:go".into()],
        ..Default::default()
    };
    let orch = Orchestrator::builder()
        .config(config)
        .work_dir(dir.path().into())
        .run_context(context)
        .dry_run(true)
        .build()
        .await
        .unwrap();
    assert_eq!(
        orch.get_config().services["amber"].variant.as_deref(),
        Some("go")
    );
    assert_eq!(orch.current_run_context().variants, ["amber:go"]);
}

#[test]
fn shipped_stack_example_validates_every_variant() {
    let config = load(include_str!("../examples/whole-stack.yaml"));
    for name in ["java", "go"] {
        let mut selected = config.clone();
        let selection = fed::config::variants::VariantSelection::from_cli(&[name.into()]).unwrap();
        fed::config::variants::resolve_variants(&mut selected, &selection, &[]).unwrap();
        selected.validate().unwrap();
        assert_eq!(selected.services["catalog"].variant.as_deref(), Some(name));
    }
}

#[test]
fn inherited_variants_resolve_their_own_template_references() {
    let mut config = load(
        r#"
templates:
  worker: { process: 'sleep 30' }
  implementations:
    variants:
      go: { extends: worker }
services:
  amber: { extends: implementations }
"#,
    );
    config.validate().unwrap();
    fed::config::variants::resolve_variants(&mut config, &Default::default(), &[]).unwrap();
    assert_eq!(
        config.services["amber"].process.as_deref(),
        Some("sleep 30")
    );
}

#[test]
fn invalid_unselected_variant_timing_is_rejected() {
    let config = load(
        r#"
services:
  amber:
    default_variant: java
    variants:
      java: { process: 'sleep 30' }
      go: { process: 'sleep 30', startup_timeout: not-a-duration }
"#,
    );
    let error = config.validate().unwrap_err().to_string();
    assert!(
        error.contains("amber:go") && error.contains("startup_timeout"),
        "{error}"
    );
}

#[test]
fn invalid_pin_is_rejected_before_persisting() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("fed.yaml"), STACK).unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_fed"))
        .current_dir(dir.path())
        .args(["variant", "set", "amber:go"])
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("has no variants"));
    assert!(!dir.path().join(".fed/variants.yaml").exists());
}

#[tokio::test]
async fn http_preflight_still_rejects_an_existing_listener() {
    use std::io::{Read, Write};
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    listener.set_nonblocking(true).unwrap();
    let done = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let server_done = done.clone();
    let server = std::thread::spawn(move || {
        while !server_done.load(std::sync::atomic::Ordering::SeqCst) {
            if let Ok((mut stream, _)) = listener.accept() {
                stream
                    .set_read_timeout(Some(Duration::from_secs(1)))
                    .unwrap();
                let _ = stream.read(&mut [0u8; 2048]);
                let _ = stream.write_all(
                    b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nOK",
                );
            } else {
                std::thread::sleep(Duration::from_millis(5));
            }
        }
    });
    let config = load(&format!(
        r#"
services:
  amber:
    process: sleep 30
    healthcheck: {{ http_get: 'http://127.0.0.1:{port}/health' }}
"#
    ));
    let dir = tempfile::tempdir().unwrap();
    let orch = Orchestrator::builder()
        .config(config)
        .work_dir(dir.path().into())
        .run_context(RunContext {
            output_mode: OutputMode::File,
            ..Default::default()
        })
        .build()
        .await
        .unwrap();
    let result = orch.start("amber").await;
    orch.stop_all().await.unwrap();
    done.store(true, std::sync::atomic::Ordering::SeqCst);
    server.join().unwrap();
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("already passes before")
    );
}

#[test]
fn empty_variants_and_orphaned_defaults_are_errors() {
    for service in [
        "{variants: {}}",
        "{process: 'sleep 30', default_variant: go}",
    ] {
        let config = load(&format!("services:\n  amber: {service}\n"));
        assert!(config.validate().is_err());
    }
}
