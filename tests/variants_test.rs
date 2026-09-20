//! Variants: one service name, several interchangeable implementations.
//!
//! The contract these tests pin down: a service that declares `variants:`
//! keeps one name, one set of ports, one healthcheck and one place in the
//! dependency graph, and the implementation behind it is chosen at start
//! time. Everything downstream of resolution must be unable to tell the
//! difference between a resolved variant and an ordinary service.

use fed::Config;
use fed::config::variants::{PersistedVariants, VariantSelection, VariantSource, resolve_variants};
use std::collections::HashMap;

#[path = "support/mod.rs"]
mod support;

/// Two services with overlapping-but-different variant sets, which is what
/// makes a shared preference list (rather than per-service flags) the right
/// control: `catalog` has java/rust, `orders` has java/go. A single
/// `--variant go,rust` has to give each of them the right answer.
const TWO_SERVICES: &str = r#"
services:
  catalog:
    depends_on: [storage]
    healthcheck:
      command: 'true'
    environment:
      SHARED: 'yes'
    variants:
      java:
        gradle_task: ':catalog:run'
      rust:
        process: cargo run
        cwd: services/catalog-rs
        environment:
          RUST_LOG: debug
    default_variant: java
  orders:
    healthcheck:
      command: 'true'
    variants:
      java:
        gradle_task: ':orders:run'
      go:
        process: go run ./cmd/orders
    default_variant: java
  storage:
    process: sleep 300
"#;

fn parse(yaml: &str) -> Config {
    support::parse_checked(yaml)
}

/// Resolve `yaml` with the given `--variant` entries and no persisted file.
fn resolve(yaml: &str, cli: &[&str]) -> fed::error::Result<Config> {
    let mut config = parse(yaml);
    config.validate()?;
    let entries: Vec<String> = cli.iter().map(|s| s.to_string()).collect();
    let tmp = tempfile::tempdir().unwrap();
    let selection = VariantSelection::load(&entries, tmp.path())?;
    resolve_variants(&mut config, &selection, &[])?;
    Ok(config)
}

// ── Parsing ───────────────────────────────────────────────────────────────

#[test]
fn variants_parse_into_partial_services() {
    let config = parse(TWO_SERVICES);
    let catalog = &config.services["catalog"];

    assert_eq!(catalog.variants.len(), 2);
    assert_eq!(catalog.default_variant.as_deref(), Some("java"));
    assert_eq!(
        catalog.variants["java"].gradle_task.as_deref(),
        Some(":catalog:run")
    );
    assert_eq!(
        catalog.variants["rust"].process.as_deref(),
        Some("cargo run")
    );

    // The outer service has no type of its own — that is the whole point.
    assert!(catalog.process.is_none());
    assert!(catalog.gradle_task.is_none());
    // And `variant` is an output of resolution, never parsed from YAML.
    assert_eq!(catalog.variant, None);
}

// ── Validation ────────────────────────────────────────────────────────────

fn validation_error(yaml: &str) -> String {
    let config = parse(yaml);
    config
        .validate()
        .expect_err("config should fail validation")
        .to_string()
}

#[test]
fn outer_service_may_not_declare_its_own_type() {
    let err = validation_error(
        r#"
services:
  catalog:
    process: sleep 300
    variants:
      rust:
        process: cargo run
"#,
    );
    assert!(
        err.contains("both `variants` and its own type-defining field"),
        "unexpected error: {err}"
    );
    assert!(
        err.contains("process"),
        "error should name the field: {err}"
    );
}

#[test]
fn a_variant_must_define_exactly_one_type() {
    let err = validation_error(
        r#"
services:
  catalog:
    variants:
      rust:
        cwd: services/catalog-rs
"#,
    );
    assert!(
        err.contains("Variant 'rust' of service 'catalog' has no type defined"),
        "unexpected error: {err}"
    );

    let err = validation_error(
        r#"
services:
  catalog:
    variants:
      rust:
        process: cargo run
        image: catalog:latest
"#,
    );
    assert!(
        err.contains("Variant 'rust' of service 'catalog' has multiple type-defining fields"),
        "unexpected error: {err}"
    );
}

#[test]
fn default_variant_is_required_with_more_than_one_variant() {
    let err = validation_error(
        r#"
services:
  catalog:
    variants:
      java:
        gradle_task: ':catalog:run'
      rust:
        process: cargo run
"#,
    );
    assert!(
        err.contains("no `default_variant`"),
        "unexpected error: {err}"
    );
    assert!(
        err.contains("java, rust"),
        "error should list the variants: {err}"
    );
}

#[test]
fn a_single_variant_needs_no_default() {
    let config = parse(
        r#"
services:
  catalog:
    variants:
      rust:
        process: cargo run
"#,
    );
    config
        .validate()
        .expect("one variant implies itself as the default");
}

#[test]
fn default_variant_must_name_an_existing_variant() {
    let err = validation_error(
        r#"
services:
  catalog:
    default_variant: go
    variants:
      java:
        gradle_task: ':catalog:run'
      rust:
        process: cargo run
"#,
    );
    assert!(
        err.contains("default_variant: 'go'") && err.contains("java, rust"),
        "unexpected error: {err}"
    );
}

#[test]
fn variants_may_not_nest() {
    let err = validation_error(
        r#"
services:
  catalog:
    variants:
      rust:
        process: cargo run
        variants:
          inner:
            process: sleep 1
"#,
    );
    assert!(err.contains("cannot nest"), "unexpected error: {err}");
}

#[test]
fn a_variant_name_may_not_contain_a_colon() {
    let err = validation_error(
        r#"
services:
  catalog:
    variants:
      'rust:nightly':
        process: cargo run
"#,
    );
    assert!(
        err.contains("cannot contain ':'"),
        "unexpected error: {err}"
    );
}

/// The `Undefined` type error must not fire for a service whose type lives
/// in its variants — that check is the reason the outer service can be
/// type-less at all.
#[test]
fn a_service_with_variants_is_not_reported_as_typeless() {
    let config = parse(TWO_SERVICES);
    config
        .validate()
        .expect("a service with variants has a type; it just lives one level down");
}

// ── Resolution ────────────────────────────────────────────────────────────

#[test]
fn resolution_falls_back_to_default_variant() {
    let config = resolve(TWO_SERVICES, &[]).unwrap();
    assert_eq!(config.services["catalog"].variant.as_deref(), Some("java"));
    assert_eq!(config.services["orders"].variant.as_deref(), Some("java"));
}

/// The case a shared vocabulary exists for: one list, two services with
/// disjoint alternatives, each landing on the name it actually has.
#[test]
fn a_preference_list_serves_services_with_disjoint_variant_sets() {
    let config = resolve(TWO_SERVICES, &["go,rust"]).unwrap();
    assert_eq!(config.services["catalog"].variant.as_deref(), Some("rust"));
    assert_eq!(config.services["orders"].variant.as_deref(), Some("go"));
}

#[test]
fn preference_list_order_decides_between_two_matches() {
    let yaml = r#"
services:
  catalog:
    default_variant: java
    variants:
      java:
        gradle_task: ':catalog:run'
      go:
        process: go run ./cmd/catalog
      rust:
        process: cargo run
"#;
    assert_eq!(
        resolve(yaml, &["rust,go"]).unwrap().services["catalog"]
            .variant
            .as_deref(),
        Some("rust")
    );
    assert_eq!(
        resolve(yaml, &["go,rust"]).unwrap().services["catalog"]
            .variant
            .as_deref(),
        Some("go")
    );
}

#[test]
fn a_service_with_no_matching_preference_keeps_its_default() {
    // `orders` has java/go and is untouched by a rust-only preference.
    let config = resolve(TWO_SERVICES, &["rust"]).unwrap();
    assert_eq!(config.services["catalog"].variant.as_deref(), Some("rust"));
    assert_eq!(config.services["orders"].variant.as_deref(), Some("java"));
}

#[test]
fn a_pin_beats_the_preference_list() {
    let config = resolve(TWO_SERVICES, &["go,rust", "catalog:java"]).unwrap();
    assert_eq!(config.services["catalog"].variant.as_deref(), Some("java"));
    // The list still applies to everything the pin didn't name.
    assert_eq!(config.services["orders"].variant.as_deref(), Some("go"));
}

/// The full precedence ladder in one test: a CLI pin outranks a CLI list,
/// which outranks a file pin, which outranks a file list, which outranks
/// `default_variant`.
#[test]
fn precedence_runs_cli_pin_then_cli_list_then_file_then_default() {
    let yaml = r#"
services:
  catalog:
    default_variant: java
    variants:
      java:
        gradle_task: ':catalog:run'
      go:
        process: go run ./cmd/catalog
      rust:
        process: cargo run
      ts:
        process: npm start
"#;

    let tmp = tempfile::tempdir().unwrap();
    let work_dir = tmp.path();
    PersistedVariants {
        prefer: vec!["ts".to_string()],
        pin: HashMap::from([("catalog".to_string(), "rust".to_string())]),
    }
    .save(work_dir)
    .unwrap();

    let pick = |cli: &[&str]| -> (String, VariantSource) {
        let config = parse(yaml);
        let entries: Vec<String> = cli.iter().map(|s| s.to_string()).collect();
        let selection = VariantSelection::load(&entries, work_dir).unwrap();
        let (variant, source) = selection
            .pick_with_source("catalog", &config.services["catalog"])
            .unwrap();
        (variant.to_string(), source)
    };

    assert_eq!(
        pick(&["catalog:java", "go"]),
        ("java".to_string(), VariantSource::CliPin),
        "a CLI pin outranks everything"
    );
    assert_eq!(
        pick(&["go"]),
        ("go".to_string(), VariantSource::CliPrefer),
        "a CLI preference list outranks a persisted pin"
    );
    assert_eq!(
        pick(&[]),
        ("rust".to_string(), VariantSource::FilePin),
        "a persisted pin outranks the persisted list"
    );

    PersistedVariants {
        prefer: vec!["ts".to_string()],
        pin: HashMap::new(),
    }
    .save(work_dir)
    .unwrap();
    assert_eq!(
        pick(&[]),
        ("ts".to_string(), VariantSource::FilePrefer),
        "the persisted list outranks default_variant"
    );

    PersistedVariants::default().save(work_dir).unwrap();
    assert_eq!(
        pick(&[]),
        ("java".to_string(), VariantSource::Default),
        "default_variant is the floor"
    );
}

#[test]
fn the_merged_service_keeps_the_outer_contract_and_the_variant_body() {
    let config = resolve(TWO_SERVICES, &["rust"]).unwrap();
    let catalog = &config.services["catalog"];

    // From the variant.
    assert_eq!(catalog.process.as_deref(), Some("cargo run"));
    assert_eq!(catalog.cwd.as_deref(), Some("services/catalog-rs"));
    assert_eq!(
        catalog.environment.get("RUST_LOG").map(String::as_str),
        Some("debug")
    );
    // The java variant's own type field must not leak into the rust pick.
    assert_eq!(catalog.gradle_task, None);

    // From the outer service — the shared contract.
    assert!(catalog.healthcheck.is_some());
    assert_eq!(catalog.depends_on.len(), 1);
    assert_eq!(
        catalog.environment.get("SHARED").map(String::as_str),
        Some("yes")
    );

    // And nothing downstream can still see the alternatives.
    assert!(catalog.variants.is_empty());
    assert_eq!(catalog.default_variant, None);
}

#[test]
fn resolution_is_idempotent() {
    let mut config = resolve(TWO_SERVICES, &["rust"]).unwrap();
    let tmp = tempfile::tempdir().unwrap();
    let selection = VariantSelection::load(&[], tmp.path()).unwrap();
    resolve_variants(&mut config, &selection, &[]).unwrap();

    // A second pass must not re-derive the default over the first choice.
    assert_eq!(config.services["catalog"].variant.as_deref(), Some("rust"));
    assert_eq!(
        config.services["catalog"].process.as_deref(),
        Some("cargo run")
    );
}

// ── Selection errors ──────────────────────────────────────────────────────

#[test]
fn pinning_a_service_without_variants_is_an_error() {
    let err = resolve(TWO_SERVICES, &["storage:go"])
        .expect_err("storage has no variants")
        .to_string();
    assert!(
        err.contains("Service 'storage' has no variants."),
        "unexpected error: {err}"
    );
}

#[test]
fn pinning_an_unknown_variant_lists_the_real_ones() {
    let err = resolve(TWO_SERVICES, &["catalog:go"])
        .expect_err("catalog has no go variant")
        .to_string();
    assert!(
        err.contains("Service 'catalog' has no variant 'go'"),
        "unexpected error: {err}"
    );
    assert!(
        err.contains("Available: java, rust"),
        "error should list what is available: {err}"
    );
}

#[test]
fn pinning_an_unknown_service_is_an_error() {
    let err = resolve(TWO_SERVICES, &["nosuch:go"])
        .expect_err("there is no such service")
        .to_string();
    assert!(err.contains("no such service"), "unexpected error: {err}");
}

/// A preference-list name nothing offers is deliberately tolerated: one
/// list is meant to serve several checkouts, most of which know only some
/// of the names in it.
#[test]
fn an_unmatched_preference_name_is_not_an_error() {
    let config = resolve(TWO_SERVICES, &["cobol,rust"]).expect("unknown names are ignored");
    assert_eq!(config.services["catalog"].variant.as_deref(), Some("rust"));
}

#[test]
fn a_malformed_pin_is_rejected() {
    for bad in ["catalog:", ":java"] {
        let err = resolve(TWO_SERVICES, &[bad])
            .expect_err("malformed pin should be rejected")
            .to_string();
        assert!(
            err.contains("A pin looks like 'service:variant'"),
            "unexpected error for {bad:?}: {err}"
        );
    }
}

// ── Persistence ───────────────────────────────────────────────────────────

#[test]
fn persisted_selection_round_trips_and_clears() {
    let tmp = tempfile::tempdir().unwrap();
    let work_dir = tmp.path();

    assert!(
        PersistedVariants::load(work_dir).unwrap().prefer.is_empty(),
        "a missing file reads as an empty selection, not an error"
    );

    PersistedVariants {
        prefer: vec!["go".to_string(), "ts".to_string()],
        pin: HashMap::from([("orders".to_string(), "java".to_string())]),
    }
    .save(work_dir)
    .unwrap();

    let loaded = PersistedVariants::load(work_dir).unwrap();
    assert_eq!(loaded.prefer, vec!["go".to_string(), "ts".to_string()]);
    assert_eq!(loaded.pin.get("orders").map(String::as_str), Some("java"));

    // Clearing removes the file rather than leaving an empty document.
    PersistedVariants::default().save(work_dir).unwrap();
    assert!(!fed::config::variants::variants_file_path(work_dir).exists());
}

// ── Downstream: the graph, the orchestrator, the preview ──────────────────

/// A dependant depends on the *service*, not the implementation, so the
/// edge must survive resolution regardless of which variant won.
#[tokio::test]
async fn dependants_start_after_the_service_whichever_variant_runs() {
    let yaml = r#"
services:
  catalog:
    healthcheck:
      command: 'true'
    variants:
      java:
        process: sleep 300
      rust:
        process: sleep 301
    default_variant: java
  frontend:
    process: sleep 302
    depends_on: [catalog]
"#;

    for (cli, expected) in [(vec![], "java"), (vec!["rust"], "rust")] {
        let config = resolve(yaml, &cli).unwrap();
        assert_eq!(
            config.services["catalog"].variant.as_deref(),
            Some(expected)
        );

        let temp_dir = tempfile::tempdir().unwrap();
        let mut orchestrator =
            support::new_ephemeral_orchestrator_for_test(config, temp_dir.path().to_path_buf())
                .await
                .unwrap();
        orchestrator.set_auto_resolve_conflicts(true);
        orchestrator.initialize_dry_run().await.unwrap();

        let graph = orchestrator.get_dependency_graph();
        assert_eq!(
            graph.get_dependencies("frontend"),
            vec!["catalog".to_string()],
            "the `depends_on` edge names the service, not the variant ({expected})"
        );
    }
}

/// The resolved service has to be indistinguishable from a hand-written one
/// as far as the orchestrator is concerned — including having a real type.
#[tokio::test]
async fn the_orchestrator_sees_an_ordinary_service() {
    let config = resolve(TWO_SERVICES, &["rust"]).unwrap();
    assert_eq!(
        config.services["catalog"].service_type(),
        fed::config::ServiceType::Process
    );

    let temp_dir = tempfile::tempdir().unwrap();
    let mut orchestrator =
        support::new_ephemeral_orchestrator_for_test(config, temp_dir.path().to_path_buf())
            .await
            .unwrap();
    orchestrator.set_auto_resolve_conflicts(true);
    orchestrator
        .initialize()
        .await
        .expect("a resolved variant service initializes like any other");

    let status = orchestrator.get_status().await;
    assert!(
        status.contains_key("catalog"),
        "the resolved service must get a manager like any other"
    );
}

/// Variant resolution runs before profile filtering, so a profile-gated
/// service still gets a variant — and is still filtered out afterwards if
/// its profile is inactive.
#[tokio::test]
async fn a_profile_gated_service_is_resolved_then_filtered_normally() {
    let yaml = r#"
services:
  catalog:
    profiles: [full]
    healthcheck:
      command: 'true'
    variants:
      java:
        process: sleep 300
      rust:
        process: sleep 301
    default_variant: java
  frontend:
    process: sleep 302
"#;
    let config = resolve(yaml, &["rust"]).unwrap();
    assert_eq!(config.services["catalog"].variant.as_deref(), Some("rust"));

    let temp_dir = tempfile::tempdir().unwrap();
    let mut orchestrator =
        support::new_ephemeral_orchestrator_for_test(config, temp_dir.path().to_path_buf())
            .await
            .unwrap();
    orchestrator.set_auto_resolve_conflicts(true);
    orchestrator.initialize().await.unwrap();

    let status = orchestrator.get_status().await;
    assert!(
        !status.contains_key("catalog"),
        "an inactive profile still filters the service out after resolution"
    );
    assert!(status.contains_key("frontend"));
}

/// `extends:` and `variants:` compose: a template can supply the shared
/// contract and the service can supply the implementations.
#[test]
fn a_service_can_inherit_its_contract_from_a_template_and_add_variants() {
    let config = parse(
        r#"
templates:
  service-base:
    healthcheck:
      command: 'true'
    tags: [backend]

services:
  catalog:
    extends: service-base
    default_variant: java
    variants:
      java:
        gradle_task: ':catalog:run'
      rust:
        process: cargo run
"#,
    );
    let mut config = config;
    fed::package::ServiceMerger::merge_local_templates(&mut config).unwrap();
    config.validate().unwrap();

    let tmp = tempfile::tempdir().unwrap();
    let selection = VariantSelection::load(&["rust".to_string()], tmp.path()).unwrap();
    resolve_variants(&mut config, &selection, &[]).unwrap();

    let catalog = &config.services["catalog"];
    assert_eq!(catalog.process.as_deref(), Some("cargo run"));
    assert!(catalog.healthcheck.is_some(), "contract from the template");
    assert_eq!(
        catalog.tags,
        vec!["backend".to_string()],
        "`tags` from a template is the bug that made merge_service completeness a prerequisite"
    );
}

/// `fed validate` must report the resolved service, and its parse must not
/// reject the two new keys.
#[test]
fn fed_validate_accepts_and_reports_variants() {
    let tmp = tempfile::tempdir().unwrap();
    let config_path = tmp.path().join("fed.yaml");
    std::fs::write(&config_path, TWO_SERVICES).unwrap();

    let output = std::process::Command::new(env!("CARGO_BIN_EXE_fed"))
        .arg("--workdir")
        .arg(tmp.path())
        .arg("--config")
        .arg(&config_path)
        .args(["--variant", "rust"])
        .arg("validate")
        .output()
        .expect("fed validate should run");

    let combined = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(output.status.success(), "validate failed: {combined}");
    assert!(
        combined.contains("catalog (process, variant: rust)"),
        "validate should report the resolved service: {combined}"
    );
    assert!(
        combined.contains("orders (gradle, variant: java)"),
        "validate should report the default where nothing matched: {combined}"
    );
}

/// `fed start --dry-run` names the variant it would run, which is the only
/// way to preview what `--variant` actually landed on.
#[test]
fn dry_run_shows_the_chosen_variant() {
    let tmp = tempfile::tempdir().unwrap();
    let config_path = tmp.path().join("fed.yaml");
    std::fs::write(&config_path, TWO_SERVICES).unwrap();

    let output = std::process::Command::new(env!("CARGO_BIN_EXE_fed"))
        .arg("--workdir")
        .arg(tmp.path())
        .arg("--config")
        .arg(&config_path)
        .args(["--variant", "rust"])
        .args(["start", "catalog", "--dry-run"])
        .output()
        .expect("fed start --dry-run should run");

    let combined = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(output.status.success(), "dry-run failed: {combined}");
    assert!(
        combined.contains("variant: rust"),
        "dry-run should name the chosen variant: {combined}"
    );
}

/// `fed variant set`/`list`/`unset`/`clear` round-trip through
/// `.fed/variants.yaml`, and `list` explains where each choice came from.
#[test]
fn fed_variant_subcommands_round_trip() {
    let tmp = tempfile::tempdir().unwrap();
    let config_path = tmp.path().join("fed.yaml");
    std::fs::write(&config_path, TWO_SERVICES).unwrap();

    let fed = |args: &[&str]| -> String {
        let output = std::process::Command::new(env!("CARGO_BIN_EXE_fed"))
            .arg("--workdir")
            .arg(tmp.path())
            .arg("--config")
            .arg(&config_path)
            .args(args)
            .output()
            .expect("fed should run");
        let combined = format!(
            "{}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(output.status.success(), "fed {args:?} failed: {combined}");
        combined
    };

    fed(&["variant", "set", "go,rust"]);
    let listed = fed(&["variant", "list"]);
    assert!(
        listed.contains("catalog") && listed.contains("rust"),
        "list should show the resolved variant: {listed}"
    );
    assert!(
        listed.contains(".fed/variants.yaml"),
        "list should say where the choice came from: {listed}"
    );

    fed(&["variant", "set", "catalog:java"]);
    let listed = fed(&["variant", "list"]);
    assert!(
        listed.contains("catalog") && listed.contains("java"),
        "a pin should win over the stored list: {listed}"
    );
    // Setting a pin must not drop the previously stored preference list.
    assert!(
        listed.contains("orders") && listed.contains("go"),
        "the stored preference list should survive adding a pin: {listed}"
    );

    fed(&["variant", "unset", "catalog"]);
    let listed = fed(&["variant", "list"]);
    assert!(
        listed.contains("rust"),
        "removing the pin restores the list's choice: {listed}"
    );

    fed(&["variant", "clear"]);
    assert!(!fed::config::variants::variants_file_path(tmp.path()).exists());
    let listed = fed(&["variant", "list"]);
    assert!(
        listed.contains("default_variant"),
        "with nothing persisted, every service falls back to its default: {listed}"
    );
}

/// A config with no variants at all must be entirely unaffected — the
/// feature is opt-in and must not change any existing behaviour.
#[test]
fn a_config_without_variants_is_untouched() {
    let config = resolve(
        r#"
services:
  worker:
    process: sleep 300
"#,
        &["go,rust"],
    )
    .unwrap();
    assert_eq!(config.services["worker"].variant, None);
    assert_eq!(
        config.services["worker"].process.as_deref(),
        Some("sleep 300")
    );
}

// ── `extends:` inside a variant ───────────────────────────────────────────

/// A variant is a partial `Service`, so it may extend a local template the
/// same way a service can. This is what collapses the repetition when every
/// Go rewrite in a monorepo shares one shape.
const VARIANT_EXTENDS: &str = r#"
templates:
  go-worker:
    process: go run ./cmd/server
    environment:
      DEMO_MODE: local
      STORAGE_DSN: 'storage://localhost/app'
    tags: [go]

services:
  ledger:
    healthcheck:
      command: 'true'
    default_variant: java
    variants:
      java:
        gradle_task: ':ledger-server:run'
      go:
        extends: go-worker
        cwd: services/ledger/ledger-go
"#;

#[test]
fn a_variant_can_extend_a_local_template() {
    let mut config = support::parse_checked(VARIANT_EXTENDS);
    fed::package::ServiceMerger::merge_local_templates(&mut config).unwrap();
    config.validate().unwrap();

    let go = &config.services["ledger"].variants["go"];
    // From the template.
    assert_eq!(go.process.as_deref(), Some("go run ./cmd/server"));
    assert_eq!(
        go.environment.get("DEMO_MODE").map(String::as_str),
        Some("local")
    );
    assert_eq!(go.tags, vec!["go".to_string()]);
    // From the variant itself.
    assert_eq!(go.cwd.as_deref(), Some("services/ledger/ledger-go"));
    // Resolved — `extends` is cleared, so nothing tries to re-merge it.
    assert_eq!(go.extends, None);
    // The sibling variant is untouched.
    assert_eq!(
        config.services["ledger"].variants["java"]
            .gradle_task
            .as_deref(),
        Some(":ledger-server:run")
    );
}

#[test]
fn a_template_backed_variant_resolves_to_a_startable_service() {
    let mut config = support::parse_checked(VARIANT_EXTENDS);
    fed::package::ServiceMerger::merge_local_templates(&mut config).unwrap();
    config.validate().unwrap();

    let tmp = tempfile::tempdir().unwrap();
    let selection = VariantSelection::load(&["go".to_string()], tmp.path()).unwrap();
    resolve_variants(&mut config, &selection, &[]).unwrap();

    let ledger = &config.services["ledger"];
    assert_eq!(ledger.variant.as_deref(), Some("go"));
    assert_eq!(ledger.process.as_deref(), Some("go run ./cmd/server"));
    assert_eq!(ledger.cwd.as_deref(), Some("services/ledger/ledger-go"));
    // The outer service's contract still wins where it speaks.
    assert!(ledger.healthcheck.is_some());
}

/// The error has to say which variant, or a 29-service config gives you a
/// template name and nothing to ledger for.
#[test]
fn an_unknown_template_in_a_variant_names_the_service_and_variant() {
    let mut config = support::parse_checked(
        r#"
services:
  ledger:
    variants:
      go:
        extends: nosuch-template
"#,
    );
    let err = fed::package::ServiceMerger::merge_local_templates(&mut config)
        .expect_err("the template does not exist")
        .to_string();
    assert!(
        err.contains("Template 'nosuch-template' not found"),
        "unexpected error: {err}"
    );
    assert!(
        err.contains("referenced in variant 'go' of service 'ledger'"),
        "the error must name the variant, not just the service: {err}"
    );
}

/// The pre-existing service-level message must keep its exact wording.
#[test]
fn an_unknown_template_on_a_service_still_says_service() {
    let mut config = support::parse_checked(
        r#"
services:
  ledger:
    extends: nosuch-template
"#,
    );
    let err = fed::package::ServiceMerger::merge_local_templates(&mut config)
        .expect_err("the template does not exist")
        .to_string();
    assert!(
        err.contains("referenced in service 'ledger'"),
        "unexpected error: {err}"
    );
}

/// A variant whose `extends:` never resolved would otherwise fail validation
/// with "has no type defined", sending the reader to the wrong file.
#[test]
fn an_unresolved_extends_in_a_variant_says_so() {
    let config = support::parse_checked(
        r#"
services:
  ledger:
    variants:
      go:
        extends: some-package.base
"#,
    );
    let err = config
        .validate()
        .expect_err("an unresolved extends leaves the variant typeless")
        .to_string();
    assert!(
        err.contains("was never resolved"),
        "unexpected error: {err}"
    );
    assert!(
        err.contains("Variant 'go' of service 'ledger'"),
        "unexpected error: {err}"
    );
}

/// Templates still may not extend templates, wherever the reference came from.
#[test]
fn a_variant_may_not_extend_a_template_that_extends_another() {
    let mut config = support::parse_checked(
        r#"
templates:
  base:
    process: sleep 300
  derived:
    extends: base

services:
  ledger:
    variants:
      go:
        extends: derived
"#,
    );
    let err = fed::package::ServiceMerger::merge_local_templates(&mut config)
        .expect_err("templates cannot chain")
        .to_string();
    assert!(
        err.contains("cannot extend another template"),
        "unexpected error: {err}"
    );
}

#[test]
#[cfg(unix)]
fn terminal_setting_is_inherited_by_process_variants() {
    let config = resolve(
        r#"
services:
  worker:
    tty: true
    variants:
      terminal:
        process: sleep 30
"#,
        &[],
    )
    .unwrap();
    assert!(config.services["worker"].tty);
}

#[test]
fn terminal_constraints_are_checked_on_every_merged_variant() {
    for yaml in [
        r#"
services:
  worker:
    tty: true
    variants:
      terminal:
        image: alpine
"#,
        r#"
services:
  worker:
    tty: true
    variants:
      terminal:
        process: sleep 30
        restart: always
"#,
    ] {
        let config = parse(yaml);
        let error = config.validate().unwrap_err().to_string();
        assert!(error.contains("tty: true"), "{error}");
        assert!(error.contains("worker:terminal"), "{error}");
    }
}
