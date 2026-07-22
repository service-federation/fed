//! Regression test for the bug fixed 2026-07-21: `fed isolate enable -e
//! staging --offline` silently ignored both flags because
//! `commands/isolate.rs` constructed `Orchestrator::new` directly plus a
//! hand-rolled `IsolateContext`, instead of going through
//! `OrchestratorBuilder`/`RunContext`/`apply_run_context`.
//!
//! This test targets the *builder path* directly (the fixed, non-buggy way
//! of building the orchestrator that backs `isolate::enable`/`rotate` after
//! the RunContext consolidation) rather than shelling out to the `fed`
//! binary, so it runs fast and fails immediately if a future refactor drops
//! `run_context.environment`/`run_context.offline` from `apply_run_context`
//! or from the builder's call to it.

use fed::RunContext;
use fed::config::Environment;

#[tokio::test]
async fn isolate_enable_inherits_environment_and_offline() {
    let yaml = r#"
parameters:
  GREETING:
    development: from-development
    staging: from-staging

services:
  db:
    process: "true"
    environment:
      GREETING: '{{GREETING}}'
"#;
    let parser = fed::Parser::new();
    let config = parser.parse_config(yaml).expect("parse");
    let temp = tempfile::tempdir().unwrap();

    let ctx = RunContext {
        environment: Environment::Staging,
        offline: true,
        is_interactive: false,
        output_mode: fed::OutputMode::Captured,
        profiles: vec![],
        required_secret_names: None,
    };

    let orchestrator = fed::Orchestrator::builder()
        .config(config)
        .work_dir(temp.path().to_path_buf())
        .run_context(ctx)
        .randomize_ports(true)
        .build()
        .await
        .expect("build");

    // Environment must have propagated: staging must be the resolver's
    // active environment, not the default (development) that `isolate.rs`'s
    // pre-fix bypass silently fell back to.
    assert_eq!(orchestrator.get_environment(), Environment::Staging);
    // Offline must have propagated too — the pre-fix bypass never called
    // set_offline on its hand-built orchestrator.
    assert!(orchestrator.get_offline());
}
