//! Regression test for the bug fixed 2026-07-21: `fed isolate enable
//! --offline` silently ignored the flag because `commands/isolate.rs`
//! constructed `Orchestrator::new` directly plus a hand-rolled
//! `IsolateContext`, instead of going through
//! `OrchestratorBuilder`/`RunContext`/`apply_run_context`.
//!
//! This test targets the *builder path* directly (the fixed, non-buggy way
//! of building the orchestrator that backs `isolate::enable`/`rotate` after
//! the RunContext consolidation) rather than shelling out to the `fed`
//! binary, so it runs fast and fails immediately if a future refactor drops
//! `run_context.offline` from `apply_run_context` or from the builder's call
//! to it.
//!
//! Note: this test originally also covered `run_context.environment`
//! propagation (the `-e`/`--env` axis) alongside `offline`; the environment
//! axis was removed entirely in fed 8.0 (see `08-environments-removal.md`),
//! so only the `offline`-propagation regression remains.

use fed::RunContext;

#[tokio::test]
async fn isolate_enable_inherits_offline() {
    let yaml = r#"
parameters:
  GREETING:
    default: hello

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
        offline: true,
        secret_cache: fed::SecretCacheMode::Memory,
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

    // Offline must have propagated — the pre-fix bypass never called
    // set_offline on its hand-built orchestrator.
    assert!(orchestrator.get_offline());
    assert_eq!(
        orchestrator.get_secret_cache(),
        fed::SecretCacheMode::Memory
    );
}
