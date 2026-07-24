//! Regression test for a latent bug found while writing the RunContext
//! consolidation plan (2026-07-21): an isolated-script child orchestrator
//! never inherited `active_profiles` from its parent (`scripts.rs` never
//! called anything equivalent to `.with_profiles(...)` on the child). Since
//! the child is built from the parent's *unfiltered* `original_config`, an
//! empty `active_profiles` on the child silently dropped every
//! profile-gated service from its config — including ones the parent's own
//! `--profile` flag had explicitly activated — so an isolated script
//! depending on a profile-gated service would fail to start it.

use fed::RunContext;

#[tokio::test]
async fn isolated_script_dependency_survives_profile_filtering() {
    let yaml = r#"
services:
  db:
    process: "sleep 30"
    profiles: [with-db]

scripts:
  migrate:
    script: "true"
    isolated: true
    depends_on: [db]
"#;
    let parser = fed::Parser::new();
    let config = parser.parse_config(yaml).expect("parse");
    let temp = tempfile::tempdir().unwrap();

    let ctx = RunContext {
        offline: true,
        secret_cache: fed::SecretCacheMode::File,
        is_interactive: false,
        output_mode: fed::OutputMode::Captured,
        profiles: vec!["with-db".to_string()],
        required_secret_names: None,
    };

    let orchestrator = fed::Orchestrator::builder()
        .config(config)
        .work_dir(temp.path().to_path_buf())
        .run_context(ctx)
        .auto_resolve_conflicts(true)
        .build()
        .await
        .expect("build");

    // Before the fix: this fails because the isolated child's
    // active_profiles defaults to empty, dropping `db` from the child's
    // config before `depends_on: [db]` is ever resolved.
    let status = orchestrator
        .run_script_interactive("migrate", &[])
        .await
        .expect("script should run — db must survive profile filtering in the isolated child");
    assert!(status.success());
}
