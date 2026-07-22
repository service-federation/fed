//! Shared helper for integration tests.
//!
//! Lives in the test target, not the library, so it can call `fed`'s still-
//! `pub` `Orchestrator::new`/`new_ephemeral` without adding a permanently
//! public "for tests" escape hatch to the library's own API surface.
//! `clippy::disallowed_methods` (see `clippy.toml`) blocks direct calls to
//! these constructors from integration tests — route through the wrappers
//! below instead.
#![allow(dead_code)]

#[allow(clippy::disallowed_methods)]
pub async fn new_orchestrator_for_test(
    config: fed::config::Config,
    work_dir: std::path::PathBuf,
) -> fed::error::Result<fed::Orchestrator> {
    fed::Orchestrator::new(config, work_dir).await
}

#[allow(clippy::disallowed_methods)]
pub async fn new_ephemeral_orchestrator_for_test(
    config: fed::config::Config,
    work_dir: std::path::PathBuf,
) -> fed::error::Result<fed::Orchestrator> {
    fed::Orchestrator::new_ephemeral(config, work_dir).await
}

/// Parse `yaml` as a fed config and panic with the exact "did you mean?"
/// message a real user would see if it contains any unrecognized key.
///
/// Use this instead of a bare `Parser::parse_config(..).unwrap()` whenever a
/// test fixture is meant to exercise a real config field — it turns a
/// silently-ignored typo (or a made-up/removed field) into an immediate,
/// loud test failure instead of a test that passes for the wrong reason
/// (see `tests/config_key_audit_test.rs` for the standing audit this backs).
pub fn parse_checked(yaml: &str) -> fed::Config {
    let config = fed::config::Parser::new()
        .parse_config(yaml)
        .expect("fixture is not valid YAML");
    let warnings = config.unknown_key_warnings();
    assert!(
        warnings.is_empty(),
        "test fixture has {} unknown config key(s) — the test is not \
         exercising what it thinks it is:\n{}",
        warnings.len(),
        warnings
            .iter()
            .map(|w| format!(
                "  {}: unknown field '{}' (known: {:?})",
                w.location, w.key, w.candidates
            ))
            .collect::<Vec<_>>()
            .join("\n")
    );
    config
}
