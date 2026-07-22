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
