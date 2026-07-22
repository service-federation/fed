mod builder;
mod core;
mod factory;
mod health;
mod lifecycle;
mod monitoring;
mod orphans;
mod ports;
mod registration;
mod run_context;
mod scripts;
pub mod supervisor;

pub use builder::OrchestratorBuilder;
pub use core::*;
pub use lifecycle::ServiceLifecycleCommands;
/// Re-exported for `fed status --json`'s `supervised_by` field
/// (`07-supervisor.md` Design §4) — see the doc comment on the function
/// itself (in the private `monitoring` module) for the union-scope formula.
pub use monitoring::supervised_service_names;
pub use run_context::RunContext;
