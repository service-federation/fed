//! Session-scoped run settings, threaded from `main.rs` through every
//! command and into `OrchestratorBuilder`/`Orchestrator`.

use crate::service::OutputMode;
use clap::ValueEnum;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;

/// Where fetched team-vault values may persist after the current `fed`
/// invocation exits.
///
/// This policy applies only to values fetched from Service Federation Cloud.
/// Locally generated secrets keep using `.fed/secrets.generated.env`, and values
/// supplied by an explicit `env_file` remain owned by that file.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, ValueEnum, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SecretCacheMode {
    /// Read and atomically update the owner-only `.fed/secrets.cache.env`
    /// fallback used for offline starts.
    #[default]
    File,
    /// Resolve vault values for this invocation and its child processes only.
    /// Any existing vault cache is removed and no cache is read or written.
    Memory,
    /// Persist vault values in the operating system credential store. Any
    /// existing plaintext vault cache is removed.
    Keychain,
}

/// Session-scoped run settings: the answers to "what did the user ask for
/// on the command line" that stay constant for the life of one `fed`
/// process invocation, independent of which subcommand is running — AND
/// that a child orchestrator (isolated-script execution) must inherit
/// from its parent verbatim rather than re-derive.
///
/// Every field here is fully round-trippable through
/// [`crate::orchestrator::Orchestrator::apply_run_context`] /
/// [`crate::orchestrator::Orchestrator::current_run_context`]: applying a
/// context and then reading it back must reproduce it exactly. This is a
/// deliberate constraint, not an aspiration — a field that can't be read
/// back losslessly doesn't belong in a struct whose entire purpose is
/// "the thing a child orchestrator inherits from its parent." (`cli.verbose`
/// is a concrete example of a field that does NOT meet this bar and is
/// therefore deliberately left out — tracing is one global subscriber per
/// process, already configured identically for parent and child by the
/// time any orchestrator exists, so there is no "inherit verbosity" to do
/// and no round-trip to invent.)
///
/// Deliberately excludes per-operation flags (`randomize_ports`,
/// `replace_mode`, `dry_run`, `readonly`, `auto_resolve_conflicts`) — those
/// are derived from *which* subcommand is running and can differ between a
/// parent orchestrator and, e.g., a dry-run preview built from the same
/// `RunContext`. `work_dir`/`config` are likewise excluded — they're
/// per-orchestrator identity, not session settings, and a `RunContext`
/// shared across a parent and an isolated-script child must not imply they
/// share a `work_dir` (they don't).
#[derive(Debug, Clone, Default)]
pub struct RunContext {
    /// Skip cloud vault lookups for manual secrets. From `--offline`.
    pub offline: bool,
    /// Persistence policy for values fetched from the team vault.
    pub secret_cache: SecretCacheMode,
    /// Whether stdin is a TTY, for interactive prompts like secret
    /// generation. From `std::io::stdin().is_terminal()`.
    pub is_interactive: bool,
    /// Output mode for process services (file/captured/passthrough).
    /// Derived from the subcommand, but must survive into a child
    /// orchestrator unchanged.
    pub output_mode: OutputMode,
    /// Active profiles for service filtering. From `--profile`. Isolated-
    /// script children must inherit this from their parent so a
    /// profile-gated `depends_on` service survives the child's own
    /// filtering pass — see `apply_run_context`'s doc comment.
    pub profiles: Vec<String>,
    /// Scope the vault query to the manual-secret names the target script
    /// transitively references. `None` fetches every missing manual secret
    /// (the safe default for interactive `fed`/`fed start`/unknown
    /// commands).
    pub required_secret_names: Option<HashSet<String>>,
}
