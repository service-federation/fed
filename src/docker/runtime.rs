//! Resolution of the container-runtime binary fed spawns.
//!
//! Every subprocess fed runs against a container runtime takes its program name
//! from [`binary()`]. Keeping that in one place means a future runtime switch is
//! a change here, not a grep across the tree — and it keeps the error strings
//! fed builds (`"{binary} rm -f"`, …) truthful rather than hard-coding `docker`
//! into text describing a command that may not have been `docker`.
//!
//! Only Docker is supported today, so resolution is deliberately dumb:
//!
//! 1. an explicit override installed via [`init()`] at startup, then
//! 2. the `FED_CONTAINER_RUNTIME` environment variable, then
//! 3. the default, [`DEFAULT_RUNTIME`].
//!
//! The intended *future* precedence is: project-local config → user-global
//! config → environment → auto-detection of an installed runtime. The first two
//! land through [`init()`] — config loading happens after this module can first
//! be touched, so the seam has to be an injected override rather than a call
//! from here into the config layer (which would invert the dependency and make
//! this module un-unit-testable). Auto-detection is intentionally absent: with a
//! single supported runtime a PATH probe has nothing to choose between, and it
//! would cost a subprocess on every cold start.
//!
//! Resolution happens once per process and is then frozen; see [`init()`] for
//! what happens when it is called too late.

use std::fmt;
use std::sync::OnceLock;

/// The runtime fed uses when nothing selects another one.
pub const DEFAULT_RUNTIME: &str = "docker";

/// Environment variable holding a runtime override.
pub const RUNTIME_ENV_VAR: &str = "FED_CONTAINER_RUNTIME";

/// Compose v1's standalone binary.
///
/// Not derived from the resolved runtime: v1 was always a separate Python
/// program named `docker-compose`, never a subcommand of any runtime, so it has
/// no `<runtime>-compose` analogue to substitute into. v2 is `<runtime> compose`
/// and does go through [`binary()`].
pub const COMPOSE_V1_BINARY: &str = "docker-compose";

/// Process-wide resolved runtime. Written at most once, by [`init()`] or by the
/// first [`binary()`] call, and read for the life of the process — a per-client
/// field would be equivalent but `DockerClient` is zero-sized and constructed
/// ad hoc in dozens of places, so there is no instance to thread it through.
static RUNTIME: OnceLock<String> = OnceLock::new();

/// Returned by [`init()`] when the runtime had already been resolved.
///
/// Carries both names so the caller can report the discrepancy; fed itself
/// downgrades this to a debug log rather than failing a command over it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlreadyResolved {
    /// The runtime `init` was asked to install.
    pub requested: String,
    /// The runtime already in force, which stays in force.
    pub resolved: &'static str,
}

impl fmt::Display for AlreadyResolved {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "container runtime already resolved to '{}'; ignoring late override '{}'",
            self.resolved, self.requested
        )
    }
}

impl std::error::Error for AlreadyResolved {}

/// Apply the precedence rules to already-extracted inputs.
///
/// Split out from the statics so precedence is testable without mutating
/// process environment (which races across the test harness's threads).
/// A present-but-blank value counts as unset: an exported-and-emptied
/// `FED_CONTAINER_RUNTIME=` reads as "I turned this off", and spawning a
/// program named "" would fail with an unrecognizable error anyway.
fn resolve_from<'a>(override_value: Option<&'a str>, env_value: Option<&'a str>) -> &'a str {
    [override_value, env_value]
        .into_iter()
        .flatten()
        .map(str::trim)
        .find(|v| !v.is_empty())
        .unwrap_or(DEFAULT_RUNTIME)
}

/// Install an explicit runtime override. Call once, from startup, before any
/// container command runs.
///
/// This is the hook config-driven selection plugs into: resolve the project's
/// and the user's configured runtime, then hand the winner here.
///
/// Returns [`AlreadyResolved`] if the runtime was frozen before this call —
/// deliberately an error rather than a panic, since losing this race changes
/// nothing about correctness for the Docker-only case and must not take down a
/// running command. Re-installing the value already in force is a no-op and
/// succeeds, so a startup path that runs twice (isolated-script children
/// re-entering `main`) does not have to guard the call.
pub fn init(name: &str) -> Result<(), AlreadyResolved> {
    // The environment participates here too: a blank override means "config
    // had nothing to say", and per the precedence rules that falls through to
    // `FED_CONTAINER_RUNTIME`, not straight to the default.
    let from_env = std::env::var(RUNTIME_ENV_VAR).ok();
    let requested = resolve_from(Some(name), from_env.as_deref()).to_string();
    match RUNTIME.set(requested.clone()) {
        Ok(()) => Ok(()),
        Err(_) => {
            let resolved = resolved_str();
            if resolved == requested {
                Ok(())
            } else {
                Err(AlreadyResolved {
                    requested,
                    resolved,
                })
            }
        }
    }
}

/// The container-runtime binary to spawn, e.g. `"docker"`.
///
/// Freezes the resolution on first call, so anything wanting to influence it
/// must have called [`init()`] first.
pub fn binary() -> &'static str {
    if RUNTIME.get().is_none() {
        let from_env = std::env::var(RUNTIME_ENV_VAR).ok();
        let _ = RUNTIME.set(resolve_from(None, from_env.as_deref()).to_string());
    }
    resolved_str()
}

/// The frozen value as a `'static` borrow — sound only because `RUNTIME` is a
/// static, and total only because every caller sets it first.
fn resolved_str() -> &'static str {
    RUNTIME.get().map_or(DEFAULT_RUNTIME, String::as_str)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn override_beats_env_beats_default() {
        assert_eq!(resolve_from(Some("a"), Some("b")), "a");
        assert_eq!(resolve_from(None, Some("b")), "b");
        assert_eq!(resolve_from(None, None), DEFAULT_RUNTIME);
        assert_eq!(resolve_from(Some("a"), None), "a");
    }

    #[test]
    fn blank_values_count_as_unset() {
        assert_eq!(resolve_from(Some(""), Some("b")), "b");
        assert_eq!(resolve_from(Some("   "), Some("b")), "b");
        assert_eq!(resolve_from(Some(""), Some("")), DEFAULT_RUNTIME);
        assert_eq!(resolve_from(None, Some("  ")), DEFAULT_RUNTIME);
    }

    #[test]
    fn values_are_trimmed() {
        // A config file or shell export can easily carry trailing whitespace;
        // spawning " docker" fails in a way nobody would connect to that.
        assert_eq!(resolve_from(Some(" docker "), None), "docker");
        assert_eq!(resolve_from(None, Some("docker\n")), "docker");
    }

    #[test]
    fn init_after_first_use_is_reported_not_panicked() {
        // Force resolution first so this test's outcome does not depend on
        // whether some other test in this binary spawned a container command.
        let resolved = binary();
        assert!(!resolved.is_empty());

        // Re-installing what is already in force is a no-op, not a conflict.
        assert_eq!(init(resolved), Ok(()));

        // Derived from the resolved value so the test cannot collide with
        // whatever FED_CONTAINER_RUNTIME happens to hold in this environment.
        let other = format!("{resolved}-other");
        let err = init(&other).expect_err("late override must be reported");
        assert_eq!(err.resolved, resolved);
        assert_eq!(err.requested, other);
        // The frozen value wins; the loser never takes effect.
        assert_eq!(binary(), resolved);
    }
}
