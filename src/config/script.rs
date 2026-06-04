//! Script configuration types.
//!
//! This module contains the [`Script`] struct for configuring
//! runnable scripts in the federation config.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Script configuration for custom commands.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Script {
    /// Working directory for the script
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cwd: Option<String>,

    /// Services that must be running before the script can execute
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub depends_on: Vec<String>,

    /// Environment variables for the script
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub environment: HashMap<String, String>,

    /// The script command to execute
    pub script: String,

    /// When true, run the script in complete isolation:
    /// - Allocate fresh random ports for all port-type parameters
    /// - Scope Docker volumes by isolation ID (myvolume → fed-{id}-myvolume)
    /// - Start dependencies in an isolated context
    /// - Clean up all resources after the script completes
    #[serde(default)]
    pub isolated: bool,

    /// Timeout for non-interactive script execution (e.g., "5m", "30s", "600").
    /// Defaults to 5 minutes if not set. Only applies to `run_script()` (captured output),
    /// not to interactive execution.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timeout: Option<String>,

    /// When true, services this script starts are left running after it
    /// finishes instead of being stopped ("borrow-or-own" cleanup is skipped).
    ///
    /// This opts the script out of ownership: it behaves like the services were
    /// pre-started with `fed start`, so they persist until `fed stop`. Useful
    /// for scenario/seed scripts that set up state for manual testing in the
    /// browser, where the stack must stay up after the script returns.
    ///
    /// Applies only to the script you invoke directly. When this script is
    /// pulled in as another script's dependency, the outermost run owns
    /// cleanup and its setting governs.
    #[serde(default)]
    pub keep_services: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keep_services_defaults_to_false() {
        let script: Script = serde_yaml::from_str("script: echo hi").unwrap();
        assert!(
            !script.keep_services,
            "keep_services must default to false (own-and-stop)"
        );
    }

    #[test]
    fn keep_services_parses_true() {
        let script: Script = serde_yaml::from_str("script: echo hi\nkeep_services: true").unwrap();
        assert!(script.keep_services);
    }

    #[test]
    fn default_script_does_not_keep_services() {
        assert!(!Script::default().keep_services);
    }
}
