//! Script execution commands for the orchestrator.
//!
//! This module contains the `ScriptRunner` struct which encapsulates all script
//! execution logic that was previously part of the main Orchestrator. Extracting
//! these operations improves separation of concerns and keeps the orchestrator
//! core focused on service coordination.

use crate::config::Script;
use crate::error::{Error, Result};
use std::path::Path;
use std::time::Duration;

use super::core::Orchestrator;

/// Short-lived runner for script execution.
///
/// Constructed on-demand from an `Orchestrator` reference. Script methods on
/// `Orchestrator` delegate here after constructing a `ScriptRunner`.
pub(super) struct ScriptRunner<'a> {
    orchestrator: &'a Orchestrator,
}

impl<'a> ScriptRunner<'a> {
    pub fn new(orchestrator: &'a Orchestrator) -> Self {
        Self { orchestrator }
    }

    /// Run a script non-interactively, capturing output.
    ///
    /// Resolves dependencies (starting services or running dependent scripts),
    /// then executes the script with a 5-minute timeout.
    pub async fn run_script(&self, script_name: &str) -> Result<std::process::Output> {
        let script = self
            .orchestrator
            .config
            .scripts
            .get(script_name)
            .ok_or_else(|| Error::ScriptNotFound(script_name.to_string()))?
            .clone();

        // Resolve dependencies - can be services or other scripts (SF-00035)
        // Note: We handle script deps by running them via run_script_interactive
        // since run_script can't easily be made recursive without boxing
        for dep in &script.depends_on {
            if self.orchestrator.config.scripts.contains_key(dep) {
                // Dependency is a script - run it via interactive runner
                // (which handles recursion via Box::pin)
                self.orchestrator.run_script_interactive(dep, &[]).await?;
            } else {
                // Dependency is a service - start it if not running
                if !self.orchestrator.is_service_running(dep).await {
                    self.orchestrator.start(dep).await?;
                }
            }
        }

        // Resolve script at execution time (scripts are not pre-resolved to support isolated)
        let params = self.orchestrator.resolver.get_resolved_parameters().clone();
        let resolved_env = self
            .orchestrator
            .resolver
            .resolve_environment(&script.environment, &params)
            .map_err(|e| {
                Error::TemplateResolution(format!(
                    "Failed to resolve environment for script '{}': {}",
                    script_name, e
                ))
            })?;

        let resolved_script_cmd = self
            .orchestrator
            .resolver
            .resolve_template_shell_safe(&script.script, &params)
            .map_err(|e| {
                Error::TemplateResolution(format!(
                    "Failed to resolve script '{}': {}",
                    script_name, e
                ))
            })?;

        // Determine working directory
        let work_dir = if let Some(ref cwd) = script.cwd {
            std::path::Path::new(cwd).to_path_buf()
        } else {
            self.orchestrator.work_dir.to_path_buf()
        };

        // Build command - handle both single commands and shell scripts
        let mut command = if cfg!(target_os = "windows") {
            let mut cmd = tokio::process::Command::new("cmd");
            cmd.args(["/C", &resolved_script_cmd]);
            cmd
        } else {
            let mut cmd = tokio::process::Command::new("sh");
            cmd.args(["-ec", &resolved_script_cmd]);
            cmd
        };

        command.current_dir(&work_dir);
        command.envs(&resolved_env);

        // Execute with timeout - configurable, default 5 minutes to prevent hanging
        let timeout = script
            .timeout
            .as_deref()
            .and_then(crate::config::parse_duration_string)
            .unwrap_or(std::time::Duration::from_secs(300));
        let output_result = tokio::time::timeout(timeout, command.output()).await;

        let output = match output_result {
            Ok(Ok(output)) => output,
            Ok(Err(e)) => {
                return Err(Error::Process(format!(
                    "Failed to execute script '{}': {}",
                    script_name, e
                )));
            }
            Err(_) => {
                return Err(Error::Process(format!(
                    "Script '{}' exceeded timeout of {} seconds",
                    script_name,
                    timeout.as_secs()
                )));
            }
        };

        Ok(output)
    }

    /// Run a script interactively with stdin/stdout/stderr passthrough.
    /// This is suitable for interactive TUIs like jest --watch.
    /// Returns only the exit status since output goes directly to terminal.
    ///
    /// Extra arguments are appended to the script command with proper shell escaping.
    /// Example: `fed test -- -t "specific test"` passes `-t "specific test"` to the script.
    ///
    /// If the script has `isolated: true`, it runs in an isolated context
    /// with fresh port allocations and isolated service instances.
    pub async fn run_script_interactive(
        &self,
        script_name: &str,
        extra_args: &[String],
    ) -> Result<std::process::ExitStatus> {
        let script = self
            .orchestrator
            .config
            .scripts
            .get(script_name)
            .ok_or_else(|| Error::ScriptNotFound(script_name.to_string()))?
            .clone();

        if script.isolated {
            // Execute in isolated context with fresh port allocations
            // Delegated back to Orchestrator since it needs to create a child Orchestrator
            self.run_script_isolated(script_name, &script, extra_args)
                .await
        } else {
            // Execute in shared context (existing behavior)
            self.run_script_shared(script_name, &script, extra_args)
                .await
        }
    }

    /// Run a script in shared context (reuses current ports and services).
    async fn run_script_shared(
        &self,
        script_name: &str,
        script: &Script,
        extra_args: &[String],
    ) -> Result<std::process::ExitStatus> {
        // Resolve dependencies - can be services or other scripts (SF-00035)
        for dep in &script.depends_on {
            if self.orchestrator.config.scripts.contains_key(dep) {
                // Dependency is a script - run it recursively
                // Note: Circular dependencies are caught by graph validation during config parsing
                // Use Box::pin to enable async recursion (avoids infinite future size)
                Box::pin(self.orchestrator.run_script_interactive(dep, &[])).await?;
            } else {
                // Dependency is a service - start it if not running
                if !self.orchestrator.is_service_running(dep).await {
                    self.orchestrator.start(dep).await?;
                }
            }
        }

        // Resolve script at execution time (scripts are not pre-resolved to support isolated)
        let params = self.orchestrator.resolver.get_resolved_parameters().clone();
        let resolved_env = self
            .orchestrator
            .resolver
            .resolve_environment(&script.environment, &params)
            .map_err(|e| {
                Error::TemplateResolution(format!(
                    "Failed to resolve environment for script '{}': {}",
                    script_name, e
                ))
            })?;

        let resolved_script = self
            .orchestrator
            .resolver
            .resolve_template_shell_safe(&script.script, &params)
            .map_err(|e| {
                Error::TemplateResolution(format!(
                    "Failed to resolve script '{}': {}",
                    script_name, e
                ))
            })?;

        // Create resolved script for execution
        let resolved = Script {
            cwd: script.cwd.clone(),
            depends_on: vec![], // Already resolved above
            environment: resolved_env,
            script: resolved_script,
            isolated: false,
            timeout: None,
        };

        // Execute the script command
        execute_script_command(&resolved, extra_args, &self.orchestrator.work_dir).await
    }

    /// Run a script in isolated context with fresh port allocations (SF-00034).
    ///
    /// Creates a child orchestrator that:
    /// 1. Allocates fresh random ports for all port-type parameters
    /// 2. Starts dependencies with the new ports
    /// 3. Runs the script in isolation
    /// 4. Cleans up all services after completion
    async fn run_script_isolated(
        &self,
        script_name: &str,
        _script: &Script, // Unused - we get the script from original_config
        extra_args: &[String],
    ) -> Result<std::process::ExitStatus> {
        tracing::info!(
            "Running script '{}' in isolated context (isolated: true)",
            script_name
        );

        // Use the ORIGINAL unresolved config so the child can re-resolve templates
        // with fresh port allocations. If original_config is None (shouldn't happen),
        // fall back to cloning the resolved config.
        let child_config = self
            .orchestrator
            .original_config
            .as_ref()
            .cloned()
            .unwrap_or_else(|| self.orchestrator.config.clone());

        // Get the script from the original config (has unresolved templates)
        let original_script = child_config
            .scripts
            .get(script_name)
            .ok_or_else(|| Error::ScriptNotFound(script_name.to_string()))?
            .clone();

        let mut child_orchestrator =
            Orchestrator::new_ephemeral(child_config, self.orchestrator.work_dir.clone()).await?;
        child_orchestrator.output_mode = self.orchestrator.output_mode;

        // Give the child its own container namespace so Docker containers don't
        // collide with (or kill) the parent's running containers.
        let isolation_id = format!("iso-{:08x}", rand::random::<u32>());
        child_orchestrator.set_isolation_id(isolation_id.clone());

        // Enable randomize mode for port isolation — NoopPortStore forces fresh allocation
        child_orchestrator.set_randomize_ports(true);

        // Install/migrate markers are scoped by `(work_dir, isolation_id)`, so the
        // child's isolation_id gives it a fresh, empty marker namespace automatically.
        // No clearing is needed: the parent's shared-scope markers stay intact, and
        // `run_migrate_if_needed` against the child sees an empty scope and correctly
        // runs migrations against the fresh isolated containers.

        // Initialize child orchestrator (allocates fresh ports due to isolated mode)
        child_orchestrator.initialize().await?;

        // Run script dependencies in the child context
        // Script-to-script dependencies run in parent context (they don't need isolation
        // unless they also have isolated)
        let mut service_deps = Vec::new();
        for dep in &original_script.depends_on {
            if self.orchestrator.config.scripts.contains_key(dep) {
                // Script dependency: run in parent context
                // (nested scripts get their own isolation if they have isolated)
                Box::pin(self.orchestrator.run_script_interactive(dep, &[])).await?;
            } else {
                // Service dependency: start in child context with isolated ports
                child_orchestrator.start(dep).await?;
                service_deps.push(dep.clone());
            }
        }

        // Wait for service dependencies to become healthy
        // This is important for isolated scripts that need DB connections etc.
        for dep in &service_deps {
            child_orchestrator
                .wait_for_healthy(dep, Duration::from_secs(60))
                .await?;
        }

        // Get the child's resolved parameters for the script environment
        let child_params = child_orchestrator
            .resolver
            .get_resolved_parameters()
            .clone();

        // Re-resolve the script's environment and command with child parameters
        // Using original_script which has unresolved templates like {{DB_PORT}}
        let resolved_env = child_orchestrator
            .resolver
            .resolve_environment(&original_script.environment, &child_params)
            .map_err(|e| {
                Error::TemplateResolution(format!(
                    "Failed to resolve environment for script '{}': {}",
                    script_name, e
                ))
            })?;

        let resolved_script_cmd = child_orchestrator
            .resolver
            .resolve_template_shell_safe(&original_script.script, &child_params)
            .map_err(|e| {
                Error::TemplateResolution(format!(
                    "Failed to resolve script '{}': {}",
                    script_name, e
                ))
            })?;

        // Create a modified script with resolved environment and command
        let isolated_script = Script {
            cwd: original_script.cwd.clone(),
            depends_on: vec![], // Already resolved
            environment: resolved_env,
            script: resolved_script_cmd,
            isolated: false, // Don't recurse
            timeout: None,
        };

        // Execute script in child context
        let result =
            execute_script_command(&isolated_script, extra_args, &self.orchestrator.work_dir).await;

        // Cleanup child orchestrator (stops all services started in isolation)
        tracing::debug!("Cleaning up isolated context for script '{}'", script_name);
        child_orchestrator.cleanup().await;

        // Remove the ephemeral marker namespace we created under this
        // isolation_id. Each isolated run gets a fresh random id, so without
        // this cleanup `~/.fed/isolated/` would accumulate one orphan
        // directory per run.
        let child_markers = crate::markers::LifecycleMarkers::new(
            self.orchestrator.work_dir.to_path_buf(),
            Some(isolation_id),
        );
        let _ = child_markers.clear_all_installed();
        let _ = child_markers.clear_all_migrated();

        // Log which ports were used (helpful for debugging)
        if !child_params.is_empty() {
            let port_params: Vec<_> = child_params
                .iter()
                .filter(|(k, _)| k.to_uppercase().contains("PORT"))
                .collect();
            if !port_params.is_empty() {
                tracing::debug!("Isolated ports released: {:?}", port_params);
            }
        }

        result
    }

    /// Get list of available scripts.
    pub fn list_scripts(&self) -> Vec<String> {
        self.orchestrator.config.scripts.keys().cloned().collect()
    }
}

/// Execute a script command with the given environment.
/// This is the common script execution logic shared by both shared and isolated contexts.
pub(super) async fn execute_script_command(
    script: &Script,
    extra_args: &[String],
    work_dir: &Path,
) -> Result<std::process::ExitStatus> {
    use crate::parameter::shell_escape;
    use std::process::Stdio;

    // Determine working directory
    let script_work_dir = if let Some(ref cwd) = script.cwd {
        std::path::Path::new(cwd).to_path_buf()
    } else {
        work_dir.to_path_buf()
    };

    // Check if script uses positional parameters ($@, $*, $1, $2, etc.)
    // If not, we'll auto-append arguments to the command for convenience
    let uses_positional_params = script_uses_positional_params(&script.script);

    // Build command with inherited stdio for full TTY passthrough
    let mut command = if cfg!(target_os = "windows") {
        let mut cmd = tokio::process::Command::new("cmd");
        if extra_args.is_empty() {
            cmd.args(["/C", &script.script]);
        } else {
            // Windows: append args to the command
            let escaped_args: Vec<String> =
                extra_args.iter().map(|arg| shell_escape(arg)).collect();
            let trimmed_script = script.script.trim_end();
            let full_script = format!("{} {}", trimmed_script, escaped_args.join(" "));
            cmd.args(["/C", &full_script]);
        }
        cmd
    } else {
        let mut cmd = tokio::process::Command::new("sh");
        cmd.arg("-ec");

        if extra_args.is_empty() {
            // No arguments - just run the script
            cmd.arg(&script.script);
        } else if uses_positional_params {
            // Script uses $@, $1, etc. - pass args as positional parameters
            // Use: sh -c 'script' script_name arg1 arg2
            // This makes $@, $1, $2, etc. work properly in the script
            // The first arg after the script becomes $0 (shown in error messages)
            cmd.arg(&script.script);
            cmd.arg("script"); // $0 - appears in error messages
            cmd.args(extra_args); // $1, $2, ... and $@
        } else {
            // Script doesn't use positional params - auto-append args to command
            // This allows `script: prisma` to work with `fed prisma generate`
            let escaped_args: Vec<String> =
                extra_args.iter().map(|arg| shell_escape(arg)).collect();
            let trimmed_script = script.script.trim_end();
            let full_script = format!("{} {}", trimmed_script, escaped_args.join(" "));
            cmd.arg(&full_script);
        }
        cmd
    };

    command.current_dir(&script_work_dir);
    command.envs(&script.environment);

    // Inherit stdio for interactive use - enables TUI, colors, and user input
    command.stdin(Stdio::inherit());
    command.stdout(Stdio::inherit());
    command.stderr(Stdio::inherit());

    // Spawn and wait for completion (no timeout for interactive scripts)
    let mut child = command
        .spawn()
        .map_err(|e| Error::Process(format!("Failed to spawn script: {}", e)))?;

    let status = child
        .wait()
        .await
        .map_err(|e| Error::Process(format!("Failed to wait for script: {}", e)))?;

    Ok(status)
}

/// Check if a script uses positional parameters ($@, $*, $1, $2, etc.)
/// Used to decide whether to pass args via positional params or auto-append.
pub(super) fn script_uses_positional_params(script: &str) -> bool {
    // Match common positional parameter patterns:
    // $@ $* $1 $2 ... $9 ${1} ${@} ${*} ${10} etc.
    // Also match "$@" "$*" etc. (quoted forms)
    let patterns = [
        "$@", "$*", "$1", "$2", "$3", "$4", "$5", "$6", "$7", "$8", "$9", "${@}", "${*}",
    ];

    for pattern in patterns {
        if script.contains(pattern) {
            return true;
        }
    }

    // Check for ${N} where N is a number (handles $10, $11, etc.)
    // Simple check: look for ${ followed by a digit
    let bytes = script.as_bytes();
    for i in 0..bytes.len().saturating_sub(2) {
        if bytes[i] == b'$' && bytes[i + 1] == b'{' {
            if let Some(&next) = bytes.get(i + 2) {
                if next.is_ascii_digit() {
                    return true;
                }
            }
        }
    }

    false
}

#[cfg(test)]
mod tests {
    use super::*;

    // ========================================================================
    // script_uses_positional_params tests
    // ========================================================================

    #[test]
    fn positional_dollar_one() {
        assert!(script_uses_positional_params("echo $1"));
    }

    #[test]
    fn positional_dollar_two() {
        assert!(script_uses_positional_params("echo $2"));
    }

    #[test]
    fn positional_dollar_nine() {
        assert!(script_uses_positional_params("echo $9"));
    }

    #[test]
    fn positional_dollar_at() {
        assert!(script_uses_positional_params("echo $@"));
    }

    #[test]
    fn positional_dollar_star() {
        assert!(script_uses_positional_params("echo $*"));
    }

    #[test]
    fn positional_braced_digit() {
        assert!(script_uses_positional_params("echo ${2}"));
    }

    #[test]
    fn positional_braced_two_digit() {
        // ${10} should match because ${ followed by digit
        assert!(script_uses_positional_params("echo ${10}"));
    }

    #[test]
    fn positional_braced_at() {
        assert!(script_uses_positional_params("echo ${@}"));
    }

    #[test]
    fn positional_braced_star() {
        assert!(script_uses_positional_params("echo ${*}"));
    }

    #[test]
    fn positional_in_quoted_string() {
        // "$@" still contains $@ as a substring
        assert!(script_uses_positional_params(r#"echo "$@""#));
    }

    #[test]
    fn no_params_plain_command() {
        assert!(!script_uses_positional_params("echo hello"));
    }

    #[test]
    fn no_params_empty_string() {
        assert!(!script_uses_positional_params(""));
    }

    #[test]
    fn dollar_zero_is_not_positional() {
        // $0 is the script name, not a positional parameter
        assert!(!script_uses_positional_params("echo $0"));
    }

    #[test]
    fn named_variable_not_positional() {
        // $HOME is a named env var, not positional
        assert!(!script_uses_positional_params("echo $HOME"));
    }

    #[test]
    fn braced_named_variable_not_positional() {
        // ${HOME} starts with ${ followed by a letter, not a digit
        assert!(!script_uses_positional_params("echo ${HOME}"));
    }

    #[test]
    fn dollar_dollar_one_matches() {
        // $$1 contains $1 as a substring, so the function returns true.
        // This is technically $$ (current PID) followed by literal "1",
        // but our simple substring check matches it.
        assert!(script_uses_positional_params("echo $$1"));
    }

    #[test]
    fn positional_mid_script() {
        assert!(script_uses_positional_params("npm test -- $1 --verbose"));
    }

    #[test]
    fn multiple_positional_params() {
        assert!(script_uses_positional_params("cmd $1 $2 $3"));
    }

    #[test]
    fn dollar_hash_not_positional() {
        // $# is the argument count, not a positional param
        assert!(!script_uses_positional_params("echo $#"));
    }

    #[test]
    fn dollar_question_not_positional() {
        // $? is the exit status, not positional
        assert!(!script_uses_positional_params("echo $?"));
    }
}
