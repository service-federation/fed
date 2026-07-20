// Allow unused_assignments at module level because thiserror's generated code
// for struct variants triggers false positive warnings - the fields ARE used
// in the Display impl but rustc's lint pass doesn't see this.
#![allow(unused_assignments)]

use crate::docker::DockerError;
use miette::Diagnostic;
use std::io;
use thiserror::Error;

#[derive(Error, Diagnostic, Debug)]
pub enum Error {
    #[error("Configuration error: {0}")]
    Config(String),

    #[error("Docker error: {0}")]
    #[diagnostic(code(fed::docker::error))]
    Docker(#[from] DockerError),

    #[error("Process error: {0}")]
    #[diagnostic(
        code(fed::process::error),
        help("Check that the command exists and is executable")
    )]
    Process(String),

    #[error("Filesystem error: {0}")]
    #[diagnostic(code(fed::filesystem::error))]
    Filesystem(String),

    #[error("Parse error: {0}")]
    Parse(String),

    #[error("YAML error: {0}")]
    Yaml(#[from] serde_yaml::Error),

    #[error("IO error: {0}")]
    Io(#[from] io::Error),

    #[error("Service not found: {0}")]
    #[diagnostic(
        code(fed::service::not_found),
        help(
            "Check available services with `fed status` or validate your service-federation.yaml"
        )
    )]
    ServiceNotFound(String),

    #[error("Parameter not found: {0}")]
    #[diagnostic(
        code(fed::parameter::not_found),
        help(
            "Declare the parameter in your service-federation.yaml:\n\nparameters:\n  {0}:\n    default: \"value\""
        )
    )]
    ParameterNotFound(String),

    #[error("Circular dependency detected: {}", .0.join(" -> "))]
    #[diagnostic(
        code(fed::dependency::circular),
        help("Services cannot depend on each other in a cycle. Review the depends_on fields")
    )]
    CircularDependency(Vec<String>),

    #[error("Invalid parameter value for '{name}': {reason}")]
    #[diagnostic(
        code(fed::parameter::invalid),
        help("Check the parameter type and constraints in your service-federation.yaml")
    )]
    InvalidParameter { name: String, reason: String },

    #[error("Service '{0}' is not running")]
    #[diagnostic(
        code(fed::service::not_running),
        help("Start the service with: fed start {0}")
    )]
    ServiceNotRunning(String),

    #[error("Service '{0}' failed to start: {1}")]
    #[diagnostic(
        code(fed::service::start_failed),
        help(
            "Check the service logs with `fed logs {0}`\nVerify the command exists and is executable"
        )
    )]
    ServiceStartFailed(String, String),

    #[error("Service '{0}' health check failed: {1}")]
    #[diagnostic(
        code(fed::service::health_check_failed),
        help(
            "Check the service logs with `fed logs {0}`\nVerify the healthcheck URL/command is correct in your config"
        )
    )]
    HealthCheckFailed(String, String),

    #[error("Port allocation failed: {0}")]
    #[diagnostic(
        code(fed::port::allocation_failed),
        help("Try using 'type: port' parameters which auto-fallback to available ports")
    )]
    PortAllocation(String),

    #[error("{service} – port {port} already allocated")]
    #[diagnostic(
        code(fed::docker::port_conflict),
        help(
            "If another checkout of this project owns the port (worktree, parallel agent):\n\n    fed isolate enable      # give this directory its own ports\n\nIf a stray external process holds it:\n\n    fed start --replace     # kills whatever holds the port — including\n                            # other checkouts' fed services"
        )
    )]
    DockerPortConflict { service: String, port: u16 },

    #[error("{service} – port {port} is held by another fed stack ({container})")]
    #[diagnostic(
        code(fed::docker::port_conflict_other_stack),
        help(
            "Container '{container}' belongs to another checkout or worktree of this project.\nGive this directory its own ports instead (persisted, applies to every later fed command):\n\n    fed isolate enable\n\nDo not use `fed start --replace` here — it would kill the other checkout's services."
        )
    )]
    DockerPortConflictOtherStack {
        service: String,
        port: u16,
        container: String,
    },

    #[error("Operation aborted by user")]
    Aborted,

    #[error("Git error: {0}")]
    Git(#[from] git2::Error),

    #[error("HTTP error: {0}")]
    Http(#[from] reqwest::Error),

    #[error("Template resolution error: {0}")]
    TemplateResolution(String),

    #[error("Timeout waiting for service '{0}'")]
    #[diagnostic(
        code(fed::service::timeout),
        help(
            "The service may be slow to start. Check logs with `fed logs {0}` or increase timeout"
        )
    )]
    Timeout(String),

    #[error("Operation cancelled for service '{0}'")]
    Cancelled(String),

    #[error("Multiple errors occurred:\n{}", .0.iter().map(|e| format!("  - {}", e)).collect::<Vec<_>>().join("\n"))]
    Multiple(Vec<Error>),

    // No help/suggestion: validation messages already describe the offending
    // field — telling the user to run `fed validate` again would be circular.
    #[error("Invalid configuration: {0}")]
    #[diagnostic(code(fed::config::validation))]
    Validation(String),

    #[error("Package error: {0}")]
    Package(String),

    #[error("Package not found: {0}")]
    PackageNotFound(String),

    #[error("Invalid package source: {0}")]
    InvalidPackageSource(String),

    #[error("Package authentication failed: {0}")]
    PackageAuthFailed(String),

    #[error("Package version not found: {0}")]
    PackageVersionNotFound(String),

    #[error("Package cache error: {0}")]
    PackageCache(String),

    #[error("Package merge conflict: {0}")]
    PackageMergeConflict(String),

    #[error("Circular package dependency detected")]
    CircularPackageDependency,

    #[error("Diamond dependency detected for package '{identity}':\n{conflicts_formatted}")]
    #[diagnostic(
        code(fed::package::diamond_dependency),
        help(
            "Pin all packages to use the same version of '{identity}', or restructure your dependencies to avoid the conflict"
        )
    )]
    DiamondDependency {
        identity: String,
        conflicts_formatted: String,
    },

    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("Database error: {0}")]
    #[diagnostic(
        code(fed::database::error),
        help("See recovery suggestions based on the specific error type")
    )]
    Database(#[from] tokio_rusqlite::Error),

    #[error("Invalid PID {pid}: {reason}")]
    InvalidPid { pid: u32, reason: String },

    #[error("Docker Compose error: {0}")]
    #[diagnostic(
        code(fed::docker::compose),
        help("Verify Docker is running with `docker ps` and check docker-compose.yml syntax")
    )]
    DockerCompose(String),

    #[error("Script not found: {0}")]
    #[diagnostic(
        code(fed::script::not_found),
        help("List available scripts in the 'scripts:' section of your service-federation.yaml")
    )]
    ScriptNotFound(String),

    #[error(
        "Environment file '{env_file}' sets undeclared parameter '{name}': declare it in service-federation.yaml or remove from .env"
    )]
    #[diagnostic(
        code(fed::env::undeclared),
        help("Add parameter declaration:\n\nparameters:\n  {name}:\n    default: \"\"")
    )]
    UndeclaredEnvVariable { name: String, env_file: String },

    #[error("Script '{name}' exited with code {exit_code}")]
    #[diagnostic(code(fed::script::failed))]
    ScriptFailed { name: String, exit_code: i32 },
}

pub type Result<T> = std::result::Result<T, Error>;

impl Error {
    /// Returns a helpful suggestion for resolving this error, if available.
    pub fn suggestion(&self) -> Option<String> {
        match self {
            Error::ServiceNotFound(name) => Some(format!(
                "No service named '{}' is defined in your config. Run 'fed status' to list services, or check service-federation.yaml for typos.",
                name
            )),
            Error::ServiceNotRunning(name) => Some(format!(
                "Start the service with: fed start {}",
                name
            )),
            Error::PortAllocation(msg) => Some(format!(
                "Port allocation failed: {}. Try using 'type: port' parameters which auto-fallback to available ports.",
                msg
            )),
            Error::DockerPortConflict { port, .. } => Some(format!(
                "Port {} is in use.\n\nIf another checkout of this project owns it (worktree, parallel agent):\n\n    fed isolate enable      # give this directory its own ports\n\nIf a stray external process holds it:\n\n    fed start --replace     # kills whatever holds the port — including\n                            # other checkouts' fed services",
                port
            )),
            Error::DockerPortConflictOtherStack { port, container, .. } => Some(format!(
                "Port {} is held by container '{}' — another checkout or worktree of this project.\n\nGive this directory its own ports instead (persisted, applies to every later fed command):\n\n    fed isolate enable\n\nDo not use `fed start --replace` here — it would kill the other checkout's services.",
                port, container
            )),
            Error::CircularDependency(path) => Some(format!(
                "Services cannot depend on each other in a cycle. Review the depends_on fields for: {}",
                path.join(", ")
            )),
            Error::HealthCheckFailed(service, _) => Some(format!(
                "Check the service logs with: fed logs {}\nAlso verify the healthcheck URL/command is correct in your config.",
                service
            )),
            Error::Config(msg) if msg.contains("Could not find") => None,
            Error::Config(_) => Some(
                "Validate your config with: fed validate".to_string()
            ),
            Error::Parse(_) | Error::Yaml(_) => Some(
                "Check the YAML syntax at the line/column mentioned above — indentation errors are the most common cause. YAML requires consistent spaces (no tabs).".to_string()
            ),
            Error::Docker(docker_err) => match docker_err {
                DockerError::Timeout { command, timeout } => Some(format!(
                    "'{}' timed out after {}s. Check Docker daemon responsiveness: docker ps",
                    command,
                    timeout.as_secs()
                )),
                DockerError::ExecFailed { .. } => Some(
                    "Docker binary not found or not executable. Install Docker or check your PATH."
                        .to_string(),
                ),
                DockerError::DaemonUnavailable => Some(
                    "Start Docker Desktop or: sudo systemctl start docker".to_string(),
                ),
                DockerError::ContainerNotFound { container } => Some(format!(
                    "Container '{}' does not exist. It may have already been removed.",
                    container
                )),
                DockerError::CommandFailed { .. } => {
                    Some("Check that Docker is running: docker ps".to_string())
                }
            },
            Error::Process(_) => Some(
                "Check that the command exists and is executable".to_string()
            ),
            Error::ScriptNotFound(name) => Some(format!(
                "Available scripts are defined in the 'scripts:' section of your config. Did you mean a different name than '{}'?",
                name
            )),
            Error::UndeclaredEnvVariable { name, .. } => Some(format!(
                "Add a parameter declaration for '{}' in your service-federation.yaml:\n\nparameters:\n  {}:\n    default: \"\"",
                name, name
            )),
            Error::Database(e) => {
                // String matching is unavoidable here: tokio_rusqlite wraps the
                // underlying rusqlite error opaquely, so we can't match on error codes.
                let err_str = e.to_string();
                if err_str.contains("database is locked") || err_str.contains("SQLITE_BUSY") {
                    Some(
                        "Another fed instance may be running. Use `fed stop` to stop all services, or check for stale lock files:\n  rm .fed/.lock\n\nIf the issue persists, find and stop other fed processes.".to_string()
                    )
                } else if err_str.contains("disk I/O error") || err_str.contains("SQLITE_IOERR") {
                    Some(
                        "Database disk I/O error detected. Check:\n  1. Disk space: df -h\n  2. File permissions on .fed/ directory\n  3. File system health\n\nIf the issue persists, backup and remove .fed/ directory.".to_string()
                    )
                } else if err_str.contains("database disk image is malformed") || err_str.contains("SQLITE_CORRUPT") {
                    Some(
                        "Database corrupted. Backup the database and try recovery:\n  cp .fed/lock.db .fed/lock.db.backup\n  sqlite3 .fed/lock.db '.recover' | sqlite3 .fed/lock.db.recovered\n\nIf recovery fails, remove .fed/ directory as a last resort:\n  rm -rf .fed/".to_string()
                    )
                } else {
                    Some(
                        "Database error encountered. If the issue persists, you can reset fed's state by running:\n  rm -rf .fed/\n\nThis will remove all service state and port allocations. Running services will need to be restarted.".to_string()
                    )
                }
            }
            _ => None,
        }
    }

    /// Formats the error with its suggestion (if any) for user-friendly display.
    pub fn with_suggestion(&self) -> String {
        match self.suggestion() {
            Some(suggestion) => format!("{}\n\nHint: {}", self, suggestion),
            None => self.to_string(),
        }
    }
}

/// Validates and converts a u32 PID to nix::unistd::Pid safely.
/// Returns Err for PID 0 (process group), PID 1 (init), or values > i32::MAX.
pub fn validate_pid(pid: u32, service_name: &str) -> Result<nix::unistd::Pid> {
    if pid == 0 {
        return Err(Error::InvalidPid {
            pid,
            reason: format!(
                "PID 0 is invalid for service '{}' (refers to process group, not a process)",
                service_name
            ),
        });
    }
    if pid == 1 {
        return Err(Error::InvalidPid {
            pid,
            reason: format!(
                "refusing to operate on PID 1 (init) for service '{}'",
                service_name
            ),
        });
    }
    if pid > i32::MAX as u32 {
        return Err(Error::InvalidPid {
            pid,
            reason: format!(
                "PID {} exceeds i32::MAX for service '{}', cannot convert safely",
                pid, service_name
            ),
        });
    }
    Ok(nix::unistd::Pid::from_raw(pid as i32))
}

/// Same as validate_pid but allows PID 1 check to be skipped for existence checks.
/// Use validate_pid for signal operations; use this for read-only checks.
pub fn validate_pid_for_check(pid: u32) -> Option<nix::unistd::Pid> {
    if pid == 0 || pid > i32::MAX as u32 {
        return None;
    }
    Some(nix::unistd::Pid::from_raw(pid as i32))
}

/// Query the kernel clock tick rate (jiffies per second) at runtime.
///
/// Falls back to 100 (the common default) if sysconf fails.
#[cfg(target_os = "linux")]
fn get_clock_ticks_per_sec() -> u64 {
    nix::unistd::sysconf(nix::unistd::SysconfVar::CLK_TCK)
        .ok()
        .flatten()
        .map(|v| v as u64)
        .unwrap_or(100)
}

/// Check if a PID belongs to the expected process by comparing start times.
///
/// Returns true if the PID appears valid (start time matches or cannot be determined),
/// false if the PID was clearly reused by a different process.
///
/// This is a defensive check against PID reuse - if a service's PID was recorded
/// but the process died and a new process reused the same PID, we don't want to
/// accidentally kill an unrelated process.
pub fn validate_pid_start_time(pid: u32, expected_start: chrono::DateTime<chrono::Utc>) -> bool {
    #[cfg(target_os = "linux")]
    {
        // On Linux, read /proc/<pid>/stat to get process start time
        let stat_path = format!("/proc/{}/stat", pid);
        if let Ok(stat) = std::fs::read_to_string(&stat_path) {
            // The stat file format has the process name in parens which can contain spaces,
            // so we find the closing paren and parse from there
            if let Some(close_paren) = stat.rfind(')') {
                let fields: Vec<&str> = stat[close_paren + 2..].split_whitespace().collect();
                // Field 20 (0-indexed after the first two fields) is starttime
                if let Some(&starttime_str) = fields.get(19)
                    && let Ok(starttime_jiffies) = starttime_str.parse::<u64>()
                {
                    let now = chrono::Utc::now();
                    let expected_age = now.signed_duration_since(expected_start);

                    // Get uptime to calculate approximate process start
                    if let Ok(uptime_str) = std::fs::read_to_string("/proc/uptime")
                        && let Some(uptime_secs_str) = uptime_str.split_whitespace().next()
                        && let Ok(uptime_secs) = uptime_secs_str.parse::<f64>()
                    {
                        let jiffies_per_sec = get_clock_ticks_per_sec();
                        let process_age_secs =
                            uptime_secs - (starttime_jiffies as f64 / jiffies_per_sec as f64);

                        // If process started more than 60 seconds before our expected time,
                        // it's likely a different process that reused the PID
                        let expected_age_secs = expected_age.num_seconds() as f64;
                        let time_diff = (process_age_secs - expected_age_secs).abs();

                        if time_diff > 60.0 {
                            tracing::warn!(
                                "PID {} appears to be reused: process age {:.0}s vs expected {:.0}s",
                                pid,
                                process_age_secs,
                                expected_age_secs
                            );
                            return false;
                        }
                    }
                }
            }
        }
    }

    #[cfg(target_os = "macos")]
    {
        // On macOS, use ps to get process start time
        use chrono::TimeZone;
        use std::process::Command;
        if let Ok(output) = Command::new("ps")
            .args(["-o", "lstart=", "-p", &pid.to_string()])
            .env("LC_ALL", "C")
            .output()
            && output.status.success()
        {
            let lstart = String::from_utf8_lossy(&output.stdout);
            // Parse the lstart format: "Mon Jan  1 12:00:00 2024"
            let lstart = lstart.trim();
            if !lstart.is_empty()
                && let Ok(process_start) =
                    chrono::NaiveDateTime::parse_from_str(lstart, "%a %b %e %H:%M:%S %Y")
            {
                // ps lstart returns local time, not UTC
                let process_start_utc = chrono::Local
                    .from_local_datetime(&process_start)
                    .earliest()
                    .map(|dt| dt.with_timezone(&chrono::Utc));
                let Some(process_start_utc) = process_start_utc else {
                    // Ambiguous/nonexistent time (DST transition) — trust the PID
                    tracing::debug!(
                        "PID {} start time {:?} is ambiguous during DST, trusting PID",
                        pid,
                        process_start
                    );
                    return true;
                };
                let time_diff = (process_start_utc - expected_start).num_seconds().abs();

                // Allow 60 seconds tolerance for timing differences
                if time_diff > 60 {
                    tracing::warn!(
                        "PID {} appears to be reused: process started at {} vs expected {}",
                        pid,
                        process_start_utc,
                        expected_start
                    );
                    return false;
                }
            }
        }
    }

    // Default: trust the PID if we can't determine start time
    true
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;

    // Port-conflict hints must lead with `fed isolate enable`: in a worktree,
    // `fed start --replace` kills the other checkout's services, and agents
    // follow whatever the error message suggests first.
    #[test]
    fn generic_port_conflict_suggests_isolation_before_replace() {
        let err = Error::DockerPortConflict {
            service: "db".to_string(),
            port: 5433,
        };
        let s = err.suggestion().expect("has suggestion");
        let isolate = s.find("fed isolate enable").expect("mentions isolate");
        let replace = s.find("fed start --replace").expect("mentions replace");
        assert!(isolate < replace, "isolation must come first: {}", s);
    }

    #[test]
    fn other_stack_port_conflict_names_container_and_forbids_replace() {
        let err = Error::DockerPortConflictOtherStack {
            service: "db".to_string(),
            port: 5433,
            container: "fed-7a80d381-postgres".to_string(),
        };
        let s = err.suggestion().expect("has suggestion");
        assert!(
            s.contains("fed-7a80d381-postgres"),
            "names the holder: {}",
            s
        );
        assert!(
            s.contains("fed isolate enable"),
            "recommends isolation: {}",
            s
        );
        assert!(
            s.contains("Do not use `fed start --replace`"),
            "warns against --replace: {}",
            s
        );
    }

    // ========================================================================
    // validate_pid_start_time tests
    //
    // This function is platform-specific:
    //   - macOS: calls `ps -o lstart=` and compares parsed time
    //   - Linux: reads /proc/<pid>/stat and /proc/uptime
    // Tests below use the current process PID (which is known to exist) and
    // work on the current platform. On unsupported platforms, the function
    // always returns true (trusts the PID).
    // ========================================================================

    #[test]
    fn matching_start_time_returns_true() {
        // Use our own PID — it's guaranteed to exist and we know roughly when it started.
        let my_pid = std::process::id();
        // Use "now" as the expected start time. Since the test process just started
        // (within the last few seconds of the test runner), this should be within
        // the 60-second tolerance window.
        let recent = Utc::now();
        assert!(
            validate_pid_start_time(my_pid, recent),
            "Current process PID with recent timestamp should validate"
        );
    }

    #[test]
    fn mismatched_start_time_returns_false() {
        // Use our own PID but claim it started far in the past (2020).
        // The function should detect the mismatch (>60s difference) and return false.
        let my_pid = std::process::id();
        let old_time = chrono::DateTime::parse_from_rfc3339("2020-01-01T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        assert!(
            !validate_pid_start_time(my_pid, old_time),
            "Current process PID with old timestamp should detect PID reuse"
        );
    }

    #[test]
    fn nonexistent_pid_returns_true() {
        // PID that almost certainly doesn't exist. The function returns true
        // (trusts the PID) when it can't determine the start time.
        let bogus_pid = u32::MAX - 1;
        let now = Utc::now();
        assert!(
            validate_pid_start_time(bogus_pid, now),
            "Non-existent PID should return true (can't verify, so trust it)"
        );
    }
}
