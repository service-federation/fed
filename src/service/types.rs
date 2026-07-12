use crate::error::Result;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;

/// Current lifecycle status of a service.
///
/// Services transition through these states as they are started, monitored, and stopped.
/// The typical lifecycle is: `Stopped` → `Starting` → `Running` → `Healthy` → `Stopping` → `Stopped`.
///
/// # State Transitions
///
/// ```text
/// Stopped ──► Starting ──► Running ──► Healthy
///    ▲                         │           │
///    │                         ▼           ▼
///    └─────── Stopping ◄─── Failing ◄─────┘
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Status {
    /// Service is not running
    Stopped,
    /// Service is in the process of starting
    Starting,
    /// Service process is running but health not yet verified
    Running,
    /// Service is running and passed health checks
    Healthy,
    /// Service is running but failing health checks
    Failing,
    /// Service is in the process of stopping
    Stopping,
    /// Oneshot service (`run:`) that executed to completion successfully.
    /// Not a running process — completion is its terminal, healthy state.
    Completed,
}

impl fmt::Display for Status {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Status::Stopped => write!(f, "stopped"),
            Status::Starting => write!(f, "starting"),
            Status::Running => write!(f, "running"),
            Status::Healthy => write!(f, "healthy"),
            Status::Failing => write!(f, "failing"),
            Status::Stopping => write!(f, "stopping"),
            Status::Completed => write!(f, "completed"),
        }
    }
}

impl std::str::FromStr for Status {
    type Err = String;
    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s {
            "stopped" => Ok(Status::Stopped),
            "starting" => Ok(Status::Starting),
            "running" => Ok(Status::Running),
            "healthy" => Ok(Status::Healthy),
            "failing" => Ok(Status::Failing),
            "stopping" => Ok(Status::Stopping),
            "completed" => Ok(Status::Completed),
            other => Err(format!("Unknown status: {}", other)),
        }
    }
}

impl Status {
    /// Check if a status transition is valid according to the state machine.
    ///
    /// Valid transitions:
    /// ```text
    /// Stopped ──► Starting ──► Running ──► Healthy
    ///    ▲                         │           │
    ///    │                         ▼           ▼
    ///    └─────── Stopping ◄─── Failing ◄─────┘
    /// ```
    ///
    /// # Examples
    ///
    /// ```
    /// use fed::service::Status;
    ///
    /// assert!(Status::Stopped.is_valid_transition(Status::Starting));
    /// assert!(Status::Starting.is_valid_transition(Status::Running));
    /// assert!(!Status::Stopped.is_valid_transition(Status::Running)); // Must go through Starting
    /// ```
    pub fn is_valid_transition(&self, to: Status) -> bool {
        use Status::*;
        match (self, to) {
            // Stopped can transition to Starting
            (Stopped, Starting) => true,

            // Starting can transition to Running or Failing (immediate crash)
            (Starting, Running) => true,
            (Starting, Failing) => true,

            // Running can transition to Healthy, Failing, or Stopping
            (Running, Healthy) => true,
            (Running, Failing) => true,
            (Running, Stopping) => true,

            // Healthy can transition to Failing or Stopping
            (Healthy, Failing) => true,
            (Healthy, Stopping) => true,

            // Failing can transition to Stopping
            (Failing, Stopping) => true,

            // Stopping always transitions to Stopped
            (Stopping, Stopped) => true,

            // Oneshot (`run:`) completion: reached from Starting/Running, and
            // torn down like any terminal state. Re-running a restored-Completed
            // oneshot goes back through Starting.
            (Starting, Completed) => true,
            (Running, Completed) => true,
            (Completed, Stopping) => true,
            (Completed, Stopped) => true,
            (Completed, Starting) => true,

            // Same state is always valid (no-op transition)
            (s1, s2) if *s1 == s2 => true,

            // All other transitions are invalid
            _ => false,
        }
    }
}

/// Atomic state transition bundling status change with metadata updates.
///
/// This type ensures that status transitions happen atomically with their associated
/// metadata (PID, container ID, etc.) to prevent inconsistent state where the database
/// shows "Running" but has no PID.
///
/// # Design Rationale
///
/// Previously, status was updated separately from PID/container_id. If a crash happened
/// between these operations, state became inconsistent. This type bundles all updates
/// into a single atomic transaction at the database level.
#[derive(Debug, Clone)]
pub struct StateTransition {
    /// Target status
    pub status: Status,
    /// Optional PID (for process services)
    pub pid: Option<u32>,
    /// Optional container ID (for Docker services)
    pub container_id: Option<String>,
    /// Whether to clear PID when transitioning to Stopped
    pub clear_pid: bool,
    /// Whether to clear container ID when transitioning to Stopped
    pub clear_container_id: bool,
}

impl StateTransition {
    /// Create a transition to Starting status
    pub fn starting() -> Self {
        Self {
            status: Status::Starting,
            pid: None,
            container_id: None,
            clear_pid: false,
            clear_container_id: false,
        }
    }

    /// Create a transition to Running status with PID
    pub fn running_with_pid(pid: u32) -> Self {
        Self {
            status: Status::Running,
            pid: Some(pid),
            container_id: None,
            clear_pid: false,
            clear_container_id: false,
        }
    }

    /// Create a transition to Running status with container ID
    pub fn running_with_container(container_id: String) -> Self {
        Self {
            status: Status::Running,
            pid: None,
            container_id: Some(container_id),
            clear_pid: false,
            clear_container_id: false,
        }
    }

    /// Create a transition to Running status (without PID/container)
    pub fn running() -> Self {
        Self {
            status: Status::Running,
            pid: None,
            container_id: None,
            clear_pid: false,
            clear_container_id: false,
        }
    }

    /// Create a transition to Stopping status
    pub fn stopping() -> Self {
        Self {
            status: Status::Stopping,
            pid: None,
            container_id: None,
            clear_pid: false,
            clear_container_id: false,
        }
    }

    /// Create a transition to Stopped status (clears PID and container ID)
    pub fn stopped() -> Self {
        Self {
            status: Status::Stopped,
            pid: None,
            container_id: None,
            clear_pid: true,
            clear_container_id: true,
        }
    }

    /// Create a transition to Failing status
    pub fn failing() -> Self {
        Self {
            status: Status::Failing,
            pid: None,
            container_id: None,
            clear_pid: false,
            clear_container_id: false,
        }
    }

    /// Create a transition to Healthy status
    pub fn healthy() -> Self {
        Self {
            status: Status::Healthy,
            pid: None,
            container_id: None,
            clear_pid: false,
            clear_container_id: false,
        }
    }

    /// Validate this transition from the given current status.
    ///
    /// Returns an error if the transition is invalid according to the state machine.
    pub fn validate(&self, from: Status) -> Result<()> {
        if !from.is_valid_transition(self.status) {
            return Err(crate::error::Error::Validation(format!(
                "Invalid state transition: {} -> {}",
                from, self.status
            )));
        }
        Ok(())
    }
}

/// Process output capture mode.
///
/// Determines how stdout/stderr from process services are handled.
/// This provides flexibility for different use cases: background services,
/// interactive development, and testing/CI pipelines.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum OutputMode {
    /// Capture output to log files on disk.
    ///
    /// - Stdout/stderr redirected to log file
    /// - Logs persisted across restarts
    /// - Used for detached background processes (`fed start`)
    File,

    /// Capture output to in-memory ring buffer.
    ///
    /// - Stdout/stderr piped to memory (max 1000 lines)
    /// - Enables real-time log viewing
    /// - Used for interactive TUI and watch mode
    #[default]
    Captured,

    /// Pass output through to parent process.
    ///
    /// - Stdout/stderr inherited from parent (Stdio::inherit)
    /// - Direct visibility without buffering
    /// - Used for testing, CI/CD, single-service debugging
    Passthrough,
}

impl OutputMode {
    /// Returns true if output should be captured to a file.
    pub fn is_file(&self) -> bool {
        matches!(self, OutputMode::File)
    }

    /// Returns true if output should be captured to memory.
    pub fn is_captured(&self) -> bool {
        matches!(self, OutputMode::Captured)
    }

    /// Returns true if output should pass through to parent.
    pub fn is_passthrough(&self) -> bool {
        matches!(self, OutputMode::Passthrough)
    }

    /// Returns true if this mode requires log capture infrastructure.
    ///
    /// Passthrough mode doesn't need log capture since output goes directly
    /// to the parent process.
    pub fn needs_log_capture(&self) -> bool {
        !matches!(self, OutputMode::Passthrough)
    }
}

impl fmt::Display for OutputMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            OutputMode::File => write!(f, "file"),
            OutputMode::Captured => write!(f, "captured"),
            OutputMode::Passthrough => write!(f, "passthrough"),
        }
    }
}

impl std::str::FromStr for OutputMode {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "file" => Ok(OutputMode::File),
            "captured" => Ok(OutputMode::Captured),
            "passthrough" => Ok(OutputMode::Passthrough),
            _ => Err(format!(
                "Invalid output mode: '{}'. Use 'file', 'captured', or 'passthrough'.",
                s
            )),
        }
    }
}

/// Core trait for managing service lifecycle.
///
/// Implement this trait to create custom service types. The orchestrator uses these
/// methods to start, stop, and monitor services.
///
/// # Required Methods
///
/// - [`start`](Self::start) - Launch the service
/// - [`stop`](Self::stop) - Gracefully stop the service
/// - [`kill`](Self::kill) - Forcefully terminate the service
/// - [`health`](Self::health) - Check if the service is healthy
/// - [`status`](Self::status) - Get current lifecycle status
/// - [`name`](Self::name) - Get the service identifier
///
/// # Optional Methods
///
/// Default implementations are provided for optional features like log retrieval,
/// PID tracking, and container ID tracking.
#[async_trait]
pub trait ServiceManager: Send + Sync {
    /// Start the service.
    ///
    /// This should launch the service and return once it's running.
    /// The service status should transition to `Starting` then `Running`.
    #[must_use = "ignoring this result means the service may not have started successfully"]
    async fn start(&mut self) -> Result<()>;

    /// Stop the service gracefully.
    ///
    /// Sends a termination signal (e.g., SIGTERM) and waits for clean shutdown.
    /// Falls back to force kill if the service doesn't stop within timeout.
    #[must_use = "ignoring this result means the service may not have stopped - resources may leak"]
    async fn stop(&mut self) -> Result<()>;

    /// Kill the service forcefully.
    ///
    /// Sends SIGKILL or equivalent to immediately terminate the service.
    /// Use only when graceful shutdown fails.
    #[must_use = "ignoring this result means the service may not have been killed - resources may leak"]
    async fn kill(&mut self) -> Result<()>;

    /// Check if the service is healthy.
    ///
    /// Returns `true` if the service is responding correctly to health checks.
    /// The specific check depends on the service configuration (HTTP, command, etc.).
    async fn health(&self) -> Result<bool>;

    /// Get the current lifecycle status.
    fn status(&self) -> Status;

    /// Get the service name/identifier.
    fn name(&self) -> &str;

    /// Get service logs (if available)
    async fn logs(&self, _tail: Option<usize>) -> Result<Vec<String>> {
        // Default implementation returns empty logs
        Ok(Vec::new())
    }

    /// Get process ID (if applicable)
    fn get_pid(&self) -> Option<u32> {
        // Default implementation returns None
        None
    }

    /// Get container ID (if applicable)
    fn get_container_id(&self) -> Option<String> {
        // Default implementation returns None
        None
    }

    /// Get port mappings (if applicable) - maps container port to host port
    /// Returns HashMap<String, String> where key is container port (e.g., "5432/tcp")
    /// and value is host port (e.g., "59890")
    ///
    /// This is async because Docker port queries can be slow, especially on macOS.
    async fn get_port_mappings(&self) -> HashMap<String, String> {
        // Default implementation returns empty map
        HashMap::new()
    }

    /// Get last error message (if any)
    fn get_last_error(&self) -> Option<String> {
        // Default implementation returns None
        None
    }

    /// Get resource usage metrics (memory, CPU, threads).
    ///
    /// Returns resource usage information if the service is running and metrics
    /// are available. Returns None if the service is not running, PID is unavailable,
    /// or the platform doesn't support resource monitoring.
    ///
    /// # Platform Support
    ///
    /// - **Linux**: Reads from `/proc/[pid]/stat` and `/proc/[pid]/status`
    /// - **macOS**: Uses `ps` command
    /// - **Docker**: Not yet implemented (returns None)
    async fn get_resource_usage(&self) -> Option<super::ResourceUsage> {
        // Default implementation returns None
        None
    }

    /// Downcast to Any for type-specific operations
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any;
}

/// Service information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServiceInfo {
    pub name: String,
    pub status: Status,
    pub service_type: String,
    pub start_time: Option<DateTime<Utc>>,
    pub last_health: Option<DateTime<Utc>>,
    pub health_error: Option<String>,
}

/// Base service structure with common fields
#[derive(Debug)]
pub struct BaseService {
    pub name: String,
    pub status: Status,
    pub environment: HashMap<String, String>,
    pub work_dir: String,
    pub start_time: Option<DateTime<Utc>>,
    pub last_error: Option<String>,
}

impl BaseService {
    pub fn new(name: String, environment: HashMap<String, String>, work_dir: String) -> Self {
        Self {
            name,
            status: Status::Stopped,
            environment,
            work_dir,
            start_time: None,
            last_error: None,
        }
    }

    pub fn set_status(&mut self, status: Status) {
        self.status = status;
        if status == Status::Running || status == Status::Starting {
            if self.start_time.is_none() {
                self.start_time = Some(Utc::now());
            }
            // Clear error on successful start
            self.last_error = None;
        } else if status == Status::Stopped {
            self.start_time = None;
        }
    }

    pub fn set_error(&mut self, error: String) {
        self.last_error = Some(error);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_output_mode_default() {
        let mode: OutputMode = Default::default();
        assert_eq!(mode, OutputMode::Captured);
    }

    #[test]
    fn test_output_mode_is_file() {
        assert!(OutputMode::File.is_file());
        assert!(!OutputMode::Captured.is_file());
        assert!(!OutputMode::Passthrough.is_file());
    }

    #[test]
    fn test_output_mode_is_captured() {
        assert!(!OutputMode::File.is_captured());
        assert!(OutputMode::Captured.is_captured());
        assert!(!OutputMode::Passthrough.is_captured());
    }

    #[test]
    fn test_output_mode_is_passthrough() {
        assert!(!OutputMode::File.is_passthrough());
        assert!(!OutputMode::Captured.is_passthrough());
        assert!(OutputMode::Passthrough.is_passthrough());
    }

    #[test]
    fn test_output_mode_needs_log_capture() {
        // File and Captured modes need log capture infrastructure
        assert!(OutputMode::File.needs_log_capture());
        assert!(OutputMode::Captured.needs_log_capture());
        // Passthrough outputs directly to parent, no capture needed
        assert!(!OutputMode::Passthrough.needs_log_capture());
    }

    #[test]
    fn test_output_mode_display() {
        assert_eq!(OutputMode::File.to_string(), "file");
        assert_eq!(OutputMode::Captured.to_string(), "captured");
        assert_eq!(OutputMode::Passthrough.to_string(), "passthrough");
    }

    #[test]
    fn test_status_from_str() {
        assert_eq!("stopped".parse::<Status>(), Ok(Status::Stopped));
        assert_eq!("starting".parse::<Status>(), Ok(Status::Starting));
        assert_eq!("running".parse::<Status>(), Ok(Status::Running));
        assert_eq!("healthy".parse::<Status>(), Ok(Status::Healthy));
        assert_eq!("failing".parse::<Status>(), Ok(Status::Failing));
        assert_eq!("stopping".parse::<Status>(), Ok(Status::Stopping));
        assert!("unknown".parse::<Status>().is_err());
    }

    // ========================================================================
    // State Transition Validation Tests
    // ========================================================================

    #[test]
    fn test_valid_transitions_stopped_to_starting() {
        assert!(Status::Stopped.is_valid_transition(Status::Starting));
    }

    #[test]
    fn test_valid_transitions_starting_to_running() {
        assert!(Status::Starting.is_valid_transition(Status::Running));
    }

    #[test]
    fn test_valid_transitions_starting_to_failing() {
        // Immediate crash during startup
        assert!(Status::Starting.is_valid_transition(Status::Failing));
    }

    #[test]
    fn test_valid_transitions_running_to_healthy() {
        assert!(Status::Running.is_valid_transition(Status::Healthy));
    }

    #[test]
    fn test_valid_transitions_running_to_failing() {
        assert!(Status::Running.is_valid_transition(Status::Failing));
    }

    #[test]
    fn test_valid_transitions_running_to_stopping() {
        assert!(Status::Running.is_valid_transition(Status::Stopping));
    }

    #[test]
    fn test_valid_transitions_healthy_to_failing() {
        assert!(Status::Healthy.is_valid_transition(Status::Failing));
    }

    #[test]
    fn test_valid_transitions_healthy_to_stopping() {
        assert!(Status::Healthy.is_valid_transition(Status::Stopping));
    }

    #[test]
    fn test_valid_transitions_failing_to_stopping() {
        assert!(Status::Failing.is_valid_transition(Status::Stopping));
    }

    #[test]
    fn test_valid_transitions_stopping_to_stopped() {
        assert!(Status::Stopping.is_valid_transition(Status::Stopped));
    }

    #[test]
    fn test_valid_transitions_same_state() {
        // Same state transitions are always valid (no-op)
        assert!(Status::Stopped.is_valid_transition(Status::Stopped));
        assert!(Status::Starting.is_valid_transition(Status::Starting));
        assert!(Status::Running.is_valid_transition(Status::Running));
        assert!(Status::Healthy.is_valid_transition(Status::Healthy));
        assert!(Status::Failing.is_valid_transition(Status::Failing));
        assert!(Status::Stopping.is_valid_transition(Status::Stopping));
    }

    #[test]
    fn test_invalid_transitions_stopped_to_running() {
        // Must go through Starting
        assert!(!Status::Stopped.is_valid_transition(Status::Running));
    }

    #[test]
    fn test_invalid_transitions_stopped_to_healthy() {
        // Must go through Starting -> Running -> Healthy
        assert!(!Status::Stopped.is_valid_transition(Status::Healthy));
    }

    #[test]
    fn test_invalid_transitions_starting_to_healthy() {
        // Must go through Running first
        assert!(!Status::Starting.is_valid_transition(Status::Healthy));
    }

    #[test]
    fn test_invalid_transitions_starting_to_stopped() {
        // Must go through Running/Failing -> Stopping -> Stopped
        assert!(!Status::Starting.is_valid_transition(Status::Stopped));
    }

    #[test]
    fn test_invalid_transitions_running_to_stopped() {
        // Must go through Stopping
        assert!(!Status::Running.is_valid_transition(Status::Stopped));
    }

    #[test]
    fn test_invalid_transitions_healthy_to_stopped() {
        // Must go through Stopping
        assert!(!Status::Healthy.is_valid_transition(Status::Stopped));
    }

    #[test]
    fn test_invalid_transitions_failing_to_stopped() {
        // Must go through Stopping
        assert!(!Status::Failing.is_valid_transition(Status::Stopped));
    }

    #[test]
    fn test_invalid_transitions_stopping_to_running() {
        // Can't go backwards to Running
        assert!(!Status::Stopping.is_valid_transition(Status::Running));
    }

    // ========================================================================
    // StateTransition Tests
    // ========================================================================

    #[test]
    fn test_state_transition_starting() {
        let transition = StateTransition::starting();
        assert_eq!(transition.status, Status::Starting);
        assert!(transition.pid.is_none());
        assert!(transition.container_id.is_none());
        assert!(!transition.clear_pid);
        assert!(!transition.clear_container_id);
    }

    #[test]
    fn test_state_transition_running_with_pid() {
        let transition = StateTransition::running_with_pid(12345);
        assert_eq!(transition.status, Status::Running);
        assert_eq!(transition.pid, Some(12345));
        assert!(transition.container_id.is_none());
        assert!(!transition.clear_pid);
    }

    #[test]
    fn test_state_transition_running_with_container() {
        let transition = StateTransition::running_with_container("abc123".to_string());
        assert_eq!(transition.status, Status::Running);
        assert!(transition.pid.is_none());
        assert_eq!(transition.container_id, Some("abc123".to_string()));
        assert!(!transition.clear_container_id);
    }

    #[test]
    fn test_state_transition_stopped() {
        let transition = StateTransition::stopped();
        assert_eq!(transition.status, Status::Stopped);
        assert!(transition.pid.is_none());
        assert!(transition.container_id.is_none());
        assert!(transition.clear_pid);
        assert!(transition.clear_container_id);
    }

    #[test]
    fn test_state_transition_validate_valid() {
        let transition = StateTransition::starting();
        assert!(transition.validate(Status::Stopped).is_ok());
    }

    #[test]
    fn test_state_transition_validate_invalid() {
        let transition = StateTransition::running();
        let result = transition.validate(Status::Stopped);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Invalid state transition"));
    }
}
