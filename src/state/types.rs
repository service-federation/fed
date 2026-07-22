use crate::config::ServiceType;
use crate::service::Status;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;
use std::str::FromStr;

/// Lock file format - persisted state of running services
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LockFile {
    /// PID of the fed process that created this lock
    pub fed_pid: u32,

    /// Working directory when services were started
    pub work_dir: String,

    /// When this orchestration run started
    pub started_at: DateTime<Utc>,

    /// All running services with their state
    pub services: HashMap<String, ServiceState>,

    /// All allocated ports (to prevent conflicts)
    pub allocated_ports: Vec<u16>,
}

impl LockFile {
    pub fn new(work_dir: String) -> Self {
        Self {
            fed_pid: std::process::id(),
            work_dir,
            started_at: Utc::now(),
            services: HashMap::new(),
            allocated_ports: Vec::new(),
        }
    }
}

/// Persisted state for a single service
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServiceState {
    /// Fully qualified service name (with namespace)
    pub id: String,

    /// Current status
    pub status: Status,

    /// Service type (process, docker, external, etc.)
    pub service_type: ServiceType,

    /// Process ID (if applicable)
    pub pid: Option<u32>,

    /// Docker container ID (if applicable)
    pub container_id: Option<String>,

    /// Port allocations for this service
    pub port_allocations: HashMap<String, u16>,

    /// When this service was started
    pub started_at: DateTime<Utc>,

    /// If this is from an external service, the repo info
    pub external_repo: Option<String>,

    /// Namespace this service belongs to (root, or external service name)
    pub namespace: String,

    /// Number of times this service has been restarted
    #[serde(default)]
    pub restart_count: u32,

    /// When the service was last restarted (if ever)
    #[serde(default)]
    pub last_restart_at: Option<DateTime<Utc>>,

    /// Number of consecutive failures (resets on successful health check)
    #[serde(default)]
    pub consecutive_failures: u32,

    /// Resolved startup message template (for display after start and in status)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub startup_message: Option<String>,

    /// Whether this service is meant to be running or was explicitly stopped.
    ///
    /// Independent of `status` (which reflects last-observed reality, e.g.
    /// `Running`/`Failing`/`Stopping`): `desired_state` is the persisted
    /// *intent* signal. Every stop path writes `Stopped` here before sending
    /// any kill signal, so a separate `fed` process sharing only the SQLite
    /// state file (see `07-supervisor.md` Design §1) can tell an intentional
    /// stop apart from a crash without relying on an in-process manager
    /// object it never touched.
    #[serde(default)]
    pub desired_state: DesiredState,

    /// Whether this service qualifies for Docker's native `--restart
    /// unless-stopped` reboot-survival flag — mirrors
    /// [`crate::config::Service::docker_native_restart_enabled`], captured
    /// at registration time (registration leaves an already-registered row
    /// untouched, same as `startup_message`).
    ///
    /// Read by `mark_dead_services` (`07-supervisor.md` Design §3): a
    /// concurrent `fed` command's container-liveness check would otherwise
    /// mark a row `'stale'` (permanently filtering it out of
    /// `get_services()`) during Docker's own brief native-restart backoff
    /// window. Services with this flag set get a short grace period
    /// (consecutive-failure counter, not a single snapshot) before staling;
    /// everything else keeps today's one-shot staleness check.
    #[serde(default)]
    pub native_restart_enabled: bool,
}

/// Persisted intent for whether a service should be running.
///
/// See [`ServiceState::desired_state`] for why this is a separate field from
/// `status`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum DesiredState {
    /// The service should be running (default — set on registration).
    #[default]
    Running,
    /// The service was explicitly stopped; nothing should try to bring it
    /// back until it's started again.
    Stopped,
}

impl fmt::Display for DesiredState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DesiredState::Running => write!(f, "running"),
            DesiredState::Stopped => write!(f, "stopped"),
        }
    }
}

impl FromStr for DesiredState {
    type Err = String;
    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s {
            "running" => Ok(DesiredState::Running),
            "stopped" => Ok(DesiredState::Stopped),
            other => Err(format!("Invalid desired_state: {}", other)),
        }
    }
}

impl ServiceState {
    pub fn new(id: String, service_type: ServiceType, namespace: String) -> Self {
        Self {
            id,
            status: Status::Starting,
            service_type,
            pid: None,
            container_id: None,
            port_allocations: HashMap::new(),
            started_at: Utc::now(),
            external_repo: None,
            namespace,
            restart_count: 0,
            last_restart_at: None,
            consecutive_failures: 0,
            startup_message: None,
            desired_state: DesiredState::Running,
            native_restart_enabled: false,
        }
    }

    pub fn with_pid(mut self, pid: u32) -> Self {
        self.pid = Some(pid);
        self
    }

    pub fn with_container_id(mut self, container_id: String) -> Self {
        self.container_id = Some(container_id);
        self
    }

    pub fn with_external_repo(mut self, repo: String) -> Self {
        self.external_repo = Some(repo);
        self
    }
}

/// Result of attempting to register a service in the state tracker.
///
/// This replaces the previous `bool` return (`true` = new, `false` = existed)
/// with a richer type that communicates the existing service's status, letting
/// callers make informed decisions without querying the DB again.
#[derive(Debug, Clone, PartialEq)]
pub enum RegistrationOutcome {
    /// Service was newly registered with `Starting` status.
    Registered,
    /// Service already exists in the state tracker with this status.
    /// The caller should not attempt to start it again.
    AlreadyExists { status: Status },
}
