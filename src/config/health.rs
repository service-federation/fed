//! Health check and restart policy configuration.
//!
//! This module contains types for configuring service health checks
//! and restart policies.

use super::parse_duration_string;
use serde::{Deserialize, Serialize};
use std::time::Duration;

/// Default health check timeout (5 seconds).
const DEFAULT_HEALTH_CHECK_TIMEOUT: Duration = Duration::from_secs(5);

/// Health check configuration for a service.
///
/// Supports multiple formats for flexibility:
///
/// ```yaml
/// # HTTP health check with timeout
/// healthcheck:
///   http_get: "http://localhost:8080/health"
///   timeout: "30s"
///
/// # Command health check with timeout
/// healthcheck:
///   command: "curl -f http://localhost:3000"
///   timeout: "5s"
///
/// # Simple command (no timeout, uses default)
/// healthcheck: "curl -f http://localhost:3000"
/// ```
///
/// # Startup semantics
///
/// `fed start` polls the healthcheck before dependents boot. Passing before
/// startup returns marks the service `Healthy` (the timeout is evaluated
/// between polling attempts, so a check in flight at the deadline may still
/// count). Not passing within `timeout` while the process is
/// still alive is non-fatal: the service stays `Running` (health unverified),
/// dependents proceed, and the timeout is reported as a startup health
/// warning (`StartHealth::TimedOut`) rather than an error. The process dying
/// before the healthcheck passes fails the start.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum HealthCheck {
    /// HTTP GET health check.
    /// Canonical key: `http_get`. `httpGet` is a legacy spelling, still accepted.
    HttpGet {
        #[serde(alias = "httpGet")]
        http_get: String,
        #[serde(flatten)]
        timing: HealthCheckTiming,
    },
    /// Command health check with explicit field.
    CommandMap {
        command: String,
        #[serde(flatten)]
        timing: HealthCheckTiming,
    },
    /// Simple command string health check (no timing config, uses defaults)
    Command(String),
}

/// Default interval between health-check probes.
///
/// 500ms, matching the poll loops in `orchestrator/health.rs` that this
/// replaced — not a new choice, the existing one made configurable.
const DEFAULT_HEALTH_CHECK_INTERVAL: Duration = Duration::from_millis(500);

/// Default number of consecutive failures before a *healthy* service is
/// marked unhealthy.
///
/// 3, matching Docker Compose. There is no older fed behaviour to preserve
/// here: before `retries` existed the monitoring loop never re-ran the
/// configured probe at all, so a live process with a failing health endpoint
/// stayed `Healthy` forever. Now that it is polled, 1 would restart a
/// service on a single blip, which is a worse default than waiting for the
/// failure to look real.
const DEFAULT_HEALTH_CHECK_RETRIES: u32 = 3;

/// The four timing knobs shared by both configurable health-check forms.
///
/// Flattened into the enum variants, so they are written as sibling keys of
/// `http_get`/`command` rather than nested.
///
/// The names follow Docker Compose, because the concepts are the same and a
/// second vocabulary for them would be a cost with no benefit:
///
/// | key | means |
/// |---|---|
/// | `start_period` | total time fed waits for the **first** pass before giving up |
/// | `interval` | time between probes |
/// | `probe_timeout` | how long a **single** probe may take |
/// | `retries` | consecutive failures that mark a healthy service unhealthy |
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct HealthCheckTiming {
    /// Total wait for the first successful check (e.g. "30s", "10m").
    ///
    /// Mutually exclusive with [`Self::timeout`], which is the older spelling
    /// of exactly this field. Kept as two fields rather than a serde alias so
    /// that writing both can be rejected instead of silently picking one.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub start_period: Option<String>,

    /// Former name for [`Self::start_period`]. Still honoured, and still the
    /// spelling in most existing configs, so it cannot simply be renamed —
    /// but it reads like a per-probe timeout and is not one, which is why
    /// `start_period` exists.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timeout: Option<String>,

    /// Time between probes (default 500ms).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub interval: Option<String>,

    /// How long one probe may take before it counts as failed.
    ///
    /// Defaults to the start period, which is what fed did when `timeout`
    /// was the only knob — a single value served as both. Set it explicitly
    /// on any service with a long start period, or one hung probe will sit
    /// there for the whole of it.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub probe_timeout: Option<String>,

    /// Consecutive failed probes before a service that had become healthy is
    /// marked unhealthy (default 3).
    ///
    /// Applies only after the service has started: during startup the start
    /// period is already the tolerance for "not up yet".
    #[serde(skip_serializing_if = "Option::is_none")]
    pub retries: Option<u32>,
}

impl HealthCheckTiming {
    /// The configured start period, under either spelling.
    fn configured_start_period(&self) -> Option<&str> {
        self.start_period.as_deref().or(self.timeout.as_deref())
    }
}

impl HealthCheck {
    /// Get the type of health check.
    pub fn health_check_type(&self) -> HealthCheckType {
        match self {
            HealthCheck::HttpGet { .. } => HealthCheckType::Http,
            HealthCheck::CommandMap { .. } | HealthCheck::Command(_) => HealthCheckType::Command,
        }
    }

    /// Get the command for command-based health checks.
    pub fn get_command(&self) -> Option<&str> {
        match self {
            HealthCheck::CommandMap { command, .. } => Some(command),
            HealthCheck::Command(cmd) => Some(cmd),
            _ => None,
        }
    }

    /// Get the URL for HTTP health checks.
    pub fn get_http_url(&self) -> Option<&str> {
        match self {
            HealthCheck::HttpGet { http_get, .. } => Some(http_get),
            _ => None,
        }
    }

    /// The timing block, or an empty one for the bare-string form.
    pub fn timing(&self) -> HealthCheckTiming {
        match self {
            HealthCheck::HttpGet { timing, .. } | HealthCheck::CommandMap { timing, .. } => {
                timing.clone()
            }
            HealthCheck::Command(_) => HealthCheckTiming::default(),
        }
    }

    /// The configured start period as written, under either spelling.
    ///
    /// `None` means "not configured here" — the caller falls back to the
    /// service's `healthcheck_start_period` and then to the 5s default. See
    /// [`crate::config::Service::effective_start_period`].
    pub fn configured_start_period(&self) -> Option<&str> {
        match self {
            HealthCheck::HttpGet { timing, .. } | HealthCheck::CommandMap { timing, .. } => {
                timing.configured_start_period()
            }
            HealthCheck::Command(_) => None,
        }
    }

    /// Both spellings of the start period, for the "don't write both" check.
    pub fn both_start_period_spellings(&self) -> Option<(&str, &str)> {
        let timing = match self {
            HealthCheck::HttpGet { timing, .. } | HealthCheck::CommandMap { timing, .. } => timing,
            HealthCheck::Command(_) => return None,
        };
        match (&timing.start_period, &timing.timeout) {
            (Some(start_period), Some(timeout)) => Some((start_period, timeout)),
            _ => None,
        }
    }

    /// Total wait for the first successful check, defaulting to 5 seconds.
    ///
    /// Prefer [`crate::config::Service::effective_start_period`], which also
    /// consults the service-level `healthcheck_start_period` that a
    /// `defaults:` block sets.
    pub fn get_start_period(&self) -> Duration {
        self.configured_start_period()
            .and_then(parse_duration_string)
            .unwrap_or(DEFAULT_HEALTH_CHECK_TIMEOUT)
    }

    /// Time between probes, defaulting to 500ms.
    pub fn get_interval(&self) -> Duration {
        self.timing()
            .interval
            .as_deref()
            .and_then(parse_duration_string)
            .unwrap_or(DEFAULT_HEALTH_CHECK_INTERVAL)
    }

    /// How long a single probe may take.
    ///
    /// Defaults to `start_period`: before this field existed, one `timeout`
    /// value served as both the total wait and the per-probe cap, and
    /// silently shortening it here would turn a slow-but-working healthcheck
    /// into one that never passes.
    pub fn get_probe_timeout(&self, start_period: Duration) -> Duration {
        self.timing()
            .probe_timeout
            .as_deref()
            .and_then(parse_duration_string)
            .unwrap_or(start_period)
    }

    /// Consecutive failures that mark a healthy service unhealthy.
    ///
    /// Validation rejects zero; clamp defensively for programmatic callers.
    pub fn get_retries(&self) -> u32 {
        self.timing()
            .retries
            .unwrap_or(DEFAULT_HEALTH_CHECK_RETRIES)
            .max(1)
    }
}

/// Type of health check configured.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HealthCheckType {
    Http,
    Command,
    None,
}

/// Restart policy for failed services.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
#[derive(Default)]
pub enum RestartPolicy {
    /// Never restart on failure
    #[default]
    No,
    /// Always restart on failure
    Always,
    /// Restart on failure with specified max retries.
    /// Canonical YAML tag: `!on_failure`. `!onfailure` is a legacy spelling, still accepted.
    #[serde(rename = "on_failure", alias = "onfailure")]
    OnFailure { max_retries: Option<u32> },
}

/// Circuit breaker configuration for crash loop detection.
///
/// The circuit breaker pattern prevents services from thrashing in crash loops
/// by temporarily disabling restart attempts when a service fails repeatedly.
///
/// # States
///
/// - **Closed** (normal): Restarts are allowed
/// - **Open** (tripped): Restarts are blocked after detecting a crash loop
/// - **Half-open**: After cooldown, one restart attempt is allowed
///
/// # Example
///
/// ```yaml
/// services:
///   api:
///     process: "node server.js"
///     restart: always
///     circuit_breaker:
///       restart_threshold: 5
///       window_secs: 60
///       cooldown_secs: 300
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CircuitBreakerConfig {
    /// Number of restarts within the time window to trigger the circuit breaker.
    ///
    /// When a service restarts this many times within `window_secs`, the circuit
    /// breaker opens and blocks further restart attempts.
    ///
    /// Default: 5
    #[serde(default = "default_restart_threshold")]
    pub restart_threshold: u32,

    /// Time window in seconds for counting restarts.
    ///
    /// Only restarts within this rolling window are counted toward the threshold.
    ///
    /// Default: 60 (1 minute)
    #[serde(default = "default_window_secs")]
    pub window_secs: u64,

    /// Cooldown period in seconds before allowing retry.
    ///
    /// After the circuit breaker opens, it remains open for this duration.
    /// After cooldown, the circuit enters "half-open" state and allows one
    /// restart attempt. If successful, the circuit closes; if not, it reopens.
    ///
    /// Default: 300 (5 minutes)
    #[serde(default = "default_cooldown_secs")]
    pub cooldown_secs: u64,
}

fn default_restart_threshold() -> u32 {
    5
}

fn default_window_secs() -> u64 {
    60
}

fn default_cooldown_secs() -> u64 {
    300
}

impl Default for CircuitBreakerConfig {
    fn default() -> Self {
        Self {
            restart_threshold: default_restart_threshold(),
            window_secs: default_window_secs(),
            cooldown_secs: default_cooldown_secs(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_http_healthcheck_with_timeout() {
        let yaml = r#"
http_get: "http://localhost:8080/health"
timeout: "30s"
"#;
        let health: HealthCheck = serde_yaml::from_str(yaml).unwrap();

        assert_eq!(health.health_check_type(), HealthCheckType::Http);
        assert_eq!(health.get_http_url(), Some("http://localhost:8080/health"));
        assert_eq!(health.get_start_period(), Duration::from_secs(30));
    }

    #[test]
    fn test_http_healthcheck_without_timeout() {
        let yaml = r#"
http_get: "http://localhost:8080/health"
"#;
        let health: HealthCheck = serde_yaml::from_str(yaml).unwrap();

        assert_eq!(health.health_check_type(), HealthCheckType::Http);
        assert_eq!(health.get_start_period(), Duration::from_secs(5)); // Default
    }

    // The legacy camelCase spelling must keep working through the untagged
    // enum: serde buffers the mapping and matches variant fields including
    // aliases, so `httpGet` still selects the HttpGet variant.
    #[test]
    fn test_http_healthcheck_legacy_camelcase_spelling() {
        let yaml = r#"
httpGet: "http://localhost:8080/health"
timeout: "30s"
"#;
        let health: HealthCheck = serde_yaml::from_str(yaml).unwrap();

        assert_eq!(health.health_check_type(), HealthCheckType::Http);
        assert_eq!(health.get_http_url(), Some("http://localhost:8080/health"));
        assert_eq!(health.get_start_period(), Duration::from_secs(30));
    }

    #[test]
    fn test_command_healthcheck_with_timeout() {
        let yaml = r#"
command: "curl -f http://localhost:3000"
timeout: "10s"
"#;
        let health: HealthCheck = serde_yaml::from_str(yaml).unwrap();

        assert_eq!(health.health_check_type(), HealthCheckType::Command);
        assert_eq!(health.get_command(), Some("curl -f http://localhost:3000"));
        assert_eq!(health.get_start_period(), Duration::from_secs(10));
    }

    #[test]
    fn test_command_healthcheck_without_timeout() {
        let yaml = r#"
command: "curl -f http://localhost:3000"
"#;
        let health: HealthCheck = serde_yaml::from_str(yaml).unwrap();

        assert_eq!(health.health_check_type(), HealthCheckType::Command);
        assert_eq!(health.get_start_period(), Duration::from_secs(5)); // Default
    }

    #[test]
    fn test_simple_command_healthcheck() {
        // Simple string format - no timeout configuration possible
        let yaml = r#""curl -f http://localhost:3000""#;
        let health: HealthCheck = serde_yaml::from_str(yaml).unwrap();

        assert_eq!(health.health_check_type(), HealthCheckType::Command);
        assert_eq!(health.get_command(), Some("curl -f http://localhost:3000"));
        assert_eq!(health.get_start_period(), Duration::from_secs(5)); // Default
    }

    #[test]
    fn test_timeout_minutes() {
        let yaml = r#"
http_get: "http://localhost:8080/health"
timeout: "2m"
"#;
        let health: HealthCheck = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(health.get_start_period(), Duration::from_secs(120));
    }

    #[test]
    fn test_timeout_milliseconds() {
        let yaml = r#"
http_get: "http://localhost:8080/health"
timeout: "500ms"
"#;
        let health: HealthCheck = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(health.get_start_period(), Duration::from_millis(500));
    }

    #[test]
    fn test_invalid_timeout_uses_default() {
        let yaml = r#"
http_get: "http://localhost:8080/health"
timeout: "invalid"
"#;
        let health: HealthCheck = serde_yaml::from_str(yaml).unwrap();
        // Invalid timeout should fall back to default
        assert_eq!(health.get_start_period(), Duration::from_secs(5));
    }

    #[test]
    fn test_http_healthcheck_serialization_without_timeout() {
        let health = HealthCheck::HttpGet {
            http_get: "http://localhost:8080/health".to_string(),
            timing: HealthCheckTiming::default(),
        };
        let yaml = serde_yaml::to_string(&health).unwrap();
        // timeout should be skipped when None
        assert!(!yaml.contains("timeout"));
        assert!(yaml.contains("http_get"));
        assert!(!yaml.contains("httpGet"));
    }

    #[test]
    fn test_restart_policy_canonical_on_failure_tag() {
        let yaml = "!on_failure\nmax_retries: 3\n";
        let policy: RestartPolicy = serde_yaml::from_str(yaml).unwrap();
        assert!(matches!(
            policy,
            RestartPolicy::OnFailure {
                max_retries: Some(3)
            }
        ));
    }

    #[test]
    fn test_restart_policy_legacy_onfailure_tag_still_accepted() {
        let yaml = "!onfailure\nmax_retries: 3\n";
        let policy: RestartPolicy = serde_yaml::from_str(yaml).unwrap();
        assert!(matches!(
            policy,
            RestartPolicy::OnFailure {
                max_retries: Some(3)
            }
        ));
    }

    #[test]
    fn test_restart_policy_serializes_canonical_tag() {
        let yaml = serde_yaml::to_string(&RestartPolicy::OnFailure {
            max_retries: Some(3),
        })
        .unwrap();
        assert!(yaml.contains("on_failure"), "got: {yaml}");
        assert!(!yaml.contains("!onfailure"), "got: {yaml}");
    }

    #[test]
    fn test_http_healthcheck_serialization_with_timeout() {
        let health = HealthCheck::HttpGet {
            http_get: "http://localhost:8080/health".to_string(),
            timing: HealthCheckTiming {
                timeout: Some("30s".to_string()),
                ..Default::default()
            },
        };
        let yaml = serde_yaml::to_string(&health).unwrap();
        assert!(yaml.contains("timeout"));
        assert!(yaml.contains("30s"));
    }

    // Circuit breaker tests

    #[test]
    fn test_circuit_breaker_defaults() {
        let cb = CircuitBreakerConfig::default();
        assert_eq!(cb.restart_threshold, 5);
        assert_eq!(cb.window_secs, 60);
        assert_eq!(cb.cooldown_secs, 300);
    }

    #[test]
    fn test_circuit_breaker_from_yaml_with_defaults() {
        let yaml = r#"{}"#;
        let cb: CircuitBreakerConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(cb.restart_threshold, 5);
        assert_eq!(cb.window_secs, 60);
        assert_eq!(cb.cooldown_secs, 300);
    }

    #[test]
    fn test_circuit_breaker_from_yaml_custom_values() {
        let yaml = r#"
restart_threshold: 3
window_secs: 30
cooldown_secs: 600
"#;
        let cb: CircuitBreakerConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(cb.restart_threshold, 3);
        assert_eq!(cb.window_secs, 30);
        assert_eq!(cb.cooldown_secs, 600);
    }

    #[test]
    fn test_circuit_breaker_partial_yaml() {
        // Only specify some fields, others use defaults
        let yaml = r#"
restart_threshold: 10
"#;
        let cb: CircuitBreakerConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(cb.restart_threshold, 10);
        assert_eq!(cb.window_secs, 60); // default
        assert_eq!(cb.cooldown_secs, 300); // default
    }

    #[test]
    fn test_circuit_breaker_serialization() {
        let cb = CircuitBreakerConfig {
            restart_threshold: 7,
            window_secs: 120,
            cooldown_secs: 600,
        };
        let yaml = serde_yaml::to_string(&cb).unwrap();
        assert!(yaml.contains("restart_threshold: 7"));
        assert!(yaml.contains("window_secs: 120"));
        assert!(yaml.contains("cooldown_secs: 600"));
    }
}
