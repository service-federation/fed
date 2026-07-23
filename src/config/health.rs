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
    /// HTTP GET health check with optional timeout.
    /// Canonical key: `http_get`. `httpGet` is a legacy spelling, still accepted.
    HttpGet {
        #[serde(alias = "httpGet")]
        http_get: String,
        /// Timeout duration (e.g., "5s", "30s", "1m")
        #[serde(skip_serializing_if = "Option::is_none")]
        timeout: Option<String>,
    },
    /// Command health check with explicit field and optional timeout
    CommandMap {
        command: String,
        /// Timeout duration (e.g., "5s", "30s", "1m")
        #[serde(skip_serializing_if = "Option::is_none")]
        timeout: Option<String>,
    },
    /// Simple command string health check (no timeout config, uses default)
    Command(String),
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

    /// Get the configured timeout, or the default (5 seconds).
    ///
    /// Parses duration strings like "5s", "30s", "1m", "500ms".
    /// Returns the default timeout if no timeout is configured or if parsing fails.
    pub fn get_timeout(&self) -> Duration {
        let timeout_str = match self {
            HealthCheck::HttpGet { timeout, .. } => timeout.as_deref(),
            HealthCheck::CommandMap { timeout, .. } => timeout.as_deref(),
            HealthCheck::Command(_) => None,
        };

        timeout_str
            .and_then(parse_duration_string)
            .unwrap_or(DEFAULT_HEALTH_CHECK_TIMEOUT)
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
        assert_eq!(health.get_timeout(), Duration::from_secs(30));
    }

    #[test]
    fn test_http_healthcheck_without_timeout() {
        let yaml = r#"
http_get: "http://localhost:8080/health"
"#;
        let health: HealthCheck = serde_yaml::from_str(yaml).unwrap();

        assert_eq!(health.health_check_type(), HealthCheckType::Http);
        assert_eq!(health.get_timeout(), Duration::from_secs(5)); // Default
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
        assert_eq!(health.get_timeout(), Duration::from_secs(30));
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
        assert_eq!(health.get_timeout(), Duration::from_secs(10));
    }

    #[test]
    fn test_command_healthcheck_without_timeout() {
        let yaml = r#"
command: "curl -f http://localhost:3000"
"#;
        let health: HealthCheck = serde_yaml::from_str(yaml).unwrap();

        assert_eq!(health.health_check_type(), HealthCheckType::Command);
        assert_eq!(health.get_timeout(), Duration::from_secs(5)); // Default
    }

    #[test]
    fn test_simple_command_healthcheck() {
        // Simple string format - no timeout configuration possible
        let yaml = r#""curl -f http://localhost:3000""#;
        let health: HealthCheck = serde_yaml::from_str(yaml).unwrap();

        assert_eq!(health.health_check_type(), HealthCheckType::Command);
        assert_eq!(health.get_command(), Some("curl -f http://localhost:3000"));
        assert_eq!(health.get_timeout(), Duration::from_secs(5)); // Default
    }

    #[test]
    fn test_timeout_minutes() {
        let yaml = r#"
http_get: "http://localhost:8080/health"
timeout: "2m"
"#;
        let health: HealthCheck = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(health.get_timeout(), Duration::from_secs(120));
    }

    #[test]
    fn test_timeout_milliseconds() {
        let yaml = r#"
http_get: "http://localhost:8080/health"
timeout: "500ms"
"#;
        let health: HealthCheck = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(health.get_timeout(), Duration::from_millis(500));
    }

    #[test]
    fn test_invalid_timeout_uses_default() {
        let yaml = r#"
http_get: "http://localhost:8080/health"
timeout: "invalid"
"#;
        let health: HealthCheck = serde_yaml::from_str(yaml).unwrap();
        // Invalid timeout should fall back to default
        assert_eq!(health.get_timeout(), Duration::from_secs(5));
    }

    #[test]
    fn test_http_healthcheck_serialization_without_timeout() {
        let health = HealthCheck::HttpGet {
            http_get: "http://localhost:8080/health".to_string(),
            timeout: None,
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
            timeout: Some("30s".to_string()),
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
