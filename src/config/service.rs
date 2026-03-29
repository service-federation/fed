//! Service configuration types.
//!
//! This module contains the [`Service`] struct and related types for
//! configuring services in the federation config.

use super::{
    parse_duration_string, CircuitBreakerConfig, DependsOn, HealthCheck, ResourceLimits,
    RestartPolicy,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Build configuration — either a shell command or a Docker image build.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum BuildConfig {
    /// Shell command (existing behavior): `build: "npm run build"`
    Command(String),
    /// Docker image build: `build: { image: "my-image", ... }`
    DockerBuild(DockerBuildConfig),
}

/// Docker image build configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DockerBuildConfig {
    /// Target image name (required)
    pub image: String,
    /// Path to Dockerfile relative to cwd (default: "Dockerfile")
    #[serde(default = "default_dockerfile")]
    pub dockerfile: String,
    /// Build arguments passed as --build-arg
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub args: HashMap<String, String>,
}

fn default_dockerfile() -> String {
    "Dockerfile".to_string()
}

/// Result of a Docker image build, used for --json output.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DockerBuildResult {
    pub service: String,
    pub image: String,
    pub tag: String,
}

/// Service configuration for a single service in the federation.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Service {
    // Package extension
    /// Reference to a package service to extend from (format: "package-alias.service-name")
    #[serde(skip_serializing_if = "Option::is_none")]
    pub extends: Option<String>,

    // Process-based service
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cwd: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub install: Option<String>,

    /// Command to run after dependencies are healthy but before the service starts.
    /// Runs once (like install). Use for database migrations, schema setup, etc.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub migrate: Option<String>,

    /// Command to clean up service artifacts (e.g., "rm -rf node_modules")
    #[serde(skip_serializing_if = "Option::is_none")]
    pub clean: Option<String>,

    /// Command to build the service (e.g., "npm run build") or Docker image build config.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub build: Option<BuildConfig>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub process: Option<String>,

    // Docker-based service
    #[serde(skip_serializing_if = "Option::is_none")]
    pub image: Option<String>,

    /// Override the Docker CMD for image-based services.
    /// Accepts a string (split on whitespace) or an array of strings.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub command: Option<DockerCommand>,

    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub volumes: Vec<String>,

    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub ports: Vec<String>,

    // External dependency service
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dependency: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub service: Option<String>,

    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub parameters: HashMap<String, String>,

    // Gradle task service
    #[serde(rename = "gradleTask", skip_serializing_if = "Option::is_none")]
    pub gradle_task: Option<String>,

    // Docker Compose service
    #[serde(rename = "composeFile", skip_serializing_if = "Option::is_none")]
    pub compose_file: Option<String>,

    #[serde(rename = "composeService", skip_serializing_if = "Option::is_none")]
    pub compose_service: Option<String>,

    // Common fields
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub environment: HashMap<String, String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub healthcheck: Option<HealthCheck>,

    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub depends_on: Vec<DependsOn>,

    // Restart policy
    #[serde(skip_serializing_if = "Option::is_none")]
    pub restart: Option<RestartPolicy>,

    // Service exposure for external consumption
    #[serde(default, skip_serializing_if = "is_false")]
    pub expose: bool,

    // Service profiles for conditional startup
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub profiles: Vec<String>,

    // Service tags for flexible grouping (e.g., "backend", "critical", "async")
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tags: Vec<String>,

    // Watch paths for auto-restart on file changes
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub watch: Vec<String>,

    // Resource limits for controlling service resource usage
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resources: Option<ResourceLimits>,

    /// Grace period for graceful shutdown before SIGKILL (default: 10s)
    /// Format: duration string like "10s", "30s", "1m", "500ms"
    #[serde(skip_serializing_if = "Option::is_none")]
    pub grace_period: Option<String>,

    /// Circuit breaker configuration for crash loop detection.
    ///
    /// When enabled, the circuit breaker monitors restart frequency and
    /// temporarily disables restarts if a service is crash looping.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub circuit_breaker: Option<CircuitBreakerConfig>,

    /// Template string printed after all services start (supports `{{PARAM}}` syntax).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub startup_message: Option<String>,
}

fn is_false(b: &bool) -> bool {
    !b
}

/// Docker CMD override. Accepts either a string (split on whitespace)
/// or an array of strings for exact argument control.
///
/// ```yaml
/// # String form (split on whitespace):
/// command: "--jetstream --store_dir /data"
///
/// # Array form (exact arguments):
/// command: ["--jetstream", "--store_dir", "/data"]
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum DockerCommand {
    String(String),
    List(Vec<String>),
}

impl DockerCommand {
    /// Convert to a list of arguments for `docker run`.
    pub fn to_args(&self) -> Vec<String> {
        match self {
            DockerCommand::String(s) => s.split_whitespace().map(String::from).collect(),
            DockerCommand::List(v) => v.clone(),
        }
    }
}

/// Types of services supported by the federation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum ServiceType {
    Process,
    Docker,
    External,
    GradleTask,
    DockerCompose,
    Undefined,
}

impl std::fmt::Display for ServiceType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ServiceType::Process => write!(f, "process"),
            ServiceType::Docker => write!(f, "docker"),
            ServiceType::External => write!(f, "external"),
            ServiceType::GradleTask => write!(f, "gradle"),
            ServiceType::DockerCompose => write!(f, "docker-compose"),
            ServiceType::Undefined => write!(f, "undefined"),
        }
    }
}

impl std::str::FromStr for ServiceType {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "process" => Ok(ServiceType::Process),
            "docker" => Ok(ServiceType::Docker),
            "external" => Ok(ServiceType::External),
            "gradle" | "gradletask" | "gradle_task" => Ok(ServiceType::GradleTask),
            "docker-compose" | "dockercompose" | "docker_compose" => Ok(ServiceType::DockerCompose),
            "undefined" => Ok(ServiceType::Undefined),
            _ => Err(format!("Unknown service type: {}", s)),
        }
    }
}

impl Service {
    /// Determine the type of service based on configured fields.
    pub fn service_type(&self) -> ServiceType {
        if self.compose_file.is_some() && self.compose_service.is_some() {
            ServiceType::DockerCompose
        } else if self.process.is_some() {
            ServiceType::Process
        } else if self.image.is_some() {
            ServiceType::Docker
        } else if self.dependency.is_some() && self.service.is_some() {
            ServiceType::External
        } else if self.gradle_task.is_some() {
            ServiceType::GradleTask
        } else {
            ServiceType::Undefined
        }
    }

    /// Get grace period duration, defaulting to 10 seconds.
    ///
    /// Parses duration strings like "10s", "30s", "1m", "500ms".
    /// Returns 10 seconds if the format is invalid or not specified.
    pub fn get_grace_period(&self) -> std::time::Duration {
        self.grace_period
            .as_ref()
            .and_then(|s| parse_duration_string(s))
            .unwrap_or(std::time::Duration::from_secs(10))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build_config_deserialize_command_string() {
        let yaml = r#"build: "npm run build""#;
        #[derive(Deserialize)]
        struct Wrapper {
            build: BuildConfig,
        }
        let w: Wrapper = serde_yaml::from_str(yaml).unwrap();
        assert!(matches!(w.build, BuildConfig::Command(ref s) if s == "npm run build"));
    }

    #[test]
    fn test_build_config_deserialize_docker_build_minimal() {
        let yaml = r#"
build:
  image: my-app
"#;
        #[derive(Deserialize)]
        struct Wrapper {
            build: BuildConfig,
        }
        let w: Wrapper = serde_yaml::from_str(yaml).unwrap();
        match w.build {
            BuildConfig::DockerBuild(ref cfg) => {
                assert_eq!(cfg.image, "my-app");
                assert_eq!(cfg.dockerfile, "Dockerfile");
                assert!(cfg.args.is_empty());
            }
            _ => panic!("Expected DockerBuild variant"),
        }
    }

    #[test]
    fn test_build_config_deserialize_docker_build_full() {
        let yaml = r#"
build:
  image: my-app
  dockerfile: Dockerfile.prod
  args:
    NODE_ENV: production
    VERSION: "1.0"
"#;
        #[derive(Deserialize)]
        struct Wrapper {
            build: BuildConfig,
        }
        let w: Wrapper = serde_yaml::from_str(yaml).unwrap();
        match w.build {
            BuildConfig::DockerBuild(ref cfg) => {
                assert_eq!(cfg.image, "my-app");
                assert_eq!(cfg.dockerfile, "Dockerfile.prod");
                assert_eq!(cfg.args.get("NODE_ENV").unwrap(), "production");
                assert_eq!(cfg.args.get("VERSION").unwrap(), "1.0");
            }
            _ => panic!("Expected DockerBuild variant"),
        }
    }

    #[test]
    fn test_service_with_docker_build() {
        let yaml = r#"
cwd: ./apps/web
build:
  image: my-web-app
  dockerfile: Dockerfile.dev
environment:
  NODE_ENV: development
"#;
        let svc: Service = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(svc.cwd.as_deref(), Some("./apps/web"));
        assert!(svc.build.is_some());
        match svc.build.as_ref().unwrap() {
            BuildConfig::DockerBuild(cfg) => {
                assert_eq!(cfg.image, "my-web-app");
                assert_eq!(cfg.dockerfile, "Dockerfile.dev");
            }
            _ => panic!("Expected DockerBuild variant"),
        }
    }

    #[test]
    fn test_build_config_serialize_roundtrip_command() {
        let config = BuildConfig::Command("cargo build".to_string());
        let yaml = serde_yaml::to_string(&config).unwrap();
        let deserialized: BuildConfig = serde_yaml::from_str(&yaml).unwrap();
        assert!(matches!(deserialized, BuildConfig::Command(ref s) if s == "cargo build"));
    }

    #[test]
    fn test_build_config_serialize_roundtrip_docker() {
        let mut args = HashMap::new();
        args.insert("KEY".to_string(), "value".to_string());
        let config = BuildConfig::DockerBuild(DockerBuildConfig {
            image: "test-image".to_string(),
            dockerfile: "Dockerfile.test".to_string(),
            args,
        });
        let yaml = serde_yaml::to_string(&config).unwrap();
        let deserialized: BuildConfig = serde_yaml::from_str(&yaml).unwrap();
        match deserialized {
            BuildConfig::DockerBuild(cfg) => {
                assert_eq!(cfg.image, "test-image");
                assert_eq!(cfg.dockerfile, "Dockerfile.test");
                assert_eq!(cfg.args.get("KEY").unwrap(), "value");
            }
            _ => panic!("Expected DockerBuild variant"),
        }
    }

    #[test]
    fn test_service_type_display_roundtrip() {
        let variants = [
            ServiceType::Process,
            ServiceType::Docker,
            ServiceType::External,
            ServiceType::GradleTask,
            ServiceType::DockerCompose,
            ServiceType::Undefined,
        ];
        for variant in &variants {
            let display = variant.to_string();
            let parsed: ServiceType = display.parse().unwrap();
            assert_eq!(*variant, parsed, "roundtrip failed for {}", display);
        }
    }

    #[test]
    fn test_service_type_from_str_case_insensitive() {
        assert_eq!(
            "PROCESS".parse::<ServiceType>().unwrap(),
            ServiceType::Process
        );
        assert_eq!(
            "Docker".parse::<ServiceType>().unwrap(),
            ServiceType::Docker
        );
        assert_eq!(
            "EXTERNAL".parse::<ServiceType>().unwrap(),
            ServiceType::External
        );
        assert_eq!(
            "Gradle".parse::<ServiceType>().unwrap(),
            ServiceType::GradleTask
        );
        assert_eq!(
            "Docker-Compose".parse::<ServiceType>().unwrap(),
            ServiceType::DockerCompose
        );
        assert_eq!(
            "UNDEFINED".parse::<ServiceType>().unwrap(),
            ServiceType::Undefined
        );
    }

    #[test]
    fn test_service_type_from_str_backwards_compat() {
        // Old Debug-formatted values should still parse
        assert_eq!(
            "GradleTask".parse::<ServiceType>().unwrap(),
            ServiceType::GradleTask
        );
        assert_eq!(
            "gradle_task".parse::<ServiceType>().unwrap(),
            ServiceType::GradleTask
        );
        assert_eq!(
            "DockerCompose".parse::<ServiceType>().unwrap(),
            ServiceType::DockerCompose
        );
        assert_eq!(
            "docker_compose".parse::<ServiceType>().unwrap(),
            ServiceType::DockerCompose
        );
    }

    #[test]
    fn test_service_type_from_str_unknown() {
        assert!("bogus".parse::<ServiceType>().is_err());
    }

    #[test]
    fn test_service_migrate_field_deserializes() {
        let yaml = r#"
process: npm start
install: npm ci
migrate: npx prisma migrate deploy
"#;
        let svc: Service = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(svc.install.as_deref(), Some("npm ci"));
        assert_eq!(svc.migrate.as_deref(), Some("npx prisma migrate deploy"));
    }

    #[test]
    fn test_service_migrate_field_optional() {
        let yaml = r#"
process: npm start
install: npm ci
"#;
        let svc: Service = serde_yaml::from_str(yaml).unwrap();
        assert!(svc.migrate.is_none());
    }

    #[test]
    fn test_service_migrate_field_roundtrip() {
        let yaml = r#"
process: npm start
migrate: ./migrate.sh
"#;
        let svc: Service = serde_yaml::from_str(yaml).unwrap();
        let serialized = serde_yaml::to_string(&svc).unwrap();
        let deserialized: Service = serde_yaml::from_str(&serialized).unwrap();
        assert_eq!(deserialized.migrate.as_deref(), Some("./migrate.sh"));
    }

    #[test]
    fn test_docker_command_string() {
        let yaml = r#"
image: nats:2.10
command: "--jetstream --store_dir /data"
"#;
        let svc: Service = serde_yaml::from_str(yaml).unwrap();
        let cmd = svc.command.unwrap();
        assert_eq!(cmd.to_args(), vec!["--jetstream", "--store_dir", "/data"]);
    }

    #[test]
    fn test_docker_command_list() {
        let yaml = r#"
image: nats:2.10
command: ["--jetstream", "--store_dir", "/data"]
"#;
        let svc: Service = serde_yaml::from_str(yaml).unwrap();
        let cmd = svc.command.unwrap();
        assert_eq!(cmd.to_args(), vec!["--jetstream", "--store_dir", "/data"]);
    }

    #[test]
    fn test_docker_command_omitted() {
        let yaml = r#"
image: redis:7
"#;
        let svc: Service = serde_yaml::from_str(yaml).unwrap();
        assert!(svc.command.is_none());
    }
}
