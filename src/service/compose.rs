use super::{BaseService, ServiceManager, Status};
use crate::config::Service as ServiceConfig;
use crate::error::{Error, Result};
use async_trait::async_trait;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::OnceCell;

const LOG_CACHE_TTL: Duration = Duration::from_secs(1);

/// Docker Compose command type (v1 or v2)
#[derive(Debug, Clone, Copy)]
enum ComposeCommand {
    V2, // docker compose
    V1, // docker-compose
}

/// Global cache for compose command detection
static COMPOSE_COMMAND: OnceCell<ComposeCommand> = OnceCell::const_new();

impl ComposeCommand {
    /// Detect which docker compose command is available
    async fn detect() -> Result<ComposeCommand> {
        // Try docker compose (v2) first
        let v2_check = tokio::process::Command::new("docker")
            .args(["compose", "version"])
            .output()
            .await;

        if let Ok(output) = v2_check {
            if output.status.success() {
                return Ok(ComposeCommand::V2);
            }
        }

        // Try docker-compose (v1) as fallback
        let v1_check = tokio::process::Command::new("docker-compose")
            .args(["--version"])
            .output()
            .await;

        if let Ok(output) = v1_check {
            if output.status.success() {
                return Ok(ComposeCommand::V1);
            }
        }

        Err(Error::Config(
            "Neither 'docker compose' (v2) nor 'docker-compose' (v1) found. Please install Docker Compose.".to_string(),
        ))
    }

    /// Get the compose command (cached)
    async fn get() -> Result<ComposeCommand> {
        COMPOSE_COMMAND
            .get_or_try_init(|| async { Self::detect().await })
            .await
            .copied()
    }

    /// Get command and args for running compose
    fn command_and_args(&self) -> (&str, Vec<&str>) {
        match self {
            ComposeCommand::V2 => ("docker", vec!["compose"]),
            ComposeCommand::V1 => ("docker-compose", vec![]),
        }
    }
}

/// Docker Compose service manager
pub struct DockerComposeService {
    name: String,
    base: Arc<RwLock<BaseService>>,
    compose_file: PathBuf,
    compose_service: String,
    project_name: String,
    /// Cached logs to avoid spawning compose subprocess on every call
    log_cache: Arc<tokio::sync::RwLock<(Vec<String>, Instant)>>,
}

impl DockerComposeService {
    pub fn new(
        name: String,
        config: ServiceConfig,
        environment: HashMap<String, String>,
        work_dir: String,
    ) -> Result<Self> {
        let compose_file = config
            .compose_file
            .as_ref()
            .ok_or_else(|| Error::DockerCompose("No composeFile specified".to_string()))?;

        let compose_service = config
            .compose_service
            .as_ref()
            .ok_or_else(|| Error::DockerCompose("No composeService specified".to_string()))?
            .clone();

        // Resolve compose file path relative to work_dir
        let compose_file_path = if PathBuf::from(compose_file).is_absolute() {
            PathBuf::from(compose_file)
        } else {
            PathBuf::from(&work_dir).join(compose_file)
        };

        // Validate compose file exists and is readable
        if !compose_file_path.exists() {
            return Err(Error::Filesystem(format!(
                "Compose file does not exist: {}",
                compose_file_path.display()
            )));
        }

        if !compose_file_path.is_file() {
            return Err(Error::Filesystem(format!(
                "Compose file path is not a file: {}",
                compose_file_path.display()
            )));
        }

        // Validate we can read the file
        if let Err(e) = std::fs::metadata(&compose_file_path) {
            return Err(Error::Filesystem(format!(
                "Cannot access compose file {}: {}",
                compose_file_path.display(),
                e
            )));
        }

        // Generate project name based on compose file path
        // This ensures services from the same compose file share a project
        let project_name = Self::get_project_name(&compose_file_path);

        Ok(Self {
            name: name.clone(),
            base: Arc::new(RwLock::new(BaseService::new(name, environment, work_dir))),
            compose_file: compose_file_path,
            compose_service,
            project_name,
            // Initialize with empty cache that's already expired
            log_cache: Arc::new(tokio::sync::RwLock::new((
                Vec::new(),
                Instant::now() - LOG_CACHE_TTL - Duration::from_secs(1),
            ))),
        })
    }

    /// Generate a short hash of the compose file path for project naming.
    ///
    /// Uses FNV-1a (32-bit) for deterministic, stable hashing across Rust versions.
    /// The path is canonicalized first so that relative and absolute paths to the
    /// same file produce the same project name.
    fn hash_path(path: &Path) -> String {
        let canonical = std::fs::canonicalize(path).unwrap_or_else(|_| path.to_path_buf());
        let bytes = canonical.as_os_str().as_encoded_bytes();
        let hash = super::fnv1a_32(bytes);
        format!("{:04x}", hash & 0xFFFF) // 4 hex chars
    }

    /// Get project name from hash of compose file path.
    ///
    /// Format: `fed-{hash}`
    fn get_project_name(compose_file_path: &Path) -> String {
        format!("fed-{}", Self::hash_path(compose_file_path))
    }

    /// Build base compose command with file and project
    async fn build_base_command(&self) -> Result<tokio::process::Command> {
        let compose_cmd = ComposeCommand::get().await?;
        let (cmd, base_args) = compose_cmd.command_and_args();

        let mut command = tokio::process::Command::new(cmd);
        command.args(base_args);

        // Add compose file and project name
        command.args([
            "-f",
            self.compose_file
                .to_str()
                .ok_or_else(|| Error::DockerCompose("Invalid compose file path".to_string()))?,
            "-p",
            &self.project_name,
        ]);

        Ok(command)
    }

    /// Get environment variables to pass to compose command
    /// Docker compose doesn't support -e flag for `up`, so we pass via command environment
    fn get_environment_vars(&self) -> HashMap<String, String> {
        let base = self.base.read();
        base.environment.clone()
    }
}

#[async_trait]
impl ServiceManager for DockerComposeService {
    async fn start(&mut self) -> Result<()> {
        {
            let mut base = self.base.write();
            if base.status == Status::Running || base.status == Status::Healthy {
                return Ok(());
            }
            base.set_status(Status::Starting);
        }

        // Clean up any orphaned containers/networks from previous runs
        // This handles cases where tests failed or were interrupted
        {
            let mut cleanup_cmd = self.build_base_command().await?;
            cleanup_cmd.args(["down", "--remove-orphans"]);
            let _ = cleanup_cmd.output().await; // Ignore errors, might not exist
        }

        let mut command = self.build_base_command().await?;

        // Add up command with detached mode
        command.arg("up").arg("-d");

        // Add environment variables via command environment (not -e flag)
        let env_vars = self.get_environment_vars();
        let workspace = self.base.read().work_dir.clone();
        command
            .envs(&env_vars)
            // Markers for the recursion check in main.rs.
            .env("FED_SPAWNED_BY_SERVICE", &self.name)
            .env("FED_SPAWNED_FROM_WORKSPACE", &workspace);

        // Add service name
        command.arg(&self.compose_service);

        // Execute command
        let service_name = self.name.clone();
        let compose_service = self.compose_service.clone();
        let output = command.output().await.map_err(|e| {
            let mut base = self.base.write();
            base.set_status(Status::Failing);
            Error::ServiceStartFailed(service_name.clone(), e.to_string())
        })?;

        if !output.status.success() {
            let error_msg = {
                let mut base = self.base.write();
                base.set_status(Status::Failing);
                let error = String::from_utf8_lossy(&output.stderr);

                // Provide more context for common errors
                if error.contains("no such service")
                    || error.contains("service") && error.contains("not found")
                {
                    format!(
                        "Service '{}' not found in compose file. Error: {}",
                        compose_service, error
                    )
                } else if error.contains("Cannot connect to the Docker daemon") {
                    format!("Docker daemon not running. Error: {}", error)
                } else if error.contains("network") {
                    format!("Network error starting service. Error: {}", error)
                } else if error.contains("port") && error.contains("already allocated") {
                    format!("Port conflict - port already in use. Error: {}", error)
                } else {
                    error.to_string()
                }
                // Lock is dropped here before await
            };

            // Clean up any partially created resources on failure
            // This prevents network leaks when compose up fails after creating networks
            let mut cleanup_cmd = self.build_base_command().await?;
            cleanup_cmd.args(["down", "--remove-orphans"]);
            let _ = cleanup_cmd.output().await; // Best effort cleanup

            return Err(Error::ServiceStartFailed(service_name, error_msg));
        }

        {
            let mut base = self.base.write();
            base.set_status(Status::Running);
        }

        Ok(())
    }

    async fn stop(&mut self) -> Result<()> {
        {
            let mut base = self.base.write();
            if base.status == Status::Stopped {
                return Ok(());
            }
            base.set_status(Status::Stopping);
        }

        // Use 'down' instead of 'stop' to properly clean up networks
        // Note: This removes the entire compose project, not just this service
        // This is intentional to prevent network resource leaks
        let mut command = self.build_base_command().await?;
        command.args(["down"]);

        let output = command.output().await;

        // Log warnings but don't fail the stop operation
        if let Ok(out) = output {
            if !out.status.success() {
                let error = String::from_utf8_lossy(&out.stderr);
                tracing::warn!(
                    "Compose down for project '{}' had warnings: {}",
                    self.project_name,
                    error
                );
            }
        }

        {
            let mut base = self.base.write();
            base.set_status(Status::Stopped);
        }

        Ok(())
    }

    async fn kill(&mut self) -> Result<()> {
        let mut command = self.build_base_command().await?;
        command.args(["kill", &self.compose_service]);

        let _ = command.output().await;

        self.stop().await
    }

    async fn health(&self) -> Result<bool> {
        let mut command = self.build_base_command().await?;
        command.args(["ps", "--format", "json", &self.compose_service]);

        let output = command.output().await?;

        if !output.status.success() {
            return Ok(false);
        }

        // Parse JSON output to check if service is running
        let stdout = String::from_utf8_lossy(&output.stdout);

        // Docker compose v2 returns JSON array, v1 may return newline-delimited JSON
        // Try parsing as JSON array first
        if let Ok(containers) = serde_json::from_str::<Vec<serde_json::Value>>(&stdout) {
            for container in containers {
                if let Some(state) = container.get("State").and_then(|s| s.as_str()) {
                    // Check if state is "running" or starts with "Up" (case-insensitive)
                    let state_lower = state.to_lowercase();
                    if state_lower == "running" || state_lower.starts_with("up") {
                        return Ok(true);
                    }
                }
            }
            return Ok(false);
        }

        // Fallback: try parsing each line as separate JSON object (newline-delimited JSON)
        for line in stdout.lines() {
            if line.trim().is_empty() {
                continue;
            }
            if let Ok(container) = serde_json::from_str::<serde_json::Value>(line) {
                if let Some(state) = container.get("State").and_then(|s| s.as_str()) {
                    let state_lower = state.to_lowercase();
                    if state_lower == "running" || state_lower.starts_with("up") {
                        return Ok(true);
                    }
                }
            }
        }

        // Last fallback: if JSON parsing completely failed, use string matching
        // This handles edge cases where compose output format is unexpected
        Ok(stdout.contains("running") || stdout.contains("Up"))
    }

    fn status(&self) -> Status {
        self.base.read().status
    }

    fn name(&self) -> &str {
        &self.name
    }

    async fn logs(&self, tail: Option<usize>) -> Result<Vec<String>> {
        // Check cache first to avoid spawning compose subprocess on every call
        {
            let cache = self.log_cache.read().await;
            if cache.1.elapsed() < LOG_CACHE_TTL {
                // Apply tail to cached logs
                if let Some(n) = tail {
                    return Ok(cache.0.iter().rev().take(n).rev().cloned().collect());
                }
                return Ok(cache.0.clone());
            }
        }

        let mut command = self.build_base_command().await?;
        command.arg("logs");

        // Always fetch a reasonable number of logs for caching
        command.args(["--tail", "200"]);
        command.arg(&self.compose_service);

        let output = command.output().await?;

        if !output.status.success() {
            return Ok(Vec::new());
        }

        let logs_str = String::from_utf8_lossy(&output.stdout);
        let combined_logs: Vec<String> = logs_str.lines().map(String::from).collect();

        // Update cache
        {
            let mut cache = self.log_cache.write().await;
            *cache = (combined_logs.clone(), Instant::now());
        }

        // Apply tail to result
        if let Some(n) = tail {
            Ok(combined_logs.into_iter().rev().take(n).rev().collect())
        } else {
            Ok(combined_logs)
        }
    }
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
}
