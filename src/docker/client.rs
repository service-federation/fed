//! Centralized Docker CLI client.
//!
//! All Docker CLI interactions go through `DockerClient`, which provides
//! consistent timeout handling, error mapping to [`DockerError`], and a single
//! point where `Command::new("docker")` is constructed.

use super::DockerError;
use std::collections::HashMap;
use std::process::Output;
use std::time::Duration;

/// Centralized client for Docker CLI operations.
///
/// Wraps all `docker` subprocess invocations with consistent timeout handling
/// and structured [`DockerError`] returns. Construct once and thread through
/// the application — the struct is cheap (zero-sized today).
#[derive(Debug, Clone)]
pub struct DockerClient;

impl DockerClient {
    pub fn new() -> Self {
        DockerClient
    }

    // ========================================================================
    // Internal helpers
    // ========================================================================

    /// Run a docker command with a timeout, returning raw Output.
    async fn run(&self, args: &[&str], timeout: Duration) -> Result<Output, DockerError> {
        let result = tokio::time::timeout(
            timeout,
            tokio::process::Command::new("docker").args(args).output(),
        )
        .await;

        let cmd_str = format!("docker {}", args.join(" "));

        match result {
            Ok(Ok(output)) => Ok(output),
            Ok(Err(e)) => Err(DockerError::exec_failed(cmd_str, e)),
            Err(_) => Err(DockerError::timeout(cmd_str, timeout)),
        }
    }

    /// Run a docker command with a timeout, returning Output only if exit 0.
    async fn run_success(&self, args: &[&str], timeout: Duration) -> Result<Output, DockerError> {
        let output = self.run(args, timeout).await?;
        if output.status.success() {
            Ok(output)
        } else {
            let cmd_str = format!("docker {}", args.join(" "));
            Err(DockerError::failed(&cmd_str, &output))
        }
    }

    /// Run a docker command synchronously, returning raw Output.
    fn run_sync(&self, args: &[&str]) -> Result<Output, DockerError> {
        let cmd_str = format!("docker {}", args.join(" "));
        std::process::Command::new("docker")
            .args(args)
            .output()
            .map_err(|e| DockerError::exec_failed(cmd_str, e))
    }

    // ========================================================================
    // Container lifecycle
    // ========================================================================

    /// Force-remove a container. Returns `Ok(())` if container doesn't exist.
    pub async fn rm_force(&self, container: &str, timeout: Duration) -> Result<(), DockerError> {
        let output = self.run(&["rm", "-f", container], timeout).await?;
        if output.status.success() {
            return Ok(());
        }
        let stderr = String::from_utf8_lossy(&output.stderr);
        if stderr.contains("No such container") {
            return Ok(());
        }
        Err(DockerError::failed("docker rm -f", &output))
    }

    /// Force-remove a container (synchronous). Returns `Ok(())` if container doesn't exist.
    pub fn rm_force_sync(&self, container: &str) -> Result<(), DockerError> {
        let output = self.run_sync(&["rm", "-f", container])?;
        if output.status.success() {
            return Ok(());
        }
        let stderr = String::from_utf8_lossy(&output.stderr);
        if stderr.contains("No such container") {
            return Ok(());
        }
        Err(DockerError::failed("docker rm -f", &output))
    }

    /// Stop a container gracefully.
    pub async fn stop(&self, container: &str, timeout: Duration) -> Result<(), DockerError> {
        let output = self.run(&["stop", container], timeout).await?;
        if output.status.success() {
            return Ok(());
        }
        Err(DockerError::failed("docker stop", &output))
    }

    /// Stop a container with a specific grace period, then remove it.
    pub async fn stop_and_remove(
        &self,
        container: &str,
        grace_secs: u32,
        timeout: Duration,
    ) -> Result<bool, DockerError> {
        let grace = grace_secs.to_string();
        let output = self
            .run(&["stop", "-t", &grace, container], timeout)
            .await?;
        let stopped = output.status.success();
        // Always try to remove, even if stop failed
        let _ = self.rm_force(container, timeout).await;
        Ok(stopped)
    }

    /// Kill a container (SIGKILL).
    pub async fn kill(&self, container: &str, timeout: Duration) -> Result<(), DockerError> {
        let output = self.run(&["kill", container], timeout).await?;
        if output.status.success() {
            return Ok(());
        }
        let stderr = String::from_utf8_lossy(&output.stderr);
        // Container already stopped or doesn't exist — not an error
        if stderr.contains("No such container") || stderr.contains("is not running") {
            return Ok(());
        }
        Err(DockerError::failed("docker kill", &output))
    }

    /// Run a container in detached mode. Returns the container ID on success.
    pub async fn run_detached(
        &self,
        args: &[String],
        timeout: Duration,
    ) -> Result<Output, DockerError> {
        // args should include everything after "docker" (e.g. ["run", "-d", ...])
        let arg_refs: Vec<&str> = args.iter().map(String::as_str).collect();
        self.run(&arg_refs, timeout).await
    }

    /// Pull a Docker image.
    pub async fn pull(&self, image: &str, timeout: Duration) -> Result<(), DockerError> {
        let output = self.run(&["pull", image], timeout).await?;
        if output.status.success() {
            return Ok(());
        }
        let stderr = String::from_utf8_lossy(&output.stderr);
        // "up to date" or "already exists" aren't real failures
        if stderr.contains("up to date") || stderr.contains("already exists") {
            return Ok(());
        }
        Err(DockerError::failed("docker pull", &output))
    }

    // ========================================================================
    // Inspection
    // ========================================================================

    /// Check if a container is running (async).
    pub async fn is_running(&self, container: &str, timeout: Duration) -> bool {
        let output = self
            .run(&["inspect", "-f", "{{.State.Running}}", container], timeout)
            .await;
        match output {
            Ok(o) if o.status.success() => String::from_utf8_lossy(&o.stdout).trim() == "true",
            _ => false,
        }
    }

    /// Check if a container is running (synchronous).
    pub fn is_running_sync(&self, container: &str) -> bool {
        match self.run_sync(&["inspect", "-f", "{{.State.Running}}", container]) {
            Ok(o) if o.status.success() => String::from_utf8_lossy(&o.stdout).trim() == "true",
            _ => false,
        }
    }

    /// Check if a container exists and is running using `docker ps -q`.
    pub async fn is_alive(&self, container_id: &str, timeout: Duration) -> bool {
        let output = self
            .run(
                &[
                    "ps",
                    "-q",
                    "--no-trunc",
                    "-f",
                    &format!("id={}", container_id),
                ],
                timeout,
            )
            .await;
        match output {
            Ok(o) => !o.stdout.is_empty(),
            Err(_) => false,
        }
    }

    /// Get port mappings for a container.
    /// Returns a map from container port (e.g. "5432/tcp") to host port (e.g. "59890").
    pub async fn inspect_ports(
        &self,
        container: &str,
        timeout: Duration,
    ) -> HashMap<String, String> {
        let output = self
            .run(
                &[
                    "inspect",
                    "--format={{json .NetworkSettings.Ports}}",
                    container,
                ],
                timeout,
            )
            .await;

        let Ok(output) = output else {
            return HashMap::new();
        };
        if !output.status.success() {
            return HashMap::new();
        }

        let json_str = String::from_utf8_lossy(&output.stdout);
        let Ok(ports_json) = serde_json::from_str::<serde_json::Value>(&json_str) else {
            return HashMap::new();
        };

        let mut mappings = HashMap::new();
        if let Some(ports_obj) = ports_json.as_object() {
            for (container_port, bindings) in ports_obj {
                if let Some(bindings_array) = bindings.as_array() {
                    if let Some(first_binding) = bindings_array.first() {
                        if let Some(host_port) =
                            first_binding.get("HostPort").and_then(|v| v.as_str())
                        {
                            mappings.insert(container_port.clone(), host_port.to_string());
                        }
                    }
                }
            }
        }
        mappings
    }

    /// List container names matching a filter.
    pub async fn ps_names(
        &self,
        filter: &str,
        timeout: Duration,
    ) -> Result<Vec<String>, DockerError> {
        let output = self
            .run_success(
                &["ps", "-a", "--filter", filter, "--format", "{{.Names}}"],
                timeout,
            )
            .await?;
        Ok(String::from_utf8_lossy(&output.stdout)
            .lines()
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect())
    }

    /// List names of RUNNING containers matching a filter (no `-a`).
    /// Used to attribute port conflicts: only a running container can hold a
    /// host port, so stopped ones would misattribute.
    pub async fn ps_running_names(
        &self,
        filter: &str,
        timeout: Duration,
    ) -> Result<Vec<String>, DockerError> {
        let output = self
            .run_success(
                &["ps", "--filter", filter, "--format", "{{.Names}}"],
                timeout,
            )
            .await?;
        Ok(String::from_utf8_lossy(&output.stdout)
            .lines()
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect())
    }

    /// List container names and ports (for port conflict detection, synchronous).
    pub fn ps_names_and_ports_sync(&self) -> Vec<(String, String)> {
        let output = match self.run_sync(&["ps", "--format", "{{.Names}}\t{{.Ports}}"]) {
            Ok(o) if o.status.success() => o,
            _ => return Vec::new(),
        };

        String::from_utf8_lossy(&output.stdout)
            .lines()
            .filter_map(|line| {
                let parts: Vec<&str> = line.splitn(2, '\t').collect();
                if parts.len() >= 2 {
                    Some((parts[0].to_string(), parts[1].to_string()))
                } else {
                    None
                }
            })
            .collect()
    }

    /// List containers matching a filter with a custom format string.
    pub async fn ps_formatted(
        &self,
        filter: &str,
        format: &str,
        timeout: Duration,
    ) -> Result<Output, DockerError> {
        self.run_success(
            &["ps", "-a", "--filter", filter, "--format", format],
            timeout,
        )
        .await
    }

    /// Check if an image exists locally.
    pub async fn image_exists(&self, image: &str) -> bool {
        match self
            .run(&["inspect", "--type=image", image], Duration::from_secs(10))
            .await
        {
            Ok(o) => o.status.success(),
            Err(_) => false,
        }
    }

    // ========================================================================
    // Exec / Logs
    // ========================================================================

    /// Run a command inside a running container.
    pub async fn exec(
        &self,
        container: &str,
        cmd: &[&str],
        timeout: Duration,
    ) -> Result<Output, DockerError> {
        let mut args = vec!["exec", container];
        args.extend_from_slice(cmd);
        self.run(&args, timeout).await
    }

    /// Run a command inside a container using `sh -c`.
    pub async fn exec_sh(
        &self,
        container: &str,
        shell_cmd: &str,
        timeout: Duration,
    ) -> Result<Output, DockerError> {
        self.run(&["exec", container, "/bin/sh", "-c", shell_cmd], timeout)
            .await
    }

    /// Fetch container logs.
    pub async fn logs(
        &self,
        container: &str,
        tail: usize,
        timeout: Duration,
    ) -> Result<Output, DockerError> {
        let tail_str = tail.to_string();
        self.run(&["logs", "--tail", &tail_str, container], timeout)
            .await
    }

    // ========================================================================
    // Build / Push
    // ========================================================================

    /// Build a Docker image. Inherits stdio for interactive output.
    pub async fn build(&self, args: &[&str]) -> Result<(), DockerError> {
        let mut full_args = vec!["build"];
        full_args.extend_from_slice(args);

        let cmd_str = format!("docker {}", full_args.join(" "));
        let status = tokio::process::Command::new("docker")
            .args(&full_args)
            .stdin(std::process::Stdio::inherit())
            .stdout(std::process::Stdio::inherit())
            .stderr(std::process::Stdio::inherit())
            .status()
            .await
            .map_err(|e| DockerError::exec_failed(&cmd_str, e))?;

        if status.success() {
            Ok(())
        } else {
            Err(DockerError::cmd_failed(
                cmd_str,
                "build failed",
                status.code(),
            ))
        }
    }

    /// Push a Docker image. Inherits stdio for interactive output.
    pub async fn push(&self, image: &str) -> Result<(), DockerError> {
        let cmd_str = format!("docker push {}", image);
        let status = tokio::process::Command::new("docker")
            .args(["push", image])
            .stdin(std::process::Stdio::inherit())
            .stdout(std::process::Stdio::inherit())
            .stderr(std::process::Stdio::inherit())
            .status()
            .await
            .map_err(|e| DockerError::exec_failed(&cmd_str, e))?;

        if status.success() {
            Ok(())
        } else {
            Err(DockerError::cmd_failed(
                cmd_str,
                "push failed",
                status.code(),
            ))
        }
    }

    /// Check if an image exists locally (returns `Result` for error propagation).
    pub async fn image_exists_checked(&self, image: &str) -> Result<bool, DockerError> {
        match self
            .run(&["image", "inspect", image], Duration::from_secs(10))
            .await
        {
            Ok(o) => Ok(o.status.success()),
            Err(DockerError::CommandFailed { .. }) => Ok(false),
            Err(e) => Err(e),
        }
    }

    // ========================================================================
    // Volume management
    // ========================================================================

    /// Force-remove a Docker volume.
    pub async fn volume_rm(&self, volume: &str) -> Result<Output, DockerError> {
        self.run(&["volume", "rm", "-f", volume], Duration::from_secs(10))
            .await
    }

    // ========================================================================
    // Daemon health
    // ========================================================================

    /// Check if the Docker daemon is healthy (async).
    pub async fn daemon_healthy(&self, timeout: Duration) -> bool {
        match self
            .run(&["info", "--format", "{{.ServerVersion}}"], timeout)
            .await
        {
            Ok(o) => o.status.success(),
            Err(_) => false,
        }
    }

    /// Check if the Docker daemon is healthy (synchronous).
    pub fn daemon_healthy_sync(&self) -> bool {
        match self.run_sync(&["info", "--format", "{{.ServerVersion}}"]) {
            Ok(o) => o.status.success(),
            Err(_) => false,
        }
    }

    /// Get Docker version string.
    pub async fn version(&self) -> Result<Output, DockerError> {
        self.run(&["--version"], Duration::from_secs(5)).await
    }

    /// Run `docker info` (for daemon status checks).
    pub async fn info_status(&self) -> bool {
        match self.run(&["info"], Duration::from_secs(5)).await {
            Ok(o) => o.status.success(),
            Err(_) => false,
        }
    }

    /// Detect Docker Compose variant (v1 or v2).
    pub async fn compose_version(&self) -> Result<Output, DockerError> {
        // Try v2 first
        let v2 = self
            .run(&["compose", "version"], Duration::from_secs(5))
            .await;
        if let Ok(ref o) = v2 {
            if o.status.success() {
                return v2;
            }
        }
        // Fall back to v1 binary
        let cmd_str = "docker-compose --version";
        let result = tokio::process::Command::new("docker-compose")
            .args(["--version"])
            .output()
            .await
            .map_err(|e| DockerError::exec_failed(cmd_str, e))?;
        Ok(result)
    }
}

impl Default for DockerClient {
    fn default() -> Self {
        Self::new()
    }
}
