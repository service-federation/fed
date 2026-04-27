use super::{BaseService, ServiceManager, Status};
use crate::config::Service as ServiceConfig;
use crate::docker::{DockerClient, DockerError};
use crate::error::{Error, Result};
use async_trait::async_trait;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Generate a Docker container name scoped by isolation ID or work directory.
///
/// - With isolation ID: `fed-{sanitized_id}-{sanitized_service}` (ID is already unique)
/// - Without isolation ID: `fed-{work_dir_hash}-{sanitized_service}` (8-hex-char hash of work_dir)
///
/// Max total length is ~69 chars (`fed-` + 32-char component + `-` + 32-char component),
/// well within Docker's 128-char container name limit.
///
/// This prevents container name collisions across different project directories.
pub(crate) fn docker_container_name(
    service_name: &str,
    session_id: Option<&str>,
    work_dir: &Path,
) -> String {
    let sanitized_service = sanitize_container_name_component(service_name);

    if let Some(session_id) = session_id {
        let sanitized_session = sanitize_container_name_component(session_id);
        format!("fed-{}-{}", sanitized_session, sanitized_service)
    } else {
        let hash = hash_work_dir(work_dir);
        format!("fed-{}-{}", hash, sanitized_service)
    }
}

/// Sanitize a string for use in Docker container names.
///
/// Docker container names must match `[a-zA-Z0-9][a-zA-Z0-9_.-]*`.
/// This function:
/// - Replaces invalid characters with underscores
/// - Truncates to 32 characters (to keep total name under Docker's 128 char limit)
/// - Ensures the result doesn't start with invalid characters
pub(crate) fn sanitize_container_name_component(input: &str) -> String {
    const MAX_COMPONENT_LEN: usize = 32;

    if input.is_empty() {
        return "unnamed".to_string();
    }

    // After this map, every char is ASCII (alphanumeric, '_', '.', '-', or replacement '_'),
    // so char count == byte count, and byte-indexing is safe on the result.
    let sanitized: String = input
        .chars()
        .take(MAX_COMPONENT_LEN)
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '_' || c == '.' || c == '-' {
                c
            } else {
                '_'
            }
        })
        .collect();

    // Ensure doesn't start with invalid character (dash, period, or underscore)
    // Docker requires first char to be alphanumeric
    // SAFETY: &sanitized[1..] is a byte-level slice, but all chars are ASCII after
    // the map above, so slicing at byte offset 1 is always a valid char boundary.
    let sanitized = if sanitized.starts_with(|c: char| !c.is_ascii_alphanumeric()) {
        format!("x{}", &sanitized[1..])
    } else {
        sanitized
    };

    // Handle edge case of empty result after sanitization
    if sanitized.is_empty() {
        "unnamed".to_string()
    } else {
        sanitized
    }
}

/// Hash a work directory path to 8 hex characters for container name scoping.
///
/// Uses FNV-1a (32-bit) for deterministic, stable hashing across Rust versions.
/// `DefaultHasher` uses SipHash with randomized seeds, making its output
/// non-reproducible across process restarts. FNV-1a is simple, fast, and
/// produces the same hash for the same input regardless of Rust toolchain.
///
/// The path is canonicalized first so that `./project` and `/abs/path/project`
/// produce the same container name.
pub fn hash_work_dir(work_dir: &Path) -> String {
    let canonical = std::fs::canonicalize(work_dir).unwrap_or_else(|_| work_dir.to_path_buf());
    let bytes = canonical.as_os_str().as_encoded_bytes();
    let hash = fnv1a_32(bytes);
    format!("{:08x}", hash)
}

/// FNV-1a 32-bit hash — deterministic across Rust versions and platforms.
pub(crate) fn fnv1a_32(data: &[u8]) -> u32 {
    const FNV_OFFSET: u32 = 2_166_136_261;
    const FNV_PRIME: u32 = 16_777_619;
    let mut hash = FNV_OFFSET;
    for &byte in data {
        hash ^= byte as u32;
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    hash
}

// Docker operation timeouts (normalized for consistency)
const DOCKER_REMOVE_TIMEOUT: Duration = Duration::from_secs(10); // Remove container
const DOCKER_START_TIMEOUT: Duration = Duration::from_secs(30); // Start container
const DOCKER_PULL_TIMEOUT: Duration = Duration::from_secs(300); // Pull image (5 minutes)
const DOCKER_STOP_TIMEOUT: Duration = Duration::from_secs(30); // Stop/kill container (Docker default 10s + margin)
const DOCKER_HEALTHCHECK_TIMEOUT: Duration = Duration::from_secs(30); // Healthcheck exec
const DOCKER_INSPECT_TIMEOUT: Duration = Duration::from_secs(10); // Inspect container
const DOCKER_LOGS_TIMEOUT: Duration = Duration::from_secs(10); // Fetch container logs
const LOG_CACHE_TTL: Duration = Duration::from_secs(1); // Cache logs for 1 second

/// Docker-based service manager
pub struct DockerService {
    name: String,
    base: Arc<RwLock<BaseService>>,
    config: ServiceConfig,
    container_id: Arc<RwLock<Option<String>>>,
    session_id: Option<String>,
    client: DockerClient,
    /// Cached logs to avoid spawning docker subprocess on every call
    log_cache: Arc<tokio::sync::RwLock<(Vec<String>, Instant)>>,
}

impl DockerService {
    pub fn new(
        name: String,
        config: ServiceConfig,
        environment: HashMap<String, String>,
        work_dir: String,
        session_id: Option<String>,
    ) -> Self {
        Self {
            name: name.clone(),
            base: Arc::new(RwLock::new(BaseService::new(name, environment, work_dir))),
            config,
            container_id: Arc::new(RwLock::new(None)),
            session_id,
            client: DockerClient::new(),
            // Initialize with empty cache that's already expired
            log_cache: Arc::new(tokio::sync::RwLock::new((
                Vec::new(),
                Instant::now() - LOG_CACHE_TTL - Duration::from_secs(1),
            ))),
        }
    }

    /// Restore container ID from state (used when reattaching to existing containers)
    pub fn set_container_id(&self, container_id: String) {
        *self.container_id.write() = Some(container_id);
        // Also set status to Running since we're restoring an existing container
        let mut base = self.base.write();
        base.set_status(Status::Running);
    }

    /// Parse Docker error message for port conflict and extract port number.
    ///
    /// Looks for patterns like:
    /// - "Bind for 0.0.0.0:5433 failed: port is already allocated"
    /// - "Bind for \[::\]:6379 failed: port is already allocated"
    fn parse_port_conflict(error: &str) -> Option<u16> {
        // Look for "Bind for X:PORT failed" pattern
        if !error.contains("port is already allocated") {
            return None;
        }

        // Find "Bind for " and extract the address:port part up to " failed"
        let bind_idx = error.find("Bind for ")?;
        let after_bind = &error[bind_idx + 9..]; // Skip "Bind for "

        // Find " failed" to know where the address:port ends
        let failed_idx = after_bind.find(" failed")?;
        let addr_port = &after_bind[..failed_idx]; // e.g., "0.0.0.0:5433" or "[::]:6379"

        // Find the last colon in just the address:port part
        let colon_idx = addr_port.rfind(':')?;
        let port_str = &addr_port[colon_idx + 1..];

        port_str.parse().ok()
    }

    /// Scope a volume specification with an isolation ID.
    ///
    /// Transforms named volumes to include a scope prefix for isolation:
    /// - `myvolume:/data` → `fed-{scope}-myvolume:/data`
    /// - `myvolume:/data:ro` → `fed-{scope}-myvolume:/data:ro`
    ///
    /// Bind mounts are returned unchanged:
    /// - `./data:/data` → `./data:/data`
    /// - `/host/path:/data` → `/host/path:/data`
    fn scope_volume_with_session(spec: &str, session_id: &str) -> String {
        if let Some(volume_name) = Self::extract_named_volume_name(spec) {
            let scoped_name = format!("fed-{}-{}", session_id, volume_name);
            spec.replacen(&volume_name, &scoped_name, 1)
        } else {
            spec.to_string()
        }
    }

    /// Extract the named volume name from a volume specification.
    ///
    /// Returns `Some(name)` for named volumes, `None` for bind mounts.
    ///
    /// Examples:
    /// - `myvolume:/data` → `Some("myvolume")`
    /// - `myvolume:/data:ro` → `Some("myvolume")`
    /// - `./data:/data` → `None` (relative bind mount)
    /// - `/host/path:/data` → `None` (absolute bind mount)
    /// - `C:/Windows:/win` → `None` (Windows path)
    fn extract_named_volume_name(spec: &str) -> Option<String> {
        let parts: Vec<&str> = spec.split(':').collect();
        if parts.is_empty() {
            return None;
        }

        let source = parts[0];

        // Empty source is not a valid volume
        if source.is_empty() {
            return None;
        }

        // Windows drive letter detection: single letter followed by path
        // e.g., "C:/path:/container" splits to ["C", "/path", "/container"]
        if source.len() == 1 {
            let first_char = source.chars().next().unwrap();
            if first_char.is_ascii_alphabetic()
                && parts.len() >= 2
                && (parts[1].starts_with('/') || parts[1].starts_with('\\'))
            {
                return None;
            }
        }

        // Bind mounts start with `.`, `/`, `~`, or contain path separators
        if source.starts_with('.')
            || source.starts_with('/')
            || source.starts_with('~')
            || source.contains('/')
            || source.contains('\\')
        {
            return None;
        }

        // It's a named volume
        Some(source.to_string())
    }

    /// Validate volume specification for security.
    ///
    /// Format: `[host-path:]container-path[:options]`
    fn validate_volume_spec(spec: &str) -> Result<()> {
        if spec.is_empty() {
            return Err(Error::Config(
                "Empty volume specification (check service volumes config)".to_string(),
            ));
        }

        // Reject absolute host paths for security
        // Check for Windows drive letters (C:\, D:\, C:/, etc.) before splitting
        // because split(':') breaks Windows paths
        // Use safe character iteration to avoid panics with multi-byte UTF-8
        let mut chars = spec.chars();
        if let (Some(first_char), Some(second_char)) = (chars.next(), chars.next()) {
            // Check for Windows drive pattern: X:\ or X:/
            if first_char.is_ascii_alphabetic() && second_char == ':' {
                if let Some(third_char) = chars.next() {
                    if third_char == '\\' || third_char == '/' {
                        return Err(Error::Config(format!(
                            "Absolute host paths not allowed in volumes for security: '{}'",
                            spec
                        )));
                    }
                }
            }
        }

        // Reject Unix absolute paths
        let parts: Vec<&str> = spec.split(':').collect();
        if parts.len() >= 2 {
            let host_part = parts[0];
            // Check for absolute Unix paths
            if host_part.starts_with('/') {
                return Err(Error::Config(format!(
                    "Absolute host paths not allowed in volumes for security: '{}'",
                    spec
                )));
            }
        }

        // Reject dangerous SELinux options (:z, :Z) and privileged options
        // Use case-insensitive check to prevent bypass via :Zo, :zo, etc.
        let spec_lower = spec.to_lowercase();
        if spec_lower.contains(":z") {
            return Err(Error::Config(format!(
                "SELinux volume options not allowed for security: '{}'",
                spec
            )));
        }

        // Also reject tilde paths (shell-dependent expansion, inconsistent behavior)
        if parts.len() >= 2 && parts[0].starts_with('~') {
            return Err(Error::Config(format!(
                "Tilde paths not allowed (shell-dependent expansion): '{}'",
                spec
            )));
        }

        Ok(())
    }

    /// Validate port specification for security.
    ///
    /// Format: `[host-ip:][host-port:]container-port[/protocol]`
    fn validate_port_spec(spec: &str) -> Result<()> {
        if spec.is_empty() {
            return Err(Error::Config(
                "Empty port specification (check service ports config)".to_string(),
            ));
        }

        // Reject binding to all interfaces for security (0.0.0.0 for IPv4, [::] for IPv6)
        if spec.contains("0.0.0.0") || spec.contains("[::]") {
            return Err(Error::Config(format!(
                "Binding to all interfaces (0.0.0.0 or [::]) not allowed for security: '{}'",
                spec
            )));
        }

        // Validate port format and reject dangerous patterns
        // Valid formats: "8080", "8080:80", "127.0.0.1:8080:80", "8080/tcp"
        let parts: Vec<&str> = spec.split('/').collect();
        let port_part = parts[0];

        // Split by : to check individual components
        let port_components: Vec<&str> = port_part.split(':').collect();

        // Validate that each numeric component is a valid port number or range
        for component in &port_components {
            // Skip IP addresses (contain dots) and IPv6 (contain brackets)
            if component.contains('.') || component.contains('[') || component.contains(']') {
                continue;
            }

            // Check for port range (e.g., "8080-8090")
            if component.contains('-') {
                let range_parts: Vec<&str> = component.split('-').collect();
                if range_parts.len() == 2 {
                    // Validate both ends of the range
                    if let (Ok(start), Ok(end)) =
                        (range_parts[0].parse::<u16>(), range_parts[1].parse::<u16>())
                    {
                        if start == 0 || end == 0 {
                            return Err(Error::Config(format!(
                                "Port 0 not allowed in port range: '{}'",
                                spec
                            )));
                        }
                        if start > end {
                            return Err(Error::Config(format!(
                                "Invalid port range (start > end): '{}'",
                                spec
                            )));
                        }
                    }
                }
            } else {
                // Try to parse as single port number
                if let Ok(port) = component.parse::<u16>() {
                    if port == 0 {
                        return Err(Error::Config(format!(
                            "Port 0 not allowed in port specification: '{}'",
                            spec
                        )));
                    }
                }
            }
        }

        Ok(())
    }
}

#[async_trait]
impl ServiceManager for DockerService {
    async fn start(&mut self) -> Result<()> {
        {
            let mut base = self.base.write();
            if base.status == Status::Running || base.status == Status::Healthy {
                return Ok(());
            }
            base.set_status(Status::Starting);
        }

        let image = self
            .config
            .image
            .as_ref()
            .ok_or_else(|| Error::Config("No image specified".to_string()))?;

        // Build docker run command
        // NOTE: Not using --rm flag to avoid race conditions with manual stop/cleanup
        // We'll manually remove containers in stop() to ensure proper state tracking
        let mut args = vec!["run".to_string(), "-d".to_string()];

        // Add deterministic container name for tracking and orphan prevention
        // Format: fed-<work_dir_hash>-<service-name> or fed-<isolation-id>-<service-name>
        let (service_name, env_args) = {
            let base = self.base.read();
            let mut env_args = Vec::new();
            for (key, value) in &base.environment {
                env_args.push("-e".to_string());
                env_args.push(format!("{}={}", key, value));
            }
            // Markers for the recursion check in main.rs. Containers
            // shelling out to fed against the host workspace would
            // recurse — the workspace path lets fed distinguish that
            // from a legitimate cross-workspace invocation.
            env_args.push("-e".to_string());
            env_args.push(format!("FED_SPAWNED_BY_SERVICE={}", base.name));
            env_args.push("-e".to_string());
            env_args.push(format!("FED_SPAWNED_FROM_WORKSPACE={}", base.work_dir));
            (base.name.clone(), env_args)
        };

        let container_name = {
            let base = self.base.read();
            docker_container_name(
                &service_name,
                self.session_id.as_deref(),
                Path::new(&base.work_dir),
            )
        };

        // Remove any existing container with this name unconditionally.
        // Using `docker rm -f` is atomic and avoids TOCTOU race conditions
        // that would occur with inspect-then-remove pattern.
        // The -f flag ensures this succeeds whether container exists or not.
        match self
            .client
            .rm_force(&container_name, DOCKER_REMOVE_TIMEOUT)
            .await
        {
            Ok(()) => {
                tracing::debug!("Removed existing container (if any): {}", container_name);
            }
            Err(e) => {
                tracing::warn!(
                    "Failed to remove container {}: {}, continuing anyway",
                    container_name,
                    e
                );
            }
        }

        args.push("--name".to_string());
        args.push(container_name.clone());

        // Add labels for container tracking and management
        args.push("--label".to_string());
        args.push("com.service-federation.managed=true".to_string());
        args.push("--label".to_string());
        args.push(format!("com.service-federation.service={}", service_name));

        args.extend(env_args);

        // Add resource limits if configured
        if let Some(ref resources) = self.config.resources {
            // Memory limit (--memory)
            if let Some(ref memory) = resources.memory {
                args.push("--memory".to_string());
                args.push(memory.clone());
            }

            // Memory reservation (soft limit, --memory-reservation)
            if let Some(ref memory_reservation) = resources.memory_reservation {
                args.push("--memory-reservation".to_string());
                args.push(memory_reservation.clone());
            }

            // Memory swap limit (--memory-swap)
            if let Some(ref memory_swap) = resources.memory_swap {
                args.push("--memory-swap".to_string());
                args.push(memory_swap.clone());
            }

            // CPU limit (--cpus)
            if let Some(ref cpus) = resources.cpus {
                args.push("--cpus".to_string());
                args.push(cpus.clone());
            }

            // CPU shares (--cpu-shares)
            if let Some(cpu_shares) = resources.cpu_shares {
                args.push("--cpu-shares".to_string());
                args.push(cpu_shares.to_string());
            }

            // PID limit (--pids-limit)
            if let Some(pids) = resources.pids {
                args.push("--pids-limit".to_string());
                args.push(pids.to_string());
            }

            // File descriptor limit (--ulimit nofile)
            if let Some(nofile) = resources.nofile {
                args.push("--ulimit".to_string());
                args.push(format!("nofile={}", nofile));
            }
        }

        // Add volumes with validation and isolation scoping
        for volume in &self.config.volumes {
            Self::validate_volume_spec(volume).map_err(|e| match e {
                Error::Config(msg) => Error::Config(format!("Service '{}': {}", self.name, msg)),
                other => other,
            })?;
            args.push("-v".to_string());
            // Scope named volumes for isolation (isolation ID or work_dir hash)
            let scoped_volume = if let Some(ref sid) = self.session_id {
                Self::scope_volume_with_session(volume, sid)
            } else {
                let base = self.base.read();
                let hash = hash_work_dir(Path::new(&base.work_dir));
                Self::scope_volume_with_session(volume, &hash)
            };
            args.push(scoped_volume);
        }

        // Add ports with validation
        for port in &self.config.ports {
            Self::validate_port_spec(port).map_err(|e| match e {
                Error::Config(msg) => Error::Config(format!("Service '{}': {}", self.name, msg)),
                other => other,
            })?;
            args.push("-p".to_string());
            args.push(port.clone());
        }

        // Add image name
        args.push(image.clone());

        // Add command override (appended after image, replaces Docker CMD)
        if let Some(ref command) = self.config.command {
            args.extend(command.to_args());
        }

        // PERFORMANCE: Check if image exists locally before pulling (avoid 5min timeout on cached images)
        let needs_pull = !self.client.image_exists(image).await;

        if needs_pull {
            tracing::info!("Pulling image '{}' (not found locally)", image);
            if let Err(e) = self.client.pull(image, DOCKER_PULL_TIMEOUT).await {
                let mut base = self.base.write();
                base.set_status(Status::Failing);
                return Err(Error::ServiceStartFailed(
                    service_name.clone(),
                    format!("Failed to pull image '{}': {}", image, e),
                ));
            }
        } else {
            tracing::debug!("Image '{}' found locally, skipping pull", image);
        }

        // Run docker command with timeout
        let output = match self.client.run_detached(&args, DOCKER_START_TIMEOUT).await {
            Ok(out) => out,
            Err(e) => {
                let mut base = self.base.write();
                base.set_status(Status::Failing);
                return Err(Error::ServiceStartFailed(
                    service_name.clone(),
                    e.to_string(),
                ));
            }
        };

        if !output.status.success() {
            {
                let mut base = self.base.write();
                base.set_status(Status::Failing);
            } // Drop lock before await

            let error = String::from_utf8_lossy(&output.stderr);

            // Clean up the orphaned container that Docker created but couldn't start
            let _ = self
                .client
                .rm_force(&container_name, DOCKER_REMOVE_TIMEOUT)
                .await;

            // Parse port conflict errors for better messaging
            if let Some(port) = Self::parse_port_conflict(&error) {
                return Err(Error::DockerPortConflict {
                    service: service_name,
                    port,
                });
            }

            return Err(Error::ServiceStartFailed(service_name, error.to_string()));
        }

        let container_id = String::from_utf8_lossy(&output.stdout).trim().to_string();
        *self.container_id.write() = Some(container_id);

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

        let container_id = self.container_id.read().clone();
        if let Some(ref id) = container_id {
            // Stop the container
            if let Err(e) = self.client.stop(id, DOCKER_STOP_TIMEOUT).await {
                let mut base = self.base.write();
                base.set_status(Status::Failing);
                return Err(e.into());
            }

            // Remove the container with retry logic (since we don't use --rm flag)
            // Use -f flag to force removal even if container is still busy
            const MAX_RETRIES: u32 = 3;
            let mut last_error = None;

            for attempt in 1..=MAX_RETRIES {
                match self.client.rm_force(id, DOCKER_REMOVE_TIMEOUT).await {
                    Ok(()) => {
                        last_error = None;
                        break;
                    }
                    Err(e) => {
                        if attempt < MAX_RETRIES {
                            // Wait before retry with exponential backoff
                            let delay_ms = 100 * 2u64.pow(attempt);
                            tracing::warn!(
                                "Failed to remove container {} (attempt {}/{}): {}. Retrying in {}ms...",
                                id,
                                attempt,
                                MAX_RETRIES,
                                e,
                                delay_ms
                            );
                            tokio::time::sleep(Duration::from_millis(delay_ms)).await;
                            last_error = Some(e);
                        } else {
                            last_error = Some(e);
                        }
                    }
                }
            }

            if let Some(e) = last_error {
                // All retries exhausted - this is a real failure
                let mut base = self.base.write();
                base.set_status(Status::Failing);
                return Err(DockerError::cmd_failed(
                    "docker rm",
                    format!(
                        "Failed to remove container {} after {} attempts: {}. \
                        Container may be orphaned. Run 'fed stop' or 'fed clean' to retry.",
                        id, MAX_RETRIES, e
                    ),
                    None,
                )
                .into());
            }
        }

        // Only clear container ID and mark as stopped after both stop AND remove succeed
        *self.container_id.write() = None;

        {
            let mut base = self.base.write();
            base.set_status(Status::Stopped);
        }

        Ok(())
    }

    async fn kill(&mut self) -> Result<()> {
        let container_id = self.container_id.read().clone();
        if let Some(ref id) = container_id {
            // Kill the container (Ok(()) if not found/not running)
            self.client.kill(id, DOCKER_STOP_TIMEOUT).await?;

            // Remove the container after killing
            if let Err(e) = self.client.rm_force(id, DOCKER_REMOVE_TIMEOUT).await {
                // Log warning but don't fail - container might already be removed
                tracing::warn!("Failed to remove container {}: {}", id, e);
            }
        }

        // Clear state directly without calling stop() to avoid double-processing
        *self.container_id.write() = None;
        {
            let mut base = self.base.write();
            base.set_status(Status::Stopped);
        }

        Ok(())
    }

    async fn health(&self) -> Result<bool> {
        let container_id = self.container_id.read().clone();
        if let Some(ref id) = container_id {
            // Check if Docker daemon is healthy first.
            // If the daemon is down/restarting, all container checks will fail.
            // In this case, we assume the container is healthy to avoid spurious restarts.
            if !crate::docker::is_daemon_healthy().await {
                tracing::warn!(
                    "Docker daemon unhealthy - assuming container '{}' is healthy to avoid spurious restarts",
                    self.name
                );
                return Ok(true);
            }

            // If a healthcheck command is configured, run it inside the container
            if let Some(ref healthcheck) = self.config.healthcheck {
                if let Some(cmd) = healthcheck.get_command() {
                    tracing::debug!(
                        "Running healthcheck for {}: docker exec {} /bin/sh -c '{}'",
                        self.name,
                        id,
                        cmd
                    );

                    let output = match self
                        .client
                        .exec_sh(id, cmd, DOCKER_HEALTHCHECK_TIMEOUT)
                        .await
                    {
                        Ok(out) => out,
                        Err(e) => {
                            tracing::debug!(
                                "Healthcheck command failed for {}: {:?}",
                                self.name,
                                e
                            );
                            return Ok(false); // Container doesn't exist or docker error
                        }
                    };

                    // Healthcheck passes if command exits with status 0
                    let success = output.status.success();
                    let stdout = String::from_utf8_lossy(&output.stdout);
                    let stderr = String::from_utf8_lossy(&output.stderr);

                    if success {
                        tracing::debug!("Healthcheck passed for {}", self.name);
                    } else {
                        // Log at warn level with actual error output for better debugging
                        let stderr_trimmed = stderr.trim();
                        let stdout_trimmed = stdout.trim();
                        if !stderr_trimmed.is_empty() {
                            tracing::warn!(
                                "Healthcheck failed for '{}': {}",
                                self.name,
                                stderr_trimmed
                            );
                        } else if !stdout_trimmed.is_empty() {
                            tracing::warn!(
                                "Healthcheck failed for '{}': {}",
                                self.name,
                                stdout_trimmed
                            );
                        } else {
                            tracing::warn!(
                                "Healthcheck failed for '{}' (exit code {})",
                                self.name,
                                output.status.code().unwrap_or(-1)
                            );
                        }
                    }
                    return Ok(success);
                }
            }

            // No healthcheck configured - fall back to checking if container is running
            Ok(self.client.is_running(id, DOCKER_INSPECT_TIMEOUT).await)
        } else {
            Ok(false)
        }
    }

    fn status(&self) -> Status {
        self.base.read().status
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn get_container_id(&self) -> Option<String> {
        self.container_id.read().clone()
    }

    async fn get_port_mappings(&self) -> HashMap<String, String> {
        let container_id = self.container_id.read().clone();
        if let Some(ref id) = container_id {
            self.client.inspect_ports(id, DOCKER_INSPECT_TIMEOUT).await
        } else {
            HashMap::new()
        }
    }

    fn get_last_error(&self) -> Option<String> {
        self.base.read().last_error.clone()
    }

    async fn logs(&self, tail: Option<usize>) -> Result<Vec<String>> {
        // Check cache first to avoid spawning docker subprocess on every call
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

        let container_id = self.container_id.read().clone();
        if let Some(ref id) = container_id {
            // Always fetch a reasonable number of logs for caching
            let fetch_count = 200;

            // Gracefully handle container not found or other errors
            let output = match self.client.logs(id, fetch_count, DOCKER_LOGS_TIMEOUT).await {
                Ok(out) => out,
                Err(_) => return Ok(Vec::new()),
            };

            // If command failed (e.g., "No such container"), return empty logs
            if !output.status.success() {
                return Ok(Vec::new());
            }

            // Combine stdout and stderr for complete log view
            let stdout_logs = String::from_utf8_lossy(&output.stdout);
            let stderr_logs = String::from_utf8_lossy(&output.stderr);

            let mut combined_logs: Vec<String> = stdout_logs.lines().map(String::from).collect();

            // Add stderr with prefix to distinguish it
            for line in stderr_logs.lines() {
                combined_logs.push(format!("[stderr] {}", line));
            }

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
        } else {
            Ok(Vec::new())
        }
    }
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ========================================================================
    // Volume Validation Tests (Security Critical)
    // ========================================================================

    #[test]
    fn test_validate_volume_valid_named_volume() {
        // Named volumes are safe (no host path)
        let result = DockerService::validate_volume_spec("myvolume:/data");
        assert!(result.is_ok(), "Named volumes should be valid");
    }

    #[test]
    fn test_validate_volume_valid_relative_path() {
        // Relative paths are allowed
        let result = DockerService::validate_volume_spec("./app:/app");
        assert!(result.is_ok(), "Relative paths should be valid");
    }

    #[test]
    fn test_validate_volume_valid_container_only() {
        // Container path only (anonymous volume)
        let result = DockerService::validate_volume_spec("/data");
        assert!(result.is_ok(), "Container-only path should be valid");
    }

    #[test]
    fn test_validate_volume_valid_with_options() {
        // Valid volume with read-only option
        let result = DockerService::validate_volume_spec("mydata:/data:ro");
        assert!(result.is_ok(), "Volume with :ro should be valid");
    }

    #[test]
    fn test_validate_volume_rejects_absolute_unix_path() {
        // SECURITY: Reject absolute Unix paths
        let result = DockerService::validate_volume_spec("/etc/passwd:/etc/passwd");
        assert!(result.is_err(), "Absolute Unix paths should be rejected");
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Absolute host paths"));
    }

    #[test]
    fn test_validate_volume_rejects_absolute_unix_root() {
        // SECURITY: Reject root filesystem mount
        let result = DockerService::validate_volume_spec("/:/host");
        assert!(result.is_err(), "Root mount should be rejected");
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Absolute host paths"));
    }

    #[test]
    fn test_validate_volume_rejects_windows_path_backslash() {
        // SECURITY: Reject Windows absolute paths (backslash)
        let result = DockerService::validate_volume_spec(r"C:\Windows:/win");
        assert!(
            result.is_err(),
            "Windows paths with backslash should be rejected"
        );
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Absolute host paths"));
    }

    #[test]
    fn test_validate_volume_rejects_windows_path_forward_slash() {
        // SECURITY: Reject Windows absolute paths (forward slash)
        let result = DockerService::validate_volume_spec("C:/Windows:/win");
        assert!(
            result.is_err(),
            "Windows paths with forward slash should be rejected"
        );
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Absolute host paths"));
    }

    #[test]
    fn test_validate_volume_rejects_selinux_lowercase_z() {
        // SECURITY: Reject SELinux :z option
        let result = DockerService::validate_volume_spec("myvolume:/data:z");
        assert!(result.is_err(), "SELinux :z option should be rejected");
        assert!(result.unwrap_err().to_string().contains("SELinux"));
    }

    #[test]
    fn test_validate_volume_rejects_selinux_uppercase_z() {
        // SECURITY: Reject SELinux :Z option
        let result = DockerService::validate_volume_spec("myvolume:/data:Z");
        assert!(result.is_err(), "SELinux :Z option should be rejected");
        assert!(result.unwrap_err().to_string().contains("SELinux"));
    }

    #[test]
    fn test_validate_volume_rejects_empty_spec() {
        // Edge case: empty specification
        let result = DockerService::validate_volume_spec("");
        assert!(result.is_err(), "Empty volume spec should be rejected");
        assert!(result.unwrap_err().to_string().contains("Empty volume"));
    }

    #[test]
    fn test_validate_volume_path_traversal_attempt() {
        // SECURITY: Path traversal attempts with relative path
        let result = DockerService::validate_volume_spec("../../etc:/etc");
        assert!(
            result.is_ok(),
            "Relative paths with .. are technically allowed by Docker"
        );
        // Note: Docker itself will resolve these paths, so we allow them but they're risky
    }

    #[test]
    fn test_validate_volume_rejects_tilde_paths() {
        // SECURITY: Tilde paths are shell-dependent and cause inconsistent behavior
        let result = DockerService::validate_volume_spec("~/myapp:/app");
        assert!(
            result.is_err(),
            "Tilde paths should be rejected (shell-dependent)"
        );
        assert!(result.unwrap_err().to_string().contains("Tilde"));
    }

    #[test]
    fn test_validate_volume_utf8_multibyte_safe() {
        // SAFETY: Ensure multi-byte UTF-8 characters don't cause panics
        // The emoji isn't a valid volume spec but shouldn't cause a crash
        let result = DockerService::validate_volume_spec("😀:/app");
        // This is actually OK because it's not detected as Windows path
        // The important thing is it doesn't panic (which it did before the fix)
        assert!(
            result.is_ok() || result.is_err(),
            "Should handle UTF-8 without panicking"
        );
    }

    #[test]
    fn test_validate_volume_selinux_case_bypass() {
        // SECURITY: Case variations should also be rejected
        assert!(DockerService::validate_volume_spec("vol:/data:Zo").is_err());
        assert!(DockerService::validate_volume_spec("vol:/data:zo").is_err());
        assert!(DockerService::validate_volume_spec("vol:/data:ZO").is_err());
    }

    #[test]
    fn test_validate_volume_complex_options() {
        // Valid volume with multiple options
        let result = DockerService::validate_volume_spec("mydata:/data:ro,cached");
        assert!(result.is_ok(), "Complex valid options should be accepted");
    }

    // ========================================================================
    // Port Validation Tests (Security Critical)
    // ========================================================================

    #[test]
    fn test_validate_port_valid_simple() {
        // Simple port mapping
        let result = DockerService::validate_port_spec("8080:80");
        assert!(result.is_ok(), "Simple port mapping should be valid");
    }

    #[test]
    fn test_validate_port_valid_single_port() {
        // Single port (host and container same)
        let result = DockerService::validate_port_spec("8080");
        assert!(result.is_ok(), "Single port should be valid");
    }

    #[test]
    fn test_validate_port_valid_with_localhost() {
        // Localhost binding (secure)
        let result = DockerService::validate_port_spec("127.0.0.1:8080:80");
        assert!(result.is_ok(), "Localhost binding should be valid");
    }

    #[test]
    fn test_validate_port_valid_with_protocol() {
        // Port with protocol
        let result = DockerService::validate_port_spec("8080:80/tcp");
        assert!(result.is_ok(), "Port with protocol should be valid");
    }

    #[test]
    fn test_validate_port_valid_udp() {
        // UDP protocol
        let result = DockerService::validate_port_spec("53:53/udp");
        assert!(result.is_ok(), "UDP ports should be valid");
    }

    #[test]
    fn test_validate_port_valid_high_port() {
        // High port number (65535 is max)
        let result = DockerService::validate_port_spec("65535:80");
        assert!(result.is_ok(), "Port 65535 should be valid");
    }

    #[test]
    fn test_validate_port_valid_low_port() {
        // Low port number (1 is min for user apps)
        let result = DockerService::validate_port_spec("1:80");
        assert!(result.is_ok(), "Port 1 should be valid");
    }

    #[test]
    fn test_validate_port_rejects_all_interfaces() {
        // SECURITY: Reject 0.0.0.0 binding (exposes to all interfaces)
        let result = DockerService::validate_port_spec("0.0.0.0:8080:80");
        assert!(result.is_err(), "0.0.0.0 binding should be rejected");
        assert!(result.unwrap_err().to_string().contains("0.0.0.0"));
    }

    #[test]
    fn test_validate_port_rejects_ipv6_all_interfaces() {
        // SECURITY: Reject binding to [::] (IPv6 all interfaces)
        let result = DockerService::validate_port_spec("[::]:8080:80");
        assert!(result.is_err(), "Binding to [::] should be rejected");
        assert!(result.unwrap_err().to_string().contains("[::]"));
    }

    #[test]
    fn test_validate_port_rejects_port_zero() {
        // SECURITY: Reject port 0
        let result = DockerService::validate_port_spec("0:80");
        assert!(result.is_err(), "Port 0 should be rejected");
        assert!(result.unwrap_err().to_string().contains("Port 0"));
    }

    #[test]
    fn test_validate_port_rejects_empty_spec() {
        // Edge case: empty specification
        let result = DockerService::validate_port_spec("");
        assert!(result.is_err(), "Empty port spec should be rejected");
        assert!(result.unwrap_err().to_string().contains("Empty port"));
    }

    #[test]
    fn test_validate_port_with_specific_ip() {
        // Specific IP binding (not 0.0.0.0)
        let result = DockerService::validate_port_spec("192.168.1.100:8080:80");
        assert!(result.is_ok(), "Specific IP binding should be valid");
    }

    #[test]
    fn test_validate_port_ipv6_localhost() {
        // IPv6 localhost
        let result = DockerService::validate_port_spec("[::1]:8080:80");
        assert!(result.is_ok(), "IPv6 localhost should be valid");
    }

    #[test]
    fn test_validate_port_range() {
        // Port range (Docker supports this)
        let result = DockerService::validate_port_spec("8080-8090:80");
        assert!(result.is_ok(), "Port ranges should be valid");
    }

    #[test]
    fn test_validate_port_malformed_too_many_colons() {
        // Malformed: too many colons
        let result = DockerService::validate_port_spec("127.0.0.1:8080:80:extra");
        assert!(
            result.is_ok(),
            "Extra colons are handled by validation logic"
        );
        // Docker will reject this, but our validation doesn't need to be that strict
    }

    #[test]
    fn test_validate_port_non_numeric() {
        // Non-numeric port (will be caught when parsing)
        let result = DockerService::validate_port_spec("abc:80");
        assert!(
            result.is_ok(),
            "Non-numeric is OK for validation (Docker will reject)"
        );
        // We only validate security issues, not all format issues
    }

    // ========================================================================
    // Container Naming Tests
    // ========================================================================

    #[test]
    fn test_container_naming_format() {
        // Test that container names follow fed-{service} format
        let config = ServiceConfig {
            image: Some("nginx:alpine".to_string()),
            ..Default::default()
        };

        let service = DockerService::new(
            "test-service".to_string(),
            config,
            HashMap::new(),
            "/tmp".to_string(),
            None, // No isolation ID
        );

        assert_eq!(service.name(), "test-service");
        // Container name would be "fed-test-service" but we can't easily test
        // without refactoring or running actual start()
    }

    #[test]
    fn test_service_initial_status() {
        let config = ServiceConfig {
            image: Some("nginx:alpine".to_string()),
            ..Default::default()
        };

        let service = DockerService::new(
            "test".to_string(),
            config,
            HashMap::new(),
            "/tmp".to_string(),
            None, // No isolation ID
        );

        assert_eq!(service.status(), Status::Stopped);
    }

    // ========================================================================
    // Edge Case Tests
    // ========================================================================

    #[test]
    fn test_validate_volume_only_options() {
        // Edge case: only options, no paths
        let result = DockerService::validate_volume_spec(":ro");
        assert!(result.is_ok(), "Only options is technically a valid spec");
    }

    #[test]
    fn test_validate_volume_many_colons() {
        // Edge case: many colons (unusual but possible)
        let result = DockerService::validate_volume_spec("vol:/data:ro:cached:z");
        assert!(result.is_err(), "Should reject due to :z in middle");
    }

    #[test]
    fn test_validate_port_zero_in_container_port() {
        // Port 0 in container port position
        let result = DockerService::validate_port_spec("8080:0");
        assert!(result.is_err(), "Port 0 in any position should be rejected");
    }

    #[test]
    fn test_validate_port_all_interfaces_with_range() {
        // 0.0.0.0 with port range
        let result = DockerService::validate_port_spec("0.0.0.0:8080-8090:80");
        assert!(
            result.is_err(),
            "0.0.0.0 should be rejected even with range"
        );
    }

    // ========================================================================
    // Security Regression Tests
    // ========================================================================

    #[test]
    fn test_security_no_host_root_mount() {
        // Critical: Never allow mounting host root
        let dangerous_mounts = vec!["/:/root", "/:/host:ro", "/:/:ro"];

        for mount in dangerous_mounts {
            let result = DockerService::validate_volume_spec(mount);
            assert!(result.is_err(), "Should reject dangerous mount: {}", mount);
        }
    }

    #[test]
    fn test_security_no_sensitive_paths() {
        // Critical: Common sensitive paths should be rejected
        let sensitive = vec![
            "/etc/shadow:/shadow",
            "/root/.ssh:/ssh",
            "/etc/passwd:/passwd",
            "/var/run/docker.sock:/docker.sock",
        ];

        for path in sensitive {
            let result = DockerService::validate_volume_spec(path);
            assert!(result.is_err(), "Should reject sensitive path: {}", path);
        }
    }

    #[test]
    fn test_security_no_public_bindings() {
        // Critical: Should not allow binding to all public interfaces
        let dangerous_bindings = vec!["0.0.0.0:22:22", "0.0.0.0:3306:3306", "0.0.0.0:5432:5432"];

        for binding in dangerous_bindings {
            let result = DockerService::validate_port_spec(binding);
            assert!(result.is_err(), "Should reject public binding: {}", binding);
        }
    }

    // ========================================================================
    // Container Name Sanitization Tests
    // ========================================================================

    #[test]
    fn test_sanitize_container_name_basic() {
        assert_eq!(
            sanitize_container_name_component("my-service"),
            "my-service"
        );
    }

    #[test]
    fn test_sanitize_container_name_special_chars() {
        assert_eq!(
            sanitize_container_name_component("my service@v1.0"),
            "my_service_v1.0"
        );
    }

    #[test]
    fn test_sanitize_container_name_unicode() {
        // Unicode characters should be replaced with underscores
        // "cafe" with accented 'e' becomes "caf_"
        assert_eq!(sanitize_container_name_component("caf\u{00e9}"), "caf_");
    }

    #[test]
    fn test_sanitize_container_name_starts_with_dash() {
        // First char replaced with 'x', rest preserved
        assert_eq!(
            sanitize_container_name_component("-myservice"),
            "xmyservice"
        );
    }

    #[test]
    fn test_sanitize_container_name_starts_with_underscore() {
        // First char replaced with 'x', rest preserved
        assert_eq!(
            sanitize_container_name_component("_myservice"),
            "xmyservice"
        );
    }

    #[test]
    fn test_sanitize_container_name_starts_with_period() {
        // First char replaced with 'x', rest preserved
        assert_eq!(
            sanitize_container_name_component(".myservice"),
            "xmyservice"
        );
    }

    #[test]
    fn test_sanitize_container_name_truncation() {
        let long_name = "a".repeat(100);
        let result = sanitize_container_name_component(&long_name);
        assert_eq!(result.len(), 32);
    }

    #[test]
    fn test_sanitize_container_name_empty() {
        assert_eq!(sanitize_container_name_component(""), "unnamed");
    }

    #[test]
    fn test_sanitize_container_name_preserves_valid_chars() {
        // Verify all valid characters are preserved
        assert_eq!(sanitize_container_name_component("aZ09_.-"), "aZ09_.-");
    }

    #[test]
    fn test_container_name_without_isolation_id() {
        // Without isolation ID, should include work_dir hash
        let name = docker_container_name("my-service", None, Path::new("/tmp/project"));
        let hash = hash_work_dir(Path::new("/tmp/project"));
        assert_eq!(name, format!("fed-{}-my-service", hash));
    }

    #[test]
    fn test_container_name_with_isolation_id() {
        // With isolation ID, should use it instead of work_dir hash
        let name = docker_container_name("my-service", Some("abc123"), Path::new("/tmp/project"));
        assert_eq!(name, "fed-abc123-my-service");
    }

    #[test]
    fn test_container_name_different_work_dirs() {
        // Different work directories should produce different container names
        let name_a = docker_container_name("svc", None, Path::new("/projects/alpha"));
        let name_b = docker_container_name("svc", None, Path::new("/projects/beta"));
        assert_ne!(
            name_a, name_b,
            "Different work dirs must produce different names"
        );
    }

    #[test]
    fn test_container_name_isolation_id_overrides_work_dir() {
        // Same isolation ID but different work dirs should produce the same name
        let name_a = docker_container_name("svc", Some("sess1"), Path::new("/projects/alpha"));
        let name_b = docker_container_name("svc", Some("sess1"), Path::new("/projects/beta"));
        assert_eq!(
            name_a, name_b,
            "Isolation ID should override work_dir scoping"
        );
    }

    // ========================================================================
    // Port Conflict Parsing Tests
    // ========================================================================

    #[test]
    fn test_parse_port_conflict_ipv4() {
        let error = "docker: Error response from daemon: failed to set up container networking: \
            driver failed programming external connectivity on endpoint fed-abc-postgres \
            (12345): Bind for 0.0.0.0:5433 failed: port is already allocated";
        assert_eq!(DockerService::parse_port_conflict(error), Some(5433));
    }

    #[test]
    fn test_parse_port_conflict_ipv6() {
        let error = "docker: Error response from daemon: Bind for [::]:6379 failed: port is already allocated";
        assert_eq!(DockerService::parse_port_conflict(error), Some(6379));
    }

    #[test]
    fn test_parse_port_conflict_not_port_error() {
        let error = "docker: Error response from daemon: image not found";
        assert_eq!(DockerService::parse_port_conflict(error), None);
    }
}
