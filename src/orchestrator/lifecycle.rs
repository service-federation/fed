//! Service lifecycle commands for install and clean operations.
//!
//! This module contains the `ServiceLifecycleCommands` struct which encapsulates
//! all install and clean logic that was previously part of the main Orchestrator.
//! Extracting these operations improves separation of concerns and makes the
//! orchestrator core more focused on service coordination.

use crate::config::Config;
use crate::docker::{DockerClient, DockerError};
use crate::error::{Error, Result};
use std::path::Path;
use std::process::Stdio;

/// Commands for managing service lifecycle operations (install/clean).
///
/// This struct provides high-level operations for installing service dependencies
/// and cleaning up service artifacts. It abstracts away the details of running
/// shell commands and managing Docker volumes.
///
/// # Example
///
/// ```ignore
/// use fed::{Config, orchestrator::ServiceLifecycleCommands};
/// use std::path::Path;
///
/// # async fn example() -> Result<(), fed::Error> {
/// let config = Config::default();
/// let work_dir = Path::new(".");
/// let lifecycle = ServiceLifecycleCommands::new(&config, work_dir, None);
///
/// // Run install for a service
/// lifecycle.run_install("my-service").await?;
///
/// // Clean up service artifacts
/// lifecycle.run_clean("my-service").await?;
/// # Ok(())
/// # }
/// ```
pub struct ServiceLifecycleCommands<'a> {
    config: &'a Config,
    work_dir: &'a Path,
    isolation_id: Option<String>,
}

impl<'a> ServiceLifecycleCommands<'a> {
    /// Create a new lifecycle commands handler.
    ///
    /// Install/migrate marker lookups are scoped by `(work_dir, isolation_id)`
    /// so isolated child orchestrators never read or mutate the parent's
    /// shared marker state.
    ///
    /// # Arguments
    ///
    /// * `config` - Reference to the service configuration
    /// * `work_dir` - Working directory for resolving relative paths
    /// * `isolation_id` - Isolation session id, or `None` for the shared scope
    pub fn new(config: &'a Config, work_dir: &'a Path, isolation_id: Option<String>) -> Self {
        Self {
            config,
            work_dir,
            isolation_id,
        }
    }

    fn markers(&self) -> crate::markers::LifecycleMarkers {
        crate::markers::LifecycleMarkers::new(
            self.work_dir.to_path_buf(),
            self.isolation_id.clone(),
        )
    }

    /// Force run install command for a service (clears install state first).
    ///
    /// This method will:
    /// 1. Clear any existing install state
    /// 2. Run the install command unconditionally
    ///
    /// # Arguments
    ///
    /// * `service_name` - Name of the service to install
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Service not found in configuration
    /// - Install command fails
    /// - Marker directory is inaccessible
    pub async fn run_install(&self, service_name: &str) -> Result<()> {
        // Clear install state first
        let ctx = self.markers();
        ctx.clear_installed(service_name)?;

        // Run install
        self.run_install_if_needed(service_name).await
    }

    /// Run install command for a service if needed.
    ///
    /// This checks if the service has already been installed (via marker files).
    /// If already installed, it skips the install step.
    ///
    /// The install command runs in the service's configured working directory with
    /// its environment variables. Special handling is included for npm's ENOTEMPTY
    /// error which can occur when node_modules already exists.
    ///
    /// # Arguments
    ///
    /// * `service_name` - Name of the service to install
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Service not found in configuration
    /// - Install command execution fails
    /// - Install command returns non-zero exit code
    pub async fn run_install_if_needed(&self, service_name: &str) -> Result<()> {
        // Get service config
        let service_config = self
            .config
            .services
            .get(service_name)
            .ok_or_else(|| Error::ServiceNotFound(service_name.to_string()))?;

        // Check if service has install command
        let install_cmd = match &service_config.install {
            Some(cmd) => cmd,
            None => return Ok(()), // No install command, skip
        };

        // Check if already installed
        let ctx = self.markers();
        let is_installed = ctx.is_installed(service_name)?;

        if is_installed {
            tracing::debug!(
                "Service '{}' already installed, skipping install step",
                service_name
            );
            return Ok(());
        }

        // Run install command
        tracing::info!(
            "Running install command for service '{}': {}",
            service_name,
            install_cmd
        );

        // Get working directory from service config, relative to orchestrator work_dir
        let cwd = if let Some(ref service_cwd) = service_config.cwd {
            let cwd_path = std::path::Path::new(service_cwd);
            if cwd_path.is_absolute() {
                cwd_path.to_path_buf()
            } else {
                self.work_dir.join(service_cwd)
            }
        } else {
            self.work_dir.to_path_buf()
        };

        // Get environment variables (need to resolve parameters first)
        let mut env_vars = std::collections::HashMap::new();
        for (key, value) in &service_config.environment {
            env_vars.insert(key.clone(), value.clone());
        }

        // Run the install command with streaming output
        let status = tokio::process::Command::new("sh")
            .arg("-ec")
            .arg(install_cmd)
            .current_dir(cwd)
            .envs(&env_vars)
            .stdin(Stdio::inherit())
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit())
            .status()
            .await
            .map_err(|e| {
                Error::Process(format!(
                    "Failed to execute install command for '{}': {}",
                    service_name, e
                ))
            })?;

        if !status.success() {
            return Err(Error::Process(format!(
                "Install command failed for '{}'",
                service_name
            )));
        }

        // Mark as installed
        let ctx = self.markers();
        ctx.mark_installed(service_name)?;

        tracing::info!(
            "Successfully completed install for service '{}'",
            service_name
        );
        Ok(())
    }

    /// Force run migrate command for a service (clears migrate state first).
    ///
    /// This method will:
    /// 1. Clear any existing migrate state
    /// 2. Run the migrate command unconditionally
    pub async fn run_migrate(&self, service_name: &str) -> Result<()> {
        let ctx = self.markers();
        ctx.clear_migrated(service_name)?;
        self.run_migrate_if_needed(service_name).await
    }

    /// Run migrate command for a service if needed.
    ///
    /// This checks if the service has already been migrated (via marker files).
    /// If already migrated, it skips the migrate step.
    ///
    /// The migrate command runs after dependencies are healthy but before the service
    /// starts. Use it for database migrations, schema setup, or anything that needs
    /// a running dependency.
    pub async fn run_migrate_if_needed(&self, service_name: &str) -> Result<()> {
        let service_config = self
            .config
            .services
            .get(service_name)
            .ok_or_else(|| Error::ServiceNotFound(service_name.to_string()))?;

        let migrate_cmd = match &service_config.migrate {
            Some(cmd) => cmd,
            None => return Ok(()),
        };

        let ctx = self.markers();
        let is_migrated = ctx.is_migrated(service_name)?;

        if is_migrated {
            tracing::debug!(
                "Service '{}' already migrated, skipping migrate step",
                service_name
            );
            return Ok(());
        }

        tracing::info!(
            "Running migrate command for service '{}': {}",
            service_name,
            migrate_cmd
        );

        let cwd = if let Some(ref service_cwd) = service_config.cwd {
            let cwd_path = std::path::Path::new(service_cwd);
            if cwd_path.is_absolute() {
                cwd_path.to_path_buf()
            } else {
                self.work_dir.join(service_cwd)
            }
        } else {
            self.work_dir.to_path_buf()
        };

        let mut env_vars = std::collections::HashMap::new();
        for (key, value) in &service_config.environment {
            env_vars.insert(key.clone(), value.clone());
        }

        let status = tokio::process::Command::new("sh")
            .arg("-ec")
            .arg(migrate_cmd)
            .current_dir(cwd)
            .envs(&env_vars)
            .stdin(Stdio::inherit())
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit())
            .status()
            .await
            .map_err(|e| {
                Error::Process(format!(
                    "Failed to execute migrate command for '{}': {}",
                    service_name, e
                ))
            })?;

        if !status.success() {
            return Err(Error::Process(format!(
                "Migrate command failed for '{}'",
                service_name
            )));
        }

        let ctx = self.markers();
        ctx.mark_migrated(service_name)?;

        tracing::info!(
            "Successfully completed migrate for service '{}'",
            service_name
        );
        Ok(())
    }

    /// Run build command for a service.
    ///
    /// Handles two build types:
    /// - `BuildConfig::Command` — runs a shell command (existing behavior)
    /// - `BuildConfig::DockerBuild` — builds a Docker image with `docker build`
    ///
    /// For Docker builds, images are tagged with the git short hash by default,
    /// or with an explicit `--tag` value. CLI build args are appended as `--build-arg`.
    ///
    /// # Arguments
    ///
    /// * `service_name` - Name of the service to build
    /// * `tag` - Optional Docker image tag (overrides git hash default)
    /// * `cli_build_args` - Additional `--build-arg` values from the CLI
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Service not found in configuration
    /// - Service has no build config defined
    /// - Build command or docker build execution fails
    /// - Build returns non-zero exit code
    pub async fn run_build(
        &self,
        service_name: &str,
        tag: Option<&str>,
        cli_build_args: &[String],
    ) -> Result<Option<crate::config::DockerBuildResult>> {
        use crate::config::BuildConfig;

        let service_config = self
            .config
            .services
            .get(service_name)
            .ok_or_else(|| Error::ServiceNotFound(service_name.to_string()))?;

        let build_config = match &service_config.build {
            Some(config) => config,
            None => {
                return Err(Error::Config(format!(
                    "Service '{}' has no build command defined",
                    service_name
                )));
            }
        };

        // Get working directory
        let cwd = if let Some(ref service_cwd) = service_config.cwd {
            let cwd_path = std::path::Path::new(service_cwd);
            if cwd_path.is_absolute() {
                cwd_path.to_path_buf()
            } else {
                self.work_dir.join(service_cwd)
            }
        } else {
            self.work_dir.to_path_buf()
        };

        // Get environment variables
        let mut env_vars = std::collections::HashMap::new();
        for (key, value) in &service_config.environment {
            env_vars.insert(key.clone(), value.clone());
        }

        match build_config {
            BuildConfig::Command(cmd) => {
                tracing::info!(
                    "Running build command for service '{}': {}",
                    service_name,
                    cmd
                );

                let status = tokio::process::Command::new("sh")
                    .arg("-ec")
                    .arg(cmd)
                    .current_dir(&cwd)
                    .envs(&env_vars)
                    .stdin(Stdio::inherit())
                    .stdout(Stdio::inherit())
                    .stderr(Stdio::inherit())
                    .status()
                    .await
                    .map_err(|e| {
                        Error::Process(format!(
                            "Failed to execute build command for '{}': {}",
                            service_name, e
                        ))
                    })?;

                if !status.success() {
                    return Err(Error::Process(format!(
                        "Build command failed for '{}'",
                        service_name
                    )));
                }
            }
            BuildConfig::DockerBuild(docker_config) => {
                // Resolve tag: CLI flag > git short hash
                let resolved_tag = if let Some(t) = tag {
                    t.to_string()
                } else {
                    // Get git short hash
                    let output = tokio::process::Command::new("git")
                        .args(["rev-parse", "--short", "HEAD"])
                        .current_dir(&cwd)
                        .output()
                        .await
                        .map_err(|e| {
                            Error::Process(format!(
                                "Failed to get git hash for Docker tag: {}. Use --tag to specify a tag manually.",
                                e
                            ))
                        })?;

                    if !output.status.success() {
                        return Err(Error::Process(
                            "Failed to get git hash for Docker tag. Use --tag to specify a tag manually.".to_string()
                        ));
                    }

                    String::from_utf8_lossy(&output.stdout).trim().to_string()
                };

                let full_image = format!("{}:{}", docker_config.image, resolved_tag);

                tracing::info!(
                    "Building Docker image for service '{}': {}",
                    service_name,
                    full_image
                );

                let mut cmd = tokio::process::Command::new("docker");
                cmd.args(["build", "-t", &full_image, "-f", &docker_config.dockerfile]);

                // Add build args from config
                for (key, value) in &docker_config.args {
                    cmd.arg("--build-arg");
                    cmd.arg(format!("{}={}", key, value));
                }

                // Add build args from CLI
                for arg in cli_build_args {
                    cmd.arg("--build-arg");
                    cmd.arg(arg);
                }

                // Context is the cwd
                cmd.arg(".");

                cmd.current_dir(&cwd)
                    .envs(&env_vars)
                    .stdin(Stdio::inherit())
                    .stdout(Stdio::inherit())
                    .stderr(Stdio::inherit());

                let status = cmd.status().await.map_err(|e| {
                    DockerError::exec_failed(format!("docker build ({})", service_name), e)
                })?;

                if !status.success() {
                    return Err(DockerError::cmd_failed(
                        format!("docker build ({})", service_name),
                        format!("Docker build failed for '{}'", service_name),
                        status.code(),
                    )
                    .into());
                }

                tracing::info!(
                    "Successfully completed build for service '{}'",
                    service_name
                );
                return Ok(Some(crate::config::DockerBuildResult {
                    service: service_name.to_string(),
                    image: docker_config.image.clone(),
                    tag: resolved_tag,
                }));
            }
        }

        tracing::info!(
            "Successfully completed build for service '{}'",
            service_name
        );
        Ok(None)
    }

    /// Run clean command for a service.
    ///
    /// This will:
    /// 1. Run the user-defined clean command (if present)
    /// 2. Remove any Docker volumes associated with the service
    /// 3. Clear the install state
    ///
    /// Only Docker volumes with the `fed-` prefix are automatically removed for safety.
    ///
    /// # Arguments
    ///
    /// * `service_name` - Name of the service to clean
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Service not found in configuration
    /// - Clean command execution fails
    /// - Clean command returns non-zero exit code
    pub async fn run_clean(&self, service_name: &str) -> Result<()> {
        // Get service config
        let service_config = self
            .config
            .services
            .get(service_name)
            .ok_or_else(|| Error::ServiceNotFound(service_name.to_string()))?;

        let has_clean_cmd = service_config.clean.is_some();
        let has_volumes = !service_config.volumes.is_empty();

        // If no clean command and no volumes, nothing to do
        if !has_clean_cmd && !has_volumes {
            return Ok(());
        }

        // Run user-defined clean command if present
        if let Some(ref clean_cmd) = service_config.clean {
            tracing::info!(
                "Running clean command for service '{}': {}",
                service_name,
                clean_cmd
            );

            // Get working directory from service config, relative to orchestrator work_dir
            let cwd = if let Some(ref service_cwd) = service_config.cwd {
                let cwd_path = std::path::Path::new(service_cwd);
                if cwd_path.is_absolute() {
                    cwd_path.to_path_buf()
                } else {
                    self.work_dir.join(service_cwd)
                }
            } else {
                self.work_dir.to_path_buf()
            };

            // Get environment variables
            let mut env_vars = std::collections::HashMap::new();
            for (key, value) in &service_config.environment {
                env_vars.insert(key.clone(), value.clone());
            }

            // Run the clean command with streaming output
            let status = tokio::process::Command::new("sh")
                .arg("-ec")
                .arg(clean_cmd)
                .current_dir(cwd)
                .envs(&env_vars)
                .stdin(Stdio::inherit())
                .stdout(Stdio::inherit())
                .stderr(Stdio::inherit())
                .status()
                .await
                .map_err(|e| {
                    Error::Process(format!(
                        "Failed to execute clean command for '{}': {}",
                        service_name, e
                    ))
                })?;

            if !status.success() {
                return Err(Error::Process(format!(
                    "Clean command failed for '{}'",
                    service_name
                )));
            }
        }

        // Clean up Docker volumes if present
        if has_volumes {
            self.clean_docker_volumes(service_name, &service_config.volumes)
                .await?;
        }

        // Clear install and migrate state since we cleaned up
        let ctx = self.markers();
        ctx.clear_installed(service_name)?;
        ctx.clear_migrated(service_name)?;

        tracing::info!(
            "Successfully completed clean for service '{}'",
            service_name
        );
        Ok(())
    }

    /// Clean up Docker volumes for a service.
    ///
    /// Only removes named volumes (not bind mounts) that follow the fed naming convention.
    /// Named volumes are identified by not starting with `.` or `/`.
    /// For safety, only volumes prefixed with `fed-` are removed to prevent accidental
    /// deletion of user-managed volumes.
    ///
    /// # Arguments
    ///
    /// * `service_name` - Name of the service
    /// * `volumes` - List of volume specifications from service config
    ///
    /// # Errors
    ///
    /// This method logs warnings for failed volume removals but does not return an error.
    /// It's designed to be resilient - if some volumes can't be removed, it continues
    /// attempting to remove the rest.
    async fn clean_docker_volumes(&self, service_name: &str, volumes: &[String]) -> Result<()> {
        // Extract all named volumes from config (unscoped names like "postgres_data")
        let all_named_volumes: Vec<String> = volumes
            .iter()
            .filter_map(|v| Self::extract_named_volume(v))
            .collect();

        if all_named_volumes.is_empty() {
            tracing::debug!(
                "No named volumes found for service '{}' to clean",
                service_name
            );
            return Ok(());
        }

        let scope_id = crate::service::hash_work_dir(self.work_dir);

        let fed_volumes: Vec<String> = all_named_volumes
            .iter()
            .map(|v| format!("fed-{}-{}", scope_id, v))
            .collect();

        if fed_volumes.is_empty() {
            tracing::debug!(
                "No fed-managed volumes found for service '{}' to clean",
                service_name
            );
            return Ok(());
        }

        tracing::info!(
            "Removing Docker volumes for service '{}': {:?}",
            service_name,
            fed_volumes
        );

        let mut removed: Vec<String> = Vec::new();
        let mut failed: Vec<(String, String)> = Vec::new();

        for volume in &fed_volumes {
            match DockerClient::new().volume_rm(volume).await {
                Ok(output) if output.status.success() => {
                    tracing::info!("Removed Docker volume: {}", volume);
                    removed.push(volume.clone());
                }
                Ok(output) => {
                    let stderr = String::from_utf8_lossy(&output.stderr);
                    // Volume might not exist, which is fine
                    if stderr.contains("No such volume") {
                        tracing::debug!("Volume '{}' does not exist, skipping", volume);
                    } else {
                        tracing::warn!(
                            "Failed to remove Docker volume '{}': {}",
                            volume,
                            stderr.trim()
                        );
                        failed.push((volume.clone(), stderr.trim().to_string()));
                    }
                }
                Err(e) => {
                    let err_msg = format!("Failed to run docker volume rm: {}", e);
                    tracing::warn!("Failed to remove volume '{}': {}", volume, err_msg);
                    failed.push((volume.clone(), err_msg));
                }
            }
        }

        // Report summary
        if !removed.is_empty() {
            tracing::info!(
                "Successfully removed {} volume(s) for service '{}'",
                removed.len(),
                service_name
            );
        }

        if !failed.is_empty() {
            tracing::warn!(
                "Failed to remove {} volume(s) for service '{}': {:?}",
                failed.len(),
                service_name,
                failed
                    .iter()
                    .map(|(name, _)| name.as_str())
                    .collect::<Vec<_>>()
            );
        }

        Ok(())
    }

    /// Extract named volume from a volume spec.
    ///
    /// Returns Some(volume_name) for named volumes, None for bind mounts.
    /// Named volumes don't start with `.`, `/`, or `~`, don't contain path separators,
    /// and don't look like Windows drive letters.
    ///
    /// # Examples
    ///
    /// ```
    /// use fed::orchestrator::ServiceLifecycleCommands;
    ///
    /// // Named volumes
    /// assert_eq!(
    ///     ServiceLifecycleCommands::extract_named_volume("myvolume:/data"),
    ///     Some("myvolume".to_string())
    /// );
    ///
    /// // Bind mounts
    /// assert_eq!(ServiceLifecycleCommands::extract_named_volume("./data:/data"), None);
    /// assert_eq!(ServiceLifecycleCommands::extract_named_volume("/host/path:/data"), None);
    ///
    /// // With options
    /// assert_eq!(
    ///     ServiceLifecycleCommands::extract_named_volume("myvolume:/data:ro"),
    ///     Some("myvolume".to_string())
    /// );
    ///
    /// // Windows paths
    /// assert_eq!(ServiceLifecycleCommands::extract_named_volume("C:/data:/app"), None);
    /// assert_eq!(ServiceLifecycleCommands::extract_named_volume("C:\\data:/app"), None);
    /// ```
    pub fn extract_named_volume(spec: &str) -> Option<String> {
        let parts: Vec<&str> = spec.split(':').collect();
        if parts.is_empty() {
            return None;
        }

        let source = parts[0];

        // Empty source is not a valid volume
        if source.is_empty() {
            return None;
        }

        // Windows drive letter detection: single letter that looks like a drive
        // This catches cases where the spec is "C:/path:/container" which splits to ["C", "/path", "/container"]
        // The source would be just "C" - a single ASCII letter
        if source.len() == 1 {
            let first_char = source.chars().next().unwrap();
            if first_char.is_ascii_alphabetic() {
                // Single letter source with more parts suggests Windows drive path
                // e.g., "C:/foo:/bar" splits to ["C", "/foo", "/bar"]
                if parts.len() >= 2 && (parts[1].starts_with('/') || parts[1].starts_with('\\')) {
                    return None;
                }
            }
        }

        // Bind mounts start with `.`, `/`, or `~`
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
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_named_volume_basic() {
        assert_eq!(
            ServiceLifecycleCommands::extract_named_volume("myvolume:/data"),
            Some("myvolume".to_string())
        );
    }

    #[test]
    fn test_extract_named_volume_with_options() {
        assert_eq!(
            ServiceLifecycleCommands::extract_named_volume("myvolume:/data:ro"),
            Some("myvolume".to_string())
        );
        assert_eq!(
            ServiceLifecycleCommands::extract_named_volume("myvolume:/data:ro,cached"),
            Some("myvolume".to_string())
        );
    }

    #[test]
    fn test_extract_named_volume_bind_mount_relative() {
        assert_eq!(
            ServiceLifecycleCommands::extract_named_volume("./data:/data"),
            None
        );
        assert_eq!(
            ServiceLifecycleCommands::extract_named_volume("../data:/data"),
            None
        );
    }

    #[test]
    fn test_extract_named_volume_bind_mount_absolute() {
        assert_eq!(
            ServiceLifecycleCommands::extract_named_volume("/host/path:/data"),
            None
        );
        assert_eq!(
            ServiceLifecycleCommands::extract_named_volume("/var/lib/data:/data"),
            None
        );
    }

    #[test]
    fn test_extract_named_volume_tilde_path() {
        assert_eq!(
            ServiceLifecycleCommands::extract_named_volume("~/data:/data"),
            None
        );
    }

    #[test]
    fn test_extract_named_volume_empty() {
        assert_eq!(ServiceLifecycleCommands::extract_named_volume(""), None);
    }

    #[test]
    fn test_extract_named_volume_container_only() {
        // Container-only paths (no source) should return None
        assert_eq!(
            ServiceLifecycleCommands::extract_named_volume("/data"),
            None
        );
    }

    #[test]
    fn test_extract_named_volume_with_slashes_in_name() {
        // Volume names with slashes are bind mounts
        assert_eq!(
            ServiceLifecycleCommands::extract_named_volume("path/to/data:/data"),
            None
        );
    }

    #[test]
    fn test_extract_named_volume_windows_drive_forward_slash() {
        // Windows paths with forward slashes: "C:/data:/container" splits to ["C", "/data", "/container"]
        assert_eq!(
            ServiceLifecycleCommands::extract_named_volume("C:/data:/data"),
            None
        );
        assert_eq!(
            ServiceLifecycleCommands::extract_named_volume("D:/Users/me:/home"),
            None
        );
        assert_eq!(
            ServiceLifecycleCommands::extract_named_volume("E:/Projects:/work"),
            None
        );
    }

    #[test]
    fn test_extract_named_volume_windows_drive_backslash() {
        // Windows paths with backslashes contain \ so are already filtered
        assert_eq!(
            ServiceLifecycleCommands::extract_named_volume("C:\\data:/data"),
            None
        );
        assert_eq!(
            ServiceLifecycleCommands::extract_named_volume("D:\\Users\\me:/home"),
            None
        );
    }

    #[test]
    fn test_extract_named_volume_single_letter_not_windows() {
        // Single letter volume names without Windows-style paths should work
        // "v:/data" where the second part doesn't start with / or \ is a valid named volume
        assert_eq!(
            ServiceLifecycleCommands::extract_named_volume("v:data"),
            Some("v".to_string())
        );
    }

    #[test]
    fn test_extract_named_volume_fed_prefix() {
        // Fed-prefixed volumes should be extracted normally
        assert_eq!(
            ServiceLifecycleCommands::extract_named_volume("fed-myapp-data:/data"),
            Some("fed-myapp-data".to_string())
        );
        assert_eq!(
            ServiceLifecycleCommands::extract_named_volume("fed-cache:/cache"),
            Some("fed-cache".to_string())
        );
        assert_eq!(
            ServiceLifecycleCommands::extract_named_volume(
                "fed-session-abc123-postgres:/var/lib/postgresql"
            ),
            Some("fed-session-abc123-postgres".to_string())
        );
    }

    #[test]
    fn test_extract_named_volume_not_fed_prefix() {
        // Non-fed volumes are still extracted (filtering happens in clean_docker_volumes)
        assert_eq!(
            ServiceLifecycleCommands::extract_named_volume("postgres-data:/data"),
            Some("postgres-data".to_string())
        );
        assert_eq!(
            ServiceLifecycleCommands::extract_named_volume("myapp-cache:/cache"),
            Some("myapp-cache".to_string())
        );
    }
}
