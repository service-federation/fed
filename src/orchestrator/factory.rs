//! Service factory for creating service managers from configuration.
//!
//! This module handles:
//! - Creating appropriate service manager implementations based on service type
//! - Grouping Gradle services for batch execution
//! - Restoring state (PIDs, container IDs) for existing services
//! - Docker container validation

use crate::config::ServiceType;
use crate::error::{Error, Result, validate_pid_for_check};
use crate::service::{
    DockerComposeService, DockerService, ExternalService, GradleService, OneshotService,
    ProcessService, ServiceManager,
};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use super::Orchestrator;

/// Type alias for the service registry entry
type ServiceEntry = Arc<tokio::sync::Mutex<Box<dyn ServiceManager>>>;

impl Orchestrator {
    /// Check if a Docker container is running
    pub(super) fn is_container_running(container_id: &str) -> bool {
        crate::docker::is_container_running_sync(container_id)
    }

    /// Group Gradle services by working directory for batch execution.
    ///
    /// Returns a tuple of:
    /// - `gradle_groups`: Vec of groups, where each group is a list of service names
    ///   that share the same working directory and can be executed together
    /// - `non_gradle`: Vec of service names that are not Gradle services
    ///
    /// This optimization allows multiple Gradle tasks in the same project to be
    /// executed in a single Gradle invocation, significantly reducing startup time.
    pub(super) fn group_gradle_services(
        &self,
        service_names: &[String],
    ) -> (Vec<Vec<String>>, Vec<String>) {
        let mut gradle_by_workdir: HashMap<PathBuf, Vec<String>> = HashMap::new();
        let mut non_gradle = Vec::new();

        for name in service_names {
            if let Some(service) = self.config.services.get(name) {
                if service.service_type() == ServiceType::GradleTask {
                    let work_dir = if let Some(ref cwd) = service.cwd {
                        let cwd_path = PathBuf::from(cwd);
                        if cwd_path.is_absolute() {
                            cwd_path
                        } else {
                            self.work_dir().join(cwd)
                        }
                    } else {
                        self.work_dir().to_path_buf()
                    };

                    gradle_by_workdir
                        .entry(work_dir)
                        .or_default()
                        .push(name.clone());
                } else {
                    non_gradle.push(name.clone());
                }
            } else {
                non_gradle.push(name.clone());
            }
        }

        let gradle_groups: Vec<Vec<String>> = gradle_by_workdir.into_values().collect();
        (gradle_groups, non_gradle)
    }

    /// Create service managers for all configured services.
    ///
    /// This method:
    /// 1. Groups Gradle services by working directory for batch execution
    /// 2. Creates the appropriate service manager for each service type
    /// 3. Restores PIDs and container IDs from the state tracker
    /// 4. Validates that restored processes/containers are still running
    pub(super) async fn create_services(&mut self) -> Result<()> {
        let mut services = self.build_service_managers()?;

        // Restore PIDs and container IDs from state tracker for existing services
        let state_services = { self.state_tracker.read().await.get_services().await };
        self.restore_service_state(&mut services, state_services)
            .await;

        *self.services.write().await = services;
        Ok(())
    }

    /// Like [`Orchestrator::create_services`], but restores PID/container/
    /// status only for rows whose persisted `desired_state` is `Running`.
    ///
    /// Used by the supervisor attach path (`Orchestrator::initialize_supervisor`,
    /// `07-supervisor.md` Design §1): a row the user explicitly stopped
    /// (`desired_state == Stopped`) must never look "attached" to the
    /// supervisor, even if it's still technically alive and hasn't been
    /// purged yet (the design's own words: "rows where desired_state ==
    /// 'stopped' are left alone entirely — not attached, not restarted").
    /// Every configured service still gets a fresh manager object (so the
    /// supervisor can drive a newly-stale, restart-worthy service through
    /// `manager.start()` afterward) — only the *restoration* step is
    /// filtered.
    pub(super) async fn create_services_for_supervisor(&mut self) -> Result<()> {
        let mut services = self.build_service_managers()?;

        let mut state_services = { self.state_tracker.read().await.get_services().await };
        state_services.retain(|_, s| s.desired_state == crate::state::DesiredState::Running);
        self.restore_service_state(&mut services, state_services)
            .await;

        *self.services.write().await = services;
        Ok(())
    }

    /// Build a fresh service manager for every configured service (Gradle
    /// services sharing a working directory are grouped into one manager).
    /// Does not restore any persisted PID/container/status — see
    /// [`Orchestrator::restore_service_state`] for that half.
    fn build_service_managers(&self) -> Result<HashMap<String, ServiceEntry>> {
        let mut services = HashMap::new();
        let mut gradle_grouped = std::collections::HashSet::new();

        // Group ALL Gradle services by working directory, regardless of dependencies
        // This is the practical approach: assume gradle tasks are run together by default
        let all_service_names: Vec<String> = self.config.services.keys().cloned().collect();
        let (gradle_groups, _) = self.group_gradle_services(&all_service_names);

        // Create grouped Gradle services
        for gradle_group in gradle_groups {
            if gradle_group.len() > 1 {
                let service_entry = self.create_gradle_group(&gradle_group)?;
                let group_name = format!("gradle-group-{}", gradle_group.join("-"));

                for service_name in &gradle_group {
                    gradle_grouped.insert(service_name.clone());
                }

                services.insert(group_name, service_entry);
            }
            // If gradle_group.len() == 1, don't add to gradle_grouped, create it normally below
        }

        // Create all services (both grouped and individual)
        for (name, service) in &self.config.services {
            // Skip if already part of a grouped service
            if gradle_grouped.contains(name) {
                continue;
            }

            let service_entry = self.create_service_manager(name, service)?;
            services.insert(name.clone(), service_entry);
        }

        Ok(services)
    }

    /// Create a grouped Gradle service for multiple tasks in the same working directory
    fn create_gradle_group(&self, gradle_group: &[String]) -> Result<ServiceEntry> {
        let group_name = format!("gradle-group-{}", gradle_group.join("-"));

        let mut configs = Vec::new();
        let mut merged_env = HashMap::new();
        let mut work_dir_str = self.work_dir().to_string_lossy().to_string();

        for service_name in gradle_group {
            if let Some(service) = self.config.services.get(service_name) {
                configs.push(service.clone());
                // Merge environments
                for (k, v) in &service.environment {
                    merged_env.insert(k.clone(), v.clone());
                }
                // Use first service's working directory
                if configs.len() == 1
                    && let Some(ref cwd) = service.cwd
                {
                    let cwd_path = PathBuf::from(cwd);
                    work_dir_str = if cwd_path.is_absolute() {
                        cwd.clone()
                    } else {
                        format!("{}/{}", self.work_dir().to_string_lossy(), cwd)
                    };
                }
            }
        }

        let grouped_service =
            GradleService::new_grouped(group_name.clone(), configs, merged_env, work_dir_str);

        Ok(Arc::new(tokio::sync::Mutex::new(
            Box::new(grouped_service) as Box<dyn ServiceManager>
        )))
    }

    /// Create a service manager for a single service
    fn create_service_manager(
        &self,
        name: &str,
        service: &crate::config::Service,
    ) -> Result<ServiceEntry> {
        let service_type = service.service_type();
        let env = service.environment.clone();
        let work_dir = self.work_dir().to_string_lossy().to_string();

        tracing::debug!(
            "Creating service '{}' with work_dir: '{}' (PathBuf: {:?})",
            name,
            work_dir,
            self.work_dir()
        );

        let manager: Box<dyn ServiceManager> = match service_type {
            ServiceType::Process => self.create_process_service(name, service, env, work_dir),
            ServiceType::Docker => self.create_docker_service(name, service, env, work_dir),
            ServiceType::External => self.create_external_service(name, service, env, work_dir),
            ServiceType::GradleTask => Box::new(GradleService::new(
                name.to_string(),
                service.clone(),
                env,
                work_dir,
            )),
            ServiceType::DockerCompose => Box::new(DockerComposeService::new(
                name.to_string(),
                service.clone(),
                env,
                work_dir,
            )?),
            ServiceType::Oneshot => Box::new(OneshotService::new(
                name.to_string(),
                service.clone(),
                env,
                work_dir,
            )),
            ServiceType::Undefined => {
                return Err(Error::Config(format!(
                    "Undefined service type for service '{}'",
                    name
                )));
            }
        };

        Ok(Arc::new(tokio::sync::Mutex::new(manager)))
    }

    /// Create a process service manager
    fn create_process_service(
        &self,
        name: &str,
        service: &crate::config::Service,
        env: HashMap<String, String>,
        work_dir: String,
    ) -> Box<dyn ServiceManager> {
        let log_file_path = if self.output_mode.is_file() {
            let logs_dir = crate::fed_dir::fed_dir(self.work_dir()).join("logs");
            if let Err(e) = crate::fed_dir::ensure_fed_dir(self.work_dir()) {
                tracing::warn!("Failed to create .fed directory: {}", e);
            }
            if let Err(e) = std::fs::create_dir_all(&logs_dir) {
                tracing::warn!("Failed to create logs directory {:?}: {}", logs_dir, e);
                None
            } else {
                let sanitized_name = name
                    .chars()
                    .map(|c| {
                        if c.is_alphanumeric() || c == '-' || c == '_' {
                            c
                        } else {
                            '_'
                        }
                    })
                    .collect::<String>();
                Some(logs_dir.join(format!("{}.log", sanitized_name)))
            }
        } else {
            None
        };

        Box::new(ProcessService::new(
            name.to_string(),
            service.clone(),
            env,
            work_dir,
            self.output_mode,
            log_file_path,
        ))
    }

    /// Create a docker service manager
    fn create_docker_service(
        &self,
        name: &str,
        service: &crate::config::Service,
        env: HashMap<String, String>,
        work_dir: String,
    ) -> Box<dyn ServiceManager> {
        let session_id = self.isolation_id.clone();

        Box::new(DockerService::new(
            name.to_string(),
            service.clone(),
            env,
            work_dir,
            session_id,
        ))
    }

    /// Create an external service manager
    fn create_external_service(
        &self,
        name: &str,
        service: &crate::config::Service,
        env: HashMap<String, String>,
        work_dir: String,
    ) -> Box<dyn ServiceManager> {
        let mut ext_service =
            ExternalService::new(name.to_string(), service.clone(), env, work_dir);
        ext_service.set_dependencies(self.config.dependencies.clone());
        Box::new(ext_service)
    }

    /// Restore PIDs and container IDs for existing services from a
    /// caller-supplied snapshot of persisted state.
    ///
    /// Taking the snapshot as a parameter (rather than fetching it here)
    /// lets callers filter it first — [`Orchestrator::create_services_for_supervisor`]
    /// passes only `desired_state == Running` rows, so a service the user
    /// explicitly stopped never gets its PID/status restored even if the
    /// row hasn't been purged yet.
    async fn restore_service_state(
        &self,
        services: &mut HashMap<String, ServiceEntry>,
        state_services: HashMap<String, crate::state::ServiceState>,
    ) {
        for (service_id, service_state) in state_services {
            // Extract service name from ID (format: "namespace/name")
            let service_name = service_id.split('/').next_back().unwrap_or(&service_id);

            if let Some(manager_arc) = services.get_mut(service_name) {
                let mut manager = manager_arc.lock().await;

                // Restore PID for process and gradle services
                if let Some(pid) = service_state.pid {
                    self.restore_process_pid(
                        &mut manager,
                        service_name,
                        pid,
                        service_state.started_at,
                    )
                    .await;
                    self.restore_gradle_pid(&mut manager, service_name, pid)
                        .await;
                }

                // Restore container ID for docker services
                if let Some(container_id) = &service_state.container_id {
                    self.restore_container_id(&mut manager, service_name, container_id)
                        .await;
                }

                // Oneshots have no PID/container to restore — carry over their
                // persisted status (e.g. Completed) so `fed status` reflects the
                // last run honestly. `restore_status` deliberately does not set
                // the run flag, so the oneshot still re-runs on the next start.
                if let Some(oneshot) = manager.as_any_mut().downcast_mut::<OneshotService>() {
                    oneshot.restore_status(service_state.status);
                }

                // Compose services persist no PID (they have no direct child
                // process) but DO persist a container id since v7.2.1 (see
                // `compose.rs`'s post-`compose up` capture) — their
                // containers live under a compose project name. When the
                // persisted status says the service was live, verify with a
                // real `compose ps` and restore Running so a fresh process
                // reports (and can stop) it instead of defaulting to Stopped.
                let is_compose = manager
                    .as_any_mut()
                    .downcast_mut::<DockerComposeService>()
                    .is_some();
                if is_compose
                    && matches!(
                        service_state.status,
                        crate::service::Status::Starting
                            | crate::service::Status::Running
                            | crate::service::Status::Healthy
                            | crate::service::Status::Failing
                    )
                    && manager.health().await.unwrap_or(false)
                    && let Some(compose) =
                        manager.as_any_mut().downcast_mut::<DockerComposeService>()
                {
                    compose.restore_status(crate::service::Status::Running);
                }
            }
        }
    }

    /// Restore a process PID if the process is still running
    async fn restore_process_pid(
        &self,
        manager: &mut Box<dyn ServiceManager>,
        service_name: &str,
        pid: u32,
        started_at: chrono::DateTime<chrono::Utc>,
    ) {
        if let Some(process_service) = manager.as_any_mut().downcast_mut::<ProcessService>() {
            self.restore_pid_for_service(service_name, pid, || {
                process_service.set_pid_with_start_time(pid, Some(started_at));
            })
            .await;
        }
    }

    /// Restore a Gradle service PID if the process is still running
    async fn restore_gradle_pid(
        &self,
        manager: &mut Box<dyn ServiceManager>,
        service_name: &str,
        pid: u32,
    ) {
        if let Some(gradle_service) = manager.as_any_mut().downcast_mut::<GradleService>() {
            self.restore_pid_for_service(service_name, pid, || {
                gradle_service.set_pid(pid);
            })
            .await;
        }
    }

    /// Validate a PID and either restore it via the callback or unregister the stale service.
    ///
    /// Shared logic for restoring process and gradle PIDs. On Unix,
    /// sends signal 0 to check whether the process is alive. On other platforms,
    /// unconditionally calls `set_pid` as a fallback.
    async fn restore_pid_for_service(&self, service_name: &str, pid: u32, set_pid: impl FnOnce()) {
        #[cfg(unix)]
        {
            use nix::sys::signal::kill;

            if let Some(nix_pid) = validate_pid_for_check(pid) {
                if kill(nix_pid, None).is_ok() {
                    tracing::debug!("Restoring PID {} for service '{}'", pid, service_name);
                    set_pid();
                } else {
                    tracing::warn!(
                        "Skipping PID {} for service '{}' - process no longer exists",
                        pid,
                        service_name
                    );
                    self.unregister_stale_service(service_name).await;
                }
            } else {
                tracing::error!(
                    "Cannot restore PID {} for service '{}' - invalid PID",
                    pid,
                    service_name
                );
                self.unregister_stale_service(service_name).await;
            }
        }

        #[cfg(not(unix))]
        {
            tracing::warn!(
                "Process validation not available on this platform, restoring PID {}",
                pid
            );
            set_pid();
        }
    }

    /// Remove a service from the state tracker, logging on failure.
    async fn unregister_stale_service(&self, service_name: &str) {
        if let Err(e) = self
            .state_tracker
            .write()
            .await
            .unregister_service(service_name)
            .await
        {
            tracing::warn!(
                "Failed to unregister stale service '{}': {}",
                service_name,
                e
            );
        }
    }

    /// Restore a container ID if the container is still running
    async fn restore_container_id(
        &self,
        manager: &mut Box<dyn ServiceManager>,
        service_name: &str,
        container_id: &str,
    ) {
        if let Some(docker_service) = manager.as_any_mut().downcast_mut::<DockerService>() {
            // Verify the container still exists before restoring
            if Self::is_container_running(container_id) {
                tracing::debug!(
                    "Restoring container ID {} for service '{}'",
                    container_id,
                    service_name
                );
                docker_service.set_container_id(container_id.to_string());
            } else {
                tracing::warn!(
                    "Skipping container ID {} for service '{}' - container no longer exists",
                    container_id,
                    service_name
                );
                self.unregister_stale_service(service_name).await;
            }
        }
    }
}

#[cfg(test)]
// clippy::disallowed_methods (clippy.toml) fires on these library-internal
// unit-test call sites too (it matches on the resolved item path, not crate
// boundaries) — same-crate, lower-risk, already bounded by #[cfg(test)], so
// they're allowed here rather than migrated to a test-only helper.
#[allow(clippy::disallowed_methods)]
mod tests {
    use super::*;
    use crate::config::Parser;

    /// Helper: parse YAML config and create an Orchestrator (no initialize — no ports, no Docker).
    async fn orchestrator_from_yaml(yaml: &str) -> Orchestrator {
        let parser = Parser::new();
        let config = parser.parse_config(yaml).expect("valid YAML");
        let temp = tempfile::tempdir().unwrap();
        Orchestrator::new(config, temp.path().to_path_buf())
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn test_gradle_services_grouped_by_cwd() {
        let orch = orchestrator_from_yaml(
            r#"
services:
  database:
    image: postgres:15

  auth-service:
    gradle_task: ':auth-service:bootRun'
    depends_on:
      - database

  user-service:
    gradle_task: ':user-service:bootRun'
    depends_on:
      - database

  notification-service:
    gradle_task: ':notification:bootRun'
    cwd: 'services/notification'
    depends_on:
      - user-service

  analytics-service:
    gradle_task: ':analytics:bootRun'
    depends_on:
      - user-service
"#,
        )
        .await;

        let names: Vec<String> = orch.config.services.keys().cloned().collect();
        let (gradle_groups, non_gradle) = orch.group_gradle_services(&names);

        // database is not a gradle service
        assert!(non_gradle.contains(&"database".to_string()));

        // auth, user, analytics share the default cwd → one group
        let main_group = gradle_groups
            .iter()
            .find(|g| g.contains(&"auth-service".to_string()))
            .expect("should have a group containing auth-service");
        assert!(
            main_group.contains(&"user-service".to_string()),
            "user-service should be in same group as auth-service"
        );
        assert!(
            main_group.contains(&"analytics-service".to_string()),
            "analytics-service should be in same group as auth-service (same cwd)"
        );

        // notification-service has a different cwd → separate group
        assert!(
            !main_group.contains(&"notification-service".to_string()),
            "notification-service has different cwd, must not be in main group"
        );
        let notif_group = gradle_groups
            .iter()
            .find(|g| g.contains(&"notification-service".to_string()))
            .expect("notification-service should be in its own group");
        assert_eq!(notif_group.len(), 1);
    }

    #[tokio::test]
    async fn test_gradle_grouping_service_count() {
        // After create_services, grouped services merge into one entry.
        // Expected: database + gradle-group(auth+user+analytics) + notification = 3
        let orch = orchestrator_from_yaml(
            r#"
services:
  database:
    image: postgres:15

  auth-service:
    gradle_task: ':auth-service:bootRun'
    depends_on:
      - database

  user-service:
    gradle_task: ':user-service:bootRun'
    depends_on:
      - database

  notification-service:
    gradle_task: ':notification:bootRun'
    cwd: 'services/notification'
    depends_on:
      - user-service

  analytics-service:
    gradle_task: ':analytics:bootRun'
    depends_on:
      - user-service
"#,
        )
        .await;

        let names: Vec<String> = orch.config.services.keys().cloned().collect();
        let (gradle_groups, non_gradle) = orch.group_gradle_services(&names);

        // 1 non-gradle (database)
        assert_eq!(non_gradle.len(), 1);

        // 2 gradle groups: one with 3 services (same cwd), one with 1 (different cwd)
        assert_eq!(gradle_groups.len(), 2);

        let mut sizes: Vec<usize> = gradle_groups.iter().map(|g| g.len()).collect();
        sizes.sort();
        assert_eq!(sizes, vec![1, 3]);
    }

    #[tokio::test]
    async fn test_single_gradle_service_not_grouped() {
        let orch = orchestrator_from_yaml(
            r#"
services:
  app:
    gradle_task: ':app:bootRun'
"#,
        )
        .await;

        let names: Vec<String> = orch.config.services.keys().cloned().collect();
        let (gradle_groups, non_gradle) = orch.group_gradle_services(&names);

        assert!(non_gradle.is_empty());
        // Single gradle service → group of 1 (create_services treats len==1 as ungrouped)
        assert_eq!(gradle_groups.len(), 1);
        assert_eq!(gradle_groups[0].len(), 1);
    }

    #[tokio::test]
    async fn test_no_gradle_services() {
        let orch = orchestrator_from_yaml(
            r#"
services:
  web:
    process: "node server.js"
  db:
    image: postgres:15
"#,
        )
        .await;

        let names: Vec<String> = orch.config.services.keys().cloned().collect();
        let (gradle_groups, non_gradle) = orch.group_gradle_services(&names);

        assert!(gradle_groups.is_empty());
        assert_eq!(non_gradle.len(), 2);
    }

    #[tokio::test]
    async fn test_absolute_cwd_separates_groups() {
        let orch = orchestrator_from_yaml(
            r#"
services:
  svc-a:
    gradle_task: ':a:run'
  svc-b:
    gradle_task: ':b:run'
    cwd: '/tmp/other-project'
"#,
        )
        .await;

        let names: Vec<String> = orch.config.services.keys().cloned().collect();
        let (gradle_groups, _) = orch.group_gradle_services(&names);

        assert_eq!(
            gradle_groups.len(),
            2,
            "different cwds must produce separate groups"
        );
        for group in &gradle_groups {
            assert_eq!(group.len(), 1);
        }
    }

    #[tokio::test]
    async fn test_isolation_id_produces_unique_container_name() {
        // Verify that two orchestrators with different isolation_ids
        // produce different Docker container names for the same service.
        let work_dir = std::path::Path::new("/tmp/test-project");

        // Parent: no isolation_id → uses work_dir hash
        let parent_name = crate::service::docker_container_name("postgres", None, work_dir);

        // Child: with isolation_id → uses isolation_id for scoping
        let child_name =
            crate::service::docker_container_name("postgres", Some("iso-deadbeef"), work_dir);

        assert_ne!(
            parent_name, child_name,
            "Isolated container name must differ from parent: parent={}, child={}",
            parent_name, child_name
        );
        assert!(
            child_name.contains("iso-deadbeef"),
            "Isolated container name should contain the isolation_id: {}",
            child_name
        );
    }

    #[tokio::test]
    async fn test_isolation_id_flows_through_factory() {
        let parser = Parser::new();
        let config = parser
            .parse_config(
                r#"
services:
  db:
    image: postgres:15
"#,
            )
            .expect("valid YAML");

        let temp = tempfile::tempdir().unwrap();
        let mut orch = Orchestrator::new_ephemeral(config, temp.path().to_path_buf())
            .await
            .unwrap();

        // Without isolation_id
        assert!(
            orch.isolation_id.is_none(),
            "isolation_id should be None by default"
        );

        // Set isolation_id
        orch.set_isolation_id("iso-cafebabe".to_string());
        assert_eq!(
            orch.isolation_id.as_deref(),
            Some("iso-cafebabe"),
            "isolation_id should be set after set_isolation_id()"
        );
    }
}
