//! External service expansion for legacy dependency-based external services.
//!
//! This module handles the expansion of services with `type: external` that reference
//! a `dependency` field pointing to an external git repository.
//!
//! # Architecture
//!
//! The expansion process:
//! 1. Find all services with `type: external`
//! 2. Clone/update the external repository
//! 3. Load the external config (`fed.yaml` or `service-federation.yaml`)
//! 4. Resolve parameter templates from parent to external
//! 5. Import the target service and its dependencies with namespacing
//! 6. Adjust paths and dependencies for the imported services

use crate::config::{Config, DependsOn, HealthCheck, Parser, Service};
use crate::dependency::GitOperations;
use crate::error::{Error, Result};
use crate::parameter::Resolver;
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};

/// Expands external services into the parent configuration.
///
/// This handles the legacy expansion of services that use `type: external`
/// with a `dependency` field referencing an external repository.
pub struct ExternalServiceExpander<'a> {
    config: &'a Config,
    resolver: &'a Resolver,
    work_dir: PathBuf,
}

impl<'a> ExternalServiceExpander<'a> {
    /// Create a new expander with references to config and resolver.
    pub fn new(config: &'a Config, resolver: &'a Resolver, work_dir: PathBuf) -> Self {
        Self {
            config,
            resolver,
            work_dir,
        }
    }

    /// Expand all external services, returning a new config with expanded services.
    ///
    /// This is the main entry point for external service expansion.
    pub async fn expand(&self) -> Result<Config> {
        let mut config = self.config.clone();

        // Find all external services
        let external_services = self.find_external_services();

        // Expand each external service
        for (service_name, service_config) in external_services {
            // In a scoped run, an external service whose parameter mapping
            // references a deferred parameter (an out-of-scope missing manual
            // secret, or a value derived from one) is outside the scanned
            // closure and will never be spawned. Skip expansion — otherwise
            // `build_parameter_mapping` resolves its templates eagerly and
            // hard-fails on a value this run never fetches. The unexpanded
            // placeholder stays in the config and is dropped by the same
            // deferral check in `resolve_config`. Unscoped runs defer nothing.
            if self.resolver.service_should_defer(&service_config) {
                tracing::debug!(
                    "deferring out-of-scope external service '{}': its parameter mapping references a deferred parameter",
                    service_name
                );
                continue;
            }
            self.expand_single_service(&mut config, &service_name, &service_config)
                .await?;
        }

        Ok(config)
    }

    /// Find all services with type: external
    fn find_external_services(&self) -> Vec<(String, Service)> {
        self.config
            .services
            .iter()
            .filter(|(_, service)| service.service_type() == crate::config::ServiceType::External)
            .map(|(name, service)| (name.clone(), service.clone()))
            .collect()
    }

    /// Expand a single external service into the config
    async fn expand_single_service(
        &self,
        config: &mut Config,
        service_name: &str,
        service_config: &Service,
    ) -> Result<()> {
        // Get dependency info
        let dep_name = service_config.dependency.as_ref().ok_or_else(|| {
            Error::Config(format!(
                "External service '{}' missing dependency",
                service_name
            ))
        })?;

        let dependency = config.dependencies.get(dep_name).ok_or_else(|| {
            Error::Config(format!(
                "Dependency '{}' not found for service '{}'",
                dep_name, service_name
            ))
        })?;

        // Clone/update the external repo
        let target_path = self.resolve_dependency_path(dep_name, dependency)?;

        // Load external config
        let external_config = self.load_external_config(&target_path)?;

        // Build parameter mapping from parent to external
        let parameter_mapping = self.build_parameter_mapping(service_config)?;

        // Get the target service name in external config
        let target_service_name = service_config.service.as_ref().ok_or_else(|| {
            Error::Config(format!(
                "External service '{}' missing 'service' field",
                service_name
            ))
        })?;

        // Check if target service exists in external config
        if !external_config.services.contains_key(target_service_name) {
            return Err(Error::Config(format!(
                "Service '{}' not found in external config in {:?}",
                target_service_name, target_path
            )));
        }

        // Collect all services to import (target + dependencies)
        let services_to_import =
            collect_service_dependencies(&external_config, target_service_name);

        // Import each service with adjustments
        for import_name in services_to_import {
            if let Some(import_service) = external_config.services.get(&import_name) {
                let namespaced_name = if import_name == *target_service_name {
                    service_name.to_string() // Main service keeps original name
                } else {
                    format!("{}:{}", service_name, import_name)
                };

                let adjusted_service = self.adjust_imported_service(
                    import_service,
                    service_name,
                    &target_path,
                    &parameter_mapping,
                )?;

                config.services.insert(namespaced_name, adjusted_service);
            }
        }

        Ok(())
    }

    /// Resolve the path for an external dependency (clone if needed)
    fn resolve_dependency_path(
        &self,
        dep_name: &str,
        dependency: &crate::config::Dependency,
    ) -> Result<PathBuf> {
        if dependency.repo.starts_with("file://") {
            let relative_path = dependency.repo.trim_start_matches("file://");
            Ok(self.work_dir.join(relative_path))
        } else {
            let cache_dir = self.work_dir.join(".service-federation/dependencies");
            std::fs::create_dir_all(&cache_dir)?;

            let repo_name = dependency
                .repo
                .split('/')
                .next_back()
                .unwrap_or(dep_name)
                .trim_end_matches(".git");

            let target = cache_dir.join(repo_name);

            GitOperations::clone_or_update(
                &dependency.repo,
                dependency.branch.as_deref(),
                &target,
            )?;

            Ok(target)
        }
    }

    /// Load external config from a repository path
    fn load_external_config(&self, repo_path: &Path) -> Result<Config> {
        let parser = Parser::new();
        let config_path =
            crate::config::discovery::config_file_in_dir(repo_path).ok_or_else(|| {
                Error::Config(format!(
                    "External config not found at {:?}",
                    repo_path.join(crate::config::discovery::DEFAULT_CONFIG_FILENAME)
                ))
            })?;

        parser.load_config(&config_path)
    }

    /// Build parameter mapping from parent templates to resolved values
    fn build_parameter_mapping(&self, service_config: &Service) -> Result<HashMap<String, String>> {
        let mut parameter_mapping = HashMap::new();

        for (external_param, parent_template) in &service_config.parameters {
            let resolved_value = self
                .resolver
                .resolve_template(parent_template, self.resolver.get_resolved_parameters())?;
            parameter_mapping.insert(external_param.clone(), resolved_value);
        }

        Ok(parameter_mapping)
    }

    /// Adjust an imported service for the parent context
    fn adjust_imported_service(
        &self,
        import_service: &Service,
        parent_service_name: &str,
        target_path: &Path,
        parameter_mapping: &HashMap<String, String>,
    ) -> Result<Service> {
        let mut adjusted = import_service.clone();

        // Adjust CWD to be relative to external repo
        adjusted.cwd = Some(if let Some(ref cwd) = adjusted.cwd {
            target_path.join(cwd).to_string_lossy().to_string()
        } else {
            target_path.to_string_lossy().to_string()
        });

        // Resolve templates in various fields
        if let Some(ref process) = adjusted.process {
            adjusted.process = Some(self.resolver.resolve_template(process, parameter_mapping)?);
        }

        if let Some(ref install) = adjusted.install {
            adjusted.install = Some(self.resolver.resolve_template(install, parameter_mapping)?);
        }

        // Resolve environment variables
        let mut resolved_env = HashMap::new();
        for (key, value) in &adjusted.environment {
            resolved_env.insert(
                key.clone(),
                self.resolver.resolve_template(value, parameter_mapping)?,
            );
        }
        adjusted.environment = resolved_env;

        // Resolve ports
        let mut resolved_ports = Vec::new();
        for port in &adjusted.ports {
            resolved_ports.push(self.resolver.resolve_template(port, parameter_mapping)?);
        }
        adjusted.ports = resolved_ports;

        // Resolve healthcheck
        if let Some(ref healthcheck) = adjusted.healthcheck {
            adjusted.healthcheck = Some(self.resolve_healthcheck(healthcheck, parameter_mapping)?);
        }

        // Adjust dependencies to use namespaced names
        adjusted.depends_on = adjusted
            .depends_on
            .iter()
            .map(|dep| DependsOn::Simple(format!("{}:{}", parent_service_name, dep.service_name())))
            .collect();

        Ok(adjusted)
    }

    /// Resolve templates in a healthcheck
    fn resolve_healthcheck(
        &self,
        healthcheck: &HealthCheck,
        parameter_mapping: &HashMap<String, String>,
    ) -> Result<HealthCheck> {
        Ok(match healthcheck {
            HealthCheck::HttpGet { http_get, timeout } => HealthCheck::HttpGet {
                http_get: self
                    .resolver
                    .resolve_template(http_get, parameter_mapping)?,
                timeout: timeout.clone(),
            },
            HealthCheck::CommandMap { command, timeout } => HealthCheck::CommandMap {
                command: self.resolver.resolve_template(command, parameter_mapping)?,
                timeout: timeout.clone(),
            },
            HealthCheck::Command(cmd) => {
                HealthCheck::Command(self.resolver.resolve_template(cmd, parameter_mapping)?)
            }
        })
    }
}

/// Collect all services that need to be imported (target + dependencies).
///
/// Uses depth-first traversal to collect dependencies before dependents,
/// ensuring proper ordering for import.
pub fn collect_service_dependencies(config: &Config, target_service: &str) -> Vec<String> {
    let mut result = Vec::new();
    let mut visited = HashSet::new();

    collect_deps_recursive(config, target_service, &mut visited, &mut result);

    result
}

/// Recursively collect service dependencies in dependency-first order.
fn collect_deps_recursive(
    config: &Config,
    service_name: &str,
    visited: &mut HashSet<String>,
    result: &mut Vec<String>,
) {
    if visited.contains(service_name) {
        return;
    }
    visited.insert(service_name.to_string());

    if let Some(service) = config.services.get(service_name) {
        // First collect dependencies
        for dep in &service.depends_on {
            collect_deps_recursive(config, dep.service_name(), visited, result);
        }

        // Then add this service
        result.push(service_name.to_string());
    }
}

#[cfg(test)]
#[allow(clippy::field_reassign_with_default)]
mod tests {
    use super::*;

    fn create_test_config() -> Config {
        let mut config = Config::default();

        // Service A depends on B
        let mut service_a = Service::default();
        service_a.depends_on = vec![DependsOn::Simple("service_b".to_string())];
        config.services.insert("service_a".to_string(), service_a);

        // Service B depends on C
        let mut service_b = Service::default();
        service_b.depends_on = vec![DependsOn::Simple("service_c".to_string())];
        config.services.insert("service_b".to_string(), service_b);

        // Service C has no dependencies
        config
            .services
            .insert("service_c".to_string(), Service::default());

        config
    }

    #[test]
    fn test_collect_service_dependencies_simple() {
        let config = create_test_config();

        let deps = collect_service_dependencies(&config, "service_c");
        assert_eq!(deps, vec!["service_c"]);
    }

    #[test]
    fn test_collect_service_dependencies_with_chain() {
        let config = create_test_config();

        let deps = collect_service_dependencies(&config, "service_a");
        // Should be: C first, then B, then A
        assert_eq!(deps, vec!["service_c", "service_b", "service_a"]);
    }

    #[test]
    fn test_collect_service_dependencies_nonexistent() {
        let config = create_test_config();

        let deps = collect_service_dependencies(&config, "nonexistent");
        assert!(deps.is_empty());
    }

    #[test]
    fn test_collect_service_dependencies_partial_chain() {
        let config = create_test_config();

        let deps = collect_service_dependencies(&config, "service_b");
        assert_eq!(deps, vec!["service_c", "service_b"]);
    }
}
