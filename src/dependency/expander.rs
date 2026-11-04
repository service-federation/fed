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

use crate::config::{Config, DependsOn, Parser, Service};
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

        // Only the directly-named target service must be exposed; services
        // pulled in transitively through its own dependency tree (collected
        // below) are exempt. This mirrors subsystem B's (dormant)
        // `merge_external_services` policy in `dependency/external.rs`.
        let target_service = &external_config.services[target_service_name];
        if !target_service.expose {
            return Err(Error::Config(format!(
                "External service '{}' (dependency '{}') is not marked expose: true in its own config. Mark it exposed before importing it as '{}'",
                target_service_name, dep_name, service_name
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
                );

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
    ) -> Service {
        let mut adjusted = import_service.clone();

        // Adjust CWD to be relative to external repo
        adjusted.cwd = Some(if let Some(ref cwd) = adjusted.cwd {
            target_path.join(cwd).to_string_lossy().to_string()
        } else {
            target_path.to_string_lossy().to_string()
        });

        // Keep templates intact until the normal resolver renders all fields.
        // Eager rendering here followed by parent resolution would scan mapped
        // values a second time, allowing literal {{OTHER}} data to become a
        // parent-parameter reference (and bypassing shell quoting).
        adjusted.template_parameters = Some(parameter_mapping.clone());

        // Adjust dependencies to use namespaced names
        adjusted.depends_on = adjusted
            .depends_on
            .iter()
            .map(|dep| DependsOn::Simple(format!("{}:{}", parent_service_name, dep.service_name())))
            .collect();

        adjusted
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
    use crate::config::HealthCheck;

    #[test]
    fn imported_shell_commands_escape_mapped_parameter_values() {
        let config = Config::default();
        let resolver = Resolver::new();
        let dir = tempfile::tempdir().unwrap();
        let expander = ExternalServiceExpander::new(&config, &resolver, dir.path().to_path_buf());
        let marker = dir.path().join("injected");
        let payload = format!(
            "literal {{{{OTHER}}}}; printf injected > {}; #",
            marker.display()
        );
        let mapping = HashMap::from([("MESSAGE".to_string(), payload.clone())]);
        let command = "printf '%s' {{MESSAGE}}".to_string();

        for healthcheck in [
            HealthCheck::Command(command.clone()),
            HealthCheck::CommandMap {
                command: command.clone(),
                timing: crate::config::HealthCheckTiming {
                    timeout: Some("2s".to_string()),
                    ..Default::default()
                },
            },
        ] {
            let imported = Service {
                process: Some(command.clone()),
                install: Some(command.clone()),
                migrate: Some(command.clone()),
                environment: HashMap::from([(
                    "RAW_MESSAGE".to_string(),
                    "{{MESSAGE}}".to_string(),
                )]),
                healthcheck: Some(healthcheck),
                ..Default::default()
            };
            let adjusted =
                expander.adjust_imported_service(&imported, "parent", dir.path(), &mapping);
            let mut expanded = Config::default();
            expanded.services.insert("imported".to_string(), adjusted);
            expanded.parameters.insert(
                "OTHER".to_string(),
                crate::config::Parameter {
                    value: Some("parent-only-secret".to_string()),
                    ..Default::default()
                },
            );
            let staged = expanded.clone();
            let resolved = Resolver::new().resolve_config(&staged).unwrap();
            assert!(resolved.services["imported"].template_parameters.is_none());
            // Consumers use resolved fields, including after a clone or Serde
            // round-trip; no hidden import scope is needed downstream.
            let yaml = serde_yaml::to_string(&resolved).unwrap();
            let reloaded: Config = serde_yaml::from_str(&yaml).unwrap();
            let adjusted = &reloaded.services["imported"];
            for command in [
                adjusted.process.as_deref().unwrap(),
                adjusted.install.as_deref().unwrap(),
                adjusted.migrate.as_deref().unwrap(),
                adjusted
                    .healthcheck
                    .as_ref()
                    .unwrap()
                    .get_command()
                    .unwrap(),
            ] {
                let output = std::process::Command::new("bash")
                    .args(["-c", command])
                    .output()
                    .unwrap();
                assert!(output.status.success());
                assert_eq!(String::from_utf8(output.stdout).unwrap(), payload);
                assert!(
                    !marker.exists(),
                    "imported command executed parameter contents"
                );
            }
            assert_eq!(adjusted.environment.get("RAW_MESSAGE"), Some(&payload));
        }
    }

    #[test]
    fn imported_templates_cannot_read_unmapped_parent_parameters() {
        let mut config = Config::default();
        config.parameters.insert(
            "PRIVATE".to_string(),
            crate::config::Parameter {
                value: Some("parent-only-secret".to_string()),
                ..Default::default()
            },
        );
        let resolver = Resolver::new();
        let dir = tempfile::tempdir().unwrap();
        let expander = ExternalServiceExpander::new(&config, &resolver, dir.path().to_path_buf());
        let imported = Service {
            process: Some("printf '%s' {{PRIVATE}}".to_string()),
            ..Default::default()
        };
        let adjusted =
            expander.adjust_imported_service(&imported, "parent", dir.path(), &HashMap::new());
        let yaml = serde_yaml::to_string(&adjusted).unwrap();
        assert!(!yaml.contains("template_parameters"));
        config.services.insert("imported".to_string(), adjusted);
        let error = Resolver::new()
            .resolve_config(&config)
            .unwrap_err()
            .to_string();
        assert!(error.contains("PRIVATE"));
        assert!(!error.contains("parent-only-secret"));
    }

    #[tokio::test]
    async fn imported_scope_is_rebuilt_from_source_on_reload() {
        let dir = tempfile::tempdir().unwrap();
        let repo = dir.path().join("external");
        std::fs::create_dir_all(&repo).unwrap();
        std::fs::write(
            repo.join("fed.yaml"),
            r#"
services:
  target:
    expose: true
    process: printf '%s' "{{MESSAGE}}"
"#,
        )
        .unwrap();
        let payload = "literal {{PARENT_ONLY}} $(not-a-command) 'quotes'";
        let mut source = make_importer_config("external");
        source
            .parameters
            .insert("VALUE".to_string(), crate::config::Parameter::default());
        source.env_file = vec![".env".to_string()];
        crate::parameter::secret::write_env_file(
            &dir.path().join(".env"),
            &[("VALUE".to_string(), payload.to_string())],
        )
        .unwrap();
        source
            .services
            .get_mut("imported")
            .unwrap()
            .parameters
            .insert("MESSAGE".to_string(), "{{VALUE}}".to_string());
        let path = dir.path().join("fed.yaml");
        std::fs::write(&path, serde_yaml::to_string(&source).unwrap()).unwrap();

        for _ in 0..2 {
            // CLI start/restart and the supervisor load the source config again;
            // they do not cache an intermediate expanded config via Serde.
            let mut loaded = Parser::new().load_config(&path).unwrap();
            let mut resolver = Resolver::new();
            resolver.set_work_dir(dir.path());
            resolver.resolve_parameters(&mut loaded).unwrap();
            let expanded =
                ExternalServiceExpander::new(&loaded, &resolver, dir.path().to_path_buf())
                    .expand()
                    .await
                    .unwrap();
            let resolved = resolver.resolve_config(&expanded).unwrap();
            let command = resolved.services["imported"].process.as_ref().unwrap();
            let output = std::process::Command::new("bash")
                .args(["-c", command])
                .output()
                .unwrap();
            assert!(output.status.success());
            assert_eq!(String::from_utf8(output.stdout).unwrap(), payload);
            assert!(resolved.services["imported"].template_parameters.is_none());
        }
    }

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

    // ── `expose: true` enforcement (D1 Option A, subsystem A) ───────────
    //
    // These build a real tempdir fixture — a parent config plus an external
    // `file://`-referenced repo with its own fed.yaml — and drive
    // `expand_single_service` end-to-end, since nothing else in the crate
    // exercises `ExternalServiceExpander::expand()` against a real fixture.

    /// Write an external repo at `<tempdir>/<dir_name>/fed.yaml` containing a
    /// `target` service (optionally exposed) that depends on a `helper`
    /// service (never exposed, to prove the transitive exemption).
    fn write_external_repo(root: &Path, dir_name: &str, target_expose: bool) -> PathBuf {
        let repo_dir = root.join(dir_name);
        std::fs::create_dir_all(&repo_dir).unwrap();

        let expose_line = if target_expose {
            "    expose: true\n"
        } else {
            ""
        };
        let yaml = format!(
            "services:\n\
             \x20\x20target:\n\
             \x20\x20\x20\x20process: \"echo target\"\n\
             {expose_line}\
             \x20\x20\x20\x20depends_on:\n\
             \x20\x20\x20\x20\x20\x20- helper\n\
             \x20\x20helper:\n\
             \x20\x20\x20\x20process: \"echo helper\"\n\
             entrypoint: target\n"
        );

        std::fs::write(repo_dir.join("fed.yaml"), yaml).unwrap();
        repo_dir
    }

    fn make_importer_config(dep_dir_name: &str) -> Config {
        let mut config = Config::default();

        let mut dependency_service = Service::default();
        dependency_service.dependency = Some("ext".to_string());
        dependency_service.service = Some("target".to_string());
        config
            .services
            .insert("imported".to_string(), dependency_service);

        config.dependencies.insert(
            "ext".to_string(),
            crate::config::Dependency {
                repo: format!("file://{}", dep_dir_name),
                branch: None,
            },
        );

        config
    }

    #[tokio::test]
    async fn test_expand_unexposed_target_fails() {
        let temp_dir = tempfile::tempdir().unwrap();
        write_external_repo(temp_dir.path(), "ext-repo", false);

        let config = make_importer_config("ext-repo");
        let resolver = Resolver::new();
        let expander =
            ExternalServiceExpander::new(&config, &resolver, temp_dir.path().to_path_buf());

        let result = expander.expand().await;

        assert!(
            result.is_err(),
            "importing an unexposed target service should fail"
        );
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("expose: true"),
            "error should mention expose: true, got: {}",
            err
        );
        assert!(
            err.contains("target") && err.contains("ext") && err.contains("imported"),
            "error should name the target service, dependency, and importing service, got: {}",
            err
        );
    }

    #[tokio::test]
    async fn test_expand_exposed_target_succeeds_and_imports_unexposed_transitive_dependency() {
        let temp_dir = tempfile::tempdir().unwrap();
        write_external_repo(temp_dir.path(), "ext-repo", true);

        let config = make_importer_config("ext-repo");
        let resolver = Resolver::new();
        let expander =
            ExternalServiceExpander::new(&config, &resolver, temp_dir.path().to_path_buf());

        let result = expander.expand().await;

        assert!(
            result.is_ok(),
            "importing an exposed target service should succeed: {:?}",
            result.err()
        );
        let expanded = result.unwrap();

        // Target is imported under the parent service's own name.
        assert!(
            expanded.services.contains_key("imported"),
            "expected the exposed target service to be imported as 'imported'"
        );

        // Its transitive dependency ("helper", never exposed) still comes
        // along, namespaced — the exemption applies to everything except the
        // directly-named target.
        assert!(
            expanded.services.contains_key("imported:helper"),
            "expected the unexposed transitive dependency 'helper' to be imported alongside the exposed target"
        );
    }
}
