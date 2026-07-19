use crate::config::{BranchStrategy, Config, DependsOn, ExternalDependency, Metadata};
use crate::dependency::GitOperations;
use crate::error::{Error, Result};
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};

/// External dependency resolver
pub struct ExternalDependencyResolver {
    /// Base working directory
    work_dir: PathBuf,
    /// Cache directory for cloned repos
    cache_dir: PathBuf,
    /// Current branch name (for branch strategy resolution)
    current_branch: Option<String>,
}

impl ExternalDependencyResolver {
    /// Create a new external dependency resolver
    pub fn new<P: AsRef<Path>>(work_dir: P) -> Result<Self> {
        let work_dir = work_dir.as_ref().to_path_buf();
        let cache_dir = work_dir.join(".service-federation/external");

        Ok(Self {
            work_dir,
            cache_dir,
            current_branch: None,
        })
    }

    /// Set the current branch for branch strategy resolution
    pub fn set_current_branch(&mut self, branch: String) {
        self.current_branch = Some(branch);
    }

    /// Get the current branch (auto-detect if not set)
    pub fn get_current_branch(&self) -> Result<String> {
        if let Some(ref branch) = self.current_branch {
            return Ok(branch.clone());
        }

        // Try to detect current branch from git
        if GitOperations::is_git_repo(&self.work_dir) {
            return GitOperations::get_current_branch(&self.work_dir);
        }

        // Default to "main" if not in a git repo
        Ok("main".to_string())
    }

    /// Resolve all external dependencies in the config
    /// Returns a map of external service names to their resolved configs and paths
    pub async fn resolve_external_dependencies(
        &self,
        config: &Config,
    ) -> Result<HashMap<String, (Config, PathBuf)>> {
        let mut resolved = HashMap::new();

        // Get external dependencies from metadata
        let external_deps = if let Some(ref metadata) = config.metadata {
            &metadata.external_dependencies
        } else {
            return Ok(resolved); // No external dependencies
        };

        // Get current branch for strategy resolution
        let current_branch = self.get_current_branch()?;

        // Resolve each external dependency
        for ext_dep in external_deps {
            let target_branch = self.resolve_branch(&current_branch, &ext_dep.branch_strategy);
            let repo_path = self.clone_or_update_repo(&ext_dep.repo, Some(&target_branch))?;

            // Load the external config
            let external_config = self.load_external_config(&repo_path)?;

            // For each service listed in the external dependency, store its config
            for service_info in &ext_dep.services {
                resolved.insert(
                    service_info.name.clone(),
                    (external_config.clone(), repo_path.clone()),
                );
            }
        }

        Ok(resolved)
    }

    /// Resolve the target branch based on current branch and strategy
    fn resolve_branch(&self, current_branch: &str, strategy: &Option<BranchStrategy>) -> String {
        if let Some(strategy) = strategy {
            strategy.resolve_branch(current_branch)
        } else {
            // Default strategy: use same branch name
            current_branch.to_string()
        }
    }

    /// Clone or update a repository
    fn clone_or_update_repo(&self, repo_url: &str, branch: Option<&str>) -> Result<PathBuf> {
        // Handle file:// URLs for local testing
        if repo_url.starts_with("file://") {
            let relative_path = repo_url.trim_start_matches("file://");
            return Ok(self.work_dir.join(relative_path));
        }

        // Create cache directory
        std::fs::create_dir_all(&self.cache_dir)?;

        // Extract repo name from URL
        let repo_name = repo_url
            .split('/')
            .next_back()
            .unwrap_or("external")
            .trim_end_matches(".git");

        let target_path = self.cache_dir.join(repo_name);

        // Clone or update
        GitOperations::clone_or_update(repo_url, branch, &target_path)?;

        Ok(target_path)
    }

    /// Load external config from a repository path
    fn load_external_config(&self, repo_path: &Path) -> Result<Config> {
        let config_path = repo_path.join("service-federation.yaml");
        if !config_path.exists() {
            let alt_path = repo_path.join("service-federation.yml");
            if alt_path.exists() {
                return self.parse_config(&alt_path);
            }
            return Err(Error::Config(format!(
                "No service-federation.yaml found in external repo at {:?}",
                repo_path
            )));
        }

        self.parse_config(&config_path)
    }

    /// Parse a config file
    fn parse_config(&self, path: &Path) -> Result<Config> {
        let content = std::fs::read_to_string(path)?;
        let config: Config = serde_yaml::from_str(&content)?;
        Ok(config)
    }

    /// Collect required external services from a config
    /// Returns set of external service names that are referenced
    pub fn collect_required_external_services(&self, config: &Config) -> HashSet<String> {
        let mut required = HashSet::new();

        for service in config.services.values() {
            for dep in &service.depends_on {
                if dep.is_external() {
                    required.insert(dep.service_name().to_string());
                }
            }
        }

        required
    }

    /// Merge external services into main config
    /// This selectively imports only the required external services and their dependencies
    pub fn merge_external_services(
        &self,
        main_config: &mut Config,
        external_configs: &HashMap<String, (Config, PathBuf)>,
    ) -> Result<()> {
        // Collect all required external services
        let required_services = self.collect_required_external_services(main_config);

        // Map to track external service name -> alias in main config
        let mut service_aliases: HashMap<String, String> = HashMap::new();

        // First pass: identify aliases from depends_on
        for service in main_config.services.values() {
            for dep in &service.depends_on {
                if dep.is_external() {
                    if let Some(alias) = dep.alias() {
                        service_aliases.insert(dep.service_name().to_string(), alias.to_string());
                    } else {
                        // If no alias, use the service name itself
                        service_aliases.insert(
                            dep.service_name().to_string(),
                            dep.service_name().to_string(),
                        );
                    }
                }
            }
        }

        // For each required external service, import it and its dependencies
        for ext_service_name in &required_services {
            if let Some((external_config, repo_path)) = external_configs.get(ext_service_name) {
                // Check if the external service is marked as expose: true
                if let Some(ext_service) = external_config.services.get(ext_service_name) {
                    if !ext_service.expose {
                        return Err(Error::Config(format!(
                            "External service '{}' is not exposed (expose: true required)",
                            ext_service_name
                        )));
                    }

                    // Collect all services needed (target + its dependencies)
                    let services_to_import =
                        Self::collect_service_tree(external_config, ext_service_name);

                    // Get alias for the main service
                    let alias = service_aliases
                        .get(ext_service_name)
                        .cloned()
                        .unwrap_or_else(|| ext_service_name.clone());

                    // Import each service
                    for import_name in services_to_import {
                        if let Some(import_service) = external_config.services.get(&import_name) {
                            let mut adjusted_service = import_service.clone();

                            // Adjust CWD to be relative to external repo
                            if let Some(ref cwd) = adjusted_service.cwd {
                                adjusted_service.cwd =
                                    Some(repo_path.join(cwd).to_string_lossy().to_string());
                            } else {
                                adjusted_service.cwd =
                                    Some(repo_path.to_string_lossy().to_string());
                            }

                            // Adjust dependencies to use prefixed names
                            // Exception: the main external service uses its alias
                            let namespaced_import_name = if import_name == *ext_service_name {
                                alias.clone()
                            } else {
                                format!("{}:{}", alias, import_name)
                            };

                            // Update dependency references
                            adjusted_service.depends_on = adjusted_service
                                .depends_on
                                .iter()
                                .map(|dep| {
                                    let dep_name = dep.service_name();
                                    // Prefix internal dependencies
                                    let new_name = if dep_name == ext_service_name {
                                        alias.clone()
                                    } else {
                                        format!("{}:{}", alias, dep_name)
                                    };
                                    DependsOn::Simple(new_name)
                                })
                                .collect();

                            // Insert into main config
                            main_config
                                .services
                                .insert(namespaced_import_name, adjusted_service);
                        }
                    }
                } else {
                    return Err(Error::Config(format!(
                        "External service '{}' not found in its repository",
                        ext_service_name
                    )));
                }
            }
        }

        // Second pass: update depends_on in main config to use aliases
        let service_names: Vec<String> = main_config.services.keys().cloned().collect();
        for service_name in service_names {
            let mut updated_deps = Vec::new();
            if let Some(service) = main_config.services.get(&service_name) {
                for dep in &service.depends_on {
                    if dep.is_external() {
                        // Replace with alias
                        let alias = service_aliases
                            .get(dep.service_name())
                            .cloned()
                            .unwrap_or_else(|| dep.service_name().to_string());
                        updated_deps.push(DependsOn::Simple(alias));
                    } else {
                        updated_deps.push(dep.clone());
                    }
                }
            }

            // Update the service
            if let Some(service) = main_config.services.get_mut(&service_name) {
                service.depends_on = updated_deps;
            }
        }

        Ok(())
    }

    /// Collect all services needed for a target service (including dependencies)
    fn collect_service_tree(config: &Config, target_service: &str) -> Vec<String> {
        let mut result = Vec::new();
        let mut visited = HashSet::new();
        Self::collect_service_tree_recursive(config, target_service, &mut visited, &mut result);
        result
    }

    /// Recursively collect service dependencies
    fn collect_service_tree_recursive(
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
                let dep_name = dep.service_name();
                Self::collect_service_tree_recursive(config, dep_name, visited, result);
            }

            // Then add this service
            result.push(service_name.to_string());
        }
    }

    /// Find external dependency definition by service name
    pub fn find_external_dependency<'a>(
        metadata: &'a Option<Metadata>,
        service_name: &str,
    ) -> Option<&'a ExternalDependency> {
        if let Some(meta) = metadata {
            for ext_dep in &meta.external_dependencies {
                for service_info in &ext_dep.services {
                    if service_info.name == service_name {
                        return Some(ext_dep);
                    }
                }
            }
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Service;

    #[test]
    fn test_resolve_branch_with_strategy() {
        let mut strategy = BranchStrategy {
            mappings: HashMap::new(),
            default: Some("main".to_string()),
        };
        strategy
            .mappings
            .insert("develop".to_string(), "dev".to_string());
        strategy
            .mappings
            .insert("staging".to_string(), "staging".to_string());

        assert_eq!(strategy.resolve_branch("develop"), "dev");
        assert_eq!(strategy.resolve_branch("staging"), "staging");
        assert_eq!(strategy.resolve_branch("feature/test"), "main"); // Falls back to default
    }

    #[test]
    fn test_resolve_branch_without_strategy() {
        let resolver = ExternalDependencyResolver::new(".").unwrap();
        let branch = resolver.resolve_branch("develop", &None);
        assert_eq!(branch, "develop");
    }

    #[test]
    fn test_collect_required_external_services() {
        let resolver = ExternalDependencyResolver::new(".").unwrap();

        let mut config = Config::default();
        let service = Service {
            depends_on: vec![
                DependsOn::External {
                    external: "profile_service".to_string(),
                    alias: Some("customer_api".to_string()),
                },
                DependsOn::Simple("postgres".to_string()),
            ],
            ..Default::default()
        };
        config.services.insert("my_service".to_string(), service);

        let required = resolver.collect_required_external_services(&config);

        assert_eq!(required.len(), 1);
        assert!(required.contains("profile_service"));
        assert!(!required.contains("postgres"));
    }
}
