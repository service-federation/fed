use crate::config::{Config, Service};
use crate::error::{Error, Result};
use crate::package::types::Package;
use std::collections::{BTreeMap, HashMap, HashSet};

/// Service merger for merging package services into main config
pub struct ServiceMerger;

impl ServiceMerger {
    /// Merge local templates into services
    /// This applies service extension by resolving `extends` references to local templates
    pub fn merge_local_templates(config: &mut Config) -> Result<()> {
        // Collect services that extend local templates (no dot in extends reference)
        let mut local_extends: Vec<(String, String)> = Vec::new();

        for (service_name, service) in &config.services {
            if let Some(ref extend_ref) = service.extends {
                // If no dot, it's a local template reference
                if !extend_ref.contains('.') {
                    local_extends.push((service_name.clone(), extend_ref.clone()));
                }
            }
        }

        // Process each local template extension
        for (service_name, template_name) in local_extends {
            // Get template
            let template = config
                .templates
                .get(&template_name)
                .ok_or_else(|| {
                    Error::Validation(format!(
                        "Template '{}' not found (referenced in service '{}')",
                        template_name, service_name
                    ))
                })?
                .clone();

            // Check for circular extends in the template
            if template.extends.is_some() {
                return Err(Error::Validation(format!(
                    "Template '{}' cannot extend another template (circular dependencies not supported)",
                    template_name
                )));
            }

            // Get local service definition
            let local_service = config
                .services
                .get_mut(&service_name)
                .ok_or_else(|| Error::ServiceNotFound(service_name.clone()))?;

            // Merge template into local
            Self::merge_service(local_service, &template)?;
        }

        Ok(())
    }

    /// Merge package services into main config
    /// This applies service extension by resolving `extends` references
    pub fn merge_packages(
        main_config: &mut Config,
        packages: &HashMap<String, Package>,
    ) -> Result<()> {
        // Collect all services that extend packages
        let extends_map = Self::build_extends_map(main_config)?;

        // Process each service with extends
        for (service_name, extend_ref) in extends_map {
            let (package_alias, package_service_name) = Self::parse_extend_ref(&extend_ref)?;

            // Find the package
            let package = packages.get(&package_alias).ok_or_else(|| {
                Error::Package(format!(
                    "Package alias '{}' not found (referenced in service '{}')",
                    package_alias, service_name
                ))
            })?;

            // Get base service from package
            let base_service = package
                .config
                .services
                .get(&package_service_name)
                .ok_or_else(|| {
                    Error::Package(format!(
                        "Service '{}' not found in package '{}' (referenced in service '{}')",
                        package_service_name, package_alias, service_name
                    ))
                })?;

            // Only services the package author marked `expose: true` may be
            // extended from outside the package. This mirrors the enforcement
            // on the live `dependency:`/`service:` import path in
            // `dependency/expander.rs`.
            if !base_service.expose {
                return Err(Error::Package(format!(
                    "Service '{}' in package '{}' is not marked expose: true. Mark it exposed in the package's fed.yaml before extending it from '{}'",
                    package_service_name, package_alias, service_name
                )));
            }

            // Check for circular extends in the base service
            if base_service.extends.is_some() {
                return Err(Error::CircularPackageDependency);
            }

            // Get local service definition
            let local_service = main_config
                .services
                .get_mut(&service_name)
                .ok_or_else(|| Error::ServiceNotFound(service_name.clone()))?;

            // Merge base into local
            Self::merge_service(local_service, base_service)?;
        }

        Ok(())
    }

    /// Build map of service names to their extends references.
    ///
    /// Uses a `BTreeMap` (rather than `HashMap`) so `merge_packages` iterates
    /// services in deterministic (sorted-by-name) order. This matters because
    /// `merge_packages`'s loop body uses `?` to bail out on the first error —
    /// with a `HashMap`'s randomized iteration order, *which* service's error
    /// surfaces first (and which not-yet-processed entries get abandoned)
    /// would vary from run to run given the same input.
    fn build_extends_map(config: &Config) -> Result<BTreeMap<String, String>> {
        let mut map = BTreeMap::new();

        for (name, service) in &config.services {
            if let Some(ref extend_ref) = service.extends {
                map.insert(name.clone(), extend_ref.clone());
            }
        }

        Ok(map)
    }

    /// Parse extends reference (format: "package-alias.service-name")
    fn parse_extend_ref(extend_ref: &str) -> Result<(String, String)> {
        let parts: Vec<&str> = extend_ref.split('.').collect();
        if parts.len() != 2 {
            return Err(Error::Validation(format!(
                "Invalid extends reference '{}'. Expected format: 'package-alias.service-name'",
                extend_ref
            )));
        }

        if parts[0].is_empty() || parts[1].is_empty() {
            return Err(Error::Validation(format!(
                "Invalid extends reference '{}'. Package alias and service name cannot be empty",
                extend_ref
            )));
        }

        Ok((parts[0].to_string(), parts[1].to_string()))
    }

    /// Merge base service into local service
    /// Strategy: local overrides base, but preserve base defaults
    fn merge_service(local: &mut Service, base: &Service) -> Result<()> {
        // 1. Copy base scalar fields if local doesn't have them
        if local.cwd.is_none() {
            local.cwd = base.cwd.clone();
        }
        if local.install.is_none() {
            local.install = base.install.clone();
        }
        if local.process.is_none() {
            local.process = base.process.clone();
        }
        if local.image.is_none() {
            local.image = base.image.clone();
        }
        if local.dependency.is_none() {
            local.dependency = base.dependency.clone();
        }
        if local.service.is_none() {
            local.service = base.service.clone();
        }
        if local.gradle_task.is_none() {
            local.gradle_task = base.gradle_task.clone();
        }
        if local.compose_file.is_none() {
            local.compose_file = base.compose_file.clone();
        }
        if local.compose_service.is_none() {
            local.compose_service = base.compose_service.clone();
        }
        if local.healthcheck.is_none() {
            local.healthcheck = base.healthcheck.clone();
        }
        if local.restart.is_none() {
            local.restart = base.restart.clone();
        }

        // 2. Merge collections (environment, volumes, ports, etc.)
        Self::merge_environment(&mut local.environment, &base.environment);
        Self::merge_volumes(&mut local.volumes, &base.volumes);
        Self::merge_ports(&mut local.ports, &base.ports);
        Self::merge_parameters(&mut local.parameters, &base.parameters);
        Self::merge_depends_on(&mut local.depends_on, &base.depends_on);

        // Clear the extends field after merging to avoid confusion
        local.extends = None;

        Ok(())
    }

    /// Merge environment variables (local overrides base for same keys)
    fn merge_environment(local: &mut HashMap<String, String>, base: &HashMap<String, String>) {
        // Add base environment variables that don't exist in local
        for (key, value) in base {
            local.entry(key.clone()).or_insert_with(|| value.clone());
        }
    }

    /// Merge volumes (combine both, local takes precedence on conflicts)
    /// If two volumes have the same target (mount point), local wins
    fn merge_volumes(local: &mut Vec<String>, base: &Vec<String>) {
        // Extract target paths from local volumes
        let local_targets: HashSet<String> = local
            .iter()
            .filter_map(|v| Self::extract_volume_target(v))
            .collect();

        // Add base volumes that don't conflict with local targets
        for base_vol in base {
            if let Some(target) = Self::extract_volume_target(base_vol) {
                if !local_targets.contains(&target) {
                    local.push(base_vol.clone());
                }
            } else {
                // Named volume or bind mount without explicit target - add it
                local.push(base_vol.clone());
            }
        }
    }

    /// Extract the target (container path) from a volume specification
    /// Handles formats: "host:container", "host:container:ro", "named-volume:/path", etc.
    fn extract_volume_target(volume: &str) -> Option<String> {
        let parts: Vec<&str> = volume.split(':').collect();
        if parts.len() >= 2 {
            Some(parts[1].to_string())
        } else {
            // Single part could be named volume without target
            None
        }
    }

    /// Merge ports (combine both, avoid duplicates)
    fn merge_ports(local: &mut Vec<String>, base: &Vec<String>) {
        for port in base {
            if !local.contains(port) {
                local.push(port.clone());
            }
        }
    }

    /// Merge parameters (local overrides base for same keys)
    fn merge_parameters(local: &mut HashMap<String, String>, base: &HashMap<String, String>) {
        // Add base parameters that don't exist in local
        for (key, value) in base {
            local.entry(key.clone()).or_insert_with(|| value.clone());
        }
    }

    /// Merge dependencies (combine both, avoid duplicates)
    fn merge_depends_on(
        local: &mut Vec<crate::config::DependsOn>,
        base: &Vec<crate::config::DependsOn>,
    ) {
        for dep in base {
            // Check if this dependency already exists (by service name)
            let dep_name = dep.service_name();
            let exists = local.iter().any(|d| d.service_name() == dep_name);
            if !exists {
                local.push(dep.clone());
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{HealthCheck, RestartPolicy};

    #[test]
    fn test_parse_extend_ref_valid() {
        let result = ServiceMerger::parse_extend_ref("db-pkg.postgres");
        assert!(result.is_ok());
        let (alias, service) = result.unwrap();
        assert_eq!(alias, "db-pkg");
        assert_eq!(service, "postgres");
    }

    #[test]
    fn test_parse_extend_ref_invalid_format() {
        let result = ServiceMerger::parse_extend_ref("invalid");
        assert!(result.is_err());

        let result = ServiceMerger::parse_extend_ref("too.many.parts");
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_extend_ref_empty_parts() {
        let result = ServiceMerger::parse_extend_ref(".postgres");
        assert!(result.is_err());

        let result = ServiceMerger::parse_extend_ref("db-pkg.");
        assert!(result.is_err());
    }

    #[test]
    fn test_merge_environment() {
        let mut local = HashMap::new();
        local.insert("LOCAL_VAR".to_string(), "local_value".to_string());
        local.insert("OVERRIDE".to_string(), "local_override".to_string());

        let mut base = HashMap::new();
        base.insert("BASE_VAR".to_string(), "base_value".to_string());
        base.insert("OVERRIDE".to_string(), "base_value".to_string());

        ServiceMerger::merge_environment(&mut local, &base);

        assert_eq!(local.len(), 3);
        assert_eq!(local.get("LOCAL_VAR").unwrap(), "local_value");
        assert_eq!(local.get("BASE_VAR").unwrap(), "base_value");
        assert_eq!(local.get("OVERRIDE").unwrap(), "local_override"); // Local wins
    }

    #[test]
    fn test_merge_volumes() {
        let mut local = vec![
            "./local-data:/var/lib/data".to_string(),
            "./logs:/var/log".to_string(),
        ];

        let base = vec![
            "./base-data:/var/lib/data".to_string(), // Conflicts with local
            "./config:/etc/config".to_string(),      // New volume
            "named-volume:/mnt/volume".to_string(),  // New named volume
        ];

        ServiceMerger::merge_volumes(&mut local, &base);

        assert_eq!(local.len(), 4);
        assert!(local.contains(&"./local-data:/var/lib/data".to_string())); // Local kept
        assert!(local.contains(&"./logs:/var/log".to_string()));
        assert!(!local.contains(&"./base-data:/var/lib/data".to_string())); // Base conflict removed
        assert!(local.contains(&"./config:/etc/config".to_string())); // Base added
        assert!(local.contains(&"named-volume:/mnt/volume".to_string())); // Named volume added
    }

    #[test]
    fn test_extract_volume_target() {
        assert_eq!(
            ServiceMerger::extract_volume_target("./data:/var/lib/data"),
            Some("/var/lib/data".to_string())
        );
        assert_eq!(
            ServiceMerger::extract_volume_target("./data:/var/lib/data:ro"),
            Some("/var/lib/data".to_string())
        );
        assert_eq!(
            ServiceMerger::extract_volume_target("named-volume:/mnt"),
            Some("/mnt".to_string())
        );
        assert_eq!(ServiceMerger::extract_volume_target("single-part"), None);
    }

    #[test]
    fn test_merge_ports() {
        let mut local = vec!["8080:8080".to_string(), "9000:9000".to_string()];
        let base = vec![
            "5432:5432".to_string(),
            "8080:8080".to_string(), // Duplicate
        ];

        ServiceMerger::merge_ports(&mut local, &base);

        assert_eq!(local.len(), 3);
        assert!(local.contains(&"8080:8080".to_string()));
        assert!(local.contains(&"9000:9000".to_string()));
        assert!(local.contains(&"5432:5432".to_string()));
    }

    #[test]
    fn test_merge_depends_on() {
        use crate::config::DependsOn;
        let mut local = vec![DependsOn::Simple("redis".to_string())];
        let base = vec![
            DependsOn::Simple("postgres".to_string()),
            DependsOn::Simple("redis".to_string()),
        ];

        ServiceMerger::merge_depends_on(&mut local, &base);

        assert_eq!(local.len(), 2);
        assert!(local.iter().any(|d| d.service_name() == "redis"));
        assert!(local.iter().any(|d| d.service_name() == "postgres"));
    }

    #[test]
    fn test_merge_service_scalar_fields() {
        let mut local = Service {
            extends: Some("pkg.base".to_string()),
            image: Some("custom-image".to_string()), // Override
            ..Default::default()
        };

        let base = Service {
            image: Some("base-image".to_string()),
            process: Some("npm start".to_string()),
            cwd: Some("/app".to_string()),
            install: Some("npm install".to_string()),
            healthcheck: Some(HealthCheck::Command("curl localhost".to_string())),
            restart: Some(RestartPolicy::Always),
            ..Default::default()
        };

        ServiceMerger::merge_service(&mut local, &base).unwrap();

        // Local override preserved
        assert_eq!(local.image.as_deref(), Some("custom-image"));
        // Base values copied
        assert_eq!(local.process.as_deref(), Some("npm start"));
        assert_eq!(local.cwd.as_deref(), Some("/app"));
        assert_eq!(local.install.as_deref(), Some("npm install"));
        assert!(local.healthcheck.is_some());
        assert!(matches!(local.restart, Some(RestartPolicy::Always)));
        // Extends cleared
        assert!(local.extends.is_none());
    }

    #[test]
    fn test_merge_service_complete() {
        let mut local_env = HashMap::new();
        local_env.insert("DB_NAME".to_string(), "myapp".to_string());

        let mut local = Service {
            extends: Some("db.postgres".to_string()),
            environment: local_env,
            volumes: vec!["./data:/var/lib/postgresql/data".to_string()],
            ports: vec!["5433:5432".to_string()],
            ..Default::default()
        };

        let mut base_env = HashMap::new();
        base_env.insert("POSTGRES_USER".to_string(), "admin".to_string());
        base_env.insert("POSTGRES_PASSWORD".to_string(), "secret".to_string());

        let base = Service {
            image: Some("postgres:15".to_string()),
            environment: base_env,
            healthcheck: Some(HealthCheck::Command("pg_isready".to_string())),
            volumes: vec!["postgres-data:/var/lib/postgresql/data".to_string()],
            ports: vec!["5432:5432".to_string()],
            restart: Some(RestartPolicy::Always),
            ..Default::default()
        };

        ServiceMerger::merge_service(&mut local, &base).unwrap();

        // Image from base
        assert_eq!(local.image.as_deref(), Some("postgres:15"));

        // Environment merged (3 keys: 1 local + 2 base)
        assert_eq!(local.environment.len(), 3);
        assert_eq!(local.environment.get("DB_NAME").unwrap(), "myapp");
        assert_eq!(local.environment.get("POSTGRES_USER").unwrap(), "admin");

        // Volumes merged (local target conflicts with base named volume, so base is added)
        assert_eq!(local.volumes.len(), 1); // Local volume kept, base volume conflicts with same target

        // Ports merged
        assert_eq!(local.ports.len(), 2);
        assert!(local.ports.contains(&"5433:5432".to_string()));
        assert!(local.ports.contains(&"5432:5432".to_string()));

        // Healthcheck from base
        assert!(local.healthcheck.is_some());

        // Restart from base
        assert!(matches!(local.restart, Some(RestartPolicy::Always)));
    }

    #[test]
    fn test_build_extends_map() {
        let mut config = Config::default();

        let service1 = Service {
            extends: Some("pkg1.base".to_string()),
            ..Default::default()
        };

        let service2 = Service {
            image: Some("test".to_string()),
            ..Default::default()
        };

        config.services.insert("svc1".to_string(), service1);
        config.services.insert("svc2".to_string(), service2);

        let map = ServiceMerger::build_extends_map(&config).unwrap();

        assert_eq!(map.len(), 1);
        assert_eq!(map.get("svc1").unwrap(), "pkg1.base");
        assert!(!map.contains_key("svc2"));
    }

    #[test]
    fn test_merge_local_templates_basic() {
        let mut config = Config::default();

        // Define a template
        let mut template_env = HashMap::new();
        template_env.insert("JAVA_OPTS".to_string(), "-Xmx512m".to_string());

        let template = Service {
            image: Some("openjdk:17".to_string()),
            environment: template_env,
            healthcheck: Some(HealthCheck::HttpGet {
                http_get: "http://localhost:8080/health".to_string(),
                timeout: None,
            }),
            restart: Some(RestartPolicy::Always),
            ..Default::default()
        };

        config
            .templates
            .insert("java-service".to_string(), template);

        // Define a service that extends the template
        let mut service_env = HashMap::new();
        service_env.insert("PORT".to_string(), "8080".to_string());

        let service = Service {
            extends: Some("java-service".to_string()),
            environment: service_env,
            ports: vec!["8080:8080".to_string()],
            ..Default::default()
        };

        config.services.insert("auth-service".to_string(), service);

        // Merge templates
        ServiceMerger::merge_local_templates(&mut config).unwrap();

        // Check that template was merged
        let merged_service = config.services.get("auth-service").unwrap();

        // Image from template
        assert_eq!(merged_service.image.as_deref(), Some("openjdk:17"));

        // Environment merged (2 keys: 1 from service + 1 from template)
        assert_eq!(merged_service.environment.len(), 2);
        assert_eq!(merged_service.environment.get("PORT").unwrap(), "8080");
        assert_eq!(
            merged_service.environment.get("JAVA_OPTS").unwrap(),
            "-Xmx512m"
        );

        // Healthcheck from template
        assert!(merged_service.healthcheck.is_some());

        // Restart from template
        assert!(matches!(
            merged_service.restart,
            Some(RestartPolicy::Always)
        ));

        // Ports from service
        assert_eq!(merged_service.ports.len(), 1);

        // Extends cleared
        assert!(merged_service.extends.is_none());
    }

    #[test]
    fn test_merge_local_templates_not_found() {
        let mut config = Config::default();

        // Service extends non-existent template
        let service = Service {
            extends: Some("missing-template".to_string()),
            ..Default::default()
        };

        config.services.insert("test-service".to_string(), service);

        // Should fail with validation error
        let result = ServiceMerger::merge_local_templates(&mut config);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), Error::Validation(_)));
    }

    #[test]
    fn test_merge_local_templates_circular_extends() {
        let mut config = Config::default();

        // Template that tries to extend something (not allowed)
        let template = Service {
            extends: Some("another-template".to_string()),
            image: Some("test".to_string()),
            ..Default::default()
        };

        config
            .templates
            .insert("bad-template".to_string(), template);

        // Service extends the bad template
        let service = Service {
            extends: Some("bad-template".to_string()),
            ..Default::default()
        };

        config.services.insert("test-service".to_string(), service);

        // Should fail with circular dependency error
        let result = ServiceMerger::merge_local_templates(&mut config);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), Error::Validation(_)));
    }

    #[test]
    fn test_merge_local_templates_vs_package_extends() {
        let mut config = Config::default();

        // Template
        let template = Service {
            image: Some("base-image".to_string()),
            ..Default::default()
        };

        config
            .templates
            .insert("local-template".to_string(), template);

        // Service extending local template (no dot)
        let local_service = Service {
            extends: Some("local-template".to_string()),
            ..Default::default()
        };

        // Service extending package (has dot)
        let package_service = Service {
            extends: Some("pkg.service".to_string()),
            ..Default::default()
        };

        config
            .services
            .insert("local-svc".to_string(), local_service);
        config
            .services
            .insert("pkg-svc".to_string(), package_service);

        // Merge local templates - should only affect local-svc
        ServiceMerger::merge_local_templates(&mut config).unwrap();

        // Local service should be merged
        let merged_local = config.services.get("local-svc").unwrap();
        assert_eq!(merged_local.image.as_deref(), Some("base-image"));
        assert!(merged_local.extends.is_none());

        // Package service should still have extends
        let pkg_svc = config.services.get("pkg-svc").unwrap();
        assert_eq!(pkg_svc.extends.as_deref(), Some("pkg.service"));
    }

    #[test]
    fn test_circular_package_dependency_detected() {
        // When a service extends a package service that itself has extends,
        // we should get CircularPackageDependency error.
        //
        // This prevents: my-service extends pkg.base, pkg.base extends pkg.deeper
        // Which would require recursive merging and could lead to cycles.

        let mut main_config = Config::default();

        // Service in main config that extends a package service
        let local_service = Service {
            extends: Some("db-pkg.postgres".to_string()),
            ..Default::default()
        };
        main_config
            .services
            .insert("my-db".to_string(), local_service);

        // Package with a service that ALSO has extends (the problem case)
        let mut pkg_config = Config::default();
        let pkg_service = Service {
            image: Some("postgres:15".to_string()),
            expose: true,
            extends: Some("another-pkg.base".to_string()), // This triggers the error
            ..Default::default()
        };
        pkg_config
            .services
            .insert("postgres".to_string(), pkg_service);

        let package = Package {
            alias: "db-pkg".to_string(),
            source: crate::package::PackageSource::Local {
                path: std::path::PathBuf::from("/fake/path"),
            },
            config: pkg_config,
            path: std::path::PathBuf::from("/fake/path"),
            metadata: crate::package::PackageMetadata {
                name: None,
                description: None,
                version: None,
                updated_at: chrono::Utc::now(),
                checksum: None,
            },
        };

        let mut packages = HashMap::new();
        packages.insert("db-pkg".to_string(), package);

        let result = ServiceMerger::merge_packages(&mut main_config, &packages);

        assert!(result.is_err(), "Should detect circular package dependency");
        assert!(
            matches!(result.unwrap_err(), Error::CircularPackageDependency),
            "Should return CircularPackageDependency error"
        );
    }

    #[test]
    fn test_package_service_without_extends_succeeds() {
        // Normal case: service extends package service that has no further extends

        let mut main_config = Config::default();

        let local_service = Service {
            extends: Some("db-pkg.postgres".to_string()),
            environment: std::collections::HashMap::from([(
                "EXTRA".to_string(),
                "value".to_string(),
            )]),
            ..Default::default()
        };
        main_config
            .services
            .insert("my-db".to_string(), local_service);

        let mut pkg_config = Config::default();
        let pkg_service = Service {
            image: Some("postgres:15".to_string()),
            environment: std::collections::HashMap::from([(
                "POSTGRES_DB".to_string(),
                "app".to_string(),
            )]),
            expose: true,
            extends: None, // No further extends - this is fine
            ..Default::default()
        };
        pkg_config
            .services
            .insert("postgres".to_string(), pkg_service);

        let package = Package {
            alias: "db-pkg".to_string(),
            source: crate::package::PackageSource::Local {
                path: std::path::PathBuf::from("/fake/path"),
            },
            config: pkg_config,
            path: std::path::PathBuf::from("/fake/path"),
            metadata: crate::package::PackageMetadata {
                name: None,
                description: None,
                version: None,
                updated_at: chrono::Utc::now(),
                checksum: None,
            },
        };

        let mut packages = HashMap::new();
        packages.insert("db-pkg".to_string(), package);

        let result = ServiceMerger::merge_packages(&mut main_config, &packages);

        assert!(
            result.is_ok(),
            "Should succeed when package service has no extends"
        );

        let merged = main_config.services.get("my-db").unwrap();
        assert_eq!(merged.image.as_deref(), Some("postgres:15"));
        assert_eq!(merged.environment.get("POSTGRES_DB").unwrap(), "app");
        assert_eq!(merged.environment.get("EXTRA").unwrap(), "value");
        assert!(
            merged.extends.is_none(),
            "extends should be cleared after merge"
        );
    }

    fn make_extends_package(pkg_service: Service) -> Package {
        let mut pkg_config = Config::default();
        pkg_config
            .services
            .insert("postgres".to_string(), pkg_service);

        Package {
            alias: "db-pkg".to_string(),
            source: crate::package::PackageSource::Local {
                path: std::path::PathBuf::from("/fake/path"),
            },
            config: pkg_config,
            path: std::path::PathBuf::from("/fake/path"),
            metadata: crate::package::PackageMetadata {
                name: None,
                description: None,
                version: None,
                updated_at: chrono::Utc::now(),
                checksum: None,
            },
        }
    }

    #[test]
    fn test_package_service_not_exposed_fails() {
        // A package service that doesn't declare `expose: true` must not be
        // extendable from outside the package.
        let mut main_config = Config::default();
        let local_service = Service {
            extends: Some("db-pkg.postgres".to_string()),
            ..Default::default()
        };
        main_config
            .services
            .insert("my-db".to_string(), local_service);

        let pkg_service = Service {
            image: Some("postgres:15".to_string()),
            expose: false,
            ..Default::default()
        };
        let package = make_extends_package(pkg_service);

        let mut packages = HashMap::new();
        packages.insert("db-pkg".to_string(), package);

        let result = ServiceMerger::merge_packages(&mut main_config, &packages);

        assert!(
            result.is_err(),
            "extending an unexposed package service should fail"
        );
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("expose: true"),
            "error should mention expose: true, got: {}",
            err
        );
        assert!(
            err.contains("my-db") && err.contains("postgres") && err.contains("db-pkg"),
            "error should name the service, package service, and package alias, got: {}",
            err
        );
    }

    #[test]
    fn test_package_service_exposed_succeeds() {
        // The positive counterpart to test_package_service_not_exposed_fails:
        // an exposed package service extends without error.
        let mut main_config = Config::default();
        let local_service = Service {
            extends: Some("db-pkg.postgres".to_string()),
            ..Default::default()
        };
        main_config
            .services
            .insert("my-db".to_string(), local_service);

        let pkg_service = Service {
            image: Some("postgres:15".to_string()),
            expose: true,
            ..Default::default()
        };
        let package = make_extends_package(pkg_service);

        let mut packages = HashMap::new();
        packages.insert("db-pkg".to_string(), package);

        let result = ServiceMerger::merge_packages(&mut main_config, &packages);

        assert!(
            result.is_ok(),
            "extending an exposed package service should succeed: {:?}",
            result.err()
        );
        let merged = main_config.services.get("my-db").unwrap();
        assert_eq!(merged.image.as_deref(), Some("postgres:15"));
    }

    #[test]
    fn test_merge_packages_deterministic_order_same_input_same_output() {
        // Regression test for D4: build_extends_map/merge_packages used to
        // traverse two unordered HashMaps, so which service's error surfaced
        // first (and which entries got merged before an early `?` abort)
        // depended on random iteration order. With a BTreeMap, the same
        // input must always produce the same result, regardless of how many
        // times we run it.
        //
        // Fixture: two services extend two different packages; one package
        // alias doesn't exist, so `merge_packages` always errors — but which
        // error (naming which service) surfaces must be stable across runs
        // since services are now processed in sorted-name order ("svc-a"
        // before "svc-b").
        fn build_config_and_packages() -> (Config, HashMap<String, Package>) {
            let mut main_config = Config::default();
            main_config.services.insert(
                "svc-a".to_string(),
                Service {
                    extends: Some("missing-pkg.thing".to_string()),
                    ..Default::default()
                },
            );
            main_config.services.insert(
                "svc-b".to_string(),
                Service {
                    extends: Some("db-pkg.postgres".to_string()),
                    ..Default::default()
                },
            );

            let pkg_service = Service {
                image: Some("postgres:15".to_string()),
                expose: true,
                ..Default::default()
            };
            let package = make_extends_package(pkg_service);
            let mut packages = HashMap::new();
            packages.insert("db-pkg".to_string(), package);

            (main_config, packages)
        }

        let mut first_error: Option<String> = None;
        for _ in 0..50 {
            let (mut main_config, packages) = build_config_and_packages();
            let result = ServiceMerger::merge_packages(&mut main_config, &packages);
            assert!(result.is_err(), "missing package alias should error");
            let err = result.unwrap_err().to_string();
            assert!(
                err.contains("svc-a"),
                "error should always name svc-a (sorted first), got: {}",
                err
            );
            match &first_error {
                None => first_error = Some(err),
                Some(expected) => assert_eq!(
                    &err, expected,
                    "merge_packages error must be identical across runs given identical input"
                ),
            }
        }
    }
}
