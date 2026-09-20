use crate::config::{Config, Service};
use crate::error::{Error, Result};
use crate::package::types::Package;
use std::collections::{HashMap, HashSet};

/// Where an `extends:` reference was written.
///
/// A variant is a partial [`Service`], so `extends:` is valid in two places:
/// on a service, and on one of its variants. Both resolve through the same
/// code; this carries which one so the target can be reached for mutation and
/// named in an error message ("referenced in variant 'go' of service
/// 'catalog'").
///
/// Ordered by `(service, variant)` with the service itself before its
/// variants, so resolution order is deterministic — see
/// [`ServiceMerger::extends_sites`].
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct ExtendsSite {
    service: String,
    /// `None` for the service itself.
    variant: Option<String>,
}

impl ExtendsSite {
    fn service(name: &str) -> Self {
        Self {
            service: name.to_string(),
            variant: None,
        }
    }

    fn variant(service: &str, variant: &str) -> Self {
        Self {
            service: service.to_string(),
            variant: Some(variant.to_string()),
        }
    }

    /// How this site is named in an error message.
    fn describe(&self) -> String {
        match &self.variant {
            Some(variant) => format!("variant '{}' of service '{}'", variant, self.service),
            None => format!("service '{}'", self.service),
        }
    }

    /// The `Service` this site refers to, for merging into.
    ///
    /// `None` means the service (or variant) named here is gone — which can
    /// only happen if the config were mutated between collecting the sites
    /// and using them. Callers turn it into a `ServiceNotFound`.
    fn get_mut<'a>(&self, config: &'a mut Config) -> Option<&'a mut Service> {
        let service = config.services.get_mut(&self.service)?;
        match &self.variant {
            None => Some(service),
            Some(variant) => service.variants.get_mut(variant),
        }
    }
}

/// Service merger for merging package services into main config
pub struct ServiceMerger;

impl ServiceMerger {
    /// Merge local templates into services and into service variants.
    ///
    /// Resolves every `extends:` reference that names a local template (no dot
    /// — a dotted reference is a package reference, handled by
    /// [`Self::merge_packages`]). A variant is a partial `Service`, so it may
    /// carry its own `extends:` and is resolved here alongside the services:
    /// that is what lets a Go rewrite pull a shared `go-worker` template
    /// while the service above it keeps holding the contract.
    pub fn merge_local_templates(config: &mut Config) -> Result<()> {
        // Resolve outer services first, then discover any variant references
        // inherited from those templates as well as explicitly declared ones.
        while let Some((site, template_name)) = Self::extends_sites(config)
            .into_iter()
            .find(|(_, extend_ref)| !extend_ref.contains('.'))
        {
            let template = config
                .templates
                .get(&template_name)
                .ok_or_else(|| {
                    Error::Validation(format!(
                        "Template '{}' not found (referenced in {})",
                        template_name,
                        site.describe()
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

            let target = site
                .get_mut(config)
                .ok_or_else(|| Error::ServiceNotFound(site.service.clone()))?;

            Self::merge_service(target, &template)?;
        }

        Ok(())
    }

    /// Merge package services into main config
    /// This applies service extension by resolving `extends` references
    pub fn merge_packages(
        main_config: &mut Config,
        packages: &HashMap<String, Package>,
    ) -> Result<()> {
        // Collect every `extends:` that names a package (dotted reference);
        // local-template references were already resolved by
        // `merge_local_templates` and are skipped here.
        let package_extends: Vec<(ExtendsSite, String)> = Self::extends_sites(main_config)
            .into_iter()
            .filter(|(_, extend_ref)| extend_ref.contains('.'))
            .collect();

        for (site, extend_ref) in package_extends {
            let (package_alias, package_service_name) = Self::parse_extend_ref(&extend_ref)?;

            // Find the package
            let package = packages.get(&package_alias).ok_or_else(|| {
                Error::Package(format!(
                    "Package alias '{}' not found (referenced in {})",
                    package_alias,
                    site.describe()
                ))
            })?;

            // Get base service from package
            let base_service = package
                .config
                .services
                .get(&package_service_name)
                .ok_or_else(|| {
                    Error::Package(format!(
                        "Service '{}' not found in package '{}' (referenced in {})",
                        package_service_name,
                        package_alias,
                        site.describe()
                    ))
                })?;

            // Only services the package author marked `expose: true` may be
            // extended from outside the package. This mirrors the enforcement
            // on the live `dependency:`/`service:` import path in
            // `dependency/expander.rs`.
            if !base_service.expose {
                return Err(Error::Package(format!(
                    "Service '{}' in package '{}' is not marked expose: true. Mark it exposed in the package's fed.yaml before extending it from {}",
                    package_service_name,
                    package_alias,
                    site.describe()
                )));
            }

            // Check for circular extends in the base service
            if base_service.extends.is_some() {
                return Err(Error::CircularPackageDependency);
            }

            // `base_service` borrows `packages`, not `main_config`, so the
            // mutable borrow below is free to descend into the site.
            let base_service = base_service.clone();
            let target = site
                .get_mut(main_config)
                .ok_or_else(|| Error::ServiceNotFound(site.service.clone()))?;

            Self::merge_service(target, &base_service)?;
        }

        // Package configs are parsed with the same legacy-spelling scan as the
        // main config, but only the main config's records reach the soft
        // "prefer snake_case" notice — carry the packages' records over, with
        // the package identity prefixed. Sorted so warning order is stable.
        let mut aliases: Vec<&String> = packages.keys().collect();
        aliases.sort();
        for alias in aliases {
            for u in &packages[alias].config.legacy_key_usages {
                main_config
                    .legacy_key_usages
                    .push(crate::config::LegacyKeyUsage {
                        location: format!("package '{}': {}", alias, u.location),
                        legacy: u.legacy,
                        canonical: u.canonical,
                    });
            }
        }

        Ok(())
    }

    /// Every `extends:` reference in the config, with the site that wrote it.
    ///
    /// Returned in deterministic (sorted) order. This matters because
    /// [`Self::merge_packages`]'s loop uses `?` to bail out on the first
    /// error — with a `HashMap`'s randomized iteration order, *which*
    /// service's error surfaces first (and which not-yet-processed entries
    /// get abandoned) would vary from run to run given the same input.
    fn extends_sites(config: &Config) -> Vec<(ExtendsSite, String)> {
        let mut sites: Vec<(ExtendsSite, String)> = Vec::new();

        for (name, service) in &config.services {
            if let Some(ref extend_ref) = service.extends {
                sites.push((ExtendsSite::service(name), extend_ref.clone()));
            }
            for (variant_name, variant) in &service.variants {
                if let Some(ref extend_ref) = variant.extends {
                    sites.push((ExtendsSite::variant(name, variant_name), extend_ref.clone()));
                }
            }
        }

        sites.sort_by(|a, b| a.0.cmp(&b.0));
        sites
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

    /// Merge `base` into `local`: `local` wins on every scalar it defines,
    /// collections are unioned with `local`'s entries taking precedence.
    ///
    /// Two callers depend on this being *complete* — every field of
    /// [`Service`] must be carried over:
    ///
    /// - `extends:` (package and local templates), where a missing field
    ///   silently drops the template's value (this is the historical
    ///   "`tags:` from a template never arrived" bug).
    /// - variant resolution ([`crate::config::variants`]), which merges the
    ///   chosen variant over the outer service to produce the service that
    ///   every later stage sees.
    ///
    /// `merge_service_all_fields_are_copied` in `config::service` tests
    /// destructures the merged `Service`, so adding a field to the struct
    /// without adding it here fails to compile.
    pub fn merge_service(local: &mut Service, base: &Service) -> Result<()> {
        // 1. Scalars: keep local's value, fall back to base's.
        if local.cwd.is_none() {
            local.cwd = base.cwd.clone();
        }
        if local.install.is_none() {
            local.install = base.install.clone();
        }
        if local.migrate.is_none() {
            local.migrate = base.migrate.clone();
        }
        if local.clean.is_none() {
            local.clean = base.clean.clone();
        }
        if local.build.is_none() {
            local.build = base.build.clone();
        }
        if local.process.is_none() {
            local.process = base.process.clone();
        }
        if local.image.is_none() {
            local.image = base.image.clone();
        }
        if local.command.is_none() {
            local.command = base.command.clone();
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
        if local.resources.is_none() {
            local.resources = base.resources.clone();
        }
        if local.grace_period.is_none() {
            local.grace_period = base.grace_period.clone();
        }
        if local.startup_timeout.is_none() {
            local.startup_timeout = base.startup_timeout.clone();
        }
        if local.circuit_breaker.is_none() {
            local.circuit_breaker = base.circuit_breaker.clone();
        }
        if local.startup_message.is_none() {
            local.startup_message = base.startup_message.clone();
        }
        if local.healthcheck_start_period.is_none() {
            local.healthcheck_start_period = base.healthcheck_start_period.clone();
        }
        if local.default_variant.is_none() {
            local.default_variant = base.default_variant.clone();
        }
        if local.variant.is_none() {
            local.variant = base.variant.clone();
        }

        // `tty`, `expose`, and `compose_imported` are booleans with no "unset"
        // state, so "local wins" can only mean "local's `true` wins" —
        // inheriting `expose: true` from a template is the whole point of
        // marking a package service exposed, and `compose_imported` is an
        // internal provenance flag that must survive any merge.
        local.tty |= base.tty;
        local.expose |= base.expose;
        local.compose_imported |= base.compose_imported;

        // 2. Collections: union, local's entries win on conflicts — unless
        // `local` wrote the key as explicitly empty, which means "none" and
        // is how a service opts out of an inherited list.
        if local.depends_on.is_empty() && base.explicit_empty.contains("depends_on") {
            local.explicit_empty.insert("depends_on".into());
        }
        let kept_empty = |key: &str| local.explicit_empty.contains(key);
        if !kept_empty("environment") {
            Self::merge_environment(&mut local.environment, &base.environment);
        }
        if !kept_empty("volumes") {
            Self::merge_volumes(&mut local.volumes, &base.volumes);
        }
        if !kept_empty("ports") {
            Self::merge_ports(&mut local.ports, &base.ports);
        }
        if !kept_empty("parameters") {
            Self::merge_parameters(&mut local.parameters, &base.parameters);
        }
        if !kept_empty("depends_on") {
            Self::merge_depends_on(&mut local.depends_on, &base.depends_on);
        }
        if !kept_empty("compose_profiles") {
            Self::merge_string_list(&mut local.compose_profiles, &base.compose_profiles);
        }
        if !kept_empty("profiles") {
            Self::merge_string_list(&mut local.profiles, &base.profiles);
        }
        if !kept_empty("tags") {
            Self::merge_string_list(&mut local.tags, &base.tags);
        }
        if !kept_empty("watch") {
            Self::merge_string_list(&mut local.watch, &base.watch);
        }

        // Variant definitions merge by name — a local variant of the same
        // name shadows the base's outright rather than merging field-wise,
        // since the two are alternative implementations, not layers.
        for (name, variant) in &base.variants {
            local
                .variants
                .entry(name.clone())
                .or_insert_with(|| variant.clone());
        }

        // Empty depends_on is inherited above when local has no dependency
        // list. Other collection-presence markers remain local to this layer.

        // Unknown (typo'd) keys carry over so the "did you mean?" warning
        // still fires for a typo that lives in the template rather than in
        // the service that extends it.
        for (key, value) in &base.unknown_fields {
            local
                .unknown_fields
                .entry(key.clone())
                .or_insert_with(|| value.clone());
        }

        // Clear the extends field after merging to avoid confusion
        local.extends = None;

        Ok(())
    }

    /// Union two plain string lists, preserving local order first and
    /// skipping duplicates.
    fn merge_string_list(local: &mut Vec<String>, base: &[String]) {
        for item in base {
            if !local.contains(item) {
                local.push(item.clone());
            }
        }
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
            } else if !local.contains(base_vol) {
                // Anonymous-volume paths are set entries too: defaults and a
                // template can both contribute the same container path.
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
    use crate::config::{HealthCheck, HealthCheckTiming, RestartPolicy};

    #[test]
    fn repeated_volume_defaults_do_not_duplicate_anonymous_paths() {
        let mut local = vec!["/cache".into(), "local:/data".into()];
        let base = vec!["/cache".into(), "base:/data".into(), "/scratch".into()];
        ServiceMerger::merge_volumes(&mut local, &base);
        ServiceMerger::merge_volumes(&mut local, &base);
        assert_eq!(local, vec!["/cache", "local:/data", "/scratch"]);
    }

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
    fn test_extends_sites_finds_services_and_variants() {
        let mut config = Config::default();

        config.services.insert(
            "svc1".to_string(),
            Service {
                extends: Some("pkg1.base".to_string()),
                ..Default::default()
            },
        );
        config.services.insert(
            "svc2".to_string(),
            Service {
                image: Some("test".to_string()),
                ..Default::default()
            },
        );
        // A variant carrying its own `extends:` is a site in its own right.
        config.services.insert(
            "svc3".to_string(),
            Service {
                variants: [
                    (
                        "go".to_string(),
                        Service {
                            extends: Some("go-worker".to_string()),
                            ..Default::default()
                        },
                    ),
                    (
                        "java".to_string(),
                        Service {
                            gradle_task: Some(":svc3:run".to_string()),
                            ..Default::default()
                        },
                    ),
                ]
                .into(),
                ..Default::default()
            },
        );

        let sites = ServiceMerger::extends_sites(&config);

        assert_eq!(
            sites,
            vec![
                (ExtendsSite::service("svc1"), "pkg1.base".to_string()),
                (ExtendsSite::variant("svc3", "go"), "go-worker".to_string()),
            ],
            "only sites that actually wrote `extends:`, in sorted order"
        );
    }

    #[test]
    fn test_extends_site_describes_where_it_was_written() {
        assert_eq!(
            ExtendsSite::service("catalog").describe(),
            "service 'catalog'"
        );
        assert_eq!(
            ExtendsSite::variant("catalog", "go").describe(),
            "variant 'go' of service 'catalog'"
        );
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
                timing: HealthCheckTiming::default(),
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

    // Legacy-spelling records found while parsing a package's own fed.yaml
    // must surface through the main config's warning list (with the package
    // identity prefixed), since only that list feeds the validate/start notice.
    #[test]
    fn test_merge_packages_propagates_legacy_key_usages() {
        let mut main_config = Config::default();
        main_config.services.insert(
            "my-db".to_string(),
            Service {
                extends: Some("db-pkg.postgres".to_string()),
                ..Default::default()
            },
        );

        let mut pkg_config = Config::default();
        pkg_config.services.insert(
            "postgres".to_string(),
            Service {
                image: Some("postgres:15".to_string()),
                expose: true,
                ..Default::default()
            },
        );
        pkg_config
            .legacy_key_usages
            .push(crate::config::LegacyKeyUsage {
                location: "service 'postgres'".to_string(),
                legacy: "httpGet",
                canonical: "http_get",
            });

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

        ServiceMerger::merge_packages(&mut main_config, &packages).unwrap();

        assert_eq!(main_config.legacy_key_usages.len(), 1);
        let u = &main_config.legacy_key_usages[0];
        assert_eq!(u.location, "package 'db-pkg': service 'postgres'");
        assert_eq!(u.legacy, "httpGet");
        assert_eq!(u.canonical, "http_get");
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
