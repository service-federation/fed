use super::{Config, LegacyKeyUsage};
use crate::error::{Error, Result};
use std::fs;
use std::path::{Path, PathBuf};

pub struct Parser;

impl Parser {
    pub fn new() -> Self {
        Self
    }

    /// Find config file starting from current directory
    pub fn find_config_file(&self) -> Result<PathBuf> {
        let current_dir = std::env::current_dir()?;
        Self::find_config_in_dir(&current_dir)
    }

    pub fn find_config_in_dir(dir: &Path) -> Result<PathBuf> {
        Self::find_config_in_dir_inner(dir, dir)
    }

    fn find_config_in_dir_inner(dir: &Path, origin: &Path) -> Result<PathBuf> {
        if let Some((config_path, warning)) =
            crate::config::discovery::config_file_in_dir_with_warning(dir)
        {
            if let Some(warning) = warning {
                eprintln!("{}", warning);
            }
            return Ok(config_path);
        }

        // Try parent directory
        if let Some(parent) = dir.parent() {
            return Self::find_config_in_dir_inner(parent, origin);
        }

        Err(Error::Config(format!(
            "Could not find fed.yaml in '{}' or any parent directory.\n\
             Create one with `fed init` or specify a path with `fed -c <path>`",
            origin.display()
        )))
    }

    /// Load config from file path.
    ///
    /// Resolves local templates and applies `defaults:`. Package extensions
    /// need the async packages-aware loader
    /// ([`Self::load_config_with_packages_offline`]); this path is for
    /// package-free configs and for callers that only need the local view.
    pub fn load_config<P: AsRef<Path>>(&self, path: P) -> Result<Config> {
        let mut config = self.load_config_pre_packages(path)?;
        crate::config::defaults::apply(&mut config)?;
        Ok(config)
    }

    /// Everything [`Self::load_config`] does except applying `defaults:`.
    ///
    /// `defaults:` has to run *after* every `extends:` has been resolved,
    /// including package ones: a template's value is more specific than a
    /// global default, so it must reach the service first or the default
    /// will already be occupying the field. That ordering is the whole reason
    /// this split exists.
    fn load_config_pre_packages<P: AsRef<Path>>(&self, path: P) -> Result<Config> {
        let content = fs::read_to_string(path.as_ref()).map_err(|e| {
            Error::Config(format!(
                "Failed to read config file '{}': {}",
                path.as_ref().display(),
                e
            ))
        })?;

        let mut config = self.parse_config(&content)?;
        // Resolve local template extensions here so every loader — including
        // the synchronous one behind `fed validate` — sees merged services.
        crate::package::ServiceMerger::merge_local_templates(&mut config)?;
        Ok(config)
    }

    /// Load config and resolve packages (async version with package extension)
    /// This is the main entry point for loading configs with package support
    pub async fn load_config_with_packages<P: AsRef<Path>>(&self, path: P) -> Result<Config> {
        self.load_config_with_packages_offline(path, false).await
    }

    /// Load config and resolve packages with offline mode option
    /// When offline=true, git operations are skipped and only cached packages are used
    pub async fn load_config_with_packages_offline<P: AsRef<Path>>(
        &self,
        path: P,
        offline: bool,
    ) -> Result<Config> {
        // Local template extensions resolve here; `defaults:` is deliberately
        // held back until after package merging below.
        let mut config = self.load_config_pre_packages(path.as_ref())?;

        // If there are no packages, there is nothing further to resolve.
        if config.packages.is_empty() {
            crate::config::defaults::apply(&mut config)?;
            return Ok(config);
        }

        // Resolve packages
        let config_dir = path
            .as_ref()
            .parent()
            .ok_or_else(|| Error::Config("Invalid config path".to_string()))?;

        let mut resolver = crate::package::PackageResolver::with_offline(config_dir, offline)?;
        let packages = resolver.resolve_all(&config.packages).await?;

        // Apply service extensions from packages
        crate::package::ServiceMerger::merge_packages(&mut config, &packages)?;

        // Every `extends:` is resolved now, so the defaults can fill what is
        // still unset without displacing a more specific package value.
        crate::config::defaults::apply(&mut config)?;

        Ok(config)
    }

    /// Parse config from YAML string
    pub fn parse_config(&self, content: &str) -> Result<Config> {
        let mut config: Config = serde_yaml::from_str(content)
            .map_err(|e| Error::Parse(format!("Failed to parse YAML config: {}", e)))?;

        // Second lightweight parse to a raw Value. Two things are only
        // visible in the raw document: serde aliases consume the legacy-cased
        // keys (httpGet, gradleTask, ...) without a trace, and an explicitly
        // empty collection (`depends_on: []`) is indistinguishable from an
        // absent one once serde has turned both into an empty `Vec`.
        if let Ok(doc) = serde_yaml::from_str::<serde_yaml::Value>(content) {
            config.legacy_key_usages = scan_legacy_spellings(&doc);
            record_explicit_empty(&doc, &mut config);
        }

        Ok(config)
    }
}

/// Record, for every service/template/variant/defaults block, which keys were
/// written with an explicitly empty collection value.
///
/// `depends_on: []` and an omitted `depends_on:` both deserialize to an empty
/// `Vec`, but they mean opposite things once a `defaults:` block is in play:
/// the first is "this service has no dependencies", the second is "this
/// service didn't say". Only the raw document can tell them apart.
fn record_explicit_empty(doc: &serde_yaml::Value, config: &mut Config) {
    fn empty_keys(fields: &serde_yaml::Mapping) -> std::collections::BTreeSet<String> {
        fields
            .iter()
            .filter_map(|(key, value)| {
                let key = key.as_str()?;
                let is_empty_collection = match value {
                    serde_yaml::Value::Sequence(seq) => seq.is_empty(),
                    serde_yaml::Value::Mapping(map) => map.is_empty(),
                    _ => false,
                };
                is_empty_collection.then(|| key.to_string())
            })
            .collect()
    }

    if let Some(fields) = doc.get("defaults").and_then(serde_yaml::Value::as_mapping)
        && let Some(defaults) = config.defaults.as_mut()
    {
        defaults.explicit_empty = empty_keys(fields);
    }

    for section in ["services", "templates"] {
        let Some(map) = doc.get(section).and_then(serde_yaml::Value::as_mapping) else {
            continue;
        };
        for (name, raw) in map {
            let (Some(name), Some(fields)) = (name.as_str(), raw.as_mapping()) else {
                continue;
            };
            let target = match section {
                "services" => config.services.get_mut(name),
                _ => config.templates.get_mut(name),
            };
            let Some(target) = target else { continue };
            target.explicit_empty = empty_keys(fields);

            // Variants are partial services and opt out the same way.
            let Some(variants) = fields
                .get("variants")
                .and_then(serde_yaml::Value::as_mapping)
            else {
                continue;
            };
            for (variant_name, raw_variant) in variants {
                let (Some(variant_name), Some(variant_fields)) =
                    (variant_name.as_str(), raw_variant.as_mapping())
                else {
                    continue;
                };
                if let Some(variant) = target.variants.get_mut(variant_name) {
                    variant.explicit_empty = empty_keys(variant_fields);
                }
            }
        }
    }
}

/// Find legacy-cased keys in the raw YAML document. Both spellings parse fine —
/// this only feeds the non-fatal "prefer snake_case" notice at validate/start.
fn scan_legacy_spellings(doc: &serde_yaml::Value) -> Vec<LegacyKeyUsage> {
    const LEGACY_SERVICE_KEYS: &[(&str, &str)] = &[
        ("gradleTask", "gradle_task"),
        ("composeFile", "compose_file"),
        ("composeService", "compose_service"),
    ];

    let mut out = Vec::new();
    for (section, label) in [("services", "service"), ("templates", "template")] {
        let Some(map) = doc.get(section).and_then(serde_yaml::Value::as_mapping) else {
            continue;
        };
        for (name, service) in map {
            let Some(fields) = service.as_mapping() else {
                continue;
            };
            let location = format!("{label} '{}'", name.as_str().unwrap_or("?"));
            for &(legacy, canonical) in LEGACY_SERVICE_KEYS {
                if fields.contains_key(legacy) {
                    out.push(LegacyKeyUsage {
                        location: location.clone(),
                        legacy,
                        canonical,
                    });
                }
            }
            if let Some(hc) = fields
                .get("healthcheck")
                .and_then(serde_yaml::Value::as_mapping)
                && hc.contains_key("httpGet")
            {
                out.push(LegacyKeyUsage {
                    location: location.clone(),
                    legacy: "httpGet",
                    canonical: "http_get",
                });
            }
            if let Some(serde_yaml::Value::Tagged(tagged)) = fields.get("restart")
                && tagged.tag == "onfailure"
            {
                out.push(LegacyKeyUsage {
                    location,
                    legacy: "!onfailure",
                    canonical: "!on_failure",
                });
            }
        }
    }
    out
}

impl Default for Parser {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_compose_import_shorthand_and_options() {
        let config = Parser::new()
            .parse_config(
                r#"
compose:
  - ./compose.yaml
  - file: ./observability.yaml
    namespace: observability
    profiles: [debug]
    environment:
      HTTP_PORT: "{{PORT}}"
"#,
            )
            .unwrap();
        assert_eq!(config.compose.len(), 2);
        assert_eq!(config.compose[0].options().file, "./compose.yaml");
        let options = config.compose[1].options();
        assert_eq!(options.namespace.as_deref(), Some("observability"));
        assert_eq!(options.profiles, ["debug"]);
        assert_eq!(options.environment["HTTP_PORT"], "{{PORT}}");
    }

    #[test]
    fn test_parse_simple_config() {
        let yaml = r#"
parameters:
  PORT:
    type: port

services:
  backend:
    process: echo "Hello"
    environment:
      PORT: "{{PORT}}"
    depends_on:
      - database

  database:
    process: echo "Database"

entrypoint: backend
"#;

        let parser = Parser::new();
        let config = parser.parse_config(yaml).unwrap();

        assert_eq!(config.services.len(), 2);
        assert_eq!(config.entrypoint, Some("backend".to_string()));
        assert!(config.parameters.contains_key("PORT"));
        assert!(config.legacy_key_usages.is_empty());
    }

    // End-to-end legacy-casing coverage: a config written entirely in the old
    // camelCase spellings must (a) parse into the same fields as snake_case,
    // (b) leak nothing into unknown_fields, and (c) be flagged for the soft
    // deprecation notice.
    #[test]
    fn test_parse_config_with_legacy_spellings() {
        let yaml = r#"
services:
  worker:
    gradleTask: ":worker:bootRun"
    healthcheck:
      httpGet: "http://localhost:8080/health"
    restart: !onfailure
      max_retries: 3
  db:
    composeFile: docker-compose.yml
    composeService: postgres
"#;
        let config = Parser::new().parse_config(yaml).unwrap();

        let worker = &config.services["worker"];
        assert_eq!(worker.gradle_task.as_deref(), Some(":worker:bootRun"));
        assert_eq!(
            worker.healthcheck.as_ref().and_then(|h| h.get_http_url()),
            Some("http://localhost:8080/health")
        );
        assert!(matches!(
            worker.restart,
            Some(crate::config::RestartPolicy::OnFailure {
                max_retries: Some(3)
            })
        ));
        let db = &config.services["db"];
        assert_eq!(db.compose_file.as_deref(), Some("docker-compose.yml"));
        assert_eq!(db.compose_service.as_deref(), Some("postgres"));
        assert!(worker.unknown_fields.is_empty());
        assert!(db.unknown_fields.is_empty());

        let mut flagged: Vec<(&str, &str)> = config
            .legacy_key_usages
            .iter()
            .map(|u| (u.location.as_str(), u.legacy))
            .collect();
        flagged.sort();
        assert_eq!(
            flagged,
            vec![
                ("service 'db'", "composeFile"),
                ("service 'db'", "composeService"),
                ("service 'worker'", "!onfailure"),
                ("service 'worker'", "gradleTask"),
                ("service 'worker'", "httpGet"),
            ]
        );
    }

    #[test]
    fn test_canonical_config_reports_no_legacy_usage() {
        let yaml = r#"
services:
  worker:
    gradle_task: ":worker:bootRun"
    healthcheck:
      http_get: "http://localhost:8080/health"
    restart: !on_failure
      max_retries: 3
  db:
    compose_file: docker-compose.yml
    compose_service: postgres
"#;
        let config = Parser::new().parse_config(yaml).unwrap();
        assert!(config.legacy_key_usages.is_empty());
        assert_eq!(
            config.services["db"].compose_service.as_deref(),
            Some("postgres")
        );
    }
}
