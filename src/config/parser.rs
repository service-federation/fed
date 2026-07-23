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

    /// Load config from file path
    pub fn load_config<P: AsRef<Path>>(&self, path: P) -> Result<Config> {
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
        // Package extensions still require the async packages-aware loader.
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
        // Load the base config (local template extensions resolve in load_config)
        let mut config = self.load_config(path.as_ref())?;

        // If there are no packages, return config after template resolution
        if config.packages.is_empty() {
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

        Ok(config)
    }

    /// Parse config from YAML string
    pub fn parse_config(&self, content: &str) -> Result<Config> {
        let mut config: Config = serde_yaml::from_str(content)
            .map_err(|e| Error::Parse(format!("Failed to parse YAML config: {}", e)))?;

        // Second lightweight parse to a raw Value: serde aliases consume the
        // legacy-cased keys (httpGet, gradleTask, ...) without a trace, so the
        // soft deprecation notice has to come from the raw document.
        if let Ok(doc) = serde_yaml::from_str::<serde_yaml::Value>(content) {
            config.legacy_key_usages = scan_legacy_spellings(&doc);
        }

        Ok(config)
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
