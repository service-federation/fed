use super::Config;
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
        let config: Config = serde_yaml::from_str(content)
            .map_err(|e| Error::Parse(format!("Failed to parse YAML config: {}", e)))?;

        Ok(config)
    }
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
    }
}
