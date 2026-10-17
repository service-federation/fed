//! Log rotation limits shared by the global and project configuration files.

use crate::error::{Error, Result};
use serde::ser::SerializeStruct;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::fs;
use std::io::ErrorKind;
use std::path::Path;

const DEFAULT_MAX_SIZE: u64 = 10 * 1024 * 1024;
const MAX_ALLOWED_SIZE: u64 = 1024 * 1024 * 1024 * 1024;
const MAX_ALLOWED_FILES: usize = 1000;

/// Effective limits for a service's retained log files.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LoggingConfig {
    pub max_size: u64,
    pub max_files: usize,
}

impl Serialize for LoggingConfig {
    fn serialize<S: Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        let mut state = serializer.serialize_struct("LoggingConfig", 2)?;
        state.serialize_field("max_size", &format!("{}B", self.max_size))?;
        state.serialize_field("max_files", &self.max_files)?;
        state.end()
    }
}

impl Default for LoggingConfig {
    fn default() -> Self {
        Self {
            max_size: DEFAULT_MAX_SIZE,
            max_files: 3,
        }
    }
}

/// A YAML layer keeps omitted fields separate from explicit values.
#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct LoggingOverrides {
    #[serde(default, deserialize_with = "deserialize_optional_size")]
    max_size: Option<u64>,
    #[serde(default, deserialize_with = "deserialize_optional_count")]
    max_files: Option<usize>,
}

impl LoggingOverrides {
    fn merge_over(self, lower: LoggingConfig) -> LoggingConfig {
        LoggingConfig {
            max_size: self.max_size.unwrap_or(lower.max_size),
            max_files: self.max_files.unwrap_or(lower.max_files),
        }
    }

    fn validate(&self) -> std::result::Result<(), String> {
        if let Some(size) = self.max_size
            && !(1..=MAX_ALLOWED_SIZE).contains(&size)
        {
            return Err("logs.max_size must be between 1B and 1TiB".into());
        }
        if let Some(files) = self.max_files
            && !(1..=MAX_ALLOWED_FILES).contains(&files)
        {
            return Err(format!(
                "logs.max_files must be between 1 and {MAX_ALLOWED_FILES}"
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct GlobalConfig {
    #[serde(default)]
    logs: LoggingOverrides,
}

impl<'de> Deserialize<'de> for LoggingConfig {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> std::result::Result<Self, D::Error> {
        let overrides = LoggingOverrides::deserialize(deserializer)?;
        overrides.validate().map_err(serde::de::Error::custom)?;
        Ok(overrides.merge_over(Self::default()))
    }
}

impl LoggingConfig {
    pub fn validate(self) -> Result<()> {
        LoggingOverrides {
            max_size: Some(self.max_size),
            max_files: Some(self.max_files),
        }
        .validate()
        .map_err(Error::Config)
    }

    /// Read `$XDG_CONFIG_HOME/fed/config.yaml`, falling back to `~/.config`.
    /// Absent files use defaults. Reading never creates a directory or file.
    pub fn load_global() -> Result<Self> {
        let directories = xdg::BaseDirectories::with_prefix("fed");
        let Some(directory) = directories.get_config_home() else {
            eprintln!(
                "Warning: cannot locate a home directory for fed's global config; using default log limits"
            );
            return Ok(Self::default());
        };
        let path = directory.join("config.yaml");
        Self::load_global_from(&path)
    }

    fn load_global_from(path: &Path) -> Result<Self> {
        let content = match fs::read_to_string(path) {
            Ok(content) => content,
            Err(error) if error.kind() == ErrorKind::NotFound => return Ok(Self::default()),
            Err(error) if error.kind() == ErrorKind::PermissionDenied => {
                eprintln!(
                    "Warning: cannot read global config '{}': {error}; using default log limits",
                    path.display()
                );
                return Ok(Self::default());
            }
            Err(error) => {
                return Err(Error::Config(format!(
                    "Failed to read global config '{}': {error}",
                    path.display()
                )));
            }
        };
        let config: GlobalConfig = serde_yaml::from_str(&content).map_err(|error| {
            Error::Config(format!(
                "Failed to parse global config '{}': {error}",
                path.display()
            ))
        })?;
        let overrides = config.logs;
        overrides.validate().map_err(|error| {
            Error::Config(format!(
                "Invalid global config '{}': {error}",
                path.display()
            ))
        })?;
        Ok(overrides.merge_over(Self::default()))
    }

    pub(super) fn merge_project(content: &str, global: Self) -> Result<Self> {
        #[derive(Deserialize)]
        struct ProjectLogging {
            #[serde(default)]
            logs: LoggingOverrides,
        }

        let project: ProjectLogging = serde_yaml::from_str(content)
            .map_err(|error| Error::Parse(format!("Failed to parse YAML config: {error}")))?;
        let overrides = project.logs;
        overrides.validate().map_err(Error::Config)?;
        Ok(overrides.merge_over(global))
    }
}

fn deserialize_optional_size<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> std::result::Result<Option<u64>, D::Error> {
    let value = String::deserialize(deserializer)?;
    parse_size(&value)
        .map(Some)
        .map_err(serde::de::Error::custom)
}

fn deserialize_optional_count<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> std::result::Result<Option<usize>, D::Error> {
    usize::deserialize(deserializer).map(Some)
}

fn parse_size(value: &str) -> std::result::Result<u64, String> {
    let value = value.trim();
    let digits = value.bytes().take_while(u8::is_ascii_digit).count();
    let number = value[..digits]
        .parse::<u64>()
        .map_err(|_| "logs.max_size must be a size such as 10MiB".to_string())?;
    let multiplier: u64 = match &value[digits..] {
        "B" => 1,
        "KiB" => 1024,
        "MiB" => 1024 * 1024,
        "GiB" => 1024 * 1024 * 1024,
        "TiB" => 1024 * 1024 * 1024 * 1024,
        _ => return Err("logs.max_size must use B, KiB, MiB, GiB, or TiB".into()),
    };
    number
        .checked_mul(multiplier)
        .ok_or_else(|| "logs.max_size is too large".into())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn project_fields_override_independently() {
        let global = LoggingConfig {
            max_size: 20 * 1024 * 1024,
            max_files: 5,
        };
        let resolved = LoggingConfig::merge_project("logs:\n  max_files: 2\n", global).unwrap();
        assert_eq!(resolved.max_size, global.max_size);
        assert_eq!(resolved.max_files, 2);

        let resolved = LoggingConfig::merge_project("logs:\n  max_size: 1GiB\n", global).unwrap();
        assert_eq!(resolved.max_size, 1024 * 1024 * 1024);
        assert_eq!(resolved.max_files, global.max_files);
    }

    #[test]
    fn reads_global_file_without_creating_paths() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("fed/config.yaml");
        assert_eq!(
            LoggingConfig::load_global_from(&path).unwrap(),
            LoggingConfig::default()
        );
        assert!(!path.parent().unwrap().exists());

        fs::create_dir(path.parent().unwrap()).unwrap();
        fs::write(&path, "logs:\n  max_size: 64KiB\n  max_files: 7\n").unwrap();
        let config = LoggingConfig::load_global_from(&path).unwrap();
        assert_eq!(config.max_size, 64 * 1024);
        assert_eq!(config.max_files, 7);
        assert_eq!(
            fs::read_to_string(&path).unwrap(),
            "logs:\n  max_size: 64KiB\n  max_files: 7\n"
        );
    }

    #[test]
    fn global_errors_include_path() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("config.yaml");
        fs::write(&path, "logs:\n  max_files: 0\n").unwrap();
        let error = LoggingConfig::load_global_from(&path)
            .unwrap_err()
            .to_string();
        assert!(error.contains("config.yaml"));
        assert!(error.contains("max_files"));

        fs::write(&path, "logs: [broken]\n").unwrap();
        let error = LoggingConfig::load_global_from(&path)
            .unwrap_err()
            .to_string();
        assert!(error.contains("config.yaml"));
    }

    #[test]
    fn rejects_invalid_limits() {
        for yaml in [
            "logs:\n  max_size: 0B\n",
            "logs:\n  max_size: 2TiB\n",
            "logs:\n  max_size: 10MB\n",
            "logs:\n  max_files: 0\n",
            "logs:\n  max_files: 1001\n",
            "logs: null\n",
            "logs:\n  max_files: null\n",
            "logs:\n  max_size: null\n",
        ] {
            assert!(LoggingConfig::merge_project(yaml, LoggingConfig::default()).is_err());
        }
    }

    #[test]
    fn serialization_round_trips() {
        let expected = LoggingConfig::default();
        let yaml = serde_yaml::to_string(&expected).unwrap();
        assert_eq!(
            serde_yaml::from_str::<LoggingConfig>(&yaml).unwrap(),
            expected
        );
    }
}
