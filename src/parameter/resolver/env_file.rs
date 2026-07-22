// Split from resolver.rs (see git history before this commit for pre-split blame).
use super::*;

impl Resolver {
    /// Load .env files and apply values to parameters.
    /// Returns error if .env file sets a variable that isn't declared as a parameter.
    ///
    /// All variables are loaded first, then applied. This means:
    /// - Later .env files override earlier ones for the same variable
    /// - The error message for undeclared variables references the last file that set it
    pub(super) fn apply_env_file_to_parameters(&self, config: &mut Config) -> Result<()> {
        if config.env_file.is_empty() {
            return Ok(());
        }

        let config_dir = self.work_dir.as_ref().ok_or_else(|| {
            Error::TemplateResolution(
                "Work directory not set, cannot resolve global env_file paths".to_string(),
            )
        })?;

        // Load all .env files first, tracking which file each variable came from.
        // Later files override earlier ones (consistent with documented behavior).
        let mut all_env_vars: HashMap<String, (String, String)> = HashMap::new();

        for env_file_path in &config.env_file {
            let full_path = crate::parameter::expand_tilde(Path::new(env_file_path));
            let full_path = if full_path.is_absolute() {
                full_path
            } else {
                config_dir.join(full_path)
            };
            let env_vars = match crate::config::env_loader::load_env_file_optional(&full_path)
                .map_err(|e| {
                    Error::TemplateResolution(format!(
                        "Failed to load environment file '{}' (resolved to '{}'): {}",
                        env_file_path,
                        full_path.display(),
                        e
                    ))
                })? {
                Some(vars) => vars,
                None => {
                    tracing::warn!(
                        "env_file '{}' (resolved to '{}') does not exist — continuing without it. \
                         Parameters that depend on values from this file may be unset.",
                        env_file_path,
                        full_path.display()
                    );
                    continue;
                }
            };

            // Track value and source file (later files override earlier)
            for (key, value) in env_vars {
                all_env_vars.insert(key, (value, env_file_path.clone()));
            }
        }

        // Now apply with a single mutable borrow
        let effective_params = config.get_effective_parameters_mut();
        for (key, (value, env_file_path)) in all_env_vars {
            if let Some(param) = effective_params.get_mut(&key) {
                // Only set if parameter doesn't already have an explicit value
                // (explicit values take precedence over .env files)
                if param.value.is_none() {
                    param.value = Some(value);
                }
            } else {
                // Variable is not declared as a parameter - this is an error
                return Err(Error::UndeclaredEnvVariable {
                    name: key,
                    env_file: env_file_path,
                });
            }
        }

        Ok(())
    }
}

#[cfg(test)]
#[allow(clippy::field_reassign_with_default)]
mod tests {
    use super::*;

    #[test]
    fn test_env_file_sets_parameter_value() {
        use crate::config::{Config, Parameter};
        use std::fs;
        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();
        let env_path = temp_dir.path().join(".env");
        fs::write(&env_path, "MY_PARAM=from_env_file\n").unwrap();

        let mut resolver = Resolver::new();
        resolver.set_work_dir(temp_dir.path());

        let mut config = Config::default();

        // Declare the parameter (must exist for .env file to work)
        let param = Parameter {
            development: None,
            develop: None,
            staging: None,
            production: None,
            param_type: None,
            default: Some(serde_yaml::Value::String("default_value".to_string())),
            either: vec![],
            source: None,
            description: None,
            optional: None,
            ..Default::default()
        };

        config.parameters.insert("MY_PARAM".to_string(), param);
        config.env_file = vec![".env".to_string()];

        resolver.resolve_parameters(&mut config).unwrap();

        let resolved = resolver.get_resolved_parameters();
        // .env file value should override the default
        assert_eq!(resolved.get("MY_PARAM").unwrap(), "from_env_file");
    }

    #[test]
    fn test_env_file_respects_explicit_value() {
        use crate::config::{Config, Parameter};
        use std::fs;
        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();
        let env_path = temp_dir.path().join(".env");
        fs::write(&env_path, "MY_PARAM=from_env_file\n").unwrap();

        let mut resolver = Resolver::new();
        resolver.set_work_dir(temp_dir.path());

        let mut config = Config::default();

        // Declare the parameter with an explicit value already set
        let param = Parameter {
            development: None,
            develop: None,
            staging: None,
            production: None,
            param_type: None,
            default: Some(serde_yaml::Value::String("default_value".to_string())),
            either: vec![],
            source: None,
            description: None,
            optional: None,
            generate: None,
            value: Some("explicit_value".to_string()), // Explicit value takes precedence
        };

        config.parameters.insert("MY_PARAM".to_string(), param);
        config.env_file = vec![".env".to_string()];

        resolver.resolve_parameters(&mut config).unwrap();

        let resolved = resolver.get_resolved_parameters();
        // Explicit value should NOT be overridden by .env file
        assert_eq!(resolved.get("MY_PARAM").unwrap(), "explicit_value");
    }

    #[test]
    fn test_env_file_rejects_undeclared_variable() {
        use crate::config::Config;
        use std::fs;
        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();
        let env_path = temp_dir.path().join(".env");
        // This variable is NOT declared in parameters - should error
        fs::write(&env_path, "UNDECLARED_VAR=some_value\n").unwrap();

        let mut resolver = Resolver::new();
        resolver.set_work_dir(temp_dir.path());

        let mut config = Config::default();
        config.env_file = vec![".env".to_string()];

        let result = resolver.resolve_parameters(&mut config);

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("UNDECLARED_VAR"));
        assert!(err.to_string().contains("undeclared parameter"));
    }

    #[test]
    fn test_env_file_works_with_service_environment() {
        use crate::config::{Config, Parameter, Service};
        use std::fs;
        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();
        let env_path = temp_dir.path().join(".env");
        fs::write(&env_path, "API_KEY=secret123\n").unwrap();

        let mut resolver = Resolver::new();
        resolver.set_work_dir(temp_dir.path());

        let mut config = Config::default();

        // Declare the parameter
        let param = Parameter {
            development: None,
            develop: None,
            staging: None,
            production: None,
            param_type: None,
            default: Some(serde_yaml::Value::String("".to_string())),
            either: vec![],
            source: None,
            description: None,
            optional: None,
            ..Default::default()
        };

        config.parameters.insert("API_KEY".to_string(), param);
        config.env_file = vec![".env".to_string()];

        // Service references parameter in its environment
        let mut env = HashMap::new();
        env.insert("SECRET".to_string(), "{{API_KEY}}".to_string());
        let service = Service {
            process: Some("echo test".to_string()),
            environment: env,
            ..Default::default()
        };

        config.services.insert("api".to_string(), service);

        resolver.resolve_parameters(&mut config).unwrap();
        let resolved_config = resolver.resolve_config(&config).unwrap();

        let resolved_service = resolved_config.services.get("api").unwrap();
        // The service environment should have the value from .env file via the parameter
        assert_eq!(
            resolved_service.environment.get("SECRET").unwrap(),
            "secret123"
        );
    }

    #[test]
    fn test_env_file_empty_file() {
        use crate::config::Config;
        use std::fs;
        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();
        let env_path = temp_dir.path().join(".env");
        // Empty file with only comments
        fs::write(&env_path, "# Just comments\n# No actual variables\n").unwrap();

        let mut resolver = Resolver::new();
        resolver.set_work_dir(temp_dir.path());

        let mut config = Config::default();
        config.env_file = vec![".env".to_string()];

        // Should succeed - empty env file is valid
        resolver.resolve_parameters(&mut config).unwrap();
    }

    #[test]
    fn test_env_file_multiple_files_later_overrides() {
        use crate::config::{Config, Parameter};
        use std::fs;
        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();
        let env1_path = temp_dir.path().join(".env1");
        let env2_path = temp_dir.path().join(".env2");

        fs::write(&env1_path, "MY_PARAM=from_first\n").unwrap();
        fs::write(&env2_path, "MY_PARAM=from_second\n").unwrap();

        let mut resolver = Resolver::new();
        resolver.set_work_dir(temp_dir.path());

        let mut config = Config::default();

        let param = Parameter {
            development: None,
            develop: None,
            staging: None,
            production: None,
            param_type: None,
            default: Some(serde_yaml::Value::String("default".to_string())),
            either: vec![],
            source: None,
            description: None,
            optional: None,
            ..Default::default()
        };

        config.parameters.insert("MY_PARAM".to_string(), param);
        config.env_file = vec![".env1".to_string(), ".env2".to_string()];

        resolver.resolve_parameters(&mut config).unwrap();

        let resolved = resolver.get_resolved_parameters();
        // Second file should win
        assert_eq!(resolved.get("MY_PARAM").unwrap(), "from_second");
    }

    #[test]
    fn test_env_file_undeclared_error_shows_file_name() {
        use crate::config::Config;
        use std::fs;
        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();
        let env_path = temp_dir.path().join(".env.test");

        // Variable is NOT declared in parameters - should error
        fs::write(&env_path, "UNDECLARED=value\n").unwrap();

        let mut resolver = Resolver::new();
        resolver.set_work_dir(temp_dir.path());

        let mut config = Config::default();
        config.env_file = vec![".env.test".to_string()];

        let result = resolver.resolve_parameters(&mut config);
        assert!(result.is_err());

        let err = result.unwrap_err();
        let err_str = err.to_string();
        assert!(err_str.contains("UNDECLARED"));
        // Error should reference the source file
        assert!(err_str.contains(".env.test"));
    }

    #[test]
    fn absolute_env_file_path_loaded() {
        use crate::config::{Config, Parameter};
        use tempfile::TempDir;

        let work_dir = TempDir::new().unwrap();
        let secrets_dir = TempDir::new().unwrap();
        let secrets_path = secrets_dir.path().join("secrets.env");
        std::fs::write(&secrets_path, "API_TOKEN=from_absolute_path\n").unwrap();

        let mut resolver = Resolver::new();
        resolver.set_work_dir(work_dir.path());

        let mut config = Config::default();
        config.env_file = vec![secrets_path.to_string_lossy().to_string()];
        config.parameters.insert(
            "API_TOKEN".to_string(),
            Parameter {
                ..Default::default()
            },
        );

        resolver.resolve_parameters(&mut config).unwrap();

        let resolved = resolver.get_resolved_parameters();
        assert_eq!(
            resolved.get("API_TOKEN").unwrap(),
            "from_absolute_path",
            "Absolute env_file paths should be loaded directly, not joined with work_dir"
        );
    }

    #[test]
    fn tilde_env_file_path_expanded() {
        use crate::config::{Config, Parameter};
        use tempfile::TempDir;

        let home = dirs::home_dir().expect("test requires home dir");
        let test_dir = home.join(".fed-test-tmp");
        std::fs::create_dir_all(&test_dir).unwrap();
        let env_file = test_dir.join("test.env");
        std::fs::write(&env_file, "TILDE_VAR=it_works\n").unwrap();

        let work_dir = TempDir::new().unwrap();
        let mut resolver = Resolver::new();
        resolver.set_work_dir(work_dir.path());

        let mut config = Config::default();
        config.env_file = vec!["~/.fed-test-tmp/test.env".to_string()];
        config.parameters.insert(
            "TILDE_VAR".to_string(),
            Parameter {
                ..Default::default()
            },
        );

        let result = resolver.resolve_parameters(&mut config);
        // Clean up before asserting
        let _ = std::fs::remove_dir_all(&test_dir);

        result.unwrap();
        let resolved = resolver.get_resolved_parameters();
        assert_eq!(resolved.get("TILDE_VAR").unwrap(), "it_works");
    }

    #[test]
    fn missing_env_file_warns_and_continues() {
        use crate::config::{Config, Parameter};
        use tempfile::TempDir;

        let work_dir = TempDir::new().unwrap();
        let mut resolver = Resolver::new();
        resolver.set_work_dir(work_dir.path());

        // Reference an env_file that doesn't exist on disk.
        let mut config = Config::default();
        config.env_file = vec!["does-not-exist.env".to_string()];
        config.parameters.insert(
            "API_KEY".to_string(),
            Parameter {
                value: Some("from-default".to_string()),
                ..Default::default()
            },
        );

        // Resolution should succeed; the parameter keeps its default value.
        resolver.resolve_parameters(&mut config).unwrap();
        let resolved = resolver.get_resolved_parameters();
        assert_eq!(resolved.get("API_KEY").unwrap(), "from-default");
    }

    #[test]
    fn missing_env_file_still_errors_on_parse_failure() {
        use crate::config::{Config, Parameter};
        use tempfile::TempDir;

        let work_dir = TempDir::new().unwrap();
        let env_path = work_dir.path().join("bad.env");
        // Invalid env name — file exists but parse/validate fails.
        std::fs::write(&env_path, "INVALID-NAME=value\n").unwrap();

        let mut resolver = Resolver::new();
        resolver.set_work_dir(work_dir.path());

        let mut config = Config::default();
        config.env_file = vec!["bad.env".to_string()];
        config
            .parameters
            .insert("WHATEVER".to_string(), Parameter::default());

        let err = resolver
            .resolve_parameters(&mut config)
            .expect_err("malformed env file should still error");
        assert!(err.to_string().contains("bad.env"));
    }
}
