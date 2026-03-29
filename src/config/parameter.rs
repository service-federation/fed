//! Parameter configuration types.
//!
//! This module contains the [`Parameter`] struct for configuring
//! variables with environment-specific defaults and type constraints.

use serde::{Deserialize, Serialize};

/// Parameter/variable configuration.
///
/// Parameters can have:
/// - A default value
/// - Environment-specific values (development, staging, production)
/// - Type constraints (e.g., "port" for automatic port allocation)
/// - Either constraints for validation
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Parameter {
    #[serde(rename = "type", skip_serializing_if = "Option::is_none")]
    pub param_type: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub default: Option<serde_yaml::Value>,

    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub either: Vec<String>,

    // Environment-specific values
    #[serde(skip_serializing_if = "Option::is_none")]
    pub development: Option<serde_yaml::Value>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub develop: Option<serde_yaml::Value>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub staging: Option<serde_yaml::Value>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub production: Option<serde_yaml::Value>,

    /// Secret source — `"manual"` for user-provided secrets, absent for auto-generated.
    /// Extension point for future providers (1password, doppler, etc.).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source: Option<String>,

    /// Human-readable description shown in error messages for missing manual secrets.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,

    /// Whether this parameter is optional (missing value resolves to empty string instead of error).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub optional: Option<bool>,

    /// Shell command to generate the parameter value.
    ///
    /// Command is run via `sh -c`, stdout is captured as the value.
    /// May reference other parameters with `{{PARAM}}` syntax — Fed
    /// resolves these in dependency order (DAG).
    ///
    /// For `type: secret`: value is persisted to the secrets file.
    /// Generated once, not regenerated unless an input dependency changes.
    ///
    /// Without `type: secret`: value is recomputed on every `fed start`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub generate: Option<String>,

    #[serde(skip)]
    pub value: Option<String>,
}

impl Parameter {
    /// Check if this parameter is a port type (for automatic allocation).
    pub fn is_port_type(&self) -> bool {
        self.param_type.as_deref() == Some("port")
    }

    /// Check if this parameter is a secret type (for auto-generation or manual entry).
    pub fn is_secret_type(&self) -> bool {
        self.param_type.as_deref() == Some("secret")
    }

    /// Check if this parameter is a manual secret (requires user-provided value).
    pub fn is_manual_secret(&self) -> bool {
        self.is_secret_type() && self.source.as_deref() == Some("manual")
    }

    /// Check if this parameter is marked as optional.
    pub fn is_optional(&self) -> bool {
        self.optional.unwrap_or(false)
    }

    /// Check if this parameter has a custom generate command.
    pub fn has_generate(&self) -> bool {
        self.generate.is_some()
    }

    /// Extract parameter names referenced in the generate command via `{{PARAM}}`.
    pub fn generate_dependencies(&self) -> Vec<String> {
        match &self.generate {
            Some(cmd) => {
                let re = regex::Regex::new(r"\{\{([^}]+)\}\}").unwrap();
                re.captures_iter(cmd)
                    .map(|cap| cap[1].trim().to_string())
                    .collect()
            }
            None => Vec::new(),
        }
    }

    /// Get the value for a specific environment.
    /// Priority: environment-specific value > default value > None
    pub fn get_value_for_environment(
        &self,
        env: &super::Environment,
    ) -> Option<&serde_yaml::Value> {
        use super::Environment;

        match env {
            Environment::Development => {
                // Try "development" first, then "develop", then fall back to default
                self.development
                    .as_ref()
                    .or(self.develop.as_ref())
                    .or(self.default.as_ref())
            }
            Environment::Staging => {
                // Try "staging", then fall back to default
                self.staging.as_ref().or(self.default.as_ref())
            }
            Environment::Production => {
                // Try "production", then fall back to default
                self.production.as_ref().or(self.default.as_ref())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Environment;

    /// Helper to create a Parameter with only a default value.
    fn param_with_default(val: &str) -> Parameter {
        Parameter {
            param_type: None,
            default: Some(serde_yaml::Value::String(val.to_string())),
            either: vec![],
            development: None,
            develop: None,
            staging: None,
            production: None,
            source: None,
            description: None,
            optional: None,
            ..Default::default()
        }
    }

    /// Helper to create an empty Parameter (no values at all).
    fn param_empty() -> Parameter {
        Parameter {
            param_type: None,
            default: None,
            either: vec![],
            development: None,
            develop: None,
            staging: None,
            production: None,
            source: None,
            description: None,
            optional: None,
            ..Default::default()
        }
    }

    // ========================================================================
    // get_value_for_environment tests
    // ========================================================================

    #[test]
    fn dev_returns_development_value_over_default() {
        let param = Parameter {
            development: Some(serde_yaml::Value::String("dev-val".into())),
            default: Some(serde_yaml::Value::String("default-val".into())),
            ..param_empty()
        };
        let val = param.get_value_for_environment(&Environment::Development);
        assert_eq!(val.unwrap(), &serde_yaml::Value::String("dev-val".into()));
    }

    #[test]
    fn dev_falls_back_to_develop_alias() {
        let param = Parameter {
            develop: Some(serde_yaml::Value::String("develop-val".into())),
            default: Some(serde_yaml::Value::String("default-val".into())),
            ..param_empty()
        };
        let val = param.get_value_for_environment(&Environment::Development);
        assert_eq!(
            val.unwrap(),
            &serde_yaml::Value::String("develop-val".into())
        );
    }

    #[test]
    fn dev_development_takes_priority_over_develop() {
        let param = Parameter {
            development: Some(serde_yaml::Value::String("development-val".into())),
            develop: Some(serde_yaml::Value::String("develop-val".into())),
            ..param_empty()
        };
        let val = param.get_value_for_environment(&Environment::Development);
        assert_eq!(
            val.unwrap(),
            &serde_yaml::Value::String("development-val".into())
        );
    }

    #[test]
    fn dev_falls_back_to_default() {
        let param = param_with_default("fallback");
        let val = param.get_value_for_environment(&Environment::Development);
        assert_eq!(val.unwrap(), &serde_yaml::Value::String("fallback".into()));
    }

    #[test]
    fn dev_returns_none_when_nothing_set() {
        let param = param_empty();
        assert!(param
            .get_value_for_environment(&Environment::Development)
            .is_none());
    }

    #[test]
    fn staging_returns_staging_value() {
        let param = Parameter {
            staging: Some(serde_yaml::Value::String("stage-val".into())),
            default: Some(serde_yaml::Value::String("default-val".into())),
            ..param_empty()
        };
        let val = param.get_value_for_environment(&Environment::Staging);
        assert_eq!(val.unwrap(), &serde_yaml::Value::String("stage-val".into()));
    }

    #[test]
    fn staging_falls_back_to_default() {
        let param = param_with_default("fallback");
        let val = param.get_value_for_environment(&Environment::Staging);
        assert_eq!(val.unwrap(), &serde_yaml::Value::String("fallback".into()));
    }

    #[test]
    fn staging_returns_none_when_nothing_set() {
        let param = param_empty();
        assert!(param
            .get_value_for_environment(&Environment::Staging)
            .is_none());
    }

    #[test]
    fn production_returns_production_value() {
        let param = Parameter {
            production: Some(serde_yaml::Value::String("prod-val".into())),
            default: Some(serde_yaml::Value::String("default-val".into())),
            ..param_empty()
        };
        let val = param.get_value_for_environment(&Environment::Production);
        assert_eq!(val.unwrap(), &serde_yaml::Value::String("prod-val".into()));
    }

    #[test]
    fn production_falls_back_to_default() {
        let param = param_with_default("fallback");
        let val = param.get_value_for_environment(&Environment::Production);
        assert_eq!(val.unwrap(), &serde_yaml::Value::String("fallback".into()));
    }

    #[test]
    fn production_returns_none_when_nothing_set() {
        let param = param_empty();
        assert!(param
            .get_value_for_environment(&Environment::Production)
            .is_none());
    }

    #[test]
    fn staging_ignores_development_and_develop_fields() {
        let param = Parameter {
            development: Some(serde_yaml::Value::String("dev-only".into())),
            develop: Some(serde_yaml::Value::String("develop-only".into())),
            ..param_empty()
        };
        // Staging should not see development/develop values
        assert!(param
            .get_value_for_environment(&Environment::Staging)
            .is_none());
    }

    #[test]
    fn production_ignores_staging_field() {
        let param = Parameter {
            staging: Some(serde_yaml::Value::String("stage-only".into())),
            ..param_empty()
        };
        assert!(param
            .get_value_for_environment(&Environment::Production)
            .is_none());
    }

    #[test]
    fn numeric_value_returned_correctly() {
        let param = Parameter {
            default: Some(serde_yaml::Value::Number(serde_yaml::Number::from(5432))),
            ..param_empty()
        };
        let val = param.get_value_for_environment(&Environment::Development);
        assert_eq!(
            val.unwrap(),
            &serde_yaml::Value::Number(serde_yaml::Number::from(5432))
        );
    }

    // ========================================================================
    // is_port_type tests
    // ========================================================================

    #[test]
    fn is_port_type_true() {
        let param = Parameter {
            param_type: Some("port".to_string()),
            ..param_empty()
        };
        assert!(param.is_port_type());
    }

    #[test]
    fn is_port_type_false_for_other_type() {
        let param = Parameter {
            param_type: Some("string".to_string()),
            ..param_empty()
        };
        assert!(!param.is_port_type());
    }

    #[test]
    fn is_port_type_false_when_none() {
        let param = param_empty();
        assert!(!param.is_port_type());
    }

    // ========================================================================
    // is_secret_type / is_manual_secret tests
    // ========================================================================

    #[test]
    fn is_secret_type_true() {
        let param = Parameter {
            param_type: Some("secret".to_string()),
            ..param_empty()
        };
        assert!(param.is_secret_type());
    }

    #[test]
    fn is_secret_type_false_for_other_type() {
        let param = Parameter {
            param_type: Some("port".to_string()),
            ..param_empty()
        };
        assert!(!param.is_secret_type());
    }

    #[test]
    fn is_secret_type_false_when_none() {
        assert!(!param_empty().is_secret_type());
    }

    #[test]
    fn is_manual_secret_true() {
        let param = Parameter {
            param_type: Some("secret".to_string()),
            source: Some("manual".to_string()),
            ..param_empty()
        };
        assert!(param.is_manual_secret());
    }

    #[test]
    fn is_manual_secret_false_without_source() {
        let param = Parameter {
            param_type: Some("secret".to_string()),
            ..param_empty()
        };
        assert!(!param.is_manual_secret());
    }

    #[test]
    fn is_manual_secret_false_for_non_secret() {
        let param = Parameter {
            param_type: Some("port".to_string()),
            source: Some("manual".to_string()),
            ..param_empty()
        };
        assert!(!param.is_manual_secret());
    }

    #[test]
    fn deserialize_secret_with_source_and_description() {
        let yaml = r#"
type: secret
source: manual
description: "GitHub OAuth client secret"
"#;
        let param: Parameter = serde_yaml::from_str(yaml).unwrap();
        assert!(param.is_secret_type());
        assert!(param.is_manual_secret());
        assert_eq!(
            param.description.as_deref(),
            Some("GitHub OAuth client secret")
        );
    }

    #[test]
    fn deserialize_secret_without_source() {
        let yaml = r#"
type: secret
"#;
        let param: Parameter = serde_yaml::from_str(yaml).unwrap();
        assert!(param.is_secret_type());
        assert!(!param.is_manual_secret());
        assert!(param.source.is_none());
    }

    // ========================================================================
    // is_optional tests
    // ========================================================================

    #[test]
    fn is_optional_true_when_set() {
        let param = Parameter {
            optional: Some(true),
            ..param_empty()
        };
        assert!(param.is_optional());
    }

    #[test]
    fn is_optional_false_when_explicitly_false() {
        let param = Parameter {
            optional: Some(false),
            ..param_empty()
        };
        assert!(!param.is_optional());
    }

    #[test]
    fn is_optional_false_when_none() {
        assert!(!param_empty().is_optional());
    }

    #[test]
    fn deserialize_optional_manual_secret() {
        let yaml = r#"
type: secret
source: manual
optional: true
description: "From https://dashboard.stripe.com/apikeys"
"#;
        let param: Parameter = serde_yaml::from_str(yaml).unwrap();
        assert!(param.is_manual_secret());
        assert!(param.is_optional());
        assert_eq!(
            param.description.as_deref(),
            Some("From https://dashboard.stripe.com/apikeys")
        );
    }
}
