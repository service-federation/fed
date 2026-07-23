//! Parameter sensitivity classification and redacted display views.
//!
//! Sensitivity is a property of *declaration and provenance*, not of a
//! parameter's name: a parameter is sensitive when it is declared
//! `type: secret`, or when its value transitively derives from one (via
//! `default`/`generate` template references — the same taint closure the
//! scoped-run machinery uses). A narrow name heuristic is layered on top as
//! defense-in-depth for undeclared secrets, but it is never the only signal.
//!
//! [`ParameterView`] is the boundary type handed to display surfaces (the
//! TUI): a sensitive row carries [`ParameterValue::Redacted`] and no raw
//! material at all, so a rendering or clipboard path cannot leak what it
//! never received.

use crate::config::Config;
use std::collections::HashSet;

/// Mask shown in place of a sensitive value.
pub const REDACTED_DISPLAY: &str = "********";

/// Compute the set of sensitive parameter names for a config.
///
/// Union of:
/// 1. Parameters declared `type: secret` (any source — manual, generated).
/// 2. Parameters whose value transitively references one (the
///    [`crate::parameter::scanner::parameters_tainted_by`] closure over
///    `default`, `generate`, and every other string field).
/// 3. A narrow name heuristic ([`name_suggests_secret`]) as defense-in-depth
///    for values that are secrets in practice but were never declared.
pub fn sensitive_parameter_names(config: &Config) -> HashSet<String> {
    let params = config.get_effective_parameters();

    let declared: HashSet<String> = params
        .iter()
        .filter(|(_, p)| p.is_secret_type())
        .map(|(name, _)| name.clone())
        .collect();

    let mut sensitive = if declared.is_empty() {
        HashSet::new()
    } else {
        crate::parameter::scanner::parameters_tainted_by(config, &declared)
    };
    sensitive.extend(declared);

    for name in params.keys() {
        if name_suggests_secret(name) {
            sensitive.insert(name.clone());
        }
    }

    sensitive
}

/// Defense-in-depth name heuristic for parameters that were not declared
/// `type: secret` but whose name marks them as one.
///
/// Matches whole `_`/non-alphanumeric-separated tokens, not substrings, so
/// `KEY_COUNT` (a count *of* keys) stays plain while `SSH_KEY` (a key) does
/// not: a trailing `KEY` token means "this value is a key", a leading one
/// merely qualifies the next word.
pub fn name_suggests_secret(name: &str) -> bool {
    const SECRET_TOKENS: &[&str] = &[
        "password",
        "passwd",
        "secret",
        "token",
        "credential",
        "credentials",
        "apikey",
    ];
    let tokens: Vec<String> = name
        .split(|c: char| !c.is_ascii_alphanumeric())
        .filter(|t| !t.is_empty())
        .map(|t| t.to_ascii_lowercase())
        .collect();
    tokens.iter().any(|t| SECRET_TOKENS.contains(&t.as_str()))
        || tokens.last().is_some_and(|t| t == "key")
}

/// A resolved parameter value prepared for display outside the resolution
/// boundary.
///
/// `Redacted` intentionally carries no payload: once a value crosses this
/// boundary as sensitive, the raw material is gone and no display, copy,
/// `Debug`, or error path can reproduce it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ParameterValue {
    /// Non-sensitive value, shown and copied as-is.
    Plain(String),
    /// Sensitive value; the raw material was dropped at the boundary.
    Redacted,
}

impl ParameterValue {
    /// The string to render for this value.
    pub fn display(&self) -> &str {
        match self {
            ParameterValue::Plain(v) => v,
            ParameterValue::Redacted => REDACTED_DISPLAY,
        }
    }

    /// The clipboard payload for this value, if copying is allowed.
    /// Sensitive values return `None` — there is nothing to copy.
    pub fn clipboard_payload(&self) -> Option<&str> {
        match self {
            ParameterValue::Plain(v) => Some(v),
            ParameterValue::Redacted => None,
        }
    }

    pub fn is_sensitive(&self) -> bool {
        matches!(self, ParameterValue::Redacted)
    }
}

/// A parameter row for display surfaces: name plus a plain-or-redacted value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParameterView {
    pub name: String,
    pub value: ParameterValue,
}

impl ParameterView {
    /// Build a view, dropping the raw value when the name is in `sensitive`.
    pub fn new(name: &str, raw_value: &str, sensitive: &HashSet<String>) -> Self {
        let value = if sensitive.contains(name) {
            ParameterValue::Redacted
        } else {
            ParameterValue::Plain(raw_value.to_string())
        };
        Self {
            name: name.to_string(),
            value,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Parameter;

    fn secret_param(source: Option<&str>) -> Parameter {
        Parameter {
            param_type: Some("secret".to_string()),
            source: source.map(|s| s.to_string()),
            ..Default::default()
        }
    }

    fn plain_param(default: &str) -> Parameter {
        Parameter {
            default: Some(serde_yaml::Value::String(default.to_string())),
            ..Default::default()
        }
    }

    #[test]
    fn manual_secret_with_innocuous_name_is_sensitive() {
        // No secret-like substring in any of these names.
        for name in ["LICENSE", "CREDENTIAL_X", "VALUE"] {
            let mut config = Config::default();
            config
                .parameters
                .insert(name.to_string(), secret_param(Some("manual")));
            let sensitive = sensitive_parameter_names(&config);
            assert!(sensitive.contains(name), "{name} must be sensitive");
        }
    }

    #[test]
    fn generated_secret_is_sensitive() {
        let mut config = Config::default();
        config
            .parameters
            .insert("LICENSE".to_string(), secret_param(None));
        assert!(sensitive_parameter_names(&config).contains("LICENSE"));
    }

    #[test]
    fn directly_derived_parameter_is_sensitive() {
        let mut config = Config::default();
        config
            .parameters
            .insert("LICENSE".to_string(), secret_param(Some("manual")));
        config.parameters.insert(
            "CONNECTION".to_string(),
            plain_param("postgres://user:{{LICENSE}}@localhost"),
        );
        let sensitive = sensitive_parameter_names(&config);
        assert!(sensitive.contains("CONNECTION"));
    }

    #[test]
    fn transitively_derived_parameter_is_sensitive() {
        let mut config = Config::default();
        config
            .parameters
            .insert("LICENSE".to_string(), secret_param(Some("manual")));
        config
            .parameters
            .insert("MIDDLE".to_string(), plain_param("{{LICENSE}}-suffix"));
        config
            .parameters
            .insert("OUTER".to_string(), plain_param("prefix-{{MIDDLE}}"));
        let sensitive = sensitive_parameter_names(&config);
        assert!(sensitive.contains("MIDDLE"));
        assert!(sensitive.contains("OUTER"));
    }

    #[test]
    fn generate_command_referencing_secret_is_sensitive() {
        let mut config = Config::default();
        config
            .parameters
            .insert("LICENSE".to_string(), secret_param(Some("manual")));
        config.parameters.insert(
            "DERIVED".to_string(),
            Parameter {
                generate: Some("echo {{LICENSE}} | sha256sum".to_string()),
                ..Default::default()
            },
        );
        assert!(sensitive_parameter_names(&config).contains("DERIVED"));
    }

    #[test]
    fn ordinary_parameter_with_key_count_name_is_not_sensitive() {
        let mut config = Config::default();
        config
            .parameters
            .insert("KEY_COUNT".to_string(), plain_param("7"));
        config
            .parameters
            .insert("LICENSE".to_string(), secret_param(Some("manual")));
        let sensitive = sensitive_parameter_names(&config);
        assert!(!sensitive.contains("KEY_COUNT"));
    }

    #[test]
    fn name_heuristic_token_boundaries() {
        // Whole tokens and trailing KEY match…
        assert!(name_suggests_secret("DB_PASSWORD"));
        assert!(name_suggests_secret("session_token"));
        assert!(name_suggests_secret("SSH_KEY"));
        assert!(name_suggests_secret("API_KEY"));
        // …but a leading KEY token or an unrelated name does not.
        assert!(!name_suggests_secret("KEY_COUNT"));
        assert!(!name_suggests_secret("KEYBOARD_LAYOUT"));
        assert!(!name_suggests_secret("MONKEY_NAME"));
        assert!(!name_suggests_secret("LICENSE"));
    }

    #[test]
    fn redacted_view_retains_no_raw_material() {
        let sentinel = "sentinel-secret-9f2c";
        let mut sensitive = HashSet::new();
        sensitive.insert("LICENSE".to_string());

        let view = ParameterView::new("LICENSE", sentinel, &sensitive);
        assert_eq!(view.value, ParameterValue::Redacted);
        assert_eq!(view.value.display(), REDACTED_DISPLAY);
        assert_eq!(view.value.clipboard_payload(), None);
        // Debug output cannot contain what the value no longer holds.
        assert!(!format!("{:?}", view).contains(sentinel));
    }

    #[test]
    fn plain_view_displays_and_copies() {
        let view = ParameterView::new("KEY_COUNT", "7", &HashSet::new());
        assert!(!view.value.is_sensitive());
        assert_eq!(view.value.display(), "7");
        assert_eq!(view.value.clipboard_payload(), Some("7"));
    }
}
