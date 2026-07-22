// Split from resolver.rs (see git history before this commit for pre-split blame).
use super::*;

/// Global template regex compiled once
static TEMPLATE_REGEX: OnceLock<Regex> = OnceLock::new();

pub(crate) fn get_template_regex() -> &'static Regex {
    TEMPLATE_REGEX
        .get_or_init(|| Regex::new(r"\{\{([^}]+)\}\}").expect("static regex pattern is valid"))
}

/// Escape a string for safe use in shell commands.
/// Wraps the string in single quotes and escapes any single quotes within.
pub(crate) fn shell_escape(s: &str) -> String {
    // If string is empty, return empty quoted string
    if s.is_empty() {
        return "''".to_string();
    }

    // If string contains no special characters, return as-is
    // Safe characters: alphanumeric, dash, underscore, dot only
    // Note: '/' and ':' are intentionally NOT in the safe list as they can be
    // exploited in path traversal or certain shell constructs
    if s.chars()
        .all(|c| c.is_alphanumeric() || c == '-' || c == '_' || c == '.')
    {
        return s.to_string();
    }

    // Wrap in single quotes and escape any single quotes by replacing ' with '\''
    format!("'{}'", s.replace('\'', r"'\''"))
}

impl Resolver {
    /// Resolve template placeholders {{VAR}} with their values
    pub fn resolve_template(
        &self,
        template: &str,
        parameters: &HashMap<String, String>,
    ) -> Result<String> {
        Self::resolve_template_static(template, parameters)
    }

    /// Resolve `{{PARAM}}` placeholders in a template string.
    /// Static version for use outside the resolver (e.g., generate commands).
    pub fn resolve_template_static(
        template: &str,
        parameters: &HashMap<String, String>,
    ) -> Result<String> {
        Self::replace_placeholders(template, parameters, false)
    }

    /// Substitute `{{PARAM}}` placeholders in a single pass over the template.
    /// Substituted values are never re-scanned, so a value that itself contains
    /// `{{...}}` is inserted literally instead of being expanded (which would
    /// let one parameter's value smuggle in another's — including past shell
    /// escaping in the shell-safe variant).
    fn replace_placeholders(
        template: &str,
        parameters: &HashMap<String, String>,
        escape: bool,
    ) -> Result<String> {
        if template.is_empty() {
            return Ok(String::new());
        }

        let mut missing: Option<String> = None;
        let result = get_template_regex().replace_all(template, |cap: &regex::Captures| {
            // Trim so `{{ FOO }}` resolves like `{{FOO}}`, matching how
            // generate_dependencies extracts names for DAG ordering.
            let var_name = cap[1].trim();
            match parameters.get(var_name) {
                Some(value) => {
                    if escape {
                        shell_escape(value)
                    } else {
                        value.clone()
                    }
                }
                None => {
                    missing.get_or_insert_with(|| var_name.to_string());
                    String::new()
                }
            }
        });

        if let Some(name) = missing {
            return Err(Error::ParameterNotFound(name));
        }

        Ok(result.into_owned())
    }

    /// Resolve template placeholders with shell escaping for safe use in shell commands.
    /// Public for use in script execution at runtime.
    pub fn resolve_template_shell_safe(
        &self,
        template: &str,
        parameters: &HashMap<String, String>,
    ) -> Result<String> {
        Self::replace_placeholders(template, parameters, true)
    }

    /// Convert YAML value to string
    pub(super) fn value_to_string(value: &serde_yaml::Value) -> String {
        match value {
            serde_yaml::Value::String(s) => s.clone(),
            serde_yaml::Value::Number(n) => n.to_string(),
            serde_yaml::Value::Bool(b) => b.to_string(),
            _ => format!("{:?}", value),
        }
    }

    /// Extract template variables from a string
    pub fn extract_template_variables(&self, template: &str) -> Vec<String> {
        get_template_regex()
            .captures_iter(template)
            .filter_map(|cap| cap.get(1).map(|m| m.as_str().to_string()))
            .collect()
    }
}

#[cfg(test)]
#[allow(clippy::field_reassign_with_default)]
mod tests {
    use super::*;

    #[test]
    fn test_resolve_template() {
        let resolver = Resolver::new();
        let mut params = HashMap::new();
        params.insert("PORT".to_string(), "8080".to_string());
        params.insert("HOST".to_string(), "localhost".to_string());

        let result = resolver
            .resolve_template("http://{{HOST}}:{{PORT}}/api", &params)
            .unwrap();

        assert_eq!(result, "http://localhost:8080/api");
    }

    #[test]
    fn test_resolve_template_missing_param() {
        let resolver = Resolver::new();
        let params = HashMap::new();

        let result = resolver.resolve_template("{{MISSING}}", &params);

        assert!(matches!(result, Err(Error::ParameterNotFound(_))));
    }

    #[test]
    fn test_extract_template_variables() {
        let resolver = Resolver::new();
        let vars = resolver.extract_template_variables("{{FOO}} and {{BAR}} and {{FOO}}");

        assert!(vars.contains(&"FOO".to_string()));
        assert!(vars.contains(&"BAR".to_string()));
    }

    #[test]
    fn test_shell_escape_simple() {
        let result = shell_escape("hello");
        assert_eq!(result, "hello");
    }

    #[test]
    fn test_shell_escape_with_semicolon() {
        let result = shell_escape("; rm -rf /");
        assert_eq!(result, "'; rm -rf /'");
    }

    #[test]
    fn test_shell_escape_with_pipe() {
        let result = shell_escape("foo | bar");
        assert_eq!(result, "'foo | bar'");
    }

    #[test]
    fn test_shell_escape_with_quotes() {
        let result = shell_escape("it's");
        // Single quote ' is escaped as '\'' in the middle of the string
        // "it's" becomes 'it'\''s'
        assert_eq!(result, "'it'\\''s'");
    }

    #[test]
    fn test_shell_escape_empty() {
        let result = shell_escape("");
        assert_eq!(result, "''");
    }

    #[test]
    fn test_shell_escape_safe_characters() {
        let result = shell_escape("hello_world-123.txt");
        assert_eq!(result, "hello_world-123.txt");
    }

    #[test]
    fn test_shell_escape_path_with_slash() {
        // '/' should be quoted now (security hardening)
        let result = shell_escape("/path/to/file");
        assert_eq!(result, "'/path/to/file'");
    }

    #[test]
    fn test_shell_escape_with_colon() {
        // ':' should be quoted now (security hardening)
        let result = shell_escape("host:port");
        assert_eq!(result, "'host:port'");
    }

    #[test]
    fn test_resolve_template_shell_safe() {
        use crate::config::{Config, Parameter, Service};

        let mut resolver = Resolver::new();
        let mut config = Config::default();

        // Create parameter with dangerous value
        let mut param = Parameter {
            param_type: None,
            default: None,
            either: vec![],
            source: None,
            description: None,
            optional: None,
            ..Default::default()
        };
        param.value = Some("; rm -rf /".to_string());

        config.parameters.insert("USER_INPUT".to_string(), param);

        // Create service with process command that uses the parameter
        let service = Service {
            process: Some("echo {{USER_INPUT}}".to_string()),
            ..Default::default()
        };

        config.services.insert("test".to_string(), service);

        resolver.resolve_parameters(&mut config).unwrap();
        let resolved_config = resolver.resolve_config(&config).unwrap();

        // The dangerous parameter should be escaped
        let resolved_service = resolved_config.services.get("test").unwrap();
        let process = resolved_service.process.as_ref().unwrap();

        // Should be escaped to prevent command injection
        assert_eq!(process, "echo '; rm -rf /'");
        // Should NOT be the unescaped dangerous version
        assert_ne!(process, "echo ; rm -rf /");
    }

    #[test]
    fn test_resolve_template_no_double_expansion() {
        // A value that literally contains another placeholder must be inserted
        // verbatim, not re-expanded — re-expansion after escaping would break
        // out of the quoting and defeat shell_escape entirely.
        let mut params = HashMap::new();
        params.insert("A".to_string(), "{{B}}".to_string());
        params.insert("B".to_string(), "x; rm -rf ~".to_string());

        let shell = Resolver::replace_placeholders("run {{A}} {{B}}", &params, true).unwrap();
        assert_eq!(shell, "run '{{B}}' 'x; rm -rf ~'");

        let plain = Resolver::resolve_template_static("run {{A}} {{B}}", &params).unwrap();
        assert_eq!(plain, "run {{B}} x; rm -rf ~");
    }

    #[test]
    fn test_resolve_template_trims_placeholder_names() {
        // generate_dependencies trims captured names; resolution must agree so
        // `{{ FOO }}` doesn't pass DAG validation and then fail at runtime.
        let mut params = HashMap::new();
        params.insert("FOO".to_string(), "bar".to_string());

        let result = Resolver::resolve_template_static("v={{ FOO }}", &params).unwrap();
        assert_eq!(result, "v=bar");
    }

    #[test]
    fn test_resolve_template_missing_parameter_errors() {
        let params = HashMap::new();
        let err = Resolver::resolve_template_static("{{MISSING}}", &params).unwrap_err();
        assert!(matches!(err, Error::ParameterNotFound(name) if name == "MISSING"));
    }
}
