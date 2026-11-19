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
    quote_shell_argument(s)
}

fn quote_shell_argument(value: &str) -> String {
    format!("'{}'", value.replace('\'', r"'\''"))
}

#[derive(Clone, Copy, Default, PartialEq)]
enum ShellQuote {
    #[default]
    Unquoted,
    Single,
    Double,
    Comment,
}

#[derive(Default)]
struct ShellTemplateContext {
    quote: ShellQuote,
    escaped: bool,
    in_word: bool,
    trailing_dollar: bool,
    unbraced_variable: bool,
}

fn unsupported_shell_template(context: &str) -> Error {
    Error::TemplateResolution(format!(
        "Cannot safely substitute parameters in {context}. Pass values through service environment variables and validate them before using them as code or expressions."
    ))
}

/// A placeholder is data only when Bash parses it as a command argument.
/// Arithmetic, assignments, conditionals and loop bindings can evaluate even
/// quoted strings again. Parse the original syntax with inert identifiers so
/// values never influence this decision or become part of the syntax tree.
fn validate_shell_argument_positions(template: &str) -> Result<()> {
    let mut source = String::with_capacity(template.len());
    let mut positions = Vec::new();
    let mut end = 0;
    for span in get_template_regex().find_iter(template) {
        source.push_str(&template[end..span.start()]);
        let start = source.len();
        source.push_str("__fed_parameter__");
        positions.push(start..source.len());
        end = span.end();
    }
    source.push_str(&template[end..]);

    let mut parser = tree_sitter::Parser::new();
    parser
        .set_language(&tree_sitter_bash::LANGUAGE.into())
        .map_err(|_| unsupported_shell_template("unrecognized shell grammar"))?;
    let tree = parser
        .parse(&source, None)
        .ok_or_else(|| unsupported_shell_template("unrecognized shell grammar"))?;
    let root = tree.root_node();
    if root.has_error() {
        return Err(unsupported_shell_template(
            "unsupported or invalid shell syntax",
        ));
    }

    for position in positions {
        let mut node = root
            .named_descendant_for_byte_range(position.start, position.end)
            .ok_or_else(|| unsupported_shell_template("unrecognized parameter position"))?;
        loop {
            match node.kind() {
                "word" | "string_content" | "string" | "raw_string" | "concatenation" => {}
                "command" => {
                    let name = node
                        .child_by_field_name("name")
                        .and_then(|name| name.utf8_text(source.as_bytes()).ok())
                        .and_then(|name| shell_words::split(name).ok())
                        .and_then(|names| names.into_iter().next());
                    if name.as_deref().is_some_and(|name| {
                        // The grammar does not model Bash's time/coproc
                        // prefixes: their following word may be the real
                        // command name rather than an ordinary argument.
                        matches!(
                            name,
                            "let"
                                | "eval"
                                | "source"
                                | "."
                                | "builtin"
                                | "command"
                                | "time"
                                | "coproc"
                        )
                    }) {
                        return Err(unsupported_shell_template(
                            "shell evaluation commands or unsupported command prefixes",
                        ));
                    }
                    // Quoting a declaration builtin's name makes it an ordinary
                    // command node, but does not change its assignment semantics.
                    // Classify its normalized spelling with the same grammar so
                    // its declaration/unset keywords remain the source of truth.
                    if let Some(name) = name
                        && name.chars().all(|c| c.is_ascii_alphabetic() || c == '_')
                    {
                        let canonical = format!("{name} __fed_parameter__");
                        let canonical_tree = parser.parse(&canonical, None).ok_or_else(|| {
                            unsupported_shell_template("unrecognized shell grammar")
                        })?;
                        if canonical_tree
                            .root_node()
                            .named_child(0)
                            .is_some_and(|node| {
                                matches!(node.kind(), "declaration_command" | "unset_command")
                            })
                        {
                            return Err(unsupported_shell_template("shell declaration builtins"));
                        }
                    }
                    break;
                }
                _ => {
                    return Err(unsupported_shell_template(
                        "parameters outside ordinary command arguments",
                    ));
                }
            }
            node = node
                .parent()
                .ok_or_else(|| unsupported_shell_template("unrecognized parameter position"))?;
        }
    }
    Ok(())
}

impl ShellTemplateContext {
    /// Scan only original command text, never interpolated values. This covers
    /// ordinary shell words and quoted arguments; nested expansion grammars are
    /// deliberately rejected rather than guessing at their quoting rules.
    fn scan(&mut self, text: &str) -> Result<()> {
        let mut chars = text.chars().peekable();
        while let Some(c) = chars.next() {
            self.unbraced_variable = self.quote != ShellQuote::Single
                && self.quote != ShellQuote::Comment
                && !self.escaped
                && if self.unbraced_variable {
                    c.is_ascii_alphanumeric() || c == '_'
                } else {
                    self.trailing_dollar && (c.is_ascii_alphabetic() || c == '_')
                };
            self.trailing_dollar = self.quote != ShellQuote::Single
                && self.quote != ShellQuote::Comment
                && !self.escaped
                && c == '$';
            if self.quote == ShellQuote::Comment {
                if c == '\n' {
                    self.quote = ShellQuote::Unquoted;
                    self.in_word = false;
                }
                continue;
            }
            if self.escaped {
                self.escaped = false;
                if c != '\n' {
                    self.in_word = true;
                }
                continue;
            }
            match (self.quote, c) {
                (ShellQuote::Single, '\'') => self.quote = ShellQuote::Unquoted,
                (ShellQuote::Single, _) => {}
                (_, '\\') => self.escaped = true,
                (ShellQuote::Unquoted, '\'') => {
                    self.quote = ShellQuote::Single;
                    self.in_word = true;
                }
                (ShellQuote::Unquoted, '"') => {
                    self.quote = ShellQuote::Double;
                    self.in_word = true;
                }
                (ShellQuote::Double, '"') => self.quote = ShellQuote::Unquoted,
                (_, '`') => {
                    return Err(unsupported_shell_template("backtick command substitutions"));
                }
                (_, '$') if matches!(chars.peek(), Some('(' | '{')) => {
                    return Err(unsupported_shell_template("nested shell expansions"));
                }
                (ShellQuote::Unquoted, '$') if matches!(chars.peek(), Some('\'' | '"')) => {
                    return Err(unsupported_shell_template("dollar-quoted shell strings"));
                }
                (ShellQuote::Unquoted, '<') if chars.peek() == Some(&'<') => {
                    return Err(unsupported_shell_template(
                        "shell here-documents or here-strings",
                    ));
                }
                (ShellQuote::Unquoted, '(') if chars.peek() == Some(&'(') => {
                    return Err(unsupported_shell_template("shell arithmetic commands"));
                }
                (ShellQuote::Unquoted, '#') if !self.in_word => self.quote = ShellQuote::Comment,
                (ShellQuote::Unquoted, c) => {
                    self.in_word = !(c.is_whitespace() || ";|&()<>".contains(c));
                }
                _ => {}
            }
        }
        Ok(())
    }

    fn escape_value(&mut self, value: &str) -> Result<String> {
        if self.trailing_dollar || self.unbraced_variable {
            return Err(unsupported_shell_template(
                "placeholders immediately following a dollar sign or an unbraced variable name",
            ));
        }
        if self.escaped || self.quote == ShellQuote::Comment {
            return Err(unsupported_shell_template(
                "escaped placeholders or shell comments",
            ));
        }
        if value.contains('\0') {
            return Err(unsupported_shell_template("values containing a NUL byte"));
        }
        self.in_word = true;
        Ok(match self.quote {
            // Even alphanumeric data must be quoted here: adjacent original
            // syntax such as `~` must not start expanding after substitution.
            ShellQuote::Unquoted => quote_shell_argument(value),
            ShellQuote::Single => value.replace('\'', r"'\''"),
            ShellQuote::Double => value
                .replace('\\', "\\\\")
                .replace('"', "\\\"")
                .replace('$', "\\$")
                .replace('`', "\\`"),
            ShellQuote::Comment => unreachable!(),
        })
    }
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
    /// Static version for data fields outside the resolver. Shell commands must
    /// use `resolve_template_shell_safe_static` instead.
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

        if escape {
            return Self::replace_shell_placeholders(template, parameters);
        }

        let mut missing: Option<String> = None;
        let result = get_template_regex().replace_all(template, |cap: &regex::Captures| {
            // Trim so `{{ FOO }}` resolves like `{{FOO}}`, matching how
            // generate_dependencies extracts names for DAG ordering.
            let var_name = cap[1].trim();
            match parameters.get(var_name) {
                Some(value) => value.clone(),
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

    fn replace_shell_placeholders(
        template: &str,
        parameters: &HashMap<String, String>,
    ) -> Result<String> {
        let regex = get_template_regex();
        if !regex.is_match(template) {
            return Ok(template.to_string());
        }
        validate_shell_argument_positions(template)?;
        let mut context = ShellTemplateContext::default();
        let mut result = String::with_capacity(template.len());
        let mut end = 0;
        for capture in regex.captures_iter(template) {
            let span = capture.get(0).expect("whole regex match");
            let preceding = &template[end..span.start()];
            context.scan(preceding)?;
            result.push_str(preceding);
            let name = capture[1].trim();
            let value = parameters
                .get(name)
                .ok_or_else(|| Error::ParameterNotFound(name.to_string()))?;
            result.push_str(&context.escape_value(value)?);
            end = span.end();
        }
        context.scan(&template[end..])?;
        if context.escaped || matches!(context.quote, ShellQuote::Single | ShellQuote::Double) {
            return Err(unsupported_shell_template(
                "commands with unfinished quotes or escapes",
            ));
        }
        result.push_str(&template[end..]);
        Ok(result)
    }

    /// Resolve placeholders as literal shell arguments, respecting unquoted,
    /// single-quoted and double-quoted contexts. Commands with placeholders and
    /// nested shell expansions / here-documents fail closed; use environment
    /// variables to pass data into those commands. Static commands are unchanged.
    pub fn resolve_template_shell_safe(
        &self,
        template: &str,
        parameters: &HashMap<String, String>,
    ) -> Result<String> {
        Self::resolve_template_shell_safe_static(template, parameters)
    }

    /// Pure shell-template substitution for callers without a resolver instance.
    pub fn resolve_template_shell_safe_static(
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
    fn shell_templates_preserve_literal_values_in_each_quote_context() {
        let dir = tempfile::tempdir().unwrap();
        let marker = dir.path().join("injected");
        let payloads = [
            format!("$(printf injected > {})", marker.display()),
            format!("`printf injected > {}`", marker.display()),
            format!("'; printf injected > {}; #", marker.display()),
            String::new(),
            "spaces and 'single' and \"double\" quotes; $HOME \\\nline two".to_string(),
        ];
        for payload in payloads {
            let parameters = HashMap::from([("VALUE".to_string(), payload.clone())]);
            for template in [
                "printf '%s' {{VALUE}}",
                "printf '%s' '{{VALUE}}'",
                "printf '%s' \"{{VALUE}}\"",
                "printf '%s' 'prefix{{VALUE}}suffix'",
                "printf '%s' \"prefix{{VALUE}}suffix\"",
            ] {
                let command = Resolver::new()
                    .resolve_template_shell_safe(template, &parameters)
                    .unwrap();
                let output = std::process::Command::new("bash")
                    .args(["-c", &command])
                    .output()
                    .unwrap();
                assert!(output.status.success(), "template {template}");
                assert!(
                    !marker.exists(),
                    "executed parameter contents in {template}"
                );
                let expected = if template.contains("prefix") {
                    format!("prefix{payload}suffix")
                } else {
                    payload.clone()
                };
                assert_eq!(
                    String::from_utf8(output.stdout).unwrap(),
                    expected,
                    "template {template}"
                );
            }
        }
    }

    #[test]
    fn shell_templates_reject_unsupported_contexts_without_exposing_values() {
        let parameters = HashMap::from([("VALUE".to_string(), "private-value".to_string())]);
        for template in [
            "printf '%s' \"$(printf '%s' {{VALUE}})\"",
            "printf '%s' `printf '%s' {{VALUE}}`",
            "cat <<EOF\n{{VALUE}}\nEOF",
            "cat <<'EOF'\n{{VALUE}}\nEOF",
            "cat <<< {{VALUE}}",
            "printf '%s' $'{{VALUE}}'",
            "printf '%s' ${NAME:-{{VALUE}}}",
            "echo ${{VALUE}}",
            "echo \\{{VALUE}}",
            "# {{VALUE}}\necho harmless",
            "echo \"{{VALUE}}",
            "(( {{VALUE}} + 1 ))",
        ] {
            let error = Resolver::new()
                .resolve_template_shell_safe(template, &parameters)
                .unwrap_err();
            assert!(
                error.to_string().contains("environment variables"),
                "{template}: {error}"
            );
            assert!(!error.to_string().contains("private-value"));
        }
    }

    #[test]
    fn shell_templates_reject_bash_evaluation_positions_before_execution() {
        let parameters = HashMap::from([("VALUE".to_string(), "ordinary-value".to_string())]);
        for template in [
            "printf '%s' $[{{VALUE}}]",
            "[[ {{VALUE}} -eq 0 ]]",
            "[[ 0 -eq {{VALUE}} ]]",
            "[[ -v {{VALUE}} ]]",
            "arr[{{VALUE}}]=1",
            "declare -a arr; arr[{{VALUE}}]=1",
            "declare -i value={{VALUE}}",
            "RANDOM={{VALUE}}",
            "for RANDOM in {{VALUE}}; do :; done",
            "let {{VALUE}}",
            "eval {{VALUE}}",
            "builtin let {{VALUE}}",
            "time eval {{VALUE}}",
            "time {{VALUE}}",
            "time -p printf '%s' {{VALUE}}",
            "coproc eval {{VALUE}}",
            "coproc worker eval {{VALUE}}",
            "coproc {{VALUE}}",
            "{{VALUE}} argument",
        ] {
            let error = Resolver::new()
                .resolve_template_shell_safe(template, &parameters)
                .expect_err(template);
            assert!(
                error.to_string().contains("environment variables"),
                "{template}: {error}"
            );
            assert!(!error.to_string().contains("ordinary-value"));
        }
    }

    #[test]
    fn shell_templates_keep_assignment_shaped_arguments_as_literal_data() {
        let parameters = HashMap::from([("VALUE".to_string(), "some value".to_string())]);
        for template in [
            "printf '%s' --port={{VALUE}}",
            "printf '%s' label={{VALUE}}",
            "printf '%s' 'label={{VALUE}}'",
        ] {
            let command = Resolver::new()
                .resolve_template_shell_safe(template, &parameters)
                .unwrap();
            let output = std::process::Command::new("bash")
                .args(["-c", &command])
                .output()
                .unwrap();
            assert!(output.status.success());
            let expected = if template.contains("--port") {
                "--port=some value"
            } else {
                "label=some value"
            };
            assert_eq!(String::from_utf8(output.stdout).unwrap(), expected);
        }
    }

    #[test]
    fn shell_templates_reject_quoted_or_escaped_declaration_names() {
        let parameters = HashMap::from([("VALUE".to_string(), "ordinary-value".to_string())]);
        for name in [
            "declare", "typeset", "export", "readonly", "local", "unset", "unsetenv",
        ] {
            for command in [
                format!("'{name}'"),
                format!("\"{name}\""),
                format!("\\{name}"),
            ] {
                let template = format!("{command} {{{{VALUE}}}}");
                assert!(
                    Resolver::new()
                        .resolve_template_shell_safe(&template, &parameters)
                        .is_err(),
                    "{template}"
                );
            }
        }
    }

    #[test]
    fn shell_templates_keep_tilde_adjacent_arguments_literal() {
        for value in ["", "root", "fed-user", "some value"] {
            let parameters = HashMap::from([("VALUE".to_string(), value.to_string())]);
            for template in ["printf '%s' ~{{VALUE}}", "printf '%s' ~{{VALUE}}/path"] {
                let command = Resolver::new()
                    .resolve_template_shell_safe(template, &parameters)
                    .unwrap();
                let output = std::process::Command::new("bash")
                    .args(["-c", &command])
                    .output()
                    .unwrap();
                assert!(output.status.success());
                let suffix = if template.ends_with("/path") {
                    "/path"
                } else {
                    ""
                };
                assert_eq!(
                    String::from_utf8(output.stdout).unwrap(),
                    format!("~{value}{suffix}")
                );
            }
        }
    }

    #[test]
    fn shell_templates_leave_static_commands_and_literal_syntax_unchanged() {
        let resolver = Resolver::new();
        let static_command = "printf '%s' \"$(pwd)\"; cat <<EOF\ntext\nEOF";
        assert_eq!(
            resolver
                .resolve_template_shell_safe(static_command, &HashMap::new())
                .unwrap(),
            static_command
        );
        let parameters = HashMap::from([("VALUE".to_string(), "literal".to_string())]);
        let command = resolver
            .resolve_template_shell_safe(
                "printf '%s' '$(not-a-command) {{VALUE}}' # ignored \"$(",
                &parameters,
            )
            .unwrap();
        let output = std::process::Command::new("bash")
            .args(["-c", &command])
            .output()
            .unwrap();
        assert!(output.status.success());
        assert_eq!(
            String::from_utf8(output.stdout).unwrap(),
            "$(not-a-command) literal"
        );
    }

    #[test]
    fn shell_templates_never_extend_unbraced_variable_names() {
        let resolver = Resolver::new();
        let parameters = HashMap::from([("SUFFIX".to_string(), "D_SECRET".to_string())]);
        for template in ["printf '%s' $FE{{SUFFIX}}", "printf '%s' \"$FE{{SUFFIX}}\""] {
            assert!(
                resolver
                    .resolve_template_shell_safe(template, &parameters)
                    .is_err(),
                "{template}"
            );
        }
        for (template, expected) in [
            ("printf '%s' \"$FE\"{{SUFFIX}}", "prefixD_SECRET"),
            ("printf '%s' '$FE{{SUFFIX}}'", "$FED_SECRET"),
            ("printf '%s' \"\\$FE{{SUFFIX}}\"", "$FED_SECRET"),
        ] {
            let command = resolver
                .resolve_template_shell_safe(template, &parameters)
                .unwrap();
            let output = std::process::Command::new("bash")
                .env("FE", "prefix")
                .env("FED_SECRET", "must-not-be-disclosed")
                .args(["-c", &command])
                .output()
                .unwrap();
            assert!(output.status.success());
            assert_eq!(String::from_utf8(output.stdout).unwrap(), expected);
        }
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
