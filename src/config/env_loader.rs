//! Environment file (.env) loader for service-federation.
//!
//! Supports loading environment variables from .env files with:
//! - Standard KEY=VALUE format
//! - Comments with #
//! - Quoted values (single and double quotes)
//! - Multiple files with merge semantics (later files override earlier)
//! - Security validation of environment variable names and values

use crate::error::{Error, Result};
use std::collections::HashMap;
use std::path::Path;

/// Load environment variables from a single .env file.
///
/// Uses dotenvy for parsing which handles:
/// - KEY=VALUE format
/// - Comments starting with #
/// - Single and double quoted values
/// - Empty lines
///
/// After loading, validates all variable names and checks values for
/// suspicious patterns (logs warnings but doesn't fail on suspicious values).
pub fn load_env_file<P: AsRef<Path>>(path: P) -> Result<HashMap<String, String>> {
    let path = path.as_ref();

    if !path.exists() {
        return Err(Error::Filesystem(format!(
            "Environment file not found: {}",
            path.display()
        )));
    }

    let mut env_vars = HashMap::new();
    load_env_file_inner(path, &mut env_vars)?;
    Ok(env_vars)
}

/// Load environment variables from a single .env file, treating a missing file
/// as `Ok(None)` rather than an error.
///
/// Parse errors, permission errors, and other I/O failures still surface as
/// `Err`. Use this when callers want to warn-and-continue on missing files
/// without swallowing real failures.
pub fn load_env_file_optional<P: AsRef<Path>>(path: P) -> Result<Option<HashMap<String, String>>> {
    let path = path.as_ref();
    if !path.exists() {
        return Ok(None);
    }
    let mut env_vars = HashMap::new();
    load_env_file_inner(path, &mut env_vars)?;
    Ok(Some(env_vars))
}

fn load_env_file_inner(path: &Path, env_vars: &mut HashMap<String, String>) -> Result<()> {
    match dotenvy::from_path_iter(path) {
        Ok(iter) => {
            for item in iter {
                match item {
                    Ok((key, value)) => {
                        env_vars.insert(key, value);
                    }
                    Err(e) => {
                        return Err(Error::Config(format!(
                            "Failed to parse environment file {}: {}",
                            path.display(),
                            e
                        )));
                    }
                }
            }
        }
        Err(e) => {
            return Err(Error::Filesystem(format!(
                "Failed to read environment file {}: {}",
                path.display(),
                e
            )));
        }
    }

    validate_and_sanitize_env(env_vars)?;

    Ok(())
}

/// Load and merge multiple .env files.
///
/// Files are loaded in order, with later files overriding values from earlier ones.
/// Paths are resolved relative to the provided base directory.
pub fn load_env_files<P: AsRef<Path>>(
    paths: &[String],
    base_dir: P,
) -> Result<HashMap<String, String>> {
    let base_dir = base_dir.as_ref();
    let mut merged = HashMap::new();

    for path_str in paths {
        let path = base_dir.join(path_str);
        let env_vars = load_env_file(&path)?;
        merged.extend(env_vars);
    }

    Ok(merged)
}

/// Merge environment variables with priority.
///
/// Priority order (highest to lowest):
/// 1. inline - explicit environment values from config
/// 2. from_files - values loaded from .env files
/// 3. base - base/inherited environment values
pub fn merge_environment(
    base: HashMap<String, String>,
    from_files: HashMap<String, String>,
    inline: HashMap<String, String>,
) -> HashMap<String, String> {
    let mut merged = base;
    merged.extend(from_files);
    merged.extend(inline); // inline wins
    merged
}

/// Validate an environment variable name.
///
/// Environment variable names must follow POSIX conventions:
/// - Start with a letter or underscore
/// - Contain only alphanumeric characters and underscores
/// - Typically uppercase by convention (though not enforced)
///
/// This prevents injection of invalid variable names that could cause
/// undefined behavior or security issues.
///
/// # Security Note
/// Environment variables are NOT shell-expanded by service-federation.
/// They are passed directly to child processes via `Command::env()`.
/// This means patterns like `$(command)` or backticks are treated as
/// literal strings, not executed as shell commands.
pub fn validate_env_name(name: &str) -> Result<()> {
    if name.is_empty() {
        return Err(Error::Config(
            "Environment variable name cannot be empty".to_string(),
        ));
    }

    // First character must be letter or underscore
    let first_char = name.chars().next().unwrap();
    if !first_char.is_ascii_alphabetic() && first_char != '_' {
        return Err(Error::Config(format!(
            "Invalid environment variable name '{}': must start with a letter or underscore",
            name
        )));
    }

    // Remaining characters must be alphanumeric or underscore
    for (i, c) in name.chars().enumerate() {
        if !c.is_ascii_alphanumeric() && c != '_' {
            return Err(Error::Config(format!(
                "Invalid environment variable name '{}': character '{}' at position {} is not allowed (must be alphanumeric or underscore)",
                name, c, i
            )));
        }
    }

    Ok(())
}

/// Check environment variable value for suspicious patterns.
///
/// This function warns about patterns that might indicate shell injection
/// attempts or unintended shell expansion, even though service-federation
/// does NOT perform shell expansion.
///
/// Suspicious patterns include:
/// - Backticks: `command`
/// - Command substitution: $(command)
/// - Variable expansion: ${VAR}
/// - Shell pipe/redirect operators: |, >, <, >>
///
/// Returns warnings as a `Vec<String>` rather than failing, since these
/// patterns may be legitimate in some contexts (e.g., passing SQL queries,
/// JSON with $, etc.).
pub fn check_suspicious_patterns(name: &str, value: &str) -> Vec<String> {
    let mut warnings = Vec::new();

    // Check for backticks
    if value.contains('`') {
        warnings.push(format!(
            "Environment variable '{}' contains backticks. \
             Note: service-federation does NOT perform shell expansion - \
             these will be passed literally to the process.",
            name
        ));
    }

    // Check for command substitution $()
    if value.contains("$(") {
        warnings.push(format!(
            "Environment variable '{}' contains '$(' which looks like command substitution. \
             Note: service-federation does NOT perform shell expansion - \
             this will be passed literally to the process.",
            name
        ));
    }

    // Check for variable expansion ${}
    if value.contains("${") {
        warnings.push(format!(
            "Environment variable '{}' contains '${{' which looks like variable expansion. \
             Note: service-federation does NOT perform shell expansion - \
             this will be passed literally to the process.",
            name
        ));
    }

    // Check for shell operators that might indicate command injection attempts
    let suspicious_operators = ["|", ">", "<", ">>", "<<", "&&", "||", ";"];
    for op in &suspicious_operators {
        if value.contains(op) {
            warnings.push(format!(
                "Environment variable '{}' contains '{}' which is a shell operator. \
                 Note: service-federation does NOT execute values as shell commands - \
                 this will be passed literally to the process.",
                name, op
            ));
            break; // Only warn once per variable
        }
    }

    warnings
}

/// Validate and sanitize environment variables.
///
/// This function:
/// 1. Validates all variable names
/// 2. Checks values for suspicious patterns and logs warnings
///
/// Returns an error if any variable name is invalid.
/// Logs warnings (via tracing) for suspicious patterns in values.
pub fn validate_and_sanitize_env(env_vars: &HashMap<String, String>) -> Result<()> {
    for (name, value) in env_vars {
        // Validate name
        validate_env_name(name)?;

        // Check for suspicious patterns (warns but doesn't fail)
        let warnings = check_suspicious_patterns(name, value);
        for warning in warnings {
            tracing::warn!("{}", warning);
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::TempDir;

    #[test]
    fn test_load_env_file_simple() {
        let temp_dir = TempDir::new().unwrap();
        let env_path = temp_dir.path().join(".env");
        fs::write(&env_path, "KEY1=value1\nKEY2=value2\n").unwrap();

        let result = load_env_file(&env_path).unwrap();
        assert_eq!(result.get("KEY1"), Some(&"value1".to_string()));
        assert_eq!(result.get("KEY2"), Some(&"value2".to_string()));
    }

    #[test]
    fn test_load_env_file_with_comments() {
        let temp_dir = TempDir::new().unwrap();
        let env_path = temp_dir.path().join(".env");
        fs::write(
            &env_path,
            "# This is a comment\nKEY=value\n# Another comment\n",
        )
        .unwrap();

        let result = load_env_file(&env_path).unwrap();
        assert_eq!(result.get("KEY"), Some(&"value".to_string()));
        assert_eq!(result.len(), 1);
    }

    #[test]
    fn test_load_env_file_with_quotes() {
        let temp_dir = TempDir::new().unwrap();
        let env_path = temp_dir.path().join(".env");
        fs::write(
            &env_path,
            "SINGLE='single quoted'\nDOUBLE=\"double quoted\"\n",
        )
        .unwrap();

        let result = load_env_file(&env_path).unwrap();
        assert_eq!(result.get("SINGLE"), Some(&"single quoted".to_string()));
        assert_eq!(result.get("DOUBLE"), Some(&"double quoted".to_string()));
    }

    #[test]
    fn test_load_env_file_missing() {
        let result = load_env_file("/nonexistent/.env");
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("not found"));
    }

    #[test]
    fn test_load_env_file_optional_missing_returns_none() {
        let result = load_env_file_optional("/nonexistent/.env").unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_load_env_file_optional_present_returns_values() {
        let temp_dir = TempDir::new().unwrap();
        let env_path = temp_dir.path().join(".env");
        fs::write(&env_path, "KEY=value\n").unwrap();

        let result = load_env_file_optional(&env_path).unwrap().unwrap();
        assert_eq!(result.get("KEY"), Some(&"value".to_string()));
    }

    #[test]
    fn test_load_env_file_optional_parse_error_still_errors() {
        // Invalid env name should propagate as Err, not collapse to None.
        let temp_dir = TempDir::new().unwrap();
        let env_path = temp_dir.path().join(".env");
        fs::write(&env_path, "INVALID-NAME=value\n").unwrap();

        let result = load_env_file_optional(&env_path);
        assert!(result.is_err());
    }

    #[test]
    fn test_load_env_files_merge() {
        let temp_dir = TempDir::new().unwrap();

        // First file
        let env1_path = temp_dir.path().join(".env");
        fs::write(&env1_path, "KEY1=first\nSHARED=from_first\n").unwrap();

        // Second file (overrides SHARED)
        let env2_path = temp_dir.path().join(".env.local");
        fs::write(&env2_path, "KEY2=second\nSHARED=from_second\n").unwrap();

        let paths = vec![".env".to_string(), ".env.local".to_string()];
        let result = load_env_files(&paths, temp_dir.path()).unwrap();

        assert_eq!(result.get("KEY1"), Some(&"first".to_string()));
        assert_eq!(result.get("KEY2"), Some(&"second".to_string()));
        assert_eq!(result.get("SHARED"), Some(&"from_second".to_string())); // Later wins
    }

    #[test]
    fn test_merge_environment_priority() {
        let base: HashMap<String, String> = [("A".to_string(), "base".to_string())]
            .into_iter()
            .collect();

        let from_files: HashMap<String, String> = [
            ("A".to_string(), "file".to_string()),
            ("B".to_string(), "file".to_string()),
        ]
        .into_iter()
        .collect();

        let inline: HashMap<String, String> = [
            ("A".to_string(), "inline".to_string()),
            ("C".to_string(), "inline".to_string()),
        ]
        .into_iter()
        .collect();

        let result = merge_environment(base, from_files, inline);

        assert_eq!(result.get("A"), Some(&"inline".to_string())); // inline wins
        assert_eq!(result.get("B"), Some(&"file".to_string())); // from file
        assert_eq!(result.get("C"), Some(&"inline".to_string())); // inline only
    }

    // Security validation tests

    #[test]
    fn test_validate_env_name_valid() {
        // Valid names
        assert!(validate_env_name("PATH").is_ok());
        assert!(validate_env_name("HOME").is_ok());
        assert!(validate_env_name("MY_VAR").is_ok());
        assert!(validate_env_name("_PRIVATE").is_ok());
        assert!(validate_env_name("VAR123").is_ok());
        assert!(validate_env_name("lowercase").is_ok());
        assert!(validate_env_name("MixedCase123_VAR").is_ok());
    }

    #[test]
    fn test_validate_env_name_invalid_start() {
        // Must start with letter or underscore
        assert!(validate_env_name("123VAR").is_err());
        assert!(validate_env_name("-VAR").is_err());
        assert!(validate_env_name(".VAR").is_err());
    }

    #[test]
    fn test_validate_env_name_invalid_chars() {
        // Only alphanumeric and underscore allowed
        assert!(validate_env_name("MY-VAR").is_err());
        assert!(validate_env_name("MY.VAR").is_err());
        assert!(validate_env_name("MY VAR").is_err());
        assert!(validate_env_name("MY$VAR").is_err());
        assert!(validate_env_name("MY@VAR").is_err());
    }

    #[test]
    fn test_validate_env_name_empty() {
        assert!(validate_env_name("").is_err());
    }

    #[test]
    fn test_check_suspicious_patterns_backticks() {
        let warnings = check_suspicious_patterns("CMD", "`whoami`");
        assert_eq!(warnings.len(), 1);
        assert!(warnings[0].contains("backticks"));
        assert!(warnings[0].contains("does NOT perform shell expansion"));
    }

    #[test]
    fn test_check_suspicious_patterns_command_substitution() {
        let warnings = check_suspicious_patterns("CMD", "$(whoami)");
        assert_eq!(warnings.len(), 1);
        assert!(warnings[0].contains("command substitution"));
    }

    #[test]
    fn test_check_suspicious_patterns_variable_expansion() {
        let warnings = check_suspicious_patterns("PATH", "/usr/bin:${HOME}/bin");
        assert_eq!(warnings.len(), 1);
        assert!(warnings[0].contains("variable expansion"));
    }

    #[test]
    fn test_check_suspicious_patterns_shell_operators() {
        // Pipe
        let warnings = check_suspicious_patterns("CMD", "cat file | grep foo");
        assert_eq!(warnings.len(), 1);
        assert!(warnings[0].contains("shell operator"));

        // Redirect
        let warnings = check_suspicious_patterns("CMD", "echo foo > /tmp/bar");
        assert_eq!(warnings.len(), 1);
        assert!(warnings[0].contains("shell operator"));

        // Chaining
        let warnings = check_suspicious_patterns("CMD", "rm -rf / && echo done");
        assert_eq!(warnings.len(), 1);
        assert!(warnings[0].contains("shell operator"));
    }

    #[test]
    fn test_check_suspicious_patterns_clean_values() {
        // These are safe and should produce no warnings
        assert_eq!(
            check_suspicious_patterns("DATABASE_URL", "postgres://localhost:5432/db").len(),
            0
        );
        assert_eq!(
            check_suspicious_patterns("API_KEY", "abc123def456").len(),
            0
        );
        assert_eq!(
            check_suspicious_patterns("JSON", r#"{"key": "value"}"#).len(),
            0
        );
    }

    #[test]
    fn test_check_suspicious_patterns_legitimate_dollar_sign() {
        // $ alone (e.g., in currency or regex) should not warn
        assert_eq!(check_suspicious_patterns("PRICE", "$19.99").len(), 0);
        assert_eq!(check_suspicious_patterns("REGEX", r"^\d+$").len(), 0);
    }

    #[test]
    fn test_validate_and_sanitize_env_valid() {
        let mut env: HashMap<String, String> = HashMap::new();
        env.insert("PATH".to_string(), "/usr/bin".to_string());
        env.insert("HOME".to_string(), "/home/user".to_string());

        assert!(validate_and_sanitize_env(&env).is_ok());
    }

    #[test]
    fn test_validate_and_sanitize_env_invalid_name() {
        let mut env: HashMap<String, String> = HashMap::new();
        env.insert("VALID_NAME".to_string(), "value".to_string());
        env.insert("INVALID-NAME".to_string(), "value".to_string());

        assert!(validate_and_sanitize_env(&env).is_err());
    }

    #[test]
    fn test_validate_and_sanitize_env_suspicious_value() {
        let mut env: HashMap<String, String> = HashMap::new();
        env.insert("CMD".to_string(), "$(whoami)".to_string());

        // Should succeed but log warnings
        assert!(validate_and_sanitize_env(&env).is_ok());
    }

    #[test]
    fn test_validate_and_sanitize_env_multiple_issues() {
        let mut env: HashMap<String, String> = HashMap::new();
        env.insert("GOOD".to_string(), "value".to_string());
        env.insert("BAD-NAME".to_string(), "value".to_string());
        env.insert("SUSPICIOUS".to_string(), "`whoami`".to_string());

        // Should fail due to invalid name
        let result = validate_and_sanitize_env(&env);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("BAD-NAME"));
    }
}
