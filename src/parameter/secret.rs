use crate::config::Config;
use crate::error::{Error, Result};
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};

/// Analysis of secret parameters in a config.
pub struct SecretAnalysis {
    /// Secret parameter names that need auto-generated values.
    pub needs_generation: Vec<String>,
    /// Required manual secrets missing from .env: (name, description).
    pub missing_manual: Vec<(String, Option<String>)>,
    /// Optional manual secrets with no value anywhere. Eligible for a vault
    /// lookup; fall back to empty string when the vault can't supply them.
    pub missing_optional_manual: Vec<String>,
    /// Path to the .env file that holds (or will hold) secrets.
    pub env_path: PathBuf,
    /// Whether the .env file is gitignored.
    pub is_gitignored: bool,
    /// Whether we're inside a git repository at all.
    pub in_git_repo: bool,
    /// Existing values already loaded from .env.
    pub existing_values: HashMap<String, String>,
}

/// Check if a path is gitignored in the enclosing repository.
///
/// Returns `(in_git_repo, is_ignored)`.
pub fn is_gitignored(work_dir: &Path, relative_path: &str) -> (bool, bool) {
    match git2::Repository::discover(work_dir) {
        Ok(repo) => {
            let ignored = repo.is_path_ignored(relative_path).unwrap_or(false);
            (true, ignored)
        }
        Err(_) => (false, false),
    }
}

/// Git status of an arbitrary (absolute) file path, discovering the enclosing
/// repository from the path itself rather than from the work dir.
///
/// Returns `(in_git_repo, is_ignored)`. Paths outside any git repository are
/// `(false, false)` — safe by construction, nothing to warn about.
pub fn path_git_status(path: &Path) -> (bool, bool) {
    // Discover from the nearest existing ancestor (the file or even its
    // parent directory may not exist yet on first run).
    let mut probe = path.parent();
    let start = loop {
        match probe {
            Some(p) if p.exists() => break p,
            Some(p) => probe = p.parent(),
            None => return (false, false),
        }
    };
    // Canonicalize so symlinked temp dirs (e.g. /var → /private/var on macOS)
    // compare correctly against the repository's workdir.
    let canon_start = start.canonicalize().unwrap_or_else(|_| start.to_path_buf());
    let path = match path.strip_prefix(start) {
        Ok(suffix) => canon_start.join(suffix),
        Err(_) => path.to_path_buf(),
    };
    let path = path.as_path();
    match git2::Repository::discover(&canon_start) {
        Ok(repo) => {
            let Some(workdir) = repo.workdir() else {
                return (false, false);
            };
            match path.strip_prefix(workdir) {
                Ok(rel) => {
                    let ignored = repo
                        .is_path_ignored(rel.to_string_lossy().as_ref())
                        .unwrap_or(false);
                    (true, ignored)
                }
                // Discovered repo doesn't actually contain the path.
                Err(_) => (false, false),
            }
        }
        Err(_) => (false, false),
    }
}

/// Scan config for secret parameters and classify what's present vs. missing.
///
/// `secrets_file` is the absolute path to the file where generated secrets are
/// stored (the deprecated `generated_secrets_file` if configured, otherwise
/// `.fed/secrets.generated.env`). `cache_file` is the absolute path to the
/// vault secrets cache (`.fed/secrets.cache.env`); values cached there count
/// as present, which is what keeps `--offline` working.
///
/// Returns `None` if no secret parameters exist.
pub fn analyze_secrets(
    config: &Config,
    work_dir: &Path,
    secrets_file: &Path,
    cache_file: &Path,
) -> Result<Option<SecretAnalysis>> {
    let effective_params = config.get_effective_parameters();

    // Collect all secret parameters
    let secret_params: Vec<(&String, &crate::config::Parameter)> = effective_params
        .iter()
        .filter(|(_, p)| p.is_secret_type())
        .collect();

    if secret_params.is_empty() {
        return Ok(None);
    }

    let env_path = secrets_file.to_path_buf();

    // Load existing values: vault cache first (lowest priority), then the
    // generated secrets file, then all env_files. Later loads override earlier
    // ones, matching runtime priority where fed's own files are prepended to
    // env_file = loaded first = lowest priority.
    let mut existing_values = load_existing_env(cache_file);
    for (k, v) in load_existing_env(secrets_file) {
        existing_values.insert(k, v);
    }
    for env_file in &config.env_file {
        let expanded = super::expand_tilde(Path::new(env_file));
        let ef_path = if expanded.is_absolute() {
            expanded
        } else {
            work_dir.join(expanded)
        };
        for (k, v) in load_existing_env(&ef_path) {
            existing_values.insert(k, v);
        }
    }

    // Git status of the secrets file itself (it may be an absolute path
    // outside this work dir's repository, or outside any repository).
    let (in_git_repo, is_gitignored) = path_git_status(&env_path);

    let mut needs_generation = Vec::new();
    let mut missing_manual = Vec::new();
    let mut missing_optional_manual = Vec::new();

    for (name, param) in &secret_params {
        // Already resolved (from .env or explicit value)?
        if param.value.is_some() || existing_values.contains_key(name.as_str()) {
            continue;
        }

        if param.is_manual_secret() {
            if param.is_optional() {
                missing_optional_manual.push((*name).clone());
            } else {
                missing_manual.push(((*name).clone(), param.description.clone()));
            }
        } else {
            needs_generation.push((*name).clone());
        }
    }

    Ok(Some(SecretAnalysis {
        needs_generation,
        missing_manual,
        missing_optional_manual,
        env_path,
        is_gitignored,
        in_git_repo,
        existing_values,
    }))
}

/// Encode a value for a `.env` line.
///
/// Generated secrets are alphanumeric, but vault-sourced values can be anything —
/// PEM keys, values with spaces, `#`, or newlines. Anything outside a conservative
/// safe set is double-quoted with escapes, which `dotenvy` parses back exactly.
pub fn encode_env_value(value: &str) -> String {
    let safe = !value.is_empty()
        && value
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || "_.:/@+-".contains(c));
    if safe {
        return value.to_string();
    }
    let mut out = String::with_capacity(value.len() + 2);
    out.push('"');
    for c in value.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            _ => out.push(c),
        }
    }
    out.push('"');
    out
}

/// Generate a random secret string using a CSPRNG (ChaCha12 via `thread_rng`).
///
/// 32-char alphanumeric (~190 bits of entropy).
pub fn generate_secret() -> String {
    use rand::Rng;
    const CHARSET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
    let mut rng = rand::thread_rng();
    (0..32)
        .map(|_| {
            let idx = rng.gen_range(0..CHARSET.len());
            CHARSET[idx] as char
        })
        .collect()
}

/// Write generated secret values to an env file.
///
/// Creates the file if it doesn't exist. For new keys, appends them.
/// For existing keys with new values (invalidated secrets), rewrites
/// the file to update them in place.
///
/// Uses an exclusive file lock to prevent concurrent write conflicts.
pub fn write_env_file(path: &Path, generated_values: &[(String, String)]) -> Result<()> {
    use fs2::FileExt;
    use std::io::Write;

    if generated_values.is_empty() {
        return Ok(());
    }

    let file = std::fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(path)
        .map_err(|e| Error::Filesystem(format!("Cannot write '{}': {}", path.display(), e)))?;

    file.lock_exclusive()
        .map_err(|e| Error::Filesystem(format!("Cannot lock '{}': {}", path.display(), e)))?;

    // Secrets file is 0600: readable by the owner only.
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let _ = std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600));
    }

    let existing = load_existing_env(path);
    let generated_map: HashMap<&str, &str> = generated_values
        .iter()
        .map(|(k, v)| (k.as_str(), v.as_str()))
        .collect();

    // Check if any existing key needs updating.
    let needs_rewrite = generated_values
        .iter()
        .any(|(k, v)| existing.get(k).is_some_and(|ev| ev != v));

    if needs_rewrite {
        // Rewrite the entire file: update changed values, keep others.
        let content = std::fs::read_to_string(path)
            .map_err(|e| Error::Filesystem(format!("Read error: {}", e)))?;

        let mut output = String::new();
        let mut written_keys: HashSet<&str> = HashSet::new();

        for line in content.lines() {
            if let Some(eq_pos) = line.find('=') {
                let key = &line[..eq_pos];
                if let Some(new_value) = generated_map.get(key) {
                    output.push_str(&format!("{key}={}\n", encode_env_value(new_value)));
                    written_keys.insert(key);
                } else {
                    output.push_str(line);
                    output.push('\n');
                }
            } else {
                output.push_str(line);
                output.push('\n');
            }
        }

        // Append any truly new keys.
        for (key, value) in generated_values {
            if !written_keys.contains(key.as_str()) && !existing.contains_key(key) {
                output.push_str(&format!("{key}={}\n", encode_env_value(value)));
            }
        }

        std::fs::write(path, output)
            .map_err(|e| Error::Filesystem(format!("Write error: {}", e)))?;
    } else {
        // Simple append — no existing keys need updating.
        let new_keys: Vec<&(String, String)> = generated_values
            .iter()
            .filter(|(k, _)| !existing.contains_key(k))
            .collect();

        if new_keys.is_empty() {
            return Ok(());
        }

        let metadata = file
            .metadata()
            .map_err(|e| Error::Filesystem(format!("Cannot stat '{}': {}", path.display(), e)))?;

        let mut writer = std::io::BufWriter::new(&file);

        if metadata.len() == 0 {
            writeln!(writer, "# Auto-generated by fed — do not commit this file")
                .map_err(|e| Error::Filesystem(format!("Write error: {}", e)))?;
        }

        // Seek to end for append.
        use std::io::Seek;
        writer
            .seek(std::io::SeekFrom::End(0))
            .map_err(|e| Error::Filesystem(format!("Seek error: {}", e)))?;

        for (key, value) in &new_keys {
            writeln!(writer, "{}={}", key, encode_env_value(value))
                .map_err(|e| Error::Filesystem(format!("Write error: {}", e)))?;
        }
    }

    Ok(())
}

/// Load key-value pairs from an existing .env file, if it exists.
fn load_existing_env(path: &Path) -> HashMap<String, String> {
    if !path.exists() {
        return HashMap::new();
    }

    // Use dotenvy's iterator to parse the file
    match dotenvy::from_path_iter(path) {
        Ok(iter) => iter.filter_map(|r| r.ok()).collect(),
        Err(_) => HashMap::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ========================================================================
    // generate_secret
    // ========================================================================

    #[test]
    fn generate_secret_length() {
        let secret = generate_secret();
        assert_eq!(secret.len(), 32);
    }

    #[test]
    fn generate_secret_alphanumeric() {
        let secret = generate_secret();
        assert!(
            secret.chars().all(|c| c.is_ascii_alphanumeric()),
            "Secret should be alphanumeric, got: {}",
            secret
        );
    }

    #[test]
    fn generate_secret_uniqueness() {
        let a = generate_secret();
        let b = generate_secret();
        assert_ne!(a, b, "Two generated secrets should differ");
    }

    // ========================================================================
    // write_env_file
    // ========================================================================

    #[test]
    fn write_env_creates_file_with_header() {
        let dir = tempfile::tempdir().unwrap();
        let env_path = dir.path().join(".env");

        write_env_file(
            &env_path,
            &[("SECRET_KEY".to_string(), "abc123".to_string())],
        )
        .unwrap();

        let content = std::fs::read_to_string(&env_path).unwrap();
        assert!(content.contains("# Auto-generated by fed"));
        assert!(content.contains("SECRET_KEY=abc123"));
    }

    #[test]
    fn write_env_appends_to_existing() {
        let dir = tempfile::tempdir().unwrap();
        let env_path = dir.path().join(".env");
        std::fs::write(&env_path, "EXISTING=value\n").unwrap();

        write_env_file(
            &env_path,
            &[("NEW_KEY".to_string(), "new_value".to_string())],
        )
        .unwrap();

        let content = std::fs::read_to_string(&env_path).unwrap();
        assert!(content.starts_with("EXISTING=value"));
        assert!(content.contains("NEW_KEY=new_value"));
        // Should NOT have the header since file already existed
        assert!(!content.contains("Auto-generated"));
    }

    // ========================================================================
    // is_gitignored
    // ========================================================================

    #[test]
    fn is_gitignored_outside_repo() {
        let dir = tempfile::tempdir().unwrap();
        let (in_repo, ignored) = is_gitignored(dir.path(), ".env");
        assert!(!in_repo);
        assert!(!ignored);
    }

    #[test]
    fn is_gitignored_not_ignored_in_repo() {
        let dir = tempfile::tempdir().unwrap();
        // Initialize a git repo without a .gitignore
        git2::Repository::init(dir.path()).unwrap();

        let (in_repo, ignored) = is_gitignored(dir.path(), ".env");
        assert!(in_repo);
        assert!(!ignored);
    }

    #[test]
    fn is_gitignored_ignored_in_repo() {
        let dir = tempfile::tempdir().unwrap();
        git2::Repository::init(dir.path()).unwrap();
        std::fs::write(dir.path().join(".gitignore"), ".env\n").unwrap();

        let (in_repo, ignored) = is_gitignored(dir.path(), ".env");
        assert!(in_repo);
        assert!(ignored);
    }

    // ========================================================================
    // analyze_secrets
    // ========================================================================

    #[test]
    fn analyze_no_secrets_returns_none() {
        let config = Config::default();
        let dir = tempfile::tempdir().unwrap();
        let secrets_file = dir.path().join(".env.secrets");
        let cache = dir.path().join(".fed/secrets.cache.env");
        let result = analyze_secrets(&config, dir.path(), &secrets_file, &cache).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn analyze_classifies_generated_and_manual() {
        let dir = tempfile::tempdir().unwrap();

        let mut config = Config::default();
        config.parameters.insert(
            "SESSION_KEY".to_string(),
            crate::config::Parameter {
                param_type: Some("secret".to_string()),
                ..Default::default()
            },
        );
        config.parameters.insert(
            "GITHUB_SECRET".to_string(),
            crate::config::Parameter {
                param_type: Some("secret".to_string()),
                source: Some("manual".to_string()),
                description: Some("GitHub OAuth secret".to_string()),
                ..Default::default()
            },
        );

        let secrets_file = dir.path().join(".env.secrets");
        let cache = dir.path().join(".fed/secrets.cache.env");
        let analysis = analyze_secrets(&config, dir.path(), &secrets_file, &cache)
            .unwrap()
            .unwrap();
        assert!(
            analysis
                .needs_generation
                .contains(&"SESSION_KEY".to_string()),
            "Should need to generate SESSION_KEY"
        );
        assert_eq!(analysis.missing_manual.len(), 1);
        assert_eq!(analysis.missing_manual[0].0, "GITHUB_SECRET");
        assert_eq!(
            analysis.missing_manual[0].1.as_deref(),
            Some("GitHub OAuth secret")
        );
    }

    #[test]
    fn analyze_skips_secrets_with_existing_env_values() {
        let dir = tempfile::tempdir().unwrap();
        let secrets_file = dir.path().join(".env.secrets");
        std::fs::write(&secrets_file, "SESSION_KEY=already_set\n").unwrap();

        let mut config = Config::default();
        config.parameters.insert(
            "SESSION_KEY".to_string(),
            crate::config::Parameter {
                param_type: Some("secret".to_string()),
                ..Default::default()
            },
        );

        let cache = dir.path().join(".fed/secrets.cache.env");
        let analysis = analyze_secrets(&config, dir.path(), &secrets_file, &cache)
            .unwrap()
            .unwrap();
        assert!(
            analysis.needs_generation.is_empty(),
            "Should not need to generate an already-present secret"
        );
    }

    #[test]
    fn analyze_separates_optional_manual_secrets() {
        let dir = tempfile::tempdir().unwrap();

        let mut config = Config::default();
        config.parameters.insert(
            "OPTIONAL_KEY".to_string(),
            crate::config::Parameter {
                param_type: Some("secret".to_string()),
                source: Some("manual".to_string()),
                optional: Some(true),
                ..Default::default()
            },
        );
        config.parameters.insert(
            "REQUIRED_KEY".to_string(),
            crate::config::Parameter {
                param_type: Some("secret".to_string()),
                source: Some("manual".to_string()),
                ..Default::default()
            },
        );

        let secrets_file = dir.path().join(".env.secrets");
        let cache = dir.path().join(".fed/secrets.cache.env");
        let analysis = analyze_secrets(&config, dir.path(), &secrets_file, &cache)
            .unwrap()
            .unwrap();
        assert_eq!(analysis.missing_manual.len(), 1);
        assert_eq!(analysis.missing_manual[0].0, "REQUIRED_KEY");
        assert_eq!(
            analysis.missing_optional_manual,
            vec!["OPTIONAL_KEY".to_string()],
            "Optional manual secrets should be listed for vault lookup"
        );
    }

    #[test]
    fn analyze_counts_cached_values_as_present() {
        let dir = tempfile::tempdir().unwrap();
        let cache = dir.path().join("secrets.cache.env");
        std::fs::write(&cache, "OPTIONAL_KEY=from_vault_cache\nAPI_KEY=cached\n").unwrap();

        let mut config = Config::default();
        config.parameters.insert(
            "OPTIONAL_KEY".to_string(),
            crate::config::Parameter {
                param_type: Some("secret".to_string()),
                source: Some("manual".to_string()),
                optional: Some(true),
                ..Default::default()
            },
        );
        config.parameters.insert(
            "API_KEY".to_string(),
            crate::config::Parameter {
                param_type: Some("secret".to_string()),
                source: Some("manual".to_string()),
                ..Default::default()
            },
        );

        let secrets_file = dir.path().join(".env.secrets");
        let analysis = analyze_secrets(&config, dir.path(), &secrets_file, &cache)
            .unwrap()
            .unwrap();
        assert!(
            analysis.missing_manual.is_empty(),
            "cache satisfies API_KEY"
        );
        assert!(
            analysis.missing_optional_manual.is_empty(),
            "cache satisfies OPTIONAL_KEY"
        );
        assert_eq!(
            analysis
                .existing_values
                .get("OPTIONAL_KEY")
                .map(|s| s.as_str()),
            Some("from_vault_cache")
        );
    }

    // ========================================================================
    // path_git_status
    // ========================================================================

    #[test]
    fn path_git_status_outside_any_repo() {
        let dir = tempfile::tempdir().unwrap();
        let (in_repo, ignored) = path_git_status(&dir.path().join("secrets.env"));
        assert!(!in_repo);
        assert!(!ignored);
    }

    #[test]
    fn path_git_status_absolute_path_outside_work_repo() {
        // A gsf pointing outside the repo (e.g. ~/shared/secrets.env) must be
        // treated as outside-any-repo even though the work dir is a repo.
        let repo_dir = tempfile::tempdir().unwrap();
        git2::Repository::init(repo_dir.path()).unwrap();
        let outside = tempfile::tempdir().unwrap();
        let (in_repo, _) = path_git_status(&outside.path().join("secrets.env"));
        assert!(!in_repo);
    }

    #[test]
    fn path_git_status_not_ignored_in_repo() {
        let dir = tempfile::tempdir().unwrap();
        git2::Repository::init(dir.path()).unwrap();
        let (in_repo, ignored) = path_git_status(&dir.path().join("secrets.env"));
        assert!(in_repo);
        assert!(!ignored);
    }

    #[test]
    fn path_git_status_ignored_via_nested_gitignore() {
        let dir = tempfile::tempdir().unwrap();
        git2::Repository::init(dir.path()).unwrap();
        crate::fed_dir::ensure_fed_dir(dir.path()).unwrap();
        let (in_repo, ignored) = path_git_status(&dir.path().join(".fed/secrets.generated.env"));
        assert!(in_repo);
        assert!(
            ignored,
            ".fed/.gitignore should make the default path ignored"
        );
    }

    #[test]
    fn path_git_status_nonexistent_ancestors() {
        let dir = tempfile::tempdir().unwrap();
        git2::Repository::init(dir.path()).unwrap();
        std::fs::write(dir.path().join(".gitignore"), "deep/\n").unwrap();
        let (in_repo, ignored) = path_git_status(&dir.path().join("deep/nested/secrets.env"));
        assert!(in_repo);
        assert!(ignored);
    }
}

#[cfg(test)]
mod env_encoding_tests {
    use super::*;

    #[test]
    fn plain_values_are_unquoted() {
        assert_eq!(encode_env_value("abc123"), "abc123");
        assert_eq!(
            encode_env_value("postgres://host:5432/db"),
            "postgres://host:5432/db"
        );
    }

    #[test]
    fn tricky_values_roundtrip_through_dotenvy() {
        let cases = [
            "value with spaces",
            "has#hash",
            "-----BEGIN KEY-----\nline2\nline3\n-----END KEY-----",
            "quote\"inside",
            "back\\slash",
            "",
        ];
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(".env");
        let pairs: Vec<(String, String)> = cases
            .iter()
            .enumerate()
            .map(|(i, v)| (format!("K{i}"), v.to_string()))
            .collect();
        write_env_file(&path, &pairs).unwrap();

        let loaded = load_existing_env(&path);
        for (k, v) in &pairs {
            assert_eq!(
                loaded.get(k).map(String::as_str),
                Some(v.as_str()),
                "key {k}"
            );
        }
    }
}
