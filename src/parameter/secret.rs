use crate::config::Config;
use crate::error::{Error, Result};
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};

/// Analysis of secret parameters in a config.
pub struct SecretAnalysis {
    /// Secret parameter names that need auto-generated values.
    pub needs_generation: Vec<String>,
    /// Manual secrets missing from .env: (name, description).
    pub missing_manual: Vec<(String, Option<String>)>,
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

/// Scan config for secret parameters and classify what's present vs. missing.
///
/// `secrets_file` is the absolute path to the file where generated secrets are stored,
/// or `None` if no `generated_secrets_file` is configured (manual-only mode).
///
/// Returns `None` if no secret parameters exist.
pub fn analyze_secrets(
    config: &Config,
    work_dir: &Path,
    secrets_file: Option<&Path>,
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

    let env_path = secrets_file
        .map(|p| p.to_path_buf())
        .unwrap_or_else(|| work_dir.join(".env"));

    // Load existing values from the secrets file (if configured) and all env_files.
    // env_file values override the secrets file (matching runtime priority where
    // generated_secrets_file is prepended = loaded first = lowest priority).
    let mut existing_values = if let Some(sf) = secrets_file {
        load_existing_env(sf)
    } else {
        HashMap::new()
    };
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

    // Gitignore check only applies when we have a secrets file to write to
    let (in_git_repo, is_gitignored) = if let Some(sf) = secrets_file {
        let relative_path = sf
            .strip_prefix(work_dir)
            .map(|p| p.to_string_lossy().to_string())
            .unwrap_or_else(|_| sf.to_string_lossy().to_string());
        is_gitignored(work_dir, &relative_path)
    } else {
        (false, false)
    };

    let mut needs_generation = Vec::new();
    let mut missing_manual = Vec::new();

    for (name, param) in &secret_params {
        // Already resolved (from .env or explicit value)?
        if param.value.is_some() || existing_values.contains_key(name.as_str()) {
            continue;
        }

        if param.is_manual_secret() {
            if !param.is_optional() {
                missing_manual.push(((*name).clone(), param.description.clone()));
            }
        } else {
            needs_generation.push((*name).clone());
        }
    }

    Ok(Some(SecretAnalysis {
        needs_generation,
        missing_manual,
        env_path,
        is_gitignored,
        in_git_repo,
        existing_values,
    }))
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
                    output.push_str(&format!("{key}={new_value}\n"));
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
                output.push_str(&format!("{key}={value}\n"));
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
            writeln!(writer, "{}={}", key, value)
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
        let result = analyze_secrets(&config, dir.path(), Some(&secrets_file)).unwrap();
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
        let analysis = analyze_secrets(&config, dir.path(), Some(&secrets_file))
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

        let analysis = analyze_secrets(&config, dir.path(), Some(&secrets_file))
            .unwrap()
            .unwrap();
        assert!(
            analysis.needs_generation.is_empty(),
            "Should not need to generate an already-present secret"
        );
    }

    #[test]
    fn analyze_skips_optional_manual_secrets() {
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
        let analysis = analyze_secrets(&config, dir.path(), Some(&secrets_file))
            .unwrap()
            .unwrap();
        assert_eq!(analysis.missing_manual.len(), 1);
        assert_eq!(analysis.missing_manual[0].0, "REQUIRED_KEY");
    }

    #[test]
    fn analyze_manual_only_without_secrets_file() {
        let dir = tempfile::tempdir().unwrap();

        let mut config = Config::default();
        config.parameters.insert(
            "API_KEY".to_string(),
            crate::config::Parameter {
                param_type: Some("secret".to_string()),
                source: Some("manual".to_string()),
                ..Default::default()
            },
        );

        // None = no generated_secrets_file configured
        let analysis = analyze_secrets(&config, dir.path(), None).unwrap().unwrap();
        assert!(analysis.needs_generation.is_empty());
        assert_eq!(analysis.missing_manual.len(), 1);
        assert_eq!(analysis.missing_manual[0].0, "API_KEY");
    }
}
