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
    /// Existing values already loaded from .env (generated secrets file and
    /// user env_files — NOT the vault cache).
    pub existing_values: HashMap<String, String>,
    /// Values from the vault cache (`.fed/secrets.cache.env`), filtered to
    /// currently-declared secret parameter names. These do NOT count as
    /// present: when online the vault is authoritative and is re-queried;
    /// the cache satisfies secrets only when the vault is unreachable.
    pub cache_values: HashMap<String, String>,
    /// Per-entry fetched-at stamps (unix seconds) parsed from the cache's
    /// comment lines, filtered to declared names. Drives the freshness bound:
    /// an entry with no stamp (pre-upgrade cache) counts as too old.
    pub cache_stamps: HashMap<String, u64>,
}

/// A vault-cache entry: the value plus when it was fetched (unix seconds).
/// `fetched_at` is `None` for values carried forward from a pre-stamp cache.
#[derive(Debug, Clone)]
pub struct CacheEntry {
    pub value: String,
    pub fetched_at: Option<u64>,
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
/// vault secrets cache (`.fed/secrets.cache.env`); its values are reported
/// separately in `cache_values` and do not count as present — the vault stays
/// authoritative when reachable, the cache covers `--offline`.
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

    // Load existing values: the generated secrets file, then all env_files.
    // Later loads override earlier ones, matching runtime priority where fed's
    // own files are prepended to env_file = loaded first = lowest priority.
    // The vault cache is deliberately NOT merged here — cached values must not
    // shadow the vault (rotated secrets would never be re-fetched); it is
    // loaded separately and only consulted when the vault can't be.
    let mut existing_values = load_existing_env(secrets_file);
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

    // Vault cache, filtered to declared secret names — stale keys left over
    // from config changes are ignored (and pruned on the next cache write).
    // Values and stamps are parsed from a SINGLE read of the file: reading it
    // twice would let a concurrent atomic cache replacement between the reads
    // pair an old value with a new stamp (making a rotated value look fresh).
    let declared: HashSet<&str> = secret_params.iter().map(|(n, _)| n.as_str()).collect();
    let (mut cache_values, mut cache_stamps) = load_cache_values_and_stamps(cache_file);
    cache_values.retain(|k, _| declared.contains(k.as_str()));
    cache_stamps.retain(|k, _| declared.contains(k.as_str()));

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
        cache_values,
        cache_stamps,
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

    let mut opts = std::fs::OpenOptions::new();
    opts.create(true).truncate(false).read(true).write(true);
    // 0600 atomically at creation — no window where the file is world-readable.
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        opts.mode(0o600);
    }
    let file = opts
        .open(path)
        .map_err(|e| Error::Filesystem(format!("Cannot write '{}': {}", path.display(), e)))?;

    file.lock_exclusive()
        .map_err(|e| Error::Filesystem(format!("Cannot lock '{}': {}", path.display(), e)))?;

    // Pre-existing files: tighten to 0600 via the held handle (fchmod — no
    // path race) and surface failures instead of silently leaving it open.
    crate::fsutil::tighten_to_owner_only(&file, path)?;

    // Read the current contents through the locked handle, never by pathname.
    let content = {
        use std::io::{Read, Seek, SeekFrom};
        let mut f = &file;
        f.seek(SeekFrom::Start(0))
            .map_err(|e| Error::Filesystem(format!("Seek error: {}", e)))?;
        let mut s = String::new();
        f.read_to_string(&mut s)
            .map_err(|e| Error::Filesystem(format!("Read error: {}", e)))?;
        s
    };
    let existing: HashMap<String, String> = dotenvy::from_read_iter(content.as_bytes())
        .filter_map(|r| r.ok())
        .collect();
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

        // Write through the held (locked) handle — reopening by pathname would
        // race against path swaps/symlinks between lock and write.
        {
            use std::io::{Seek, SeekFrom, Write};
            let mut f = &file;
            f.seek(SeekFrom::Start(0))
                .map_err(|e| Error::Filesystem(format!("Seek error: {}", e)))?;
            file.set_len(0)
                .map_err(|e| Error::Filesystem(format!("Truncate error: {}", e)))?;
            f.write_all(output.as_bytes())
                .map_err(|e| Error::Filesystem(format!("Write error: {}", e)))?;
        }
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

/// Replace the vault secrets cache wholesale (owner-only, sorted keys).
///
/// The cache is fed-managed state, so unlike `write_env_file` this does not
/// merge: entries absent from `entries` are pruned (stale keys from config
/// changes, vault misses on rotation). An empty map removes the file.
///
/// The replacement is atomic: contents are written to a 0600 temp file in the
/// same directory and renamed over the destination, so concurrent readers
/// (dry-runs, direct resolver calls) see either the old or the new cache,
/// never a truncated one.
pub fn write_cache_file(path: &Path, entries: &HashMap<String, CacheEntry>) -> Result<()> {
    if entries.is_empty() {
        if path.exists() {
            std::fs::remove_file(path).map_err(|e| {
                Error::Filesystem(format!("Cannot remove '{}': {}", path.display(), e))
            })?;
        }
        return Ok(());
    }

    let mut out = String::from("# Vault secrets cache — managed by fed, do not commit\n");
    let mut keys: Vec<&String> = entries.keys().collect();
    keys.sort();
    for key in keys {
        let entry = &entries[key];
        // Stamp lives in an adjacent comment so the value lines stay a plain
        // KEY=VALUE .env that dotenvy (and offline resolution) reads as-is;
        // the freshness bound reads the stamps separately.
        if let Some(ts) = entry.fetched_at {
            out.push_str(&format!("# fetched-at {key} {ts}\n"));
        }
        out.push_str(&format!("{key}={}\n", encode_env_value(&entry.value)));
    }
    // Atomic 0600 replacement (shared helper): concurrent readers (dry-runs,
    // direct resolver calls) see either the old or the new cache, never a
    // truncated one. sync=true so a freshly fetched cache survives a crash —
    // cheap, since the cache is rewritten at most once per run.
    crate::fsutil::write_owner_only_atomic(path, out.as_bytes(), true)
}

/// Parse per-entry fetched-at stamps from the vault cache's comment lines.
///
/// Lines have the shape `# fetched-at <NAME> <unix-seconds>`. Entries without a
/// stamp (a pre-upgrade cache, or a hand-written file) simply don't appear —
/// the freshness bound then treats them as too old, costing one announced
/// refresh after upgrading.
pub fn load_cache_stamps(path: &Path) -> HashMap<String, u64> {
    let Ok(content) = std::fs::read_to_string(path) else {
        return HashMap::new();
    };
    parse_cache_stamps(&content)
}

/// Load both the cached values and their fetched-at stamps from a **single**
/// read of the cache file.
///
/// Reading values and stamps in two separate `read_to_string` calls opens a
/// race: the cache is replaced atomically (temp file + rename), so a rotation
/// landing between the two reads would pair an *old* value with the *new*
/// stamp — making a rotated value look freshly fetched and bypassing the
/// freshness bound. One read means values and stamps always come from the same
/// on-disk snapshot.
pub fn load_cache_values_and_stamps(
    path: &Path,
) -> (HashMap<String, String>, HashMap<String, u64>) {
    let Ok(content) = std::fs::read_to_string(path) else {
        return (HashMap::new(), HashMap::new());
    };
    let values: HashMap<String, String> = dotenvy::from_read_iter(content.as_bytes())
        .filter_map(|r| r.ok())
        .collect();
    let stamps = parse_cache_stamps(&content);
    (values, stamps)
}

/// Parse `# fetched-at <NAME> <unix-seconds>` comment lines from an already-read
/// cache buffer.
fn parse_cache_stamps(content: &str) -> HashMap<String, u64> {
    let mut out = HashMap::new();
    for line in content.lines() {
        if let Some(rest) = line.strip_prefix("# fetched-at ") {
            let mut parts = rest.split_whitespace();
            if let (Some(name), Some(ts)) = (parts.next(), parts.next()) {
                if let Ok(t) = ts.parse::<u64>() {
                    out.insert(name.to_string(), t);
                }
            }
        }
    }
    out
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
    fn analyze_reports_cache_separately_and_still_missing() {
        // Cached values must NOT count as present — the vault stays
        // authoritative when online — but they are surfaced in cache_values
        // (filtered to declared names) for offline fallback.
        let dir = tempfile::tempdir().unwrap();
        let cache = dir.path().join("secrets.cache.env");
        std::fs::write(
            &cache,
            "OPTIONAL_KEY=from_vault_cache\nAPI_KEY=cached\nSTALE_KEY=leftover\n",
        )
        .unwrap();

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
        assert_eq!(analysis.missing_manual.len(), 1, "cache must not satisfy");
        assert_eq!(analysis.missing_optional_manual.len(), 1);
        assert_eq!(
            analysis.cache_values.get("API_KEY").map(String::as_str),
            Some("cached")
        );
        assert!(
            !analysis.cache_values.contains_key("STALE_KEY"),
            "undeclared keys are filtered out of the loaded cache"
        );
        assert!(
            !analysis.existing_values.contains_key("API_KEY"),
            "cache values must not leak into existing_values"
        );
    }

    fn entry(value: &str, fetched_at: Option<u64>) -> CacheEntry {
        CacheEntry {
            value: value.to_string(),
            fetched_at,
        }
    }

    #[test]
    fn write_cache_file_replaces_wholesale_and_removes_when_empty() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("secrets.cache.env");
        std::fs::write(&path, "OLD=1\nKEEP=old\n").unwrap();

        let entries: HashMap<String, CacheEntry> =
            [("KEEP".to_string(), entry("new", Some(1_721_000_000)))]
                .into_iter()
                .collect();
        write_cache_file(&path, &entries).unwrap();
        let content = std::fs::read_to_string(&path).unwrap();
        assert!(content.contains("KEEP=new"));
        assert!(!content.contains("OLD="), "unlisted entries are pruned");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(&path).unwrap().permissions().mode();
            assert_eq!(mode & 0o777, 0o600);
        }

        write_cache_file(&path, &HashMap::new()).unwrap();
        assert!(!path.exists(), "empty cache removes the file");
    }

    #[test]
    fn cache_stamps_round_trip_and_values_stay_dotenv_readable() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("secrets.cache.env");
        let entries: HashMap<String, CacheEntry> = [
            ("A".to_string(), entry("va", Some(1_700_000_000))),
            ("B".to_string(), entry("vb", Some(1_700_000_500))),
        ]
        .into_iter()
        .collect();
        write_cache_file(&path, &entries).unwrap();

        // Values still parse as a plain .env (offline resolution reads these).
        let loaded = load_existing_env(&path);
        assert_eq!(loaded.get("A").map(String::as_str), Some("va"));
        assert_eq!(loaded.get("B").map(String::as_str), Some("vb"));

        // Stamps parse back from the comment lines.
        let stamps = load_cache_stamps(&path);
        assert_eq!(stamps.get("A"), Some(&1_700_000_000));
        assert_eq!(stamps.get("B"), Some(&1_700_000_500));
    }

    #[test]
    fn load_values_and_stamps_come_from_one_read() {
        // Values and stamps must be parsed from the same buffer so a concurrent
        // atomic replacement can never pair an old value with a new stamp.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("secrets.cache.env");
        let entries: HashMap<String, CacheEntry> = [
            ("A".to_string(), entry("va", Some(1_700_000_000))),
            ("B".to_string(), entry("vb", None)),
        ]
        .into_iter()
        .collect();
        write_cache_file(&path, &entries).unwrap();

        let (values, stamps) = load_cache_values_and_stamps(&path);
        assert_eq!(values.get("A").map(String::as_str), Some("va"));
        assert_eq!(values.get("B").map(String::as_str), Some("vb"));
        assert_eq!(stamps.get("A"), Some(&1_700_000_000));
        assert!(
            !stamps.contains_key("B"),
            "an unstamped entry yields a value but no stamp"
        );
        // A missing file yields two empty maps, never a panic.
        let (v, s) = load_cache_values_and_stamps(&dir.path().join("nope"));
        assert!(v.is_empty() && s.is_empty());
    }

    #[test]
    fn unstamped_entries_have_no_stamp() {
        // A pre-upgrade cache (plain KEY=VALUE, no comments) yields values but
        // no stamps — the freshness bound then treats them as too old.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("secrets.cache.env");
        std::fs::write(&path, "# header\nAPI_KEY=cached\n").unwrap();
        assert_eq!(
            load_existing_env(&path).get("API_KEY").map(String::as_str),
            Some("cached")
        );
        assert!(
            load_cache_stamps(&path).is_empty(),
            "no stamp comments means no stamps"
        );
    }

    #[test]
    fn entry_without_stamp_writes_bare_value_line() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("secrets.cache.env");
        let entries: HashMap<String, CacheEntry> =
            [("K".to_string(), entry("v", None))].into_iter().collect();
        write_cache_file(&path, &entries).unwrap();
        let content = std::fs::read_to_string(&path).unwrap();
        assert!(content.contains("K=v"));
        assert!(
            !content.contains("fetched-at"),
            "a stampless entry writes no stamp comment: {content}"
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
