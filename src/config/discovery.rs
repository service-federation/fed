//! Config file discovery.
//!
//! fed recognizes two config names: `fed.yaml` (preferred) and
//! `service-federation.yaml` (legacy, fully supported). Each also accepts a
//! `.yml` extension. When several candidates exist in the same directory the
//! priority is name-major: `fed.yaml`, `fed.yml`, `service-federation.yaml`,
//! `service-federation.yml`.

use std::path::{Path, PathBuf};

/// Config filenames fed recognizes, in priority order.
pub const CONFIG_FILENAMES: [&str; 4] = [
    "fed.yaml",
    "fed.yml",
    "service-federation.yaml",
    "service-federation.yml",
];

/// The preferred config filename. `fed init` generates this.
pub const DEFAULT_CONFIG_FILENAME: &str = "fed.yaml";

/// The legacy config filename, still fully supported.
pub const LEGACY_CONFIG_FILENAME: &str = "service-federation.yaml";

/// Find a config file directly in `dir` (no parent traversal).
///
/// Returns the highest-priority existing candidate from [`CONFIG_FILENAMES`].
pub fn config_file_in_dir(dir: &Path) -> Option<PathBuf> {
    CONFIG_FILENAMES
        .iter()
        .map(|name| dir.join(name))
        .find(|path| path.exists())
}

/// Like [`config_file_in_dir`], but also produces a one-line warning when the
/// chosen file is a `fed.*` config and a legacy `service-federation.*` config
/// exists in the same directory.
pub fn config_file_in_dir_with_warning(dir: &Path) -> Option<(PathBuf, Option<String>)> {
    let chosen = config_file_in_dir(dir)?;
    let chosen_name = chosen
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or(DEFAULT_CONFIG_FILENAME)
        .to_string();

    let warning = if chosen_name.starts_with("fed.") {
        ["service-federation.yaml", "service-federation.yml"]
            .iter()
            .find(|name| dir.join(name).exists())
            .map(|legacy| {
                format!(
                    "warning: both {} and {} exist in {}; using {}",
                    chosen_name,
                    legacy,
                    dir.display(),
                    chosen_name
                )
            })
    } else {
        None
    };

    Some((chosen, warning))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::TempDir;

    #[test]
    fn test_prefers_fed_yaml_over_legacy() {
        let dir = TempDir::new().unwrap();
        fs::write(dir.path().join("fed.yaml"), "services: {}").unwrap();
        fs::write(dir.path().join("service-federation.yaml"), "services: {}").unwrap();

        let found = config_file_in_dir(dir.path()).unwrap();
        assert_eq!(found, dir.path().join("fed.yaml"));
    }

    #[test]
    fn test_falls_back_to_legacy_name() {
        let dir = TempDir::new().unwrap();
        fs::write(dir.path().join("service-federation.yaml"), "services: {}").unwrap();

        let found = config_file_in_dir(dir.path()).unwrap();
        assert_eq!(found, dir.path().join("service-federation.yaml"));
    }

    #[test]
    fn test_accepts_fed_yml_extension() {
        let dir = TempDir::new().unwrap();
        fs::write(dir.path().join("fed.yml"), "services: {}").unwrap();

        let found = config_file_in_dir(dir.path()).unwrap();
        assert_eq!(found, dir.path().join("fed.yml"));
    }

    #[test]
    fn test_prefers_yaml_extension_over_yml() {
        let dir = TempDir::new().unwrap();
        fs::write(dir.path().join("fed.yaml"), "services: {}").unwrap();
        fs::write(dir.path().join("fed.yml"), "services: {}").unwrap();

        let found = config_file_in_dir(dir.path()).unwrap();
        assert_eq!(found, dir.path().join("fed.yaml"));
    }

    #[test]
    fn test_no_config_returns_none() {
        let dir = TempDir::new().unwrap();
        assert!(config_file_in_dir(dir.path()).is_none());
    }

    #[test]
    fn test_warning_when_both_names_present() {
        let dir = TempDir::new().unwrap();
        fs::write(dir.path().join("fed.yaml"), "services: {}").unwrap();
        fs::write(dir.path().join("service-federation.yaml"), "services: {}").unwrap();

        let (path, warning) = config_file_in_dir_with_warning(dir.path()).unwrap();
        assert_eq!(path, dir.path().join("fed.yaml"));
        let warning = warning.expect("expected a warning when both configs exist");
        assert!(warning.contains("fed.yaml"));
        assert!(warning.contains("service-federation.yaml"));
        assert!(warning.contains("using fed.yaml"));
    }

    #[test]
    fn test_no_warning_with_single_config() {
        let dir = TempDir::new().unwrap();
        fs::write(dir.path().join("fed.yaml"), "services: {}").unwrap();

        let (_, warning) = config_file_in_dir_with_warning(dir.path()).unwrap();
        assert!(warning.is_none());

        let dir = TempDir::new().unwrap();
        fs::write(dir.path().join("service-federation.yaml"), "services: {}").unwrap();

        let (path, warning) = config_file_in_dir_with_warning(dir.path()).unwrap();
        assert_eq!(path, dir.path().join("service-federation.yaml"));
        assert!(warning.is_none());
    }
}
