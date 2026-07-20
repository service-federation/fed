use crate::error::{Error, Result};
use std::path::{Path, PathBuf};

/// Local package source handler
pub struct LocalSourceHandler;

impl LocalSourceHandler {
    /// Resolve a local package path
    ///
    /// # Arguments
    /// * `source` - The source path (can be relative or absolute)
    /// * `config_dir` - The directory containing the config file (for relative path resolution)
    ///
    /// # Returns
    /// The absolute path to the package
    pub fn resolve(source: &str, config_dir: &Path) -> Result<PathBuf> {
        let path = Path::new(source);

        // Convert to absolute path
        let absolute_path = if path.is_absolute() {
            path.to_path_buf()
        } else {
            config_dir.join(path)
        };

        // Canonicalize to resolve .. and . components
        let canonical_path = absolute_path.canonicalize().map_err(|e| {
            Error::PackageNotFound(format!(
                "Local package not found at {}: {}",
                absolute_path.display(),
                e
            ))
        })?;

        // Verify it's a directory
        if !canonical_path.is_dir() {
            return Err(Error::InvalidPackageSource(format!(
                "Package path {} is not a directory",
                canonical_path.display()
            )));
        }

        // Verify it contains a fed config file
        if crate::config::discovery::config_file_in_dir(&canonical_path).is_none() {
            return Err(Error::Package(format!(
                "Package at {} does not contain fed.yaml or service-federation.yaml (.yml also accepted)",
                canonical_path.display()
            )));
        }

        Ok(canonical_path)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::TempDir;

    #[test]
    fn test_resolve_absolute_path() {
        let temp_dir = TempDir::new().unwrap();
        let package_dir = temp_dir.path().join("package");
        fs::create_dir(&package_dir).unwrap();
        fs::write(package_dir.join("service-federation.yaml"), "services: {}").unwrap();

        let result = LocalSourceHandler::resolve(package_dir.to_str().unwrap(), temp_dir.path());

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), package_dir.canonicalize().unwrap());
    }

    #[test]
    fn test_resolve_relative_path() {
        let temp_dir = TempDir::new().unwrap();
        let package_dir = temp_dir.path().join("package");
        fs::create_dir(&package_dir).unwrap();
        fs::write(package_dir.join("service-federation.yaml"), "services: {}").unwrap();

        let result = LocalSourceHandler::resolve("./package", temp_dir.path());

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), package_dir.canonicalize().unwrap());
    }

    #[test]
    fn test_resolve_parent_relative_path() {
        let temp_dir = TempDir::new().unwrap();
        let package_dir = temp_dir.path().join("package");
        let config_dir = temp_dir.path().join("configs");
        fs::create_dir(&package_dir).unwrap();
        fs::create_dir(&config_dir).unwrap();
        fs::write(package_dir.join("service-federation.yaml"), "services: {}").unwrap();

        let result = LocalSourceHandler::resolve("../package", &config_dir);

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), package_dir.canonicalize().unwrap());
    }

    #[test]
    fn test_resolve_yml_extension() {
        let temp_dir = TempDir::new().unwrap();
        let package_dir = temp_dir.path().join("package");
        fs::create_dir(&package_dir).unwrap();
        fs::write(package_dir.join("service-federation.yml"), "services: {}").unwrap();

        let result = LocalSourceHandler::resolve("./package", temp_dir.path());

        assert!(result.is_ok());
    }

    #[test]
    fn test_resolve_not_found() {
        let temp_dir = TempDir::new().unwrap();

        let result = LocalSourceHandler::resolve("./nonexistent", temp_dir.path());

        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), Error::PackageNotFound(_)));
    }

    #[test]
    fn test_resolve_not_directory() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("file.txt");
        fs::write(&file_path, "test").unwrap();

        let result = LocalSourceHandler::resolve("./file.txt", temp_dir.path());

        assert!(result.is_err());
    }

    #[test]
    fn test_resolve_no_config_file() {
        let temp_dir = TempDir::new().unwrap();
        let package_dir = temp_dir.path().join("package");
        fs::create_dir(&package_dir).unwrap();

        let result = LocalSourceHandler::resolve("./package", temp_dir.path());

        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), Error::Package(_)));
    }
}
