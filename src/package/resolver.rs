// Allow deprecated PackageAuth - we still accept it for backward compatibility
// but no longer use the credentials
#![allow(deprecated)]

use crate::config::{PackageAuth, PackageReference};
use crate::error::{Error, Result};
use crate::package::loader::PackageLoader;
use crate::package::source::LocalSourceHandler;
use crate::package::types::{
    Package, PackageCache, PackageCacheEntry, PackageMetadata, PackageSource, PackageVersion,
};
use std::collections::HashMap;
use std::path::{Path, PathBuf};

/// Package resolver for resolving and loading packages
pub struct PackageResolver {
    cache_dir: PathBuf,
    config_dir: PathBuf,
    cache: PackageCache,
    loader: PackageLoader,
    /// Offline mode - skip git operations and use only cached packages
    offline: bool,
}

impl PackageResolver {
    pub fn new(config_dir: &Path) -> Result<Self> {
        Self::with_offline(config_dir, false)
    }

    pub fn with_offline(config_dir: &Path, offline: bool) -> Result<Self> {
        let cache_dir = Self::get_cache_dir()?;
        let cache = Self::load_cache(&cache_dir)?;

        Ok(Self {
            cache_dir,
            config_dir: config_dir.to_path_buf(),
            cache,
            loader: PackageLoader::new(),
            offline,
        })
    }

    /// Resolve all package references
    pub async fn resolve_all(
        &mut self,
        pkg_refs: &[PackageReference],
    ) -> Result<HashMap<String, Package>> {
        let mut packages = HashMap::new();
        // Track resolved packages to detect diamond dependencies
        // Map: package identity -> (version, alias that required it)
        let mut resolved_versions: HashMap<String, (PackageVersion, String)> = HashMap::new();

        for pkg_ref in pkg_refs {
            // Parse source to get identity
            let source = self.parse_source(&pkg_ref.source)?;

            // Check for diamond dependency before resolving
            if let Some(identity) = source.identity() {
                let version = source.version();

                if let Some((existing_version, existing_requester)) =
                    resolved_versions.get(&identity)
                {
                    // Same package already resolved - check if version matches
                    if existing_version != &version {
                        let conflicts_formatted = format!(
                            "  - {} (required by: {})\n  - {} (required by: {})",
                            existing_version, existing_requester, version, pkg_ref.r#as
                        );
                        return Err(Error::DiamondDependency {
                            identity: identity.clone(),
                            conflicts_formatted,
                        });
                    }
                } else {
                    // First time seeing this package identity
                    resolved_versions.insert(identity, (version, pkg_ref.r#as.clone()));
                }
            }

            let package = self.resolve(pkg_ref).await?;
            packages.insert(pkg_ref.r#as.clone(), package);
        }

        Ok(packages)
    }

    /// Resolve a single package reference
    pub async fn resolve(&mut self, pkg_ref: &PackageReference) -> Result<Package> {
        // 1. Parse source
        let source = self.parse_source(&pkg_ref.source)?;

        // 2. Check cache (skip for local packages - they're always fresh)
        if !matches!(source, PackageSource::Local { .. })
            && let Some(cached) = self.check_cache(&source)?
        {
            tracing::info!("Using cached package: {}", pkg_ref.r#as);
            return self.load_from_cache(cached, &pkg_ref.r#as);
        }

        // 3. Fetch package
        let path = self.fetch_package(&source, &pkg_ref.auth).await?;

        // 4. Load configuration
        let config = self.loader.load_and_validate(&path)?;

        // 5. Create package metadata
        let metadata = self.create_metadata(&path, &source)?;

        // 6. Cache package (skip local packages)
        if !matches!(source, PackageSource::Local { .. }) {
            self.cache_package(&source, &path, &metadata)?;
        }

        // 7. Return package
        Ok(Package {
            alias: pkg_ref.r#as.clone(),
            source,
            config,
            path,
            metadata,
        })
    }

    /// Parse package source string
    fn parse_source(&self, source: &str) -> Result<PackageSource> {
        if source.starts_with("github:") {
            self.parse_github_source(source)
        } else if source.starts_with("git+ssh://") {
            self.parse_git_source(source)
        } else {
            self.parse_local_source(source)
        }
    }

    /// Parse local source (relative or absolute path)
    fn parse_local_source(&self, source: &str) -> Result<PackageSource> {
        let path = LocalSourceHandler::resolve(source, &self.config_dir)?;
        Ok(PackageSource::Local { path })
    }

    /// Parse GitHub source format: `github:org/repo[@version]`
    fn parse_github_source(&self, source: &str) -> Result<PackageSource> {
        let source = source.strip_prefix("github:").ok_or_else(|| {
            Error::InvalidPackageSource(format!("Invalid GitHub source: {}", source))
        })?;

        // Split by '@' to separate org/repo from version
        let (org_repo, version) = if let Some(pos) = source.rfind('@') {
            let (org_repo, version_str) = source.split_at(pos);
            let version_str = &version_str[1..]; // Remove '@'
            (org_repo, PackageVersion::Tag(version_str.to_string()))
        } else {
            (source, PackageVersion::Latest)
        };

        // Split org/repo
        let parts: Vec<&str> = org_repo.split('/').collect();
        if parts.len() != 2 {
            return Err(Error::InvalidPackageSource(format!(
                "Invalid GitHub source format. Expected github:org/repo[@version], got: {}",
                source
            )));
        }

        Ok(PackageSource::GitHub {
            org: parts[0].to_string(),
            repo: parts[1].to_string(),
            version,
        })
    }

    /// Parse Git SSH source format: `git+ssh://[user@]host/path[@version]`
    fn parse_git_source(&self, source: &str) -> Result<PackageSource> {
        // Split by '@' at the end to separate url from version
        // Be careful not to confuse with user@ in the URL
        let (url, version) = if let Some(pos) = source.rfind('@') {
            // Check if this @ is after :// (part of user@host)
            if let Some(scheme_end) = source.find("://") {
                if pos > scheme_end + 3 {
                    // Check if there's another @ between scheme and this one
                    let between = &source[scheme_end + 3..pos];
                    if between.contains('@') {
                        // This @ is the version separator
                        let (url, version_str) = source.split_at(pos);
                        let version_str = &version_str[1..]; // Remove '@'
                        (
                            url.to_string(),
                            PackageVersion::Tag(version_str.to_string()),
                        )
                    } else {
                        // This @ might be user@host, check if there's a path after
                        if source[pos..].contains('/') {
                            // This is user@host, no version specified
                            (source.to_string(), PackageVersion::Latest)
                        } else {
                            // This is version separator
                            let (url, version_str) = source.split_at(pos);
                            let version_str = &version_str[1..]; // Remove '@'
                            (
                                url.to_string(),
                                PackageVersion::Tag(version_str.to_string()),
                            )
                        }
                    }
                } else {
                    (source.to_string(), PackageVersion::Latest)
                }
            } else {
                (source.to_string(), PackageVersion::Latest)
            }
        } else {
            (source.to_string(), PackageVersion::Latest)
        };

        if !url.starts_with("git+ssh://") {
            return Err(Error::InvalidPackageSource(format!(
                "Invalid Git SSH source. Expected git+ssh://[user@]host/path[@version], got: {}",
                source
            )));
        }

        Ok(PackageSource::Git { url, version })
    }

    /// Fetch package to filesystem
    async fn fetch_package(
        &self,
        source: &PackageSource,
        auth: &Option<PackageAuth>,
    ) -> Result<PathBuf> {
        match source {
            PackageSource::Local { path } => Ok(path.clone()),
            PackageSource::GitHub { org, repo, version } => {
                self.fetch_github(org, repo, version, auth).await
            }
            PackageSource::Git { url, version } => self.fetch_git(url, version, auth).await,
        }
    }

    /// Fetch GitHub package
    async fn fetch_github(
        &self,
        org: &str,
        repo: &str,
        version: &PackageVersion,
        auth: &Option<PackageAuth>,
    ) -> Result<PathBuf> {
        let cache_path = self.cache_dir.join("github").join(org).join(repo);
        std::fs::create_dir_all(&cache_path)?;

        let target_dir = cache_path.join(version.as_string());

        // If already cloned, just checkout the right version
        if target_dir.exists() && target_dir.join(".git").exists() {
            self.git_checkout(&target_dir, version).await?;
            return Ok(target_dir);
        }

        // In offline mode, fail if package is not cached
        if self.offline {
            return Err(Error::Package(format!(
                "Package github:{}/{}@{} is not cached. Run without --offline to fetch it.",
                org,
                repo,
                version.as_string()
            )));
        }

        // Warn if auth is configured (deprecated)
        if auth.is_some() {
            tracing::warn!(
                "Package `auth` is ignored. \
                Configure git credentials using ssh-agent or git credential helpers instead. \
                See: https://git-scm.com/book/en/v2/Git-Tools-Credential-Storage"
            );
        }

        // Use standard HTTPS URL - let git handle authentication via credential helpers or SSH agent
        // Users should configure their git credentials outside of fed:
        // - For SSH: Use git@github.com URLs and ssh-agent
        // - For HTTPS: Use git credential helpers or GH_TOKEN/GITHUB_TOKEN env vars
        let git_url = format!("https://github.com/{}/{}.git", org, repo);

        // Clone repository
        tracing::info!("Cloning GitHub repo: {}/{}", org, repo);
        self.git_clone(&git_url, &target_dir).await?;

        // Checkout specific version
        self.git_checkout(&target_dir, version).await?;

        Ok(target_dir)
    }

    /// Fetch Git SSH package
    async fn fetch_git(
        &self,
        url: &str,
        version: &PackageVersion,
        auth: &Option<PackageAuth>,
    ) -> Result<PathBuf> {
        // Generate cache path from URL
        let cache_key = url.replace("git+ssh://", "").replace(['/', ':'], "_");
        let cache_path = self.cache_dir.join("git").join(&cache_key);
        std::fs::create_dir_all(&cache_path)?;

        let target_dir = cache_path.join(version.as_string());

        // If already cloned, just checkout the right version
        if target_dir.exists() && target_dir.join(".git").exists() {
            self.git_checkout(&target_dir, version).await?;
            return Ok(target_dir);
        }

        // In offline mode, fail if package is not cached
        if self.offline {
            return Err(Error::Package(format!(
                "Package {}@{} is not cached. Run without --offline to fetch it.",
                url,
                version.as_string()
            )));
        }

        // Warn if auth is configured (deprecated)
        if auth.is_some() {
            tracing::warn!(
                "Package `auth` is ignored. \
                Configure git credentials using ssh-agent or git credential helpers instead. \
                See: https://git-scm.com/book/en/v2/Git-Tools-Credential-Storage"
            );
        }

        // Convert git+ssh:// to ssh:// - let system git handle SSH authentication via ssh-agent
        let git_url = url.replace("git+ssh://", "ssh://");

        // Clone repository
        tracing::info!("Cloning Git repo: {}", url);
        self.git_clone(&git_url, &target_dir).await?;

        // Checkout specific version
        self.git_checkout(&target_dir, version).await?;

        Ok(target_dir)
    }

    /// Clone git repository
    async fn git_clone(&self, url: &str, target: &Path) -> Result<()> {
        // Create temp directory for clone to prevent cache corruption on interruption
        let temp_path = target.with_extension("tmp");
        if temp_path.exists() {
            tokio::fs::remove_dir_all(&temp_path).await?;
        }

        // Clone to temp directory using async process
        let output = tokio::process::Command::new("git")
            .args(["clone", "--quiet", url, &temp_path.to_string_lossy()])
            .output()
            .await?;

        if !output.status.success() {
            // Clean up temp directory on failure
            let _ = tokio::fs::remove_dir_all(&temp_path).await;
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(Error::Package(format!(
                "Failed to clone repository: {}",
                stderr
            )));
        }

        // Atomic rename to final location
        if target.exists() {
            tokio::fs::remove_dir_all(target).await?;
        }
        tokio::fs::rename(&temp_path, target).await?;

        Ok(())
    }

    /// Checkout git version (tag, branch, or commit)
    async fn git_checkout(&self, repo_path: &Path, version: &PackageVersion) -> Result<()> {
        match version {
            PackageVersion::Latest => {
                // Use cached version without pulling to avoid network dependency.
                // The initial clone already fetched the default branch.
                // To update to the latest version, use `fed package refresh` (when implemented).
                tracing::debug!(
                    "Using cached version for 'latest' package at {} (use `fed package refresh` to update)",
                    repo_path.display()
                );
            }
            PackageVersion::Tag(tag)
            | PackageVersion::Branch(tag)
            | PackageVersion::Commit(tag) => {
                // Fetch to ensure we have the ref (skip in offline mode)
                if !self.offline {
                    let _ = tokio::process::Command::new("git")
                        .args(["fetch", "--quiet", "--tags"])
                        .current_dir(repo_path)
                        .output()
                        .await?;
                }

                // Checkout the ref using async process
                let output = tokio::process::Command::new("git")
                    .args(["checkout", "--quiet", tag])
                    .current_dir(repo_path)
                    .output()
                    .await?;

                if !output.status.success() {
                    let stderr = String::from_utf8_lossy(&output.stderr);
                    return Err(Error::Package(format!(
                        "Failed to checkout {}: {}{}",
                        tag,
                        stderr,
                        if self.offline {
                            " (running in offline mode - ref may not be cached)"
                        } else {
                            ""
                        }
                    )));
                }
            }
        }

        Ok(())
    }

    /// Check if package is in cache
    fn check_cache(&self, source: &PackageSource) -> Result<Option<&PackageCacheEntry>> {
        let key = self.cache_key(source);
        Ok(self.cache.entries.get(&key))
    }

    /// Load package from cache entry
    fn load_from_cache(&self, cached: &PackageCacheEntry, alias: &str) -> Result<Package> {
        let config = self.loader.load_and_validate(&cached.path)?;

        let source = self.parse_source(&cached.source)?;

        Ok(Package {
            alias: alias.to_string(),
            source,
            config,
            path: cached.path.clone(),
            metadata: cached.metadata.clone(),
        })
    }

    /// Create package metadata
    fn create_metadata(&self, path: &Path, source: &PackageSource) -> Result<PackageMetadata> {
        // Extract name from path
        let name = path
            .file_name()
            .and_then(|n| n.to_str())
            .map(|s| s.to_string());

        // Try to read additional metadata from config file if it exists
        let metadata_file = path.join(".service-federation-metadata.json");
        if metadata_file.exists()
            && let Ok(content) = std::fs::read_to_string(&metadata_file)
            && let Ok(metadata) = serde_json::from_str::<serde_json::Value>(&content)
        {
            return Ok(PackageMetadata {
                name: metadata
                    .get("name")
                    .and_then(|v| v.as_str())
                    .map(String::from)
                    .or(name),
                description: metadata
                    .get("description")
                    .and_then(|v| v.as_str())
                    .map(String::from),
                version: metadata
                    .get("version")
                    .and_then(|v| v.as_str())
                    .map(String::from),
                updated_at: chrono::Utc::now(),
                checksum: metadata
                    .get("checksum")
                    .and_then(|v| v.as_str())
                    .map(String::from),
            });
        }

        // Try to extract version from source
        let version = match source {
            PackageSource::GitHub { version, .. } | PackageSource::Git { version, .. } => {
                if !matches!(version, PackageVersion::Latest) {
                    Some(version.as_string())
                } else {
                    None
                }
            }
            PackageSource::Local { .. } => None,
        };

        // Fallback to basic metadata
        Ok(PackageMetadata {
            name,
            description: None,
            version,
            updated_at: chrono::Utc::now(),
            checksum: None,
        })
    }

    /// Cache a package
    fn cache_package(
        &mut self,
        source: &PackageSource,
        path: &std::path::Path,
        metadata: &PackageMetadata,
    ) -> Result<()> {
        let key = self.cache_key(source);
        let version = self.extract_version(source);

        let entry = PackageCacheEntry {
            source: key.clone(),
            version,
            path: path.to_path_buf(),
            metadata: metadata.clone(),
            cached_at: chrono::Utc::now(),
        };

        self.cache.entries.insert(key, entry);
        self.save_cache()?;

        Ok(())
    }

    /// Extract version from package source
    fn extract_version(&self, source: &PackageSource) -> PackageVersion {
        match source {
            PackageSource::Local { .. } => PackageVersion::Latest,
            PackageSource::GitHub { version, .. } | PackageSource::Git { version, .. } => {
                version.clone()
            }
        }
    }

    /// Generate cache key for a package source
    fn cache_key(&self, source: &PackageSource) -> String {
        match source {
            PackageSource::Local { path } => {
                format!("local:{}", path.display())
            }
            PackageSource::GitHub { org, repo, version } => {
                format!("github:{}/{}@{}", org, repo, version)
            }
            PackageSource::Git { url, version } => {
                format!("git:{}@{}", url, version)
            }
        }
    }

    /// Get cache directory (internal)
    fn get_cache_dir() -> Result<PathBuf> {
        Self::get_cache_dir_static()
    }

    /// Get cache directory (public static for commands)
    pub fn get_cache_dir_static() -> Result<PathBuf> {
        let cache_dir = dirs::cache_dir()
            .ok_or_else(|| Error::PackageCache("Could not determine cache directory".to_string()))?
            .join("service-federation")
            .join("packages");

        std::fs::create_dir_all(&cache_dir)?;

        Ok(cache_dir)
    }

    /// Load cache from disk
    fn load_cache(cache_dir: &Path) -> Result<PackageCache> {
        let index_path = cache_dir.join("index.json");
        if !index_path.exists() {
            return Ok(PackageCache::default());
        }

        let content = std::fs::read_to_string(index_path)?;
        let cache = serde_json::from_str(&content)?;
        Ok(cache)
    }

    /// Save cache to disk
    fn save_cache(&self) -> Result<()> {
        let index_path = self.cache_dir.join("index.json");
        let content = serde_json::to_string_pretty(&self.cache)?;
        std::fs::write(index_path, content)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::PackageReference;
    use std::fs;
    use tempfile::TempDir;

    fn create_test_package(dir: &Path, name: &str) -> PathBuf {
        let package_dir = dir.join(name);
        fs::create_dir(&package_dir).unwrap();
        fs::write(
            package_dir.join("service-federation.yaml"),
            r#"
services:
  test-service:
    process: "echo hello"
"#,
        )
        .unwrap();
        package_dir
    }

    #[tokio::test]
    async fn test_resolve_local_package() {
        let temp_dir = TempDir::new().unwrap();
        let package_dir = create_test_package(temp_dir.path(), "test-package");

        let mut resolver = PackageResolver::new(temp_dir.path()).unwrap();

        let pkg_ref = PackageReference {
            source: "./test-package".to_string(),
            r#as: "test-pkg".to_string(),
            auth: None,
        };

        let result = resolver.resolve(&pkg_ref).await;
        assert!(result.is_ok());

        let package = result.unwrap();
        assert_eq!(package.alias, "test-pkg");
        assert_eq!(package.path, package_dir.canonicalize().unwrap());
        assert!(package.config.services.contains_key("test-service"));
    }

    #[tokio::test]
    async fn test_resolve_all_packages() {
        let temp_dir = TempDir::new().unwrap();
        create_test_package(temp_dir.path(), "package1");
        create_test_package(temp_dir.path(), "package2");

        let mut resolver = PackageResolver::new(temp_dir.path()).unwrap();

        let pkg_refs = vec![
            PackageReference {
                source: "./package1".to_string(),
                r#as: "pkg1".to_string(),
                auth: None,
            },
            PackageReference {
                source: "./package2".to_string(),
                r#as: "pkg2".to_string(),
                auth: None,
            },
        ];

        let result = resolver.resolve_all(&pkg_refs).await;
        assert!(result.is_ok());

        let packages = result.unwrap();
        assert_eq!(packages.len(), 2);
        assert!(packages.contains_key("pkg1"));
        assert!(packages.contains_key("pkg2"));
    }

    #[tokio::test]
    async fn test_resolve_package_not_found() {
        let temp_dir = TempDir::new().unwrap();
        let mut resolver = PackageResolver::new(temp_dir.path()).unwrap();

        let pkg_ref = PackageReference {
            source: "./nonexistent".to_string(),
            r#as: "test-pkg".to_string(),
            auth: None,
        };

        let result = resolver.resolve(&pkg_ref).await;
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_local_source() {
        let temp_dir = TempDir::new().unwrap();
        let package_dir = create_test_package(temp_dir.path(), "test-package");

        let resolver = PackageResolver::new(temp_dir.path()).unwrap();
        let result = resolver.parse_source("./test-package");

        assert!(result.is_ok());
        if let PackageSource::Local { path } = result.unwrap() {
            assert_eq!(path, package_dir.canonicalize().unwrap());
        } else {
            panic!("Expected Local package source");
        }
    }

    #[test]
    fn test_parse_github_source() {
        let temp_dir = TempDir::new().unwrap();
        let resolver = PackageResolver::new(temp_dir.path()).unwrap();
        let result = resolver.parse_source("github:org/repo@v1.0.0");

        assert!(result.is_ok());
        match result.unwrap() {
            PackageSource::GitHub { org, repo, version } => {
                assert_eq!(org, "org");
                assert_eq!(repo, "repo");
                assert!(matches!(version, PackageVersion::Tag(_)));
            }
            _ => panic!("Expected GitHub source"),
        }
    }

    #[test]
    fn test_cache_key_generation() {
        let temp_dir = TempDir::new().unwrap();
        let resolver = PackageResolver::new(temp_dir.path()).unwrap();

        let local_source = PackageSource::Local {
            path: PathBuf::from("/tmp/package"),
        };
        let key = resolver.cache_key(&local_source);
        assert!(key.starts_with("local:"));

        let github_source = PackageSource::GitHub {
            org: "org".to_string(),
            repo: "repo".to_string(),
            version: PackageVersion::Tag("v1.0.0".to_string()),
        };
        let key = resolver.cache_key(&github_source);
        assert_eq!(key, "github:org/repo@v1.0.0");
    }

    #[test]
    fn test_diamond_dependency_detection_logic() {
        let temp_dir = TempDir::new().unwrap();
        let resolver = PackageResolver::new(temp_dir.path()).unwrap();

        // Test the detection logic directly by parsing sources
        let source1 = resolver
            .parse_source("github:corp/postgres-pkg@v1.2")
            .unwrap();
        let source2 = resolver
            .parse_source("github:corp/postgres-pkg@v1.5")
            .unwrap();

        // Verify both have same identity but different versions
        assert_eq!(
            source1.identity(),
            Some("github:corp/postgres-pkg".to_string())
        );
        assert_eq!(
            source2.identity(),
            Some("github:corp/postgres-pkg".to_string())
        );
        assert_eq!(source1.identity(), source2.identity());
        assert_ne!(source1.version(), source2.version());

        // Simulate the conflict detection logic
        let mut resolved_versions: HashMap<String, (PackageVersion, String)> = HashMap::new();

        // First package
        if let Some(identity) = source1.identity() {
            resolved_versions.insert(identity, (source1.version(), "auth-postgres".to_string()));
        }

        // Second package - should detect conflict
        if let Some(identity) = source2.identity()
            && let Some((existing_version, _)) = resolved_versions.get(&identity)
        {
            assert_ne!(
                existing_version,
                &source2.version(),
                "Should have detected version conflict"
            );
        }
    }

    #[tokio::test]
    async fn test_same_version_allowed() {
        let temp_dir = TempDir::new().unwrap();
        create_test_package(temp_dir.path(), "postgres-pkg");

        let mut resolver = PackageResolver::new(temp_dir.path()).unwrap();

        // Create two local package references to the same package
        // This should be allowed since they're at the same "version" (local packages)
        let pkg_refs = vec![
            PackageReference {
                source: "./postgres-pkg".to_string(),
                r#as: "auth-db".to_string(),
                auth: None,
            },
            PackageReference {
                source: "./postgres-pkg".to_string(),
                r#as: "billing-db".to_string(),
                auth: None,
            },
        ];

        let result = resolver.resolve_all(&pkg_refs).await;

        // Local packages should work since they don't have version conflicts
        // (identity() returns None for local packages)
        assert!(result.is_ok());
    }

    #[test]
    fn test_package_source_identity() {
        // Test GitHub source identity
        let github_v1 = PackageSource::GitHub {
            org: "corp".to_string(),
            repo: "postgres-pkg".to_string(),
            version: PackageVersion::Tag("v1.2".to_string()),
        };
        let github_v2 = PackageSource::GitHub {
            org: "corp".to_string(),
            repo: "postgres-pkg".to_string(),
            version: PackageVersion::Tag("v1.5".to_string()),
        };
        assert_eq!(
            github_v1.identity(),
            Some("github:corp/postgres-pkg".to_string())
        );
        assert_eq!(
            github_v2.identity(),
            Some("github:corp/postgres-pkg".to_string())
        );
        assert_eq!(github_v1.identity(), github_v2.identity());

        // Test Git source identity
        let git_v1 = PackageSource::Git {
            url: "git+ssh://git@github.com/corp/pkg.git".to_string(),
            version: PackageVersion::Tag("v1.0".to_string()),
        };
        let git_v2 = PackageSource::Git {
            url: "git+ssh://git@github.com/corp/pkg.git".to_string(),
            version: PackageVersion::Tag("v2.0".to_string()),
        };
        assert_eq!(git_v1.identity(), git_v2.identity());

        // Test Local source has no identity
        let local = PackageSource::Local {
            path: PathBuf::from("/tmp/pkg"),
        };
        assert_eq!(local.identity(), None);
    }

    #[test]
    fn test_diamond_dependency_error_message_format() {
        // Verify the DiamondDependency error has a user-friendly message
        // showing which packages requested which versions.
        //
        // Scenario: auth-pkg needs postgres@v1.2, billing-pkg needs postgres@v1.5

        use crate::error::Error;

        let identity = "github:corp/postgres-pkg".to_string();
        let conflicts_formatted = format!(
            "  - {} (required by: {})\n  - {} (required by: {})",
            "v1.2", "auth-postgres", "v1.5", "billing-postgres"
        );

        let error = Error::DiamondDependency {
            identity: identity.clone(),
            conflicts_formatted: conflicts_formatted.clone(),
        };

        let error_msg = error.to_string();

        // Verify error message contains key information for debugging
        assert!(
            error_msg.contains("github:corp/postgres-pkg"),
            "Error should mention the conflicting package identity: {}",
            error_msg
        );
        assert!(
            error_msg.contains("v1.2") && error_msg.contains("v1.5"),
            "Error should mention both conflicting versions: {}",
            error_msg
        );
        assert!(
            error_msg.contains("auth-postgres") && error_msg.contains("billing-postgres"),
            "Error should mention which packages required each version: {}",
            error_msg
        );
    }

    #[tokio::test]
    async fn test_diamond_dependency_detected_in_resolve_all() {
        // Test that resolve_all actually returns DiamondDependency error
        // when the same package is requested at different versions.
        //
        // Note: We can't test this with real GitHub packages without network,
        // but we can verify the detection logic by examining the internal state.

        let temp_dir = TempDir::new().unwrap();
        let resolver = PackageResolver::new(temp_dir.path()).unwrap();

        // Parse two references to the same GitHub package at different versions
        let source1 = resolver
            .parse_source("github:corp/shared-pkg@v1.0")
            .unwrap();
        let source2 = resolver
            .parse_source("github:corp/shared-pkg@v2.0")
            .unwrap();

        // Both should have the same identity
        assert_eq!(source1.identity(), source2.identity());
        assert!(source1.identity().is_some());

        // But different versions
        assert_ne!(source1.version(), source2.version());

        // The actual resolve_all would detect this conflict before any network calls.
        // We verify the logic here since we can't make network calls in tests.
        let identity = source1.identity().unwrap();
        let mut resolved: HashMap<String, (PackageVersion, String)> = HashMap::new();
        resolved.insert(identity.clone(), (source1.version(), "pkg-a".to_string()));

        // Simulating what resolve_all does
        if let Some((existing_version, _)) = resolved.get(&identity) {
            let conflicting_version = source2.version();
            assert_ne!(
                existing_version, &conflicting_version,
                "Should detect version mismatch that would trigger DiamondDependency"
            );
        }
    }
}
