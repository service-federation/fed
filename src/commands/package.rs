use crate::cli::PackageCommands;
use crate::output::UserOutput;
use anyhow::Result;
use fed::package::PackageResolver;
use std::path::PathBuf;

/// Run package commands
pub async fn run_package(cmd: &PackageCommands, out: &dyn UserOutput) -> Result<()> {
    match cmd {
        PackageCommands::List { json } => list_cached_packages(*json, out).await,
        PackageCommands::Refresh { package } => refresh_package(package.as_deref(), out).await,
        PackageCommands::Clear { force } => clear_cache(*force, out).await,
    }
}

/// List all cached packages
async fn list_cached_packages(json: bool, out: &dyn UserOutput) -> Result<()> {
    let cache_dir = PackageResolver::get_cache_dir_static()?;

    if !cache_dir.exists() {
        if json {
            out.status("{\"packages\":[]}");
        } else {
            out.status("No packages cached.");
        }
        return Ok(());
    }

    let mut packages = Vec::new();

    // Check github cache
    let github_cache = cache_dir.join("github");
    if github_cache.exists() {
        for org_entry in std::fs::read_dir(&github_cache)? {
            let org_entry = org_entry?;
            if org_entry.file_type()?.is_dir() {
                let org_name = org_entry.file_name().to_string_lossy().to_string();
                for repo_entry in std::fs::read_dir(org_entry.path())? {
                    let repo_entry = repo_entry?;
                    if repo_entry.file_type()?.is_dir() {
                        let repo_name = repo_entry.file_name().to_string_lossy().to_string();
                        for version_entry in std::fs::read_dir(repo_entry.path())? {
                            let version_entry = version_entry?;
                            if version_entry.file_type()?.is_dir() {
                                let version =
                                    version_entry.file_name().to_string_lossy().to_string();
                                packages.push(CachedPackage {
                                    source: format!("github:{}/{}", org_name, repo_name),
                                    version,
                                    path: version_entry.path(),
                                });
                            }
                        }
                    }
                }
            }
        }
    }

    // Check git cache
    let git_cache = cache_dir.join("git");
    if git_cache.exists() {
        for cache_entry in std::fs::read_dir(&git_cache)? {
            let cache_entry = cache_entry?;
            if cache_entry.file_type()?.is_dir() {
                let cache_key = cache_entry.file_name().to_string_lossy().to_string();
                for version_entry in std::fs::read_dir(cache_entry.path())? {
                    let version_entry = version_entry?;
                    if version_entry.file_type()?.is_dir() {
                        let version = version_entry.file_name().to_string_lossy().to_string();
                        packages.push(CachedPackage {
                            source: format!("git+ssh://{}", cache_key.replace('_', "/")),
                            version,
                            path: version_entry.path(),
                        });
                    }
                }
            }
        }
    }

    if json {
        let output: Vec<_> = packages
            .iter()
            .map(|p| {
                serde_json::json!({
                    "source": p.source,
                    "version": p.version,
                    "path": p.path.display().to_string(),
                })
            })
            .collect();
        out.status(&serde_json::to_string_pretty(
            &serde_json::json!({ "packages": output }),
        )?);
    } else if packages.is_empty() {
        out.status("No packages cached.");
    } else {
        out.status("Cached packages:");
        for pkg in &packages {
            out.status(&format!(
                "  {} @ {} ({})",
                pkg.source,
                pkg.version,
                pkg.path.display()
            ));
        }
        out.status(&format!("\nTotal: {} package(s)", packages.len()));
    }

    Ok(())
}

/// Refresh a specific package or all packages in current config
async fn refresh_package(package: Option<&str>, out: &dyn UserOutput) -> Result<()> {
    let cache_dir = PackageResolver::get_cache_dir_static()?;

    if let Some(source) = package {
        // Refresh specific package
        let path_to_remove = find_cached_package_path(&cache_dir, source)?;
        if let Some(path) = path_to_remove {
            out.status(&format!("Removing cached package: {}", path.display()));
            std::fs::remove_dir_all(&path)?;
            out.success(&format!(
                "Cache cleared for {}. Package will be re-fetched on next use.",
                source
            ));
        } else {
            out.status(&format!("Package {} not found in cache.", source));
        }
    } else {
        // Refresh all packages - just remove the entire cache
        // Packages will be re-fetched on next `fed start`
        if cache_dir.exists() {
            let github_cache = cache_dir.join("github");
            let git_cache = cache_dir.join("git");

            let mut count = 0;
            if github_cache.exists() {
                count += count_dirs(&github_cache);
                std::fs::remove_dir_all(&github_cache)?;
            }
            if git_cache.exists() {
                count += count_dirs(&git_cache);
                std::fs::remove_dir_all(&git_cache)?;
            }

            if count > 0 {
                out.success(&format!(
                    "Cleared {} cached package(s). Packages will be re-fetched on next use.",
                    count
                ));
            } else {
                out.status("No packages in cache.");
            }
        } else {
            out.status("No packages in cache.");
        }
    }

    Ok(())
}

/// Clear the entire package cache
async fn clear_cache(force: bool, out: &dyn UserOutput) -> Result<()> {
    let cache_dir = PackageResolver::get_cache_dir_static()?;

    if !cache_dir.exists() {
        out.status("Package cache is empty.");
        return Ok(());
    }

    if !force {
        out.progress("This will remove all cached packages. Continue? [y/N] ");

        let mut input = String::new();
        std::io::stdin().read_line(&mut input)?;

        if !input.trim().eq_ignore_ascii_case("y") {
            out.status("Aborted.");
            return Ok(());
        }
    }

    std::fs::remove_dir_all(&cache_dir)?;
    out.success("Package cache cleared.");

    Ok(())
}

/// Find the path to a cached package
fn find_cached_package_path(cache_dir: &std::path::Path, source: &str) -> Result<Option<PathBuf>> {
    if source.starts_with("github:") {
        let rest = source.strip_prefix("github:").unwrap();
        let parts: Vec<&str> = rest.split('@').collect();
        let org_repo = parts[0];
        let org_repo_parts: Vec<&str> = org_repo.split('/').collect();
        if org_repo_parts.len() == 2 {
            let path = cache_dir
                .join("github")
                .join(org_repo_parts[0])
                .join(org_repo_parts[1]);
            if path.exists() {
                return Ok(Some(path));
            }
        }
    } else if source.starts_with("git+ssh://") {
        let cache_key = source.replace("git+ssh://", "").replace(['/', ':'], "_");
        let path = cache_dir.join("git").join(&cache_key);
        if path.exists() {
            return Ok(Some(path));
        }
    }

    Ok(None)
}

/// Count directories recursively (for counting cached packages)
fn count_dirs(path: &std::path::Path) -> usize {
    let mut count = 0;
    if let Ok(entries) = std::fs::read_dir(path) {
        for entry in entries.flatten() {
            if let Ok(file_type) = entry.file_type()
                && file_type.is_dir()
            {
                count += 1;
                count += count_dirs(&entry.path());
            }
        }
    }
    count
}

#[derive(Debug)]
struct CachedPackage {
    source: String,
    version: String,
    path: PathBuf,
}
