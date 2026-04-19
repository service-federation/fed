use crate::output::UserOutput;
use fed::{
    config::{BuildConfig, Config, DockerBuildResult},
    docker::DockerClient,
    orchestrator::ServiceLifecycleCommands,
};
use std::path::Path;

/// Warn if the git working tree has uncommitted or staged changes.
///
/// This is relevant for Docker builds that default-tag with the git short hash,
/// since the tag won't reflect the actual file contents if the tree is dirty.
pub fn warn_if_dirty_tree() {
    if let Ok(output) = std::process::Command::new("git")
        .args(["diff", "--quiet"])
        .output()
    {
        if !output.status.success() {
            eprintln!("Warning: Git working tree has uncommitted changes. Docker images will be tagged with the current commit hash, which may not reflect the actual contents.");
        }
    }
    if let Ok(output) = std::process::Command::new("git")
        .args(["diff", "--quiet", "--cached"])
        .output()
    {
        if !output.status.success() {
            eprintln!("Warning: Git index has staged but uncommitted changes. Docker images will be tagged with the current commit hash, which may not reflect the actual contents.");
        }
    }
}

/// Filter services to only those with `BuildConfig::DockerBuild`.
fn docker_build_services(config: &Config, requested: &[String]) -> Vec<String> {
    if requested.is_empty() {
        config
            .services
            .iter()
            .filter(|(_, svc)| matches!(svc.build.as_ref(), Some(BuildConfig::DockerBuild(_))))
            .map(|(name, _)| name.clone())
            .collect()
    } else {
        requested
            .iter()
            .filter(|name| {
                config
                    .services
                    .get(name.as_str())
                    .and_then(|s| s.build.as_ref())
                    .map(|b| matches!(b, BuildConfig::DockerBuild(_)))
                    .unwrap_or(false)
            })
            .cloned()
            .collect()
    }
}

/// Resolve the image tag: CLI --tag takes precedence, otherwise git short hash.
async fn resolve_tag(tag: Option<String>, work_dir: &Path) -> anyhow::Result<String> {
    if let Some(t) = tag {
        return Ok(t);
    }
    let output = tokio::process::Command::new("git")
        .args(["rev-parse", "--short", "HEAD"])
        .current_dir(work_dir)
        .output()
        .await?;
    if !output.status.success() {
        anyhow::bail!(
            "Failed to get git hash for Docker tag. Use --tag to specify a tag manually."
        );
    }
    Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
}

pub async fn run_docker_build(
    config: &Config,
    work_dir: &Path,
    services: Vec<String>,
    tag: Option<String>,
    build_args: Vec<String>,
    json: bool,
    out: &dyn UserOutput,
) -> anyhow::Result<()> {
    let services_to_build = docker_build_services(config, &services);

    if services_to_build.is_empty() {
        if services.is_empty() {
            out.status("No services with Docker build configuration found");
        } else {
            out.status(&format!(
                "None of the specified services have Docker build configuration: {}",
                services.join(", ")
            ));
        }
        return Ok(());
    }

    // Warn about dirty tree when using git hash as tag
    if tag.is_none() {
        warn_if_dirty_tree();
    }

    if !json {
        out.status(&format!(
            "Building Docker images for: {}",
            services_to_build.join(", ")
        ));
    }

    // Docker build does not read or mutate lifecycle markers, so the
    // isolation scope passed here is irrelevant; use the shared scope.
    let lifecycle = ServiceLifecycleCommands::new(config, work_dir, None);
    let mut results: Vec<DockerBuildResult> = Vec::new();

    for service in &services_to_build {
        if !json {
            out.status(&format!("\n[docker build] {}", service));
        }
        match lifecycle
            .run_build(service, tag.as_deref(), &build_args)
            .await
        {
            Ok(Some(result)) => {
                if !json {
                    out.status(&format!(
                        "[docker build] {} -> {}:{}",
                        service, result.image, result.tag
                    ));
                }
                results.push(result);
            }
            Ok(None) => {
                // Shouldn't happen since we filtered to DockerBuild only
            }
            Err(e) => {
                if !json {
                    out.status(&format!("[docker build] {} failed: {}", service, e));
                }
                return Err(e.into());
            }
        }
    }

    if json {
        out.status(&serde_json::to_string_pretty(&results)?);
    } else {
        out.success("\nAll Docker image builds completed successfully.");
    }

    Ok(())
}

pub async fn run_docker_push(
    config: &Config,
    work_dir: &Path,
    services: Vec<String>,
    tag: Option<String>,
    out: &dyn UserOutput,
) -> anyhow::Result<()> {
    let services_to_push = docker_build_services(config, &services);

    if services_to_push.is_empty() {
        if services.is_empty() {
            out.status("No services with Docker build configuration found");
        } else {
            out.status(&format!(
                "None of the specified services have Docker build configuration: {}",
                services.join(", ")
            ));
        }
        return Ok(());
    }

    let resolved_tag = resolve_tag(tag, work_dir).await?;

    out.status(&format!(
        "Pushing Docker images for: {}",
        services_to_push.join(", ")
    ));

    for service_name in &services_to_push {
        let svc = config.services.get(service_name.as_str()).unwrap();
        let docker_config = match svc.build.as_ref() {
            Some(BuildConfig::DockerBuild(cfg)) => cfg,
            _ => continue,
        };

        let full_image = format!("{}:{}", docker_config.image, resolved_tag);

        let docker = DockerClient::new();

        // Check that image exists locally
        let exists = docker
            .image_exists_checked(&full_image)
            .await
            .map_err(|e| anyhow::anyhow!("{}", e))?;

        if !exists {
            anyhow::bail!(
                "Image '{}' not found locally. Run `fed docker build` first.",
                full_image
            );
        }

        out.status(&format!(
            "\n[docker push] {} -> {}",
            service_name, full_image
        ));

        docker
            .push(&full_image)
            .await
            .map_err(|e| anyhow::anyhow!("Docker push failed for '{}': {}", full_image, e))?;
    }

    out.success("\nAll Docker images pushed successfully.");
    Ok(())
}
