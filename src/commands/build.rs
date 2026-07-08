use crate::output::UserOutput;
use fed::{
    config::{BuildConfig, Config, DockerBuildResult},
    Orchestrator,
};

pub async fn run_build(
    orchestrator: &Orchestrator,
    config: &Config,
    services: Vec<String>,
    tag: Option<String>,
    cli_build_args: Vec<String>,
    json: bool,
    out: &dyn UserOutput,
) -> anyhow::Result<()> {
    let services_to_build = if services.is_empty() {
        config
            .services
            .iter()
            .filter(|(_, svc)| svc.build.is_some())
            .map(|(name, _)| name.clone())
            .collect::<Vec<_>>()
    } else {
        // Expand tag references (e.g., @backend) into service names
        config.expand_service_selection(&services)
    };

    if services_to_build.is_empty() {
        out.status("No services with build field found");
        return Ok(());
    }

    // Check for Docker builds that need a tag — warn if tree is dirty
    let has_docker_builds = services_to_build.iter().any(|name| {
        config
            .services
            .get(name)
            .and_then(|s| s.build.as_ref())
            .map(|b| matches!(b, BuildConfig::DockerBuild(_)))
            .unwrap_or(false)
    });

    if has_docker_builds && tag.is_none() {
        super::docker::warn_if_dirty_tree();
    }

    out.status(&format!(
        "Running build for services: {}",
        services_to_build.join(", ")
    ));

    let mut results: Vec<DockerBuildResult> = Vec::new();

    for service in &services_to_build {
        out.status(&format!("\n[build] {}", service));
        match orchestrator
            .run_build(service, tag.as_deref(), &cli_build_args)
            .await
        {
            Ok(Some(result)) => {
                out.status(&format!(
                    "[build] {} -> {}:{}",
                    service, result.image, result.tag
                ));
                results.push(result);
            }
            Ok(None) => {}
            Err(e) => {
                // Name the failing service here; main prints the error once.
                out.error(&format!("[build] {} failed", service));
                return Err(e.into());
            }
        }
    }

    out.success("\nAll builds completed successfully.");

    if json && !results.is_empty() {
        out.status(&serde_json::to_string_pretty(&results)?);
    }

    Ok(())
}
