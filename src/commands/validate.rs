use crate::output::UserOutput;
use fed::Parser as ConfigParser;
use std::path::PathBuf;

/// Report what the `defaults:` block did, once for the whole config.
///
/// The `depends_on` skip is the part worth stating out loud: a reader who
/// sees `defaults.depends_on: [storage, cache]` reasonably expects it on
/// every service, and would otherwise have to infer the exception from a
/// dependency graph that quietly omits it.
fn report_defaults(config: &fed::Config, out: &dyn UserOutput) {
    if config.defaults.is_none() {
        return;
    }

    let infra: Vec<_> = fed::config::defaults::excluded_services(config)
        .into_iter()
        .collect();

    out.status(&format!(
        "Defaults applied to {} service(s).",
        config.services.len() - infra.len()
    ));
    if !infra.is_empty() {
        out.status(&format!(
            "  {} named in defaults.depends_on, so {} did not inherit defaults.",
            infra.join(", "),
            if infra.len() == 1 { "it" } else { "they" }
        ));
    }

    let mut opted_out: Vec<String> = config
        .services
        .iter()
        .flat_map(|(name, service)| {
            service
                .explicit_empty
                .iter()
                .map(move |key| format!("{name}.{key}"))
        })
        .collect();
    opted_out.sort();
    if !opted_out.is_empty() {
        out.status(&format!(
            "  Opted out of a default by writing it empty: {}",
            opted_out.join(", ")
        ));
    }
    out.status("");
}

pub async fn run_validate(
    config_path: Option<PathBuf>,
    workdir: Option<PathBuf>,
    offline: bool,
    cli_variants: &[String],
    profiles: &[String],
    out: &dyn UserOutput,
) -> anyhow::Result<()> {
    let parser = ConfigParser::new();
    let config_path = if let Some(path) = config_path {
        path
    } else {
        match workdir
            .as_deref()
            .map(ConfigParser::find_config_in_dir)
            .unwrap_or_else(|| parser.find_config_file())
        {
            Ok(path) => path,
            Err(_) => {
                anyhow::bail!(
                    "No configuration file found.\n\nSearched for fed.yaml in:\n  - Current directory: {}\n  - Parent directories up to root\n\nRun 'fed init' to create a starter fed.yaml.",
                    workdir
                        .as_deref()
                        .unwrap_or(&std::env::current_dir()?)
                        .display()
                );
            }
        }
    };

    out.status(&format!("Validating {}...", config_path.display()));

    // Resolve packages the same way `fed start` does — validating the
    // pre-merge config rejects `extends: "pkg.service"` services that start
    // accepts. On failure, return the error and let main print it once.
    let mut config = parser
        .load_config_with_packages_offline(&config_path, offline)
        .await?;

    // Surface typo'd keys before hard validation, so a typo that also breaks validation still
    // gets its "did you mean?" hint — not just the downstream validation error.
    crate::commands::emit_config_warnings(&config, out);

    config.validate()?;

    // Resolve variants so the summary below reports the service that would
    // actually start, not the abstract contract with no type of its own.
    // Validation runs first: the variant rules are validation's job, and
    // resolution assumes they already hold.
    let work_dir = super::ports::resolve_work_dir(workdir, Some(&config_path))?;
    fed::config::variants::resolve_variants(
        &mut config,
        &fed::config::variants::VariantSelection::load(cli_variants, &work_dir)?,
        profiles,
    )?;

    out.success("Configuration is valid\n");

    report_defaults(&config, out);

    // Show summary
    out.status(&format!("Services: {}", config.services.len()));
    let mut services: Vec<_> = config.services.iter().collect();
    services.sort_by(|a, b| a.0.cmp(b.0));
    for (name, service) in services {
        let service_type = if service.process.is_some() {
            "process"
        } else if service.image.is_some() {
            "docker"
        } else if service.compose_file.is_some() {
            "docker-compose"
        } else if service.gradle_task.is_some() {
            "gradle"
        } else if service.install.is_some() || service.migrate.is_some() {
            "hook-only"
        } else {
            "unknown"
        };
        match &service.variant {
            Some(variant) => out.status(&format!(
                "  - {} ({}, variant: {})",
                name, service_type, variant
            )),
            None => out.status(&format!("  - {} ({})", name, service_type)),
        }
    }

    if !config.parameters.is_empty() {
        out.status(&format!("\nParameters: {}", config.parameters.len()));
        for (name, param) in &config.parameters {
            if let Some(param_type) = &param.param_type {
                out.status(&format!("  - {} (type: {})", name, param_type));
            } else {
                out.status(&format!("  - {} (string)", name));
            }
        }
    }

    if let Some(ref ep) = config.entrypoint {
        out.status(&format!("\nEntrypoint: {}", ep));
    } else if !config.entrypoints.is_empty() {
        out.status(&format!("\nEntrypoints: {}", config.entrypoints.join(", ")));
    }

    Ok(())
}
