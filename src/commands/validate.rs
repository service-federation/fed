use crate::output::UserOutput;
use fed::Parser as ConfigParser;
use std::path::PathBuf;

pub fn run_validate(config_path: Option<PathBuf>, out: &dyn UserOutput) -> anyhow::Result<()> {
    let parser = ConfigParser::new();
    let config_path = if let Some(path) = config_path {
        path
    } else {
        match parser.find_config_file() {
            Ok(path) => path,
            Err(_) => {
                anyhow::bail!(
                    "No configuration file found.\n\nSearched for fed.yaml (and service-federation.yaml) in:\n  - Current directory: {}\n  - Parent directories up to root\n\nRun 'fed init' to create a starter fed.yaml.",
                    std::env::current_dir()?.display()
                );
            }
        }
    };

    out.status(&format!("Validating {}...", config_path.display()));

    // On failure, return the error and let main print it once (with hints).
    let config = parser.load_config(&config_path)?;

    // Surface typo'd keys before hard validation, so a typo that also breaks validation still
    // gets its "did you mean?" hint — not just the downstream validation error.
    crate::commands::emit_config_warnings(&config, out);

    config.validate()?;

    out.success("Configuration is valid\n");

    // Show summary
    out.status(&format!("Services: {}", config.services.len()));
    for (name, service) in &config.services {
        let service_type = if service.process.is_some() {
            "process"
        } else if service.image.is_some() {
            "docker"
        } else if service.compose_file.is_some() {
            "docker-compose"
        } else if service.gradle_task.is_some() {
            "gradle"
        } else {
            "unknown"
        };
        out.status(&format!("  - {} ({})", name, service_type));
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
