use crate::output::UserOutput;
use fed::{Orchestrator, config::Config};

pub async fn run_restart(
    orchestrator: &mut Orchestrator,
    config: &Config,
    services: Vec<String>,
    out: &dyn UserOutput,
) -> anyhow::Result<()> {
    if services.is_empty() {
        out.status("Restarting all services in dependency-aware order...");
        orchestrator.restart_all().await?;
        out.success("All services restarted successfully!");
    } else {
        // Expand tag references (e.g., @backend) into service names
        let services_to_restart = config.expand_service_selection(&services);

        out.status(&format!(
            "Restarting services: {}",
            services_to_restart.join(", ")
        ));

        // Defer to `orchestrator.restart`, which atomically stops + starts each
        // service AND brings its dependents back (a plain stop/start pair would
        // leave them stranded — see Orchestrator::restart docs).
        let mut errors: Vec<(String, String)> = Vec::new();

        for service in &services_to_restart {
            out.progress(&format!("  Restarting {}...", service));
            match orchestrator.restart(service).await {
                Ok(_) => out.finish_progress(" done"),
                Err(e) => {
                    out.finish_progress(&format!(" failed ({})", e));
                    errors.push((service.clone(), e.to_string()));
                }
            }
        }

        if !errors.is_empty() {
            out.blank();
            out.status("Failed to restart:");
            for (service, error) in &errors {
                out.status(&format!("  - {}: {}", service, error));
            }

            return Err(anyhow::anyhow!(
                "{} service(s) failed to restart",
                errors.len()
            ));
        }

        out.blank();
        out.success("Services restarted successfully!");
    }

    Ok(())
}
