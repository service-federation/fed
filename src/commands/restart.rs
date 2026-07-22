use crate::output::UserOutput;
use fed::{Orchestrator, config::Config};
use std::path::Path;

pub async fn run_restart(
    orchestrator: &mut Orchestrator,
    config: &Config,
    services: Vec<String>,
    config_path: &Path,
    offline: bool,
    profiles: Vec<String>,
    out: &dyn UserOutput,
) -> anyhow::Result<()> {
    let restarted_names: Vec<String> = if services.is_empty() {
        out.status("Restarting all services in dependency-aware order...");
        orchestrator.restart_all().await?;
        out.success("All services restarted successfully!");
        config.services.keys().cloned().collect()
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
        services_to_restart
    };

    // `fed restart` already mutates state and runs the full `initialize()`
    // path, so — per the scaled-back self-heal promise
    // (`07-supervisor.md`) — it also gets to respawn a dead/missing
    // supervisor, same as `fed start`. `fed status` deliberately does
    // neither.
    if super::supervise::any_has_restart_policy(config, restarted_names.iter()) {
        let work_dir = orchestrator.work_dir().to_path_buf();
        if let Err(e) =
            super::supervise::spawn_if_needed(&work_dir, config_path, offline, &profiles)
        {
            out.warning(&format!(
                "Warning: failed to start the restart-policy supervisor: {}",
                e
            ));
        }
    }

    Ok(())
}
