use crate::output::UserOutput;
use fed::{Orchestrator, StartOutcome, config::Config};
use std::path::Path;

/// Print the post-restart summary: reserve the unconditional success line for
/// fully healthy restarts, list healthcheck timeouts otherwise (non-fatal —
/// the processes are up, so the command still exits 0).
fn report_restart_outcome(outcome: &StartOutcome, all: bool, out: &dyn UserOutput) {
    let warnings: Vec<_> = outcome.warnings().collect();
    if warnings.is_empty() {
        if all {
            out.success("All services restarted successfully!");
        } else {
            out.success("Services restarted successfully!");
        }
    } else {
        out.warning(&format!(
            "Services restarted with {} health warning(s):",
            warnings.len()
        ));
        for (name, health) in warnings {
            if let Some(text) = health.warning_text() {
                out.warning(&format!("  - {}: {}", name, text));
            }
        }
    }
}

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
        let outcome = orchestrator.restart_all().await?;
        report_restart_outcome(&outcome, true, out);
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
        let mut outcome = StartOutcome::default();

        for service in &services_to_restart {
            out.progress(&format!("  Restarting {}...", service));
            match orchestrator.restart(service).await {
                Ok(service_outcome) => {
                    let suffix = match service_outcome.get(service) {
                        Some(fed::StartHealth::TimedOut { timeout }) => {
                            format!(" done (healthcheck timed out after {:?})", timeout)
                        }
                        Some(fed::StartHealth::CheckerInvalid { .. }) => {
                            " done (healthcheck invalid, never run)".to_string()
                        }
                        _ => " done".to_string(),
                    };
                    outcome.merge(service_outcome);
                    out.finish_progress(&suffix);
                }
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
        report_restart_outcome(&outcome, false, out);
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
