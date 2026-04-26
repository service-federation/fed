use crate::output::UserOutput;
use fed::{config::Config, Orchestrator};

pub async fn run_clean(
    orchestrator: &Orchestrator,
    config: &Config,
    services: Vec<String>,
    out: &dyn UserOutput,
) -> anyhow::Result<()> {
    let cleaning_all = services.is_empty();

    // When cleaning all, first remove any orphaned containers and processes
    // (from failed starts, crashes, etc.) so volumes/ports can be freed
    if cleaning_all {
        match orchestrator.remove_orphaned_containers().await {
            Ok(count) if count > 0 => {
                out.status(&format!("Removed {} orphaned container(s)", count));
            }
            Ok(_) => {}
            Err(e) => {
                out.warning(&format!(
                    "Warning: Failed to clean orphaned containers: {}",
                    e
                ));
            }
        }

        let process_count = orchestrator.remove_orphaned_processes().await;
        if process_count > 0 {
            out.status(&format!("Killed {} orphaned process(es)", process_count));
        }
    }

    let services_to_clean = if cleaning_all {
        // Include services that have either a clean command or Docker volumes
        config
            .services
            .iter()
            .filter(|(_, svc)| svc.clean.is_some() || !svc.volumes.is_empty())
            .map(|(name, _)| name.clone())
            .collect::<Vec<_>>()
    } else {
        // Expand tag references (e.g., @backend) into service names
        config.expand_service_selection(&services)
    };

    if services_to_clean.is_empty() {
        out.status("No services with clean field or Docker volumes found");
        return Ok(());
    }

    out.status(&format!(
        "Running clean for services: {}",
        services_to_clean.join(", ")
    ));

    for service in &services_to_clean {
        out.status(&format!("\n[clean] {}", service));
        if let Err(e) = orchestrator.run_clean(service).await {
            out.status(&format!("[clean] {} failed: {}", service, e));
            return Err(e.into());
        }
    }

    // When cleaning all services, also clear persisted port allocations
    // (from `fed ports randomize`). Partial cleans leave port state intact.
    if cleaning_all {
        orchestrator
            .state_tracker
            .write()
            .await
            .clear_port_resolutions()
            .await?;

        // Per-service clean already cleared install/migrate markers for
        // each service in `services_to_clean` (those with `clean:` or
        // `volumes:`). But services that only have `migrate:` (e.g. a
        // Rust service whose migrate creates a database in a sibling
        // postgres) aren't in that list — yet their migrations are
        // effectively invalidated when the dependency's volumes are
        // wiped. A full `fed clean` is the "wipe everything for a
        // fresh start" operation, so clear all install and migrate
        // markers in the workspace.
        let work_dir = orchestrator.work_dir();
        if let Err(e) = fed::markers::clear_all_migrated_global(work_dir) {
            out.warning(&format!(
                "Warning: Failed to clear all migrate markers: {}",
                e
            ));
        }
        if let Err(e) = fed::markers::clear_all_installed_global(work_dir) {
            out.warning(&format!(
                "Warning: Failed to clear all install markers: {}",
                e
            ));
        }
    }

    out.success("\nAll clean commands completed successfully.");

    Ok(())
}
