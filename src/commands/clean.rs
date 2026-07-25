use crate::output::UserOutput;
use fed::{Orchestrator, config::Config};

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
        if !cleaning_all {
            return Ok(());
        }
        // A full `fed clean` still resets persisted ports and install markers
        // below — an install-only project has markers to clear even when no
        // service has a `clean:` hook or volumes.
    } else {
        out.status(&format!(
            "Running clean for services: {}",
            services_to_clean.join(", ")
        ));
    }

    // One service's clean command failing must not abandon the rest of the
    // cleanup. `fed clean` is what AGENTS.md tells you to run before removing a
    // worktree, and a fresh worktree is exactly where a command like
    // `rm -r node_modules` hits an already-absent path — aborting there would
    // leave every later service's containers and volumes behind. Failures are
    // collected, reported together at the end, and still exit non-zero.
    let mut failures: Vec<String> = Vec::new();

    for service in &services_to_clean {
        out.status(&format!("\n[clean] {}", service));
        if let Err(e) = orchestrator.run_clean(service).await {
            out.error(&format!("[clean] {} failed: {}", service, e));
            failures.push(service.clone());
        }
    }

    // When cleaning all services, also clear persisted port allocations
    // (from `fed ports randomize`). Partial cleans leave port state intact.
    if cleaning_all {
        if let Err(e) = orchestrator
            .state_tracker
            .write()
            .await
            .clear_port_resolutions()
            .await
        {
            out.error(&format!(
                "Failed to clear persisted port allocations: {}",
                e
            ));
            failures.push("port allocations".to_string());
        }

        // Per-service clean already cleared install markers for each service
        // in `services_to_clean` (those with `clean:` or `volumes:`). But a
        // service that only has `install:` isn't in that list — yet a full
        // `fed clean` is the "wipe everything for a fresh start" operation, so
        // clear all install markers in the workspace. (migrate has no marker in
        // fed 6.0 — it re-runs on every start — so there's nothing to clear.)
        let work_dir = orchestrator.work_dir();
        let markers = fed::markers::LifecycleMarkers::new(work_dir.to_path_buf(), None);
        if let Err(e) = markers.clear_all_installed() {
            out.warning(&format!(
                "Warning: Failed to clear all install markers: {}",
                e
            ));
        }
    }

    if !failures.is_empty() {
        // main prints the returned error once; keep it to the summary line.
        anyhow::bail!(
            "clean failed for: {} — everything else was still cleaned",
            failures.join(", ")
        );
    }

    out.success("\nAll clean commands completed successfully.");

    Ok(())
}
