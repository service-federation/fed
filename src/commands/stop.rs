use crate::output::UserOutput;
use fed::{Orchestrator, config::Config, service::Status, state::StateTracker};
use std::path::Path;

use super::lifecycle::{StopResult, remove_orphan_containers_for_workdir, stop_service_by_state};

pub async fn run_stop(
    orchestrator: &mut Orchestrator,
    config: &Config,
    services: Vec<String>,
    out: &dyn UserOutput,
) -> anyhow::Result<()> {
    if services.is_empty() {
        out.status("Stopping all services...");
        orchestrator.stop_all().await?;

        // If the config changed since services were started, some running services may
        // still exist in state but not in the current config (so stop_all won't see them).
        // Best-effort stop those remaining state-tracked services.
        let extra_stopped = stop_remaining_state_services(orchestrator, out).await;
        if extra_stopped > 0 {
            out.status(&format!(
                "Stopped {} additional service(s) from state",
                extra_stopped
            ));
        }

        // Also remove any orphaned containers (from failed starts, etc.)
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

        // Also remove any orphaned processes (from crashed services, etc.)
        let process_count = orchestrator.remove_orphaned_processes().await;
        if process_count > 0 {
            out.status(&format!("Killed {} orphaned process(es)", process_count));
        }

        orchestrator.cleanup().await;
    } else {
        // Expand tag references (e.g., @backend) into service names
        let services_to_stop = config.expand_service_selection(&services);

        // Reject unknown names up front — a typo'd name must not report success.
        // Services running from an older config live in state, so accept those too.
        let state_services = orchestrator.state_tracker.read().await.get_services().await;
        let unknown: Vec<&String> = services_to_stop
            .iter()
            .filter(|name| {
                !config.services.contains_key(*name) && !state_services.contains_key(*name)
            })
            .collect();
        if let Some(name) = unknown.first() {
            let mut msg = super::suggest::with_did_you_mean(
                &format!("Service '{}' not found.", name),
                name,
                config.services.keys().map(String::as_str),
            );
            msg.push_str("\n\nConfigured services:");
            let mut names: Vec<_> = config.services.keys().collect();
            names.sort();
            for n in names {
                msg.push_str(&format!("\n  - {}", n));
            }
            anyhow::bail!(msg);
        }

        let mut failures: Vec<(String, String)> = Vec::new();
        for service in services_to_stop {
            out.progress(&format!("  Stopping {}...", service));
            match orchestrator.stop(&service).await {
                Ok(_) => out.finish_progress(" done"),
                Err(e) => {
                    out.finish_progress(&format!(" failed ({})", e));
                    failures.push((service, e.to_string()));
                }
            }
        }
        orchestrator.state_tracker.write().await.save().await?;

        if !failures.is_empty() {
            let list = failures
                .iter()
                .map(|(name, err)| format!("  - {}: {}", name, err))
                .collect::<Vec<_>>()
                .join("\n");
            anyhow::bail!("Failed to stop {} service(s):\n{}", failures.len(), list);
        }
    }
    out.success("Services stopped");

    Ok(())
}

pub(crate) fn state_status_is_active(status: Status) -> bool {
    matches!(
        status,
        Status::Running | Status::Healthy | Status::Starting | Status::Failing | Status::Stopping
    )
}

/// Stop any services that remain in state after config-based stop operations.
///
/// This catches cases where services are running but no longer appear in the
/// current config (e.g., services renamed/removed).
async fn stop_remaining_state_services(orchestrator: &Orchestrator, out: &dyn UserOutput) -> usize {
    use fed::state::SqliteStateTracker;

    // Avoid holding the outer RwLock across await by cloning the DB connection.
    let conn = orchestrator.state_tracker.read().await.clone_connection();
    let services = SqliteStateTracker::fetch_services_from_connection(&conn).await;

    if services.is_empty() {
        return 0;
    }

    let mut stopped_names: Vec<String> = Vec::new();

    for (name, state) in services {
        if !state_status_is_active(state.status) {
            continue;
        }

        // These services are not necessarily present in the current config.
        // Stop by state (PID/container) and unregister regardless of config.
        out.progress(&format!("  Stopping {} (from state)...", name));

        match stop_service_by_state(&name, &state).await {
            StopResult::Stopped => {
                out.finish_progress(" done");
                stopped_names.push(name);
            }
            StopResult::Skipped(reason) => {
                out.finish_progress(&format!(" skipped ({})", reason));
                stopped_names.push(name);
            }
            StopResult::Failed => {
                out.finish_progress(" failed");
            }
        }
    }

    if stopped_names.is_empty() {
        return 0;
    }

    let mut tracker = orchestrator.state_tracker.write().await;
    for name in &stopped_names {
        if let Err(e) = tracker.unregister_service(name).await {
            tracing::warn!("Failed to unregister service '{}' from state: {}", name, e);
        }
    }
    if let Err(e) = tracker.save().await {
        tracing::warn!("Failed to save state after stopping services: {}", e);
    }

    stopped_names.len()
}

/// Stop services using only the state tracker (no config required).
/// Used when config is invalid but we still need to stop running services.
pub async fn run_stop_from_state(
    work_dir: &Path,
    services: Vec<String>,
    out: &dyn UserOutput,
) -> anyhow::Result<()> {
    let mut tracker = StateTracker::new(work_dir.to_path_buf()).await?;
    tracker.initialize().await?;

    let all_services = tracker.get_services().await;
    if all_services.is_empty() {
        out.status("No services found in state tracker.");
        return Ok(());
    }

    let services_to_stop: Vec<_> = if services.is_empty() {
        all_services.into_iter().collect()
    } else {
        // No tag expansion in fallback mode — use names directly
        all_services
            .into_iter()
            .filter(|(name, _)| services.contains(name))
            .collect()
    };

    out.status(&format!(
        "Stopping {} service(s) from state tracker...",
        services_to_stop.len()
    ));

    let mut failed: Vec<String> = Vec::new();
    for (name, state) in &services_to_stop {
        // Failing/Stopping services still have a live process or container —
        // they must be stopped here, not skipped and then erased from state.
        if !state_status_is_active(state.status) {
            continue;
        }

        out.progress(&format!("  Stopping {}...", name));

        match stop_service_by_state(name, state).await {
            StopResult::Stopped => {
                out.finish_progress(" done");
            }
            StopResult::Skipped(reason) => {
                out.finish_progress(&format!(" skipped ({})", reason));
            }
            StopResult::Failed => {
                out.finish_progress(" failed");
                failed.push(name.clone());
            }
        }
    }

    // Update state:
    // - stop all with no failures: clear entire DB
    // - otherwise: unregister only the services we actually stopped, so a
    //   still-running process is never erased from state
    if services.is_empty() && failed.is_empty() {
        tracker.clear().await?;
    } else {
        for (name, _) in &services_to_stop {
            if failed.contains(name) {
                continue;
            }
            if let Err(e) = tracker.unregister_service(name).await {
                tracing::warn!("Failed to unregister service '{}' from state: {}", name, e);
            }
        }
        tracker.save().await?;
    }

    // Also remove any orphaned containers not in state DB
    let removed = remove_orphan_containers_for_workdir(work_dir).await;
    if removed > 0 {
        out.status(&format!("Removed {} orphaned container(s)", removed));
    }

    if !failed.is_empty() {
        anyhow::bail!(
            "Failed to stop {} service(s): {} (still tracked in state)",
            failed.len(),
            failed.join(", ")
        );
    }

    out.success("Services stopped");

    Ok(())
}
