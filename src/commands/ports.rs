use crate::cli::{IsolateCommands, PortsCommands};
use crate::output::UserOutput;
use fed::service::Status;
use fed::state::StateTracker;
use fed::Parser as ConfigParser;
use std::path::PathBuf;

pub async fn run_ports(
    cmd: &PortsCommands,
    workdir: Option<PathBuf>,
    config_path: Option<PathBuf>,
    out: &dyn UserOutput,
) -> anyhow::Result<()> {
    match cmd {
        PortsCommands::List { json } => {
            let work_dir = resolve_work_dir(workdir, config_path.as_deref())?;
            list_ports(&work_dir, *json, out).await
        }
        // The randomize/reset commands are deprecated aliases. They delegate to
        // the isolate commands rather than reimplementing port persistence —
        // standalone randomization has no isolation scope to persist into and
        // would leak random ports into the non-isolated start path.
        PortsCommands::Randomize { force } => {
            eprintln!(
                "Warning: `fed ports randomize` is deprecated. Use `fed isolate enable` instead."
            );
            super::isolate::run_isolate(
                &IsolateCommands::Enable { force: *force },
                workdir,
                config_path,
                out,
            )
            .await
        }
        PortsCommands::Reset { force } => {
            eprintln!(
                "Warning: `fed ports reset` is deprecated. Use `fed isolate disable` instead."
            );
            super::isolate::run_isolate(
                &IsolateCommands::Disable { force: *force },
                workdir,
                config_path,
                out,
            )
            .await
        }
    }
}

pub(super) fn resolve_work_dir(
    workdir: Option<PathBuf>,
    config_path: Option<&std::path::Path>,
) -> anyhow::Result<PathBuf> {
    if let Some(w) = workdir {
        return Ok(w);
    }
    // Try to derive from config path
    let parser = ConfigParser::new();
    let resolved_config = if let Some(path) = config_path {
        path.to_path_buf()
    } else {
        parser.find_config_file()?
    };
    if let Some(parent) = resolved_config.parent() {
        if parent.as_os_str().is_empty() {
            Ok(std::env::current_dir()?)
        } else {
            Ok(parent.to_path_buf())
        }
    } else {
        Ok(std::env::current_dir()?)
    }
}

async fn list_ports(
    work_dir: &std::path::Path,
    json: bool,
    out: &dyn UserOutput,
) -> anyhow::Result<()> {
    let tracker = StateTracker::new(work_dir.to_path_buf()).await?;
    // Show the allocations for the currently-active scope: the isolation session
    // if enabled, otherwise the shared (non-isolated) scope.
    let (enabled, isolation_id) = tracker.get_isolation_mode().await;
    let scope = if enabled {
        isolation_id.as_deref()
    } else {
        None
    };
    let ports = tracker.get_global_port_allocations(scope).await;

    if json {
        out.status(&serde_json::to_string_pretty(&ports)?);
    } else {
        out.status("\nPort Allocations");
        out.status("================\n");

        if ports.is_empty() {
            out.status("No ports are currently allocated.");
            out.status("Ports are allocated on `fed start` or `fed isolate enable`.\n");
            return Ok(());
        }

        let mut sorted: Vec<_> = ports.iter().collect();
        sorted.sort_by_key(|(_, port)| *port);

        for (param, port) in &sorted {
            out.status(&format!("  {:>5}  {}", port, param));
        }
        out.blank();
    }

    Ok(())
}

/// Ensure no services are running. With --force, auto-stop them.
/// Without --force, prompt the user.
pub(super) async fn ensure_services_stopped(
    work_dir: &std::path::Path,
    force: bool,
    out: &dyn UserOutput,
) -> anyhow::Result<()> {
    let tracker = StateTracker::new(work_dir.to_path_buf()).await?;
    let services = tracker.get_services().await;

    let running: Vec<_> = services
        .iter()
        .filter(|(_, state)| {
            matches!(
                state.status,
                Status::Running | Status::Healthy | Status::Starting
            )
        })
        .map(|(name, _)| name.clone())
        .collect();

    if running.is_empty() {
        return Ok(());
    }

    if force {
        out.status(&format!("Stopping {} running service(s)...", running.len()));
        // Use the state-tracker-based stop (no config needed)
        super::run_stop_from_state(work_dir, vec![], out).await?;
    } else {
        out.status(&format!(
            "The following services are running: {}",
            running.join(", ")
        ));
        out.progress("Stop them to continue? [y/N] ");

        let mut input = String::new();
        std::io::stdin().read_line(&mut input)?;
        if !input.trim().eq_ignore_ascii_case("y") {
            anyhow::bail!("Aborted: services must be stopped first");
        }

        super::run_stop_from_state(work_dir, vec![], out).await?;
    }

    Ok(())
}
