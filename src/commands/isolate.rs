use crate::cli::IsolateCommands;
use crate::output::UserOutput;
use fed::state::StateTracker;
use fed::{Orchestrator, Parser as ConfigParser, RunContext};
use std::path::PathBuf;

pub async fn run_isolate(
    cmd: &IsolateCommands,
    workdir: Option<PathBuf>,
    config_path: Option<PathBuf>,
    ctx: RunContext,
    out: &dyn UserOutput,
) -> anyhow::Result<()> {
    let work_dir = super::ports::resolve_work_dir(workdir, config_path.as_deref())?;

    match cmd {
        IsolateCommands::Enable { force } => {
            enable(&work_dir, config_path, &ctx, *force, out).await
        }
        IsolateCommands::Disable { force } => disable(&work_dir, *force, out).await,
        IsolateCommands::Status => status(&work_dir, out).await,
        IsolateCommands::Rotate { force } => {
            rotate(&work_dir, config_path, &ctx, *force, out).await
        }
    }
}

async fn enable(
    work_dir: &std::path::Path,
    config_path: Option<PathBuf>,
    ctx: &RunContext,
    force: bool,
    out: &dyn UserOutput,
) -> anyhow::Result<()> {
    // Check if already isolated
    let tracker = StateTracker::new(work_dir.to_path_buf()).await?;
    let (enabled, existing_id) = tracker.get_isolation_mode().await;
    if enabled {
        out.status(&format!(
            "Isolation mode is already enabled (ID: {}).",
            existing_id.as_deref().unwrap_or("unknown")
        ));
        out.status("Use `fed isolate rotate` to re-roll ports and ID.\n");
        return Ok(());
    }
    drop(tracker);

    super::ports::ensure_services_stopped(work_dir, force, out).await?;

    // Load config
    let parser = ConfigParser::new();
    let resolved_config = if let Some(path) = config_path {
        path
    } else {
        parser.find_config_file()?
    };
    let config = parser.load_config(&resolved_config)?;
    config.validate()?;

    // Generate the isolation ID up front and apply it BEFORE initialize so the
    // randomized ports persist into this isolation scope — not the shared scope
    // that non-isolated `fed start` reads from.
    let isolation_id = format!("iso-{:08x}", rand::random::<u32>());

    // Create orchestrator with randomized ports and initialize to resolve them.
    // isolation_id is applied before build()'s internal initialize() call so
    // ports resolve and persist under this new scope, not the shared one.
    let orchestrator = Orchestrator::builder()
        .config(config)
        .work_dir(work_dir.to_path_buf())
        .run_context(ctx.clone())
        .randomize_ports(true)
        .isolation_id(isolation_id.clone())
        .build()
        .await?;

    // Persist isolation mode now that ports are resolved under this scope.
    let tracker = orchestrator.state_tracker.read().await;
    tracker
        .set_isolation_mode(true, Some(isolation_id.clone()))
        .await?;

    // Markers are scoped by isolation_id, so the new session's namespace is
    // empty by construction — no clearing needed. The shared-scope markers
    // stay intact and will be reused when the user runs `fed isolate disable`.

    // Display allocated ports for this isolation scope
    let ports = tracker
        .get_global_port_allocations(Some(&isolation_id))
        .await;
    drop(tracker);

    out.status("\nIsolation mode enabled.");
    out.status(&format!("Isolation ID: {}", isolation_id));
    out.status("\nAllocated ports:");

    let mut sorted: Vec<_> = ports.iter().collect();
    sorted.sort_by_key(|(_, port)| *port);
    for (param, port) in &sorted {
        out.status(&format!("  {:>5}  {}", port, param));
    }
    out.blank();
    out.status("Services will use unique container names and these ports.");
    out.status("Use `fed isolate disable` to return to defaults.\n");

    Ok(())
}

async fn disable(
    work_dir: &std::path::Path,
    force: bool,
    out: &dyn UserOutput,
) -> anyhow::Result<()> {
    // Read current isolation state. Unlike before, "already disabled" is NOT a
    // no-op: it still clears any persisted port allocations so that disable is
    // always a reliable way back to configured ports. (This is the escape hatch
    // for projects left with stale randomized ports by older versions or the
    // deprecated `fed ports randomize`.)
    let (was_enabled, previous_isolation_id) = {
        let tracker = StateTracker::new(work_dir.to_path_buf()).await?;
        tracker.get_isolation_mode().await
    };

    super::ports::ensure_services_stopped(work_dir, force, out).await?;

    // Clear port resolutions across all scopes (shared + any isolation sessions)
    let mut tracker = StateTracker::new(work_dir.to_path_buf()).await?;
    tracker.initialize().await?;
    tracker.clear_port_resolutions().await?;

    // Clear isolation mode (no-op if it was never set)
    tracker.clear_isolation_mode().await?;

    if was_enabled {
        // Clear shared-scope install markers so install re-runs against the
        // now-active shared containers. (The shared containers may have been
        // torn down or diverged while the user was in isolation mode. migrate
        // has no marker in fed 6.0 — it re-runs on every start regardless.)
        let shared_markers = fed::markers::LifecycleMarkers::new(work_dir.to_path_buf(), None);
        shared_markers.clear_all_installed()?;

        // Also clean up the abandoned isolation scope's marker directory so it
        // doesn't linger in `~/.fed/isolated/`.
        if let Some(id) = previous_isolation_id {
            let iso_markers = fed::markers::LifecycleMarkers::new(work_dir.to_path_buf(), Some(id));
            let _ = iso_markers.clear_all_installed();
        }

        out.success(
            "Isolation mode disabled. Next `fed start` will use default ports and shared containers.\n",
        );
    } else {
        out.success(
            "Isolation already disabled; cleared persisted port allocations. Next `fed start` will use default ports.\n",
        );
    }

    Ok(())
}

async fn status(work_dir: &std::path::Path, out: &dyn UserOutput) -> anyhow::Result<()> {
    let tracker = StateTracker::new(work_dir.to_path_buf()).await?;
    let (enabled, isolation_id) = tracker.get_isolation_mode().await;

    out.status("\nIsolation Status");
    out.status("================\n");

    if enabled {
        out.status("  Mode:         enabled");
        out.status(&format!(
            "  Isolation ID: {}",
            isolation_id.as_deref().unwrap_or("unknown")
        ));

        // Show port allocations for this isolation scope
        let ports = tracker
            .get_global_port_allocations(isolation_id.as_deref())
            .await;
        if !ports.is_empty() {
            out.status("\n  Port allocations:");
            let mut sorted: Vec<_> = ports.iter().collect();
            sorted.sort_by_key(|(_, port)| *port);
            for (param, port) in &sorted {
                out.status(&format!("    {:>5}  {}", port, param));
            }
        }
    } else {
        out.status("  Mode: disabled");
        out.status("\n  Using default ports and shared container names.");
        out.status("  Run `fed isolate enable` to activate isolation.");
    }

    out.blank();

    Ok(())
}

async fn rotate(
    work_dir: &std::path::Path,
    config_path: Option<PathBuf>,
    ctx: &RunContext,
    force: bool,
    out: &dyn UserOutput,
) -> anyhow::Result<()> {
    // Check isolation is currently enabled
    let tracker = StateTracker::new(work_dir.to_path_buf()).await?;
    let (enabled, previous_id) = tracker.get_isolation_mode().await;
    if !enabled {
        anyhow::bail!("Isolation mode is not enabled. Run `fed isolate enable` first.");
    }
    drop(tracker);

    super::ports::ensure_services_stopped(work_dir, force, out).await?;

    // Load config
    let parser = ConfigParser::new();
    let resolved_config = if let Some(path) = config_path {
        path
    } else {
        parser.find_config_file()?
    };
    let config = parser.load_config(&resolved_config)?;
    config.validate()?;

    // Drop the previous session's persisted ports (all scopes) before resolving
    // the new ones, so the abandoned isolation scope doesn't linger in the table.
    {
        let mut tracker = StateTracker::new(work_dir.to_path_buf()).await?;
        tracker.initialize().await?;
        tracker.clear_port_resolutions().await?;
    }

    // Generate the new isolation ID up front and apply it BEFORE initialize so
    // the freshly randomized ports persist under the new scope.
    let isolation_id = format!("iso-{:08x}", rand::random::<u32>());

    // Create orchestrator with randomized ports and initialize to resolve new ports.
    // isolation_id is applied before build()'s internal initialize() call so
    // ports resolve and persist under this new scope, not the previous one.
    let orchestrator = Orchestrator::builder()
        .config(config)
        .work_dir(work_dir.to_path_buf())
        .run_context(ctx.clone())
        .randomize_ports(true)
        .isolation_id(isolation_id.clone())
        .build()
        .await?;

    // Persist the new isolation mode now that ports are resolved under this scope.
    let tracker = orchestrator.state_tracker.read().await;
    tracker
        .set_isolation_mode(true, Some(isolation_id.clone()))
        .await?;

    // Clean up the previous isolation session's marker directory. The new
    // session's namespace is empty by construction (scoped by isolation_id),
    // so `install` will re-run against the rotated containers without us having
    // to clear anything — we just tidy up the abandoned dir. (migrate has no
    // marker in fed 6.0 — it re-runs on every start regardless.)
    if let Some(id) = previous_id {
        let old_markers = fed::markers::LifecycleMarkers::new(work_dir.to_path_buf(), Some(id));
        let _ = old_markers.clear_all_installed();
    }

    // Display new ports for this isolation scope
    let ports = tracker
        .get_global_port_allocations(Some(&isolation_id))
        .await;
    drop(tracker);

    out.status("\nIsolation rotated.");
    out.status(&format!("New isolation ID: {}", isolation_id));
    out.status("\nNew port allocations:");

    let mut sorted: Vec<_> = ports.iter().collect();
    sorted.sort_by_key(|(_, port)| *port);
    for (param, port) in &sorted {
        out.status(&format!("  {:>5}  {}", port, param));
    }
    out.blank();
    out.status("Use `fed isolate status` to view current allocations.\n");

    Ok(())
}
