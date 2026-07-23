use crate::output::UserOutput;
use fed::{
    Error as FedError, Orchestrator, WatchMode,
    config::{Config, ServiceType},
    parameter::PortResolutionReason,
    port::PortConflict,
    service::Status,
};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use super::lifecycle::{graceful_docker_stop, graceful_process_kill, validate_pid_start_time};

pub struct StartOptions<'a> {
    pub watch: bool,
    pub replace: bool,
    pub dry_run: bool,
    pub parallel: bool,
    pub config_path: &'a std::path::Path,
    /// Threaded to `spawn_if_needed` so a spawned `fed supervise` sees the
    /// same `--offline`/`--profile` session settings as this invocation
    /// (`07-supervisor.md` Design §1's note on settings-threading).
    pub offline: bool,
    pub profiles: Vec<String>,
}

pub async fn run_start(
    orchestrator: &mut Orchestrator,
    config: &Config,
    services: Vec<String>,
    opts: StartOptions<'_>,
    out: &dyn UserOutput,
) -> anyhow::Result<()> {
    let StartOptions {
        watch,
        replace,
        dry_run,
        parallel,
        config_path,
        offline,
        profiles,
    } = opts;
    let services_to_start = if services.is_empty() {
        // Use entrypoint
        if let Some(ref ep) = config.entrypoint {
            vec![ep.clone()]
        } else if !config.entrypoints.is_empty() {
            config.entrypoints.clone()
        } else {
            let mut msg = String::from(
                "No services specified and no entrypoint configured.\n\n\
                 Start a specific service with 'fed start <service>' or set an \
                 'entrypoint:' in your config.",
            );
            if !config.services.is_empty() {
                msg.push_str("\n\nConfigured services:");
                let mut names: Vec<_> = config.services.keys().collect();
                names.sort();
                for name in names {
                    msg.push_str(&format!("\n  - {}", name));
                }
            }
            out.status(&msg);
            return Ok(());
        }
    } else {
        // Expand tag references (e.g., @backend) into service names
        config.expand_service_selection(&services)
    };

    // Handle dry run mode - show what would happen without starting services
    if dry_run {
        return run_dry_run(orchestrator, config, services_to_start, out).await;
    }

    // If --replace is set, first stop any fed-managed services gracefully,
    // then kill any remaining external processes occupying required ports
    if replace {
        // First, gracefully stop stale services from a previous run
        let (stopped_services, failed_stops) = stop_stale_services(orchestrator, out).await;
        if stopped_services > 0 {
            out.status(&format!(
                "Stopped {} service(s) from previous run\n",
                stopped_services
            ));
            // Give a moment for ports to be fully released
            tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        }

        // Free ports still held by external processes. This must run even when
        // fed services were stopped above: a service whose stop failed (or a
        // crash-looping predecessor) may still hold its port, and skipping this
        // pass would hand the new instance an EADDRINUSE. PortConflict::check
        // is a no-op for ports that are already free.
        let mut freed_any = false;
        for resolution in orchestrator.get_port_resolutions() {
            let Some(conflict) = PortConflict::check(resolution.resolved_port) else {
                continue;
            };

            out.progress(&format!(
                "Freeing port {} ({})... ",
                resolution.resolved_port, resolution.param_name
            ));
            match conflict.free_port() {
                Ok(msg) => {
                    out.finish_progress(&msg);
                    freed_any = true;
                }
                Err(e) => {
                    out.error(&format!("failed: {}", e));
                }
            }
        }

        if freed_any {
            out.blank();
        }

        // A service whose graceful stop failed keeps its state row (so the
        // live process stays tracked), but the port-freeing pass above may
        // have killed that same process by port. Drop rows whose process is
        // now provably dead — a leftover row would make registration return
        // AlreadyExists and silently skip starting the replacement.
        unregister_dead_services(orchestrator).await;

        // If a stale service survived both the graceful stop and the port
        // kill, its row is still registered: the start below would get
        // AlreadyExists and report success while the old process keeps
        // running. Fail honestly instead.
        if !failed_stops.is_empty() {
            let conn = orchestrator.state_tracker.read().await.clone_connection();
            let remaining =
                fed::state::SqliteStateTracker::fetch_services_from_connection(&conn).await;
            let still_alive: Vec<String> = failed_stops
                .into_iter()
                .filter(|name| remaining.iter().any(|(n, _)| n == name))
                .collect();
            if !still_alive.is_empty() {
                anyhow::bail!(
                    "--replace could not stop {} service(s) from the previous run: {}. \
                     Stop them manually (e.g. fed stop or kill the PID shown by fed status) \
                     and retry.",
                    still_alive.len(),
                    still_alive.join(", ")
                );
            }
        }
    }

    // Show what we're about to start with their dependencies
    let dep_graph = orchestrator.get_dependency_graph();
    for service in &services_to_start {
        let deps = dep_graph.get_dependencies(service);
        if deps.is_empty() {
            out.status(&format!("Starting: {}", service));
        } else {
            out.status(&format!(
                "Starting: {} (with deps: {})",
                service,
                deps.join(", ")
            ));
        }
    }
    out.blank();

    // Pre-pull Docker images in parallel before starting services
    {
        let dep_graph = orchestrator.get_dependency_graph();
        let mut all_services: Vec<String> = Vec::new();
        for service in &services_to_start {
            for dep in dep_graph.get_dependencies(service) {
                if !all_services.contains(&dep) {
                    all_services.push(dep);
                }
            }
            if !all_services.contains(service) {
                all_services.push(service.clone());
            }
        }

        let pull_results = orchestrator.pre_pull_images(&all_services).await;
        if !pull_results.is_empty() {
            let label = if pull_results.len() == 1 {
                "image"
            } else {
                "images"
            };
            out.status(&format!(
                "Pulling {} docker {}...",
                pull_results.len(),
                label
            ));

            let mut had_errors = false;
            for result in &pull_results {
                match &result.outcome {
                    Ok(()) => out.success(&format!("  \u{2713} {}", result.image)),
                    Err(e) => {
                        out.error(&format!("  \u{2717} {} ({})", result.image, e));
                        had_errors = true;
                    }
                }
            }

            if had_errors {
                orchestrator.cleanup().await;
                return Err(anyhow::anyhow!("Failed to pull docker image(s)"));
            }
            out.blank();
        }
    }

    // Set up Ctrl+C handler during startup to allow aborting
    let startup_abort = Arc::new(AtomicBool::new(false));
    let startup_abort_clone = startup_abort.clone();
    tokio::spawn(async move {
        tokio::signal::ctrl_c().await.ok();
        startup_abort_clone.store(true, Ordering::SeqCst);
    });

    // Full startup plan: dependencies first, then targets, deduplicated,
    // in dependency order.
    let mut plan: Vec<String> = Vec::new();
    {
        let dep_graph = orchestrator.get_dependency_graph();
        for service in &services_to_start {
            for dep in dep_graph.get_dependencies(service) {
                if !plan.contains(&dep) {
                    plan.push(dep);
                }
            }
            if !plan.contains(service) {
                plan.push(service.clone());
            }
        }
    }
    let name_width = plan.iter().map(|s| s.chars().count()).max().unwrap_or(0);

    // Track which services we've already started (to avoid duplicate messages)
    let mut started: std::collections::HashSet<String> = std::collections::HashSet::new();
    let mut warnings: Vec<String> = Vec::new();
    let startup_timer = std::time::Instant::now();

    // Group the plan into dependency levels. With --parallel, services in
    // the same level start concurrently; otherwise one at a time.
    let groups: Vec<Vec<String>> = if parallel {
        match parallel_groups_for_plan(orchestrator.get_dependency_graph(), &plan) {
            Ok(groups) => groups,
            Err(FedError::ServiceNotFound(missing)) => {
                return Err(unknown_service_error(config, &missing));
            }
            Err(e) => {
                return Err(anyhow::anyhow!(
                    "Cannot compute parallel start order: {}",
                    e
                ));
            }
        }
    } else {
        plan.iter().map(|name| vec![name.clone()]).collect()
    };

    for group in &groups {
        // Check if user aborted during startup
        if startup_abort.load(Ordering::SeqCst) {
            out.status("\n\nStartup aborted. Cleaning up...");
            orchestrator.cleanup().await;
            out.status("Cleanup complete");
            return Ok(());
        }

        let group: Vec<&String> = group.iter().filter(|s| !started.contains(*s)).collect();
        let result = if group.len() == 1 {
            let service = group[0];
            let outcome =
                start_one_service(orchestrator, config, service, name_width, true, out).await;
            outcome.map(|w| {
                started.insert(service.clone());
                if let Some(w) = w {
                    warnings.push(w);
                }
            })
        } else {
            let starts = group.iter().map(|service| {
                start_one_service(orchestrator, config, service, name_width, false, out)
            });
            let mut first_err = None;
            for (service, outcome) in group.iter().zip(futures::future::join_all(starts).await) {
                match outcome {
                    Ok(w) => {
                        started.insert((*service).clone());
                        if let Some(w) = w {
                            warnings.push(w);
                        }
                    }
                    Err(e) => first_err = first_err.or(Some(e)),
                }
            }
            match first_err {
                None => Ok(()),
                Some(e) => Err(e),
            }
        };

        if let Err(e) = result {
            orchestrator.cleanup().await;

            // Unknown service: build one rich error (did-you-mean + service
            // list) and let main print it once.
            if let FedError::ServiceNotFound(ref missing) = e {
                return Err(unknown_service_error(config, missing));
            }

            return Err(e.into());
        }
    }

    let elapsed = fmt_duration(startup_timer.elapsed());
    if warnings.is_empty() {
        out.success(&format!(
            "\nAll {} services started in {}",
            started.len(),
            elapsed
        ));
    } else {
        let warning_word = if warnings.len() == 1 {
            "warning"
        } else {
            "warnings"
        };
        out.status(&format!(
            "\nStarted {} services in {} — {} {}:",
            started.len(),
            elapsed,
            warnings.len(),
            warning_word
        ));
        for warning in &warnings {
            out.warning(&format!("  ⚠ {}", warning));
        }
    }

    // Print startup messages from the resolved config (templates substituted)
    print_startup_messages(orchestrator.get_config(), &started, out);

    // Mark startup complete - enables monitoring to clean up dead services
    orchestrator.mark_startup_complete();

    // Brief delay to let processes bind ports and potentially fail with EADDRINUSE.
    // Then use active status check to detect processes that crashed after spawn.
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    out.status("\nService Status:");
    let status = orchestrator.get_status().await;

    // Collect port conflicts for all port parameters
    let mut port_conflicts: Vec<(String, u16, String, Option<u32>)> = Vec::new();
    let has_failing = status.values().any(|s| *s == Status::Failing);

    let mut status_entries: Vec<(&String, &Status)> = status.iter().collect();
    status_entries.sort_by(|a, b| a.0.cmp(b.0));
    for (name, stat) in status_entries {
        let status_str = match stat {
            Status::Running => "Running",
            Status::Healthy => "Healthy",
            Status::Failing => "Failing",
            Status::Stopped => "Stopped",
            Status::Starting => "Starting",
            Status::Stopping => "Stopping",
            Status::Completed => "Completed",
        };
        out.status(&format!("  {}: {}", name, status_str));
    }

    // If any services are failing, check ALL port parameters for conflicts
    if has_failing {
        // Collect PIDs of all fed-managed services to filter them out
        let mut managed_pids: std::collections::HashSet<u32> = std::collections::HashSet::new();
        for name in status.keys() {
            if let Ok(Some(pid)) = orchestrator.get_service_pid(name).await {
                managed_pids.insert(pid);
            }
        }

        // Collect names of running/healthy services to match against process names
        // This helps identify when our own services are holding ports
        let running_services: std::collections::HashSet<String> = status
            .iter()
            .filter(|(_, s)| matches!(s, Status::Running | Status::Healthy))
            .map(|(name, _)| name.to_lowercase())
            .collect();

        // Check if any Docker services are running (to skip com.docker.backend as "conflict")
        let has_running_docker_services = orchestrator.has_docker_services()
            && running_services
                .iter()
                .any(|name| orchestrator.is_docker_service(name));

        // Check if any process-based services are running (non-Docker, non-Gradle)
        // These typically run as node/npm/bun/python/java etc.
        let has_running_process_services = running_services
            .iter()
            .any(|name| orchestrator.is_process_service(name));

        for resolution in orchestrator.get_port_resolutions() {
            let port = resolution.resolved_port;
            let Some(conflict) = PortConflict::check(port) else {
                continue;
            };

            for process in &conflict.processes {
                // Skip if this is a fed-managed service (by PID)
                if managed_pids.contains(&process.pid) {
                    continue;
                }

                // Skip Docker daemon if we have running Docker services
                // (it holds ports on behalf of containers)
                let name_lower = process.name.to_lowercase();
                if has_running_docker_services
                    && (name_lower.contains("docker") || name_lower.contains("com.docker"))
                {
                    continue;
                }

                // Skip if process name matches a running service name
                // (handles forked processes and containers)
                let matches_service = running_services
                    .iter()
                    .any(|svc| name_lower.contains(svc) || svc.contains(&name_lower));
                if matches_service {
                    continue;
                }

                // Skip common runtime processes if we have running process services
                // (node/npm/bun for JS, python for Python, java for JVM, etc.)
                const COMMON_RUNTIMES: &[&str] = &[
                    "node", "npm", "npx", "bun", "deno", "python", "python3", "java", "gradle",
                    "ruby", "go", "cargo", "rust",
                ];
                if has_running_process_services
                    && COMMON_RUNTIMES.iter().any(|rt| name_lower == *rt)
                {
                    continue;
                }

                port_conflicts.push((
                    resolution.param_name.clone(),
                    port,
                    process.name.clone(),
                    Some(process.pid),
                ));
            }
            // Only report unknown if no processes found at all
            if conflict.processes.is_empty() {
                port_conflicts.push((
                    resolution.param_name.clone(),
                    port,
                    "unknown".to_string(),
                    None,
                ));
            }
        }

        out.blank();
        if !port_conflicts.is_empty() {
            out.error("Port conflicts detected:");
            for (param_name, port, process_name, pid) in &port_conflicts {
                if let Some(p) = pid {
                    out.error(&format!(
                        "  {} (port {}) - occupied by '{}' (PID {})",
                        param_name, port, process_name, p
                    ));
                } else {
                    out.error(&format!(
                        "  {} (port {}) - occupied by external process",
                        param_name, port
                    ));
                }
            }
            out.blank();
            out.status("Hint: If another checkout of this project owns these ports (worktree, parallel agent):");
            out.status("          fed isolate enable      # give this directory its own ports");
            out.status("      If a stray external process holds them:");
            out.status(
                "          fed start --replace     # kills whatever holds the ports — including",
            );
            out.status("                                  # other checkouts' fed services");
        }

        // Show per-service failure details
        let failing_services: Vec<&String> = status
            .iter()
            .filter(|(_, s)| **s == Status::Failing)
            .map(|(name, _)| name)
            .collect();

        if !failing_services.is_empty() {
            out.error("Failing services:");
            for name in &failing_services {
                out.error(&format!("  {}", name));
                if let Some(error) = orchestrator.get_last_error(name).await {
                    for line in error.lines() {
                        out.status(&format!("    {}", line));
                    }
                } else if let Ok(logs) = orchestrator.get_logs(name, Some(5)).await
                    && !logs.is_empty()
                {
                    out.status("    Recent logs:");
                    for line in &logs {
                        out.status(&format!("      {}", line));
                    }
                }
            }
            out.blank();
            out.status("Use 'fed logs <service>' for full logs");
        }
    }

    if !watch {
        out.status("\nServices running in background");
        out.status("  Use 'fed stop' to stop them");
        out.status("  Use 'fed tui' for interactive mode");

        // A plain, non-watch `fed start` backgrounds services and this
        // process exits immediately — without a supervisor, a `restart:`
        // policy would never fire again (`07-supervisor.md`). Spawn one iff
        // it's actually needed and not already running; `fed status` never
        // does this (Design's scaled-back self-heal promise).
        if super::supervise::any_has_restart_policy(config, started.iter()) {
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
    } else {
        run_watch_mode(orchestrator, config, config_path, out).await?;
    }

    Ok(())
}

/// Group `plan` into dependency levels using the graph's parallel groups.
///
/// Every plan entry must land in a group: a requested service the graph
/// doesn't know would otherwise be silently dropped by the filtering and
/// never started (while the command still reports success). Such names
/// return `ServiceNotFound` instead.
fn parallel_groups_for_plan(
    graph: &fed::dependency::Graph,
    plan: &[String],
) -> Result<Vec<Vec<String>>, FedError> {
    let mut groups = graph.get_parallel_groups()?;
    for group in &mut groups {
        group.retain(|name| plan.iter().any(|p| p == name));
        group.sort();
    }
    groups.retain(|g| !g.is_empty());

    let grouped: std::collections::HashSet<&String> = groups.iter().flatten().collect();
    for name in plan {
        if !grouped.contains(name) {
            return Err(FedError::ServiceNotFound(name.clone()));
        }
    }
    Ok(groups)
}

/// Rich unknown-service error: did-you-mean plus the configured service list.
fn unknown_service_error(config: &Config, missing: &str) -> anyhow::Error {
    let mut msg = super::suggest::with_did_you_mean(
        &format!("Service '{}' not found.", missing),
        missing,
        config.services.keys().map(String::as_str),
    );
    if !config.services.is_empty() {
        msg.push_str("\n\nAvailable services:");
        let mut names: Vec<_> = config.services.keys().collect();
        names.sort();
        for name in names {
            msg.push_str(&format!("\n  - {}", name));
        }
    }
    anyhow::anyhow!(msg)
}

/// Human-friendly duration: "0.4s", "12.3s", "2m05s".
fn fmt_duration(d: std::time::Duration) -> String {
    let secs = d.as_secs_f64();
    if secs < 60.0 {
        format!("{:.1}s", secs)
    } else {
        format!("{}m{:02}s", d.as_secs() / 60, d.as_secs() % 60)
    }
}

/// Start one service and print a single outcome line for it.
///
/// With `inline_progress`, an in-place "starting" line is drawn first and
/// replaced by the outcome (sequential mode); without it, only the outcome
/// line is printed when done (parallel groups, where several services share
/// the terminal).
///
/// Returns `Ok(Some(warning))` when the service started but deserves a
/// warning in the summary (e.g. healthcheck timed out).
async fn start_one_service(
    orchestrator: &Orchestrator,
    config: &Config,
    name: &str,
    name_width: usize,
    inline_progress: bool,
    out: &dyn UserOutput,
) -> Result<Option<String>, FedError> {
    let timer = std::time::Instant::now();
    if inline_progress {
        out.progress(&format!(
            "  ⋯ {:<width$}  starting",
            name,
            width = name_width
        ));
    }

    match orchestrator.start(name).await {
        Ok(_) => {
            let elapsed = fmt_duration(timer.elapsed());
            // Healthcheck results are recorded in the state tracker (the
            // manager's in-memory status stays Running), so read the tracker
            // first and fall back to the live manager status.
            let status = {
                let tracked = {
                    let tracker = orchestrator.state_tracker.read().await;
                    tracker.get_service(name).await.map(|s| s.status)
                };
                match tracked {
                    Some(status) => Some(status),
                    None => orchestrator.get_service(name).await,
                }
            };
            let has_healthcheck = config
                .services
                .get(name)
                .map(|s| s.healthcheck.is_some())
                .unwrap_or(false);

            let (line, warning) = match status {
                Some(Status::Healthy) => (
                    format!("  ✓ {:<w$}  healthy in {}", name, elapsed, w = name_width),
                    None,
                ),
                Some(Status::Completed) => (
                    format!("  ✓ {:<w$}  completed in {}", name, elapsed, w = name_width),
                    None,
                ),
                Some(Status::Running) if has_healthcheck => (
                    format!(
                        "  ⚠ {:<w$}  started, but not healthy after {}",
                        name,
                        elapsed,
                        w = name_width
                    ),
                    Some(format!(
                        "'{}' has not passed its healthcheck — see 'fed logs {}'",
                        name, name
                    )),
                ),
                Some(Status::Running) => (
                    format!("  ✓ {:<w$}  running ({})", name, elapsed, w = name_width),
                    None,
                ),
                Some(other) => (
                    format!(
                        "  ⚠ {:<w$}  {} after {}",
                        name,
                        other,
                        elapsed,
                        w = name_width
                    ),
                    Some(format!("'{}' is {} after startup", name, other)),
                ),
                None => (
                    format!("  ✓ {:<w$}  started ({})", name, elapsed, w = name_width),
                    None,
                ),
            };

            if inline_progress {
                out.finish_progress_with(&line);
            } else {
                out.status(&line);
            }
            Ok(warning)
        }
        Err(e) => {
            let line = format!("  ✗ {:<w$}  failed", name, w = name_width);
            if inline_progress {
                out.finish_progress_with(&line);
            } else {
                out.error(&line);
            }
            Err(e)
        }
    }
}

/// Print startup messages from services in a Unicode box.
///
/// Collects `startup_message` from started services, sorts entrypoint messages
/// last, and renders them in a bordered box.
fn print_startup_messages(
    config: &Config,
    started: &std::collections::HashSet<String>,
    out: &dyn UserOutput,
) {
    // Collect (service_name, message) pairs for started services
    let mut messages: Vec<(&str, &str)> = Vec::new();
    for (name, service) in &config.services {
        if started.contains(name)
            && let Some(ref msg) = service.startup_message
        {
            messages.push((name, msg));
        }
    }

    if messages.is_empty() {
        return;
    }

    // Determine which services are entrypoints
    let entrypoints: std::collections::HashSet<&str> = {
        let mut set = std::collections::HashSet::new();
        if let Some(ref ep) = config.entrypoint {
            set.insert(ep.as_str());
        }
        for ep in &config.entrypoints {
            set.insert(ep.as_str());
        }
        set
    };

    // Stable sort: non-entrypoints first (preserve insertion order), entrypoints last
    messages.sort_by_key(|(name, _)| entrypoints.contains(name));

    // Calculate box width (max message length + 2 for padding)
    let max_len = messages.iter().map(|(_, msg)| msg.len()).max().unwrap_or(0);
    let box_width = max_len + 2; // 1 space padding on each side

    let horizontal = "\u{2500}".repeat(box_width);

    out.blank();
    out.status(&format!("\u{256d}{}\u{256e}", horizontal));
    for (i, (_, msg)) in messages.iter().enumerate() {
        if i > 0 {
            out.status(&format!("\u{251c}{}\u{2524}", horizontal));
        }
        out.status(&format!(
            "\u{2502} {:width$} \u{2502}",
            msg,
            width = max_len
        ));
    }
    out.status(&format!("\u{2570}{}\u{256f}", horizontal));
}

async fn run_watch_mode(
    orchestrator: &mut Orchestrator,
    config: &Config,
    config_path: &std::path::Path,
    out: &dyn UserOutput,
) -> anyhow::Result<()> {
    out.status("\nServices running with watch mode enabled");
    out.status("  Files will be monitored for changes. Press Ctrl+C to stop...");

    // Set up watch mode
    let work_dir = if let Some(parent) = config_path.parent() {
        if parent.as_os_str().is_empty() {
            std::env::current_dir()?
        } else {
            parent.to_path_buf()
        }
    } else {
        std::env::current_dir()?
    };

    let mut watch_mode = match WatchMode::new(config, &work_dir) {
        Ok(wm) => {
            out.status("  Watching for file changes...");
            Some(wm)
        }
        Err(e) => {
            out.warning(&format!("Failed to start watch mode: {}", e));
            out.warning("  Continuing without file watching...");
            None
        }
    };

    // Install signal handler for SIGINT (Ctrl+C) and SIGTERM
    let (shutdown_tx, mut shutdown_rx) = tokio::sync::mpsc::channel::<()>(1);
    let force_quit = Arc::new(AtomicBool::new(false));
    let force_quit_clone = force_quit.clone();

    // Clone state tracker, cancellation token, and services for force quit cleanup
    let state_tracker_clone = orchestrator.state_tracker.clone();
    let cancel_token_clone = orchestrator.child_token();
    let services_clone = orchestrator.get_services_arc();

    // Signal handler runs in a spawned task — uses println! directly since
    // the `out` reference can't be easily passed to a 'static future.
    tokio::spawn(async move {
        use tokio::signal::unix::{SignalKind, signal};

        // Set up signal handlers, logging warnings if they fail
        let mut sigint = match signal(SignalKind::interrupt()) {
            Ok(s) => Some(s),
            Err(e) => {
                tracing::warn!("Failed to create SIGINT handler: {}", e);
                None
            }
        };
        let mut sigterm = match signal(SignalKind::terminate()) {
            Ok(s) => Some(s),
            Err(e) => {
                tracing::warn!("Failed to create SIGTERM handler: {}", e);
                None
            }
        };

        // If neither signal handler works, just wait forever (process can still be killed)
        if sigint.is_none() && sigterm.is_none() {
            tracing::warn!(
                "No signal handlers available - process can only be terminated externally"
            );
            std::future::pending::<()>().await;
            return;
        }

        let mut signal_count = 0;
        loop {
            tokio::select! {
                _ = async {
                    if let Some(ref mut s) = sigint {
                        s.recv().await
                    } else {
                        std::future::pending::<Option<()>>().await
                    }
                } => {
                    signal_count += 1;

                    if signal_count == 1 {
                        println!("\n\nStopping services... (Press Ctrl+C again to force quit)");
                        shutdown_tx.send(()).await.ok();
                    } else {
                        println!("\n\nForce quitting...");
                        force_quit_clone.store(true, Ordering::SeqCst);

                        // Kill all running services before exit
                        let services_map = services_clone.read().await;
                        for service_arc in services_map.values() {
                            if let Ok(mut manager) = service_arc.try_lock() {
                                let _ = manager.kill().await;
                            }
                        }
                        drop(services_map);

                        // Save state tracker before exit
                        if let Err(e) = state_tracker_clone.write().await.save().await {
                            eprintln!("Failed to save state: {}", e);
                        }

                        // Signal monitoring task to shut down
                        cancel_token_clone.cancel();

                        std::process::exit(130);
                    }
                }
                _ = async {
                    if let Some(ref mut s) = sigterm {
                        s.recv().await
                    } else {
                        std::future::pending::<Option<()>>().await
                    }
                } => {
                    println!("\n\nReceived SIGTERM, stopping services gracefully...");
                    shutdown_tx.send(()).await.ok();
                    break;
                }
            }
        }
    });

    // Main event loop: watch for file changes or shutdown signal
    loop {
        tokio::select! {
            _ = shutdown_rx.recv() => {
                break;
            }
            event = async {
                if let Some(ref mut wm) = watch_mode {
                    wm.next_event().await
                } else {
                    std::future::pending::<Option<fed::watch::FileChangeEvent>>().await
                }
            } => {
                if let Some(event) = event {
                    out.status(&format!("\nFile change detected in service '{}': {} file(s) changed",
                        event.service_name, event.changed_paths.len()));
                    out.status(&format!("  Restarting {}...", event.service_name));

                    // Stop the service
                    match orchestrator.stop(&event.service_name).await {
                        Ok(_) => {
                            match orchestrator.start(&event.service_name).await {
                                Ok(_) => {
                                    out.success(&format!("  {} restarted successfully", event.service_name));
                                }
                                Err(e) => {
                                    out.error(&format!("  Failed to start {}: {}", event.service_name, e));
                                }
                            }
                        }
                        Err(e) => {
                            out.error(&format!("  Failed to stop {}: {}", event.service_name, e));
                        }
                    }
                }
            }
        }
    }

    // Perform cleanup if not force quitting
    if !force_quit.load(Ordering::SeqCst) {
        orchestrator.cleanup().await;
        out.success("All services stopped");
    }

    Ok(())
}

/// Run in dry-run mode: show what would happen without starting services.
///
/// This displays:
/// 1. Services to start (with their dependencies)
/// 2. Start order (topological sort)
/// 3. Port conflict detection
/// 4. Environment variables per service (with secrets masked)
/// 5. Resource limits
/// 6. Validation summary
async fn run_dry_run(
    orchestrator: &Orchestrator,
    config: &Config,
    services_to_start: Vec<String>,
    out: &dyn UserOutput,
) -> anyhow::Result<()> {
    out.status("=== Dry Run Mode ===\n");

    let dep_graph = orchestrator.get_dependency_graph();

    // 1. Show services that would be started with their dependencies
    out.status("Services to start:");
    for service in &services_to_start {
        let deps = dep_graph.get_dependencies(service);
        if deps.is_empty() {
            out.status(&format!("  - {}", service));
        } else {
            out.status(&format!(
                "  - {} (depends on: {})",
                service,
                deps.join(", ")
            ));
        }
    }

    // 2. Calculate and show start order (topological sort of all services to start)
    // Collect all services including dependencies
    let mut all_services: Vec<String> = Vec::new();
    for service in &services_to_start {
        let deps = dep_graph.get_dependencies(service);
        for dep in deps {
            if !all_services.contains(&dep) {
                all_services.push(dep);
            }
        }
        if !all_services.contains(service) {
            all_services.push(service.clone());
        }
    }

    out.status("\nStart order:");
    for (i, service) in all_services.iter().enumerate() {
        let service_config = config.services.get(service);
        let service_type = service_config
            .map(|s| s.service_type())
            .unwrap_or(ServiceType::Undefined);
        out.status(&format!("  {}. {} ({:?})", i + 1, service, service_type));
    }

    // 3. Check for port conflicts using resolution tracking
    // Release port listeners first so our own listeners don't appear as conflicts.
    // Safe in dry-run since we never start services.
    orchestrator.release_port_listeners();

    out.status("\nPort availability:");
    let port_resolutions = orchestrator.get_port_resolutions();
    let mut conflicts_found = false;
    if port_resolutions.is_empty() {
        out.status("  No port parameters detected");
    } else {
        for resolution in port_resolutions {
            match &resolution.reason {
                PortResolutionReason::DefaultAvailable | PortResolutionReason::Cached => {
                    // Check if port is still available (it might have been taken since resolution)
                    if let Some(conflict) = PortConflict::check(resolution.resolved_port) {
                        conflicts_found = true;
                        out.status(&format!(
                            "  [CONFLICT] Port {} ({}):",
                            resolution.resolved_port, resolution.param_name
                        ));
                        if conflict.processes.is_empty() {
                            out.status("    - Port in use by unknown process");
                        } else {
                            for process in &conflict.processes {
                                out.status(&format!(
                                    "    - '{}' (PID {})",
                                    process.name, process.pid
                                ));
                            }
                        }
                    } else {
                        out.status(&format!(
                            "  [OK] Port {} ({}) is available",
                            resolution.resolved_port, resolution.param_name
                        ));
                    }
                }
                PortResolutionReason::ConflictAutoResolved {
                    default_port,
                    conflict_pid,
                    conflict_process,
                } => {
                    conflicts_found = true;
                    let process_info = match (conflict_pid, conflict_process) {
                        (Some(pid), Some(name)) => format!("'{}' (PID {})", name, pid),
                        (Some(pid), None) => format!("PID {}", pid),
                        _ => "unknown process".to_string(),
                    };
                    out.status(&format!(
                        "  [CONFLICT] Default port {} ({}) occupied by {} - resolved to {}",
                        default_port, resolution.param_name, process_info, resolution.resolved_port
                    ));
                }
                PortResolutionReason::Random => {
                    out.status(&format!(
                        "  [OK] Port {} ({}) randomly allocated",
                        resolution.resolved_port, resolution.param_name
                    ));
                }
            }
        }
        if !conflicts_found {
            out.status(&format!(
                "  All {} port(s) available",
                port_resolutions.len()
            ));
        }
    }

    // 4. Show environment variables per service (mask sensitive values)
    out.status("\nService configuration:");
    for service_name in &all_services {
        if let Some(service_config) = config.services.get(service_name) {
            out.status(&format!("  {}:", service_name));

            // Show service type
            let service_type = service_config.service_type();
            out.status(&format!("    type: {:?}", service_type));

            // Show process command or image
            if let Some(ref process) = service_config.process {
                out.status(&format!("    command: {}", process));
            }
            if let Some(ref image) = service_config.image {
                out.status(&format!("    image: {}", image));
            }
            if let Some(ref gradle_task) = service_config.gradle_task {
                out.status(&format!("    gradle_task: {}", gradle_task));
            }

            // Show working directory if set
            if let Some(ref cwd) = service_config.cwd {
                out.status(&format!("    cwd: {}", cwd));
            }

            // Show health check if configured
            if let Some(ref healthcheck) = service_config.healthcheck {
                let timeout = healthcheck.get_timeout();
                match healthcheck.get_http_url() {
                    Some(url) => {
                        out.status(&format!(
                            "    healthcheck: HTTP GET {} (timeout: {:?})",
                            url, timeout
                        ));
                    }
                    None => {
                        if let Some(cmd) = healthcheck.get_command() {
                            out.status(&format!(
                                "    healthcheck: command '{}' (timeout: {:?})",
                                cmd, timeout
                            ));
                        }
                    }
                }
            }

            // Show environment variables with masked secrets
            if !service_config.environment.is_empty() {
                out.status("    environment:");
                let mut sorted_env: Vec<_> = service_config.environment.iter().collect();
                sorted_env.sort_by_key(|(k, _)| *k);
                for (key, value) in sorted_env {
                    let display_value = mask_sensitive_value(key, value);
                    out.status(&format!("      {}: {}", key, display_value));
                }
            }

            // Show restart policy if configured
            if let Some(ref restart) = service_config.restart {
                out.status(&format!("    restart: {:?}", restart));
            }
        }
    }

    // 5. Show resource limits
    out.status("\nResource limits:");
    let mut any_limits = false;
    for service_name in &all_services {
        if let Some(service_config) = config.services.get(service_name)
            && let Some(ref resources) = service_config.resources
        {
            any_limits = true;
            out.status(&format!("  {}:", service_name));
            if let Some(ref mem) = resources.memory {
                out.status(&format!("    memory: {}", mem));
            }
            if let Some(ref cpus) = resources.cpus {
                out.status(&format!("    cpus: {}", cpus));
            }
            if let Some(nofile) = resources.nofile {
                out.status(&format!("    nofile: {}", nofile));
            }
            if let Some(pids) = resources.pids {
                out.status(&format!("    pids: {}", pids));
            }
        }
    }
    if !any_limits {
        out.status("  No resource limits configured");
    }

    // 6. Validation summary
    out.status("\n=== Validation Summary ===");
    out.status("  Configuration: OK (parsed successfully)");
    out.status(&format!("  Services to start: {}", all_services.len()));
    if conflicts_found {
        out.status("  Port conflicts: DETECTED (auto-resolved to alternative ports; `fed start --replace` reclaims the defaults by killing their holders)");
    } else {
        out.status("  Port conflicts: None");
    }

    out.status("\n=== Dry run complete ===");
    out.status("Run without --dry-run to actually start services");

    Ok(())
}

/// Mask sensitive environment variable values.
///
/// Returns "***" for values whose keys contain sensitive keywords,
/// otherwise returns the original value.
fn mask_sensitive_value(key: &str, value: &str) -> String {
    let key_lower = key.to_lowercase();
    let sensitive_patterns = [
        "secret",
        "password",
        "token",
        "api_key",
        "apikey",
        "private_key",
        "privatekey",
        "auth",
        "credential",
    ];

    for pattern in &sensitive_patterns {
        if key_lower.contains(pattern) {
            return "***".to_string();
        }
    }

    value.to_string()
}

/// Stop stale services from a previous fed run gracefully.
///
/// This is called by `--replace` to cleanly stop fed-managed services
/// before killing any remaining external processes.
///
/// Returns the number of services stopped.
async fn stop_stale_services(
    orchestrator: &Orchestrator,
    out: &dyn UserOutput,
) -> (usize, Vec<String>) {
    use fed::state::SqliteStateTracker;

    // Clone the database connection while briefly holding the read lock.
    // This avoids holding the RwLock across the async database query,
    // which could cause contention with health monitoring or status checks.
    let conn = orchestrator.state_tracker.read().await.clone_connection();
    // Lock released here - the cloned connection is internally thread-safe

    let services = SqliteStateTracker::fetch_services_from_connection(&conn).await;

    if services.is_empty() {
        return (0, Vec::new());
    }

    let mut stopped = 0;
    let mut failed: Vec<String> = Vec::new();
    // Services whose state row is safe to remove: stopped successfully, or
    // skipped because there is provably nothing to stop.
    let mut cleaned: Vec<String> = Vec::new();

    for (name, state) in &services {
        // Starting/Failing/Stopping services also have (or may have) a live
        // process — skipping them here and then clearing state would orphan
        // exactly the conflicting process --replace exists to remove.
        if !super::stop::state_status_is_active(state.status) {
            cleaned.push(name.clone());
            continue;
        }

        out.progress(&format!("Stopping {} ({})... ", name, state.service_type));

        let success = if let Some(ref container_id) = state.container_id {
            // Docker service - stop and remove container
            graceful_docker_stop(container_id).await
        } else if let Some(pid) = state.pid {
            // Validate PID hasn't been reused by checking process start time
            if !validate_pid_start_time(pid, state.started_at) {
                out.finish_progress(&format!(
                    "skipped (PID {} was reused by another process)",
                    pid
                ));
                cleaned.push(name.clone());
                continue;
            }
            // Process service - graceful kill
            graceful_process_kill(pid).await
        } else {
            // No PID or container - nothing to stop
            out.finish_progress("skipped (no PID/container)");
            cleaned.push(name.clone());
            continue;
        };

        if success {
            out.finish_progress("stopped");
            stopped += 1;
            cleaned.push(name.clone());
        } else {
            out.warning("failed");
            failed.push(name.clone());
        }
    }

    // Remove state rows only for services that were actually stopped (or had
    // nothing to stop). A blanket clear() would erase rows for services whose
    // stop failed, orphaning their still-running processes.
    if !cleaned.is_empty() {
        let mut tracker = orchestrator.state_tracker.write().await;
        for name in &cleaned {
            if let Err(e) = tracker.unregister_service(name).await {
                tracing::warn!("Failed to unregister service '{}' from state: {}", name, e);
            }
        }
        if let Err(e) = tracker.save().await {
            tracing::warn!("Failed to save state after stopping services: {}", e);
        }
    }

    (stopped, failed)
}

/// Unregister state rows for process services whose PID is no longer alive
/// (or was reused by another process). Used by --replace after force-freeing
/// ports, which can kill processes whose rows were deliberately kept when
/// their graceful stop failed.
async fn unregister_dead_services(orchestrator: &Orchestrator) {
    use fed::state::SqliteStateTracker;

    let conn = orchestrator.state_tracker.read().await.clone_connection();
    let services = SqliteStateTracker::fetch_services_from_connection(&conn).await;

    // Consult Docker only if the daemon responds — with the daemon down we
    // can't prove a container is gone, so its row must be kept.
    let docker_available = fed::docker::is_daemon_healthy().await;

    let mut dead: Vec<String> = Vec::new();
    for (name, state) in services {
        let alive = if let Some(ref container_id) = state.container_id {
            // graceful_docker_stop can report failure even after its rm -f
            // removed the container, and the port-free pass can kill one too.
            if !docker_available {
                continue;
            }
            fed::docker::is_container_running(container_id).await
        } else if let Some(pid) = state.pid {
            #[cfg(unix)]
            {
                nix::sys::signal::kill(nix::unistd::Pid::from_raw(pid as i32), None).is_ok()
                    && validate_pid_start_time(pid, state.started_at)
            }
            #[cfg(not(unix))]
            {
                true
            }
        } else {
            continue;
        };

        if !alive {
            dead.push(name);
        }
    }

    if dead.is_empty() {
        return;
    }

    let mut tracker = orchestrator.state_tracker.write().await;
    for name in &dead {
        if let Err(e) = tracker.unregister_service(name).await {
            tracing::warn!("Failed to unregister dead service '{}': {}", name, e);
        }
    }
    if let Err(e) = tracker.save().await {
        tracing::warn!("Failed to save state after removing dead services: {}", e);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mask_sensitive_value_secrets() {
        // Should mask secrets
        assert_eq!(mask_sensitive_value("API_SECRET", "my-secret"), "***");
        assert_eq!(mask_sensitive_value("secret_key", "value"), "***");
        assert_eq!(mask_sensitive_value("MY_SECRET_VALUE", "hidden"), "***");
    }

    #[test]
    fn test_mask_sensitive_value_passwords() {
        assert_eq!(mask_sensitive_value("PASSWORD", "pass123"), "***");
        assert_eq!(mask_sensitive_value("db_password", "dbpass"), "***");
        assert_eq!(mask_sensitive_value("USER_PASSWORD", "userpass"), "***");
    }

    #[test]
    fn test_mask_sensitive_value_tokens() {
        assert_eq!(mask_sensitive_value("AUTH_TOKEN", "token123"), "***");
        assert_eq!(mask_sensitive_value("access_token", "abc"), "***");
        assert_eq!(mask_sensitive_value("REFRESH_TOKEN", "xyz"), "***");
    }

    #[test]
    fn test_mask_sensitive_value_api_keys() {
        assert_eq!(mask_sensitive_value("API_KEY", "key123"), "***");
        assert_eq!(mask_sensitive_value("APIKEY", "key456"), "***");
        assert_eq!(mask_sensitive_value("my_api_key", "key789"), "***");
    }

    #[test]
    fn test_mask_sensitive_value_auth() {
        assert_eq!(mask_sensitive_value("AUTH_HEADER", "bearer xxx"), "***");
        assert_eq!(mask_sensitive_value("OAUTH_TOKEN", "oauth123"), "***");
    }

    #[test]
    fn test_mask_sensitive_value_credentials() {
        assert_eq!(mask_sensitive_value("CREDENTIAL", "cred123"), "***");
        assert_eq!(mask_sensitive_value("aws_credentials", "xxx"), "***");
    }

    #[test]
    fn test_mask_sensitive_value_private_keys() {
        assert_eq!(mask_sensitive_value("PRIVATE_KEY", "-----BEGIN"), "***");
        assert_eq!(mask_sensitive_value("privatekey", "key"), "***");
    }

    #[test]
    fn test_mask_sensitive_value_non_sensitive() {
        // Non-sensitive values should NOT be masked
        assert_eq!(
            mask_sensitive_value("DATABASE_URL", "postgres://localhost"),
            "postgres://localhost"
        );
        assert_eq!(mask_sensitive_value("PORT", "8080"), "8080");
        assert_eq!(mask_sensitive_value("NODE_ENV", "production"), "production");
        assert_eq!(mask_sensitive_value("DEBUG", "true"), "true");
    }

    #[test]
    fn test_mask_sensitive_value_case_insensitive() {
        // Should be case insensitive
        assert_eq!(mask_sensitive_value("password", "pass"), "***");
        assert_eq!(mask_sensitive_value("PASSWORD", "pass"), "***");
        assert_eq!(mask_sensitive_value("Password", "pass"), "***");
        assert_eq!(mask_sensitive_value("PaSsWoRd", "pass"), "***");
    }

    fn test_graph() -> fed::dependency::Graph {
        // a ── b ── d,  c independent
        let mut graph = fed::dependency::Graph::new();
        graph.add_node("a".to_string());
        graph.add_node("b".to_string());
        graph.add_node("c".to_string());
        graph.add_node("d".to_string());
        graph.add_edge("b".to_string(), "a".to_string());
        graph.add_edge("d".to_string(), "b".to_string());
        graph
    }

    #[test]
    fn parallel_groups_cover_the_plan_in_dependency_order() {
        let graph = test_graph();
        let plan = vec!["a".to_string(), "b".to_string(), "d".to_string()];
        let groups = parallel_groups_for_plan(&graph, &plan).unwrap();

        // c is outside the plan and must be filtered out; levels stay ordered
        assert_eq!(groups, vec![vec!["a"], vec!["b"], vec!["d"]]);
    }

    #[test]
    fn parallel_groups_keep_independent_services_in_one_level() {
        let graph = test_graph();
        let plan = vec!["a".to_string(), "c".to_string()];
        let groups = parallel_groups_for_plan(&graph, &plan).unwrap();
        assert_eq!(groups, vec![vec!["a", "c"]]);
    }

    #[test]
    fn parallel_groups_reject_unknown_services() {
        // A service missing from the graph must fail loudly, not be
        // silently dropped from the filtered groups.
        let graph = test_graph();
        let plan = vec!["a".to_string(), "typo".to_string()];
        match parallel_groups_for_plan(&graph, &plan) {
            Err(FedError::ServiceNotFound(name)) => assert_eq!(name, "typo"),
            other => panic!("expected ServiceNotFound, got {:?}", other.map(|_| ())),
        }
    }
}
