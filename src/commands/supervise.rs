//! `fed supervise` — the restart-policy supervisor daemon
//! (`07-supervisor.md`).
//!
//! This file has two halves:
//! - [`run_supervise`]: the daemon's own body, invoked by `main.rs` when
//!   `cli.command` is the hidden `Commands::Supervise` variant.
//! - [`spawn_if_needed`]: called from `fed start`'s non-watch branch and
//!   `fed restart` to launch a detached `fed supervise` process when a
//!   service with a `restart:` policy is running and no live supervisor
//!   already exists for this workspace.

use fed::config::Config;
use fed::orchestrator::supervisor::{live_supervisor_pid, try_acquire};
use fed::{Orchestrator, OutputMode, RunContext};
use std::path::{Path, PathBuf};
use std::time::Duration;

/// The `fed supervise` daemon's entry point.
///
/// Runs until either:
/// - SIGTERM arrives (from `fed stop`'s teardown, or the watch/tui
///   pre-flight handoff in `main.rs`) — calls `stop_monitoring_only()` and
///   exits.
/// - No supervised service remains `desired_state == Running` — the daemon
///   has nothing left to protect, so it stops monitoring and exits on its
///   own, per the per-tick self-exit check
///   (`Orchestrator::any_supervised_service_desired_running`).
///
/// Single-instance enforcement (`.fed/supervisor.lock`) is acquired here,
/// before any `Orchestrator` is built — losing the race to an already-live
/// supervisor is not an error, just a no-op exit (this happens whenever two
/// `fed start`/`fed restart` invocations both decide to spawn one at
/// roughly the same time).
pub async fn run_supervise(config: Config, work_dir: PathBuf) -> anyhow::Result<()> {
    let lock = match try_acquire(&work_dir) {
        Ok(lock) => lock,
        Err(e) => {
            tracing::info!(
                "fed supervise: another supervisor already holds the lock for {:?} ({}) — exiting",
                work_dir,
                e
            );
            return Ok(());
        }
    };
    tracing::info!(
        "fed supervise: acquired {:?}, attaching to {:?}",
        lock.path(),
        work_dir
    );

    // supervisor_attach's own construction path (initialize_supervisor) is
    // what matters here, not output_mode — but File is the accurate label
    // for what this daemon exists to watch (backgrounded services).
    let run_context = RunContext {
        output_mode: OutputMode::File,
        ..Default::default()
    };

    let orchestrator = Orchestrator::builder()
        .config(config)
        .work_dir(work_dir.clone())
        .run_context(run_context)
        .supervisor_attach(true)
        .build()
        .await?;

    tracing::info!("fed supervise: attached and monitoring supervised services");

    run_until_done(&orchestrator).await;

    drop(lock);
    tracing::info!("fed supervise: exiting");
    Ok(())
}

#[cfg(unix)]
async fn run_until_done(orchestrator: &Orchestrator) {
    use tokio::signal::unix::{SignalKind, signal};

    let mut sigterm = match signal(SignalKind::terminate()) {
        Ok(s) => Some(s),
        Err(e) => {
            tracing::warn!("fed supervise: failed to install SIGTERM handler: {}", e);
            None
        }
    };

    // Matches the monitoring loop's own tick interval — cheap (a HashMap
    // scan plus one SQLite read per supervised service) and frequent enough
    // that the daemon exits promptly once nothing is left to protect.
    let mut poll = tokio::time::interval(Duration::from_secs(5));

    loop {
        tokio::select! {
            _ = async {
                if let Some(ref mut s) = sigterm {
                    s.recv().await
                } else {
                    std::future::pending::<Option<()>>().await
                }
            } => {
                tracing::info!("fed supervise: SIGTERM received, stopping monitoring only");
                orchestrator.stop_monitoring_only().await;
                break;
            }
            _ = poll.tick() => {
                // Reconcile the in-flight-restart race: a restart that
                // passed the desired-state gate just before a partial
                // `fed stop` wrote Stopped lands after the kill and leaves
                // the service alive with desired_state=stopped. The gate
                // only prevents future restarts of DEAD services; a live
                // resurrected one must be actively stopped here.
                orchestrator.stop_supervised_not_desired_running().await;

                if !orchestrator.any_supervised_service_desired_running().await {
                    tracing::info!(
                        "fed supervise: no supervised service remains desired-running, exiting"
                    );
                    orchestrator.stop_monitoring_only().await;
                    break;
                }
            }
        }
    }
}

#[cfg(not(unix))]
async fn run_until_done(_orchestrator: &Orchestrator) {
    // No supervisor daemon on non-unix platforms (07-supervisor.md: macOS +
    // Linux only) — this function is unreachable in practice since
    // `spawn_if_needed` never spawns one there, but is kept total.
}

/// Spawn a detached `fed supervise` for `work_dir`, unless one is already
/// running.
///
/// Called from `fed start`'s non-watch branch (when any started service has
/// a `restart:` policy) and from `fed restart` (per the scaled-back
/// self-heal promise — `fed status` calls neither this nor any respawn
/// logic; it only reads the lock file for display).
///
/// Daemonization (`07-supervisor.md` Design §5): `fed supervise` is fed's
/// own binary, so detachment doesn't need the `nohup bash -c` shell-wrapper
/// trick `ProcessService` uses for opaque user commands — SIGHUP is ignored
/// directly at the top of `main()` (see `main.rs`), and `.process_group(0)`
/// here isolates the daemon from job-control signals the same way
/// `ProcessService::spawn_process` does for detached services
/// (`src/service/process.rs:265-266`). No `daemonize`/fork-crate dependency,
/// no double-fork: the parent doesn't wait on the child, which is sufficient
/// once SIGHUP is handled.
pub fn spawn_if_needed(
    work_dir: &Path,
    config_path: &Path,
    offline: bool,
    profiles: &[String],
) -> anyhow::Result<()> {
    if let Some(pid) = live_supervisor_pid(work_dir) {
        tracing::debug!(
            "fed supervise already running for {:?} (pid {}), not spawning",
            work_dir,
            pid
        );
        return Ok(());
    }

    let exe = std::env::current_exe().map_err(|e| {
        anyhow::anyhow!(
            "failed to locate fed's own executable to spawn the supervisor: {}",
            e
        )
    })?;

    let mut cmd = tokio::process::Command::new(exe);
    cmd.arg("--workdir").arg(work_dir);
    cmd.arg("--config").arg(config_path);
    if offline {
        cmd.arg("--offline");
    }
    for profile in profiles {
        cmd.arg("--profile").arg(profile);
    }
    cmd.arg("supervise");

    cmd.stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .kill_on_drop(false) // detach — the daemon must outlive this process
        .process_group(0); // isolate from this terminal's job-control signals

    match cmd.spawn() {
        Ok(_child) => {
            tracing::info!("Spawned fed supervise for {:?}", work_dir);
            Ok(())
        }
        Err(e) => Err(anyhow::anyhow!("failed to spawn fed supervise: {}", e)),
    }
}

/// True if any of `service_names` has a `restart:` policy other than `No`
/// in `config`. Used by both `fed start` (services just started) and `fed
/// restart` (services just restarted) to decide whether spawning a
/// supervisor is worth doing at all.
pub fn any_has_restart_policy(
    config: &Config,
    service_names: impl IntoIterator<Item = impl AsRef<str>>,
) -> bool {
    service_names.into_iter().any(|name| {
        config
            .services
            .get(name.as_ref())
            .and_then(|s| s.restart.clone())
            .map(|policy| !matches!(policy, fed::RestartPolicy::No))
            .unwrap_or(false)
    })
}
