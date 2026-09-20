//! `fed supervise` — the service supervisor daemon.
//!
//! This file has two halves:
//! - [`run_supervise`]: the daemon's own body, invoked by `main.rs` when
//!   `cli.command` is the hidden `Commands::Supervise` variant.
//! - [`spawn_if_needed`]: called after background start/restart dispatch
//!   (including error recovery), or for an interactive service's dependencies,
//!   to launch a detached observer for healthchecks and restart policies.

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
pub async fn run_supervise(
    mut config: Config,
    work_dir: PathBuf,
    mut run_context: RunContext,
) -> anyhow::Result<()> {
    // Serialize attach with start/restart parameter resolution and registration.
    // A just-spawned daemon may not hold supervisor.lock yet when another
    // command begins; it must wait for that command's committed state.
    let attach_lock = fed::orchestrator::StartLock::acquire(&work_dir).await?;
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

    // A later start may only change one service. The persisted registration
    // owns the implementation of every still-running (or restartable) service.
    let tracker = fed::state::StateTracker::new_for_supervisor(work_dir.clone()).await?;
    let mut running_names = Vec::new();
    for name in config.services.keys() {
        if let Some(state) = tracker.get_service(name).await
            && state.desired_state == fed::state::DesiredState::Running
        {
            if let Some(variant) = state.variant {
                run_context.variants.push(format!("{name}:{variant}"));
            }
            running_names.push(name.clone());
        }
    }
    fed::config::variants::resolve_variants(
        &mut config,
        &fed::config::variants::VariantSelection::load(&run_context.variants, &work_dir)?,
        &run_context.profiles,
    )?;
    for name in running_names {
        for profile in &config.services[&name].profiles {
            if !run_context.profiles.contains(profile) {
                run_context.profiles.push(profile.clone());
            }
        }
    }
    drop(tracker);

    // supervisor_attach's own construction path (initialize_supervisor) is
    // what matters here, not output_mode — but File is the accurate label
    // for what this daemon exists to watch (backgrounded services).
    run_context.output_mode = OutputMode::File;

    let orchestrator = Orchestrator::builder()
        .config(config)
        .work_dir(work_dir.clone())
        .run_context(run_context)
        .supervisor_attach(true)
        .build()
        .await?;

    drop(attach_lock);
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
    // No supervisor daemon outside `cfg(unix)` — this function is
    // unreachable in practice since `spawn_if_needed` never spawns one
    // there, but is kept total.
}

/// The session-scoped flags a spawned `fed supervise` must inherit verbatim.
///
/// The daemon is a fresh process that re-reads `fed.yaml` from scratch, so
/// anything that changed how *this* invocation interpreted the config has to
/// be replayed on its command line or it will supervise a different stack
/// than the one that was started. Named fields rather than a row of
/// positional `&[String]`s, since `profiles` and `variants` are otherwise
/// indistinguishable at the call site.
#[derive(Debug, Clone, Default)]
pub struct InheritedFlags {
    /// From `--offline`.
    pub offline: bool,
    /// From `--profile`.
    pub profiles: Vec<String>,
    /// From `--variant`.
    pub variants: Vec<String>,
}

/// The full argument list for a `fed supervise` child, as a pure function of
/// the parent's own settings.
///
/// Split out from [`spawn_if_needed`] so the forwarding can be asserted
/// directly — spawning a real daemon to read its `/proc/<pid>/cmdline` would
/// be a slow and flaky way to test a list of strings.
fn supervisor_args(
    work_dir: &Path,
    config_path: &Path,
    flags: &InheritedFlags,
) -> Vec<std::ffi::OsString> {
    let mut args: Vec<std::ffi::OsString> = vec![
        "--workdir".into(),
        work_dir.into(),
        "--config".into(),
        config_path.into(),
    ];
    if flags.offline {
        args.push("--offline".into());
    }
    for profile in &flags.profiles {
        args.push("--profile".into());
        args.push(profile.into());
    }
    // The daemon re-reads fed.yaml in a fresh process, so it must be told the
    // same variant selection — otherwise it would resolve a service's variants
    // from `.fed/variants.yaml`/`default_variant` alone and could end up
    // supervising a different implementation than the one actually running.
    for variant in &flags.variants {
        args.push("--variant".into());
        args.push(variant.into());
    }
    args.push("supervise".into());
    args
}

/// Spawn a detached `fed supervise` for `work_dir`, unless one is already
/// running.
///
/// Called after background start/restart dispatch, or before waiting on an
/// interactive service. A fresh daemon attaches to the committed service state. `fed status` calls neither this
/// nor any other respawn logic; it only reads the lock file for display,
/// staying strictly read-only.
///
/// Daemonization: `fed supervise` is fed's own binary, so detachment
/// doesn't need the `nohup bash -c` shell-wrapper trick `ProcessService`
/// uses for opaque user commands — SIGHUP is ignored
/// directly at the top of `main()` (see `main.rs`), and `.process_group(0)`
/// here isolates the daemon from job-control signals the same way
/// `ProcessService::spawn_process` does for detached services
/// (`src/service/process.rs:265-266`). No `daemonize`/fork-crate dependency,
/// no double-fork: the parent doesn't wait on the child, which is sufficient
/// once SIGHUP is handled.
pub fn spawn_if_needed(
    work_dir: &Path,
    config_path: &Path,
    flags: &InheritedFlags,
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
    cmd.args(supervisor_args(work_dir, config_path, flags));

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

/// Whether any started service needs health monitoring or restart/dependency
/// supervision. Uses the same scope as the daemon itself.
pub fn any_needs_supervision(
    config: &Config,
    service_names: impl IntoIterator<Item = impl AsRef<str>>,
) -> bool {
    let scope = fed::orchestrator::supervised_service_names(config);
    service_names
        .into_iter()
        .any(|name| scope.contains(name.as_ref()))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args_of(flags: &InheritedFlags) -> Vec<String> {
        supervisor_args(Path::new("/w"), Path::new("/w/fed.yaml"), flags)
            .into_iter()
            .map(|a| a.to_string_lossy().into_owned())
            .collect()
    }

    #[test]
    fn supervisor_inherits_no_flags_when_none_were_given() {
        assert_eq!(
            args_of(&InheritedFlags::default()),
            vec!["--workdir", "/w", "--config", "/w/fed.yaml", "supervise"]
        );
    }

    /// `--variant` must reach the daemon verbatim, entry by entry: it
    /// re-resolves every service's variants from scratch, so a dropped flag
    /// means it supervises a different implementation than the one `fed
    /// start` launched.
    #[test]
    fn supervisor_inherits_profiles_and_variants_verbatim() {
        let args = args_of(&InheritedFlags {
            offline: true,
            profiles: vec!["full".to_string()],
            variants: vec!["go,ts".to_string(), "catalog:java".to_string()],
        });
        assert_eq!(
            args,
            vec![
                "--workdir",
                "/w",
                "--config",
                "/w/fed.yaml",
                "--offline",
                "--profile",
                "full",
                "--variant",
                "go,ts",
                "--variant",
                "catalog:java",
                "supervise",
            ]
        );
    }

    /// The comma list is passed through as one argument rather than split
    /// here: `--variant` parsing lives in one place (`config::variants`), and
    /// splitting in both would be two implementations to keep in agreement.
    #[test]
    fn supervisor_does_not_split_a_comma_list() {
        let args = args_of(&InheritedFlags {
            variants: vec!["go,ts,rust".to_string()],
            ..Default::default()
        });
        assert!(args.contains(&"go,ts,rust".to_string()));
        assert_eq!(
            args.iter().filter(|a| *a == "--variant").count(),
            1,
            "one entry must stay one argument"
        );
    }
}
