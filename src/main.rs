mod cli;
mod commands;
mod output;

use crate::output::UserOutput;
use std::io::IsTerminal;
use std::path::PathBuf;

use clap::{CommandFactory, Parser};
use cli::{Cli, Commands};
use fed::{
    Error as FedError, Orchestrator, OutputMode, Parser as ConfigParser, RunContext,
    orchestrator::StartLock,
};

/// Best-effort discovery of the workspace directory for the current
/// invocation. Used by the recursion check to compare the child's
/// workspace with the parent's `FED_SPAWNED_FROM_WORKSPACE`.
///
/// Peeks at argv for `-w <path>` / `--workdir <path>` /
/// `--workdir=<path>` *before* clap runs, then falls back to
/// `current_dir()`. Returns None on any failure; the caller treats
/// None as "can't tell, be conservative."
fn detect_current_workspace() -> Option<PathBuf> {
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        if arg == "-w" || arg == "--workdir" {
            if let Some(path) = args.next() {
                return Some(PathBuf::from(path));
            }
        } else if let Some(rest) = arg.strip_prefix("--workdir=") {
            return Some(PathBuf::from(rest));
        } else if let Some(rest) = arg.strip_prefix("-w=") {
            return Some(PathBuf::from(rest));
        }
    }

    std::env::current_dir().ok()
}

/// Compare two paths for canonical equality. Falls back to lexical
/// equality if either side fails to canonicalize (file may not exist
/// yet, etc.).
fn canonicalize_eq(a: &str, b: &PathBuf) -> bool {
    let a = std::path::Path::new(a);
    match (std::fs::canonicalize(a), std::fs::canonicalize(b)) {
        (Ok(ac), Ok(bc)) => ac == bc,
        _ => a == b.as_path(),
    }
}

#[tokio::main]
async fn main() {
    if let Err(e) = run().await {
        // Script failures: the script's own stderr is the user feedback.
        // Just propagate the exit code without printing a redundant error.
        if let Some(FedError::ScriptFailed { exit_code, .. }) = e.downcast_ref::<FedError>() {
            std::process::exit(*exit_code);
        }

        // All other errors: print with suggestions
        if let Some(fed_error) = e.downcast_ref::<FedError>() {
            eprintln!("Error: {}", fed_error);
            if let Some(suggestion) = fed_error.suggestion() {
                eprintln!("\nHint: {}", suggestion);
            }
        } else {
            eprintln!("Error: {:#}", e);
        }
        std::process::exit(1);
    }
}

async fn run() -> anyhow::Result<()> {
    // CRITICAL: Detect *same-workspace* circular dependency before
    // doing anything else.
    //
    // The original check fired on every nested fed invocation regardless
    // of workspace. That blocks legitimate cross-config patterns — e.g.
    // an orchestrator service running under one fed config that shells
    // out to fed for managed environments under a *different* config.
    // The infinite loop only happens when the same workspace recurses;
    // cross-config calls terminate naturally.
    //
    // Heuristic: the spawning fed sets FED_SPAWNED_FROM_WORKSPACE to
    // its own canonical work_dir. The child compares against its own
    // canonical work_dir (current_dir() or `--workdir`). Same dir →
    // recursion; different dir → independent invocation, allow.
    //
    // FED_ALLOW_RECURSION=1 escapes the check entirely for unusual cases.
    if std::env::var("FED_ALLOW_RECURSION").ok().as_deref() != Some("1")
        && let Ok(parent_service) = std::env::var("FED_SPAWNED_BY_SERVICE")
    {
        let parent_workspace = std::env::var("FED_SPAWNED_FROM_WORKSPACE").ok();
        let child_workspace = detect_current_workspace();

        let same_workspace = match (&parent_workspace, &child_workspace) {
            (Some(p), Some(c)) => canonicalize_eq(p, c),
            // Older fed binaries (pre-3.6.3) didn't set
            // FED_SPAWNED_FROM_WORKSPACE. Be conservative: treat as
            // same-workspace and block, matching the old behavior.
            _ => true,
        };

        if same_workspace {
            eprintln!("Error: Circular dependency detected!");
            eprintln!();
            eprintln!(
                "  Service '{}' invoked 'fed' against the same workspace,",
                parent_service
            );
            eprintln!("  which would create an infinite loop.");
            eprintln!();
            eprintln!("  Detected call chain:");
            eprintln!("    fed start -> {} (process) -> fed", parent_service);
            eprintln!();
            eprintln!("  Fix: Change the process command in your fed config to run");
            eprintln!("  the actual application directly instead of invoking 'fed'.");
            eprintln!();
            eprintln!("  Example: Instead of 'npm run dev' where dev runs 'fed start',");
            eprintln!("  use 'npx next dev' or the direct command.");
            eprintln!();
            eprintln!("  Cross-config use case (orchestrator service shelling out to fed");
            eprintln!("  for a different workspace): set FED_ALLOW_RECURSION=1 in the");
            eprintln!("  child's environment, or invoke fed against a different");
            eprintln!("  --workdir / cwd.");
            std::process::exit(1);
        }
    }

    // Check if help is requested - we'll add scripts after standard help
    let args: Vec<String> = std::env::args().collect();
    // Only intercept top-level help (e.g., `fed --help`, `fed -h`, `fed help`).
    // Subcommand help like `fed docker build --help` is handled by clap.
    let wants_help = if args.len() == 2 {
        args[1] == "--help" || args[1] == "-h" || args[1] == "help"
    } else {
        false
    };

    if wants_help {
        // Print standard help first
        use clap::CommandFactory;
        let mut cmd = Cli::command();
        cmd.print_help().ok();
        println!();

        // Try to load config to show available scripts
        let parser = ConfigParser::new();
        if let Ok(config_path) = parser.find_config_file()
            && let Ok(config) = parser.load_config(&config_path)
            && !config.scripts.is_empty()
        {
            println!();
            println!("Scripts (run with `fed <script>` or `fed run <script>`):");
            let mut script_names: Vec<_> = config.scripts.keys().collect();
            script_names.sort();
            for name in script_names {
                println!("  {}", name);
            }
        }
        return Ok(());
    }

    let cli = Cli::parse();

    // The detached commands (`fed supervise`, `fed host`) ignore SIGHUP at
    // the very start, before anything else runs — this is what lets them
    // survive terminal close. Unlike a user `process:` command (which needs
    // the `nohup bash -c` shell-wrapper trick because it's an opaque shell
    // string), they are fed's own binary, so they can just ignore the
    // signal directly rather than needing a wrapping shell.
    // Placed here, immediately after argv parsing and before tracing/config
    // I/O, so there's no window where a terminal-close SIGHUP could still
    // reach the default handler.
    if is_daemon_command(&cli.command) {
        #[cfg(unix)]
        unsafe {
            let _ = nix::sys::signal::signal(
                nix::sys::signal::Signal::SIGHUP,
                nix::sys::signal::SigHandler::SigIgn,
            );
        }
    }

    // -e/--env was removed in fed 8.0 (the development/staging/production
    // axis no longer exists). The flag is kept registered (hidden, optional)
    // so a stale invocation gets this explicit migration error instead of a
    // generic clap "unexpected argument" failure. Checked immediately after
    // parsing, before any config loading happens.
    if cli.env.is_some() {
        eprintln!(
            "error: -e/--env was removed in fed 8.0 — the development/staging/production axis no longer exists. Move deployment-specific parameter values into an env_file instead (see env_file: in fed.yaml docs)."
        );
        std::process::exit(1);
    }

    // Initialize tracing and output
    let is_tui = matches!(cli.command, Commands::Tui { .. });
    // A detached command has no terminal to print to, so its tracing output
    // goes to a file under `.fed/logs/` instead.
    let daemon_log = match &cli.command {
        Commands::Supervise => Some("supervisor.log".to_string()),
        #[cfg(unix)]
        Commands::Host { service } => Some(format!(
            "{}-host.log",
            fed::fed_dir::service_file_stem(service)
        )),
        _ => None,
    };
    let is_tty = std::io::stderr().is_terminal();
    let is_interactive = std::io::stdin().is_terminal();
    init_tracing(is_tui, daemon_log, cli.workdir.clone(), cli.verbose, is_tty)?;
    let out = output::CliOutput::new(is_tty);

    // Session-scoped run settings, threaded through every command and into
    // OrchestratorBuilder. `output_mode`/`required_secret_names` depend on
    // `cli.command` (tier-1 dispatch below) and, for the latter, on
    // `config.scripts` (loaded after tier 1/2), so they're filled in with
    // their tier-1 defaults here and finalized just before the
    // orchestrator-needing tier — see the reassignment below. Tier-1
    // commands (`Ports`/`Isolate`) don't read either field, so the partial
    // context is correct for them as-is.
    let mut run_context = RunContext {
        offline: cli.offline,
        secret_cache: cli.secret_cache.unwrap_or_default().effective(),
        is_interactive,
        output_mode: OutputMode::default(),
        profiles: cli.profile.clone(),
        variants: cli.variant.clone(),
        required_secret_names: None,
        foreground: None,
    };

    // ── Tier 1: Commands that need NO config ──────────────────────────
    match &cli.command {
        #[cfg(unix)]
        Commands::Attach {
            service,
            no_stdin,
            detach_keys,
        } => {
            let config_path = cli.config.clone().unwrap_or_else(|| {
                if cli.workdir.is_some() {
                    PathBuf::from("fed.yaml")
                } else {
                    ConfigParser::new()
                        .find_config_file()
                        .unwrap_or_else(|_| PathBuf::from("fed.yaml"))
                }
            });
            let work_dir = resolve_work_dir(cli.workdir.clone(), &config_path)?;
            let code = commands::run_attach(work_dir, service, *no_stdin, detach_keys).await?;
            std::process::exit(code);
        }
        Commands::Init { output, force } => {
            return commands::run_init(output, *force, &out);
        }
        Commands::Validate => {
            return commands::run_validate(
                cli.config.clone(),
                cli.workdir.clone(),
                cli.offline,
                &cli.variant,
                &cli.profile,
                &out,
            )
            .await;
        }
        Commands::Variant { cmd } => {
            return commands::run_variant(
                &cmd.clone().unwrap_or_default(),
                cli.workdir.clone(),
                cli.config.clone(),
                &cli.variant,
                cli.offline,
                &out,
            )
            .await;
        }
        Commands::Completions { shell } => {
            let mut cmd = Cli::command();
            let bin_name = cmd.get_name().to_string();
            clap_complete::generate(*shell, &mut cmd, bin_name, &mut std::io::stdout());
            return Ok(());
        }
        Commands::Doctor => {
            return commands::run_doctor(&out).await;
        }
        Commands::Prune { force } => {
            return commands::run_prune(*force, &out).await;
        }
        Commands::Package(package_cmd) => {
            return commands::run_package(package_cmd, &out).await;
        }
        Commands::Ports { cmd: ports_cmd } => {
            return commands::run_ports(
                &ports_cmd.clone().unwrap_or_default(),
                cli.workdir.clone(),
                cli.config.clone(),
                run_context.clone(),
                &out,
            )
            .await;
        }
        Commands::Workspace(ws_cmd) => {
            return commands::run_workspace(ws_cmd, &out).await;
        }
        Commands::Login {
            no_browser,
            print_url,
            label,
            url,
        } => {
            return commands::run_login(*no_browser, *print_url, label.clone(), url.clone(), &out)
                .await;
        }
        Commands::Logout => {
            return commands::run_logout(cli.offline, &out).await;
        }
        Commands::Whoami => {
            return commands::run_whoami(&out).await;
        }
        Commands::Link { target } => {
            return commands::run_link(target.clone(), cli.workdir.clone(), &out).await;
        }
        Commands::Secrets(secrets_cmd) => {
            return commands::run_secrets(secrets_cmd, cli.workdir.clone(), &out).await;
        }
        Commands::Isolate(isolate_cmd) => {
            return commands::run_isolate(
                isolate_cmd,
                cli.workdir.clone(),
                cli.config.clone(),
                run_context.clone(),
                &out,
            )
            .await;
        }
        // The host resolves nothing: `fed start` hands it a launch spec on
        // stdin, so it needs neither the config nor the work dir. Its
        // blocking pty and socket loops get a thread of their own so they
        // never sit on a runtime thread.
        #[cfg(unix)]
        Commands::Host { service } => {
            let service = service.clone();
            return tokio::task::spawn_blocking(move || commands::run_host(&service)).await?;
        }
        _ => {} // fall through to config-loading path
    }

    // ── Load config ─────────────────────────────────────────────────
    let parser = ConfigParser::new();
    let config_path = if let Some(path) = cli.config.clone() {
        path
    } else {
        if let Some(workdir) = cli.workdir.as_deref() {
            ConfigParser::find_config_in_dir(workdir)?
        } else {
            parser.find_config_file()?
        }
    };

    // ── Tier 2: Commands that need config but NOT orchestrator ──────
    match &cli.command {
        Commands::Docker(docker_cmd) => {
            let config = parser.load_config(&config_path)?;
            let work_dir = resolve_work_dir(cli.workdir, &config_path)?;
            match docker_cmd {
                cli::DockerCommands::Build {
                    services,
                    tag,
                    build_args,
                    json,
                } => {
                    return commands::run_docker_build(
                        &config,
                        &work_dir,
                        services.clone(),
                        tag.clone(),
                        build_args.clone(),
                        *json,
                        &out,
                    )
                    .await;
                }
                cli::DockerCommands::Push { services, tag } => {
                    return commands::run_docker_push(
                        &config,
                        &work_dir,
                        services.clone(),
                        tag.clone(),
                        &out,
                    )
                    .await;
                }
            }
        }
        Commands::Debug(debug_cmd) => {
            let config = parser.load_config(&config_path)?;
            let work_dir = resolve_work_dir(cli.workdir, &config_path)?;

            let debug_command = match debug_cmd {
                cli::DebugCommands::State { .. } => commands::DebugCommand::State,
                cli::DebugCommands::Ports { .. } => commands::DebugCommand::Ports,
                cli::DebugCommands::CircuitBreaker { service, .. } => {
                    commands::DebugCommand::CircuitBreaker {
                        service: service.clone(),
                    }
                }
            };

            let json = match debug_cmd {
                cli::DebugCommands::State { json } => *json,
                cli::DebugCommands::Ports { json } => *json,
                cli::DebugCommands::CircuitBreaker { json, .. } => *json,
            };

            return commands::run_debug(debug_command, &config, work_dir, json, &out).await;
        }
        _ => {} // fall through to orchestrator path
    }

    // ── Load config with package resolution (uses cache-only in offline mode) ──
    let config_result = async {
        let config = parser
            .load_config_with_packages_offline(&config_path, cli.offline)
            .await?;
        config.validate()?;
        Ok::<_, anyhow::Error>(config)
    }
    .await;

    // If config loading fails and we're stopping, fall back to state-tracker-only stop
    let config = match (&cli.command, config_result) {
        (Commands::Stop { services }, Err(config_err)) => {
            eprintln!(
                "Warning: Config invalid ({}), stopping from state tracker",
                config_err
            );
            let work_dir = resolve_work_dir(cli.workdir, &config_path)
                .unwrap_or_else(|_| std::env::current_dir().unwrap_or_default());
            commands::run_stop_from_state(&work_dir, services.clone(), &out).await?;
            return Ok(());
        }
        (_, Err(e)) => return Err(e),
        (_, Ok(config)) => config,
    };

    // Non-breaking: warn on unknown (typo'd) config keys before starting.
    if matches!(cli.command, Commands::Start { .. }) {
        commands::emit_config_warnings(&config, &out);
    }

    let work_dir = resolve_work_dir(cli.workdir.clone(), &config_path)?;

    run_context.secret_cache = cli
        .secret_cache
        .or_else(|| fed::cloud::load_link(&work_dir).map(|link| link.secret_cache))
        .unwrap_or_default()
        .effective();

    // The supervisor must recover each running service's actual variant/profile,
    // rather than resolving everything from the latest caller's preferences.
    if matches!(cli.command, Commands::Supervise) {
        return commands::run_supervise(config, work_dir, run_context).await;
    }

    // Pick an implementation for every service that declares `variants:` and
    // replace it with the merged result, before anything — the orchestrator,
    // `fed status`, the dry-run preview, the supervisor it may spawn — reads
    // `config.services`. Everything downstream sees ordinary services.
    let mut config = config;
    fed::config::variants::resolve_variants(
        &mut config,
        &fed::config::variants::VariantSelection::load(&cli.variant, &work_dir)?,
        &cli.profile,
    )?;
    let config = config;

    // Resolved before the start lock and any orchestrator, so a rejected
    // `fed start -i` leaves the stack untouched.
    if let Commands::Start {
        interactive: true,
        services,
        ..
    } = &cli.command
    {
        run_context.foreground = Some(commands::resolve_foreground_target(
            &config,
            services,
            &cli.profile,
        )?);
    }

    // Parameter resolution and service registration are two separate phases.
    // Serialize real `fed start` invocations before either phase so concurrent
    // commands cannot resolve different port sets and then split registration
    // wins between them. Dry-run is read-only and must remain non-blocking.
    let _restart_lock = if matches!(cli.command, Commands::Restart { .. }) {
        Some(StartLock::acquire(&work_dir).await?)
    } else {
        None
    };
    let start_lock = match &cli.command {
        Commands::Start { dry_run: false, .. } => Some(StartLock::acquire(&work_dir).await?),
        _ => None,
    };

    // ── Tier 3: Commands that need orchestrator ─────────────────────

    // Determine output mode before initializing.
    // File mode (background) disables the monitoring task since we don't need it for:
    // - Start without watch (we exit immediately after starting)
    // - Stop (we're stopping services, not monitoring them)
    // - Restart (same as start without watch)
    // - Status/Logs (read-only operations)
    let output_mode = match &cli.command {
        Commands::Start {
            watch,
            output,
            dry_run,
            ..
        } => {
            // Dry run doesn't need any output mode setup since we won't start services
            if *dry_run {
                OutputMode::Captured
            } else if let Some(mode) = output {
                match mode.parse::<OutputMode>() {
                    Ok(m) => m,
                    Err(e) => {
                        eprintln!("{}", e);
                        std::process::exit(1);
                    }
                }
            } else if *watch {
                // Watch mode defaults to captured (interactive)
                OutputMode::Captured
            } else {
                // Start without watch defaults to file (background)
                OutputMode::File
            }
        }
        Commands::Stop { .. }
        | Commands::Restart { .. }
        | Commands::Status { .. }
        | Commands::Logs { .. } => OutputMode::File,
        Commands::Tui { .. } => OutputMode::Captured,
        Commands::Run {
            output: Some(mode), ..
        } => match mode.parse::<OutputMode>() {
            Ok(m) => m,
            Err(e) => {
                eprintln!("{}", e);
                std::process::exit(1);
            }
        },
        _ => OutputMode::Captured,
    };

    // Status reads persisted state without resolving parameters. Logs and stop
    // must fully resolve imported Compose projects because Compose interpolates
    // its model for those commands too.
    let readonly = matches!(cli.command, Commands::Status { .. });

    // --isolate enables isolation mode (randomize ports + unique container names)
    let isolate = matches!(&cli.command, Commands::Start { isolate: true, .. });

    let randomize = isolate;

    // --replace kills blocking processes/containers and uses original ports
    let replace = matches!(&cli.command, Commands::Start { replace: true, .. });

    // start --dry-run should resolve configuration without mutating persisted state
    let dry_run = matches!(&cli.command, Commands::Start { dry_run: true, .. });

    // For script commands, check if the script has isolated: true
    // If so, set auto_resolve_conflicts to avoid prompts for ports that will be re-allocated anyway
    let auto_resolve = match &cli.command {
        Commands::Run { name, .. } => config
            .scripts
            .get(name)
            .map(|s| s.isolated)
            .unwrap_or(false),
        Commands::External(args) if !args.is_empty() => config
            .scripts
            .get(&args[0])
            .map(|s| s.isolated)
            .unwrap_or(false),
        _ => false,
    };

    // Scope the vault query to what the target script transitively references.
    // Only script-running commands are scoped; interactive `fed`, `fed start`,
    // and unknown commands fetch every missing manual secret (None).
    let required_secret_names: Option<std::collections::HashSet<String>> = {
        let scoped_script = match &cli.command {
            Commands::Run { name, .. } if config.scripts.contains_key(name) => Some(name.clone()),
            Commands::External(args)
                if !args.is_empty() && config.scripts.contains_key(&args[0]) =>
            {
                Some(args[0].clone())
            }
            _ => None,
        };
        scoped_script.map(|name| fed::parameter::scanner::required_parameter_names(&config, &name))
    };

    // Finalize the run context now that output_mode/required_secret_names
    // are known (they depend on cli.command and, for the latter, on
    // config.scripts — neither is available at the tier-1 dispatch point
    // above).
    run_context.output_mode = output_mode;
    run_context.required_secret_names = required_secret_names;

    // If --isolate flag is used (and not dry-run), persist isolation mode before building orchestrator
    if isolate && !dry_run {
        let work_dir_for_isolation = resolve_work_dir(cli.workdir.clone(), &config_path)?;
        let mut tracker = fed::state::StateTracker::new(work_dir_for_isolation).await?;
        tracker.initialize().await?;
        let (already_isolated, _) = tracker.get_isolation_mode().await;
        if !already_isolated {
            let isolation_id = format!("iso-{:08x}", rand::random::<u32>());
            tracker.set_isolation_mode(true, Some(isolation_id)).await?;
        }
    }

    // Watch/tui pre-flight handoff: a foreground `--watch`/`fed tui`
    // orchestrator starts its own in-process monitoring loop as part of
    // `initialize()`, *before* `run_watch_mode`/`run_tui` is ever reached —
    // so tearing down a leftover background supervisor has to happen here,
    // before `Orchestrator::builder()...build()` below, not inside the
    // watch loop after the fact. Otherwise there would be a window (however
    // brief) with two live monitoring loops racing over the same services.
    // A plain lock-file read/SIGTERM; no `Orchestrator`/`StateTracker`
    // needed for this step. Once this foreground session exits, nothing
    // auto-respawns the daemon — it comes back only via the next `fed
    // start`/`fed restart` (`spawn_if_needed`, wired into both below).
    let is_watch_or_tui = matches!(
        &cli.command,
        Commands::Start {
            watch: true,
            dry_run: false,
            ..
        } | Commands::Tui { .. }
    );
    let refresh_supervisor = matches!(
        &cli.command,
        Commands::Start {
            watch: false,
            interactive: false,
            dry_run: false,
            ..
        } | Commands::Restart { .. }
    );
    let previous_supervisor =
        fed::orchestrator::supervisor::live_supervisor_pid(&work_dir).is_some();
    let resume_work_dir = work_dir.clone();
    let resume_config_path = config_path.clone();
    let resume_flags = commands::InheritedFlags {
        offline: cli.offline,
        profiles: cli.profile.clone(),
        variants: cli.variant.clone(),
    };
    let needs_supervision = commands::any_needs_supervision(&config, config.services.keys());
    // Mutating starts/restarts replace managers and can change variants. Stop
    // the old observer before that mutation, then reattach to committed state.
    if is_watch_or_tui || refresh_supervisor {
        let stopped = fed::orchestrator::supervisor::signal_stop_and_wait(
            &work_dir,
            std::time::Duration::from_secs(10),
        )
        .await;
        anyhow::ensure!(
            stopped,
            "Could not stop the previous supervisor before changing services"
        );
    }

    let result = async {
        // Build orchestrator with all settings applied and initialized
        let mut orchestrator = Orchestrator::builder()
            .config(config.clone())
            .work_dir(work_dir)
            .run_context(run_context)
            .randomize_ports(randomize)
            .replace_mode(replace)
            .dry_run(dry_run)
            .auto_resolve_conflicts(auto_resolve)
            .readonly(readonly)
            .build()
            .await?;

        match cli.command {
            Commands::Start {
                all,
                services,
                watch,
                replace,
                output: _,
                interactive: _,
                dry_run,
                isolate: _,
                jobs,
            } => {
                let exit_code = commands::run_start(
                    &mut orchestrator,
                    &config,
                    services,
                    commands::StartOptions {
                        all,
                        watch,
                        replace,
                        dry_run,
                        jobs: jobs as usize,
                        config_path: &config_path,
                        flags: commands::InheritedFlags {
                            offline: cli.offline,
                            profiles: cli.profile.clone(),
                            variants: cli.variant.clone(),
                        },
                        profiles: cli.profile.clone(),
                        start_lock,
                    },
                    &out,
                )
                .await?;

                // Every state write is already committed, so exiting here
                // only skips the remaining drops.
                if let Some(code) = exit_code {
                    use std::io::Write;
                    let _ = std::io::stdout().flush();
                    let _ = std::io::stderr().flush();
                    std::process::exit(code);
                }
            }
            Commands::Stop { services } => {
                commands::run_stop(&mut orchestrator, &config, services, &out).await?;
            }
            Commands::Restart { services, all: _ } => {
                commands::run_restart(&mut orchestrator, &config, services, &out).await?;
            }
            Commands::Status { json, tag } => {
                commands::run_status(&orchestrator, &config, json, tag, &out).await?;
            }
            Commands::Logs {
                service,
                tail,
                follow,
            } => {
                commands::run_logs(&orchestrator, &service, tail, follow, &out).await?;
            }
            Commands::Tui { watch } => {
                commands::run_tui(orchestrator, watch, Some(&config)).await?;
            }
            Commands::Run { name, args, .. } => {
                // Strip leading "--" from args (clap captures it literally)
                let extra_args: Vec<String> =
                    args.into_iter().skip_while(|arg| arg == "--").collect();
                commands::run_script(&mut orchestrator, &name, &extra_args, false, &out).await?;
            }
            Commands::External(args) => {
                // Handle `fed <script>` shorthand - first arg is the script name
                // Extra args after script name are passed to the script
                if args.is_empty() {
                    anyhow::bail!("No script name provided");
                }
                let script_name = &args[0];

                // Extract extra arguments (everything after the script name)
                // Skip leading "--" since clap's external_subcommand captures it literally
                // e.g., `fed test -- -t auth` gives args = ["test", "--", "-t", "auth"]
                let extra_args: Vec<String> = args
                    .iter()
                    .skip(1)
                    .skip_while(|arg| *arg == "--")
                    .cloned()
                    .collect();

                let available_scripts = orchestrator.list_scripts();
                if available_scripts.contains(script_name) {
                    commands::run_script(&mut orchestrator, script_name, &extra_args, false, &out)
                        .await?;
                } else {
                    // Suggest the closest script or built-in command for typos
                    // like `fed strat` or `fed migrte`.
                    const BUILTIN_COMMANDS: &[&str] = &[
                        "start",
                        "login",
                        "logout",
                        "whoami",
                        "link",
                        "secrets",
                        "stop",
                        "restart",
                        "status",
                        "logs",
                        "tui",
                        "run",
                        "install",
                        "clean",
                        "build",
                        "package",
                        "ports",
                        "init",
                        "validate",
                        "variant",
                        "completions",
                        "doctor",
                        "top",
                        "debug",
                        "docker",
                        "isolate",
                        "workspace",
                    ];
                    let candidates = available_scripts
                        .iter()
                        .map(String::as_str)
                        .chain(BUILTIN_COMMANDS.iter().copied());
                    eprintln!(
                        "{}",
                        commands::suggest::with_did_you_mean(
                            &format!("Unknown command or script: '{}'.", script_name),
                            script_name,
                            candidates,
                        )
                    );
                    if !available_scripts.is_empty() {
                        eprintln!("\nAvailable scripts:");
                        let mut names = available_scripts.clone();
                        names.sort();
                        for script in names {
                            eprintln!("  - {}", script);
                        }
                    }
                    eprintln!("\nRun 'fed --help' for available commands.");
                    std::process::exit(1);
                }
            }
            Commands::Install { services } => {
                commands::run_install(&orchestrator, &config, services, &out).await?;
            }
            Commands::Clean { services } => {
                commands::run_clean(&orchestrator, &config, services, &out).await?;
            }
            Commands::Build {
                services,
                tag,
                build_args,
                json,
            } => {
                commands::run_build(
                    &orchestrator,
                    &config,
                    services,
                    tag,
                    build_args,
                    json,
                    &out,
                )
                .await?;
            }
            Commands::Top { interval } => {
                commands::run_top(&orchestrator, interval, &out).await?;
            }
            #[cfg(unix)]
            Commands::Attach { .. } => unreachable!("handled in earlier dispatch tiers"),
            // Handled in earlier tiers
            Commands::Init { .. }
            | Commands::Validate
            | Commands::Variant { .. }
            | Commands::Completions { .. }
            | Commands::Doctor
            | Commands::Prune { .. }
            | Commands::Package(_)
            | Commands::Ports { .. }
            | Commands::Docker(_)
            | Commands::Debug(_)
            | Commands::Workspace(_)
            | Commands::Isolate(_)
            | Commands::Login { .. }
            | Commands::Logout
            | Commands::Whoami
            | Commands::Link { .. }
            | Commands::Secrets(_)
            | Commands::Supervise => {
                unreachable!("handled in earlier dispatch tiers");
            }
            #[cfg(unix)]
            Commands::Host { .. } => {
                unreachable!("handled in earlier dispatch tiers");
            }
        }

        Ok(())
    }
    .await;

    // A failed start must not leave previously running services unsupervised.
    if refresh_supervisor
        && (previous_supervisor || needs_supervision)
        && let Err(error) = commands::spawn_supervisor_if_needed(
            &resume_work_dir,
            &resume_config_path,
            &resume_flags,
        )
    {
        out.warning(&format!("Failed to resume the service supervisor: {error}"));
    }
    result
}

/// True for the subcommands that run detached from the caller's terminal,
/// which therefore ignore SIGHUP and trace to a file.
fn is_daemon_command(command: &Commands) -> bool {
    #[cfg(unix)]
    if matches!(command, Commands::Host { .. }) {
        return true;
    }
    matches!(command, Commands::Supervise)
}

/// Resolve the work directory from CLI `--workdir` or the config file's parent directory.
fn resolve_work_dir(
    workdir: Option<std::path::PathBuf>,
    config_path: &std::path::Path,
) -> anyhow::Result<std::path::PathBuf> {
    if let Some(workdir) = workdir {
        return std::fs::canonicalize(&workdir).map_err(|error| {
            anyhow::anyhow!(
                "Cannot resolve working directory '{}': {}",
                workdir.display(),
                error
            )
        });
    }
    if let Some(parent) = config_path.parent() {
        if parent.as_os_str().is_empty() {
            Ok(std::env::current_dir()?)
        } else {
            // Detached host identity and persisted socket paths must stay
            // stable when later commands run from a different directory.
            std::fs::canonicalize(parent).map_err(|error| {
                anyhow::anyhow!(
                    "Cannot resolve working directory '{}': {}",
                    parent.display(),
                    error
                )
            })
        }
    } else {
        Ok(std::env::current_dir()?)
    }
}

fn init_tracing(
    is_tui: bool,
    daemon_log: Option<String>,
    workdir: Option<PathBuf>,
    verbose: bool,
    is_tty: bool,
) -> anyhow::Result<()> {
    if let Some(daemon_log) = daemon_log {
        // A detached daemon has no attached terminal to print to — logs go
        // to `.fed/logs/<daemon>.log`, matching the existing per-service log
        // convention.
        // `--workdir` is always passed explicitly by `spawn_if_needed`
        // (the only thing that ever spawns `fed supervise`), so this is
        // reliable; a bare manual invocation without `--workdir` falls back
        // to the current directory, same as every other command.
        let work_dir = workdir.unwrap_or_else(|| PathBuf::from("."));
        let log_dir = work_dir.join(".fed").join("logs");
        std::fs::create_dir_all(&log_dir)?;

        let log_path = log_dir.join(daemon_log);
        let log_file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&log_path)?;

        tracing_subscriber::fmt()
            .with_env_filter(
                tracing_subscriber::EnvFilter::try_from_default_env()
                    .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
            )
            .with_writer(std::sync::Mutex::new(log_file))
            .with_ansi(false)
            .init();
    } else if is_tui {
        // For TUI mode, write logs to a file
        let log_dir = dirs::home_dir()
            .unwrap_or_else(|| std::path::PathBuf::from("."))
            .join(".fed")
            .join("logs");
        std::fs::create_dir_all(&log_dir)?;

        let log_path = log_dir.join("tui.log");
        let log_file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&log_path)?;

        tracing_subscriber::fmt()
            .with_env_filter(
                tracing_subscriber::EnvFilter::try_from_default_env()
                    .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn")),
            )
            .with_writer(std::sync::Mutex::new(log_file))
            .with_ansi(false)
            .init();
    } else {
        let default_level = if verbose { "debug" } else { "info" };
        tracing_subscriber::fmt()
            .with_env_filter(
                tracing_subscriber::EnvFilter::try_from_default_env()
                    .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(default_level)),
            )
            .with_writer(std::io::stderr)
            .with_ansi(false)
            .event_format(output::CliFormatter::new(is_tty))
            .init();
    }

    Ok(())
}
