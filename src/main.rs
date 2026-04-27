mod cli;
mod commands;
mod output;

use std::io::IsTerminal;
use std::path::PathBuf;

use clap::{CommandFactory, Parser};
use cli::{Cli, Commands};
use fed::{Error as FedError, Orchestrator, OutputMode, Parser as ConfigParser};

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
    if std::env::var("FED_ALLOW_RECURSION").ok().as_deref() != Some("1") {
        if let Ok(parent_service) = std::env::var("FED_SPAWNED_BY_SERVICE") {
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
                eprintln!("  Fix: Change the process command in service-federation.yaml to run");
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
        if let Ok(config_path) = parser.find_config_file() {
            if let Ok(config) = parser.load_config(&config_path) {
                if !config.scripts.is_empty() {
                    println!();
                    println!("Scripts (run with `fed <script>` or `fed run <script>`):");
                    let mut script_names: Vec<_> = config.scripts.keys().collect();
                    script_names.sort();
                    for name in script_names {
                        println!("  {}", name);
                    }
                }
            }
        }
        return Ok(());
    }

    let cli = Cli::parse();

    // Initialize tracing and output
    let is_tui = matches!(cli.command, Commands::Tui { .. });
    let is_tty = std::io::stderr().is_terminal();
    let is_interactive = std::io::stdin().is_terminal();
    init_tracing(is_tui, cli.verbose, is_tty)?;
    let out = output::CliOutput::new(is_tty);

    // ── Tier 1: Commands that need NO config ──────────────────────────
    match &cli.command {
        Commands::Init { output, force } => {
            return commands::run_init(output, *force, &out);
        }
        Commands::Validate => {
            return commands::run_validate(cli.config.clone(), &out);
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
        Commands::Package(package_cmd) => {
            return commands::run_package(package_cmd, &out).await;
        }
        Commands::Ports(ref ports_cmd) => {
            return commands::run_ports(ports_cmd, cli.workdir.clone(), cli.config.clone(), &out)
                .await;
        }
        Commands::Workspace(ref ws_cmd) => {
            return commands::run_workspace(ws_cmd, &out).await;
        }
        Commands::Isolate(ref isolate_cmd) => {
            return commands::run_isolate(
                isolate_cmd,
                cli.workdir.clone(),
                cli.config.clone(),
                &out,
            )
            .await;
        }
        _ => {} // fall through to config-loading path
    }

    // ── Load config ─────────────────────────────────────────────────
    let parser = ConfigParser::new();
    let config_path = if let Some(path) = cli.config.clone() {
        path
    } else {
        parser.find_config_file()?
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

    let work_dir = resolve_work_dir(cli.workdir.clone(), &config_path)?;

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
        _ => OutputMode::Captured,
    };

    // Read-only commands skip parameter resolution and Docker cleanup
    // to avoid interactive prompts and stale service recreation.
    let readonly = matches!(
        cli.command,
        Commands::Status { .. } | Commands::Logs { .. } | Commands::Stop { .. }
    );

    // --isolate enables isolation mode (randomize ports + unique container names)
    let isolate = matches!(&cli.command, Commands::Start { isolate: true, .. });

    // --randomize is deprecated, treat as --isolate
    let deprecated_randomize = matches!(
        &cli.command,
        Commands::Start {
            randomize: true,
            ..
        }
    );

    if deprecated_randomize {
        eprintln!("Warning: --randomize is deprecated. Use --isolate instead.");
    }

    let randomize = isolate || deprecated_randomize;

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

    // Build orchestrator with all settings applied and initialized
    let mut orchestrator = Orchestrator::builder()
        .config(config.clone())
        .work_dir(work_dir)
        .profiles(cli.profile.clone())
        .output_mode(output_mode)
        .randomize_ports(randomize)
        .replace_mode(replace)
        .dry_run(dry_run)
        .auto_resolve_conflicts(auto_resolve)
        .readonly(readonly)
        .is_interactive(is_interactive)
        .build()
        .await?;

    match cli.command {
        Commands::Start {
            services,
            watch,
            replace,
            output: _,
            dry_run,
            randomize: _,
            isolate: _,
        } => {
            commands::run_start(
                &mut orchestrator,
                &config,
                services,
                commands::StartOptions {
                    watch,
                    replace,
                    dry_run,
                    config_path: &config_path,
                },
                &out,
            )
            .await?;
        }
        Commands::Stop { services } => {
            commands::run_stop(&mut orchestrator, &config, services, &out).await?;
        }
        Commands::Restart { services } => {
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
        Commands::Run { name, args } => {
            // Strip leading "--" from args (clap captures it literally)
            let extra_args: Vec<String> = args.into_iter().skip_while(|arg| arg == "--").collect();
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
                eprintln!("Unknown command or script: '{}'", script_name);
                eprintln!("\nAvailable scripts:");
                for script in available_scripts {
                    eprintln!("  - {}", script);
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
        // Handled in earlier tiers
        Commands::Init { .. }
        | Commands::Validate
        | Commands::Completions { .. }
        | Commands::Doctor
        | Commands::Package(_)
        | Commands::Ports(_)
        | Commands::Docker(_)
        | Commands::Debug(_)
        | Commands::Workspace(_)
        | Commands::Isolate(_) => {
            unreachable!("handled in earlier dispatch tiers");
        }
    }

    Ok(())
}

/// Resolve the work directory from CLI `--workdir` or the config file's parent directory.
fn resolve_work_dir(
    workdir: Option<std::path::PathBuf>,
    config_path: &std::path::Path,
) -> anyhow::Result<std::path::PathBuf> {
    if let Some(workdir) = workdir {
        return Ok(workdir);
    }
    if let Some(parent) = config_path.parent() {
        if parent.as_os_str().is_empty() {
            Ok(std::env::current_dir()?)
        } else {
            Ok(parent.to_path_buf())
        }
    } else {
        Ok(std::env::current_dir()?)
    }
}

fn init_tracing(is_tui: bool, verbose: bool, is_tty: bool) -> anyhow::Result<()> {
    if is_tui {
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
