use clap::{Parser, Subcommand};
use fed::SecretCacheMode;
use std::path::PathBuf;

#[derive(Parser)]
#[command(name = "fed", version)]
#[command(about = "Run native apps and Docker dependencies as one local dev stack")]
pub struct Cli {
    /// Config file path (defaults to fed.yaml)
    #[arg(short, long)]
    pub config: Option<PathBuf>,

    /// Working directory
    #[arg(short, long)]
    pub workdir: Option<PathBuf>,

    /// Removed in fed 8.0 — kept hidden and optional so a stale invocation
    /// gets an explicit migration error instead of a clap parse failure.
    #[arg(short, long, hide = true)]
    pub env: Option<String>,

    /// Active profiles for conditional service startup (can be repeated)
    #[arg(short, long)]
    pub profile: Vec<String>,

    /// Which implementation to run for services that declare `variants:`.
    ///
    /// Takes an ordered preference list of variant names shared across
    /// services (`--variant go,ts,rust`: prefer Go, then TypeScript, then
    /// Rust, for whichever services offer them), or a `service:variant` pin
    /// for the odd one out (`--variant catalog:java`). Repeatable; a pin
    /// beats the list. `fed variant set` persists the same thing.
    ///
    /// Global, so `fed start --variant go` and `fed --variant go start` are
    /// both accepted — unlike `--profile`, which predates this and only takes
    /// the leading position.
    #[arg(long, global = true, value_name = "LIST|SERVICE:VARIANT")]
    pub variant: Vec<String>,

    /// Offline mode: skip network package and vault lookups; use cached values only
    #[arg(long)]
    pub offline: bool,

    /// Override `.fed/cloud.yaml`'s team-vault cache policy for this invocation
    /// (memory unless the linked project sets `secret_cache: file`)
    #[arg(long, global = true, value_enum)]
    pub secret_cache: Option<SecretCacheMode>,

    /// Show verbose debug output
    #[arg(short, long, global = true)]
    pub verbose: bool,

    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand)]
pub enum Commands {
    /// Start services
    #[command(alias = "s")]
    Start {
        /// Services to start (defaults to entrypoint)
        services: Vec<String>,

        /// Start every service enabled by the active profiles
        #[arg(long, conflicts_with_all = ["services", "interactive"])]
        all: bool,

        /// Watch for file changes and auto-restart services (runs in foreground)
        #[arg(short, long)]
        watch: bool,

        /// Kill any external processes occupying required ports before starting
        #[arg(long)]
        replace: bool,

        /// Output mode for process services. file: append output to
        /// .fed/logs/<service>.log and return (default). captured: keep output in
        /// memory (default for --watch and the TUI). passthrough: inherit fed's
        /// stdin/stdout/stderr with no log capture; fed still returns after
        /// startup, so this suits CI log visibility, not interactive programs.
        #[arg(long, value_name = "MODE", value_parser = ["file", "captured", "passthrough"])]
        output: Option<String>,

        /// Run a single process service in the foreground: it inherits fed's
        /// terminal, so it can read stdin, and fed waits for it and exits with
        /// its exit code. Its dependencies still start in the background.
        /// When it doesn't receive a service, it uses the entrypoint. Just like
        /// `fed start`. The entrypoint must be a process service.
        #[arg(short, long, conflicts_with_all = ["watch", "output", "dry_run"])]
        interactive: bool,

        /// Preview what would happen without actually starting services
        #[arg(long)]
        dry_run: bool,

        /// Enable isolation mode before starting (persisted)
        #[arg(long)]
        isolate: bool,

        /// Maximum services starting concurrently within a dependency level.
        /// Dependency levels still run in order; services with no dependency
        /// edge between them must tolerate starting in any order. Use -j 1
        /// for fully sequential startup.
        #[arg(short = 'j', long, default_value_t = 4, value_parser = clap::value_parser!(u16).range(1..))]
        jobs: u16,
    },
    /// Stop services
    Stop {
        /// Services to stop (defaults to all)
        services: Vec<String>,
    },
    /// Restart services
    Restart {
        /// Restart every service enabled by the active profiles
        #[arg(long, conflicts_with = "services")]
        all: bool,
        /// Services to restart (defaults to all)
        services: Vec<String>,
    },
    /// Show service status
    Status {
        /// Output as JSON
        #[arg(long)]
        json: bool,

        /// Filter services by tag
        #[arg(long)]
        tag: Option<String>,
    },
    /// Connect your terminal to a running tty service
    #[cfg(unix)]
    Attach {
        /// Service name
        service: String,
        /// Follow output without forwarding input or changing terminal mode
        #[arg(long)]
        no_stdin: bool,
        /// Two control keys that detach without stopping the service
        #[arg(long, default_value = "ctrl-p,ctrl-q")]
        detach_keys: String,
    },
    /// Show service logs
    Logs {
        /// Service name
        service: String,
        /// Number of lines to show
        #[arg(short = 'n', long)]
        tail: Option<usize>,
        /// Follow log output
        #[arg(short, long)]
        follow: bool,
    },
    /// Launch interactive TUI
    Tui {
        /// Watch for file changes and auto-restart services
        #[arg(short, long)]
        watch: bool,
    },
    /// Run a script defined in fed.yaml
    Run {
        /// Script name
        name: String,
        /// Arguments to pass to the script (after --)
        #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
        args: Vec<String>,
        /// Output mode for the script's dependent process services: file =
        /// logs to .fed/logs, captured = in-memory ring buffer (default),
        /// passthrough = inherit stdio (use to surface logs in CI)
        #[arg(long, value_name = "MODE", value_parser = ["file", "captured", "passthrough"])]
        output: Option<String>,
    },
    /// Run install commands for services
    Install {
        /// Services to install (defaults to all services with install field)
        services: Vec<String>,
    },
    /// Run clean commands for services
    Clean {
        /// Services to clean (defaults to all services with clean field)
        services: Vec<String>,
    },
    /// Run build commands for services
    Build {
        /// Services to build (defaults to all services with build field)
        services: Vec<String>,
        /// Tag for Docker images (default: git short hash)
        #[arg(long)]
        tag: Option<String>,
        /// Additional build arguments for Docker builds (can be repeated)
        #[arg(long = "build-arg", value_name = "KEY=VALUE")]
        build_args: Vec<String>,
        /// Output build results as JSON
        #[arg(long)]
        json: bool,
    },
    /// Manage package cache
    #[command(subcommand)]
    Package(PackageCommands),
    /// Manage port allocations
    Ports {
        /// Defaults to `list` when no subcommand is given
        #[command(subcommand)]
        cmd: Option<PortsCommands>,
    },
    /// Initialize a new fed.yaml config
    Init {
        /// Output file path
        #[arg(short, long, default_value = "fed.yaml")]
        output: PathBuf,
        /// Overwrite existing file
        #[arg(short, long)]
        force: bool,
    },
    /// Validate configuration without starting services
    Validate,
    /// Choose which implementation runs for services that declare `variants:`
    Variant {
        /// Defaults to `list` when no subcommand is given
        #[command(subcommand)]
        cmd: Option<VariantCommands>,
    },
    /// Generate shell completions
    Completions {
        /// Shell to generate completions for
        #[arg(value_name = "SHELL")]
        shell: clap_complete::Shell,
    },
    /// Check system requirements (Docker, etc.)
    Doctor,
    /// Remove orphaned fed-managed Docker volumes (dangling only; dry-run + confirm, --force to delete)
    Prune {
        /// Skip the confirmation prompt; required to delete in a non-interactive context
        #[arg(short, long)]
        force: bool,
    },
    /// Show resource usage for all services
    Top {
        /// Refresh interval in seconds
        #[arg(short, long, default_value = "2")]
        interval: u64,
    },
    /// Debug commands for inspecting internal state
    #[command(subcommand)]
    Debug(DebugCommands),

    /// Docker image commands
    #[command(subcommand)]
    Docker(DockerCommands),

    /// Manage project isolation (ports + containers)
    #[command(subcommand)]
    Isolate(IsolateCommands),

    /// Sign in to Service Federation Cloud
    Login {
        /// Don't try to open a browser; just print the sign-in URL (openable
        /// on any machine — approval reaches this terminal automatically)
        #[arg(long)]
        no_browser: bool,
        /// Deprecated: the sign-in URL is now always printed
        #[arg(long, hide = true)]
        print_url: bool,
        /// Label identifying this device on the authorization page and in the
        /// token list (defaults to this machine's hostname). Sent in the
        /// request body, never in a URL.
        #[arg(long)]
        label: Option<String>,
        /// Cloud URL override (defaults to https://app.service-federation.com)
        #[arg(long, hide = true)]
        url: Option<String>,
    },
    /// Sign out (removes local credentials)
    Logout,
    /// Show who you're signed in as
    Whoami,
    /// Bind this checkout to a Cloud project (org/project)
    Link {
        /// Target as org/project (interactive picker if omitted)
        target: Option<String>,
    },
    /// Team development secrets (Service Federation Cloud)
    #[command(subcommand)]
    Secrets(SecretsCommands),

    /// Manage git worktrees for isolated service stacks
    #[command(subcommand, alias = "ws")]
    Workspace(WorkspaceCommands),

    /// Run a script by name (shorthand for `fed run <script>`)
    #[command(external_subcommand)]
    External(Vec<String>),

    /// Internal: run the restart-policy supervisor daemon for this
    /// workspace. Spawned automatically by `fed start`/`fed restart` when
    /// a started service has a `restart:` policy — never invoke this
    /// directly.
    ///
    /// Named `supervise`, not `__supervise` as originally sketched:
    /// clap_complete's bash generator uses a literal `__` as
    /// its internal subcommand-path separator (it joins/splits function
    /// names on it), and a subcommand whose own name already contains `__`
    /// desyncs that reconstruction — `fed completions bash` panicked
    /// (`Option::unwrap()` on `None` in `clap_complete`'s
    /// `find_subcommand_with_path`) with the double-underscore name. Since
    /// this command is hidden and only ever invoked by fed's own
    /// `spawn_if_needed`, the exact token is an implementation detail, not
    /// a user-facing contract — `hide = true` is what actually keeps it out
    /// of `--help`/normal discovery.
    #[command(hide = true)]
    Supervise,

    /// Internal: own the pseudo-terminal of one `tty: true` service, write
    /// its output to the service's log file and serve its attach socket.
    /// Started by `fed start` for such a service, which writes the launch
    /// spec to this process's stdin — never invoke this directly. Unix
    /// only.
    ///
    /// Named `host` rather than `_host` for the reason `supervise` gives
    /// above: clap_complete's bash generator breaks on such a name.
    #[cfg(unix)]
    #[command(hide = true)]
    Host {
        /// Service whose terminal this process owns
        service: String,
    },
}

#[derive(Subcommand)]
pub enum SecretsCommands {
    /// List the linked project's secrets (names and last-updated info, never values)
    Ls {
        /// Removed in fed 8.0 — kept hidden and optional so a stale
        /// invocation gets an explicit migration error instead of a clap
        /// parse failure.
        #[arg(long, hide = true)]
        env: Option<String>,
    },
}

#[derive(Subcommand)]
pub enum DebugCommands {
    /// Show full state tracker contents
    State {
        /// Output as JSON
        #[arg(long)]
        json: bool,
    },
    /// Show port allocations
    Ports {
        /// Output as JSON
        #[arg(long)]
        json: bool,
    },
    /// Show circuit breaker state for a service
    CircuitBreaker {
        /// Service name
        service: String,
        /// Output as JSON
        #[arg(long)]
        json: bool,
    },
}

#[derive(Subcommand)]
pub enum DockerCommands {
    /// Build Docker images for services
    Build {
        /// Services to build (defaults to all with Docker build config)
        services: Vec<String>,
        /// Tag for images (default: git short hash)
        #[arg(long)]
        tag: Option<String>,
        /// Additional build arguments (can be repeated)
        #[arg(long = "build-arg", value_name = "KEY=VALUE")]
        build_args: Vec<String>,
        /// Output build results as JSON
        #[arg(long)]
        json: bool,
    },
    /// Push Docker images to registry
    Push {
        /// Services to push (defaults to all with Docker build config)
        services: Vec<String>,
        /// Tag to push (default: git short hash)
        #[arg(long)]
        tag: Option<String>,
    },
}

#[derive(Subcommand)]
pub enum PackageCommands {
    /// List cached packages
    List {
        /// Output as JSON
        #[arg(long)]
        json: bool,
    },
    /// Clear a cached package so it is fetched again on next use
    Refresh {
        /// Package source (e.g., github:org/repo or git+ssh://...)
        /// If not specified, refreshes all packages in current config
        package: Option<String>,
    },
    /// Clear the entire package cache
    Clear {
        /// Skip confirmation prompt
        #[arg(long, short)]
        force: bool,
    },
}

#[derive(Subcommand, Clone)]
pub enum PortsCommands {
    /// List current port allocations [default when no subcommand given]
    #[command(alias = "ls")]
    List {
        /// Output as JSON
        #[arg(long)]
        json: bool,
    },
}

/// Subcommands of `fed variant`, which reads and writes `.fed/variants.yaml`.
/// The same entries `--variant` accepts, made durable so `fed restart`,
/// `fed status`, the TUI and the supervisor agree without repeating the flag.
#[derive(Subcommand, Clone)]
pub enum VariantCommands {
    /// Show the resolved variant for each service, and what decided it
    /// [default when no subcommand given]
    #[command(alias = "ls")]
    List {
        /// Output as JSON
        #[arg(long)]
        json: bool,
    },
    /// Persist a preference list and/or pins
    ///
    /// `fed variant set go,ts,rust` sets the preference list;
    /// `fed variant set catalog:java` adds a pin. Both forms may be combined
    /// in one invocation. Setting a preference list replaces the previous one.
    Set {
        /// Preference-list names and/or `service:variant` pins
        #[arg(value_name = "LIST|SERVICE:VARIANT", required = true)]
        entries: Vec<String>,
    },
    /// Remove the pin for one or more services
    Unset {
        /// Service names whose pins to drop
        #[arg(value_name = "SERVICE", required = true)]
        services: Vec<String>,
    },
    /// Remove the persisted preference list and every pin
    Clear,
}

impl Default for VariantCommands {
    fn default() -> Self {
        VariantCommands::List { json: false }
    }
}

impl Default for PortsCommands {
    fn default() -> Self {
        PortsCommands::List { json: false }
    }
}

#[derive(Subcommand)]
pub enum WorkspaceCommands {
    /// Create a worktree and enter it
    New {
        /// Branch name (existing or new with -b)
        branch: String,
        /// Create a new branch instead of checking out an existing one
        #[arg(short = 'b', long)]
        create_branch: bool,
    },
    /// List all worktrees
    #[command(alias = "ls")]
    List,
    /// Switch to an existing worktree
    Cd {
        /// Worktree branch name
        name: String,
    },
    /// Remove a worktree (stops services first)
    #[command(alias = "remove")]
    Rm {
        /// Worktree branch name
        name: String,
        /// Force removal even with uncommitted changes
        #[arg(long, short)]
        force: bool,
    },
    /// Unregister worktrees whose directories are gone and remove empty leftovers
    Prune,
    /// Install shell integration into your shell rc file (zsh or bash, one-time)
    Setup,
    /// Print shell function for eval (used internally by setup)
    #[command(hide = true)]
    InitShell,
}

#[derive(Subcommand)]
pub enum IsolateCommands {
    /// Enable isolation mode (randomize ports + unique container names)
    Enable {
        /// Skip confirmation, auto-stop running services
        #[arg(long, short)]
        force: bool,
    },
    /// Disable isolation mode (return to default ports and the directory's normal container namespace)
    Disable {
        /// Skip confirmation, auto-stop running services
        #[arg(long, short)]
        force: bool,
    },
    /// Show current isolation status and port allocations
    Status,
    /// Re-roll ports and isolation ID (must be currently isolated)
    Rotate {
        /// Skip confirmation, auto-stop running services
        #[arg(long, short)]
        force: bool,
    },
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    #[test]
    fn secret_cache_has_no_cli_override_by_default() {
        let cli = Cli::try_parse_from(["fed", "status"]).expect("parse");
        assert_eq!(cli.secret_cache, None);
    }

    #[test]
    fn secret_cache_memory_is_a_global_option() {
        let before =
            Cli::try_parse_from(["fed", "--secret-cache", "memory", "status"]).expect("parse");
        let after =
            Cli::try_parse_from(["fed", "status", "--secret-cache", "memory"]).expect("parse");
        assert_eq!(before.secret_cache, Some(SecretCacheMode::Memory));
        assert_eq!(after.secret_cache, Some(SecretCacheMode::Memory));
    }

    #[test]
    fn secrets_ls_still_parses() {
        let cli =
            Cli::try_parse_from(["fed", "secrets", "ls"]).expect("`fed secrets ls` must parse");
        assert!(matches!(
            cli.command,
            Commands::Secrets(SecretsCommands::Ls { .. })
        ));
    }

    #[test]
    fn secrets_set_is_rejected() {
        // fed 7.0 removed `fed secrets set` — secret writes are dashboard-only,
        // so a leaked bearer token cannot write. The subcommand must not parse.
        assert!(
            Cli::try_parse_from(["fed", "secrets", "set", "API_KEY"]).is_err(),
            "`fed secrets set` must be rejected after fed 7.0",
        );
    }

    #[test]
    fn start_interactive_parses() {
        let cli = Cli::try_parse_from(["fed", "start", "-i", "shell"])
            .expect("`fed start -i shell` must parse");
        assert!(matches!(
            cli.command,
            Commands::Start {
                interactive: true,
                ..
            }
        ));
    }

    #[test]
    fn start_interactive_conflicts_with_lifecycle_flags() {
        for conflicting in [
            vec!["fed", "start", "-i", "--watch", "shell"],
            vec!["fed", "start", "-i", "--output", "passthrough", "shell"],
            vec!["fed", "start", "-i", "--dry-run", "shell"],
        ] {
            assert!(
                Cli::try_parse_from(&conflicting).is_err(),
                "{:?} must be rejected",
                conflicting
            );
        }
    }
}
