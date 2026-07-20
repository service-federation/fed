use clap::{Parser, Subcommand};
use std::path::PathBuf;

#[derive(Parser)]
#[command(name = "fed", version)]
#[command(about = "Service Federation - Orchestrate complex service dependencies")]
pub struct Cli {
    /// Config file path (defaults to service-federation.yaml)
    #[arg(short, long)]
    pub config: Option<PathBuf>,

    /// Working directory
    #[arg(short, long)]
    pub workdir: Option<PathBuf>,

    /// Environment for variable resolution (development, staging, production)
    #[arg(short, long, default_value = "development")]
    pub env: String,

    /// Active profiles for conditional service startup (can be repeated)
    #[arg(short, long)]
    pub profile: Vec<String>,

    /// Offline mode: skip git operations and use only cached packages
    #[arg(long)]
    pub offline: bool,

    /// Show verbose debug output
    #[arg(short, long, global = true)]
    pub verbose: bool,

    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand)]
pub enum Commands {
    /// Start services
    Start {
        /// Services to start (defaults to entrypoint)
        services: Vec<String>,

        /// Watch for file changes and auto-restart services (runs in foreground)
        #[arg(short, long)]
        watch: bool,

        /// Kill any external processes occupying required ports before starting
        #[arg(long)]
        replace: bool,

        /// Output mode for process services: file = logs to files (default for
        /// start), captured = logs to memory (default for watch/tui),
        /// passthrough = inherit stdio (for testing/CI)
        #[arg(long, value_name = "MODE", value_parser = ["file", "captured", "passthrough"])]
        output: Option<String>,

        /// Preview what would happen without actually starting services
        #[arg(long)]
        dry_run: bool,

        /// [deprecated: use --isolate] Allocate fresh random ports
        #[arg(long, hide = true)]
        randomize: bool,

        /// Enable isolation mode before starting (persisted)
        #[arg(long)]
        isolate: bool,
    },
    /// Stop services
    Stop {
        /// Services to stop (defaults to all)
        services: Vec<String>,
    },
    /// Restart services
    Restart {
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
    /// Run a script defined in service-federation.yaml
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
    #[command(subcommand)]
    Ports(PortsCommands),
    /// Initialize a new service-federation.yaml config
    Init {
        /// Output file path
        #[arg(short, long, default_value = "service-federation.yaml")]
        output: PathBuf,
        /// Overwrite existing file
        #[arg(short, long)]
        force: bool,
    },
    /// Validate configuration without starting services
    Validate,
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
        /// Print the URL and paste the token manually (for SSH sessions)
        #[arg(long)]
        no_browser: bool,
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
}

#[derive(Subcommand)]
pub enum SecretsCommands {
    /// List the linked project's secrets (names only)
    Ls {
        /// Environment
        #[arg(long, default_value = "development")]
        env: String,
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
    /// Refresh (re-fetch) a cached package
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
    /// [deprecated: use `fed isolate enable`] Allocate fresh random ports for all port parameters
    #[command(hide = true)]
    Randomize {
        /// Skip confirmation, auto-stop running services
        #[arg(long, short)]
        force: bool,
    },
    /// [deprecated: use `fed isolate disable`] Clear port allocations (next start uses defaults)
    #[command(hide = true)]
    Reset {
        /// Skip confirmation, auto-stop running services
        #[arg(long, short)]
        force: bool,
    },
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
    /// Remove worktrees for deleted branches
    Prune,
    /// Install shell integration into ~/.zshrc (one-time)
    Setup,
    /// Print shell function for eval (used internally by setup)
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
    /// Disable isolation mode (return to default ports + shared containers)
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
}
