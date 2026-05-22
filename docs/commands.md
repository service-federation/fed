# Command Reference

Run `fed --help` or `fed <command> --help` for full details.

## Global Flags

These flags apply to all commands and must appear before the subcommand.

| Flag | Description |
|------|-------------|
| `-c` / `--config <PATH>` | Config file path (default: `service-federation.yaml`) |
| `-w` / `--workdir <PATH>` | Working directory |
| `-e` / `--env <ENV>` | Environment for variable resolution (default: `development`) |
| `-p` / `--profile <NAME>` | Active profiles (repeatable) |
| `--offline` | Skip git operations (package fetches) |
| `-v` / `--verbose` | Debug output |
| `--version` | Print version |

```bash
fed -p backend -p monitoring start    # Multiple profiles
fed -e staging status                  # Non-default environment
fed -c custom.yaml start              # Custom config file
```

## Starting & Stopping

### `fed start [services...]`

Start services. Defaults to the entrypoint if no services are specified. Waits for health checks, then backgrounds.

| Flag | Description |
|------|-------------|
| `-w` / `--watch` | Watch mode -- foreground, auto-restart on file changes |
| `--replace` | Kill processes occupying required ports, then start |
| `--output <MODE>` | Output mode: `file` (default), `captured`, `passthrough` |
| `--dry-run` | Preview what would start without starting |
| `--isolate` | Enable isolation mode before starting (persisted) |

```bash
fed start                        # Start entrypoint services
fed start api gateway            # Start specific services
fed start --watch api            # Watch mode with auto-restart
fed start --replace              # Reclaim occupied ports
fed start --isolate              # Isolate, then start
```

### `fed stop [services...]`

Stop services. Defaults to all running services.

```bash
fed stop                         # Stop everything
fed stop api                     # Stop a single service
```

### `fed restart [services...]`

Restart services. Defaults to all running services.

## Observability

### `fed status`

Show service status.

| Flag | Description |
|------|-------------|
| `--json` | Machine-readable output |
| `--tag <TAG>` | Filter services by tag |

### `fed logs <service>`

View logs for a service.

| Flag | Description |
|------|-------------|
| `-f` / `--follow` | Stream logs |
| `-n` / `--tail <N>` | Show last N lines |

```bash
fed logs api                     # View full log
fed logs api -f                  # Tail logs
fed logs api -n 50               # Last 50 lines
```

### `fed tui`

Launch interactive TUI dashboard.

| Flag | Description |
|------|-------------|
| `-w` / `--watch` | Watch for file changes and auto-restart |

### `fed top`

Show resource usage for all services. Refreshes periodically.

| Flag | Description |
|------|-------------|
| `-i` / `--interval <SECS>` | Refresh interval in seconds (default: 2) |

## Scripts

### `fed run <script> [-- args...]`

Run a named script. Services in `depends_on` are started first and, if the script started them, stopped again after it completes ("borrow or own"). Services that were already running (e.g. via `fed start`) are left running. See [Scripts](scripts.md#service-lifecycle-borrow-or-own).

| Flag | Description |
|------|-------------|
| `--output <MODE>` | Output mode for the script's dependent process services: `file`, `captured` (default), `passthrough` |

```bash
fed run db:migrate
fed db:migrate                          # Shorthand (if no command collision)
fed test:integration -- -t "auth"       # Pass arguments after --
fed run e2e --output passthrough        # Surface service logs (use in CI)
```

## Isolation

### `fed isolate enable`

Enable isolation mode -- randomize ports and scope Docker containers.

| Flag | Description |
|------|-------------|
| `-f` / `--force` | Auto-stop running services without prompting |

### `fed isolate disable`

Disable isolation mode -- return to default ports and shared containers.

| Flag | Description |
|------|-------------|
| `-f` / `--force` | Auto-stop running services without prompting |

### `fed isolate status`

Show current isolation state and port allocations.

### `fed isolate rotate`

Re-roll ports and isolation ID. Requires services stopped or `--force`.

| Flag | Description |
|------|-------------|
| `-f` / `--force` | Auto-stop running services without prompting |

## Ports

### `fed ports list`

Show current port allocations. Alias: `fed ports ls`.

| Flag | Description |
|------|-------------|
| `--json` | Machine-readable output |

## Lifecycle

### `fed install [services...]`

Run `install` hooks. Defaults to all services with an `install` field.

### `fed build [services...]`

Run `build` hooks (shell and Docker).

| Flag | Description |
|------|-------------|
| `--tag <TAG>` | Custom tag for Docker images |
| `--build-arg <KEY=VALUE>` | Extra build arguments (repeatable) |
| `--json` | Machine-readable output |

### `fed clean [services...]`

Run `clean` hooks and remove Docker volumes with `fed-` prefix.

### `fed validate`

Validate configuration without starting services.

## Docker

### `fed docker build [services...]`

Build Docker images only (skip shell build hooks).

| Flag | Description |
|------|-------------|
| `--tag <TAG>` | Custom tag |
| `--build-arg <KEY=VALUE>` | Extra build arguments (repeatable) |
| `--json` | Machine-readable output |

### `fed docker push [services...]`

Push built images to registry.

| Flag | Description |
|------|-------------|
| `--tag <TAG>` | Push specific tag |

## Packages

### `fed package list`

List cached packages.

| Flag | Description |
|------|-------------|
| `--json` | Machine-readable output |

### `fed package refresh [source]`

Re-fetch packages. Without an argument, refreshes all packages in the current config.

### `fed package clear`

Clear the entire package cache.

| Flag | Description |
|------|-------------|
| `-f` / `--force` | Skip confirmation prompt |

## Workspaces

Alias: `fed ws` for `fed workspace`.

### `fed ws new <branch>`

Create a worktree for an existing branch, or a new branch with `-b`.

| Flag | Description |
|------|-------------|
| `-b` / `--create-branch` | Create a new branch (otherwise checks out existing) |

```bash
fed ws new my-feature -b    # Create new branch + worktree
fed ws new main             # Worktree for existing branch
```

### `fed ws list`

Show all worktrees with service status. Alias: `fed ws ls`.

### `fed ws cd <name>`

Switch to another worktree.

### `fed ws rm <name>`

Stop services and remove a worktree. Alias: `fed ws remove`.

| Flag | Description |
|------|-------------|
| `-f` / `--force` | Force removal even with uncommitted changes |

### `fed ws prune`

Remove worktrees for deleted branches.

### `fed ws setup`

Install shell integration into `~/.zshrc` (one-time).

## Utilities

### `fed doctor`

Check system requirements (Docker, Gradle, etc.).

### `fed init`

Create a starter `service-federation.yaml`.

| Flag | Description |
|------|-------------|
| `-o` / `--output <PATH>` | Output file path (default: `service-federation.yaml`) |
| `-f` / `--force` | Overwrite existing file |

### `fed completions <SHELL>`

Generate shell completions. Supported shells: `bash`, `zsh`, `fish`, `elvish`, `powershell`.

```bash
fed completions zsh > ~/.zfunc/_fed
```

## Debug

### `fed debug state`

Show full state tracker contents.

| Flag | Description |
|------|-------------|
| `--json` | Machine-readable output |

### `fed debug ports`

Show port allocation internals.

| Flag | Description |
|------|-------------|
| `--json` | Machine-readable output |

### `fed debug circuit-breaker <service>`

Show circuit breaker state for a service.

| Flag | Description |
|------|-------------|
| `--json` | Machine-readable output |
