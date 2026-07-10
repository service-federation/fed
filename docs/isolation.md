# Isolation

Service Federation scopes all state by working directory. This gives you isolation between projects, worktrees, and CI agents without any explicit configuration.

## Ports must be parameters

Isolation can only remap ports it knows about — the ones declared as `type: port` parameters and interpolated with `{{...}}`. A literal `4321` in a command or healthcheck is invisible to fed and will collide across workspaces. Write every port the fed way:

```yaml
parameters:
  APP_PORT:
    type: port
    default: 4321
  DB_PORT:
    type: port
    default: 5433

services:
  database:
    image: postgres:16-alpine
    ports: ["{{DB_PORT}}:5432"]          # never "5433:5432"

  app:
    process: npm run dev -- --port {{APP_PORT}}
    startup_message: 'http://localhost:{{APP_PORT}}'
    environment:
      DATABASE_URL: 'postgres://localhost:{{DB_PORT}}/app'
    healthcheck:
      httpGet: 'http://localhost:{{APP_PORT}}/health'

entrypoint: app
```

In normal (non-isolated) mode the defaults are used, so nothing changes day to day. Under isolation each workspace gets its own random ports — but only for ports declared this way. fed warns once at startup when it finds literal `localhost:<port>` references or literal host ports in the config.

## Directory Scoping

All state is scoped to the directory containing your `service-federation.yaml`:

- Port allocations
- Docker container names
- Docker volumes
- The SQLite state file

Two directories are two independent stacks. There's nothing to configure — it just works.

## Project Isolation

When you need multiple instances of the same project (e.g., worktrees, Cursor agents), enable isolation mode:

```bash
fed isolate enable       # Randomize ports + unique container names (persisted)
fed isolate status       # Show isolation state and port allocations
fed isolate rotate       # Re-roll ports and isolation ID
fed isolate disable      # Return to default ports and shared containers
```

Isolation mode persists across restarts — `fed stop && fed start` reuses the same isolated allocations.

### Quick isolation

```bash
fed start --isolate      # Enable isolation + start (one command)
```

### What isolation does

- **Ports**: All port-type parameters get randomized values instead of defaults. Literal ports (a hardcoded `4321` in a command or healthcheck) are **not** remapped — see [Ports must be parameters](#ports-must-be-parameters)
- **Docker containers**: Containers get a unique isolation ID in their names, preventing collisions
- **Docker volumes**: Scoped to the isolation ID

## Port Management

```bash
fed ports list            # Show current allocations
fed ports list --json     # Machine-readable output
```

Ports persist across restarts — `fed stop && fed start` reuses the same allocations until you `fed isolate disable`.

## Git Worktrees

Git worktrees give you full isolation for free because each worktree is a separate directory:

```bash
# Main worktree — default ports
~/project $ fed start

# Second worktree — isolated ports, separate volumes and containers
~/project-review $ fed start --isolate
```

Both stacks run simultaneously on the same machine.

### Workspace Management (beta)

`fed ws` (alias for `fed workspace`) automates git worktree creation and switching:

```bash
fed ws setup                # One-time: install shell integration
fed ws new -b my-feature    # Create branch + worktree, cd into it
fed ws list                 # Show all worktrees with service status
fed ws cd main              # Switch to another worktree
fed ws rm my-feature        # Stop services and remove worktree
fed ws prune                # Clean up worktrees for deleted branches
```

Worktrees are created as siblings to the repo in a `<repo>-worktrees/` directory. Shell integration enables auto-cd — without it, `fed ws new` and `fed ws cd` print the path instead.

## Cursor and Parallel Agents

Cursor's parallel agents create git worktrees under the hood — each agent works in its own directory. If your project has a `service-federation.yaml`, each agent can spin up the full stack independently:

```bash
fed install && fed start --isolate
```

No plugin or integration needed. Directory-scoped isolation means it works the same whether Cursor created the worktree or you did.

## Isolated Scripts

Scripts can opt into full isolation with `isolated: true` — fresh ports, scoped containers and volumes, automatic cleanup. See [Scripts](./scripts.md#isolated-scripts) for details.
