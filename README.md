# fed

[![Release](https://img.shields.io/github/v/release/service-federation/fed?color=green)](https://github.com/service-federation/fed/releases/latest)
[![CI](https://github.com/service-federation/fed/actions/workflows/ci.yml/badge.svg)](https://github.com/service-federation/fed/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/github/license/service-federation/fed)](./LICENSE)

`git clone`, `fed start`, and the whole project is running: your app as a native process, your dependencies as Docker containers, all in one dependency graph with healthcheck-gated startup. Think `docker compose up`, except the app isn't in a container and every git worktree can run its own isolated copy of the stack.

This README gets you running. The full command, configuration, and secrets references live at **[service-federation.com/docs](https://www.service-federation.com/docs/)**.

![fed demo: editing the config, starting the stack, and running a second isolated stack from a git worktree](docs/fed-demo.gif)

## Quick Start

```bash
# macOS / Linux (Homebrew)
brew install service-federation/tap/fed

# Prebuilt binary
curl --proto '=https' --tlsv1.2 -LsSf https://github.com/service-federation/fed/releases/latest/download/fed-installer.sh | sh

# From source
cargo install --git https://github.com/service-federation/fed
```

Create `fed.yaml` (or run `fed init`). Adapt the config to your app. This example assumes a Node backend in `./backend` that reads `PORT` and serves `/health`:

```yaml
parameters:
  API_PORT:
    type: port
    default: 8080
  DB_PORT:
    type: port
    default: 5432
  DB_PASSWORD:
    type: secret        # generated on first start, never committed

services:
  database:
    image: postgres:15
    ports: ["{{DB_PORT}}:5432"]
    environment:
      POSTGRES_PASSWORD: '{{DB_PASSWORD}}'
      POSTGRES_DB: app
    healthcheck:
      command: pg_isready -U postgres

  backend:
    process: npm start
    cwd: ./backend
    depends_on: [database]
    environment:
      PORT: '{{API_PORT}}'
      DATABASE_URL: 'postgres://postgres:{{DB_PASSWORD}}@localhost:{{DB_PORT}}/app'
    healthcheck:
      httpGet: 'http://localhost:{{API_PORT}}/health'

entrypoint: backend
```

Then start it. Each service is health-checked before its dependents start:

```console
$ fed start
Starting: backend (with deps: database)

  database (dependency)... ready
  backend... ready

All services started successfully!

╭──────────────────────────────╮
│ API on http://localhost:8080 │
╰──────────────────────────────╯
```

```bash
fed status       # What's running
fed logs backend # View logs
fed stop         # Stop all
```

That's the whole workflow. For the next teammate, it's `git clone`, `fed start`.

## In a repo that already uses fed?

If you found a `fed.yaml` in a project, and especially if you're a coding agent working in a checkout, these four rules keep you out of trouble:

1. **New worktree? Isolate first.** Run `fed isolate enable` before any other fed command. It persists: every fed command after it gets this directory's own ports, containers, and volumes.
2. **Run tasks through fed.** `fed <script>` (like `fed test:integration` or `fed psql`) resolves the ports and `DATABASE_URL` this directory was actually allocated. The same command run bare hits whichever checkout owns the default ports. The `scripts:` section of `fed.yaml` lists what's available.
3. **Port conflict on start?** Another checkout owns those ports, and the fix is `fed isolate enable`. Never `fed start --replace` in a worktree: it takes the port by killing the other checkout's services.
4. **Look before you guess.** `fed status`, `fed ports list`, and `fed logs <service> --tail 100` show what's running, where, and why it failed.

Details: [coding agents & isolation](https://www.service-federation.com/docs/isolation/#coding-agents).

## Why fed

**Clone to running, one command.** One `fed.yaml` declares your Docker containers, native processes, and Compose services. `fed start` brings them up in dependency order and holds each service until its dependencies are healthy. Onboarding stops being a README of setup steps nobody reruns.

**Parallel checkouts stop fighting.** Containers, volumes, and state are scoped to the working directory automatically, and `fed isolate enable` gives a checkout its own ports too. Your checkout plus four coding agents can run five full stacks on one laptop without a single port collision, and nobody kills anybody else's database.

**No more override-file archaeology.** Port parameters, templating, profiles, and cross-project packages replace the `docker-compose.override.yml` pile and the `.env` juggling that grows around every multi-service repo.

## Highlights

Each of these is a one-liner here and a full chapter in the docs:

- **Startup-gating hooks**: `install:` runs once per checkout, `migrate:` runs on every start. Both run after a service's dependencies are healthy and before its dependents boot, so nothing starts against an empty database or missing `node_modules`. [Lifecycle hooks →](https://www.service-federation.com/docs/configuration/)
- **Hook-only services**: a node that *is* the migration. Declare `migrate:` with no process, and every service that needs the schema depends on it. [Hook-only services →](https://www.service-federation.com/docs/configuration/#hook-only)
- **Isolated scripts**: `isolated: true` gives integration tests a throwaway stack on fresh ports, cleaned up on every exit path, while your dev stack keeps running. [Scripts →](https://www.service-federation.com/docs/scripts/#isolated-scripts)
- **Generated secrets**: `type: secret` parameters get random values on first start, stored under `.fed/` where fed's own gitignore keeps them out of commits. No more `POSTGRES_PASSWORD: password`. [Generated secrets →](https://www.service-federation.com/docs/generated-secrets/)
- **Team secrets**: set a development API key once in the [dashboard](https://app.service-federation.com); every teammate's `fed start` finds it after `fed login` + `fed link`. Free for the first 3 people in an org, €8/seat/month beyond. [Team secrets →](https://www.service-federation.com/docs/secrets/)
- **`{{FED_PROJECT_ID}}`**: a stable per-checkout identifier, available in every template without declaring it. Namespace whatever parallel stacks would otherwise share, like cookie names and queue prefixes. [Details →](https://www.service-federation.com/docs/configuration/#fed-project-id)
- **Worktrees & coding agents**: one worktree per agent, one stack per worktree. `fed ws new feature -b`, `fed ws list`, and `fed ws cd main` manage the worktrees themselves. [Worktrees →](https://www.service-federation.com/docs/isolation/)

Running coding agents? Add one rule to your `AGENTS.md` so every agent isolates before it collides:

```markdown
## Worktrees
Run `fed isolate enable` before any other fed command in a new worktree.
```

## Documentation

`fed --help` is always current. The full references live on the website:

- [Quickstart](https://www.service-federation.com/docs/): install to running stack
- [Configuration](https://www.service-federation.com/docs/configuration/): services, parameters, secrets, health checks, templates, profiles, packages, lifecycle hooks, resource limits
- [Commands](https://www.service-federation.com/docs/commands/): all commands, flags, and subcommands
- [Scripts](https://www.service-federation.com/docs/scripts/): script lifecycle ("borrow or own"), isolated scripts, argument passing
- [Isolation](https://www.service-federation.com/docs/isolation/): directory scoping, worktrees, coding agents
- [Generated secrets](https://www.service-federation.com/docs/generated-secrets/): stable local passwords and keys
- [Team secrets](https://www.service-federation.com/docs/secrets/): shared development secrets via Service Federation Cloud

## Examples

See [`examples/`](./examples):

- [`simple.yaml`](./examples/simple.yaml): basic multi-service setup
- [`scripts-example.yaml`](./examples/scripts-example.yaml): scripts with dependencies
- [`env-file/`](./examples/env-file): environment files
- [`templates-example.yaml`](./examples/templates-example.yaml): service templates
- [`parameters-example.yaml`](./examples/parameters-example.yaml): parameter types, defaults, and constraints
- [`resource-limits-example.yaml`](./examples/resource-limits-example.yaml): memory, CPU, file descriptor limits
- [`docker-compose-example/`](./examples/docker-compose-example): Docker Compose integration
- [`profiles-example.yaml`](./examples/profiles-example.yaml): profiles
- [`service-merging/`](./examples/service-merging): package imports

## Questions & support

Stuck or found a bug? [Open an issue](https://github.com/service-federation/fed/issues). For environment problems, include the output of `fed doctor`.

## Contributing

Issues and PRs welcome. See [CONTRIBUTING.md](./CONTRIBUTING.md).

## License

MIT
