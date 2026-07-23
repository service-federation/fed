# fed

[![Release](https://img.shields.io/github/v/release/service-federation/fed?color=green)](https://github.com/service-federation/fed/releases/latest)
[![CI](https://github.com/service-federation/fed/actions/workflows/ci.yml/badge.svg)](https://github.com/service-federation/fed/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/github/license/service-federation/fed)](./LICENSE)

`git clone`, `fed start`, and the whole project is running: your app as a native process, your dependencies as Docker containers, one dependency graph, healthcheck-gated startup. Think `docker compose up`, except the app isn't in a container and every git worktree can run its own isolated copy of the stack.

![fed demo: editing the config, starting the stack, and running a second isolated stack from a git worktree](docs/fed-demo.gif)

## Why fed

- **Your coding agents stop killing each other's databases.** One worktree per agent, one full stack per worktree, zero shared ports.
- **Onboarding is `git clone`, `fed start`.** No README of setup steps nobody reruns. Dependencies start in order and wait for real healthchecks.
- **No override-file archaeology.** Port parameters, templates, profiles, and packages replace the `docker-compose.override.yml` pile.

## Quick Start

```bash
brew install service-federation/tap/fed
```

Other installs: [prebuilt binary, cargo](https://www.service-federation.com/docs/).

Create `fed.yaml` (or run `fed init`). This example assumes a Node backend in `./backend` that reads `PORT` and serves `/health`:

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

Start it:

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

Day to day: `fed status`, `fed logs backend`, `fed stop`.

### Startup health semantics

`fed start` waits for each service's healthcheck before starting its dependents.

- A healthcheck that passes before startup returns marks the service `healthy`. The `timeout` is evaluated between polling attempts, so a check already in flight at the deadline may still count.
- A healthcheck that does not pass within its `timeout` (default 5s) is a warning, not an error: the process keeps running, dependents still start, and the run ends with `Services started with N health warning(s)` instead of the success line. The exit code stays 0.
- A process that dies before its healthcheck passes fails the start with a non-zero exit code.

In `fed status`, `running` means the process is up but no healthcheck has confirmed it; only `healthy` means verified. `fed status --json` reports the same distinction as `"status": "running"` with `"health": "unknown"`.

## Built for coding agents

Claude Code, Cursor, and Codex parallelize with one worktree per task. Without isolation, those worktrees fight over ports and databases: one agent's test run wipes another's schema, and the fix is you, untangling it.

With fed, each worktree runs its own full stack:

```console
~/app          $ fed start                 # your stack, default ports
~/app-agent-1  $ fed isolate enable        # its own ports, containers, volumes
~/app-agent-1  $ fed start
~/app-agent-2  $ fed isolate enable
~/app-agent-2  $ fed start                 # three stacks, one laptop, no collisions
```

Two lines in your `AGENTS.md` make every agent do this unprompted:

```markdown
Run `fed isolate enable` before any other fed command in a new worktree.
Run `fed clean` before removing a worktree.
```

Scripts complete the story: `fed test:integration` resolves the ports and `DATABASE_URL` *this* worktree was allocated, so agents never guess. [Coding agents & isolation →](https://www.service-federation.com/docs/isolation/#coding-agents)

## Highlights

- **Startup-gating hooks**: `install:` runs once, `migrate:` runs every start, both before dependents boot. [Docs →](https://www.service-federation.com/docs/configuration/)
- **Hook-only services**: a node that *is* the migration; everything needing the schema depends on it. [Docs →](https://www.service-federation.com/docs/configuration/#hook-only)
- **Isolated scripts**: `isolated: true` gives tests a throwaway stack while your dev stack keeps running. [Docs →](https://www.service-federation.com/docs/scripts/#isolated-scripts)
- **Secrets**: generated locally under `.fed/`, or shared via the team vault. Free for 3 people, €8/seat after. [Generated →](https://www.service-federation.com/docs/generated-secrets/) · [Team →](https://www.service-federation.com/docs/secrets/)
- **`{{FED_PROJECT_ID}}`**: a per-checkout id for namespacing what parallel stacks would share, like cookie names. [Docs →](https://www.service-federation.com/docs/configuration/#fed-project-id)

## Documentation

`fed --help` is always current. On the site: [Quickstart](https://www.service-federation.com/docs/) · [Configuration](https://www.service-federation.com/docs/configuration/) · [Commands](https://www.service-federation.com/docs/commands/) · [Scripts](https://www.service-federation.com/docs/scripts/) · [Isolation](https://www.service-federation.com/docs/isolation/) · [Generated secrets](https://www.service-federation.com/docs/generated-secrets/) · [Team secrets](https://www.service-federation.com/docs/secrets/)

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

[Open an issue](https://github.com/service-federation/fed/issues). For environment problems, include `fed doctor` output.

## Contributing

Issues and PRs welcome. See [CONTRIBUTING.md](./CONTRIBUTING.md).

## License

MIT
