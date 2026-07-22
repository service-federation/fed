# fed

[![Release](https://img.shields.io/github/v/release/service-federation/fed?color=green)](https://github.com/service-federation/fed/releases/latest)
[![CI](https://github.com/service-federation/fed/actions/workflows/ci.yml/badge.svg)](https://github.com/service-federation/fed/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/github/license/service-federation/fed)](./LICENSE)

`git clone`, `fed start`, and the whole project is running: your app as a native process, your dependencies as Docker containers, one dependency graph, healthcheck-gated startup. Think `docker compose up`, except the app isn't in a container and every git worktree can run its own isolated copy of the stack.

Full references: **[service-federation.com/docs](https://www.service-federation.com/docs/)**.

![fed demo: editing the config, starting the stack, and running a second isolated stack from a git worktree](docs/fed-demo.gif)

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

## In a repo that already uses fed?

Four rules, especially if you're a coding agent in a checkout:

1. **New worktree? Isolate first.** Run `fed isolate enable` before any other fed command. It persists.
2. **Run tasks through fed.** `fed test:integration` gets this directory's ports and `DATABASE_URL`. The same command run bare hits another checkout's.
3. **Port conflict on start?** Another checkout owns those ports. Fix: `fed isolate enable`.
4. **Look before you guess.** `fed status`, `fed ports list`, `fed logs <service> --tail 100`.

Details: [coding agents & isolation](https://www.service-federation.com/docs/isolation/#coding-agents).

## Why fed

- **Clone to running, one command.** Containers, native processes, and Compose services in one file, started in dependency order, gated on real healthchecks.
- **Parallel checkouts stop fighting.** Five worktrees, five full stacks, one laptop, zero port collisions.
- **No override-file archaeology.** Port parameters, templates, profiles, and packages replace the `docker-compose.override.yml` pile.

## Highlights

- **Startup-gating hooks**: `install:` runs once, `migrate:` runs every start, both before dependents boot. [Docs →](https://www.service-federation.com/docs/configuration/)
- **Hook-only services**: a node that *is* the migration; everything needing the schema depends on it. [Docs →](https://www.service-federation.com/docs/configuration/#hook-only)
- **Isolated scripts**: `isolated: true` gives tests a throwaway stack while your dev stack keeps running. [Docs →](https://www.service-federation.com/docs/scripts/#isolated-scripts)
- **Secrets**: generated locally under `.fed/`, or shared via the team vault. Free for 3 people, €8/seat after. [Generated →](https://www.service-federation.com/docs/generated-secrets/) · [Team →](https://www.service-federation.com/docs/secrets/)
- **Worktrees & agents**: one stack per worktree (`fed ws new feature -b`); `{{FED_PROJECT_ID}}` namespaces what parallel stacks would share. [Docs →](https://www.service-federation.com/docs/isolation/)

Running coding agents? One line in your `AGENTS.md`:

```markdown
Run `fed isolate enable` before any other fed command in a new worktree.
```

## Documentation

`fed --help` is always current. On the site: [Quickstart](https://www.service-federation.com/docs/) · [Configuration](https://www.service-federation.com/docs/configuration/) · [Commands](https://www.service-federation.com/docs/commands/) · [Scripts](https://www.service-federation.com/docs/scripts/) · [Isolation](https://www.service-federation.com/docs/isolation/) · [Generated secrets](https://www.service-federation.com/docs/generated-secrets/) · [Team secrets](https://www.service-federation.com/docs/secrets/)

Runnable examples: [`examples/`](./examples), from a basic multi-service stack to templates, profiles, resource limits, and package imports.

## Questions & support

[Open an issue](https://github.com/service-federation/fed/issues). For environment problems, include `fed doctor` output.

## Contributing

Issues and PRs welcome. See [CONTRIBUTING.md](./CONTRIBUTING.md).

## License

MIT
