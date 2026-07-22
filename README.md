# fed

fed runs your app as a native process and your dependencies as Docker containers, in one dependency graph with healthcheck-gated startup. Each git worktree can get its own ports, containers, and volumes (`fed isolate enable`), so parallel checkouts and coding agents never collide.

This README gets you running. Everything else — full command, configuration, and secrets references — lives at **[service-federation.com/docs](https://www.service-federation.com/docs/)**.

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

Create `fed.yaml` (or run `fed init`). The older `service-federation.yaml` name is still fully supported. Adapt the config to your app — this example assumes a Node backend in `./backend` that reads `PORT` and serves `/health`:

```yaml
parameters:
  API_PORT:
    type: port
    default: 8080
  DB_PORT:
    type: port
    default: 5432

services:
  database:
    image: postgres:15
    ports: ["{{DB_PORT}}:5432"]
    environment:
      POSTGRES_PASSWORD: password
      POSTGRES_DB: app
    healthcheck:
      command: pg_isready -U postgres

  backend:
    process: npm start
    cwd: ./backend
    depends_on: [database]
    environment:
      PORT: '{{API_PORT}}'
      DATABASE_URL: 'postgres://postgres:password@localhost:{{DB_PORT}}/app'
    healthcheck:
      httpGet: 'http://localhost:{{API_PORT}}/health'

entrypoint: backend
```

```bash
fed start        # Start services (waits for healthchecks, backgrounds)
fed status       # What's running
fed logs backend # View logs
fed stop         # Stop all
```

That's the whole workflow. `git clone`, add a config, `fed start`, the project is running.

## In a repo that already uses fed?

If you found a `fed.yaml` (or `service-federation.yaml`) in a project — especially if you're a coding agent working in a checkout — these four rules keep you out of trouble:

1. **New worktree? Isolate first.** Run `fed isolate enable` before any other fed command. It persists: every fed command after it gets this directory's own ports, containers, and volumes.
2. **Run tasks through fed.** `fed <script>` (like `fed test:integration` or `fed psql`) resolves the ports and `DATABASE_URL` this directory was actually allocated. The same command run bare hits whichever checkout owns the default ports. The `scripts:` section of `fed.yaml` lists what's available.
3. **Port conflict on start?** Another checkout owns those ports — the fix is `fed isolate enable`. Never `fed start --replace` in a worktree: it takes the port by killing the other checkout's services.
4. **Look before you guess.** `fed status`, `fed ports list`, and `fed logs <service> --tail 100` show what's running, where, and why it failed.

Details: [coding agents & isolation](https://www.service-federation.com/docs/isolation/#coding-agents).

## Why fed

- **One config, one command** — Docker containers, native processes, and Compose services all live in one `fed.yaml`. `fed start` handles dependency ordering and health checks.
- **Directory-scoped isolation** — Containers, volumes, and state are namespaced by working directory automatically; `fed isolate enable` gives a checkout its own ports too. Git worktrees plus one command = parallel environments.
- **No Docker Compose sprawl** — Port parameters, templating, profiles, and cross-project packages replace the pile of override files and `.env` juggling.

## Highlights

Each of these is a one-liner here and a full chapter in the docs:

- **Startup-gating hooks** — `install:` runs once per checkout, `migrate:` runs on every start; both run after a service's dependencies are healthy and before its dependents boot, so nothing starts against an empty database or missing `node_modules`. [Lifecycle hooks →](https://www.service-federation.com/docs/configuration/)
- **Hook-only services** — a node that *is* the migration: declare `migrate:` with no process, and every service that needs the schema depends on it. [Hook-only services →](https://www.service-federation.com/docs/configuration/#hook-only)
- **Isolated scripts** — `isolated: true` gives integration tests a throwaway stack on fresh ports, cleaned up on every exit path, while your dev stack keeps running. [Scripts →](https://www.service-federation.com/docs/scripts/#isolated-scripts)
- **Generated secrets** — `type: secret` parameters get random values on first start, stored under `.fed/` where fed's own gitignore keeps them out of commits. No more `POSTGRES_PASSWORD: password`. [Generated secrets →](https://www.service-federation.com/docs/generated-secrets/)
- **Team secrets** — set a development API key once in the [dashboard](https://app.service-federation.com); every teammate's `fed start` finds it after `fed login` + `fed link`. Free for the first 3 people in an org, €8/seat/month beyond. [Team secrets →](https://www.service-federation.com/docs/secrets/)
- **`{{FED_PROJECT_ID}}`** — a stable per-checkout identifier, available in every template without declaring it. Namespace whatever parallel stacks would otherwise share — cookie names, queue prefixes. [Details →](https://www.service-federation.com/docs/configuration/#fed-project-id)
- **Worktrees & coding agents** — one worktree per agent, one stack per worktree; `fed ws new feature -b` / `fed ws list` / `fed ws cd main` manage the worktrees themselves. [Worktrees →](https://www.service-federation.com/docs/isolation/)

Running coding agents? Add one rule to your `AGENTS.md` so every agent isolates before it collides:

```markdown
## Worktrees
Run `fed isolate enable` before any other fed command in a new worktree.
```

## Documentation

`fed --help` is always current. The full references live on the website:

- [Quickstart](https://www.service-federation.com/docs/) — install to running stack
- [Configuration](https://www.service-federation.com/docs/configuration/) — services, parameters, secrets, health checks, templates, profiles, packages, lifecycle hooks, resource limits
- [Commands](https://www.service-federation.com/docs/commands/) — all commands, flags, and subcommands
- [Scripts](https://www.service-federation.com/docs/scripts/) — script lifecycle ("borrow or own"), isolated scripts, argument passing
- [Isolation](https://www.service-federation.com/docs/isolation/) — directory scoping, worktrees, coding agents
- [Generated secrets](https://www.service-federation.com/docs/generated-secrets/) — stable local passwords and keys
- [Team secrets](https://www.service-federation.com/docs/secrets/) — shared development secrets via Service Federation Cloud

## Examples

See [`examples/`](./examples):

- [`simple.yaml`](./examples/simple.yaml) — Basic multi-service setup
- [`scripts-example.yaml`](./examples/scripts-example.yaml) — Scripts with dependencies
- [`env-file/`](./examples/env-file) — Environment files
- [`templates-example.yaml`](./examples/templates-example.yaml) — Service templates
- [`parameters-example.yaml`](./examples/parameters-example.yaml) — Environment-specific parameters
- [`resource-limits-example.yaml`](./examples/resource-limits-example.yaml) — Memory, CPU, file descriptor limits
- [`docker-compose-example/`](./examples/docker-compose-example) — Docker Compose integration
- [`profiles-example.yaml`](./examples/profiles-example.yaml) — Profiles
- [`service-merging/`](./examples/service-merging) — Package imports

## Contributing

Issues and PRs welcome. See [CONTRIBUTING.md](./CONTRIBUTING.md).

## License

MIT
