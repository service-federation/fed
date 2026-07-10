# fed

Orchestrate your local dev stack from one config file. Docker containers and native processes with dependency-aware startup, healthchecks, and directory-scoped isolation.

Full documentation: **[service-federation.com/docs](https://www.service-federation.com/docs/)** — this README is the short version.

## Quick Start

```bash
# macOS / Linux (Homebrew)
brew install service-federation/tap/fed

# Prebuilt binary
curl --proto '=https' --tlsv1.2 -LsSf https://github.com/service-federation/fed/releases/latest/download/fed-installer.sh | sh

# From source
cargo install --git https://github.com/service-federation/fed
```

Create `service-federation.yaml` (or run `fed init`). Adapt it to your app — this example assumes a Node backend in `./backend` that reads `PORT` and serves `/health`:

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

If you found a `service-federation.yaml` in a project — especially if you're a coding agent working in a checkout — these four rules keep you out of trouble:

1. **New worktree? Isolate first.** Run `fed isolate enable` before any other fed command. It persists: every fed command after it gets this directory's own ports, containers, and volumes.
2. **Run tasks through fed.** `fed <script>` (like `fed test:integration` or `fed psql`) resolves the ports and `DATABASE_URL` this directory was actually allocated. The same command run bare hits whichever checkout owns the default ports. The `scripts:` section of `service-federation.yaml` lists what's available.
3. **Port conflict on start?** Another checkout owns those ports — the fix is `fed isolate enable`. Never `fed start --replace` in a worktree: it takes the port by killing the other checkout's services.
4. **Look before you guess.** `fed status`, `fed ports list`, and `fed logs <service> --tail 100` show what's running, where, and why it failed.

Details: [isolation docs](https://www.service-federation.com/docs/isolation/).

## Why fed

- **One config, one command** — Docker containers, native processes, and Compose services all live in one `service-federation.yaml`. `fed start` handles dependency ordering and health checks.
- **Directory-scoped isolation** — Containers, volumes, and state are namespaced by working directory automatically; `fed isolate enable` gives a checkout its own ports too. Git worktrees plus one command = parallel environments.
- **No Docker Compose sprawl** — Port parameters, templating, profiles, and cross-project packages replace the pile of override files and `.env` juggling.

## Highlights

**Isolated scripts** — integration tests get a throwaway stack (fresh ports, scoped containers, cleaned up on every exit path) while your dev stack keeps running:

```yaml
scripts:
  test:integration:
    isolated: true
    depends_on: [database, api]
    script: npm run test:e2e
```

**Generated secrets** — `type: secret` parameters are generated on first `fed start` and written to a gitignored file; `source: manual` secrets fail startup with a message saying exactly what to provide and where. No more `POSTGRES_PASSWORD: password` in the config.

**Team secrets** — put manual secrets in your team's vault once (`fed login`, `fed link acme/web`, `fed secrets set STRIPE_SECRET_KEY`) and every teammate's `fed start` resolves them. Part of [Service Federation Cloud](https://www.service-federation.com), free during early access. Development secrets only — it's a dev tool, not a production vault.

**Worktrees & coding agents** — Claude Code, Cursor, and Codex parallelize with one worktree per agent, and each worktree can run its own full stack. Add one rule to your `AGENTS.md` so every agent isolates before it collides:

```markdown
## Worktrees
Run `fed isolate enable` before any other fed command in a new worktree.
```

`fed ws` manages worktrees directly: `fed ws new feature -b`, `fed ws list`, `fed ws cd main`.

## Commands & configuration

`fed --help` is always current. The full references live on the website:

- [Command reference](https://www.service-federation.com/docs/commands/) — all commands, flags, and subcommands
- [Configuration reference](https://www.service-federation.com/docs/configuration/) — services, parameters, secrets, health checks, templates, profiles, packages, lifecycle hooks, resource limits
- [Scripts](https://www.service-federation.com/docs/scripts/) — script lifecycle ("borrow or own"), isolated scripts, argument passing
- [Isolation](https://www.service-federation.com/docs/isolation/) — directory scoping, worktrees, coding agents
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
