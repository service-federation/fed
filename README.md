# fed

[![Release](https://img.shields.io/github/v/release/service-federation/fed?color=green)](https://github.com/service-federation/fed/releases/latest)
[![CI](https://github.com/service-federation/fed/actions/workflows/ci.yml/badge.svg)](https://github.com/service-federation/fed/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/github/license/service-federation/fed)](./LICENSE)

**Git isolates files. `fed` isolates the stack.**

Run your app natively and its dependencies in Docker from one `fed.yaml`. Give every Git worktree and coding agent its own ports, containers, and volumes — and stop copy-pasting `.env` files across checkouts and teammates.

```console
$ fed start
Starting: api (with deps: database, schema)

  database... ready
  schema... ready
  api... ready

API ready: http://localhost:8080
```

![fed demo: starting a mixed native and Docker stack, then running an isolated copy from a Git worktree](docs/fed-demo.gif)

## Install

```bash
# macOS / Linux (Homebrew)
brew install service-federation/tap/fed

# macOS / Linux (installer script)
curl -LsSf https://github.com/service-federation/fed/releases/latest/download/fed-installer.sh | sh
```

Prebuilt binaries are on [GitHub Releases](https://github.com/service-federation/fed/releases/latest), or build from source with `cargo install --git https://github.com/service-federation/fed --locked`. Run `fed doctor` to verify Docker and Git.

## One stack per Git worktree

Git isolates files. `fed` isolates the runtime state that still collides: declared ports, Docker container names, named volumes, generated secrets, and `.fed/` state.

```console
~/app          $ fed start

~/app-task-1   $ fed isolate enable
~/app-task-1   $ fed start

~/app-task-2   $ fed isolate enable
~/app-task-2   $ fed start
```

Your primary checkout stays untouched. For coding agents, add this to `AGENTS.md` or `CLAUDE.md`:

```markdown
Run `fed isolate enable` before any other fed command in a new worktree.
Run project tasks through fed so they receive this worktree's ports and credentials.
Run `fed clean` before removing the worktree.
```

No editor plugin or agent wrapper is required — the isolation boundary is the checkout directory. Deleted a worktree without stopping it first? `fed prune` finds and cleans up orphaned containers and volumes ([details](docs/advanced.md#cleaning-up-after-deleted-worktrees)). See [Worktrees and coding agents](https://www.service-federation.com/docs/isolation/).

## Stop copy-pasting `.env` files

- **Local generated secrets (free, offline):** `type: secret` generates a stable per-checkout value in a mode-`0600`, Git-ignored file under `.fed/`, with no account or network access.
- **Optional team dev vault:** For shared dev API keys (Google Maps, Stripe, OAuth, private image pull tokens), link the repo once with `fed link org/project`. Teammates sign in with GitHub (`fed login`), and `fed start` injects the values into every checkout and worktree.

```bash
fed secrets set GOOGLE_MAPS_API_KEY               # hidden prompt in a terminal
printf '%s' "$KEY" | fed secrets set STRIPE_KEY   # or from stdin
```

**Pricing:** Free for up to 3 people, then €8/seat/month. Billing only gates adding new members — it never blocks `fed start`.

> Built for development credentials, not production secrets. The MIT CLI and local secret generation work completely offline without an account. See [Team secrets](https://www.service-federation.com/docs/secrets/) for the security model, cache modes, and rotation guidance.

## Quick start (`fed.yaml`)

Create `fed.yaml` (or run `fed init`) in your project root:

```yaml
parameters:
  API_PORT: { type: port, default: 8080 }
  DB_PORT: { type: port, default: 5432 }
  DB_PASSWORD: { type: secret }

services:
  database:
    image: postgres:16-alpine
    ports: ["{{DB_PORT}}:5432"]
    volumes: ["app-data:/var/lib/postgresql/data"]
    environment:
      POSTGRES_PASSWORD: "{{DB_PASSWORD}}"
      POSTGRES_DB: app
    healthcheck:
      command: pg_isready -U postgres

  api:
    process: npm start
    depends_on: [database]
    environment:
      PORT: "{{API_PORT}}"
      DATABASE_URL: "postgres://postgres:{{DB_PASSWORD}}@localhost:{{DB_PORT}}/app"
    healthcheck:
      http_get: "http://localhost:{{API_PORT}}/health"
    startup_message: "API ready: http://localhost:{{API_PORT}}"

entrypoint: api
```

```bash
fed validate
fed start
fed status
fed logs api
fed stop
```

Want to verify your machine against a runnable Python + PostgreSQL project first?

```bash
git clone https://github.com/service-federation/fed.git
cd fed/docs/demo
fed start
curl http://localhost:18480/health
fed stop && fed clean
```

## Project scripts & isolated integration tests

Declare database tools, seeds, and integration tests under `scripts:`. `fed` starts missing dependencies in health-checked order, injects the checkout's resolved ports and secrets, and stops what it started.

```yaml
scripts:
  test:integration:
    depends_on: [database, api]
    isolated: true
    environment:
      DATABASE_URL: "postgres://postgres:{{DB_PASSWORD}}@localhost:{{DB_PORT}}/app"
    script: npm run test:integration
```

```bash
fed test:integration
```

With `isolated: true`, the test gets a throwaway stack and leaves your development database untouched. See [Scripts and tests](https://www.service-federation.com/docs/scripts/).

## More features & reference

- **Interactive shells, TTY, & `fed attach`** — run foreground processes (`fed start -i`), keep REPLs alive with `tty: true`, and attach/detach live terminals ([`docs/advanced.md`](docs/advanced.md#services-run-in-the-background)).
- **Service variants & shared defaults** — swap implementations behind one service contract (`variants:`) and share config via `defaults:` / `templates:` ([`docs/advanced.md`](docs/advanced.md#one-service-several-implementations)).
- **Healthcheck timing & startup semantics** — tune `start_period`, `interval`, `probe_timeout`, and `retries` ([`docs/advanced.md`](docs/advanced.md#healthcheck-timing)).
- **Log retention, private registries, & host networking** — configure log rotation (`logs:`), pull private images via `registry_auth`, pin `platform: linux/amd64`, and reach host services via `host.docker.internal` ([`docs/advanced.md`](docs/advanced.md#log-retention)).
- **Existing Docker Compose files** — import basic Compose stacks alongside native processes ([`docs/advanced.md`](docs/advanced.md#existing-docker-compose-projects), [Compose docs](https://www.service-federation.com/docs/compose/)).

### What `fed` touches

`fed` shells out to `docker` and `docker compose` with your user's existing Docker access. Per-checkout state, logs, port allocations, and generated secrets live inside `.fed/` (with `.fed/.gitignore` managed automatically). It only opens network connections to configured healthcheck URLs and, if you explicitly run `fed login` and `fed link`, the optional team vault.

## Documentation & examples

- [Quickstart](https://www.service-federation.com/docs/) · [Worktrees & coding agents](https://www.service-federation.com/docs/isolation/) · [Team secrets](https://www.service-federation.com/docs/secrets/) · [Configuration reference](https://www.service-federation.com/docs/configuration/) · [Command reference](https://www.service-federation.com/docs/commands/)
- Examples: [Rails + Sidekiq](./examples/rails-sidekiq.yaml), [FastAPI + Celery](./examples/python-fastapi.yaml), [Go microservices](./examples/go-microservices.yaml), [Node microservices](./examples/nodejs-microservices.yaml), [Service variants](./examples/variants-example.yaml)

`fed --help` is the command-line source of truth for your installed version.

## Support & License

[Open an issue](https://github.com/service-federation/fed/issues) with `fed doctor` output for bugs or questions. Contributions are welcome — see [CONTRIBUTING.md](./CONTRIBUTING.md).

MIT
