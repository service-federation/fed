# fed

Orchestrate your local dev stack from one config file. Docker containers and native processes with dependency-aware startup, healthchecks, and directory-scoped isolation.

## Quick Start

```bash
# macOS / Linux (Homebrew)
brew install service-federation/tap/fed

# Prebuilt binary
curl --proto '=https' --tlsv1.2 -LsSf https://github.com/service-federation/fed/releases/latest/download/fed-installer.sh | sh

# From source
cargo install --git https://github.com/service-federation/fed
```

Create `service-federation.yaml`. Adapt it to your app — this example assumes a Node backend in `./backend` that reads `PORT` and serves `/health`:

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

## Why fed

- **One config, one command** — Docker containers, native processes, and Compose services all live in one `service-federation.yaml`. `fed start` handles dependency ordering and health checks.
- **Directory-scoped isolation** — Containers, volumes, and state are namespaced by working directory automatically; `fed start --isolate` gives a checkout its own ports too. Git worktrees plus one flag = parallel environments.
- **No Docker Compose sprawl** — Port parameters, templating, profiles, and cross-project packages replace the pile of override files and `.env` juggling.

## Isolated Scripts

Run integration tests against a throwaway stack without touching your dev services:

```yaml
scripts:
  test:integration:
    isolated: true     # Fresh ports, scoped volumes, separate containers
    depends_on: [database, api]
    script: npm run test:e2e
```

```bash
fed start                # Dev stack stays running
fed test:integration     # Tests get their own stack, cleaned up after
```

`isolated: true` gives the script fresh ports, scoped Docker containers and volumes, and automatic cleanup when it finishes. See [the scripts docs](https://www.service-federation.com/docs/scripts/) for details.

## Secrets

No more `POSTGRES_PASSWORD: password` in your config. Secret parameters are generated on first `fed start` and kept out of git:

```yaml
generated_secrets_file: .env.secrets  # must be in .gitignore

parameters:
  DB_PASSWORD:
    type: secret
  SESSION_KEY:
    type: secret
```

`fed start` generates random values, writes them to `.env.secrets`, and uses them everywhere `{{DB_PASSWORD}}` and `{{SESSION_KEY}}` appear. Values are stable across restarts — generated once, reused forever.

For secrets you manage yourself (API keys, OAuth credentials), use `source: manual`:

```yaml
parameters:
  STRIPE_SECRET_KEY:
    type: secret
    source: manual
    description: "From https://dashboard.stripe.com/apikeys"
```

`fed start` will tell you exactly what's missing and where to put it. See [the configuration reference](https://www.service-federation.com/docs/configuration/) for details.

### Share them with your team

Manual secrets are the ones teammates end up passing around in Slack. Put them in your team's vault instead — then `fed start` fills them in for everyone:

```bash
fed login              # GitHub sign-in, once per machine
fed link acme/web      # bind this repo to your team's project (commit .fed/cloud.yaml)
fed secrets set STRIPE_SECRET_KEY   # value read from stdin, never argv
```

From then on, every teammate's `fed start` resolves `source: manual` secrets from the vault — clone, `fed login`, running. Values are cached locally (0600, gitignored), so `--offline` keeps working. Removing someone from the org blocks their next fetch — values already cached on their disk stay readable, as with any local cache.

Team secrets are part of [Service Federation Cloud](https://www.service-federation.com) — free during early access, development secrets only (it's a dev tool, not a production vault). See [the team secrets docs](https://www.service-federation.com/docs/secrets/).

## Worktree & Cursor Isolation

Git worktrees are first-class. Each worktree gets its own containers, volumes, and state automatically — add `--isolate` for its own ports too:

```bash
~/project        $ fed start                # Default ports
~/project-review $ fed start --isolate    # Isolated ports, separate stack
```

Cursor's parallel agents create worktrees under the hood — `fed install && fed start --isolate` just works in each one. No plugin needed.

`fed ws` manages worktrees directly: `fed ws new feature -b`, `fed ws list`, `fed ws cd main`. See [the isolation docs](https://www.service-federation.com/docs/isolation/).

## Commands

```bash
fed start [--isolate|--replace|--dry-run|-w]  # Start services
fed stop / restart                               # Stop / restart
fed status [--json]                              # Service status
fed logs <svc> [--follow]                        # View logs
fed tui / top                                    # Dashboard / resource usage
fed isolate enable / disable / status / rotate  # Isolation mode
fed ports list [--json]                         # Port allocations
fed run <script> [-- args]                       # Run a script
fed install / build / clean                      # Lifecycle hooks
fed validate                                     # Validate config
fed docker build [--json] / push                 # Docker images
fed ws new / list / cd / rm                      # Worktrees (beta)
fed login / logout / whoami                      # Service Federation Cloud auth
fed link <org>/<project>                         # Bind checkout to a Cloud project
fed secrets ls / set <NAME>                      # Team development secrets
fed doctor                                       # Check requirements
fed init                                         # Create starter config
```

Global flags: `-v`, `-c <config>`, `-e <env>`, `-p <profile>`, `--offline`. Full reference: [the command reference](https://www.service-federation.com/docs/commands/).

## Configuration

Services can be processes, Docker images, or Compose services. Config supports parameters with port allocation and secret generation, `.env` files, templates, profiles, cross-project packages, lifecycle hooks (`install`, `migrate`, `build`, `clean`), and startup messages.

Full reference: [the configuration reference](https://www.service-federation.com/docs/configuration/).

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

## Troubleshooting

**Services not starting?**
```bash
fed logs <service> --tail 100
```

**Port conflicts?**
```bash
fed start --isolate     # Sidestep conflicts
fed start --replace     # Kill conflicting processes
```

## Documentation

- [Configuration Reference](https://www.service-federation.com/docs/configuration/) — Services, parameters, health checks, templates, profiles, packages, resource limits, restart policies
- [Scripts](https://www.service-federation.com/docs/scripts/) — Scripts, isolated scripts, argument passing
- [Isolation](https://www.service-federation.com/docs/isolation/) — Directory scoping, worktrees, Cursor agents
- [Command Reference](https://www.service-federation.com/docs/commands/) — All commands, flags, and subcommands
- [Team Secrets](https://www.service-federation.com/docs/secrets/) — Shared development secrets via Service Federation Cloud

## Contributing

Issues and PRs welcome.

## License

MIT
