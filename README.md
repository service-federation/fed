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

Create `service-federation.yaml`:

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
    healthcheck:
      command: pg_isready -U postgres

  backend:
    process: npm start
    cwd: ./backend
    depends_on: [database]
    environment:
      PORT: '{{API_PORT}}'
      DATABASE_URL: 'postgres://localhost:{{DB_PORT}}/db'
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
- **Directory-scoped isolation** — All state (ports, containers, volumes) is scoped by working directory. Two directories are two independent stacks. Git worktrees give you parallel environments for free.
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

`isolated: true` gives the script fresh ports, scoped Docker containers and volumes, and automatic cleanup when it finishes. See [docs/scripts.md](docs/scripts.md) for details.

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

`fed start` will tell you exactly what's missing and where to put it. See [docs/configuration.md](docs/configuration.md) for details.

### Share them with your team

Manual secrets are the ones teammates end up passing around in Slack. Put them in your team's vault instead — then `fed start` fills them in for everyone:

```bash
fed login              # GitHub sign-in, once per machine
fed link acme/web      # bind this repo to your team's project (commit .fed/cloud.yaml)
fed secrets set STRIPE_SECRET_KEY   # value read from stdin, never argv
```

From then on, every teammate's `fed start` resolves `source: manual` secrets from the vault — clone, `fed login`, running. Values are cached locally (0600, gitignored), so `--offline` keeps working, and removing someone from the org revokes their access immediately.

Team secrets are part of [Service Federation Cloud](https://www.service-federation.com) — free during early access, development secrets only (it's a dev tool, not a production vault). See [docs/team-secrets.md](docs/team-secrets.md).

## Worktree & Cursor Isolation

Git worktrees are first-class. Each worktree gets its own ports, containers, and volumes:

```bash
~/project        $ fed start                # Default ports
~/project-review $ fed start --isolate    # Isolated ports, separate stack
```

Cursor's parallel agents create worktrees under the hood — `fed install && fed start --isolate` just works in each one. No plugin needed.

`fed ws` manages worktrees directly: `fed ws new feature -b`, `fed ws list`, `fed ws cd main`. See [docs/isolation.md](docs/isolation.md).

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

Global flags: `-v`, `-c <config>`, `-e <env>`, `-p <profile>`, `--offline`. Full reference: [docs/commands.md](docs/commands.md).

## Configuration

Services can be processes, Docker images, Compose services, or Gradle tasks. Config supports parameters with port allocation and secret generation, `.env` files, templates, profiles, cross-project packages, lifecycle hooks (`install`, `migrate`, `build`, `clean`), and startup messages.

Full reference: [docs/configuration.md](docs/configuration.md).

## Examples

See [`examples/`](./examples):

- [`simple.yaml`](./examples/simple.yaml) — Basic multi-service setup
- [`scripts-example.yaml`](./examples/scripts-example.yaml) — Scripts with dependencies
- [`env-file/`](./examples/env-file) — Environment files
- [`templates-example.yaml`](./examples/templates-example.yaml) — Service templates
- [`variables-example.yaml`](./examples/variables-example.yaml) — Environment-specific variables
- [`resource-limits-example.yaml`](./examples/resource-limits-example.yaml) — Memory, CPU, file descriptor limits
- [`docker-compose-example/`](./examples/docker-compose-example) — Docker Compose integration
- [`gradle-grouping.yaml`](./examples/gradle-grouping.yaml) — Gradle task batching
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

- [Configuration Reference](docs/configuration.md) — Services, parameters, health checks, templates, profiles, packages, resource limits, restart policies
- [Scripts](docs/scripts.md) — Scripts, isolated scripts, argument passing
- [Isolation](docs/isolation.md) — Directory scoping, worktrees, Cursor agents
- [Command Reference](docs/commands.md) — All commands, flags, and subcommands
- [Team Secrets](docs/team-secrets.md) — Shared development secrets via Service Federation Cloud

## Contributing

Issues and PRs welcome.

## License

MIT
