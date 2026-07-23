# fed

[![Release](https://img.shields.io/github/v/release/service-federation/fed?color=green)](https://github.com/service-federation/fed/releases/latest)
[![CI](https://github.com/service-federation/fed/actions/workflows/ci.yml/badge.svg)](https://github.com/service-federation/fed/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/github/license/service-federation/fed)](./LICENSE)

Run your app natively and its dependencies in Docker from one config file. Think `docker compose up`, except the app isn't in a container and every Git worktree can run its own isolated copy of the stack.

```console
$ fed start
Starting: api (with deps: database, schema)

  database... ready
  schema... ready
  api... ready

API ready: http://localhost:8080
```

fed handles the parts of local development that shell scripts and Compose override files usually leave to people:

- Starts native processes, Docker containers, and Compose services in dependency order.
- Polls configured health checks and reports the result for each service.
- Allocates stable ports and scopes containers, volumes, and state to each checkout.
- Runs project commands with the ports and credentials allocated to that checkout.
- Starts and cleans up throwaway native and image-backed stacks for integration tests.

The CLI is open source, works without an account, and does not replace Docker or Compose. It coordinates them with the processes that should stay on your host.

![fed demo: starting a mixed native and Docker stack, then running an isolated copy from a Git worktree](docs/fed-demo.gif)

## Install

```bash
brew install service-federation/tap/fed
```

macOS and Linux binaries are also available from [GitHub Releases](https://github.com/service-federation/fed/releases/latest). To build the current main branch from source:

```bash
cargo install --git https://github.com/service-federation/fed --locked
```

Run `fed doctor` after installation to check Docker, Compose, and Git.

## Try a real stack

The repository includes a small Python and PostgreSQL project. It has a native API, a Docker database, a schema migration, and health checks.

```bash
git clone https://github.com/service-federation/fed.git
cd fed/docs/demo
fed start
curl http://localhost:18480/health
fed stop
fed clean
```

This proves the whole path before you write a config for your own project.

## Add fed to a project

Create `fed.yaml` in the project root. This example runs PostgreSQL in Docker and a Node API on the host:

```yaml
parameters:
  API_PORT: { type: port, default: 8080 }
  DB_PORT: { type: port, default: 5432 }
  DB_PASSWORD: { type: secret }

services:
  database:
    image: postgres:16-alpine
    ports: ["{{DB_PORT}}:5432"]
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
      httpGet: "http://localhost:{{API_PORT}}/health"
    startup_message: "API ready: http://localhost:{{API_PORT}}"

entrypoint: api
```

Then run:

```bash
fed validate
fed start
fed status
fed logs api
fed stop
```

`fed init` can create a starter file. The [configuration reference](https://www.service-federation.com/docs/configuration/) covers all service types and fields.

### Startup health semantics

`fed start` waits for each service's healthcheck before starting its dependents.

- A healthcheck that passes before startup returns marks the service `healthy`. The `timeout` is evaluated between polling attempts, so a check already in flight at the deadline may still count.
- A healthcheck that does not pass within its `timeout` (default 5s) is a warning, not an error: the process keeps running, dependents still start, and the run ends with `Services started with N health warning(s)` instead of the success line. The exit code stays 0.
- A process that dies before its healthcheck passes fails the start with a non-zero exit code.

In `fed status`, `running` means the process is up but no healthcheck has confirmed it; only `healthy` means verified. `fed status --json` reports the same distinction as `"status": "running"` with `"health": "unknown"`.

## One stack per worktree

Your coding agents stop killing each other's databases. One worktree per agent, one full stack per worktree, zero shared ports.

Git isolates files. fed isolates the runtime state that usually still collides.

```console
~/app         $ fed start

~/app-task-1 $ fed isolate enable
~/app-task-1 $ fed start

~/app-task-2 $ fed isolate enable
~/app-task-2 $ fed start
```

Each isolated checkout gets its own values for declared `type: port` parameters, direct Docker container names, named volumes, generated secrets, and fed state.

Caveat: fed cannot remap a port hardcoded inside a command, URL, or Compose file. Declare every host port as a `type: port` parameter. See [Worktrees and coding agents](https://www.service-federation.com/docs/isolation/) for Compose behavior, bind mounts, cookies, cleanup, and the full isolation model.

For coding agents, put this in `AGENTS.md` or `CLAUDE.md`:

```markdown
Run `fed isolate enable` before any other fed command in a new worktree.
Run project tasks through fed so they receive this worktree's ports and credentials.
Run `fed clean` before removing the worktree.
```

No editor plugin or agent integration is required. The boundary is the checkout directory.

## Project commands and integration tests

Scripts declared in `fed.yaml` know which services they need:

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

fed starts missing dependencies, polls their configured health checks, runs the command with resolved values, and cleans up the services it started. For native and direct image-backed services, an isolated script gets a throwaway stack and leaves your development stack alone. Compose-backed dependencies have the limits described in the guide below.

See [Scripts and tests](https://www.service-federation.com/docs/scripts/) for lifecycle rules, argument passing, output modes, and isolation limits.

## Existing Docker Compose projects

You do not need to translate every container into fed. A service can point at an existing Compose service while your application stays native:

```yaml
services:
  database:
    composeFile: ./compose.yaml
    composeService: postgres

  api:
    process: npm run dev
    depends_on: [database]
```

Read [Worktrees and coding agents](https://www.service-federation.com/docs/isolation/) before enabling worktree isolation. Compose ports must use environment substitution so fed can allocate them.

## Local and team secrets

`type: secret` creates a stable local development value in a mode-0600, Git-ignored file. This needs no account or network access.

For credentials a team must share, the optional Service Federation Cloud vault can fill `source: manual` values during `fed start`. The first three people in an organization are free, then each additional seat costs €8 per month. It is for development credentials, not production secrets or compliance workloads.

Removing a member blocks new server fetches. It cannot erase values already cached on that person's machine, so rotate credentials after removing someone who had access. Read [Generated secrets](https://www.service-federation.com/docs/generated-secrets/) and [Team secrets](https://www.service-federation.com/docs/secrets/) before choosing either path.

## Documentation and examples

- [Quickstart](https://www.service-federation.com/docs/)
- [Worktrees and coding agents](https://www.service-federation.com/docs/isolation/)
- [Configuration reference](https://www.service-federation.com/docs/configuration/)
- [Scripts and tests](https://www.service-federation.com/docs/scripts/)
- [Command reference](https://www.service-federation.com/docs/commands/)

Example configs include [Rails with Sidekiq](./examples/rails-sidekiq.yaml), [FastAPI with Celery](./examples/python-fastapi.yaml), [Go microservices](./examples/go-microservices.yaml), [Node services](./examples/nodejs-microservices.yaml), and [an existing Docker Compose project](./examples/docker-compose-example/).

`fed --help` is the command-line source of truth for the installed version.

## Support and contributing

[Open an issue](https://github.com/service-federation/fed/issues) for bugs, questions, or feature requests. Include `fed doctor` output when the problem depends on the local environment.

Contributions are welcome. See [CONTRIBUTING.md](./CONTRIBUTING.md).

## License

MIT
