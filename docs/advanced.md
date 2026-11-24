# Advanced configuration and runtime behavior

Detailed reference for process lifecycle, log retention, deleted-worktree pruning, service variants, shared defaults, healthcheck timing, private registries, multi-architecture images, host networking, and Docker Compose imports. See the [full documentation](https://www.service-federation.com/docs/) for guides and command references.

## Process lifecycle, logs, TTY, and health semantics

### Startup health semantics

`fed start` waits for each service's healthcheck before starting its dependents.

- A healthcheck that passes before startup returns marks the service `healthy`. The `timeout` is evaluated between polling attempts, so a check already in flight at the deadline may still count.
- A healthcheck that does not pass within its `timeout` (default 5s) is a warning, not an error: the process keeps running, dependents still start, and the run ends with `Services started with N health warning(s)` instead of the success line. The exit code stays 0.
- A process that dies before its healthcheck passes fails the start with a non-zero exit code.

In `fed status`, `running` means the process is up but no healthcheck has confirmed it; only `healthy` means verified. `fed status --json` reports the same distinction as `"status": "running"` with `"health": "unknown"`.

### Services run in the background

`fed start` detaches every process service: it gets no stdin, and its output goes to `.fed/logs/<service>.log`. Follow it with `fed logs -f <service>` or watch everything in `fed tui`. A program that waits for terminal input, such as a shell or a REPL, exits as soon as it starts, because there is nothing to read.

To keep such a program running in the background, give it `tty: true` in `fed.yaml`. fed then runs it under a terminal of its own, so a shell or a REPL stays up with nothing to read from. Everything the program shows on that terminal goes to the log file, including the input it echoes back. Process services only, and not on Windows.

To run one such service in your terminal instead, start it with `-i`:

```console
$ fed start -i shell
```

fed starts that service's dependencies in the background as usual, hands it your terminal, and waits for it. Ctrl+C goes to the service, not to fed. When it exits, fed exits with the same code and leaves the dependencies running for `fed stop`. One service at a time, process services only.

When `fed start -i` doesn't receive a service, it uses the entrypoint. Just like `fed start`. That works when the entrypoint is itself a process service. An entrypoint that only groups other services (a `depends_on:` list with no `process:`) has nothing to hand the terminal to, so fed asks you to pass one of them.

### Log retention

Service logs rotate at 10 MiB and retain three files by default, including the active log. Set `logs.max_size` and `logs.max_files` in `fed.yaml` to change either limit for the project, or put the same `logs:` block in `$XDG_CONFIG_HOME/fed/config.yaml` (normally `~/.config/fed/config.yaml`) for all projects. Each project setting overrides the corresponding global setting. For example:

```yaml
logs:
  max_size: 20MiB
  max_files: 5
```

`max_size` accepts `B`, `KiB`, `MiB`, `GiB`, and `TiB` suffixes. The limits are 1 byte to 1 TiB and 1 to 1000 files.

Changes to `logs:` apply when a service restarts. At startup, fed trims oversized existing log files and removes backups beyond the new limit; `fed logs` reads the retained files in order, including archives. The same global limits bound supervisor, host, and TUI diagnostics. Docker logs remain managed by Docker.

If `XDG_CONFIG_HOME` is unset, empty, or relative, fed uses `~/.config`. Reading an absent global config does not create a directory or file.

### Attach to a running service

Give a process service `tty: true`, then run `fed start <service>` and
`fed attach <service>`. Your input goes to the service and its output appears
in your terminal. Press Ctrl+P, then Ctrl+Q to detach; the service keeps running.
Use `--detach-keys ctrl-a,ctrl-d` to choose another pair.

`fed attach <service> --no-stdin` follows output without forwarding input.
Ctrl+C ends that client. Multiple clients can attach to the same service.
An attached client exits with the service's exit code when the service stops.

Terminal output, including typed input the program echoes, is saved in
`.fed/logs/<service>.log` and available through `fed logs <service>`.

## Worktree cleanup

### Cleaning up after deleted worktrees

`fed stop` removes a stack's containers but keeps its named volumes, so a stopped stack still has its database when you start it again. A worktree deleted without `fed stop` leaves its containers and volumes behind. `fed prune` finds those:

```bash
fed prune          # list what it would remove, grouped by reason, with the checkout path and age
fed prune --force  # remove it (without --force it asks, or in a script only lists)
```

Every fed command that starts, stops or restarts services or runs a script in a checkout records which directory owns its stack in `~/.fed/stacks/`. `fed prune` removes a stack only when it can show that its directory no longer exists, that the checkout has moved to another isolation id (`fed isolate rotate` or `disable`), or that it belonged to an `isolated: true` script run whose process is dead. A stopped stack in a checkout that still exists is kept. So is any stack created by fed 8.3.2 or earlier until a fed command runs in its checkout. If you move or rename a checkout, run any fed command in it before `fed prune`: until then the registry still points at the old path, which no longer exists. `fed prune --adopt ~/worktrees` registers the checkouts it finds below a directory so those stacks are recognised; adding `--unowned` also removes the stacks none of them claims, if nothing in them is running and their newest container or volume was created at least a week ago (`--unowned-min-age-days`; Docker records creation, not last use, so a stopped stack is as old as its volumes). That is you vouching that the `--adopt` directories hold every checkout you still use.

An `isolated: true` script run removes its throwaway stack on Ctrl+C, SIGTERM and SIGHUP too. SIGKILL cannot be caught, so a killed run leaves its stack; the next fed command in any checkout notices that the run's process is gone and removes it, and `fed prune` lists it. `fed doctor` reports how many stacks are reclaimable.

## One service, several implementations

A rewrite keeps the old and the new implementation side by side for months. Both listen on the same port, both satisfy the same dependants, and only one may run at a time. Profiles can't express that — they add services, they never replace one.

`variants:` keeps one service name and one contract, and moves only the implementation:

```yaml
services:
  catalog:
    depends_on: [storage]
    healthcheck: { http_get: 'http://localhost:{{CATALOG_PORT}}/health' }
    default_variant: java
    variants:
      java:
        gradle_task: ':catalog:run'
      go:
        process: go run ./cmd/catalog
        cwd: services/catalog-go
```

The outer service holds the shared contract — ports, healthcheck, `depends_on`, tags — and carries no type-defining field of its own. Each variant supplies exactly one, plus whatever else differs. The chosen variant is merged over the outer service before anything else runs, so dependants, `fed status` and the dependency graph see an ordinary service; `frontend`'s `depends_on: [catalog]` holds whichever implementation is live.

Variant names are a vocabulary shared across services, so the main control is an ordered preference list rather than a per-service flag:

```bash
fed start --variant go,ts,rust    # first name each service actually has
fed start --variant catalog:java  # pin the odd one out
fed variant set go,ts,rust        # persist it; fed variant list shows the result
```

For each service, the first name in the list it offers wins; otherwise its `default_variant`. A name no service has produces a warning, so one list can serve several checkouts. Precedence, highest first: a `--variant` pin, the `--variant` list, the choice remembered from an earlier `fed start --variant` or `fed restart --variant` (`.fed/variants.session.yaml`), `.fed/variants.yaml` (written by `fed variant set`), the variant a service is running as right now, then `default_variant`. Persisting matters because `fed start web`, `fed restart`, `fed status`, the TUI and the background supervisor all have to agree without the flag being repeated: after `fed start --variant catalog:java` the checkout keeps `java` for `fed start web`, `fed restart` and a stopped-and-restarted `catalog` until a later `--variant` replaces it, `fed variant set`, `unset` or `clear` says otherwise, or a whole-stack `fed stop` ends the stack.

To show which implementation runs, `fed start` and `fed status` print the variant next to the service name, as in `catalog (go)`. When the choice comes from `.fed/variants.yaml` or an earlier `--variant`, `fed start` says so and prints the command that undoes it.

Switching variant while the service runs is a `fed stop` and a `fed start` — the ports and container names are the same, so the two implementations can't coexist. A variant that needs a *different* port is a different service, not a variant: the contract lives on the outer service by design.

See [`examples/variants-example.yaml`](../examples/variants-example.yaml) for a minimal version, or the [variant stack walkthrough](../examples/variant-stack/) for real HTTP services, a browser page, health failure/recovery, and an attachable console.

### Shared defaults and whole-stack startup

Use `defaults:` for common service fields. A service or template's own values
win; collections are combined. An explicit `depends_on: []` opts out of
inherited dependencies, including through a template or variant. Services
named in `defaults.depends_on` are excluded from all defaults to avoid cycles;
`fed validate` reports that exclusion once.

```yaml
defaults:
  depends_on: [storage]
  healthcheck_timeout: 30s
  startup_timeout: 1m

templates:
  go-worker:
    process: go run ./cmd/worker

services:
  storage: { process: sleep 300 }
  catalog:
    variants:
      java: { process: sleep 300 }
      go: { extends: go-worker, cwd: workers/catalog }
    default_variant: java
  standalone: { process: sleep 300, depends_on: [] }

entrypoint: '*'
```

Variants can extend local templates or exposed package services, using the
same `extends:` rules as ordinary services. Templates cannot extend templates.
Defaults fill fields left unset after extension resolution; the chosen variant
then overrides the outer service. `healthcheck_timeout` (also accepted as
`healthcheck_start_period`) supplies a startup health wait without defining a
probe. An explicit `healthcheck.start_period` or legacy `timeout` wins over it.

`fed start --all` starts every service enabled by the active profiles.
`entrypoint: '*'` makes that the default for plain `fed start`.
`fed restart --all` explicitly requests the existing restart-everything behavior.
A service with only `depends_on` is a grouping node: it completes after its
dependencies start. Interactive startup still requires a single process service;
use `fed start -i <service>` with a wildcard or grouping entrypoint.

See [examples/whole-stack.yaml](../examples/whole-stack.yaml) for a runnable
example with defaults, variant templates, grouping, and optional profiles.

### Healthcheck timing

start_period is how long fed waits for the first pass. It is separate from the
time limit for an individual probe.

| Field | Meaning | Default |
| --- | --- | --- |
| `start_period` | Startup wait for the first successful probe | 5s |
| `interval` | Delay between probes | 500ms |
| `probe_timeout` | Time limit for one probe | The effective start period, preserving legacy behavior |
| `retries` | Consecutive failed probes before unhealthy | 3 |

These fields work alongside either `http_get` or `command`. A bare command
string uses the defaults. `timeout` remains the legacy alias for `start_period`;
setting both is an error. `interval` must be positive and `retries` at least 1.
`startup_timeout` still caps the entire start attempt; validation warns when
it is shorter than the health wait.

A probe timeout counts as one failed probe. Startup continues polling until the
start period expires; expiry while the process is alive produces a health
warning, not a failed start. A probe already in flight at the deadline may
finish. After startup, the supervisor (or watch/TUI monitor) runs the configured
probes, preserves health through fewer than `retries` failures, and records
unhealthy at the threshold. A success resets the streak and restores healthy
status. Process death bypasses probe retries. Healthchecks alone enable
supervision; restarting still requires a restart policy.

## Existing Docker Compose projects

You do not need to translate every container into fed. Include the Compose file once and fed expands its services and dependency edges into the same graph as native services:

```yaml
compose:
  - ./compose.yaml

services:
  api:
    process: npm run dev
    depends_on: [postgres]
```

Use a namespace when names could collide, and pass Fed parameters to Compose interpolation once for the whole project:

```yaml
parameters:
  DATABASE_PORT:
    type: port

compose:
  - file: ./compose.yaml
    namespace: infrastructure
    environment:
      DATABASE_PORT: "{{DATABASE_PORT}}"
```

That exposes names such as `infrastructure/postgres` and `infrastructure/redis`. The Compose file consumes values with its normal `${DATABASE_PORT}` syntax. Its own `.env`, `env_file`, and container `environment` behavior remain Compose-owned.

The older per-service `compose_file` + `compose_service` form is still accepted for compatibility but is deprecated; use a top-level import for new configurations.

Compose files with hardcoded host ports work as they are. One checkout starts exactly as it does today. You only need `${VAR}` substitution on a port when you want *two* checkouts of that service running at the same time, because fed exports the allocated value and Compose's own substitution picks it up. Leave a port hardcoded and the second checkout stops on the bind rather than quietly sharing the first one's container. So you can retrofit ports when you actually hit a collision.

[`examples/compose-siblings/`](../examples/compose-siblings/) is a runnable Postgres and Redis project exercised against a real Docker daemon by the integration suite.

## Container images, registries, and host networking

### Private registry images

When your `image:` services come from a private registry, the team can share one pull token through the vault instead of everyone running `docker login`:

```yaml
parameters:
  REGISTRY_TOKEN:
    type: secret
    source: manual
    optional: true
    description: Read-only token for pulling team images from ghcr.io
registry_auth:
  ghcr.io:
    username: acme-bot          # literal or a {{PARAM}} template
    password: '{{REGISTRY_TOKEN}}'
```

Each key is a registry host. An image without a host, such as `postgres:16`, comes from `docker.io`. The password must reference a `type: secret` parameter, so `fed validate` rejects a literal password.

When `REGISTRY_TOKEN` has a value, fed pulls that registry's images with a temporary Docker config that holds this one login. The config is private to your user and is deleted after the pull. fed never runs `docker login` and never writes to `~/.docker`. When the token has no value, fed pulls with your own Docker credentials, as it does without `registry_auth`. If the registry refuses the token, fed prints one warning and retries with your own credentials.

Before you add a token:

- **Use a read-only, pull-only bot token.** Everyone with access to the project can read it.
- **ECR passwords expire after 12 hours.** For ECR, keep using `aws ecr get-login-password | docker login`.

`registry_auth` covers `image:` services and the image pull before `fed start`. Compose services and the base images of Dockerfile `build:` services still pull with your own Docker credentials.

### Images for another architecture

When an image is published only for `linux/amd64`, the pull fails on an Apple Silicon Mac with `no matching manifest for linux/arm64/v8`. Set `platform:` on the service:

```yaml
services:
  presentation:
    image: example/presentation:latest
    platform: linux/amd64
```

fed passes `--platform` to the pull and to `docker run`. Docker Desktop runs the container under emulation (Rosetta), which is slower than a native build. A local copy of the image for another platform does not count, so fed pulls the one you asked for.

`platform:` is valid only next to `image:`. In a service with variants, put it in the variant that sets the image. It does not change Dockerfile `build:` services or Compose services.

### Reaching the host from a container

A container reaches services on your machine, native processes and other services' published ports, at `host.docker.internal`:

```yaml
services:
  worker:
    image: example/worker:latest
    environment:
      DATABASE_URL: 'postgresql://app:secret@host.docker.internal:5432/app'
```

Docker Desktop and Podman define that name in every container. Docker Engine on Linux does not, so on Linux fed starts each container with `--add-host host.docker.internal:host-gateway`, which needs Docker Engine 20.10 or later.

On Linux the traffic arrives on the Docker bridge (`docker0`), and a host firewall that denies incoming traffic drops it: the name resolves, but connections time out. With ufw, allow the bridge:

```bash
sudo ufw allow in on docker0
```
