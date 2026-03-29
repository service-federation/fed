# Configuration Reference

Service Federation is configured through a `service-federation.yaml` file in your project root. Run `fed init` to generate a starter config.

## Services

Every service has one type-defining field — `process`, `image`, `composeFile`+`composeService`, or `gradleTask`.

### Process

Run any command on the host:

```yaml
services:
  api:
    process: npm start
    cwd: ./api
```

### Docker Container

Run a Docker image:

```yaml
services:
  redis:
    image: redis:7-alpine
    ports: ["6379:6379"]
```

### Docker Compose Service

Reuse services from an existing `docker-compose.yml`:

```yaml
services:
  postgres:
    composeFile: ./docker-compose.yml
    composeService: postgres
```

### Gradle Task

Tasks sharing a `cwd` are batched into a single Gradle command:

```yaml
services:
  auth-service:
    gradleTask: ':auth:bootRun'
    cwd: ./backend
```

### Volumes

Docker services can mount named volumes and bind mounts:

```yaml
services:
  postgres:
    image: postgres:16
    volumes:
      - postgres_data:/var/lib/postgresql/data    # Named volume
      - ./init-scripts:/docker-entrypoint-initdb.d  # Bind mount
```

Named volumes with a `fed-` prefix are automatically cleaned up by `fed clean`.

### Tags

Flexible grouping for service selectors. Reference with `@tag`:

```yaml
services:
  api:
    process: npm start
    tags: [backend, critical]

  worker:
    process: npm run worker
    tags: [backend, async]
```

```bash
fed start @backend        # Start all services tagged "backend"
fed stop @async           # Stop all services tagged "async"
fed install @critical     # Install only critical services
```

### Watch

File paths for auto-restart when used with `fed start --watch`:

```yaml
services:
  api:
    process: npm start
    watch:
      - ./src/**/*.ts
      - ./package.json
```

### Restart Policy

```yaml
services:
  worker:
    process: npm run worker
    restart: always           # Always restart on failure

  api:
    process: npm start
    restart:
      on_failure:
        max_retries: 3        # Restart up to 3 times on failure
```

Values: `no` (default), `always`, `on_failure` (with optional `max_retries`).

### Grace Period

Graceful shutdown timeout before SIGKILL:

```yaml
services:
  api:
    process: npm start
    grace_period: "30s"       # Default: 10s
```

Accepts duration strings: `"10s"`, `"1m"`, `"500ms"`.

### Circuit Breaker

Crash loop detection. Requires `restart: always` or `on_failure`:

```yaml
services:
  api:
    process: npm start
    restart: always
    circuit_breaker:
      restart_threshold: 5    # Trips after 5 restarts... (default: 5)
      window_secs: 60         # ...within 60 seconds (default: 60)
      cooldown_secs: 300      # Wait 5 minutes before retrying (default: 300)
```

States: **closed** (normal, restarts allowed), **open** (tripped, restarts blocked), **half-open** (after cooldown, one retry allowed).

### Expose

Mark a service for external consumption:

```yaml
services:
  api:
    process: npm start
    expose: true
```

## Parameters

Values referenced throughout your config with `{{PARAM}}`:

```yaml
parameters:
  API_PORT:
    type: port
    default: 8080  # Tries 8080, falls back to random if occupied

  DB_PORT:
    type: port     # No default — always random available port

  API_KEY:
    default: "dev-key"  # String parameter
```

### Environment-specific values

```yaml
parameters:
  API_URL:
    default: "http://localhost:8080"
    development: "http://localhost:8080"     # Used with -e development (default)
    staging: "https://staging.example.com"   # Used with -e staging
    production: "https://api.example.com"    # Used with -e production
```

`develop` is accepted as an alias for `development`.

### Resolution priority

1. Explicit `value` field (set programmatically)
2. Environment variable from the shell
3. `env_file` entry
4. Environment-specific field (`development`, `staging`, `production`)
5. `default`

### Validation with `either`

```yaml
parameters:
  LOG_LEVEL:
    default: "info"
    either: [debug, info, warn, error]    # Validated at parse time
```

### `variables` vs `parameters`

Both keys are accepted at the top level. `variables` takes precedence if both are present. `parameters` is the original key and remains supported.

### Secrets

Parameters with `type: secret` are auto-generated on first `fed start` and stored in a file you gitignore:

```yaml
generated_secrets_file: .env.secrets

parameters:
  DB_PASSWORD:
    type: secret
  SESSION_KEY:
    type: secret

services:
  database:
    image: postgres:15
    environment:
      POSTGRES_PASSWORD: '{{DB_PASSWORD}}'
```

On `fed start`, if `.env.secrets` doesn't contain a value for `DB_PASSWORD` or `SESSION_KEY`, fed generates 32-character random alphanumeric strings and writes them to `.env.secrets`. Values are never overwritten once generated — they're stable across restarts.

The `generated_secrets_file` **must be in `.gitignore`**. Fed checks this and refuses to generate if it isn't:

```
# .gitignore
.env.secrets
```

In a terminal, fed asks for confirmation before generating. In CI or non-interactive contexts, it generates silently.

#### Manual secrets

For secrets you provide yourself (API keys, OAuth credentials), add `source: manual`:

```yaml
parameters:
  GITHUB_CLIENT_SECRET:
    type: secret
    source: manual
    description: "GitHub OAuth client secret"

  STRIPE_SECRET_KEY:
    type: secret
    source: manual
    optional: true
    description: "From https://dashboard.stripe.com/apikeys"
```

Fed will not generate a value for manual secrets. Instead, it fails at startup with a message listing what's missing and where to add it (your `env_file` entries). The `description` field is shown in this error message.

Add `optional: true` for secrets that not every team member needs (e.g., a Stripe key only some developers have). Optional manual secrets resolve to an empty string when not provided, instead of failing at startup.

Manual secrets don't require `generated_secrets_file` — you can use them standalone when you only need to enforce that certain values are provided.

#### Constraints

Secret parameters cannot have `default`, environment-specific values (`development`, `staging`, `production`), or `either` constraints. Secrets are provided externally, not baked into config.

#### Resolution priority

`generated_secrets_file` is prepended to the `env_file` list at runtime, giving it the lowest priority. This means your `.env` or `.env.local` files can override generated values when needed.

### Custom Generation

Use `generate` to run a shell command that produces the parameter value:

```yaml
parameters:
  # Generate an Ed25519 keypair for JWT signing.
  JWT_PRIVATE_KEY:
    type: secret
    generate: openssl genpkey -algorithm ed25519 -outform PEM 2>/dev/null | base64 -w0

  JWT_PUBLIC_KEY:
    type: secret
    generate: echo '{{JWT_PRIVATE_KEY}}' | base64 -d | openssl pkey -pubout -outform PEM 2>/dev/null | base64 -w0

  # Computed parameter (not a secret — recomputed every start).
  DB_URL:
    generate: echo "postgres://app:{{DB_PASSWORD}}@localhost:{{DB_PORT}}/mydb"
```

The command runs via `sh -c`. Stdout is captured as the value (trimmed). Stderr is discarded on success, shown on failure.

**`{{PARAM}}` references** create dependencies. Fed resolves parameters in topological order (DAG). Circular dependencies are detected and rejected.

**Invalidation:** When a secret with `generate` is missing and gets generated, all secrets that depend on it (via `{{PARAM}}` references) are regenerated — even if they already have values. This ensures derived secrets stay in sync with their sources.

**Persistence rules:**
- `type: secret` with `generate`: generated once, persisted to secrets file. Not regenerated unless an input dependency changes.
- `generate` without `type: secret`: recomputed on every `fed start`. Not persisted.
- `type: secret` without `generate`: random 32-char alphanumeric (default behavior).

## Dependencies & Health Checks

Services declare dependencies with `depends_on`. A service waits for its dependencies to become healthy before starting.

### Simple form

```yaml
services:
  api:
    process: npm start
    depends_on: [database, cache]
```

### Structured form

Control behavior when a dependency fails:

```yaml
services:
  api:
    process: npm start
    depends_on:
      - database                           # Simple: stop if database fails
      - service: cache
        on_failure: ignore                 # Keep running if cache fails
      - service: worker
        on_failure: restart                # Restart if worker fails
```

`on_failure` values: `stop` (default), `restart`, `ignore`.

### Health check types

```yaml
services:
  database:
    image: postgres:15
    healthcheck:
      command: pg_isready -U postgres  # Runs INSIDE the container
      timeout: 10s

  api:
    process: npm start
    healthcheck:
      httpGet: 'http://localhost:{{API_PORT}}/health'
      timeout: 5s  # Optional, default 5s
```

- `httpGet` — HTTP request from the host.
- `command` — For Docker services, runs inside the container via `docker exec`. For process services, runs on the host.

Simple string form (uses default 5s timeout):

```yaml
healthcheck: "curl -f http://localhost:8080/health"
```

## Environment Files

`.env` files set parameter values (not service environment directly):

```yaml
parameters:
  API_KEY:
    default: ""

env_file:
  - .env         # API_KEY=secret123
  - .env.local   # Later files override earlier
```

All `.env` variables must be declared as parameters.

See [`examples/env-file/`](../examples/env-file).

## Startup Messages

Show where to access services after startup:

```yaml
services:
  api:
    startup_message: "API docs: http://localhost:{{API_PORT}}/docs"
  frontend:
    startup_message: "App: http://localhost:{{NEXT_PORT}}"
```

```
╭──────────────────────────────────────────────────╮
│ API docs: http://localhost:8081/docs             │
├──────────────────────────────────────────────────┤
│ App: http://localhost:3000                       │
╰──────────────────────────────────────────────────╯
```

Entrypoint services sort last. A warning is emitted if an entrypoint has no `startup_message` — particularly useful when ports are randomized.

## Templates

Reusable base configurations:

```yaml
templates:
  java-service:
    image: openjdk:17-slim
    environment:
      JAVA_OPTS: '-Xmx512m'
    healthcheck:
      httpGet: 'http://localhost:{{PORT}}/actuator/health'

services:
  auth-service:
    extends: java-service
    ports: ["8080:8080"]
```

See [`examples/templates-example.yaml`](../examples/templates-example.yaml).

## Profiles

Conditionally include services:

```yaml
services:
  api:
    process: npm start       # No profiles = always included

  worker:
    profiles: [worker]       # Only with -p worker

  debug-tools:
    profiles: [debug]        # Only with -p debug
```

```bash
fed start                    # Starts profileless services only
fed start -p worker          # Starts api + worker
fed start -p worker -p debug # Starts api + worker + debug-tools
```

## Packages

Import service configurations across projects:

```yaml
packages:
  - source: "github:org/repo@v1.0"
    as: "infra"

services:
  database:
    extends: "infra.postgres"
    environment:
      POSTGRES_DB: "myapp"
```

Packages are cached locally. `fed package refresh` to re-fetch, `fed --offline start` to skip git.

See [`examples/service-merging/`](../examples/service-merging).

## Lifecycle Hooks

```yaml
services:
  backend:
    process: npm start
    cwd: ./backend
    install: npm ci                       # Before first start (offline prep)
    migrate: npx prisma migrate deploy    # After deps healthy, before start
    build: npm run build                  # Runs with `fed build`
    clean: rm -rf node_modules dist       # Runs with `fed clean`
```

**`install`** runs once before the service starts (skipped on subsequent `fed start` until `fed clean` or `fed install`). Use it for dependency installation (e.g., `npm ci`). Re-run with `fed install`.

**`migrate`** runs once after the service's dependencies are healthy but before the service itself starts (skipped on subsequent `fed start` until `fed clean`). Use it for database migrations, schema setup, or anything that needs a running dependency. The service's full resolved environment is available. Dependents wait for `migrate` to complete before they start.

**`build`** runs with `fed build`. **`clean`** runs with `fed clean`.

`fed clean` also removes Docker volumes with `fed-` prefix and clears both install and migrate state.

## Resource Limits

```yaml
services:
  api:
    process: npm start
    resources:
      memory: "512m"              # Hard memory limit
      memory_reservation: "256m"  # Soft limit (Docker only)
      cpus: "0.5"                 # CPU limit (0.5 = 50% of one core)
      cpu_shares: 512             # Relative CPU weight (default: 1024)
      pids: 100                   # Max processes/threads
      nofile: 65536               # Max open file descriptors
      strict_limits: false        # Fail startup if limits can't be set (default: false)
```

For Docker services, these map to `docker run` flags (`--memory`, `--cpus`, etc.). For process services, they map to system resource limits (rlimit/cgroups).

`strict_limits` controls whether failing to set a limit is fatal. Default `false` logs a warning and continues — useful on platforms where `setrlimit` is restricted (Docker containers, macOS with SIP).

See [`examples/resource-limits-example.yaml`](../examples/resource-limits-example.yaml).

## Docker Image Builds

```yaml
services:
  web:
    cwd: ./apps/web
    build:
      image: my-app
      # dockerfile: Dockerfile  (default)
      # args:                    (optional build arguments)
      #   NODE_ENV: production
```

`fed build` builds all services with a `build` field. `fed docker build` builds only Docker images. Images are tagged with the git short hash by default.

```bash
fed build                          # Build all (shell + Docker)
fed build --tag v1.0.0             # Custom tag
fed build --build-arg KEY=VALUE    # Extra build args
fed docker build                   # Build Docker images only
fed docker build --tag v1.0.0      # Custom tag
fed docker build --json            # Machine-readable output
fed docker push                    # Push images to registry
fed docker push --tag v1.0.0       # Push specific tag
```

## Entrypoint

Declare the main service(s) — affects startup order display and `startup_message` sorting:

```yaml
entrypoint: backend           # Single entrypoint

# Or multiple:
entrypoints: [frontend, backend]
```

Cannot specify both `entrypoint` and `entrypoints`.

## Scripts

Custom commands with dependencies and environment:

```yaml
scripts:
  test:
    script: npm test
    cwd: ./api
    depends_on: [database]
    environment:
      NODE_ENV: test
    timeout: "5m"             # Default: 5 minutes

  integration:
    script: npm run test:integration
    depends_on: [database, redis]
    isolated: true            # Fresh ports, scoped volumes, full cleanup
```

```bash
fed run test                  # Start deps, run script, stop deps
fed run integration           # Runs in complete isolation
```

`isolated: true` allocates fresh random ports, scopes Docker volumes, and cleans up after completion.

See also [Isolation](./isolation.md) for how directory scoping works.
