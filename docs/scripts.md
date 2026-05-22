# Scripts

Scripts are commands that can depend on services or other scripts. They're defined in the `scripts` section of your config and run with `fed run <name>` or the shorthand `fed <name>`.

## Defining Scripts

```yaml
scripts:
  db:migrate:
    depends_on: [database]
    script: npx prisma db push

  test:integration:
    depends_on: [db:migrate, api]
    cwd: ./tests                      # Working directory (optional)
    timeout: "5m"                     # Timeout for non-interactive execution (optional)
    script: npm run test:e2e -- "$@"  # "$@" passes arguments from CLI
```

## Running Scripts

```bash
fed run db:migrate                    # Run a script
fed db:migrate                        # Shorthand (if no command collision)
fed test:integration -- -t "auth"     # Pass arguments after --
```

## Service Lifecycle ("borrow or own")

A script is a good guest: it stops the services *it* started, and leaves alone the
services that were already running.

- If a dependency was **not running**, the script starts it and **stops it again**
  when the script finishes — including transitive dependencies, and even if the
  script fails.
- If a dependency was **already running** (e.g. you ran `fed start` first), the script
  **borrows** it and leaves it running.

So `fed start` is how you keep a service up across many script runs:

```bash
fed start database          # database is now session-owned
fed test:integration        # borrows database, leaves it running
fed test:integration        # ...and again, no slow restart

fed run db:migrate          # nothing was running, so db is started…
                            # …and stopped again when the migration finishes
```

There is no config flag for this — the lifecycle is decided at runtime by *who started
the service*. When a script depends on another script, only the outermost run performs
cleanup, so nested script-dependencies never tear down services mid-run.

## Isolated Scripts

For integration tests that need a throwaway stack without interfering with your running dev services:

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

When `isolated: true` is set:

- **Fresh ports** are allocated, independent of your dev stack.
- **Docker containers and volumes** are scoped with a unique isolation ID — no collisions with your running services.
- **Cleanup happens automatically** when the script finishes — containers are stopped and removed.

This is the recommended way to run integration tests. Your dev stack is untouched, and each test run gets a clean environment.

## Environment Variables

Scripts inherit the resolved parameters from the config. You can also set script-specific environment variables:

```yaml
scripts:
  test:integration:
    depends_on: [database]
    environment:
      TEST_MODE: "true"
    script: npm run test:e2e
```

## Working Directory

Scripts run in the project root by default. Use `cwd` to change this:

```yaml
scripts:
  test:e2e:
    cwd: ./tests/e2e
    script: npx playwright test
```

## Timeout

Non-interactive scripts default to a 5-minute timeout. Override with `timeout`:

```yaml
scripts:
  long-migration:
    depends_on: [database]
    timeout: "30m"
    script: ./run-heavy-migration.sh
```

Supports duration strings: `"30s"`, `"5m"`, `"1h"`.

## Script Dependencies

Scripts can depend on both services and other scripts:

```yaml
scripts:
  db:migrate:
    depends_on: [database]
    script: npx prisma migrate deploy

  db:seed:
    depends_on: [db:migrate]
    script: npx prisma db seed

  test:e2e:
    depends_on: [db:seed, api]
    script: npm run test:e2e
```

Dependencies are started in order. Service dependencies are started and health-checked before the script runs.
