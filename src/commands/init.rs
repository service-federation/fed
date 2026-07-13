use crate::output::UserOutput;
use std::path::Path;

const TEMPLATE: &str = r#"# service-federation.yaml — your repository's runnable dev stack.
# `fed start` brings the whole thing up in dependency order, waiting on each healthcheck.
# This is a starting point: rename the services, swap the commands, delete what you don't use.
# Full reference: https://www.service-federation.com/docs/

parameters:
  # `type: port` grabs a free port — it prefers the default and falls back if it's taken,
  # so two checkouts of this repo never fight over the same port.
  API_PORT:
    type: port
    default: 8080
  FRONTEND_PORT:
    type: port
    default: 3000
  DB_PORT:
    type: port
    default: 5432

  DB_NAME:
    default: myapp_dev
  DB_USER:
    default: postgres

  # `type: secret` is generated on the first `fed start` and written to the file below.
  # No password in your config, no secret in git.
  DB_PASSWORD:
    type: secret

# Generated secrets land here. Add `.fed/` to .gitignore and they stay out of git.
generated_secrets_file: .fed/secrets.env

services:
  database:
    image: postgres:16-alpine
    ports: ["{{DB_PORT}}:5432"]
    environment:
      POSTGRES_DB: '{{DB_NAME}}'
      POSTGRES_USER: '{{DB_USER}}'
      POSTGRES_PASSWORD: '{{DB_PASSWORD}}'
    # Dependents wait for this to pass, so nothing starts against a database that isn't
    # accepting connections yet.
    healthcheck:
      command: 'pg_isready -U {{DB_USER}}'

  backend:
    process: npm start
    cwd: ./backend
    depends_on: [database]
    # `install:` runs once per checkout — the first `fed start` runs it, later starts skip
    # it. Re-run it with `fed install`; clear it with `fed clean`. Use it for the slow setup
    # that only changes when you say so.
    install: npm install
    # `migrate:` runs on every `fed start`, after the database is healthy and before the
    # backend starts — so the schema is always current. Keep it idempotent.
    migrate: npm run migrate
    environment:
      PORT: '{{API_PORT}}'
      DATABASE_URL: 'postgres://{{DB_USER}}:{{DB_PASSWORD}}@localhost:{{DB_PORT}}/{{DB_NAME}}'
    healthcheck:
      httpGet: 'http://localhost:{{API_PORT}}/health'

  frontend:
    process: npm run dev
    cwd: ./frontend
    depends_on: [backend]
    environment:
      PORT: '{{FRONTEND_PORT}}'
      BACKEND_URL: 'http://localhost:{{API_PORT}}'
    # Printed once the stack is up. In isolated mode it shows the port this checkout
    # actually got, so you always know where to open the app.
    startup_message: 'http://localhost:{{FRONTEND_PORT}}'

# The service `fed start` brings up by default (its dependencies come along).
entrypoint: frontend

# Scripts run a one-off task against this stack, with the same ports and secrets your
# services got. Run them with `fed <name>` — e.g. `fed test`.
scripts:
  test:
    depends_on: [backend]
    environment:
      API_URL: 'http://localhost:{{API_PORT}}'
    script: |
      npm test

# Profiles switch services on by context: tag one with `profiles: [production]`, then
# `fed start --profile production`. See https://www.service-federation.com/docs/configuration/
"#;

pub fn run_init(output: &Path, force: bool, out: &dyn UserOutput) -> anyhow::Result<()> {
    // Check if file exists and force flag not set
    if output.exists() && !force {
        out.error(&format!("Error: {} already exists", output.display()));
        out.error("Use --force to overwrite");
        return Err(anyhow::anyhow!("File already exists"));
    }

    std::fs::write(output, TEMPLATE)?;
    out.success(&format!("Created {}", output.display()));
    out.status("\nNext steps:");
    out.status(&format!(
        "  1. Edit {} to match your services",
        output.display()
    ));
    out.status("  2. Add .fed/ to .gitignore");
    out.status("  3. Run: fed start");

    Ok(())
}
