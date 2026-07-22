//! `fed secrets ls` — read the team vault from the terminal. Secrets are written
//! in the dashboard (session-authenticated); the CLI never writes.

use crate::cli::SecretsCommands;
use crate::output::UserOutput;
use anyhow::{Result, bail};
use fed::cloud;
use std::path::PathBuf;

fn context(workdir: Option<PathBuf>) -> Result<(cloud::Credentials, cloud::CloudLink)> {
    let work_dir = match workdir {
        Some(d) => d,
        None => std::env::current_dir()?,
    };
    let Some(creds) = cloud::load_credentials() else {
        bail!("not signed in — run `fed login`");
    };
    let Some(link) = cloud::load_link(&work_dir) else {
        bail!("this checkout isn't linked — run `fed link org/project`");
    };
    Ok((creds, link))
}

pub async fn run_secrets(
    cmd: &SecretsCommands,
    workdir: Option<PathBuf>,
    out: &dyn UserOutput,
) -> Result<()> {
    match cmd {
        SecretsCommands::Ls { env } => {
            // Removed in fed 8.0 — kept hidden and optional so a stale
            // invocation gets this explicit migration error instead of a
            // generic clap "unexpected argument" failure.
            if env.is_some() {
                bail!(
                    "--env was removed in fed 8.0 — the development/staging/production axis no longer exists. Move deployment-specific parameter values into an env_file instead (see env_file: in fed.yaml docs)."
                );
            }

            let (creds, link) = context(workdir)?;
            // Cloud-first and blocking: the user is deliberately asking the
            // cloud, so correctness wins — generous budget + waking hint, never
            // a cache fallback (D5).
            let secrets = cloud::with_waking_hint(cloud::list_secrets(&creds, &link)).await?;
            if secrets.is_empty() {
                out.status(&format!(
                    "No secrets in {}/{} yet — add one in the dashboard.",
                    link.org, link.project
                ));
                return Ok(());
            }
            out.status(&format!("{}/{}", link.org, link.project));
            for s in secrets {
                out.status(&format!(
                    "  {}  (updated {} by {})",
                    s.name,
                    &s.updated_at[..10.min(s.updated_at.len())],
                    s.updated_by
                ));
            }
            Ok(())
        }
    }
}
