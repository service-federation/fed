//! `fed secrets ls|set` — the team vault from the terminal.

use crate::cli::SecretsCommands;
use crate::output::UserOutput;
use anyhow::{bail, Result};
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
            let (creds, link) = context(workdir)?;
            // Cloud-first and blocking: the user is deliberately asking the
            // cloud, so correctness wins — generous budget + waking hint, never
            // a cache fallback (D5).
            let secrets = cloud::with_waking_hint(cloud::list_secrets(&creds, &link, env)).await?;
            if secrets.is_empty() {
                out.status(&format!(
                    "No {} secrets in {}/{} yet — `fed secrets set NAME` adds one.",
                    env, link.org, link.project
                ));
                return Ok(());
            }
            out.status(&format!("{}/{} · {}", link.org, link.project, env));
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
        SecretsCommands::Set { name, env } => {
            let (creds, link) = context(workdir)?;
            // Value comes from stdin, never argv — argv leaks into shell history
            // and process lists.
            let value = if std::io::IsTerminal::is_terminal(&std::io::stdin()) {
                eprint!("Value for {} (input hidden is not supported; typed value is not echoed to logs): ", name);
                let mut input = String::new();
                std::io::stdin().read_line(&mut input)?;
                input.trim_end_matches(['\r', '\n']).to_string()
            } else {
                let mut input = String::new();
                std::io::Read::read_to_string(&mut std::io::stdin(), &mut input)?;
                input.trim_end_matches(['\r', '\n']).to_string()
            };
            if value.is_empty() {
                bail!("empty value");
            }
            cloud::with_waking_hint(cloud::put_secret(&creds, &link, env, name, &value)).await?;
            out.success(&format!(
                "{} set in {}/{} ({})",
                name, link.org, link.project, env
            ));
            Ok(())
        }
    }
}
