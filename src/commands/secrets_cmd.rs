//! `fed secrets ls|set|rm` — the linked project's team vault from the terminal.
//! Values are read from stdin or a hidden prompt, never from arguments, and are
//! never printed.

use crate::cli::SecretsCommands;
use crate::output::UserOutput;
use anyhow::{Result, bail};
use fed::cloud;
use std::io::{IsTerminal, Read};
use std::path::{Path, PathBuf};

fn work_dir(workdir: Option<PathBuf>) -> Result<PathBuf> {
    Ok(match workdir {
        Some(d) => d,
        None => std::env::current_dir()?,
    })
}

fn context(work_dir: &Path) -> Result<(cloud::Credentials, cloud::CloudLink)> {
    let Some(creds) = cloud::load_credentials() else {
        bail!("not signed in — run `fed login`");
    };
    let Some(link) = cloud::load_link(work_dir) else {
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

            let (creds, link) = context(&work_dir(workdir)?)?;
            // Cloud-first and blocking: the user is deliberately asking the
            // cloud, so correctness wins — generous budget + progress hint,
            // never a cache fallback (D5).
            let secrets = cloud::with_slow_fetch_hint(cloud::list_secrets(&creds, &link)).await?;
            if secrets.is_empty() {
                out.status(&format!(
                    "No secrets in {}/{} yet — `fed secrets set NAME` adds one.",
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
        SecretsCommands::Set { name } => {
            let work_dir = work_dir(workdir)?;
            let (creds, link) = context(&work_dir)?;
            // Checked before the prompt, so a typo does not cost a typed value.
            if !cloud::valid_secret_name(name) {
                bail!(cloud::INVALID_SECRET_NAME);
            }
            let value = read_value(name)?;
            cloud::put_secret(&creds, &link, name, &value).await?;
            forget_cached(&work_dir, name, out);
            out.success(&format!("Set {} in {}/{}", name, link.org, link.project));
            Ok(())
        }
        SecretsCommands::Rm { name } => {
            let work_dir = work_dir(workdir)?;
            let (creds, link) = context(&work_dir)?;
            if !cloud::valid_secret_name(name) {
                bail!(cloud::INVALID_SECRET_NAME);
            }
            cloud::delete_secret(&creds, &link, name).await?;
            forget_cached(&work_dir, name, out);
            out.success(&format!(
                "Removed {} from {}/{}",
                name, link.org, link.project
            ));
            Ok(())
        }
    }
}

/// Drop the name from this checkout's vault cache, so the next `fed start`
/// fetches the new value instead of reusing the cached one. The vault write
/// already succeeded, so a failure here is a warning.
fn forget_cached(work_dir: &Path, name: &str, out: &dyn UserOutput) {
    let path = fed::fed_dir::secrets_cache_path(work_dir);
    if let Err(e) = fed::parameter::secret::forget_cached_secret(&path, name) {
        out.warning(&format!(
            "could not drop {name} from {}: {e}. Delete that file so the next run fetches the new value.",
            path.display()
        ));
    }
}

/// In a terminal, a hidden prompt. Otherwise all of stdin, minus one trailing
/// newline, so `printf '%s' "$V" | fed secrets set NAME` and `echo "$V" | ...`
/// both send `$V`.
fn read_value(name: &str) -> Result<String> {
    let value = if std::io::stdin().is_terminal() {
        rpassword::prompt_password(format!("Value for {name} (hidden): "))
            .map_err(|e| anyhow::anyhow!("reading the secret value: {e}"))?
    } else {
        let mut input = String::new();
        std::io::stdin()
            .read_to_string(&mut input)
            .map_err(|e| anyhow::anyhow!("reading the secret value from stdin: {e}"))?;
        strip_one_newline(input)
    };
    if value.is_empty() {
        bail!("the value for {name} is empty — nothing was set");
    }
    Ok(value)
}

fn strip_one_newline(mut value: String) -> String {
    if value.ends_with('\n') {
        value.pop();
        if value.ends_with('\r') {
            value.pop();
        }
    }
    value
}

#[cfg(test)]
mod tests {
    use super::strip_one_newline;

    #[test]
    fn strips_exactly_one_trailing_newline() {
        assert_eq!(strip_one_newline("v\n".into()), "v");
        assert_eq!(strip_one_newline("v\r\n".into()), "v");
        assert_eq!(strip_one_newline("v\n\n".into()), "v\n");
        assert_eq!(strip_one_newline("v".into()), "v");
        assert_eq!(strip_one_newline("line1\nline2\n".into()), "line1\nline2");
        assert_eq!(strip_one_newline("\n".into()), "");
    }
}
