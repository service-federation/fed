//! `fed link [org/project]` — bind this checkout to a Cloud project.

use crate::output::UserOutput;
use anyhow::{Result, bail};
use fed::cloud;
use fed::parameter::secret::is_gitignored;
use std::path::PathBuf;

pub async fn run_link(
    target: Option<String>,
    workdir: Option<PathBuf>,
    out: &dyn UserOutput,
) -> Result<()> {
    let work_dir = match workdir {
        Some(d) => d,
        None => std::env::current_dir()?,
    };

    let (org, project) = match target {
        Some(t) => match t.split_once('/') {
            Some((o, p)) if !o.is_empty() && !p.is_empty() => (o.to_string(), p.to_string()),
            _ => bail!("expected org/project, e.g. `fed link acme/web`"),
        },
        None => pick_interactively(out).await?,
    };

    let secret_cache = cloud::load_link(&work_dir)
        .map(|link| link.secret_cache)
        .unwrap_or_default();
    let link = cloud::CloudLink {
        org,
        project,
        secret_cache,
    };
    let path = cloud::save_link(&work_dir, &link)?;
    out.success(&format!(
        "Linked to {}/{} ({})",
        link.org,
        link.project,
        path.display()
    ));

    let (in_repo, ignored) = is_gitignored(&work_dir, ".fed/cloud.yaml");
    if in_repo && ignored {
        // fed's own .fed/.gitignore explicitly unignores cloud.yaml, so if it's
        // still ignored the root .gitignore must be ignoring .fed wholesale.
        out.warning(
            ".fed/cloud.yaml is gitignored — teammates won't inherit the link. Your root .gitignore likely ignores `.fed/` wholesale; remove that entry (fed's own .fed/.gitignore keeps everything except cloud.yaml out of git), then commit .fed/cloud.yaml.",
        );
    } else if in_repo {
        out.status("Commit .fed/cloud.yaml so teammates inherit the link — it's the only file in .fed/ that fed doesn't gitignore.");
    }
    out.status("Teammates: `fed login`, then `fed start` pulls the team's development secrets.");
    Ok(())
}

async fn pick_interactively(out: &dyn UserOutput) -> Result<(String, String)> {
    let Some(creds) = cloud::load_credentials() else {
        bail!("not signed in — run `fed login` first, or pass org/project explicitly");
    };
    let me = cloud::whoami(&creds).await?;
    if me.orgs.is_empty() {
        bail!("you're not in any org — create one at {}", creds.url);
    }
    let mut options: Vec<(String, String)> = Vec::new();
    for org in &me.orgs {
        for project in cloud::list_projects(&creds, &org.slug).await? {
            options.push((org.slug.clone(), project.slug));
        }
    }
    if options.is_empty() {
        bail!("no projects yet — create one at {}", creds.url);
    }
    out.status("Pick a project:");
    for (i, (org, project)) in options.iter().enumerate() {
        out.status(&format!("  {}) {}/{}", i + 1, org, project));
    }
    eprint!("Number: ");
    let mut input = String::new();
    std::io::stdin().read_line(&mut input)?;
    let idx: usize = input.trim().parse().map_err(|_| {
        anyhow::anyhow!("not a number — run `fed link org/project` to pass it explicitly")
    })?;
    if idx == 0 || idx > options.len() {
        bail!("out of range");
    }
    Ok(options[idx - 1].clone())
}
