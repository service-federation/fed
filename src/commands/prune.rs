//! `fed prune` — reap orphaned fed-managed Docker volumes.
//!
//! Isolation and per-run containers accumulate Docker volumes that nothing else reaps:
//! anonymous image volumes left by `docker rm` before the `-v` fix, retired isolation
//! stacks, and deleted worktrees. This command removes the fed-managed volumes that are
//! *dangling* — referenced by no container, running or stopped — so it can never delete a
//! live stack's data, nor a stopped stack's (a stopped container still references its named
//! volume, so that volume is not dangling and is left alone).
//!
//! Safety model: dry-run + confirm. In an interactive terminal it lists the volumes and
//! prompts. In a non-interactive context (agent, CI, pipe) it lists them and does nothing
//! unless `--force` is passed — so nothing is ever deleted unprompted.

use crate::output::UserOutput;
use fed::docker::DockerClient;
use std::io::{IsTerminal, Write};
use std::time::Duration;

pub async fn run_prune(force: bool, out: &dyn UserOutput) -> anyhow::Result<()> {
    let docker = DockerClient::new();

    if !docker.daemon_healthy(Duration::from_secs(5)).await {
        anyhow::bail!("Docker daemon is not running — nothing to prune.");
    }

    // Dangling (referenced by no container) AND prefixed `fed-`. See the helper for why the
    // prefix is enforced in code rather than left to Docker's substring `name=` filter.
    let orphans = docker.orphaned_fed_volumes().await?;

    if orphans.is_empty() {
        out.success("No orphaned fed volumes to prune.");
        return Ok(());
    }

    out.status(&format!(
        "{} orphaned fed volume(s) — referenced by no container:",
        orphans.len()
    ));
    for v in &orphans {
        out.status(&format!("  {v}"));
    }

    let proceed = if force {
        true
    } else if std::io::stdin().is_terminal() {
        eprint!("\nRemove these {} volume(s)? [y/N] ", orphans.len());
        std::io::stderr().flush().ok();
        let mut input = String::new();
        std::io::stdin().read_line(&mut input)?;
        matches!(input.trim().to_ascii_lowercase().as_str(), "y" | "yes")
    } else {
        out.status(&format!(
            "\nDry run (non-interactive): re-run with --force to remove these {} volume(s).",
            orphans.len()
        ));
        false
    };

    if !proceed {
        out.status("Nothing removed.");
        return Ok(());
    }

    let (mut removed, mut failed) = (0usize, 0usize);
    for v in &orphans {
        match docker.volume_rm(v).await {
            Ok(o) if o.status.success() => removed += 1,
            _ => failed += 1,
        }
    }

    if failed > 0 {
        out.warning(&format!("Removed {removed} volume(s); {failed} failed."));
    } else {
        out.success(&format!("Removed {removed} volume(s)."));
    }
    Ok(())
}
