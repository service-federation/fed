//! `fed prune` — remove the Docker stacks of checkouts that are gone.
//!
//! Isolated checkouts and `isolated: true` script runs leave containers and
//! volumes behind that nothing else reaps: a checkout deleted without `fed stop`
//! (agents remove git worktrees), a script run that was killed. What is *not*
//! abandoned is just as common: `fed stop` removes a stack's containers but keeps
//! its named volumes on purpose, so a stopped stack in a checkout that still
//! exists looks, to Docker, exactly like an orphan. The stack registry
//! ([`fed::stack_registry`]) is what tells them apart, so prune removes a stack
//! only when it can prove one of:
//!
//! * its checkout directory no longer exists,
//! * its checkout moved on to another isolation id (`fed isolate rotate`/`disable`),
//! * it was the stack of an `isolated: true` script run whose process is dead.
//!
//! Stacks created by a fed that predates the registry have no owner on record
//! and are left alone. `--adopt <dir>` registers every checkout under a
//! directory, and `--unowned` (which requires `--adopt`) then also removes the
//! stacks none of those checkouts claims: the user, not fed, vouches that the
//! adopted directories hold all their checkouts.
//!
//! Safety model: dry-run + confirm. In an interactive terminal it lists what it
//! would remove, grouped by reason, and prompts. In a non-interactive context
//! (agent, CI, pipe) it lists and does nothing unless `--force` is passed.

use crate::output::UserOutput;
use chrono::{DateTime, Utc};
use fed::docker::DockerClient;
use fed::stack_cleanup::{self, Decision, Inventory, Keep, Owner, Plan, Remove, Verdict};
use fed::stack_registry::{CheckoutState, StackRegistry};
use std::collections::BTreeMap;
use std::io::{IsTerminal, Write};
use std::path::PathBuf;
use std::time::Duration;

/// How deep `--adopt` looks for checkouts below each directory it is given.
const ADOPT_DEPTH: usize = 6;

pub struct PruneOptions {
    pub force: bool,
    pub adopt: Vec<PathBuf>,
    pub unowned: bool,
}

pub async fn run_prune(opts: PruneOptions, out: &dyn UserOutput) -> anyhow::Result<()> {
    let docker = DockerClient::new();

    if !docker.daemon_healthy(Duration::from_secs(5)).await {
        anyhow::bail!("Docker daemon is not running — nothing to prune.");
    }

    let registry = StackRegistry::default_location();

    for root in &opts.adopt {
        let Some(registry) = &registry else {
            anyhow::bail!("--adopt needs a home directory to keep its registry in.");
        };
        if !root.is_dir() {
            anyhow::bail!("--adopt {}: not a directory.", root.display());
        }
        let adopted = registry.adopt(root, ADOPT_DEPTH);
        out.status(&format!(
            "Adopted {} checkout(s) under {}.",
            adopted.len(),
            root.display()
        ));
    }

    let inventory = Inventory::load(&docker).await?;
    let total = inventory.groups.len();
    let plan = stack_cleanup::plan(
        inventory,
        &|scope| match &registry {
            Some(registry) => stack_cleanup::owner_of(registry, scope),
            None => Owner::Unknown,
        },
        opts.unowned,
    );

    print_plan(&plan, total, opts.unowned, !opts.adopt.is_empty(), out);

    if plan.remove.is_empty() {
        out.success("Nothing to remove.");
        return Ok(());
    }

    let (containers, volumes) = (plan.container_count(), plan.volume_count());
    let proceed = if opts.force {
        true
    } else if std::io::stdin().is_terminal() {
        eprint!(
            "\nRemove these {} stack(s) ({containers} container(s), {volumes} volume(s))? [y/N] ",
            plan.remove.len()
        );
        std::io::stderr().flush().ok();
        let mut input = String::new();
        std::io::stdin().read_line(&mut input)?;
        matches!(input.trim().to_ascii_lowercase().as_str(), "y" | "yes")
    } else {
        out.status(&format!(
            "\nDry run (non-interactive): re-run with --force to remove these {} stack(s) \
             ({containers} container(s), {volumes} volume(s)).",
            plan.remove.len()
        ));
        false
    };

    if !proceed {
        out.status("Nothing removed.");
        return Ok(());
    }

    let outcome = stack_cleanup::execute(&docker, registry.as_ref(), &plan.remove).await;
    if let Some(registry) = &registry {
        let present = plan
            .keep
            .iter()
            .chain(&plan.remove)
            .map(|d| d.group.scope.clone())
            .collect();
        stack_cleanup::forget_stale_entries(registry, &present);
    }
    for failure in &outcome.failures {
        out.warning(&format!("  could not remove {failure}"));
    }
    let summary = format!(
        "Removed {} container(s) and {} volume(s) from {} stack(s).",
        outcome.containers_removed, outcome.volumes_removed, outcome.scopes_cleared
    );
    if outcome.failures.is_empty() {
        out.success(&summary);
    } else {
        out.warning(&format!(
            "{summary} {} item(s) failed; run `fed prune` again.",
            outcome.failures.len()
        ));
    }
    Ok(())
}

fn print_plan(
    plan: &Plan,
    total: usize,
    unowned_requested: bool,
    adopted: bool,
    out: &dyn UserOutput,
) {
    let now = Utc::now();

    let mut by_reason: BTreeMap<Remove, Vec<&Decision>> = BTreeMap::new();
    for d in &plan.remove {
        if let Verdict::Remove(why) = &d.verdict {
            by_reason.entry(*why).or_default().push(d);
        }
    }
    for (why, decisions) in &by_reason {
        let containers: usize = decisions.iter().map(|d| d.group.containers.len()).sum();
        let volumes: usize = decisions.iter().map(|d| d.group.volumes.len()).sum();
        out.status(&format!(
            "\nWill remove — {}: {} stack(s), {containers} container(s), {volumes} volume(s)",
            why.heading(),
            decisions.len()
        ));
        for d in decisions {
            out.status(&format!("  {}", describe(d, now)));
        }
    }

    // What is staying, and why: the part a person reads to trust the list above.
    let mut kept: BTreeMap<String, Vec<&Decision>> = BTreeMap::new();
    for d in &plan.keep {
        if let Verdict::Keep(why) = &d.verdict {
            kept.entry(why.heading().to_string()).or_default().push(d);
        }
    }
    if !kept.is_empty() {
        out.status("\nKeeping:");
    }
    for (heading, decisions) in &kept {
        let containers: usize = decisions.iter().map(|d| d.group.containers.len()).sum();
        let volumes: usize = decisions.iter().map(|d| d.group.volumes.len()).sum();
        out.status(&format!(
            "  {} stack(s), {containers} container(s), {volumes} volume(s) — {heading}",
            decisions.len()
        ));
        // Reasons that need a person's attention are listed; the bulk is not.
        if matches!(
            decisions[0].verdict,
            Verdict::Keep(Keep::Unverifiable(_)) | Verdict::Keep(Keep::SupersededButRunning)
        ) {
            for d in decisions {
                out.status(&format!("      {}", describe(d, now)));
            }
        }
    }

    let unknown = plan
        .keep
        .iter()
        .filter(|d| matches!(d.verdict, Verdict::Keep(Keep::UnknownOwner)))
        .count();
    if unknown > 0 {
        out.status(&format!(
            "\n{unknown} stack(s) have no owner on record (made by a fed older than 8.4 or never started \
             since). They are never removed on a guess."
        ));
        if !adopted {
            out.status(
                "  To clean them up, name the directories that hold all your checkouts:\n    \
                 fed prune --adopt <dir> [--adopt <dir>...]   (registers the checkouts it finds there)\n    \
                 fed prune --adopt <dir> --unowned            (also removes stacks none of them uses)",
            );
        } else if !unowned_requested {
            out.status(
                "  None of the adopted checkouts uses them. To remove them, add --unowned \
                 (you are vouching that the --adopt directories hold every checkout you still use).",
            );
        }
    }
    if plan.unscoped_containers + plan.unscoped_volumes > 0 {
        out.status(&format!(
            "{} container(s) and {} volume(s) are labeled as fed's but not named after a stack; left alone.",
            plan.unscoped_containers, plan.unscoped_volumes
        ));
    }
    out.status(&format!("\n{total} stack(s) in Docker in total."));
}

fn describe(d: &Decision, now: DateTime<Utc>) -> String {
    let running = d.group.containers.iter().filter(|c| c.is_running()).count();
    let mut resources = format!(
        "{} container(s){}, {} volume(s)",
        d.group.containers.len(),
        if running > 0 {
            format!(" ({running} running)")
        } else {
            String::new()
        },
        d.group.volumes.len()
    );
    if let Some(newest) = d.group.newest() {
        resources.push_str(&format!(", newest {}", ago(now, newest)));
    }
    let owner = match &d.owner {
        Owner::Checkout { workdir, state } => match state {
            CheckoutState::Unverifiable(why) => format!("{} ({why})", workdir.display()),
            CheckoutState::Superseded { current } => format!(
                "{} (now {})",
                workdir.display(),
                current.as_deref().unwrap_or("not isolated")
            ),
            _ => workdir.display().to_string(),
        },
        Owner::Script { workdir, .. } => format!("script run from {}", workdir.display()),
        Owner::Unknown => "owner unknown".to_string(),
    };
    format!("{:<13} {owner}: {resources}", d.group.scope)
}

fn ago(now: DateTime<Utc>, then: DateTime<Utc>) -> String {
    let secs = (now - then).num_seconds().max(0);
    match secs {
        0..=89 => "just now".to_string(),
        90..=5399 => format!("{} min ago", secs / 60),
        5400..=172_799 => format!("{} h ago", secs / 3600),
        _ => format!("{} days ago", secs / 86_400),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ages_read_naturally() {
        let now = Utc::now();
        let at = |secs: i64| now - chrono::Duration::seconds(secs);
        assert_eq!(ago(now, at(10)), "just now");
        assert_eq!(ago(now, at(600)), "10 min ago");
        assert_eq!(ago(now, at(3 * 3600)), "3 h ago");
        assert_eq!(ago(now, at(5 * 86_400)), "5 days ago");
        assert_eq!(ago(now, now + chrono::Duration::seconds(30)), "just now");
    }
}
