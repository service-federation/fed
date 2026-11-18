//! Deciding which fed-made Docker stacks are abandoned, and removing them.
//!
//! `fed prune` and the leak sweep share this. Resources are grouped by scope
//! (see [`crate::stack_registry`]), each scope's owner is looked up, and every
//! scope gets one verdict. [`plan`] is pure: the owner lookup is a parameter,
//! so every rule below is unit-tested without Docker or a filesystem.
//!
//! A scope is removed only when something *proves* it abandoned:
//!
//! | owner | verdict |
//! |---|---|
//! | script run, lock free | remove (the process died without cleaning up) |
//! | script run, lock held | keep |
//! | checkout, directory gone | remove |
//! | checkout, isolation id replaced, nothing running | remove |
//! | checkout, current | keep (stopped stacks keep their database volume) |
//! | checkout, cannot be verified | keep |
//! | no registry entry | keep, unless `include_unowned` |
//!
//! Unowned removal is only offered together with `--adopt`, where the user
//! names the directories holding their checkouts: a scope no adopted checkout
//! claims is then the user's statement, not fed's guess. Even then it only takes
//! stacks whose newest resource was created longer ago than a minimum age, and
//! never one with something running. That age is creation, not last use (Docker
//! records nothing else): containers are recreated on every start, but a
//! stopped stack is only its volumes, dated when they were first made.

use crate::docker::DockerClient;
use crate::docker::client::{FedContainer, FedVolume};
use crate::stack_registry::{
    CheckoutState, StackEntry, StackKind, StackRegistry, checkout_state, scope_of_docker_name,
};
use chrono::{DateTime, Utc};
use std::collections::BTreeMap;
use std::path::PathBuf;

/// Everything fed made for one scope.
#[derive(Debug, Clone, Default)]
pub struct Group {
    pub scope: String,
    pub containers: Vec<FedContainer>,
    pub volumes: Vec<FedVolume>,
}

impl Group {
    pub fn any_running(&self) -> bool {
        self.containers.iter().any(FedContainer::is_running)
    }

    /// When the newest of its resources was created.
    pub fn newest(&self) -> Option<DateTime<Utc>> {
        self.containers
            .iter()
            .filter_map(|c| c.created)
            .chain(self.volumes.iter().filter_map(|v| v.created))
            .max()
    }
}

/// Docker resources grouped by scope, plus the ones whose name names no scope.
#[derive(Debug, Default)]
pub struct Inventory {
    pub groups: BTreeMap<String, Group>,
    /// Labeled as fed's but not named `fed-<scope>-...`: never touched.
    pub unscoped_containers: usize,
    pub unscoped_volumes: usize,
}

impl Inventory {
    pub fn build(containers: Vec<FedContainer>, volumes: Vec<FedVolume>) -> Self {
        let mut inv = Inventory::default();
        for c in containers {
            match scope_of_docker_name(&c.name) {
                Some(scope) => inv.group(&scope).containers.push(c),
                None => inv.unscoped_containers += 1,
            }
        }
        for v in volumes {
            match scope_of_docker_name(&v.name) {
                Some(scope) => inv.group(&scope).volumes.push(v),
                None => inv.unscoped_volumes += 1,
            }
        }
        inv
    }

    fn group(&mut self, scope: &str) -> &mut Group {
        self.groups
            .entry(scope.to_string())
            .or_insert_with(|| Group {
                scope: scope.to_string(),
                ..Default::default()
            })
    }

    pub async fn load(docker: &DockerClient) -> Result<Self, crate::docker::DockerError> {
        Ok(Self::build(
            docker.list_fed_containers().await?,
            docker.list_fed_volumes().await?,
        ))
    }
}

/// Who a scope belongs to, as far as fed can tell.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Owner {
    Checkout {
        workdir: PathBuf,
        state: CheckoutState,
    },
    Script {
        workdir: PathBuf,
        alive: bool,
    },
    /// No registry entry: created by a fed that predates the registry, or in a
    /// checkout this version has not run in yet.
    Unknown,
}

/// Look `scope` up in the registry and the filesystem.
pub fn owner_of(registry: &StackRegistry, scope: &str) -> Owner {
    match registry.get(scope) {
        None => Owner::Unknown,
        Some(StackEntry {
            kind: StackKind::Script,
            workdir,
            scope,
            ..
        }) => Owner::Script {
            workdir,
            alive: registry.script_run_alive(&scope),
        },
        Some(entry) => Owner::Checkout {
            state: checkout_state(&entry),
            workdir: entry.workdir,
        },
    }
}

/// Why a scope is removed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Remove {
    /// An `isolated: true` script run whose process is gone.
    ScriptRunDead,
    /// The checkout directory no longer exists.
    CheckoutGone,
    /// The checkout exists but moved to another isolation id.
    Superseded,
    /// No registered owner, and the user vouched for the directories searched.
    Unowned,
}

impl Remove {
    pub fn heading(self) -> &'static str {
        match self {
            Remove::ScriptRunDead => "isolated script run whose process is dead",
            Remove::CheckoutGone => "checkout directory deleted",
            Remove::Superseded => "isolation id replaced (isolate rotate/disable)",
            Remove::Unowned => "no checkout among the --adopt directories uses it",
        }
    }
}

/// Why a scope is left alone.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Keep {
    /// A script run is in progress.
    ScriptRunActive,
    /// The checkout exists and its stack is up.
    Running,
    /// The checkout exists and its stack is stopped: the data is what a person
    /// expects to find when they start it again.
    Stopped,
    /// The id was replaced, but something is still running under it.
    SupersededButRunning,
    /// A verdict was not possible. The reason says why.
    Unverifiable(String),
    /// Nobody registered it.
    UnknownOwner,
}

impl Keep {
    pub fn heading(&self) -> &'static str {
        match self {
            Keep::ScriptRunActive => "isolated script run in progress",
            Keep::Running => "checkout exists, stack running",
            Keep::Stopped => "checkout exists, stack stopped (data kept for the next start)",
            Keep::SupersededButRunning => "isolation id replaced but still running: fed stop it",
            Keep::Unverifiable(_) => "owner could not be verified",
            Keep::UnknownOwner => {
                "owner unknown: made by a fed without the registry, or never started since"
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Verdict {
    Remove(Remove),
    Keep(Keep),
}

#[derive(Debug, Clone)]
pub struct Decision {
    pub group: Group,
    pub owner: Owner,
    pub verdict: Verdict,
}

impl Decision {
    pub fn workdir(&self) -> Option<&PathBuf> {
        match &self.owner {
            Owner::Checkout { workdir, .. } | Owner::Script { workdir, .. } => Some(workdir),
            Owner::Unknown => None,
        }
    }
}

#[derive(Debug, Default)]
pub struct Plan {
    pub remove: Vec<Decision>,
    pub keep: Vec<Decision>,
    pub unscoped_containers: usize,
    pub unscoped_volumes: usize,
}

impl Plan {
    pub fn container_count(&self) -> usize {
        self.remove.iter().map(|d| d.group.containers.len()).sum()
    }
    pub fn volume_count(&self) -> usize {
        self.remove.iter().map(|d| d.group.volumes.len()).sum()
    }
}

/// Whether, and how aggressively, stacks with no owner on record are removed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UnownedPolicy {
    /// Only stacks whose newest container or volume is at least this old.
    pub min_age: chrono::Duration,
    pub now: DateTime<Utc>,
}

/// The verdict for one scope.
pub fn decide(group: &Group, owner: &Owner, unowned: Option<UnownedPolicy>) -> Verdict {
    match owner {
        Owner::Script { alive: true, .. } => Verdict::Keep(Keep::ScriptRunActive),
        Owner::Script { alive: false, .. } => Verdict::Remove(Remove::ScriptRunDead),
        Owner::Checkout { state, .. } => match state {
            CheckoutState::Current if group.any_running() => Verdict::Keep(Keep::Running),
            CheckoutState::Current => Verdict::Keep(Keep::Stopped),
            // A checkout whose directory is gone cannot be stopped by anyone;
            // what still runs for it is the leak.
            CheckoutState::Gone => Verdict::Remove(Remove::CheckoutGone),
            CheckoutState::Superseded { .. } if group.any_running() => {
                Verdict::Keep(Keep::SupersededButRunning)
            }
            CheckoutState::Superseded { .. } => Verdict::Remove(Remove::Superseded),
            CheckoutState::Unverifiable(why) => Verdict::Keep(Keep::Unverifiable(why.clone())),
        },
        Owner::Unknown => match unowned {
            Some(policy)
                if !group.any_running()
                    && group
                        .newest()
                        .is_some_and(|at| policy.now - at >= policy.min_age) =>
            {
                Verdict::Remove(Remove::Unowned)
            }
            _ => Verdict::Keep(Keep::UnknownOwner),
        },
    }
}

/// Decide every scope in `inventory`.
pub fn plan(
    inventory: Inventory,
    owner_of: &dyn Fn(&str) -> Owner,
    unowned: Option<UnownedPolicy>,
) -> Plan {
    let mut plan = Plan {
        unscoped_containers: inventory.unscoped_containers,
        unscoped_volumes: inventory.unscoped_volumes,
        ..Default::default()
    };
    for (scope, group) in inventory.groups {
        let owner = owner_of(&scope);
        let verdict = decide(&group, &owner, unowned);
        let decision = Decision {
            group,
            owner,
            verdict: verdict.clone(),
        };
        match verdict {
            Verdict::Remove(_) => plan.remove.push(decision),
            Verdict::Keep(_) => plan.keep.push(decision),
        }
    }
    plan
}

/// What removing a plan achieved.
#[derive(Debug, Default)]
pub struct Outcome {
    pub containers_removed: usize,
    pub volumes_removed: usize,
    pub failures: Vec<String>,
    /// Scopes fully cleaned up, whose registry entry was dropped.
    pub scopes_cleared: usize,
}

/// Remove the scopes in `decisions`: containers first (a volume a container
/// still references cannot go), then volumes. A scope's registry entry is dropped
/// only when nothing of it failed to go, so a half-cleaned script run is found
/// again by the next sweep.
pub async fn execute(
    docker: &DockerClient,
    registry: Option<&StackRegistry>,
    decisions: &[Decision],
) -> Outcome {
    let mut out = Outcome::default();
    for d in decisions {
        let failed_before = out.failures.len();
        let ids: Vec<String> = d.group.containers.iter().map(|c| c.id.clone()).collect();
        let failed_containers = docker.rm_containers(&ids).await;
        out.containers_removed += ids.len() - failed_containers.len();
        for (id, why) in failed_containers {
            let name = d
                .group
                .containers
                .iter()
                .find(|c| c.id == id)
                .map_or(id.as_str(), |c| c.name.as_str());
            out.failures.push(format!("container {name}: {why}"));
        }
        for v in &d.group.volumes {
            match docker.volume_rm(&v.name).await {
                Ok(o) if o.status.success() => out.volumes_removed += 1,
                Ok(o) => out.failures.push(format!(
                    "volume {}: {}",
                    v.name,
                    String::from_utf8_lossy(&o.stderr).trim()
                )),
                Err(e) => out.failures.push(format!("volume {}: {e}", v.name)),
            }
        }
        if out.failures.len() == failed_before {
            if let Some(registry) = registry
                && let Some(entry) = registry.get(&d.group.scope)
            {
                registry.remove(&entry.scope);
                // Install markers of an isolated scope live under
                // ~/.fed/isolated; a removed stack must not leave them behind.
                if entry.scope.starts_with("iso-") {
                    let _ = crate::markers::LifecycleMarkers::new(entry.workdir, Some(entry.scope))
                        .clear_all_installed();
                }
            }
            out.scopes_cleared += 1;
        }
    }
    out
}

/// Drop registry entries for checkouts that no longer exist and have nothing in
/// Docker, so the registry does not grow with every worktree ever made.
/// `present` is every scope Docker still has resources for.
pub fn forget_stale_entries(
    registry: &StackRegistry,
    present: &std::collections::HashSet<String>,
) -> usize {
    let mut forgotten = 0;
    for entry in registry.entries() {
        if entry.kind == StackKind::Checkout
            && !present.contains(&entry.scope)
            && checkout_state(&entry) == CheckoutState::Gone
        {
            registry.remove(&entry.scope);
            forgotten += 1;
        }
    }
    forgotten
}

/// Remove what dead `isolated: true` script runs left behind.
///
/// Runs on every orchestrated fed command. The registry directory is read
/// first and Docker is asked nothing unless a script run died, so a clean
/// machine pays one `readdir`. A run whose process was SIGKILLed cannot clean
/// up after itself; this is how the next fed command finishes the job.
/// Best effort: every failure is swallowed, and the entry stays for the next try.
pub async fn sweep_dead_script_runs(registry: &StackRegistry) -> usize {
    let dead = registry.dead_script_runs();
    if dead.is_empty() {
        return 0;
    }
    let docker = DockerClient::new();
    let mut inventory = match Inventory::load(&docker).await {
        Ok(inventory) => inventory,
        // No container runtime on this machine: the dead runs cannot have
        // left anything in it, so there is nothing to finish. Keeping their
        // records would make every later command look for Docker again.
        Err(crate::docker::DockerError::ExecFailed { .. }) => {
            for entry in &dead {
                registry.remove(&entry.scope);
            }
            return dead.len();
        }
        // Installed but not answering: try again next time.
        Err(_) => return 0,
    };
    let decisions: Vec<Decision> = dead
        .iter()
        .map(|entry| Decision {
            group: inventory
                .groups
                .remove(&entry.scope)
                .unwrap_or_else(|| Group {
                    scope: entry.scope.clone(),
                    ..Default::default()
                }),
            owner: Owner::Script {
                workdir: entry.workdir.clone(),
                alive: false,
            },
            verdict: Verdict::Remove(Remove::ScriptRunDead),
        })
        .collect();
    let outcome = execute(&docker, Some(registry), &decisions).await;
    if outcome.containers_removed + outcome.volumes_removed > 0 {
        tracing::info!(
            "Removed {} container(s) and {} volume(s) left by {} isolated script run(s) that died",
            outcome.containers_removed,
            outcome.volumes_removed,
            decisions.len()
        );
    }
    outcome.scopes_cleared
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    fn container(name: &str, state: &str) -> FedContainer {
        FedContainer {
            id: format!("id-{name}"),
            name: name.to_string(),
            state: state.to_string(),
            created: Some(Utc::now() - chrono::Duration::days(30)),
        }
    }

    fn policy(min_age_days: i64) -> Option<UnownedPolicy> {
        Some(UnownedPolicy {
            min_age: chrono::Duration::days(min_age_days),
            now: Utc::now(),
        })
    }

    fn volume(name: &str) -> FedVolume {
        FedVolume {
            name: name.to_string(),
            created: Some(Utc::now() - chrono::Duration::days(30)),
            dangling: true,
        }
    }

    fn checkout(state: CheckoutState) -> Owner {
        Owner::Checkout {
            workdir: Path::new("/work/a").into(),
            state,
        }
    }

    fn one_scope(containers: &[(&str, &str)], volumes: &[&str]) -> Group {
        Group {
            scope: "iso-aaaaaaaa".into(),
            containers: containers.iter().map(|(n, s)| container(n, s)).collect(),
            volumes: volumes.iter().map(|v| volume(v)).collect(),
        }
    }

    #[test]
    fn a_stopped_stack_in_a_checkout_that_exists_is_kept_with_its_volumes() {
        // The bug: `fed stop` removes containers, so the database volume of a
        // stack that is merely stopped was listed as an orphan.
        let group = one_scope(&[], &["fed-iso-aaaaaaaa-pgdata"]);
        assert_eq!(
            decide(&group, &checkout(CheckoutState::Current), None),
            Verdict::Keep(Keep::Stopped)
        );
        // Even with the exited container still around.
        let group = one_scope(
            &[("fed-iso-aaaaaaaa-pg", "exited")],
            &["fed-iso-aaaaaaaa-pgdata"],
        );
        assert_eq!(
            decide(&group, &checkout(CheckoutState::Current), policy(7)),
            Verdict::Keep(Keep::Stopped)
        );
    }

    #[test]
    fn a_running_stack_in_an_existing_checkout_is_kept() {
        let group = one_scope(&[("fed-iso-aaaaaaaa-pg", "running")], &[]);
        assert_eq!(
            decide(&group, &checkout(CheckoutState::Current), None),
            Verdict::Keep(Keep::Running)
        );
    }

    #[test]
    fn a_deleted_checkout_loses_exited_and_running_containers_and_volumes() {
        for containers in [
            vec![("fed-iso-aaaaaaaa-pg", "exited")],
            vec![("fed-iso-aaaaaaaa-pg", "running")],
            vec![],
        ] {
            let group = one_scope(&containers, &["fed-iso-aaaaaaaa-pgdata"]);
            assert_eq!(
                decide(&group, &checkout(CheckoutState::Gone), None),
                Verdict::Remove(Remove::CheckoutGone)
            );
        }
    }

    #[test]
    fn a_replaced_isolation_id_is_removed_unless_it_is_running() {
        let state = CheckoutState::Superseded {
            current: Some("iso-bbbbbbbb".into()),
        };
        assert_eq!(
            decide(
                &one_scope(&[("c", "exited")], &["v"]),
                &checkout(state.clone()),
                None
            ),
            Verdict::Remove(Remove::Superseded)
        );
        assert_eq!(
            decide(&one_scope(&[("c", "running")], &[]), &checkout(state), None),
            Verdict::Keep(Keep::SupersededButRunning)
        );
    }

    #[test]
    fn a_checkout_that_cannot_be_judged_is_kept() {
        let state = CheckoutState::Unverifiable("no lock.db".into());
        assert_eq!(
            decide(
                &one_scope(&[("c", "exited")], &["v"]),
                &checkout(state),
                policy(7)
            ),
            Verdict::Keep(Keep::Unverifiable("no lock.db".into()))
        );
    }

    #[test]
    fn a_script_run_is_removed_exactly_when_its_process_is_dead() {
        let script = |alive| Owner::Script {
            workdir: Path::new("/work/a").into(),
            alive,
        };
        let group = one_scope(
            &[("fed-iso-aaaaaaaa-pg", "running")],
            &["fed-iso-aaaaaaaa-pgdata"],
        );
        assert_eq!(
            decide(&group, &script(false), None),
            Verdict::Remove(Remove::ScriptRunDead),
            "the two postgres/redis pairs that ran for 22 hours"
        );
        assert_eq!(
            decide(&group, &script(true), policy(0)),
            Verdict::Keep(Keep::ScriptRunActive)
        );
    }

    #[test]
    fn resources_from_before_the_registry_are_kept_unless_the_user_vouches() {
        let group = one_scope(
            &[("fed-iso-aaaaaaaa-pg", "exited")],
            &["fed-iso-aaaaaaaa-pgdata"],
        );
        assert_eq!(
            decide(&group, &Owner::Unknown, None),
            Verdict::Keep(Keep::UnknownOwner)
        );
        assert_eq!(
            decide(&group, &Owner::Unknown, policy(7)),
            Verdict::Remove(Remove::Unowned)
        );
        assert_eq!(
            decide(&group, &Owner::Unknown, policy(60)),
            Verdict::Keep(Keep::UnknownOwner),
            "a stack created within the minimum age is not removed on a directory list"
        );
        let running = one_scope(&[("fed-iso-aaaaaaaa-pg", "running")], &[]);
        assert_eq!(
            decide(&running, &Owner::Unknown, policy(0)),
            Verdict::Keep(Keep::UnknownOwner),
            "even a vouched-for scope is not removed while something runs in it"
        );
        let mut undated = group.clone();
        undated.containers[0].created = None;
        undated.volumes[0].created = None;
        assert_eq!(
            decide(&undated, &Owner::Unknown, policy(0)),
            Verdict::Keep(Keep::UnknownOwner),
            "a stack whose age is unknown cannot be shown to be old"
        );
    }

    #[test]
    fn the_plan_groups_resources_by_scope_and_ignores_what_has_no_scope() {
        let inventory = Inventory::build(
            vec![
                container("fed-iso-aaaaaaaa-pg", "exited"),
                container("fed-iso-aaaaaaaa-redis", "exited"),
                container("fed-0a1b2c3d-pg", "exited"),
                container("fed-backup", "exited"),
            ],
            vec![
                volume("fed-iso-aaaaaaaa-pgdata"),
                volume("fed-0a1b2c3d-pgdata"),
                volume("fed-weird"),
            ],
        );
        assert_eq!(inventory.groups.len(), 2);
        assert_eq!(inventory.unscoped_containers, 1);
        assert_eq!(inventory.unscoped_volumes, 1);

        let plan = plan(
            inventory,
            &|scope| {
                if scope == "iso-aaaaaaaa" {
                    checkout(CheckoutState::Gone)
                } else {
                    checkout(CheckoutState::Current)
                }
            },
            None,
        );
        assert_eq!(plan.remove.len(), 1);
        assert_eq!(plan.remove[0].group.scope, "iso-aaaaaaaa");
        assert_eq!(plan.container_count(), 2);
        assert_eq!(plan.volume_count(), 1);
        assert_eq!(plan.keep.len(), 1);
        assert_eq!(plan.keep[0].verdict, Verdict::Keep(Keep::Stopped));
    }

    #[test]
    fn owner_lookup_follows_the_registry() {
        let tmp = tempfile::tempdir().unwrap();
        let registry = StackRegistry::at(tmp.path().join("stacks"));
        assert_eq!(owner_of(&registry, "iso-aaaaaaaa"), Owner::Unknown);

        let work = tmp.path().join("checkout");
        std::fs::create_dir(&work).unwrap();
        let scope = crate::service::hash_work_dir(&work);
        registry.register_checkout(&work, &scope).unwrap();
        assert!(matches!(
            owner_of(&registry, &scope),
            Owner::Checkout {
                state: CheckoutState::Current,
                ..
            }
        ));
        std::fs::remove_dir(&work).unwrap();
        assert!(matches!(
            owner_of(&registry, &scope),
            Owner::Checkout {
                state: CheckoutState::Gone,
                ..
            }
        ));

        let run = registry
            .begin_script_run(tmp.path(), "iso-cafef00d")
            .unwrap();
        assert!(matches!(
            owner_of(&registry, "iso-cafef00d"),
            Owner::Script { alive: true, .. }
        ));
        drop(run);
        assert!(matches!(
            owner_of(&registry, "iso-cafef00d"),
            Owner::Script { alive: false, .. }
        ));
    }
}
