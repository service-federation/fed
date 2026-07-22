use crate::output::UserOutput;
use fed::{Orchestrator, config::Config};

/// Bumped only on a breaking/renaming change to the per-service JSON shape.
const SCHEMA_VERSION: u32 = 1;

/// JSON shape for a single service under `fed status --json`.
///
/// Every field is always present (never omitted): an agent doing
/// `data[svc]["pid"]` should never hit a `KeyError` depending on service
/// type or lifecycle state. Fields sourced from `ServiceState` are `null`/
/// empty when no persisted row exists (never started, or stopped — the row
/// is deleted on stop).
///
/// `supervisor_running`/`supervisor_pid` are directory-wide facts (there is
/// exactly one `fed supervise` daemon per `.fed/` directory), repeated
/// inside every per-service object rather than hoisted to a top-level
/// sibling key — the same collision-avoidance reasoning `schema_version`
/// already uses below: service names are unrestricted YAML keys with no
/// reserved-word protection, so a top-level `"supervisor_pid"` key could
/// collide with a real service literally named that. This is additive to
/// the schema (every existing key keeps its exact shape), so
/// `SCHEMA_VERSION` stays at `1` per its own "bumped only on a
/// breaking/renaming change" rule (`07-supervisor.md` Design §4).
#[derive(serde::Serialize)]
struct ServiceStatusJson {
    status: &'static str,
    schema_version: u32,
    health: &'static str,
    service_type: String,
    pid: Option<u32>,
    container_id: Option<String>,
    started_at: Option<String>,
    uptime_seconds: Option<i64>,
    // best-effort; empty for docker-compose services until compose port
    // introspection is added, see 03-status-json.md
    ports: std::collections::HashMap<String, u16>,
    startup_message: Option<String>,
    /// `"fed"` — in the supervisor's filtered health-check scope
    /// (`fed::orchestrator::supervised_service_names`); `"docker-native"` —
    /// a Docker service with `restart: always`, protected by Docker's own
    /// `--restart unless-stopped` even with no fed process alive (this
    /// implies `"fed"`-scope membership too, since `restart: always` is
    /// always in the union — `"docker-native"` is reported instead of
    /// `"fed"` in that case because it's the more specific, more
    /// informative fact: fed-level supervision requires a live `fed
    /// supervise`/`--watch`/`tui` process, the native flag doesn't); or
    /// `"none"` — outside the union, never health-checked by the
    /// supervisor.
    supervised_by: &'static str,
    /// Whether a `fed supervise` daemon currently holds
    /// `.fed/supervisor.lock` for this project. Directory-wide.
    supervisor_running: bool,
    /// PID of the live supervisor, or `null` if none is running.
    /// Directory-wide.
    supervisor_pid: Option<u32>,
}

/// Coarse, agent-facing health bucket derived from the raw `Status`.
///
/// Distinct from `status` on purpose: `status` is the exact fed-internal
/// state word, `health` collapses it to a small set an agent can branch on
/// without knowing all 7 `Status` variants.
fn health_bucket(status: fed::Status) -> &'static str {
    match status {
        fed::Status::Healthy | fed::Status::Completed => "healthy",
        fed::Status::Failing => "unhealthy",
        fed::Status::Starting => "starting",
        fed::Status::Stopping => "stopping",
        fed::Status::Stopped => "stopped",
        // Process/container is up but no healthcheck has confirmed it yet
        // (or the service has no healthcheck configured, so it never leaves
        // Running) — deliberately NOT "healthy": conflating "alive" with
        // "verified healthy" is exactly the ambiguity this field exists to
        // resolve, not reintroduce.
        fed::Status::Running => "unknown",
    }
}

/// Which supervision mechanism, if any, protects `name` — see
/// `ServiceStatusJson::supervised_by`'s doc comment for the precedence
/// rationale (docker-native takes priority over the plain "fed" bucket
/// since a `restart: always` Docker service is always in `scope` too).
fn supervised_by_bucket(
    config: &Config,
    name: &str,
    scope: &std::collections::HashSet<String>,
) -> &'static str {
    let docker_native = config
        .services
        .get(name)
        .map(|s| s.docker_native_restart_enabled())
        .unwrap_or(false);

    if docker_native {
        "docker-native"
    } else if scope.contains(name) {
        "fed"
    } else {
        "none"
    }
}

pub async fn run_status(
    orchestrator: &Orchestrator,
    config: &Config,
    json: bool,
    tag: Option<String>,
    out: &dyn UserOutput,
) -> anyhow::Result<()> {
    let mut status = orchestrator.get_status().await;
    let total_services = status.len();

    // Filter by tag if specified
    let tag_filter = tag;
    if let Some(ref tag_filter) = tag_filter {
        let services_with_tag = config.services_with_tag(tag_filter);
        status.retain(|name, _| services_with_tag.contains(name));
    }

    // Directory-wide supervisor facts, computed once regardless of the
    // json/human branch below — a plain file read plus a non-blocking
    // flock probe (`live_supervisor_pid`), not an Orchestrator/state-tracker
    // call, so this never spawns or respawns anything (`fed status` stays
    // strictly read-only, per `07-supervisor.md` Design §1's scaled-back
    // self-heal promise).
    let supervisor_pid =
        fed::orchestrator::supervisor::live_supervisor_pid(orchestrator.work_dir());
    let supervised_scope = fed::orchestrator::supervised_service_names(config);

    if json {
        // Fetch persisted state once, not per service — this is the single
        // read-lock acquisition and single SQLite query for the whole
        // command. Nothing here calls `manager.get_port_mappings()`/
        // `get_pid()`/`get_container_id()` on live managers, so this adds
        // zero Docker calls beyond what `get_status()` already made above.
        let service_states = orchestrator.state_tracker.read().await.get_services().await;

        let status_obj = status
            .into_iter()
            .map(|(name, stat)| {
                let status_str = match stat {
                    fed::Status::Running => "running",
                    fed::Status::Healthy => "healthy",
                    fed::Status::Stopped => "stopped",
                    fed::Status::Starting => "starting",
                    fed::Status::Failing => "failing",
                    fed::Status::Stopping => "stopping",
                    fed::Status::Completed => "completed",
                };

                let service_type = config
                    .services
                    .get(&name)
                    .map(|s| s.service_type().to_string())
                    .unwrap_or_else(|| "undefined".to_string());

                let service_state = service_states.get(&name);

                let entry = ServiceStatusJson {
                    status: status_str,
                    schema_version: SCHEMA_VERSION,
                    health: health_bucket(stat),
                    service_type,
                    pid: service_state.and_then(|s| s.pid),
                    container_id: service_state.and_then(|s| s.container_id.clone()),
                    started_at: service_state.map(|s| s.started_at.to_rfc3339()),
                    uptime_seconds: service_state
                        .map(|s| (chrono::Utc::now() - s.started_at).num_seconds()),
                    ports: service_state
                        .map(|s| s.port_allocations.clone())
                        .unwrap_or_default(),
                    startup_message: service_state.and_then(|s| s.startup_message.clone()),
                    supervised_by: supervised_by_bucket(config, &name, &supervised_scope),
                    supervisor_running: supervisor_pid.is_some(),
                    supervisor_pid,
                };

                (name, entry)
            })
            .collect::<std::collections::BTreeMap<_, _>>();

        out.status(&serde_json::to_string_pretty(&status_obj)?);
    } else {
        out.status("Service Status:");
        out.status(&format!("{:-<50}", ""));
        out.status(&match supervisor_pid {
            Some(pid) => format!("Supervisor: active (pid {})", pid),
            None => "Supervisor: none".to_string(),
        });

        if status.is_empty() {
            match tag_filter {
                Some(tag) if total_services > 0 => {
                    out.status(&format!(
                        "  No services match tag '{}' ({} service(s) configured)",
                        tag, total_services
                    ));
                }
                _ => out.status("  No services configured"),
            }
        } else {
            for (name, stat) in status {
                let status_icon = match stat {
                    fed::Status::Healthy => "✓",
                    fed::Status::Completed => "✓",
                    fed::Status::Running => "+",
                    fed::Status::Stopped => "o",
                    fed::Status::Starting => "~",
                    fed::Status::Stopping => "-",
                    fed::Status::Failing => "x",
                };
                out.status(&format!("  {} {:<30} {}", status_icon, name, stat));
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every `Status` variant must map to exactly the bucket documented in
    /// the schema design (03-status-json.md) — cheapest, fastest test to
    /// catch a future accidental remap (e.g. someone "simplifying" `Running`
    /// to `"healthy"`).
    #[test]
    fn health_bucket_covers_all_status_variants() {
        assert_eq!(health_bucket(fed::Status::Healthy), "healthy");
        assert_eq!(health_bucket(fed::Status::Completed), "healthy");
        assert_eq!(health_bucket(fed::Status::Failing), "unhealthy");
        assert_eq!(health_bucket(fed::Status::Starting), "starting");
        assert_eq!(health_bucket(fed::Status::Stopping), "stopping");
        assert_eq!(health_bucket(fed::Status::Stopped), "stopped");
        assert_eq!(health_bucket(fed::Status::Running), "unknown");
    }

    // --- supervised_by_bucket (07-supervisor.md Design §4) ---

    /// `docker-native` takes precedence over `fed` when both technically
    /// apply — a Docker service with `restart: always` is always in the
    /// `fed` scope too (`restart != No`), so this is the only reachable
    /// outcome for that combination; asserting it here pins the precedence
    /// choice against an accidental flip.
    #[test]
    fn supervised_by_prefers_docker_native_over_fed_when_both_apply() {
        let mut config = Config::default();
        config.services.insert(
            "web".to_string(),
            fed::config::Service {
                image: Some("nginx".to_string()),
                restart: Some(fed::RestartPolicy::Always),
                ..Default::default()
            },
        );
        let scope = fed::orchestrator::supervised_service_names(&config);
        assert!(scope.contains("web"), "restart: always must be in scope");
        assert_eq!(
            supervised_by_bucket(&config, "web", &scope),
            "docker-native"
        );
    }

    #[test]
    fn supervised_by_reports_fed_for_process_service_with_restart_policy() {
        let mut config = Config::default();
        config.services.insert(
            "worker".to_string(),
            fed::config::Service {
                process: Some("sleep 300".to_string()),
                restart: Some(fed::RestartPolicy::Always),
                ..Default::default()
            },
        );
        let scope = fed::orchestrator::supervised_service_names(&config);
        assert_eq!(supervised_by_bucket(&config, "worker", &scope), "fed");
    }

    #[test]
    fn supervised_by_reports_none_outside_the_union_scope() {
        let mut config = Config::default();
        config.services.insert(
            "idle".to_string(),
            fed::config::Service {
                process: Some("sleep 300".to_string()),
                restart: Some(fed::RestartPolicy::No),
                ..Default::default()
            },
        );
        let scope = fed::orchestrator::supervised_service_names(&config);
        assert_eq!(supervised_by_bucket(&config, "idle", &scope), "none");
    }

    #[test]
    fn supervised_by_reports_none_for_unknown_service_name() {
        let config = Config::default();
        let scope = fed::orchestrator::supervised_service_names(&config);
        assert_eq!(supervised_by_bucket(&config, "ghost", &scope), "none");
    }
}
