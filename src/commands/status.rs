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
                };

                (name, entry)
            })
            .collect::<std::collections::BTreeMap<_, _>>();

        out.status(&serde_json::to_string_pretty(&status_obj)?);
    } else {
        out.status("Service Status:");
        out.status(&format!("{:-<50}", ""));

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
}
