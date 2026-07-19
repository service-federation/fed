use crate::output::UserOutput;
use fed::{Orchestrator, config::Config};

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
        use serde_json::json;

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
                (
                    name,
                    json!({
                        "status": status_str
                    }),
                )
            })
            .collect::<serde_json::Map<_, _>>();

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
