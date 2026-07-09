use crate::output::UserOutput;
use fed::config::Config;
use fed::error::Error;
use fed::state::SqliteStateTracker;
use serde::Serialize;
use std::collections::HashMap;
use std::path::PathBuf;

/// Debug subcommands
#[derive(Debug, Clone)]
pub enum DebugCommand {
    /// Show full state tracker contents
    State,
    /// Show port allocations
    Ports,
    /// Show circuit breaker state for a service
    CircuitBreaker { service: String },
}

/// Output format for debug commands
#[derive(Debug, Serialize)]
struct StateDebugOutput {
    services: Vec<ServiceDebugInfo>,
    allocated_ports: Vec<PortDebugInfo>,
}

#[derive(Debug, Serialize)]
struct ServiceDebugInfo {
    name: String,
    status: String,
    service_type: String,
    pid: Option<u32>,
    container_id: Option<String>,
    started_at: String,
    restart_count: u32,
    last_restart_at: Option<String>,
    consecutive_failures: u32,
    circuit_breaker_state: String,
    port_allocations: HashMap<String, u16>,
}

#[derive(Debug, Clone, Serialize)]
struct PortDebugInfo {
    port: u16,
    service: String,
    parameter: String,
}

#[derive(Debug, Serialize)]
struct CircuitBreakerDebugOutput {
    service: String,
    status: String,
    restart_count: u32,
    consecutive_failures: u32,
    recent_restarts: Vec<RestartEvent>,
    circuit_breaker_open: bool,
    open_until: Option<String>,
}

#[derive(Debug, Serialize)]
struct RestartEvent {
    timestamp: String,
}

/// Run the debug command
pub async fn run_debug(
    command: DebugCommand,
    _config: &Config,
    work_dir: PathBuf,
    json: bool,
    out: &dyn UserOutput,
) -> anyhow::Result<()> {
    match command {
        DebugCommand::State => {
            let state = collect_state_info(work_dir).await?;

            if json {
                out.status(&serde_json::to_string_pretty(&state)?);
            } else {
                print_state_human_readable(&state, out);
            }
        }
        DebugCommand::Ports => {
            let state = collect_state_info(work_dir).await?;

            if json {
                let ports: Vec<_> = state.allocated_ports.clone();
                out.status(&serde_json::to_string_pretty(&ports)?);
            } else {
                print_ports_human_readable(&state.allocated_ports, out);
            }
        }
        DebugCommand::CircuitBreaker { service } => {
            let info = collect_circuit_breaker_info(work_dir, &service).await?;

            if json {
                out.status(&serde_json::to_string_pretty(&info)?);
            } else {
                print_circuit_breaker_human_readable(&info, out);
            }
        }
    }

    Ok(())
}

async fn collect_state_info(work_dir: PathBuf) -> anyhow::Result<StateDebugOutput> {
    let state_tracker = SqliteStateTracker::new(work_dir).await?;
    let all_services = state_tracker.get_services().await;

    let mut services = Vec::new();
    for service_state in all_services.values() {
        let circuit_breaker_open = state_tracker
            .is_circuit_breaker_open(&service_state.id)
            .await;

        let circuit_breaker_state = if circuit_breaker_open {
            "Open (preventing restarts)".to_string()
        } else {
            "Closed (restarts allowed)".to_string()
        };

        services.push(ServiceDebugInfo {
            name: service_state.id.clone(),
            status: service_state.status.to_string(),
            service_type: service_state.service_type.to_string(),
            pid: service_state.pid,
            container_id: service_state.container_id.clone(),
            started_at: service_state.started_at.to_rfc3339(),
            restart_count: service_state.restart_count,
            last_restart_at: service_state.last_restart_at.map(|dt| dt.to_rfc3339()),
            consecutive_failures: service_state.consecutive_failures,
            circuit_breaker_state,
            port_allocations: service_state.port_allocations.clone(),
        });
    }

    let mut allocated_ports = Vec::new();
    for service_state in all_services.values() {
        for (param_name, port) in &service_state.port_allocations {
            allocated_ports.push(PortDebugInfo {
                port: *port,
                service: service_state.id.clone(),
                parameter: param_name.clone(),
            });
        }
    }

    // Sort ports for consistent output
    allocated_ports.sort_by_key(|p| p.port);

    Ok(StateDebugOutput {
        services,
        allocated_ports,
    })
}

async fn collect_circuit_breaker_info(
    work_dir: PathBuf,
    service: &str,
) -> anyhow::Result<CircuitBreakerDebugOutput> {
    let state_tracker = SqliteStateTracker::new(work_dir).await?;

    let service_state = state_tracker
        .get_service(service)
        .await
        .ok_or_else(|| Error::ServiceNotFound(service.to_string()))?;

    // Get restart history from the database
    let recent_restarts = get_recent_restarts(&state_tracker, service).await?;

    let circuit_breaker_open = state_tracker.is_circuit_breaker_open(service).await;

    let status = if circuit_breaker_open {
        "Open (preventing restarts)".to_string()
    } else {
        "Closed (restarts allowed)".to_string()
    };

    Ok(CircuitBreakerDebugOutput {
        service: service.to_string(),
        status,
        restart_count: service_state.restart_count,
        consecutive_failures: service_state.consecutive_failures,
        recent_restarts,
        circuit_breaker_open,
        open_until: None, // We don't expose the exact open_until timestamp
    })
}

async fn get_recent_restarts(
    state_tracker: &SqliteStateTracker,
    service: &str,
) -> anyhow::Result<Vec<RestartEvent>> {
    let timestamps = state_tracker.get_restart_history(service).await?;
    Ok(timestamps
        .into_iter()
        .map(|timestamp| RestartEvent { timestamp })
        .collect())
}

fn print_state_human_readable(state: &StateDebugOutput, out: &dyn UserOutput) {
    out.status("\nService Federation State Tracker");
    out.status("================================\n");

    if state.services.is_empty() {
        out.status("No services are currently tracked.\n");
    } else {
        out.status("Services:\n");
        for service in &state.services {
            out.status(&format!("  {} ({})", service.name, service.status));
            out.status(&format!("    Type: {}", service.service_type));

            if let Some(pid) = service.pid {
                out.status(&format!("    PID: {}", pid));
            }
            if let Some(container_id) = &service.container_id {
                out.status(&format!("    Container: {}", container_id));
            }

            out.status(&format!("    Started: {}", service.started_at));
            out.status(&format!("    Restarts: {}", service.restart_count));

            if let Some(last_restart) = &service.last_restart_at {
                out.status(&format!("    Last Restart: {}", last_restart));
            }

            if service.consecutive_failures > 0 {
                out.status(&format!(
                    "    Consecutive Failures: {}",
                    service.consecutive_failures
                ));
            }

            out.status(&format!(
                "    Circuit Breaker: {}",
                service.circuit_breaker_state
            ));

            if !service.port_allocations.is_empty() {
                out.status("    Ports:");
                for (param, port) in &service.port_allocations {
                    out.status(&format!("      {} = {}", param, port));
                }
            }

            out.blank();
        }
    }

    if !state.allocated_ports.is_empty() {
        out.status("Allocated Ports:\n");
        for port_info in &state.allocated_ports {
            out.status(&format!(
                "  {} -> {} (parameter: {})",
                port_info.port, port_info.service, port_info.parameter
            ));
        }
        out.blank();
    }
}

fn print_ports_human_readable(ports: &[PortDebugInfo], out: &dyn UserOutput) {
    out.status("\nPort Allocations");
    out.status("================\n");

    if ports.is_empty() {
        out.status("No ports are currently allocated.\n");
        return;
    }

    for port_info in ports {
        out.status(&format!(
            "  Port {}: {} (parameter: {})",
            port_info.port, port_info.service, port_info.parameter
        ));
    }
    out.blank();
}

fn print_circuit_breaker_human_readable(info: &CircuitBreakerDebugOutput, out: &dyn UserOutput) {
    out.status(&format!("\nCircuit Breaker Status for '{}'", info.service));
    out.status("=====================================\n");

    out.status(&format!("Status: {}", info.status));

    if let Some(ref open_until) = info.open_until {
        out.status(&format!("Open Until: {}", open_until));
    }

    out.status(&format!("Total Restarts: {}", info.restart_count));
    out.status(&format!(
        "Consecutive Failures: {}",
        info.consecutive_failures
    ));

    if !info.recent_restarts.is_empty() {
        out.status(&format!(
            "\nRecent Restarts (last {} events):",
            info.recent_restarts.len()
        ));
        for event in &info.recent_restarts {
            out.status(&format!("  - {}", event.timestamp));
        }
    } else {
        out.status("\nNo recent restarts recorded.");
    }

    out.blank();
}
