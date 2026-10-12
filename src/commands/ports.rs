use crate::cli::PortsCommands;
use crate::output::UserOutput;
use fed::Parser as ConfigParser;
use fed::RunContext;
use fed::service::Status;
use fed::state::StateTracker;
use std::path::PathBuf;

pub async fn run_ports(
    cmd: &PortsCommands,
    workdir: Option<PathBuf>,
    config_path: Option<PathBuf>,
    ctx: RunContext,
    out: &dyn UserOutput,
) -> anyhow::Result<()> {
    match cmd {
        PortsCommands::List { json } => {
            let work_dir = resolve_work_dir(workdir, config_path.as_deref())?;
            list_ports(&work_dir, *json, out).await
        }
        PortsCommands::Resolve { pins } => {
            let pins = pins
                .iter()
                .map(|pin| parse_pin(pin))
                .collect::<anyhow::Result<Vec<_>>>()?;
            let work_dir = resolve_work_dir(workdir, config_path.as_deref())?;
            resolve_ports(&work_dir, config_path, &pins, ctx, out).await
        }
    }
}

/// A `--pin` value: `PARAM=PORT`.
pub(crate) fn parse_pin(pin: &str) -> anyhow::Result<(String, u16)> {
    let Some((param, port)) = pin.split_once('=').filter(|(p, _)| !p.is_empty()) else {
        anyhow::bail!(
            "fed ports resolve --pin needs PARAM=PORT, such as WEB_PORT=3000. It received `{pin}`."
        );
    };
    match port.parse::<u16>() {
        Ok(port) if port > 0 => Ok((param.to_string(), port)),
        _ => anyhow::bail!("fed ports resolve --pin {pin} needs a port from 1 to 65535."),
    }
}

/// Pin `pins`, resolve every port parameter the way `fed start` does and
/// save the result, without starting anything. Prints the saved ports as
/// one line of JSON (`{"PARAM": port}`).
async fn resolve_ports(
    work_dir: &std::path::Path,
    config_path: Option<PathBuf>,
    pins: &[(String, u16)],
    ctx: RunContext,
    out: &dyn UserOutput,
) -> anyhow::Result<()> {
    let parser = ConfigParser::new();
    let config_file = match config_path {
        Some(path) => path,
        None => parser.find_config_file()?,
    };
    let config = parser
        .load_config_with_packages_offline(&config_file, ctx.offline)
        .await?;
    config.validate()?;
    let params = config.get_effective_parameters();
    for (param, port) in pins {
        if !params.get(param).is_some_and(|p| p.is_port_type()) {
            anyhow::bail!(
                "fed ports resolve --pin {param}={port} needs a port parameter, and fed.yaml has no port parameter called {param}."
            );
        }
    }

    // The same lock as `fed start`, so a start cannot resolve between the
    // pin and this resolution.
    let _lock = fed::orchestrator::StartLock::acquire(work_dir).await?;
    let scope = {
        let mut tracker = StateTracker::new(work_dir.to_path_buf()).await?;
        tracker.initialize().await?;
        let (enabled, isolation_id) = tracker.get_isolation_mode().await;
        let scope = if enabled { isolation_id } else { None };
        tracker.pin_ports(pins, scope.as_deref()).await?;
        scope
    };

    let orchestrator = fed::Orchestrator::builder()
        .config(config)
        .work_dir(work_dir.to_path_buf())
        .run_context(ctx)
        .auto_resolve_conflicts(true)
        .build()
        .await?;
    let ports = orchestrator
        .state_tracker
        .read()
        .await
        .get_global_port_allocations(scope.as_deref())
        .await;
    out.status(&serde_json::to_string(&ports)?);
    Ok(())
}

pub(super) fn resolve_work_dir(
    workdir: Option<PathBuf>,
    config_path: Option<&std::path::Path>,
) -> anyhow::Result<PathBuf> {
    if let Some(w) = workdir {
        return Ok(w);
    }
    // Try to derive from config path
    let parser = ConfigParser::new();
    let resolved_config = if let Some(path) = config_path {
        path.to_path_buf()
    } else {
        parser.find_config_file()?
    };
    if let Some(parent) = resolved_config.parent() {
        if parent.as_os_str().is_empty() {
            Ok(std::env::current_dir()?)
        } else {
            Ok(parent.to_path_buf())
        }
    } else {
        Ok(std::env::current_dir()?)
    }
}

async fn list_ports(
    work_dir: &std::path::Path,
    json: bool,
    out: &dyn UserOutput,
) -> anyhow::Result<()> {
    let tracker = StateTracker::new(work_dir.to_path_buf()).await?;
    // Show the allocations for the currently-active scope: the isolation session
    // if enabled, otherwise the shared (non-isolated) scope.
    let (enabled, isolation_id) = tracker.get_isolation_mode().await;
    let scope = if enabled {
        isolation_id.as_deref()
    } else {
        None
    };
    let ports = tracker.get_global_port_allocations(scope).await;

    if json {
        out.status(&serde_json::to_string_pretty(&ports)?);
    } else {
        out.status("\nPort Allocations");
        out.status("================\n");

        if ports.is_empty() {
            out.status("No ports are currently allocated.");
            out.status("Ports are allocated on `fed start` or `fed isolate enable`.\n");
            return Ok(());
        }

        let mut sorted: Vec<_> = ports.iter().collect();
        sorted.sort_by_key(|(_, port)| *port);

        for (param, port) in &sorted {
            out.status(&format!("  {:>5}  {}", port, param));
        }
        out.blank();
    }

    Ok(())
}

/// Ensure no services are running. With --force, auto-stop them.
/// Without --force, prompt the user.
pub(super) async fn ensure_services_stopped(
    work_dir: &std::path::Path,
    force: bool,
    out: &dyn UserOutput,
) -> anyhow::Result<()> {
    let tracker = StateTracker::new(work_dir.to_path_buf()).await?;
    let services = tracker.get_services().await;

    let running: Vec<_> = services
        .iter()
        .filter(|(_, state)| {
            matches!(
                state.status,
                Status::Running | Status::Healthy | Status::Starting
            )
        })
        .map(|(name, _)| name.clone())
        .collect();

    if running.is_empty() {
        return Ok(());
    }

    if force {
        out.status(&format!("Stopping {} running service(s)...", running.len()));
        // Use the state-tracker-based stop (no config needed)
        super::run_stop_from_state(work_dir, vec![], out).await?;
    } else {
        out.status(&format!(
            "The following services are running: {}",
            running.join(", ")
        ));
        out.progress("Stop them to continue? [y/N] ");

        let mut input = String::new();
        std::io::stdin().read_line(&mut input)?;
        if !input.trim().eq_ignore_ascii_case("y") {
            anyhow::bail!("Aborted: services must be stopped first");
        }

        super::run_stop_from_state(work_dir, vec![], out).await?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_pin_is_a_parameter_and_a_port() {
        assert_eq!(
            parse_pin("WEB_PORT=3000").unwrap(),
            ("WEB_PORT".into(), 3000)
        );
        for bad in [
            "WEB_PORT",
            "=3000",
            "WEB_PORT=0",
            "WEB_PORT=70000",
            "WEB_PORT=x",
        ] {
            let error = parse_pin(bad).unwrap_err().to_string();
            assert!(
                error.starts_with("fed ports resolve --pin"),
                "{bad}: {error}"
            );
        }
    }
}
