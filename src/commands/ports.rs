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
        PortsCommands::Resolve { json } => {
            let work_dir = resolve_work_dir(workdir, config_path.as_deref())?;
            resolve_ports(&work_dir, config_path, *json, ctx, out).await
        }
    }
}

/// A `--pin` value of `fed start`: `PARAM=PORT`.
pub fn parse_pin(pin: &str) -> anyhow::Result<(String, u16)> {
    let Some((param, port)) = pin.split_once('=').filter(|(p, _)| !p.is_empty()) else {
        anyhow::bail!(
            "fed start --pin needs PARAM=PORT, such as WEB_PORT=3000. It received `{pin}`."
        );
    };
    match port.parse::<u16>() {
        Ok(port) if port > 0 => Ok((param.to_string(), port)),
        _ => anyhow::bail!("fed start --pin {pin} needs a port from 1 to 65535."),
    }
}

/// How `fed ports resolve` describes the port a parameter would get:
/// `kept` when `fed start` keeps it, `random` when `fed start` would pick
/// another random port, `taken` when a program outside the stack uses it.
fn port_state(reason: &fed::parameter::PortResolutionReason) -> &'static str {
    use fed::parameter::PortResolutionReason as Reason;
    match reason {
        Reason::Taken => "taken",
        Reason::Random | Reason::ConflictAutoResolved { .. } => "random",
        Reason::Explicit | Reason::DefaultAvailable | Reason::Cached | Reason::Pinned => "kept",
    }
}

/// Work out the port of every port parameter the way `fed start` would, and
/// print it with its state. Saves nothing and moves nothing: a port that
/// another program uses is reported as `taken`.
async fn resolve_ports(
    work_dir: &std::path::Path,
    config_path: Option<PathBuf>,
    json: bool,
    ctx: RunContext,
    out: &dyn UserOutput,
) -> anyhow::Result<()> {
    let mut config = load_config(config_path, ctx.offline).await?;
    let tracker = StateTracker::new(work_dir.to_path_buf()).await?;
    let (enabled, isolation_id) = tracker.get_isolation_mode().await;
    let scope = if enabled { isolation_id } else { None };
    let port_params: std::collections::HashSet<String> = config
        .get_effective_parameters()
        .iter()
        .filter(|(_, p)| p.is_port_type())
        .map(|(name, _)| name.clone())
        .collect();

    let mut resolver = fed::parameter::Resolver::new();
    resolver.set_work_dir(work_dir);
    resolver.set_isolation_id(scope.clone());
    resolver.set_offline(ctx.offline);
    resolver.set_secret_cache(ctx.secret_cache);
    // Ports never depend on secrets, so no vault value is needed.
    resolver.set_required_names(Some(port_params));
    resolver.set_port_store(Box::new(fed::port::SqlitePortStore::with_pins(
        tracker.get_global_port_allocations(scope.as_deref()).await,
        tracker.get_pinned_ports(scope.as_deref()).await,
    )));
    resolver.set_prefer_config_defaults(scope.is_none());
    resolver.set_report_taken(true);
    let tracker = std::sync::Arc::new(tokio::sync::RwLock::new(tracker));
    fed::orchestrator::collect_managed_ports(&mut resolver, &tracker, scope.as_deref()).await;
    resolver.resolve_parameters(&mut config)?;

    let ports: std::collections::BTreeMap<&str, serde_json::Value> = resolver
        .get_port_resolutions()
        .iter()
        .map(|r| {
            let state = port_state(&r.reason);
            (
                r.param_name.as_str(),
                serde_json::json!({ "port": r.resolved_port, "state": state }),
            )
        })
        .collect();
    if json {
        out.status(&serde_json::to_string(&ports)?);
    } else {
        for (param, port) in &ports {
            out.status(&format!(
                "  {:>5}  {param} ({})",
                port["port"], port["state"]
            ));
        }
    }
    Ok(())
}

async fn load_config(
    config_path: Option<PathBuf>,
    offline: bool,
) -> anyhow::Result<fed::config::Config> {
    let parser = ConfigParser::new();
    let config_file = match config_path {
        Some(path) => path,
        None => parser.find_config_file()?,
    };
    let config = parser
        .load_config_with_packages_offline(&config_file, offline)
        .await?;
    config.validate()?;
    Ok(config)
}

/// Prepare `fed start --pin` under the start lock, before anything is
/// resolved or saved. Fails when a pin names no port parameter, or when a
/// program outside the stack uses a pinned port. Then stops the running
/// services that use a parameter whose port changes, so they start on the
/// new port.
pub async fn prepare_pins(
    work_dir: &std::path::Path,
    config: &fed::config::Config,
    pins: &std::collections::HashMap<String, u16>,
    out: &dyn UserOutput,
) -> anyhow::Result<()> {
    let params = config.get_effective_parameters();
    for (param, port) in pins {
        if !params.get(param).is_some_and(|p| p.is_port_type()) {
            anyhow::bail!(
                "fed start --pin {param}={port} needs a port parameter, and fed.yaml has no port parameter called {param}."
            );
        }
    }

    let tracker = StateTracker::new(work_dir.to_path_buf()).await?;
    let (enabled, isolation_id) = tracker.get_isolation_mode().await;
    let scope = if enabled { isolation_id } else { None };
    let current = tracker.get_global_port_allocations(scope.as_deref()).await;
    let changed: std::collections::HashSet<String> = pins
        .iter()
        .filter(|(param, port)| current.get(*param) != Some(*port))
        .map(|(param, _)| param.clone())
        .collect();
    for param in &changed {
        let port = pins[param];
        if !fed::port::PortConflict::is_port_available(port) {
            return Err(fed::Error::PinnedPortTaken {
                param: param.clone(),
                port,
            }
            .into());
        }
    }

    let using = fed::parameter::scanner::services_using(config, &changed);
    let mut running: Vec<String> = tracker
        .get_services()
        .await
        .into_iter()
        .filter(|(name, state)| {
            using.contains(name)
                && matches!(
                    state.status,
                    Status::Running | Status::Healthy | Status::Starting | Status::Failing
                )
        })
        .map(|(name, _)| name)
        .collect();
    drop(tracker);
    if running.is_empty() {
        return Ok(());
    }
    running.sort();
    out.status(&format!(
        "Stopping {} so that they start on their new ports",
        running.join(", ")
    ));
    super::run_stop_from_state(work_dir, running, out).await
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
            assert!(error.starts_with("fed start --pin"), "{bad}: {error}");
        }
    }
}
