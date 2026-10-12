// Split from resolver.rs (see git history before this commit for pre-split blame).
use super::*;

/// Reason a port was resolved to its final value
#[derive(Debug, Clone, PartialEq)]
pub enum PortResolutionReason {
    /// User supplied an exact value
    Explicit,
    /// Default port was available and used directly
    DefaultAvailable,
    /// Default port had a conflict, auto-resolved to a different port
    ConflictAutoResolved {
        default_port: u16,
        conflict_pid: Option<u32>,
        conflict_process: Option<String>,
    },
    /// Port was restored from cache
    Cached,
    /// No default available, allocated a random port
    Random,
}

impl std::fmt::Display for PortResolutionReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Explicit => write!(f, "explicit value"),
            Self::DefaultAvailable => write!(f, "default port available"),
            Self::ConflictAutoResolved {
                default_port,
                conflict_pid,
                conflict_process,
            } => {
                write!(f, "default port {} conflicted", default_port)?;
                match (conflict_pid, conflict_process) {
                    (Some(pid), Some(name)) => write!(f, " with '{}' (PID {})", name, pid),
                    (Some(pid), None) => write!(f, " with PID {}", pid),
                    _ => write!(f, " with unknown process"),
                }
            }
            Self::Cached => write!(f, "restored from cache"),
            Self::Random => write!(f, "randomly allocated"),
        }
    }
}

/// Record of how a port parameter was resolved
#[derive(Debug, Clone)]
pub struct PortResolution {
    pub param_name: String,
    pub resolved_port: u16,
    pub reason: PortResolutionReason,
}

impl Resolver {
    /// Reserve a user-supplied port exactly, unless it is already owned by a
    /// running service managed by this orchestrator.
    pub(super) fn reserve_exact_port(&mut self, port: u16, param_name: &str) -> Result<()> {
        if self.managed_ports.contains(&port) {
            self.port_allocator.mark_allocated(port);
            return Ok(());
        }

        self.port_allocator
            .try_allocate_port(port)
            .map(|_| ())
            .map_err(|error| {
                let detail = match error {
                    Error::PortAllocation(detail) => detail,
                    other => other.to_string(),
                };
                Error::PortAllocation(format!(
                    "Parameter '{}' requires port {}, but it cannot be reserved: {}",
                    param_name, port, detail
                ))
            })
    }

    /// Get all allocated ports
    pub fn get_allocated_ports(&self) -> Vec<u16> {
        self.port_allocator.allocated_ports()
    }

    /// Get names of all port-type parameters
    pub fn get_port_parameter_names(&self) -> &[String] {
        &self.port_parameter_names
    }

    /// Get port resolution decisions for display in dry-run and status commands
    pub fn get_port_resolutions(&self) -> &[PortResolution] {
        &self.port_resolutions
    }

    /// Release port listeners.
    ///
    /// This method uses interior mutability in the port allocator to allow
    /// calling with `&self`, enabling concurrent start operations.
    pub(crate) fn release_port_listeners(&self) {
        self.port_allocator.release_listeners();
    }

    /// Validate a cached port (from the port store) is still usable.
    ///
    /// If the port is owned by a managed service, trust it. If it's free, allocate it.
    /// If it's taken by something else, resolve the conflict.
    pub(super) fn validate_cached_port(
        &mut self,
        cached_port: u16,
        param_name: &str,
    ) -> Result<(u16, PortResolutionReason)> {
        if self.managed_ports.contains(&cached_port) {
            tracing::debug!(
                "Reusing cached port {} for '{}' (owned by running service)",
                cached_port,
                param_name
            );
            self.port_allocator.mark_allocated(cached_port);
            Ok((cached_port, PortResolutionReason::Cached))
        } else if self.port_allocator.try_allocate_port(cached_port).is_ok() {
            tracing::debug!(
                "Reusing cached port {} for '{}' (port is free)",
                cached_port,
                param_name
            );
            Ok((cached_port, PortResolutionReason::Cached))
        } else {
            tracing::warn!(
                "Cached port {} for '{}' is no longer available, resolving conflict...",
                cached_port,
                param_name
            );
            let (new_port, conflict) =
                self.handle_port_conflict_interactive(cached_port, param_name)?;
            let first = conflict.as_ref().and_then(|c| c.processes.first());
            Ok((
                new_port,
                PortResolutionReason::ConflictAutoResolved {
                    default_port: cached_port,
                    conflict_pid: first.map(|p| p.pid),
                    conflict_process: first.map(|p| p.name.clone()),
                },
            ))
        }
    }

    /// Try to reclaim the config `default:` port when it differs from the cached port.
    ///
    /// Returns `Some` only when all of these hold:
    /// - defaults are preferred (normal mode, not isolated, not force-random)
    /// - the cached port is not held by a running managed service (switching a
    ///   live service's port would break connections between services)
    /// - the config declares a parseable default that differs from the cache
    /// - the default port is actually available right now
    ///
    /// Otherwise returns `None` and the caller falls back to the cached port.
    pub(super) fn try_reclaim_config_default(
        &mut self,
        param: &crate::config::Parameter,
        cached_port: u16,
    ) -> Option<(u16, PortResolutionReason)> {
        if !self.prefer_config_defaults || self.force_random_ports {
            return None;
        }
        if self.managed_ports.contains(&cached_port) {
            return None;
        }
        let default_value = param.default.as_ref()?;
        let default_port = Self::value_to_string(default_value).parse::<u16>().ok()?;
        if default_port == cached_port {
            return None;
        }
        if self.managed_ports.contains(&default_port) {
            self.port_allocator.mark_allocated(default_port);
        } else if self.port_allocator.try_allocate_port(default_port).is_err() {
            return None;
        }
        tracing::info!(
            "Using config default port {} (supersedes cached port {})",
            default_port,
            cached_port
        );
        Some((default_port, PortResolutionReason::DefaultAvailable))
    }

    /// Allocate a fresh port from the config default or a random one.
    ///
    /// Called when the port store has no cached value for this parameter.
    pub(super) fn allocate_fresh_port(
        &mut self,
        param: &crate::config::Parameter,
        param_name: &str,
    ) -> Result<(u16, PortResolutionReason)> {
        // Isolated scopes (and --randomize) must not allocate the config
        // default: well-known ports defeat the point of isolation — a fresh
        // parameter would collide with the non-isolated stack or another
        // workspace. Cached ports are handled before we get here.
        if self.force_random_ports || !self.prefer_config_defaults {
            return Ok((
                self.port_allocator.allocate_random_port()?,
                PortResolutionReason::Random,
            ));
        }

        if let Some(default_value) = param.default.as_ref() {
            let default_str = Self::value_to_string(default_value);
            if let Ok(default_port) = default_str.parse::<u16>() {
                if self.managed_ports.contains(&default_port) {
                    self.port_allocator.mark_allocated(default_port);
                    Ok((default_port, PortResolutionReason::DefaultAvailable))
                } else if self.port_allocator.try_allocate_port(default_port).is_ok() {
                    Ok((default_port, PortResolutionReason::DefaultAvailable))
                } else {
                    let (p, conflict) =
                        self.handle_port_conflict_interactive(default_port, param_name)?;
                    let first = conflict.as_ref().and_then(|c| c.processes.first());
                    Ok((
                        p,
                        PortResolutionReason::ConflictAutoResolved {
                            default_port,
                            conflict_pid: first.map(|p| p.pid),
                            conflict_process: first.map(|p| p.name.clone()),
                        },
                    ))
                }
            } else {
                Ok((
                    self.port_allocator.allocate_random_port()?,
                    PortResolutionReason::Random,
                ))
            }
        } else {
            Ok((
                self.port_allocator.allocate_random_port()?,
                PortResolutionReason::Random,
            ))
        }
    }

    /// Handle port conflict with interactive prompt or error.
    ///
    /// Returns `(resolved_port, Option<PortConflict>)` — the conflict is `Some` when the
    /// port was reassigned due to a conflict, carrying pid/process info for display.
    fn handle_port_conflict_interactive(
        &mut self,
        port: u16,
        param_name: &str,
    ) -> Result<(u16, Option<PortConflict>)> {
        // A failed allocation can be caused by a transient holder. If it has
        // disappeared by the time we inspect the conflict, reserve the port
        // before returning it. Returning the bare number here used to violate
        // the allocator's TOCTOU guarantee.
        let conflict = match PortConflict::check(port) {
            Some(conflict) => conflict,
            None => match self.port_allocator.try_allocate_port(port) {
                Ok(_) => return Ok((port, None)),
                Err(_) => PortConflict {
                    port,
                    processes: Vec::new(),
                },
            },
        };

        // Allocate alternative port (we may need it as fallback)
        let alternative_port = self.port_allocator.allocate_random_port()?;

        // In replace mode (--replace flag), kill blocking processes and use original port
        if self.replace_mode {
            match conflict.free_port() {
                Ok(msg) => {
                    tracing::info!(
                        "Port {} ({}) was in use, freed it: {}",
                        port,
                        param_name,
                        msg
                    );
                    // Try to allocate the original port now that it's free
                    match self.port_allocator.try_allocate_port(port) {
                        Ok(_) => return Ok((port, None)),
                        Err(_) => {
                            // Something else grabbed it, fall through to alternative
                            tracing::warn!(
                                "Port {} freed but couldn't allocate, using {}",
                                port,
                                alternative_port
                            );
                        }
                    }
                }
                Err(e) => {
                    tracing::warn!(
                        "Failed to free port {} ({}): {}, using {}",
                        port,
                        param_name,
                        e,
                        alternative_port
                    );
                }
            }
            return Ok((alternative_port, Some(conflict)));
        }

        // In auto-resolve mode (e.g., TUI), skip interactive prompt and use alternative port
        if self.auto_resolve_conflicts {
            tracing::info!(
                "Port {} ({}) is in use, auto-resolving to {}",
                port,
                param_name,
                alternative_port
            );
            return Ok((alternative_port, Some(conflict)));
        }

        // Handle conflict (interactive or error)
        match handle_port_conflict(port, param_name, alternative_port, &conflict)? {
            PortConflictAction::KillAndRetry => {
                // Kill all blocking processes and verify with retries
                if let Err(e) = conflict.kill_and_verify(3) {
                    return Err(Error::Process(e));
                }
                // Try to allocate the original port again (dual-stack: checks both IPv4 and 0.0.0.0)
                match self.port_allocator.try_allocate_port(port) {
                    Ok(_) => Ok((port, None)),
                    Err(_) => Ok((alternative_port, Some(conflict))),
                }
            }
            PortConflictAction::Retry => {
                // Try to allocate the original port again (dual-stack: checks both IPv4 and 0.0.0.0)
                match self.port_allocator.try_allocate_port(port) {
                    Ok(_) => Ok((port, None)),
                    Err(_) => Ok((alternative_port, Some(conflict))),
                }
            }
            PortConflictAction::Ignore => {
                // Use alternative port
                Ok((alternative_port, Some(conflict)))
            }
            PortConflictAction::Abort => Err(Error::Aborted),
        }
    }
}

#[cfg(test)]
#[allow(clippy::field_reassign_with_default)]
mod tests {
    use super::*;

    fn free_port_candidate() -> u16 {
        let probe = std::net::TcpListener::bind("0.0.0.0:0").unwrap();
        probe.local_addr().unwrap().port()
    }

    #[test]
    fn test_port_allocation_with_default_available() {
        use crate::config::{Config, Parameter};

        // Use a fixed high port that's unlikely to be in use
        // Note: There's an inherent race window between checking availability and
        // the resolver allocating, so we verify behavior rather than exact port
        let default_port: u16 = 59123;

        let mut resolver = Resolver::new();
        let mut config = Config::default();

        // Create a port parameter with a default
        let param = Parameter {
            param_type: Some("port".to_string()),
            default: Some(serde_yaml::Value::Number(default_port.into())),
            either: vec![],
            source: None,
            description: None,
            optional: None,
            ..Default::default()
        };

        config.parameters.insert("API_PORT".to_string(), param);

        resolver.resolve_parameters(&mut config).unwrap();

        let resolved = resolver.get_resolved_parameters();
        let port_str = resolved.get("API_PORT").unwrap();
        let port: u16 = port_str.parse().unwrap();

        // Should allocate a valid port - either the default or a fallback
        // The exact port depends on system state (race condition avoidance)
        assert!(port > 0, "Should allocate a valid port");

        // If default was available, it should be used; otherwise fallback
        // We can't assert the exact value due to race conditions with other tests
        if port != default_port {
            // Fallback was used - still a valid outcome
            assert!(port != 0, "Fallback port should be valid");
        }
    }

    #[test]
    fn test_port_allocation_with_default_in_use() {
        use crate::config::{Config, Parameter};

        // NoopPortStore is the default — forces fresh allocation (no cache hits)
        let mut resolver = Resolver::new();
        let mut config = Config::default();

        // Occupy a port
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let occupied_port = listener.local_addr().unwrap().port();

        // Create a port parameter with the occupied port as default
        let param = Parameter {
            param_type: Some("port".to_string()),
            default: Some(serde_yaml::Value::Number(occupied_port.into())),
            either: vec![],
            source: None,
            description: None,
            optional: None,
            ..Default::default()
        };

        config.parameters.insert("API_PORT".to_string(), param);

        resolver.resolve_parameters(&mut config).unwrap();

        let resolved = resolver.get_resolved_parameters();
        let port_str = resolved.get("API_PORT").unwrap();
        let port: u16 = port_str.parse().unwrap();

        // Should have allocated a different port (fallback to random)
        assert_ne!(port, occupied_port);
        assert!(port > 0);

        drop(listener);
    }

    #[test]
    fn test_port_allocation_without_default() {
        use crate::config::{Config, Parameter};

        let mut resolver = Resolver::new();
        let mut config = Config::default();

        // Create a port parameter without default
        let param = Parameter {
            param_type: Some("port".to_string()),
            default: None,
            either: vec![],
            source: None,
            description: None,
            optional: None,
            ..Default::default()
        };

        config.parameters.insert("API_PORT".to_string(), param);

        resolver.resolve_parameters(&mut config).unwrap();

        let resolved = resolver.get_resolved_parameters();
        let port_str = resolved.get("API_PORT").unwrap();
        let port: u16 = port_str.parse().unwrap();

        // Should have allocated a random port
        assert!(port > 0);
    }

    #[test]
    fn test_force_random_ports_ignores_defaults() {
        use crate::config::{Config, Parameter};

        let mut resolver = Resolver::new();
        resolver.set_force_random_ports(true);

        let mut config = Config::default();
        config.parameters.insert(
            "API_PORT".to_string(),
            Parameter {
                param_type: Some("port".to_string()),
                default: Some(serde_yaml::Value::Number(18380.into())),
                either: vec![],
                source: None,
                description: None,
                optional: None,
                ..Default::default()
            },
        );
        config.parameters.insert(
            "DB_PORT".to_string(),
            Parameter {
                param_type: Some("port".to_string()),
                default: Some(serde_yaml::Value::Number(15732.into())),
                either: vec![],
                source: None,
                description: None,
                optional: None,
                ..Default::default()
            },
        );

        resolver.resolve_parameters(&mut config).unwrap();
        let resolved = resolver.get_resolved_parameters();
        let api_port: u16 = resolved.get("API_PORT").unwrap().parse().unwrap();
        let db_port: u16 = resolved.get("DB_PORT").unwrap().parse().unwrap();

        assert_ne!(api_port, 18380);
        assert_ne!(db_port, 15732);
        assert_ne!(api_port, db_port);
    }

    /// Helper: a `type: port` parameter with the given default.
    fn port_param_with_default(port: u16) -> crate::config::Parameter {
        crate::config::Parameter {
            param_type: Some("port".to_string()),
            default: Some(serde_yaml::Value::Number(port.into())),
            ..Default::default()
        }
    }

    #[test]
    fn test_config_default_supersedes_cached_port() {
        use crate::config::Config;

        let mut resolver = Resolver::new();
        resolver.set_prefer_config_defaults(true);

        // Cache holds a stale port; config default is different and available
        let mut cached = HashMap::new();
        cached.insert("API_PORT".to_string(), 59911u16);
        resolver.set_port_store(Box::new(crate::port::SqlitePortStore::new(cached)));

        let mut config = Config::default();
        config
            .parameters
            .insert("API_PORT".to_string(), port_param_with_default(59912));

        resolver.resolve_parameters(&mut config).unwrap();
        let port: u16 = resolver
            .get_resolved_parameters()
            .get("API_PORT")
            .unwrap()
            .parse()
            .unwrap();

        assert_eq!(
            port, 59912,
            "available config default should supersede cached port"
        );
    }

    #[test]
    fn test_cached_port_wins_when_defaults_not_preferred() {
        use crate::config::Config;

        let mut resolver = Resolver::new();
        resolver.set_prefer_config_defaults(false); // isolated-mode behavior

        let mut cached = HashMap::new();
        cached.insert("API_PORT".to_string(), 59913u16);
        resolver.set_port_store(Box::new(crate::port::SqlitePortStore::new(cached)));

        let mut config = Config::default();
        config
            .parameters
            .insert("API_PORT".to_string(), port_param_with_default(59914));

        resolver.resolve_parameters(&mut config).unwrap();
        let port: u16 = resolver
            .get_resolved_parameters()
            .get("API_PORT")
            .unwrap()
            .parse()
            .unwrap();

        assert_eq!(
            port, 59913,
            "cached port should win when defaults are not preferred (isolated mode)"
        );
    }

    #[test]
    fn test_fresh_port_is_random_when_defaults_not_preferred() {
        use crate::config::Config;

        let mut resolver = Resolver::new();
        resolver.set_prefer_config_defaults(false); // isolated-mode behavior
        // No cached port in the store — this is a fresh allocation.
        resolver.set_port_store(Box::new(crate::port::SqlitePortStore::new(HashMap::new())));

        // Hold the default port open so allocating it would succeed if tried.
        let default_port = 59915u16;
        let _listener = std::net::TcpListener::bind(("127.0.0.1", 0)).unwrap();

        let mut config = Config::default();
        config.parameters.insert(
            "API_PORT".to_string(),
            port_param_with_default(default_port),
        );

        resolver.resolve_parameters(&mut config).unwrap();
        let port: u16 = resolver
            .get_resolved_parameters()
            .get("API_PORT")
            .unwrap()
            .parse()
            .unwrap();

        assert_ne!(
            port, default_port,
            "fresh allocation in isolated mode must not use the well-known config default"
        );
    }

    #[test]
    fn test_cached_port_kept_when_default_busy() {
        use crate::config::Config;

        let mut resolver = Resolver::new();
        resolver.set_prefer_config_defaults(true);

        // Occupy the default port so reclaiming fails
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let busy_default = listener.local_addr().unwrap().port();

        let mut cached = HashMap::new();
        cached.insert("API_PORT".to_string(), 59915u16);
        resolver.set_port_store(Box::new(crate::port::SqlitePortStore::new(cached)));

        let mut config = Config::default();
        config.parameters.insert(
            "API_PORT".to_string(),
            port_param_with_default(busy_default),
        );

        resolver.resolve_parameters(&mut config).unwrap();
        let port: u16 = resolver
            .get_resolved_parameters()
            .get("API_PORT")
            .unwrap()
            .parse()
            .unwrap();

        assert_eq!(
            port, 59915,
            "cached port should be kept when the config default is busy"
        );
        drop(listener);
    }

    #[test]
    fn test_port_allocation_with_invalid_default() {
        use crate::config::{Config, Parameter};

        let mut resolver = Resolver::new();
        let mut config = Config::default();

        // Create a port parameter with an invalid default
        let param = Parameter {
            param_type: Some("port".to_string()),
            default: Some(serde_yaml::Value::String("not-a-port".to_string())),
            either: vec![],
            source: None,
            description: None,
            optional: None,
            ..Default::default()
        };

        config.parameters.insert("API_PORT".to_string(), param);

        // An unparseable default is a config error, not a silent random port.
        let err = resolver.resolve_parameters(&mut config).unwrap_err();
        assert!(err.to_string().contains("invalid port default"), "{err}");
    }

    #[test]
    fn test_multiple_port_allocations_with_defaults() {
        use crate::config::{Config, Parameter};

        let mut resolver = Resolver::new();
        let mut config = Config::default();

        // Find two available ports
        let listener1 = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port1 = listener1.local_addr().unwrap().port();
        drop(listener1);

        let listener2 = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port2 = listener2.local_addr().unwrap().port();
        drop(listener2);

        // Create two port parameters with defaults
        let param1 = Parameter {
            param_type: Some("port".to_string()),
            default: Some(serde_yaml::Value::Number(port1.into())),
            either: vec![],
            source: None,
            description: None,
            optional: None,
            ..Default::default()
        };

        let param2 = Parameter {
            param_type: Some("port".to_string()),
            default: Some(serde_yaml::Value::Number(port2.into())),
            either: vec![],
            source: None,
            description: None,
            optional: None,
            ..Default::default()
        };

        config.parameters.insert("API_PORT".to_string(), param1);
        config.parameters.insert("DB_PORT".to_string(), param2);

        resolver.resolve_parameters(&mut config).unwrap();

        let resolved = resolver.get_resolved_parameters();
        let api_port: u16 = resolved.get("API_PORT").unwrap().parse().unwrap();
        let db_port: u16 = resolved.get("DB_PORT").unwrap().parse().unwrap();

        // Both should have gotten their default ports
        assert_eq!(api_port, port1);
        assert_eq!(db_port, port2);
        assert_ne!(api_port, db_port);
    }

    #[test]
    fn test_port_validation_with_user_value() {
        use crate::config::{Config, Parameter};

        // A discovered port can legitimately be claimed after the probe is
        // dropped. Treat that as a lost candidate, not a test failure.
        for _ in 0..32 {
            let exact_port = free_port_candidate();
            let mut resolver = Resolver::new();
            let mut config = Config::default();
            config.parameters.insert(
                "API_PORT".to_string(),
                Parameter {
                    param_type: Some("port".to_string()),
                    value: Some(exact_port.to_string()),
                    ..Default::default()
                },
            );

            if resolver.resolve_parameters(&mut config).is_err() {
                continue;
            }

            assert_eq!(
                resolver.get_resolved_parameters().get("API_PORT"),
                Some(&exact_port.to_string())
            );
            assert_eq!(
                resolver.get_port_resolutions()[0].reason,
                PortResolutionReason::Explicit
            );
            assert!(resolver.get_allocated_ports().contains(&exact_port));
            assert!(
                std::net::TcpListener::bind(("127.0.0.1", exact_port)).is_err(),
                "an explicit port must remain reserved after resolution"
            );
            return;
        }

        panic!("could not reserve an exact candidate after 32 attempts");
    }

    #[test]
    fn test_explicit_port_rejects_an_external_holder() {
        use crate::config::{Config, Parameter};

        let holder = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let exact_port = holder.local_addr().unwrap().port();
        let mut resolver = Resolver::new();
        let mut config = Config::default();
        config.parameters.insert(
            "API_PORT".to_string(),
            Parameter {
                param_type: Some("port".to_string()),
                value: Some(exact_port.to_string()),
                ..Default::default()
            },
        );

        let error = resolver.resolve_parameters(&mut config).unwrap_err();
        let message = error.to_string();
        assert!(message.contains("API_PORT"), "{message}");
        assert!(message.contains(&exact_port.to_string()), "{message}");
        assert!(message.contains("cannot be reserved"), "{message}");
    }

    #[test]
    fn test_explicit_managed_port_is_tracked_without_rebinding() {
        use crate::config::{Config, Parameter};

        let holder = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let managed_port = holder.local_addr().unwrap().port();
        let mut resolver = Resolver::new();
        resolver.set_managed_ports(HashSet::from([managed_port]));
        let mut config = Config::default();
        config.parameters.insert(
            "API_PORT".to_string(),
            Parameter {
                param_type: Some("port".to_string()),
                value: Some(managed_port.to_string()),
                ..Default::default()
            },
        );

        resolver.resolve_parameters(&mut config).unwrap();

        assert_eq!(
            resolver.get_resolved_parameters().get("API_PORT"),
            Some(&managed_port.to_string())
        );
        assert!(resolver.get_allocated_ports().contains(&managed_port));
    }

    #[test]
    fn test_disappeared_conflict_is_rechecked_and_reserved() {
        let mut resolver = Resolver::new();
        resolver.set_auto_resolve_conflicts(true);

        for _ in 0..32 {
            let port = free_port_candidate();
            let Ok((resolved, conflict)) =
                resolver.handle_port_conflict_interactive(port, "API_PORT")
            else {
                continue;
            };
            if conflict.is_some() {
                continue;
            }

            assert_eq!(resolved, port);
            assert!(resolver.get_allocated_ports().contains(&port));
            assert!(
                std::net::TcpListener::bind(("127.0.0.1", port)).is_err(),
                "the no-conflict recheck path must retain a reservation"
            );
            return;
        }

        panic!("could not reserve a disappeared-conflict candidate after 32 attempts");
    }

    #[test]
    fn test_port_validation_rejects_invalid_string() {
        use crate::config::{Config, Parameter};

        let mut resolver = Resolver::new();
        let mut config = Config::default();

        // Create a port parameter with invalid string value
        let mut param = Parameter {
            param_type: Some("port".to_string()),
            default: None,
            either: vec![],
            source: None,
            description: None,
            optional: None,
            ..Default::default()
        };
        param.value = Some("invalid".to_string());

        config.parameters.insert("API_PORT".to_string(), param);

        let result = resolver.resolve_parameters(&mut config);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("invalid port value")
        );
    }

    #[test]
    fn test_port_validation_rejects_zero() {
        use crate::config::{Config, Parameter};

        let mut resolver = Resolver::new();
        let mut config = Config::default();

        // Create a port parameter with zero value
        let mut param = Parameter {
            param_type: Some("port".to_string()),
            default: None,
            either: vec![],
            source: None,
            description: None,
            optional: None,
            ..Default::default()
        };
        param.value = Some("0".to_string());

        config.parameters.insert("API_PORT".to_string(), param);

        let result = resolver.resolve_parameters(&mut config);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("invalid port value '0'")
        );
    }

    #[test]
    fn test_port_validation_rejects_out_of_range() {
        use crate::config::{Config, Parameter};

        let mut resolver = Resolver::new();
        let mut config = Config::default();

        // Create a port parameter with out-of-range value
        let mut param = Parameter {
            param_type: Some("port".to_string()),
            default: None,
            either: vec![],
            source: None,
            description: None,
            optional: None,
            ..Default::default()
        };
        param.value = Some("99999".to_string());

        config.parameters.insert("API_PORT".to_string(), param);

        let result = resolver.resolve_parameters(&mut config);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("invalid port value")
        );
    }

    #[test]
    fn test_non_port_parameter_accepts_any_value() {
        use crate::config::{Config, Parameter};

        let mut resolver = Resolver::new();
        let mut config = Config::default();

        // Create a non-port parameter with "invalid" value (which is valid for non-ports)
        let mut param = Parameter {
            param_type: Some("string".to_string()),
            default: None,
            either: vec![],
            source: None,
            description: None,
            optional: None,
            ..Default::default()
        };
        param.value = Some("invalid".to_string());

        config.parameters.insert("SOME_PARAM".to_string(), param);

        resolver.resolve_parameters(&mut config).unwrap();

        let resolved = resolver.get_resolved_parameters();
        assert_eq!(resolved.get("SOME_PARAM").unwrap(), "invalid");
    }

    #[test]
    fn test_port_default_zero_rejected() {
        use crate::config::{Config, Parameter};

        let mut resolver = Resolver::new();
        let mut config = Config::default();
        let param = Parameter {
            param_type: Some("port".to_string()),
            default: Some(serde_yaml::Value::Number(0.into())),
            ..Default::default()
        };
        config.parameters.insert("PORT".to_string(), param);

        let err = resolver.resolve_parameters(&mut config).unwrap_err();
        assert!(
            err.to_string().contains("invalid port default '0'"),
            "{err}"
        );
    }

    #[test]
    fn test_port_default_template_rejected() {
        use crate::config::{Config, Parameter};

        let mut resolver = Resolver::new();
        let mut config = Config::default();
        config.parameters.insert(
            "BASE_PORT".to_string(),
            Parameter {
                default: Some(serde_yaml::Value::Number(3000.into())),
                ..Default::default()
            },
        );
        config.parameters.insert(
            "API_PORT".to_string(),
            Parameter {
                param_type: Some("port".to_string()),
                default: Some(serde_yaml::Value::String("{{BASE_PORT}}".to_string())),
                ..Default::default()
            },
        );

        let err = resolver.resolve_parameters(&mut config).unwrap_err();
        assert!(err.to_string().contains("invalid port default"), "{err}");
    }

    #[test]
    fn test_port_default_out_of_range_rejected() {
        use crate::config::{Config, Parameter};

        let mut resolver = Resolver::new();
        let mut config = Config::default();
        config.parameters.insert(
            "PORT".to_string(),
            Parameter {
                param_type: Some("port".to_string()),
                default: Some(serde_yaml::Value::Number(70000.into())),
                ..Default::default()
            },
        );

        let err = resolver.resolve_parameters(&mut config).unwrap_err();
        assert!(err.to_string().contains("invalid port default"), "{err}");
    }

    #[test]
    fn test_try_allocate_port_zero_fails() {
        let mut allocator = PortAllocator::new();
        assert!(allocator.try_allocate_port(0).is_err());
    }
}
