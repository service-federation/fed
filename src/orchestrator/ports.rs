//! Port management utilities for the orchestrator.
//!
//! This module contains port-related operations extracted from the main
//! Orchestrator: collecting managed ports from running services and
//! releasing port listeners (TOCTOU race prevention).
//!
//! # Why free functions instead of a runner struct
//!
//! Other extracted modules (scripts, orphans, health) use a runner struct
//! that borrows `&Orchestrator`. Port management can't follow this pattern
//! because `collect_managed_ports` needs `&mut Resolver` — and since `Resolver`
//! is owned by `Orchestrator`, you can't borrow `&Orchestrator` (immutable)
//! and `&mut orchestrator.resolver` (mutable) at the same time. Free functions
//! that take individual fields sidestep this borrow conflict.

use crate::parameter::Resolver;
use crate::service::Status;
use crate::state::StateTracker;
use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::sync::RwLock;

/// Collect ports owned by running managed services so the resolver can
/// avoid re-allocating them.
///
/// Sources checked (in order):
/// 1. Per-service `port_allocations` from state DB
/// 2. Global port resolutions persisted by previous `fed start`
///
/// If ANY service is alive, all globally persisted ports are trusted as managed.
/// This handles process services which don't write to `port_allocations`.
pub(super) async fn collect_managed_ports(
    resolver: &mut Resolver,
    state_tracker: &Arc<RwLock<StateTracker>>,
    isolation_id: Option<&str>,
) {
    let state = state_tracker.read().await;
    let services = state.get_services().await;
    let mut managed_ports = HashSet::new();
    let mut has_live_service = false;

    for svc in services.values() {
        if !matches!(svc.status, Status::Running | Status::Healthy) {
            continue;
        }

        // Verify the process/container is actually alive before trusting the port
        let is_alive = if let Some(pid) = svc.pid {
            super::core::is_pid_alive(pid)
        } else if svc.container_id.is_some() {
            // Can't cheaply verify container without Docker — trust the status.
            // If Docker is down, mark_dead_services will skip container checks anyway.
            true
        } else {
            false
        };

        if is_alive {
            has_live_service = true;
            for &port in svc.port_allocations.values() {
                managed_ports.insert(port);
            }
        }
    }

    // If any real service is alive, trust globally persisted port resolutions.
    // These cover all port parameters including process/Gradle services.
    if has_live_service {
        let global_ports = state.get_global_port_allocations(isolation_id).await;
        for port in global_ports.values() {
            managed_ports.insert(*port);
        }
    }

    if !managed_ports.is_empty() {
        tracing::debug!(
            "Found {} managed port(s) from running services: {:?}",
            managed_ports.len(),
            managed_ports
        );
    }
    // Must drop the state read lock before mutating resolver
    drop(state);
    resolver.set_managed_ports(managed_ports);
}

/// Release port listeners exactly once (idempotent via atomic CAS).
///
/// After port resolution, the resolver holds `TcpListener`s on allocated ports
/// to prevent TOCTOU races. This function releases them just before services
/// bind, minimising the race window.
pub(super) fn release_port_listeners_once(
    port_listeners_released: &AtomicBool,
    resolver: &Resolver,
) {
    if port_listeners_released
        .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
        .is_ok()
    {
        resolver.release_port_listeners();
    }
}
