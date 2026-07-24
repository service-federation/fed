use crate::error::{Error, Result};
use parking_lot::Mutex;
use std::collections::HashMap;
use std::net::TcpListener;

/// Port allocator for dynamically assigning available ports
///
/// Every allocated port has a corresponding entry in `allocations`, whose
/// state records whether fed still owns a reservation, trusts an already
/// running managed service, or deliberately released the reservation for
/// process startup.
///
/// # Thread Safety
///
/// All methods that modify state use interior mutability through Mutex guards,
/// allowing the allocator to be used from multiple threads safely. Methods that
/// only need `&self` can still modify internal state through the Mutex.
pub struct PortAllocator {
    allocations: Mutex<HashMap<u16, PortAllocation>>,
}

enum PortAllocation {
    Reserved(Vec<TcpListener>),
    Managed,
    Released,
}

impl PortAllocator {
    pub fn new() -> Self {
        Self {
            allocations: Mutex::new(HashMap::new()),
        }
    }

    /// Allocate a random available port
    ///
    /// Thread-safe: Uses interior mutability to allow concurrent allocation.
    pub fn allocate_random_port(&mut self) -> Result<u16> {
        // Let the OS pick and reserve a port on the v4 wildcard. Keep that
        // listener alive while checking loopback so there is no probe/drop/
        // rebind gap in which another process can take the candidate.
        for _ in 0..16 {
            let listener_any = TcpListener::bind("0.0.0.0:0").map_err(|e| {
                Error::PortAllocation(format!("Failed to bind to random port: {}", e))
            })?;
            if let Ok(port) = self.reserve_bound_wildcard(listener_any) {
                return Ok(port);
            }
        }
        Err(Error::PortAllocation(
            "Failed to find an available random port after 16 attempts".to_string(),
        ))
    }

    /// Allocate a port, preferring the default port if available, otherwise allocating a random port.
    ///
    /// Checks both 127.0.0.1 and 0.0.0.0 to detect dual-stack conflicts.
    ///
    /// Thread-safe: Uses interior mutability to allow concurrent allocation.
    pub fn allocate_port_with_default(&mut self, default_port: u16) -> Result<u16> {
        // Binding port 0 always succeeds (the OS picks an ephemeral port), so it
        // can never be honored as a literal default.
        if default_port == 0 {
            return self.allocate_random_port();
        }
        match self.try_allocate_port(default_port) {
            Ok(port) => Ok(port),
            Err(_) => self.allocate_random_port(),
        }
    }

    /// Try to allocate a specific port, keeping listeners alive to prevent TOCTOU races.
    /// Returns Ok(port) if successful, Err if port is unavailable.
    ///
    /// Checks both 127.0.0.1 and 0.0.0.0 to detect IPv6/dual-stack conflicts.
    /// A process binding `:::PORT` (IPv6 all interfaces) won't conflict with an
    /// IPv4-only check, so we must check both to match `PortConflict::check` behavior.
    ///
    /// Thread-safe: Uses interior mutability to allow concurrent allocation.
    pub fn try_allocate_port(&mut self, port: u16) -> Result<u16> {
        // bind(0) always succeeds by assigning an ephemeral port, which would
        // make this function report success for a port nothing can listen on.
        if port == 0 {
            return Err(Error::PortAllocation(
                "Port 0 cannot be allocated: valid ports are 1-65535".to_string(),
            ));
        }
        if self.allocations.lock().contains_key(&port) {
            return Err(Error::PortAllocation(format!(
                "Port {} is already allocated",
                port
            )));
        }
        // The wildcard bind is the load-bearing conflict check and is bound
        // FIRST. It is MANDATORY: with SO_REUSEADDR (which std sets), a
        // loopback bind succeeds alongside a dual-stack [::]:PORT listener —
        // every node/Next.js dev server — while the wildcard bind correctly
        // fails. Swallowing this failure used to let fed hand out ports that
        // dual-stack listeners already held.
        let listener_any = TcpListener::bind(("0.0.0.0", port)).map_err(|e| {
            Error::PortAllocation(format!("Port {} not available (0.0.0.0): {}", port, e))
        })?;
        self.reserve_bound_wildcard(listener_any)
    }

    /// Finish reserving a port whose wildcard listener is already held.
    ///
    /// Keeping `listener_any` alive while attempting the loopback bind is what
    /// makes random allocation atomic from the allocator's point of view.
    fn reserve_bound_wildcard(&mut self, listener_any: TcpListener) -> Result<u16> {
        let port = listener_any
            .local_addr()
            .map_err(|e| Error::PortAllocation(format!("Failed to get local address: {}", e)))?
            .port();
        if self.allocations.lock().contains_key(&port) {
            return Err(Error::PortAllocation(format!(
                "Port {} is already allocated",
                port
            )));
        }

        // Also hold loopback (TOCTOU guard: on BSD kernels a 127.0.0.1 bind
        // coexists with our wildcard, so without this a squatter could still
        // steal the port before the service starts).
        let listener_lo = match TcpListener::bind(("127.0.0.1", port)) {
            Ok(l) => Some(l),
            Err(e) => {
                if cfg!(any(
                    target_os = "macos",
                    target_os = "freebsd",
                    target_os = "openbsd",
                    target_os = "netbsd",
                    target_os = "dragonfly"
                )) {
                    // Coexistence is allowed here, so this failure means a
                    // real loopback-only holder (e.g. Vite's default bind).
                    return Err(Error::PortAllocation(format!(
                        "Port {} not available (127.0.0.1): {}",
                        port, e
                    )));
                }
                // Linux/Windows: our own wildcard listener blocks the
                // loopback bind, and the wildcard bind above already proved
                // nobody else holds any v4 address on this port.
                None
            }
        };

        let mut listeners = vec![listener_any];
        if let Some(l) = listener_lo {
            listeners.push(l);
        }
        self.allocations
            .lock()
            .insert(port, PortAllocation::Reserved(listeners));
        Ok(port)
    }

    /// Mark a port as allocated without binding a listener.
    ///
    /// Used for ports already held by managed services — we trust the port is
    /// occupied by us and don't need to bind-check it.
    pub fn mark_allocated(&mut self, port: u16) {
        self.allocations
            .lock()
            .entry(port)
            .or_insert(PortAllocation::Managed);
    }

    /// Release all listeners but keep ports marked as allocated.
    ///
    /// This method uses interior mutability (via Mutex) to allow calling with `&self`,
    /// which is essential for concurrent start operations where we need to release
    /// port listeners without holding exclusive access to the entire orchestrator.
    pub fn release_listeners(&self) {
        for allocation in self.allocations.lock().values_mut() {
            if let PortAllocation::Reserved(listeners) = allocation {
                listeners.clear();
                *allocation = PortAllocation::Released;
            }
        }
    }

    /// Release all allocated resources
    ///
    /// Thread-safe: Uses interior mutability to allow concurrent cleanup.
    pub fn release_all(&mut self) {
        self.allocations.lock().clear();
    }

    /// Release listeners only (for cleanup with &self)
    ///
    /// Note: This only releases the listeners, not the allocated_ports set.
    /// Use this when you only have &self access.
    pub fn release_listeners_for_cleanup(&self) {
        self.release_listeners();
    }

    /// Get all allocated ports
    ///
    /// Returns a copy of the allocated ports to avoid holding the lock.
    /// Thread-safe: Uses interior mutability for concurrent access.
    pub fn allocated_ports(&self) -> Vec<u16> {
        self.allocations.lock().keys().copied().collect()
    }
}

impl Default for PortAllocator {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for PortAllocator {
    fn drop(&mut self) {
        // Note: release_all takes &mut self which we have in Drop
        self.release_all();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Regression test: a node/Next.js dev server binds the IPv6 dual-stack
    /// wildcard ([::]). With SO_REUSEADDR a loopback bind still succeeds
    /// alongside it on macOS, so only the (previously swallowed) wildcard
    /// bind reveals the conflict. fed used to hand out such ports and the
    /// service then crashed with EADDRINUSE.
    #[test]
    #[cfg(unix)]
    fn test_try_allocate_rejects_dual_stack_v6_listener() {
        let v6_holder = TcpListener::bind("[::]:0").unwrap();
        let port = v6_holder.local_addr().unwrap().port();

        let mut allocator = PortAllocator::new();
        assert!(
            allocator.try_allocate_port(port).is_err(),
            "port {} is held by a [::] dual-stack listener and must not be allocated",
            port
        );
    }

    /// A server bound only to 127.0.0.1 (e.g. Vite's default) must also be
    /// detected — the loopback probe covers what the wildcard bind can miss
    /// on macOS.
    #[test]
    fn test_try_allocate_rejects_loopback_only_listener() {
        let lo_holder = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = lo_holder.local_addr().unwrap().port();

        let mut allocator = PortAllocator::new();
        assert!(
            allocator.try_allocate_port(port).is_err(),
            "port {} is held by a loopback-only listener and must not be allocated",
            port
        );
    }

    /// allocate_port_with_default falls back to a random port instead of
    /// failing when the default is held by a dual-stack [::] listener.
    #[test]
    #[cfg(unix)]
    fn test_default_held_by_v6_listener_falls_back_to_random() {
        let v6_holder = TcpListener::bind("[::]:0").unwrap();
        let taken = v6_holder.local_addr().unwrap().port();

        let mut allocator = PortAllocator::new();
        let port = allocator.allocate_port_with_default(taken).unwrap();
        assert_ne!(port, taken, "must not allocate the held default");
    }

    #[test]
    fn test_allocate_random_port() {
        let mut allocator = PortAllocator::new();
        let port = allocator.allocate_random_port().unwrap();

        assert!(port > 0);
        assert!(allocator.allocated_ports().contains(&port));
    }

    #[test]
    fn test_multiple_allocations() {
        let mut allocator = PortAllocator::new();
        let port1 = allocator.allocate_random_port().unwrap();
        let port2 = allocator.allocate_random_port().unwrap();

        assert_ne!(port1, port2);
        assert_eq!(allocator.allocated_ports().len(), 2);
    }

    #[test]
    fn test_release_listeners() {
        let mut allocator = PortAllocator::new();
        let port = allocator.allocate_random_port().unwrap();

        allocator.release_listeners();

        // Port should still be in allocated set
        assert!(allocator.allocated_ports().contains(&port));
        // But its reservation should be cleared
        assert!(matches!(
            allocator.allocations.lock().get(&port),
            Some(PortAllocation::Released)
        ));
    }

    #[test]
    fn test_allocate_port_with_default_available() {
        let mut allocator = PortAllocator::new();

        // Use a high port that's likely available.
        // We can't assert we get exactly this port due to TOCTOU -
        // another process could grab it between our check and bind.
        // The allocator correctly falls back to a random port if needed.
        let preferred_port = 59123;

        let port = allocator
            .allocate_port_with_default(preferred_port)
            .unwrap();

        // Verify we got a valid port and it's properly tracked
        assert!(port > 0);
        assert!(allocator.allocated_ports().contains(&port));
        // Listener is held to prevent TOCTOU until release_listeners() is called
        assert!(matches!(
            allocator.allocations.lock().get(&port),
            Some(PortAllocation::Reserved(listeners)) if !listeners.is_empty()
        ));
    }

    #[test]
    fn test_allocate_port_with_default_in_use() {
        let mut allocator = PortAllocator::new();

        // Occupy a port
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let occupied_port = listener.local_addr().unwrap().port();

        // Try to allocate with that port as default
        let port = allocator.allocate_port_with_default(occupied_port).unwrap();

        // Should get a different port (random fallback)
        assert_ne!(port, occupied_port);
        assert!(allocator.allocated_ports().contains(&port));
        assert_eq!(allocator.allocated_ports().len(), 1);

        drop(listener);
    }

    #[test]
    fn test_allocate_port_with_default_multiple() {
        let mut allocator = PortAllocator::new();

        // Allocate first port with default
        let listener1 = TcpListener::bind("127.0.0.1:0").unwrap();
        let default1 = listener1.local_addr().unwrap().port();
        drop(listener1);

        let port1 = allocator.allocate_port_with_default(default1).unwrap();
        assert_eq!(port1, default1);

        // Try to allocate the same default port again (should fail and get random)
        let port2 = allocator.allocate_port_with_default(default1).unwrap();
        assert_ne!(port2, default1);
        assert_ne!(port1, port2);

        assert_eq!(allocator.allocated_ports().len(), 2);
    }

    #[test]
    fn test_allocate_port_with_default_common_ports() {
        let mut allocator = PortAllocator::new();

        // Checking availability of well-known ports (8080, 3000, 5432) and
        // then asserting on the result is a TOCTOU race against every other
        // parallel test and whatever runs on the machine. Instead, make each
        // case deterministic by owning the port state ourselves.

        // Case 1: default is genuinely taken — we hold the listener, so the
        // allocator must fall back to a different port.
        let held = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let taken_port = held.local_addr().unwrap().port();
        let fallback = allocator.allocate_port_with_default(taken_port).unwrap();
        assert_ne!(fallback, taken_port);
        assert!(allocator.allocated_ports().contains(&fallback));
        drop(held);

        // Case 2: default is free — bind an ephemeral port to discover a free
        // one, release it, and allocate immediately. The race window here is
        // microseconds against the ephemeral range, not a well-known port.
        let probe = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let free_port = probe.local_addr().unwrap().port();
        drop(probe);
        let got = allocator.allocate_port_with_default(free_port).unwrap();
        assert_eq!(got, free_port);
        assert!(allocator.allocated_ports().contains(&got));

        // Both allocations are tracked and distinct.
        assert_eq!(allocator.allocated_ports().len(), 2);
    }

    #[test]
    fn test_thread_safety_allocated_ports() {
        use std::sync::Arc;
        use std::thread;

        // Create a shared allocator (wrapped in Arc for sharing)
        // Note: In real use, the allocator is behind &mut self methods,
        // but the interior mutability allows safe concurrent reads
        let allocator = Arc::new(parking_lot::Mutex::new(PortAllocator::new()));

        // Spawn multiple threads to allocate ports concurrently
        let mut handles = vec![];
        for _ in 0..4 {
            let alloc = Arc::clone(&allocator);
            handles.push(thread::spawn(move || {
                let mut guard = alloc.lock();
                guard.allocate_random_port().unwrap()
            }));
        }

        // Collect all allocated ports
        let mut ports: Vec<u16> = handles.into_iter().map(|h| h.join().unwrap()).collect();

        // All ports should be unique
        ports.sort();
        ports.dedup();
        assert_eq!(ports.len(), 4, "All allocated ports should be unique");

        // Verify all ports are tracked
        let final_ports = allocator.lock().allocated_ports();
        assert_eq!(final_ports.len(), 4);
        for port in &ports {
            assert!(final_ports.contains(port));
        }
    }
}
