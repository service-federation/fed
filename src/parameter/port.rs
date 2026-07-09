use crate::error::{Error, Result};
use parking_lot::Mutex;
use std::collections::HashSet;
use std::net::TcpListener;

/// Port allocator for dynamically assigning available ports
///
/// Uses interior mutability via Mutex for both `allocated_ports` and `listeners`
/// to enable concurrent access patterns. This makes the allocator fully thread-safe.
///
/// # Thread Safety
///
/// All methods that modify state use interior mutability through Mutex guards,
/// allowing the allocator to be used from multiple threads safely. Methods that
/// only need `&self` can still modify internal state through the Mutex.
pub struct PortAllocator {
    /// Set of allocated ports, protected by Mutex for thread-safe access
    allocated_ports: Mutex<HashSet<u16>>,
    /// Listeners are wrapped in Mutex to allow release_listeners() to work with &self.
    /// This is important because releasing listeners needs to happen during start operations
    /// which may be called concurrently.
    listeners: Mutex<Vec<TcpListener>>,
}

impl PortAllocator {
    pub fn new() -> Self {
        Self {
            allocated_ports: Mutex::new(HashSet::new()),
            listeners: Mutex::new(Vec::new()),
        }
    }

    /// Allocate a random available port
    ///
    /// Thread-safe: Uses interior mutability to allow concurrent allocation.
    pub fn allocate_random_port(&mut self) -> Result<u16> {
        // Bind to port 0 to let the OS assign an available port
        let listener_v4 = TcpListener::bind("127.0.0.1:0")
            .map_err(|e| Error::PortAllocation(format!("Failed to bind to random port: {}", e)))?;

        let port = listener_v4
            .local_addr()
            .map_err(|e| Error::PortAllocation(format!("Failed to get local address: {}", e)))?
            .port();

        // Also try 0.0.0.0 to catch dual-stack conflicts (e.g. :::PORT).
        // On Linux, this may fail with EADDRINUSE because the kernel treats
        // 127.0.0.1:PORT as overlapping with 0.0.0.0:PORT — that's fine,
        // the 127.0.0.1 bind already reserves it.
        let listener_any = TcpListener::bind(("0.0.0.0", port)).ok();

        let mut listeners = self.listeners.lock();
        listeners.push(listener_v4);
        if let Some(l) = listener_any {
            listeners.push(l);
        }
        self.allocated_ports.lock().insert(port);

        Ok(port)
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
        // Try to bind to the default port on 127.0.0.1
        let listener_v4 = match TcpListener::bind(("127.0.0.1", default_port)) {
            Ok(l) => l,
            Err(_) => return self.allocate_random_port(),
        };
        // Also try 0.0.0.0 to catch dual-stack conflicts.
        // On Linux this may fail because 127.0.0.1 already covers it — that's fine.
        // On macOS the two binds coexist, so holding both prevents conflicts from
        // processes binding on either address.
        let listener_any = TcpListener::bind(("0.0.0.0", default_port)).ok();

        let mut listeners = self.listeners.lock();
        listeners.push(listener_v4);
        if let Some(l) = listener_any {
            listeners.push(l);
        }
        self.allocated_ports.lock().insert(default_port);
        Ok(default_port)
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
        let listener_v4 = TcpListener::bind(("127.0.0.1", port)).map_err(|e| {
            Error::PortAllocation(format!("Port {} not available (127.0.0.1): {}", port, e))
        })?;
        // Also try 0.0.0.0 to catch dual-stack conflicts.
        // On Linux this may fail because 127.0.0.1 already covers it — that's fine.
        let listener_any = TcpListener::bind(("0.0.0.0", port)).ok();

        let mut listeners = self.listeners.lock();
        listeners.push(listener_v4);
        if let Some(l) = listener_any {
            listeners.push(l);
        }
        self.allocated_ports.lock().insert(port);
        Ok(port)
    }

    /// Mark a port as allocated without binding a listener.
    ///
    /// Used for ports already held by managed services — we trust the port is
    /// occupied by us and don't need to bind-check it.
    pub fn mark_allocated(&mut self, port: u16) {
        self.allocated_ports.lock().insert(port);
    }

    /// Release all listeners but keep ports marked as allocated.
    ///
    /// This method uses interior mutability (via Mutex) to allow calling with `&self`,
    /// which is essential for concurrent start operations where we need to release
    /// port listeners without holding exclusive access to the entire orchestrator.
    pub fn release_listeners(&self) {
        self.listeners.lock().clear();
    }

    /// Release all allocated resources
    ///
    /// Thread-safe: Uses interior mutability to allow concurrent cleanup.
    pub fn release_all(&mut self) {
        self.listeners.lock().clear();
        self.allocated_ports.lock().clear();
    }

    /// Release listeners only (for cleanup with &self)
    ///
    /// Note: This only releases the listeners, not the allocated_ports set.
    /// Use this when you only have &self access.
    pub fn release_listeners_for_cleanup(&self) {
        self.listeners.lock().clear();
    }

    /// Get all allocated ports
    ///
    /// Returns a copy of the allocated ports to avoid holding the lock.
    /// Thread-safe: Uses interior mutability for concurrent access.
    pub fn allocated_ports(&self) -> Vec<u16> {
        self.allocated_ports.lock().iter().copied().collect()
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
        // But listeners should be cleared
        assert!(allocator.listeners.lock().is_empty());
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
        assert!(!allocator.listeners.lock().is_empty());
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

        // Common ports that are likely in use (8080, 3000, 5432)
        // These should fallback to random ports
        let ports_to_try = vec![8080, 3000, 5432];
        let mut allocated = vec![];

        for default_port in ports_to_try {
            // First check if the port is available
            let is_available = crate::port::PortConflict::is_port_available(default_port);

            let port = allocator.allocate_port_with_default(default_port).unwrap();
            allocated.push(port);

            if is_available {
                // If port was available, we should have gotten it
                assert_eq!(port, default_port);
            }
            // Port should be tracked
            assert!(allocator.allocated_ports().contains(&port));
        }

        // All allocated ports should be unique
        allocated.sort();
        allocated.dedup();
        assert_eq!(allocated.len(), 3);
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
