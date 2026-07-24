//! Tests for port allocation TOCTOU (Time-of-Check-Time-of-Use) race condition prevention
//!
//! These tests verify that the PortAllocator correctly prevents race conditions by
//! keeping TcpListeners alive until the startup handoff begins, preventing other
//! processes from stealing ports during allocation and configuration resolution.

use fed::parameter::PortAllocator;
use std::net::TcpListener;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;

// =============================================================================
// Basic try_allocate_port Tests
// =============================================================================

#[test]
fn test_try_allocate_port_success() {
    let mut allocator = PortAllocator::new();

    // Find an available port
    let listener = TcpListener::bind("127.0.0.1:0").expect("Bind to get free port");
    let port = listener.local_addr().unwrap().port();
    drop(listener); // Release it

    // Now try to allocate it
    let result = allocator.try_allocate_port(port);
    assert!(
        result.is_ok(),
        "Should successfully allocate available port"
    );
    assert_eq!(result.unwrap(), port);

    // Port should be in allocated set
    assert!(allocator.allocated_ports().contains(&port));
}

#[test]
fn test_try_allocate_port_already_in_use() {
    let mut allocator = PortAllocator::new();

    // Occupy a port
    let listener = TcpListener::bind("127.0.0.1:0").expect("Bind to occupy port");
    let occupied_port = listener.local_addr().unwrap().port();

    // Try to allocate the occupied port
    let result = allocator.try_allocate_port(occupied_port);
    assert!(result.is_err(), "Should fail for occupied port");

    // Error message should mention the port
    let err = result.unwrap_err();
    assert!(
        err.to_string().contains(&occupied_port.to_string())
            || err.to_string().contains("not available"),
        "Error should mention port: {}",
        err
    );

    drop(listener);
}

#[test]
fn test_try_allocate_port_keeps_listener_alive() {
    let mut allocator = PortAllocator::new();

    // Find and allocate a port
    let temp_listener = TcpListener::bind("127.0.0.1:0").expect("Bind");
    let port = temp_listener.local_addr().unwrap().port();
    drop(temp_listener);

    // Allocate with our allocator
    allocator.try_allocate_port(port).expect("Allocate");

    // Try to bind to the same port - should fail because allocator holds the listener
    let rebind_result = TcpListener::bind(("127.0.0.1", port));
    assert!(
        rebind_result.is_err(),
        "Should not be able to rebind - allocator holds the listener"
    );
}

#[test]
fn test_try_allocate_port_released_after_drop() {
    let port: u16;

    {
        let mut allocator = PortAllocator::new();

        // Find and allocate a port
        let temp_listener = TcpListener::bind("127.0.0.1:0").expect("Bind");
        port = temp_listener.local_addr().unwrap().port();
        drop(temp_listener);

        allocator.try_allocate_port(port).expect("Allocate");

        // Allocator goes out of scope and is dropped
    }

    // Now the port should be available again
    let rebind_result = TcpListener::bind(("127.0.0.1", port));
    assert!(
        rebind_result.is_ok(),
        "Port should be available after allocator drop"
    );
}

// =============================================================================
// TOCTOU Race Condition Prevention Tests
// =============================================================================

#[test]
fn test_toctou_prevention_single_allocator() {
    let mut allocator = PortAllocator::new();

    // Allocate a port
    let port = allocator.allocate_random_port().expect("Random port");

    // Simulate a racing process trying to grab the port
    let steal_result = TcpListener::bind(("127.0.0.1", port));
    assert!(
        steal_result.is_err(),
        "External process should not be able to steal allocated port"
    );
}

#[test]
fn test_toctou_prevention_vs_old_check_pattern() {
    // This demonstrates why the old pattern was vulnerable:
    //
    // OLD (vulnerable):
    //   if TcpListener::bind(port).is_ok() { Some(port) }
    //   // Listener dropped here - race window!
    //   // ... later use port
    //
    // NEW (safe):
    //   allocator.try_allocate_port(port)  // Keeps listener alive
    //   // ... later use port
    //   allocator.release_listeners()  // Release when service started

    let test_port: u16;

    // Find a free port
    {
        let listener = TcpListener::bind("127.0.0.1:0").expect("Bind");
        test_port = listener.local_addr().unwrap().port();
    }

    // OLD PATTERN (simulated) - shows the vulnerability
    let old_check_result = TcpListener::bind(("127.0.0.1", test_port));
    drop(old_check_result); // This was the bug - listener dropped immediately

    // Race window: another process could grab the port here
    // Simulate racing process
    let racer = TcpListener::bind(("127.0.0.1", test_port));
    if racer.is_ok() {
        // Race succeeded - this demonstrates the vulnerability
        println!(
            "OLD PATTERN: Port {} was stolen during race window",
            test_port
        );
    }
    drop(racer);

    // NEW PATTERN - no race window
    let new_port: u16;
    {
        let listener = TcpListener::bind("127.0.0.1:0").expect("Bind");
        new_port = listener.local_addr().unwrap().port();
    }

    let mut allocator = PortAllocator::new();
    allocator.try_allocate_port(new_port).expect("Allocate");

    // No race window - listener is held by allocator
    let racer2 = TcpListener::bind(("127.0.0.1", new_port));
    assert!(
        racer2.is_err(),
        "NEW PATTERN: Port should be protected from racing"
    );
}

#[test]
fn test_concurrent_allocation_attempts() {
    // Test that once a port is allocated, a concurrent attempt to grab it fails
    // This is a more deterministic test that verifies the core invariant

    let mut allocator = PortAllocator::new();

    // Allocate a port first (this holds the listener)
    let port = allocator.allocate_random_port().expect("Allocate");

    let port_stolen = Arc::new(AtomicBool::new(false));
    let port_stolen_clone = port_stolen.clone();

    // Spawn multiple threads trying to steal the allocated port
    let handles: Vec<_> = (0..5)
        .map(|_| {
            let stolen = port_stolen_clone.clone();
            thread::spawn(move || {
                let result = TcpListener::bind(("127.0.0.1", port));
                if result.is_ok() {
                    stolen.store(true, Ordering::SeqCst);
                }
                result.is_ok()
            })
        })
        .collect();

    // Wait for all threads
    let results: Vec<bool> = handles.into_iter().map(|h| h.join().unwrap()).collect();

    // None should succeed - the allocator holds the port
    assert!(
        results.iter().all(|&r| !r),
        "No thread should succeed in stealing an allocated port"
    );
    assert!(
        !port_stolen.load(Ordering::SeqCst),
        "Port should not have been stolen"
    );
}

// =============================================================================
// Multiple Port Allocation Tests
// =============================================================================

#[test]
fn test_multiple_random_allocations_retain_ownership() {
    let mut allocator = PortAllocator::new();

    // Ask the allocator to discover and retain ownership in one operation.
    // Probing loopback, dropping the probe, and later requiring a wildcard
    // bind made this test race macOS's ephemeral-port users.
    let ports: Vec<u16> = (0..5)
        .map(|_| allocator.allocate_random_port().expect("Allocate"))
        .collect();

    // All should be in allocated set
    for port in &ports {
        assert!(
            allocator.allocated_ports().contains(port),
            "Port {} should be tracked",
            port
        );
    }

    // None should be stealable
    for port in &ports {
        let steal = TcpListener::bind(("127.0.0.1", *port));
        assert!(steal.is_err(), "Port {} should not be stealable", port);
    }
}

#[test]
fn test_allocate_same_port_twice() {
    let mut allocator = PortAllocator::new();

    // Find a free port
    let listener = TcpListener::bind("127.0.0.1:0").expect("Bind");
    let port = listener.local_addr().unwrap().port();
    drop(listener);

    // First allocation should succeed
    let result1 = allocator.try_allocate_port(port);
    assert!(result1.is_ok(), "First allocation should succeed");

    // Second allocation of same port should fail (already held by allocator)
    let result2 = allocator.try_allocate_port(port);
    assert!(
        result2.is_err(),
        "Second allocation of same port should fail"
    );
}

// =============================================================================
// Release and Cleanup Tests
// =============================================================================

#[test]
fn test_release_listeners_frees_ports() {
    let mut allocator = PortAllocator::new();

    // Allocate a port
    let listener = TcpListener::bind("127.0.0.1:0").expect("Bind");
    let port = listener.local_addr().unwrap().port();
    drop(listener);

    allocator.try_allocate_port(port).expect("Allocate");

    // Can't steal while held
    assert!(TcpListener::bind(("127.0.0.1", port)).is_err());

    // Release listeners
    allocator.release_listeners();

    // Now port should be free (but still in allocated_ports set)
    let rebind = TcpListener::bind(("127.0.0.1", port));
    assert!(
        rebind.is_ok(),
        "Port should be free after release_listeners"
    );

    // Port still tracked as allocated (for reference)
    assert!(allocator.allocated_ports().contains(&port));
}

#[test]
fn test_release_all_clears_everything() {
    let mut allocator = PortAllocator::new();

    // Allocate several ports
    for _ in 0..3 {
        allocator.allocate_random_port().expect("Allocate");
    }

    assert_eq!(allocator.allocated_ports().len(), 3);

    // Release all
    allocator.release_all();

    assert_eq!(
        allocator.allocated_ports().len(),
        0,
        "All ports should be cleared"
    );
}

// =============================================================================
// Integration with allocate_port_with_default
// =============================================================================

#[test]
fn test_allocate_port_with_default_uses_toctou_safe_pattern() {
    let mut allocator = PortAllocator::new();

    // Find a free port to use as default
    let listener = TcpListener::bind("127.0.0.1:0").expect("Bind");
    let default_port = listener.local_addr().unwrap().port();
    drop(listener);

    // Allocate with default
    let result = allocator.allocate_port_with_default(default_port);
    assert!(result.is_ok(), "Should allocate default port");
    assert_eq!(result.unwrap(), default_port);

    // Port should be protected
    let steal = TcpListener::bind(("127.0.0.1", default_port));
    assert!(
        steal.is_err(),
        "Default port should be protected from theft"
    );
}

#[test]
fn test_allocate_port_with_default_fallback_also_protected() {
    let mut allocator = PortAllocator::new();

    // Occupy a port
    let blocker = TcpListener::bind("127.0.0.1:0").expect("Bind");
    let blocked_port = blocker.local_addr().unwrap().port();

    // Try to allocate with blocked port as default - should fallback
    let result = allocator.allocate_port_with_default(blocked_port);
    assert!(result.is_ok(), "Should fallback to random port");

    let allocated_port = result.unwrap();
    assert_ne!(
        allocated_port, blocked_port,
        "Should get different port than blocked one"
    );

    // Fallback port should also be protected
    let steal = TcpListener::bind(("127.0.0.1", allocated_port));
    assert!(steal.is_err(), "Fallback port should be protected");

    drop(blocker);
}

// =============================================================================
// Stress Tests
// =============================================================================

#[test]
fn test_rapid_allocation_and_release_cycles() {
    for _ in 0..10 {
        let mut allocator = PortAllocator::new();

        // Allocate several ports
        let mut allocated = Vec::new();
        for _ in 0..5 {
            let port = allocator.allocate_random_port().expect("Allocate");
            allocated.push(port);
        }

        // Verify all are protected
        for port in &allocated {
            assert!(TcpListener::bind(("127.0.0.1", *port)).is_err());
        }

        // Release
        allocator.release_all();

        // Ports should now be available
        for port in &allocated {
            let rebind = TcpListener::bind(("127.0.0.1", *port));
            // Port might be grabbed by OS for other purposes, so don't assert
            drop(rebind);
        }
    }
}

#[test]
fn test_many_sequential_allocations() {
    let mut allocator = PortAllocator::new();

    // Allocate many ports in sequence
    let mut ports = Vec::new();
    for _ in 0..20 {
        let port = allocator.allocate_random_port().expect("Allocate");
        ports.push(port);
    }

    // All should be unique
    ports.sort();
    ports.dedup();
    assert_eq!(ports.len(), 20, "All ports should be unique");

    // All should be tracked
    assert_eq!(allocator.allocated_ports().len(), 20);
}

// =============================================================================
// Edge Cases
// =============================================================================

#[test]
fn test_try_allocate_privileged_port() {
    let mut allocator = PortAllocator::new();

    // Port 80 typically requires root/admin
    let result = allocator.try_allocate_port(80);

    // Should either succeed (if running as root) or fail gracefully
    // Either way, it shouldn't panic
    match result {
        Ok(_) => println!("Successfully allocated port 80 (running as root?)"),
        Err(e) => {
            // Error message should be meaningful
            assert!(!e.to_string().is_empty(), "Error should have message");
        }
    }
}

#[test]
fn test_is_port_available_doesnt_hold_port() {
    // This verifies the static method doesn't prevent later allocation
    let listener = TcpListener::bind("127.0.0.1:0").expect("Bind");
    let port = listener.local_addr().unwrap().port();
    drop(listener);

    // Check availability (static method)
    assert!(
        fed::port::PortConflict::is_port_available(port),
        "Port should be available"
    );

    // Now allocate it
    let mut allocator = PortAllocator::new();
    let result = allocator.try_allocate_port(port);
    assert!(
        result.is_ok(),
        "Should still be able to allocate after is_port_available check"
    );
}
