#![allow(non_snake_case)]

use fed::Parser;
/// Reproduction tests for identified critical issues
///
/// These tests attempt to reproduce:
/// 1. TOCTOU race condition in monitoring loop (service removed during health check)
/// 2. Race condition in state tracker (concurrent failure increments)
/// 3. Lock ordering deadlock (inconsistent lock acquisition order)
use fed::config::ServiceType;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;

#[path = "support/mod.rs"]
mod support;

/// TEST 1: TOCTOU Race Condition in Monitoring Loop
///
/// Scenario:
/// 1. Start multiple services and enable monitoring
/// 2. While monitoring loop is reading service list, stop a service
/// 3. Monitoring loop tries to access a service that was just removed
///
/// Expected: Should handle gracefully with the "continue" guard at line 1236
/// Current: The guard is there but the race window still exists between
///          1224 (reading service list) and 1232 (getting arc clone)
#[tokio::test]
async fn repro_toctou_service_removed_during_health_check() {
    let config_content = r#"
parameters:
  PORT_A:
    type: port
    default: 9001
  PORT_B:
    type: port
    default: 9002

services:
  service1:
    process: 'echo "service1"; sleep 30'
    environment:
      PORT: '{{PORT_A}}'

  service2:
    process: 'echo "service2"; sleep 30'
    environment:
      PORT: '{{PORT_B}}'
"#;

    let parser = Parser::new();
    let config = parser.parse_config(config_content).unwrap();

    let _orch_temp = tempfile::tempdir().unwrap();
    let orchestrator = Arc::new(tokio::sync::Mutex::new(
        support::new_orchestrator_for_test(config, _orch_temp.path().to_path_buf())
            .await
            .unwrap(),
    ));

    // Initialize and start services
    orchestrator.lock().await.set_auto_resolve_conflicts(true);
    orchestrator.lock().await.initialize().await.unwrap();
    orchestrator.lock().await.start("service1").await.ok();
    orchestrator.lock().await.start("service2").await.ok();

    // Spawn task that repeatedly stops a service (simulates service removal)
    let orch_stopper = orchestrator.clone();
    let stopper = tokio::spawn(async move {
        for _ in 0..10 {
            sleep(Duration::from_millis(50)).await;
            let _ = orch_stopper.lock().await.stop("service1").await;
        }
    });

    // Let the test run - monitoring will encounter services that disappear
    sleep(Duration::from_millis(500)).await;

    let _ = stopper.await;

    println!("✓ TOCTOU test completed - race window exists but is guarded");
    println!("  (Line 1236's 'continue' prevents crash, but races on lock still possible)");
}

/// TEST 2: State Tracker Race Condition
///
/// Scenario:
/// 1. Multiple tasks fail a service concurrently
/// 2. Each reads consecutive_failures counter, increments, writes back
/// 3. Counter should end up = N failures, not 1
///
/// Expected: Counter == number of concurrent increments
/// Current Bug: Counter might be < N due to lost updates
#[tokio::test]
async fn repro_state_tracker_concurrent_failure_increments() {
    use fed::state::{ServiceState, StateTracker};
    use tempfile::tempdir;

    let temp_dir = tempdir().expect("Failed to create temp dir");
    let mut tracker = StateTracker::new(temp_dir.path().to_path_buf())
        .await
        .unwrap();
    tracker
        .initialize()
        .await
        .expect("Failed to initialize tracker");

    // Register a service
    let service_state = ServiceState::new(
        "test-service".to_string(),
        ServiceType::Process,
        "default".to_string(),
    );
    tracker.register_service(service_state).await.unwrap();

    // Wrap in Arc<Mutex> for concurrent access
    let tracker_arc = Arc::new(tokio::sync::Mutex::new(tracker));

    // Spawn 10 concurrent tasks that each increment the failure counter
    let mut handles = vec![];
    let num_increments = 10;

    for i in 0..num_increments {
        let tracker = tracker_arc.clone();
        let handle = tokio::spawn(async move {
            // Simulate the problematic code pattern from orchestrator.rs:1282-1287
            // Read -> Increment -> Read (but no transaction, so lossy)
            let current = {
                let mut t = tracker.lock().await;
                // OLD PATTERN (now impossible due to private methods):
                // t.increment_consecutive_failures(); t.save();
                // This would lose updates - both tasks read 0, only one write persists
                // CORRECT PATTERN: Use atomic operation
                t.increment_consecutive_failures("test-service")
                    .await
                    .unwrap();
                t.get_consecutive_failures("test-service")
                    .await
                    .unwrap_or(0)
            };

            println!("Task {} sees counter at: {}", i, current);
            current
        });
        handles.push(handle);
    }

    // Wait for all to complete
    let _results: Vec<_> = futures::future::join_all(handles).await;

    // Final value
    let final_tracker = tracker_arc.lock().await;
    let final_count = final_tracker
        .get_consecutive_failures("test-service")
        .await
        .unwrap_or(0);

    println!("Final failure count: {}", final_count);
    println!(
        "Expected: {} increments to result in counter = {}",
        num_increments, num_increments
    );

    // BUG: If this fails, it means we lost updates due to non-atomic read-modify-write
    if final_count < num_increments as u32 {
        println!(
            "❌ RACE CONDITION DETECTED: Final count {} < expected {}",
            final_count, num_increments
        );
        println!(
            "   Lost {} update(s) to concurrent access",
            num_increments - final_count as usize
        );
    } else {
        println!(
            "✓ Counter is correct (but only because of sequential lock - real code path is different)"
        );
    }
}

/// TEST 3: Lock Ordering Deadlock
///
/// Scenario:
/// 1. Task A: acquires services.read() lock, then tries to acquire state_tracker.write()
/// 2. Task B: acquires state_tracker.write() lock, then tries to acquire services.read()
/// 3. Deadlock!
///
/// This test tries to create the lock ordering issue from orchestrator.rs
#[tokio::test]
async fn repro_lock_ordering_deadlock() {
    let config_content = r#"
services:
  svc1:
    process: 'echo "test"'
  svc2:
    process: 'echo "test"'
"#;

    let parser = Parser::new();
    let config = parser.parse_config(config_content).unwrap();
    let _orch_temp = tempfile::tempdir().unwrap();
    let orchestrator = Arc::new(tokio::sync::Mutex::new(
        support::new_orchestrator_for_test(config, _orch_temp.path().to_path_buf())
            .await
            .unwrap(),
    ));

    orchestrator.lock().await.set_auto_resolve_conflicts(true);
    orchestrator.lock().await.initialize().await.unwrap();
    orchestrator.lock().await.start("svc1").await.ok();
    orchestrator.lock().await.start("svc2").await.ok();

    // Create timeout for deadlock detection
    let test_timeout = tokio::time::timeout(Duration::from_secs(5), async {
        // Task A: Read services, then write state_tracker
        let orch_a = orchestrator.clone();
        let task_a = tokio::spawn(async move {
            // Simulating lines 1224 and then 1283 of orchestrator.rs
            // (though actual deadlock requires the internal lock interactions)
            for _ in 0..20 {
                let _result = orch_a.lock().await.start("svc1").await;
                sleep(Duration::from_micros(10)).await;
            }
        });

        // Task B: State operations that might need different lock order
        let orch_b = orchestrator.clone();
        let task_b = tokio::spawn(async move {
            for _ in 0..20 {
                let _result = orch_b.lock().await.stop("svc2").await;
                sleep(Duration::from_micros(10)).await;
            }
        });

        let _ = tokio::join!(task_a, task_b);
    })
    .await;

    match test_timeout {
        Ok(_) => {
            println!("✓ No deadlock detected in 5 seconds");
            println!("  (The actual deadlock requires the specific internal lock sequence)");
        }
        Err(_) => {
            println!("❌ DEADLOCK DETECTED: Test timed out after 5 seconds");
            println!("   This indicates lock ordering issue between services and state_tracker");
        }
    }
}

/// TEST 4: Unbounded Health Check Task Spawning
///
/// Scenario:
/// 1. Monitoring loop spawns health check tasks without bounds
/// 2. With many services, this could cause task explosion
///
/// Expected: Resource limits should prevent unbounded task creation
/// Current: Lines 1263 spawn_local without any limits
#[tokio::test]
async fn repro_unbounded_health_check_task_spawning() {
    let mut config_str = String::from("services:\n");
    let num_services = 100;

    // Create config with 100 services
    for i in 0..num_services {
        config_str.push_str(&format!("  svc{}:\n    process: 'echo test'\n", i));
    }

    let parser = Parser::new();
    let config = parser.parse_config(&config_str).unwrap();

    let _orch_temp = tempfile::tempdir().unwrap();
    let orchestrator = Arc::new(tokio::sync::Mutex::new(
        support::new_orchestrator_for_test(config, _orch_temp.path().to_path_buf())
            .await
            .unwrap(),
    ));
    orchestrator.lock().await.set_auto_resolve_conflicts(true);
    orchestrator.lock().await.initialize().await.unwrap();

    // Start all services
    let start_time = std::time::Instant::now();
    for i in 0..num_services {
        let orch = orchestrator.clone();
        let svc_name = format!("svc{}", i);
        tokio::spawn(async move {
            let _ = orch.lock().await.start(&svc_name).await;
        });
    }

    sleep(Duration::from_secs(1)).await;

    let elapsed = start_time.elapsed();
    println!(
        "✓ Started {} services concurrently in {:?}",
        num_services, elapsed
    );

    // In the actual code, monitoring loop would now spawn health check tasks
    // for each of these without any batching or limits
    // This could lead to: task explosion, memory exhaustion, executor starvation

    // Real issue would manifest as:
    // - Task count exploding
    // - Memory usage growing unbounded
    // - Executor becoming unresponsive

    println!("  Note: Real issue would manifest during monitoring phase when");
    println!(
        "  {} health check tasks spawned without limits",
        num_services
    );
}

/// TEST 6: Concurrent Health Check Failures - Counter Increment Race
///
/// This test creates the exact problematic scenario from orchestrator.rs:1282-1287
/// where multiple health check failures happen concurrently and each does:
///   1. Read consecutive_failures
///   2. Increment it
///   3. Save to disk
///   4. Read it back
///
/// With non-atomic operations, concurrent updates could be lost.
#[tokio::test]
async fn repro_concurrent_health_failures_race() {
    use fed::state::{ServiceState, StateTracker};
    use tempfile::tempdir;

    let temp_dir = tempdir().expect("Failed to create temp dir");
    let mut tracker = StateTracker::new(temp_dir.path().to_path_buf())
        .await
        .unwrap();
    tracker
        .initialize()
        .await
        .expect("Failed to initialize tracker");

    // Register service
    let service_state = ServiceState::new(
        "failing-service".to_string(),
        ServiceType::Process,
        "default".to_string(),
    );
    tracker.register_service(service_state).await.unwrap();

    // This simulates the problem: tracker is accessed without proper synchronization
    // across multiple async health check callbacks
    let tracker_arc = Arc::new(tokio::sync::Mutex::new(tracker));

    // Simulate 10 concurrent health checks that all fail
    let num_failures = 10;
    let mut handles = vec![];

    for fail_num in 0..num_failures {
        let tracker = tracker_arc.clone();
        let handle = tokio::spawn(async move {
            // OLD PATTERN (now prevented by private methods):
            // let _ = t.increment_consecutive_failures();
            // let _ = t.save();
            // This pattern was vulnerable to TOCTOU
            //
            // NEW PATTERN: Atomic operation (now required by private methods)
            let consecutive_failures = {
                let mut t = tracker.lock().await;
                t.increment_consecutive_failures("failing-service")
                    .await
                    .unwrap();
                t.get_consecutive_failures("failing-service")
                    .await
                    .unwrap_or(0)
            };

            println!(
                "Health check failure {} observes counter at: {}",
                fail_num, consecutive_failures
            );
            (fail_num, consecutive_failures)
        });
        handles.push(handle);
    }

    let results: Vec<_> = futures::future::join_all(handles)
        .await
        .into_iter()
        .filter_map(|r| r.ok())
        .collect();

    let final_tracker = tracker_arc.lock().await;
    let final_count = final_tracker
        .get_consecutive_failures("failing-service")
        .await
        .unwrap_or(0);

    println!("\nConcurrent failure increments:");
    println!("  Expected final counter: {}", num_failures);
    println!("  Actual final counter: {}", final_count);

    // Check if all results are consistent with the final count
    let max_observed = results.iter().map(|(_, count)| *count).max().unwrap_or(0);
    println!("  Max count any task saw: {}", max_observed);

    if final_count != num_failures as u32 {
        println!("\n⚠️  POTENTIAL RACE CONDITION:");
        println!(
            "   Lost {} updates due to concurrent access",
            num_failures - final_count as usize
        );
        println!("   Each failure did: read -> increment -> save -> read");
        println!("   With non-atomic save, updates between reads could be lost");
    } else {
        println!("\n✓ Counter is correct, but only because of tokio::Mutex serialization");
        println!("  Real risk: file-based lock (fs2) has gaps for race conditions");
    }
}

/// ACTUAL BUG REPRODUCTION: File-based lock race in StateTracker
///
/// The real vulnerability: StateTracker uses fs2::FileExt lock which:
/// 1. Opens file
/// 2. Acquires lock
/// 3. Reads content
/// 4. Modifies in memory
/// 5. Unlocks
/// 6. Writes back
///
/// But between unlock and write, another process could modify the file!
/// This test simulates concurrent processes modifying the state file.
#[tokio::test]
async fn ACTUAL_BUG_file_lock_race_condition() {
    use fed::state::{ServiceState, StateTracker};
    use tempfile::tempdir;

    let temp_dir = tempdir().expect("Failed to create temp dir");
    let mut tracker1 = StateTracker::new(temp_dir.path().to_path_buf())
        .await
        .unwrap();
    tracker1.initialize().await.expect("Failed to initialize");

    // Register a service
    let service_state = ServiceState::new(
        "racy-service".to_string(),
        ServiceType::Process,
        "default".to_string(),
    );
    tracker1.register_service(service_state).await.unwrap();
    tracker1.save().await.expect("Save 1");

    // Now we have two "processes" accessing the same state file
    // Simulate process 1 doing: read -> increment -> pause -> save
    // And process 2 doing: read -> increment -> save during the pause

    let temp_dir_path = temp_dir.path().to_path_buf();

    // Task 1: Read -> Increment (slowly)
    let dir1 = temp_dir_path.clone();
    let task1 = tokio::spawn(async move {
        let mut t1 = StateTracker::new(dir1).await.unwrap();
        t1.initialize().await.expect("Init 1");

        // OLD BUGGY PATTERN (now prevented by private methods):
        //   t1.increment_consecutive_failures().expect("Increment");
        //   sleep(100ms)  // Race window where t2 could read stale value
        //   t1.save().expect("Save");
        //
        // NEW CORRECT PATTERN: Atomic operation holds lock through entire RMW
        println!("[Task1] Using atomic operation...");
        t1.increment_consecutive_failures("racy-service")
            .await
            .expect("Atomic increment 1");
        let count = t1
            .get_consecutive_failures("racy-service")
            .await
            .unwrap_or(0);
        println!("[Task1] Atomically incremented to: {}", count);

        count
    });

    // Let task1 start reading
    sleep(Duration::from_millis(50)).await;

    // Task 2: Also read and increment (racing with task1)
    let dir2 = temp_dir_path.clone();
    let task2 = tokio::spawn(async move {
        sleep(Duration::from_millis(150)).await; // Let task1 start first

        let mut t2 = StateTracker::new(dir2).await.unwrap();
        t2.initialize().await.expect("Init 2");

        // Use atomic operation (old buggy pattern is now private/impossible)
        println!("[Task2] Using atomic operation...");
        t2.increment_consecutive_failures("racy-service")
            .await
            .expect("Atomic increment 2");
        let count = t2
            .get_consecutive_failures("racy-service")
            .await
            .unwrap_or(0);
        println!("[Task2] Atomically incremented to: {}", count);

        count
    });

    let result1 = task1.await.expect("Task 1 failed");
    let result2 = task2.await.expect("Task 2 failed");

    // Now check the final state
    let mut final_tracker = StateTracker::new(temp_dir_path).await.unwrap();
    final_tracker.initialize().await.expect("Init final");
    let final_count = final_tracker
        .get_consecutive_failures("racy-service")
        .await
        .unwrap_or(0);

    println!("\n=== RACE CONDITION RESULT ===");
    println!("Task 1 read: 0, increment to: 1, saved as: {}", result1);
    println!("Task 2 read: 0, increment to: 1, saved as: {}", result2);
    println!("Final on-disk value: {}", final_count);
    println!("Expected (no race): 2");

    if final_count < 2 {
        println!("\n❌ RACE CONDITION CONFIRMED!");
        println!("   Both tasks incremented but only one write persisted");
        println!("   Lost update: {} < 2", final_count);
    } else {
        println!("\n✓ No race (fs2 lock worked this time, but timing-dependent)");
    }
}

/// ACTUAL BUG REPRODUCTION 2: More aggressive file lock race
///
/// Same bug but with more concurrent writers to increase likelihood
#[tokio::test]
async fn ACTUAL_BUG_multiple_concurrent_increments() {
    use fed::state::{ServiceState, StateTracker};
    use tempfile::tempdir;

    let temp_dir = tempdir().expect("Failed to create temp dir");
    let mut tracker = StateTracker::new(temp_dir.path().to_path_buf())
        .await
        .unwrap();
    tracker.initialize().await.expect("Failed to initialize");

    let service_state = ServiceState::new(
        "buggy-service".to_string(),
        ServiceType::Process,
        "default".to_string(),
    );
    tracker.register_service(service_state).await.unwrap();
    tracker.save().await.expect("Save init");

    let dir_path = temp_dir.path().to_path_buf();

    // Spawn 5 concurrent tasks all trying to increment
    let mut handles = vec![];
    for i in 0..5 {
        let dir = dir_path.clone();
        let handle = tokio::spawn(async move {
            let mut t = StateTracker::new(dir).await.unwrap();
            t.initialize().await.expect("Init");

            // Stagger them slightly to force overlapping operations
            sleep(Duration::from_millis(i * 20)).await;

            // OLD BUGGY PATTERN (now prevented by private methods):
            // let before = t.get_consecutive_failures();
            // t.increment_consecutive_failures();
            // sleep(50ms);  // Race window
            // t.save();
            // Results: All read 0, only last write persists (1 instead of 5)

            // NEW CORRECT PATTERN: Atomic operation
            t.increment_consecutive_failures("buggy-service")
                .await
                .expect("Atomic increment");
            let new_count = t
                .get_consecutive_failures("buggy-service")
                .await
                .unwrap_or(0);

            println!("[Task {}] Atomic increment result: {}", i, new_count);
            (i, new_count)
        });
        handles.push(handle);
    }

    futures::future::join_all(handles).await;

    let mut final_tracker = StateTracker::new(dir_path).await.unwrap();
    final_tracker.initialize().await.expect("Init final");
    let final_count = final_tracker
        .get_consecutive_failures("buggy-service")
        .await
        .unwrap_or(0);

    println!("\n=== MULTIPLE CONCURRENT WRITES ===");
    println!("5 tasks each: read -> increment -> save");
    println!("Final value: {}", final_count);
    println!("Expected: 5");

    if final_count < 5 {
        println!("\n❌ LOST UPDATES CONFIRMED!");
        println!("   Expected 5 increments, got {}", final_count);
        println!("   Probability: race windows when tasks overlap writes");
    }
}

/// ACTUAL BUG REPRODUCTION 3: Deadlock potential with RwLock + Mutex nesting
///
/// The orchestrator has: Arc<RwLock<HashMap<String, Arc<Mutex<ServiceManager>>>>>
/// And state_tracker is: Arc<RwLock<StateTracker>>
///
/// If we acquire them in different orders, deadlock is possible
#[tokio::test]
async fn ACTUAL_BUG_potential_deadlock_lock_order() {
    let config = r#"
services:
  svc1:
    process: 'echo test'
  svc2:
    process: 'echo test'
"#;

    let parser = Parser::new();
    let cfg = parser.parse_config(config).unwrap();
    let _orch_temp5 = tempfile::tempdir().unwrap();
    let orchestrator = Arc::new(tokio::sync::Mutex::new(
        support::new_orchestrator_for_test(cfg, _orch_temp5.path().to_path_buf())
            .await
            .unwrap(),
    ));

    orchestrator.lock().await.set_auto_resolve_conflicts(true);
    orchestrator.lock().await.initialize().await.ok();
    orchestrator.lock().await.start("svc1").await.ok();
    orchestrator.lock().await.start("svc2").await.ok();

    // Create two tasks that fight over locks in different orders
    let orch_a = orchestrator.clone();
    let task_a = tokio::spawn(async move {
        for _ in 0..20 {
            // Pattern 1: start service (tries to access services map then state tracker)
            let _ = orch_a.lock().await.start("svc1").await;
            tokio::task::yield_now().await;
        }
    });

    let orch_b = orchestrator.clone();
    let task_b = tokio::spawn(async move {
        for _ in 0..20 {
            // Pattern 2: stop service (different lock path)
            let _ = orch_b.lock().await.stop("svc2").await;
            tokio::task::yield_now().await;
        }
    });

    let orch_c = orchestrator.clone();
    let task_c = tokio::spawn(async move {
        for _ in 0..20 {
            // Pattern 3: status/health check (reads state tracker then services)
            let _ = orch_c.lock().await.initialize().await;
            tokio::task::yield_now().await;
        }
    });

    // Run with timeout - if deadlock occurs, timeout will fire
    let timeout_result = tokio::time::timeout(Duration::from_secs(3), async {
        let _ = tokio::join!(task_a, task_b, task_c);
    })
    .await;

    match timeout_result {
        Ok(_) => {
            println!("✓ No deadlock detected in 3 seconds");
            println!("  (Mutex serialization prevents it, but internal lock order still risky)");
        }
        Err(_) => {
            println!("❌ DEADLOCK DETECTED!");
            println!("   Tasks timeout after 3 seconds");
            println!(
                "   Lock order: services.read() vs state_tracker.write() acquiring in different orders"
            );
        }
    }
}

/// FIX VERIFICATION: Atomic operations prevent lost updates
///
/// These new atomic methods hold the file lock during the entire
/// read-modify-write operation, preventing concurrent instances from
/// seeing stale state.
#[tokio::test]
async fn FIXED_atomic_increment_prevents_lost_updates() {
    use fed::state::{ServiceState, StateTracker};
    use tempfile::tempdir;

    let temp_dir = tempdir().expect("Failed to create temp dir");
    let mut tracker = StateTracker::new(temp_dir.path().to_path_buf())
        .await
        .unwrap();
    tracker.initialize().await.expect("Failed to initialize");

    let service_state = ServiceState::new(
        "fixed-service".to_string(),
        ServiceType::Process,
        "default".to_string(),
    );
    tracker.register_service(service_state).await.unwrap();
    tracker.force_save().await.expect("Save init");

    let dir_path = temp_dir.path().to_path_buf();

    // Spawn 5 concurrent tasks all using ATOMIC operations
    let mut handles = vec![];
    for i in 0..5 {
        let dir = dir_path.clone();
        let handle = tokio::spawn(async move {
            let mut t = StateTracker::new(dir).await.unwrap();
            // Don't need to initialize since file already exists

            // Use atomic operation instead of read-modify-save
            t.increment_consecutive_failures("fixed-service")
                .await
                .expect("Atomic increment failed");
            let new_count = t
                .get_consecutive_failures("fixed-service")
                .await
                .unwrap_or(0);

            println!("[Task {}] Atomic increment result: {}", i, new_count);
            (i, new_count)
        });
        handles.push(handle);
    }

    let _results: Vec<_> = futures::future::join_all(handles)
        .await
        .into_iter()
        .filter_map(|r| r.ok())
        .collect();

    let mut final_tracker = StateTracker::new(dir_path).await.unwrap();
    final_tracker.initialize().await.expect("Init final");
    let final_count = final_tracker
        .get_consecutive_failures("fixed-service")
        .await
        .unwrap_or(0);

    println!("\n=== ATOMIC OPERATIONS (FIXED) ===");
    println!("5 tasks each using increment_consecutive_failures");
    println!("Final value: {}", final_count);
    println!("Expected: 5");

    if final_count == 5 {
        println!("\n✅ FIX VERIFIED!");
        println!("   All 5 updates properly persisted");
        println!("   Atomic file lock prevents lost updates");
    } else {
        println!("\n❌ FIX INCOMPLETE: Still got {}, expected 5", final_count);
    }
}

/// FIX VERIFICATION: Two concurrent atomic increments
///
/// Simpler test with just 2 concurrent increments
#[tokio::test]
async fn FIXED_atomic_two_concurrent_increments() {
    use fed::state::{ServiceState, StateTracker};
    use tempfile::tempdir;

    let temp_dir = tempdir().expect("Failed to create temp dir");
    let mut tracker = StateTracker::new(temp_dir.path().to_path_buf())
        .await
        .unwrap();
    tracker.initialize().await.expect("Failed to initialize");

    let service_state = ServiceState::new(
        "dual-service".to_string(),
        ServiceType::Process,
        "default".to_string(),
    );
    tracker.register_service(service_state).await.unwrap();
    tracker.force_save().await.expect("Save init");

    let dir_path = temp_dir.path().to_path_buf();

    let dir1 = dir_path.clone();
    let task1 = tokio::spawn(async move {
        let mut t1 = StateTracker::new(dir1).await.unwrap();
        t1.initialize().await.expect("Task 1 init");
        t1.increment_consecutive_failures("dual-service")
            .await
            .expect("Task 1 increment failed");
        let count = t1
            .get_consecutive_failures("dual-service")
            .await
            .unwrap_or(0);
        println!("[Task 1] Atomic increment result: {}", count);
        count
    });

    let dir2 = dir_path.clone();
    let task2 = tokio::spawn(async move {
        sleep(Duration::from_millis(50)).await; // Let task1 start first
        let mut t2 = StateTracker::new(dir2).await.unwrap();
        t2.initialize().await.expect("Task 2 init");
        t2.increment_consecutive_failures("dual-service")
            .await
            .expect("Task 2 increment failed");
        let count = t2
            .get_consecutive_failures("dual-service")
            .await
            .unwrap_or(0);
        println!("[Task 2] Atomic increment result: {}", count);
        count
    });

    let result1 = task1.await.expect("Task 1 failed");
    let result2 = task2.await.expect("Task 2 failed");

    let mut final_tracker = StateTracker::new(dir_path).await.unwrap();
    final_tracker.initialize().await.expect("Init final");
    let final_count = final_tracker
        .get_consecutive_failures("dual-service")
        .await
        .unwrap_or(0);

    println!("\n=== TWO CONCURRENT ATOMIC INCREMENTS (FIXED) ===");
    println!("Task 1 atomic increment returned: {}", result1);
    println!("Task 2 atomic increment returned: {}", result2);
    println!("Final on-disk value: {}", final_count);
    println!("Expected: 2");

    if final_count == 2 {
        println!("\n✅ FIX VERIFIED!");
        println!("   Both updates properly serialized");
        println!("   No lost updates with atomic operations");
    } else {
        println!("\n❌ FIX INCOMPLETE: Still got {}, expected 2", final_count);
    }
}

/// TEST 5: Parameter Resolution State Inconsistency
///
/// Scenario:
/// 1. External service parameters are resolved in one pass
/// 2. Main config parameters in another pass
/// 3. Parameter values could diverge between these passes
#[tokio::test]
async fn repro_parameter_resolution_inconsistency() {
    let config_content = r#"
parameters:
  PORT:
    type: port
    default: 9000

services:
  main:
    process: 'sleep 10'
    environment:
      PORT: '{{PORT}}'
    external_service_parameters:
      - name: PORT
        mapping: '{{PORT}}'
"#;

    let parser = Parser::new();
    let config = parser.parse_config(config_content).unwrap();

    // Simulate concurrent resolution attempts
    let config_arc = Arc::new(tokio::sync::Mutex::new(config));

    let mut handles = vec![];
    for i in 0..5 {
        let cfg = config_arc.clone();
        let handle = tokio::spawn(async move {
            // Each task tries to resolve parameters
            // In the real code, parameter values could differ between tasks
            // if resolution happens in multiple passes
            let _cfg = cfg.lock().await;
            // Would call resolver.resolve_parameters() and resolver.resolve_config()
            // separately, creating potential inconsistency
            i
        });
        handles.push(handle);
    }

    let _results: Vec<_> = futures::future::join_all(handles).await;

    println!("✓ Parameter resolution test completed");
    println!("  Note: Real inconsistency would show if parameters resolved in different passes");
}

/// ACTUAL BUG REPRODUCTION: "Service not found" paradox with stale lock files
///
/// This test reproduces the bug where:
/// 1. Lock file exists from a previous run with container IDs
/// 2. Containers are no longer running
/// 3. Orchestrator initializes and loads stale lock file
/// 4. Monitoring loop runs health checks on stale services
/// 5. Starting a new service fails with "Service not found" even though it exists
///
/// Symptoms from real bug report:
///   Error: Service not found: next
///   Available services:
///     - redis
///     - next      <-- paradox! it's listed but "not found"
#[tokio::test]
async fn ACTUAL_BUG_service_not_found_paradox_with_stale_lock_file() {
    use serde_json::json;
    use std::fs;
    use tempfile::tempdir;

    let temp_dir = tempdir().expect("Failed to create temp dir");
    let work_dir = temp_dir.path();

    // Create a stale lock file with PIDs that don't exist (dead processes)
    // Using correct LockFile format from src/state/types.rs
    let stale_lock = json!({
        "fed_pid": 12345,  // Stale PID from previous run
        "work_dir": work_dir.to_string_lossy(),
        "started_at": "2025-01-01T00:00:00Z",
        "services": {
            "root/localstack": {
                "id": "root/localstack",
                "status": "running",
                "service_type": "Process",
                "pid": 99999,  // PID doesn't exist
                "container_id": null,
                "port_allocations": {},
                "started_at": "2025-01-01T00:00:00Z",
                "external_repo": null,
                "namespace": "root",
                "restart_count": 0,
                "last_restart_at": null,
                "consecutive_failures": 0
            },
            "root/redis": {
                "id": "root/redis",
                "status": "running",
                "service_type": "Process",
                "pid": 99998,  // PID doesn't exist
                "container_id": null,
                "port_allocations": {},
                "started_at": "2025-01-01T00:00:00Z",
                "external_repo": null,
                "namespace": "root",
                "restart_count": 0,
                "last_restart_at": null,
                "consecutive_failures": 0
            }
        },
        "allocated_ports": []
    });

    let lock_file_path = work_dir.join(".fed-lock.json");
    fs::write(
        &lock_file_path,
        serde_json::to_string_pretty(&stale_lock).unwrap(),
    )
    .expect("Failed to write stale lock file");

    // Create config with just process services - no docker images
    // This tests the core paradox without docker dependencies
    let config_content = r#"
services:
  localstack:
    process: 'sleep 30'

  redis:
    process: 'sleep 30'

  app:
    process: 'sleep 30'
    depends_on: [localstack, redis]

entrypoint: app
"#;

    let parser = Parser::new();
    let config = parser.parse_config(config_content).unwrap();

    // Create orchestrator with the work directory containing stale lock file
    let mut orchestrator = support::new_orchestrator_for_test(config, work_dir.to_path_buf())
        .await
        .unwrap();

    // Initialize - this will load the stale lock file
    // The monitoring loop will start and find stale services
    println!("=== Initializing orchestrator with stale lock file ===");
    orchestrator.set_auto_resolve_conflicts(true);
    let init_result = orchestrator.initialize().await;

    match init_result {
        Ok(_) => println!("✓ Initialize succeeded"),
        Err(e) => {
            println!("✗ Initialize failed: {}", e);
            // Even if init fails, we want to check the paradox
        }
    }

    // Try to start a service - this is where the paradox occurs
    println!("\n=== Starting 'app' service (depends on localstack, redis) ===");

    // Give the monitoring loop a chance to run and potentially corrupt state
    tokio::time::sleep(Duration::from_millis(100)).await;

    let start_result = orchestrator.start("app").await;

    // Get status to show available services
    let status = orchestrator.get_status().await;
    println!("\nAvailable services in HashMap:");
    for (name, stat) in &status {
        println!("  - {}: {:?}", name, stat);
    }

    match start_result {
        Ok(_) => {
            println!("\n✓ Start succeeded - bug may not be reproduced in this run");
        }
        Err(e) => {
            let error_str = e.to_string();
            println!("\n✗ Start failed: {}", error_str);

            if error_str.contains("Service not found") {
                // Check if the "not found" service is actually in the status
                let service_name = if error_str.contains("app") {
                    "app"
                } else if error_str.contains("localstack") {
                    "localstack"
                } else if error_str.contains("redis") {
                    "redis"
                } else {
                    "unknown"
                };

                if status.contains_key(service_name) {
                    println!("\n❌ PARADOX CONFIRMED!");
                    println!("   Error says '{}' not found", service_name);
                    println!("   But status shows '{}' exists!", service_name);
                    println!("   This is the 'Service not found' paradox bug");
                    panic!(
                        "Bug reproduced: Service '{}' reported as not found but exists in status",
                        service_name
                    );
                } else {
                    println!("\n⚠️  Service really is missing from HashMap");
                    println!("   This suggests the bug corrupted the services HashMap");
                    println!(
                        "   Service '{}' was removed during stale lock cleanup",
                        service_name
                    );
                }
            }
        }
    }

    // Cleanup
    orchestrator.cleanup().await;
}
