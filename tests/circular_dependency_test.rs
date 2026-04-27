/// Tests for circular dependency detection
///
/// When a service's process command invokes `fed` (directly or indirectly),
/// it creates an infinite loop. We detect this at startup by checking for
/// the FED_SPAWNED_BY_SERVICE environment variable that we set when spawning
/// service processes.
use std::process::Command;

/// Test that `fed` detects when it's invoked from within a service process
/// and exits with an appropriate error message.
#[test]
fn test_circular_dependency_detection() {
    // Build the binary first
    let build = Command::new("cargo")
        .args(["build", "--bin", "fed"])
        .output()
        .expect("Failed to build fed binary");
    assert!(
        build.status.success(),
        "Failed to build fed: {:?}",
        String::from_utf8_lossy(&build.stderr)
    );

    // Run fed with FED_SPAWNED_BY_SERVICE set (simulating being called from a service)
    let output = Command::new("cargo")
        .args(["run", "--bin", "fed", "--", "start"])
        .env("FED_SPAWNED_BY_SERVICE", "test-service")
        .output()
        .expect("Failed to run fed");

    // Should exit with non-zero status
    assert!(
        !output.status.success(),
        "Expected fed to fail when FED_SPAWNED_BY_SERVICE is set, but it succeeded"
    );

    // Check error message contains helpful information
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("Circular dependency detected"),
        "Expected 'Circular dependency detected' in error message, got: {}",
        stderr
    );
    assert!(
        stderr.contains("test-service"),
        "Expected service name 'test-service' in error message, got: {}",
        stderr
    );
    assert!(
        stderr.contains("infinite loop"),
        "Expected 'infinite loop' warning in error message, got: {}",
        stderr
    );
}

/// Path to the built fed binary. Cargo sets CARGO_BIN_EXE_<name> for
/// integration tests of binary crates; using it sidesteps the
/// `cargo run` requirement of being inside a Cargo workspace, which
/// matters for tests that change `current_dir` to a tempdir.
fn fed_binary() -> String {
    env!("CARGO_BIN_EXE_fed").to_string()
}

/// 3.6.3: When the spawning fed sets FED_SPAWNED_FROM_WORKSPACE and
/// the child fed runs against a *different* workspace, the recursion
/// check should NOT fire — that's a legitimate cross-config invocation.
#[test]
fn test_cross_workspace_invocation_allowed() {
    use std::fs;
    use tempfile::tempdir;

    // Two distinct workspaces.
    let parent_ws = tempdir().expect("create parent workspace");
    let child_ws = tempdir().expect("create child workspace");

    // Minimal config in the child workspace so fed has something to validate.
    fs::write(
        child_ws.path().join("service-federation.yaml"),
        "services:\n  noop:\n    process: 'true'\n",
    )
    .expect("write child config");

    let output = Command::new(fed_binary())
        .args(["validate"])
        .env("FED_SPAWNED_BY_SERVICE", "test-service")
        .env("FED_SPAWNED_FROM_WORKSPACE", parent_ws.path())
        .current_dir(child_ws.path())
        .output()
        .expect("Failed to run fed");

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        !stderr.contains("Circular dependency detected"),
        "cross-workspace invocation must not trip the recursion check, got stderr:\n{}",
        stderr
    );
    assert!(
        output.status.success(),
        "validate against fresh workspace failed: {}",
        stderr
    );
}

/// 3.6.3: Same-workspace recursion must still be blocked when both
/// markers are set and resolve to the same canonical path.
#[test]
fn test_same_workspace_invocation_blocked() {
    use std::fs;
    use tempfile::tempdir;

    let ws = tempdir().expect("create workspace");
    fs::write(
        ws.path().join("service-federation.yaml"),
        "services:\n  noop:\n    process: 'true'\n",
    )
    .expect("write config");

    let output = Command::new(fed_binary())
        .args(["validate"])
        .env("FED_SPAWNED_BY_SERVICE", "test-service")
        .env("FED_SPAWNED_FROM_WORKSPACE", ws.path())
        .current_dir(ws.path())
        .output()
        .expect("Failed to run fed");

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("Circular dependency detected"),
        "same-workspace recursion must trip the check, got:\n{}",
        stderr
    );
    assert!(!output.status.success());
}

/// 3.6.3: FED_ALLOW_RECURSION=1 escapes the check entirely, even
/// when same-workspace markers would otherwise block.
#[test]
fn test_allow_recursion_env_var_escapes_check() {
    use std::fs;
    use tempfile::tempdir;

    let ws = tempdir().expect("create workspace");
    fs::write(
        ws.path().join("service-federation.yaml"),
        "services:\n  noop:\n    process: 'true'\n",
    )
    .expect("write config");

    let output = Command::new(fed_binary())
        .args(["validate"])
        .env("FED_SPAWNED_BY_SERVICE", "test-service")
        .env("FED_SPAWNED_FROM_WORKSPACE", ws.path())
        .env("FED_ALLOW_RECURSION", "1")
        .current_dir(ws.path())
        .output()
        .expect("Failed to run fed");

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        !stderr.contains("Circular dependency detected"),
        "FED_ALLOW_RECURSION=1 must skip the check, got:\n{}",
        stderr
    );
}

/// Test that `fed` runs normally when FED_SPAWNED_BY_SERVICE is not set
#[test]
fn test_normal_startup_without_circular_dependency_marker() {
    // Build the binary first
    let build = Command::new("cargo")
        .args(["build", "--bin", "fed"])
        .output()
        .expect("Failed to build fed binary");
    assert!(
        build.status.success(),
        "Failed to build fed: {:?}",
        String::from_utf8_lossy(&build.stderr)
    );

    // Run fed --help without the marker (should work normally)
    let output = Command::new("cargo")
        .args(["run", "--bin", "fed", "--", "--help"])
        .env_remove("FED_SPAWNED_BY_SERVICE") // Explicitly remove in case it's set
        .output()
        .expect("Failed to run fed");

    // Should succeed
    assert!(
        output.status.success(),
        "Expected fed --help to succeed, but got error: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    // Should show help output
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("Service Federation"),
        "Expected help output, got: {}",
        stdout
    );
}
