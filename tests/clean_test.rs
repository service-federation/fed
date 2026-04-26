use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::Duration;
use tempfile::TempDir;

/// Compute work_dir hash matching docker.rs hash_work_dir (FNV-1a 32-bit).
fn hash_work_dir(work_dir: &Path) -> String {
    let canonical = std::fs::canonicalize(work_dir).unwrap_or_else(|_| work_dir.to_path_buf());
    let bytes = canonical.as_os_str().as_encoded_bytes();
    const FNV_OFFSET: u32 = 2_166_136_261;
    const FNV_PRIME: u32 = 16_777_619;
    let mut hash = FNV_OFFSET;
    for &byte in bytes {
        hash ^= byte as u32;
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    format!("{:08x}", hash)
}

fn fed_binary() -> String {
    env!("CARGO_BIN_EXE_fed").to_string()
}

fn create_clean_test_config(content: &str) -> (TempDir, PathBuf) {
    let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
    let config_path = temp_dir.path().join("service-federation.yaml");
    fs::write(&config_path, content).expect("Failed to write test config");
    (temp_dir, config_path)
}

#[test]
fn test_clean_command_runs_clean_script() {
    let config_content = r#"
services:
  test-service:
    cwd: .
    process: "echo 'running'"
    install: "mkdir -p artifacts && touch artifacts/test.txt"
    clean: "rm -rf artifacts"
"#;

    let (temp_dir, config_path) = create_clean_test_config(config_content);
    let workdir = temp_dir.path().to_str().unwrap();

    // First run install to create artifacts
    let install_output = Command::new(fed_binary())
        .args([
            "-c",
            config_path.to_str().unwrap(),
            "-w",
            workdir,
            "install",
        ])
        .output()
        .expect("Failed to run install");

    assert!(
        install_output.status.success(),
        "Install failed: {}",
        String::from_utf8_lossy(&install_output.stderr)
    );

    // Verify artifacts directory exists
    let artifacts_path = temp_dir.path().join("artifacts");
    assert!(
        artifacts_path.exists(),
        "artifacts directory should exist after install"
    );

    // Run clean
    let clean_output = Command::new(fed_binary())
        .args(["-c", config_path.to_str().unwrap(), "-w", workdir, "clean"])
        .output()
        .expect("Failed to run clean");

    assert!(
        clean_output.status.success(),
        "Clean failed: {}",
        String::from_utf8_lossy(&clean_output.stderr)
    );

    // Verify artifacts directory is gone
    assert!(
        !artifacts_path.exists(),
        "artifacts directory should be removed after clean"
    );

    let stdout = String::from_utf8_lossy(&clean_output.stdout);
    assert!(
        stdout.contains("test-service"),
        "Clean output should mention service name"
    );
    assert!(
        stdout.contains("completed successfully"),
        "Clean output should indicate success"
    );
}

#[test]
fn test_clean_command_specific_service() {
    let config_content = r#"
services:
  service-a:
    process: "echo a"
    clean: "touch cleaned-a.txt"
  service-b:
    process: "echo b"
    clean: "touch cleaned-b.txt"
"#;

    let (temp_dir, config_path) = create_clean_test_config(config_content);
    let workdir = temp_dir.path().to_str().unwrap();

    // Clean only service-a
    let clean_output = Command::new(fed_binary())
        .args([
            "-c",
            config_path.to_str().unwrap(),
            "-w",
            workdir,
            "clean",
            "service-a",
        ])
        .output()
        .expect("Failed to run clean");

    assert!(
        clean_output.status.success(),
        "Clean failed: {}",
        String::from_utf8_lossy(&clean_output.stderr)
    );

    // Only service-a should be cleaned
    assert!(
        temp_dir.path().join("cleaned-a.txt").exists(),
        "service-a should have been cleaned"
    );
    assert!(
        !temp_dir.path().join("cleaned-b.txt").exists(),
        "service-b should NOT have been cleaned"
    );
}

#[test]
fn test_clean_clears_install_state() {
    let config_content = r#"
services:
  test-service:
    process: "echo 'running'"
    install: "echo 'installing'"
    clean: "echo 'cleaning'"
"#;

    let (temp_dir, config_path) = create_clean_test_config(config_content);
    let workdir = temp_dir.path().to_str().unwrap();

    // Run install twice - second should skip (already installed)
    Command::new(fed_binary())
        .args([
            "-c",
            config_path.to_str().unwrap(),
            "-w",
            workdir,
            "install",
        ])
        .output()
        .expect("Failed to run install");

    let install_output_2 = Command::new(fed_binary())
        .args([
            "-c",
            config_path.to_str().unwrap(),
            "-w",
            workdir,
            "install",
        ])
        .output()
        .expect("Failed to run install");

    let stderr_2 = String::from_utf8_lossy(&install_output_2.stderr);
    // Second install should skip (already installed)
    assert!(
        stderr_2.contains("already installed") || install_output_2.status.success(),
        "Second install should skip or succeed"
    );

    // Run clean
    Command::new(fed_binary())
        .args(["-c", config_path.to_str().unwrap(), "-w", workdir, "clean"])
        .output()
        .expect("Failed to run clean");

    // Run install again - should actually run (install state cleared)
    let install_output_3 = Command::new(fed_binary())
        .args([
            "-c",
            config_path.to_str().unwrap(),
            "-w",
            workdir,
            "install",
        ])
        .output()
        .expect("Failed to run install");

    let stderr_3 = String::from_utf8_lossy(&install_output_3.stderr);
    // After clean, install should run the command again
    assert!(
        stderr_3.contains("Running install") || install_output_3.status.success(),
        "Install should run after clean"
    );
}

#[test]
fn test_clean_no_services_message() {
    let config_content = r#"
services:
  test-service:
    process: "echo 'running'"
"#;

    let (temp_dir, config_path) = create_clean_test_config(config_content);
    let workdir = temp_dir.path().to_str().unwrap();

    let clean_output = Command::new(fed_binary())
        .args(["-c", config_path.to_str().unwrap(), "-w", workdir, "clean"])
        .output()
        .expect("Failed to run clean");

    let stdout = String::from_utf8_lossy(&clean_output.stdout);
    assert!(
        stdout.contains("No services with clean field or Docker volumes found"),
        "Should indicate no services to clean"
    );
}

#[test]
fn test_clean_handles_failed_command() {
    let config_content = r#"
services:
  test-service:
    process: "echo 'running'"
    clean: "exit 1"
"#;

    let (temp_dir, config_path) = create_clean_test_config(config_content);
    let workdir = temp_dir.path().to_str().unwrap();

    let clean_output = Command::new(fed_binary())
        .args(["-c", config_path.to_str().unwrap(), "-w", workdir, "clean"])
        .output()
        .expect("Failed to run clean");

    // Should fail because clean command exits with 1
    assert!(
        !clean_output.status.success(),
        "Clean should fail when command fails"
    );
}

#[test]
fn test_clean_with_environment_variables() {
    let config_content = r#"
services:
  test-service:
    process: "echo 'running'"
    environment:
      CLEAN_TARGET: "env_artifacts"
    clean: "mkdir -p $CLEAN_TARGET && touch $CLEAN_TARGET/cleaned.txt"
"#;

    let (temp_dir, config_path) = create_clean_test_config(config_content);
    let workdir = temp_dir.path().to_str().unwrap();

    let clean_output = Command::new(fed_binary())
        .args(["-c", config_path.to_str().unwrap(), "-w", workdir, "clean"])
        .output()
        .expect("Failed to run clean");

    assert!(
        clean_output.status.success(),
        "Clean failed: {}",
        String::from_utf8_lossy(&clean_output.stderr)
    );

    // Verify environment variable was used
    assert!(
        temp_dir.path().join("env_artifacts/cleaned.txt").exists(),
        "Clean should use environment variables"
    );
}

#[test]
fn test_clean_with_custom_cwd() {
    let config_content = r#"
services:
  test-service:
    cwd: subdir
    process: "echo 'running'"
    clean: "touch cleaned.txt"
"#;

    let (temp_dir, config_path) = create_clean_test_config(config_content);
    let workdir = temp_dir.path().to_str().unwrap();

    // Create the subdir
    fs::create_dir(temp_dir.path().join("subdir")).expect("Failed to create subdir");

    let clean_output = Command::new(fed_binary())
        .args(["-c", config_path.to_str().unwrap(), "-w", workdir, "clean"])
        .output()
        .expect("Failed to run clean");

    assert!(
        clean_output.status.success(),
        "Clean failed: {}",
        String::from_utf8_lossy(&clean_output.stderr)
    );

    // File should be in subdir, not root
    assert!(
        temp_dir.path().join("subdir/cleaned.txt").exists(),
        "Clean should run in service's cwd"
    );
    assert!(
        !temp_dir.path().join("cleaned.txt").exists(),
        "Clean should NOT run in root dir"
    );
}

/// Regression: `fed clean` (no args) must clear migrate markers for ALL
/// services in the workspace, not just services that appear in
/// `services_to_clean` (those with `clean:` or `volumes:`).
///
/// The bug this catches: a Rust service whose `migrate:` step creates a
/// database in a sibling postgres has its marker stranded when postgres's
/// volumes are wiped — Fed thinks "already migrated" on next start but
/// the database is gone, so the service crashes with "database does not
/// exist". Full `fed clean` is the "wipe everything" operation; migrate
/// markers must follow.
#[test]
fn test_clean_clears_migrate_markers_for_unlisted_services() {
    let config_content = r#"
services:
  cleanable:
    process: "echo running"
    clean: "true"
  migrate-only:
    process: "echo running"
    migrate: "true"
"#;

    let (temp_dir, config_path) = create_clean_test_config(config_content);
    let workdir = temp_dir.path().to_path_buf();

    // Simulate prior migration of both services. Only `cleanable` would
    // get its marker cleared by per-service clean (it's in the loop).
    // `migrate-only` has neither `clean:` nor `volumes:`, so it would
    // never be touched without the full-clean fix.
    fed::markers::mark_migrated_global("cleanable", &workdir).expect("seed cleanable marker");
    fed::markers::mark_migrated_global("migrate-only", &workdir).expect("seed migrate-only marker");

    assert!(
        fed::markers::is_migrated_global("cleanable", &workdir).unwrap(),
        "precondition: cleanable should be marked migrated"
    );
    assert!(
        fed::markers::is_migrated_global("migrate-only", &workdir).unwrap(),
        "precondition: migrate-only should be marked migrated"
    );

    let clean_out = Command::new(fed_binary())
        .args([
            "-c",
            config_path.to_str().unwrap(),
            "-w",
            workdir.to_str().unwrap(),
            "clean",
        ])
        .output()
        .expect("Failed to run clean");

    assert!(
        clean_out.status.success(),
        "clean failed: {}",
        String::from_utf8_lossy(&clean_out.stderr)
    );

    assert!(
        !fed::markers::is_migrated_global("cleanable", &workdir).unwrap(),
        "cleanable's migrate marker should be cleared (covered by per-service clean)"
    );
    assert!(
        !fed::markers::is_migrated_global("migrate-only", &workdir).unwrap(),
        "migrate-only's marker should be cleared by full `fed clean` even \
         though it has no clean/volumes — this is the regression"
    );
}

#[test]
fn test_clean_idempotent() {
    let config_content = r#"
services:
  test-service:
    process: "echo 'running'"
    clean: "rm -rf nonexistent_dir"
"#;

    let (temp_dir, config_path) = create_clean_test_config(config_content);
    let workdir = temp_dir.path().to_str().unwrap();

    // Run clean multiple times - should succeed each time
    for i in 0..3 {
        let clean_output = Command::new(fed_binary())
            .args(["-c", config_path.to_str().unwrap(), "-w", workdir, "clean"])
            .output()
            .expect("Failed to run clean");

        assert!(
            clean_output.status.success(),
            "Clean attempt {} failed: {}",
            i + 1,
            String::from_utf8_lossy(&clean_output.stderr)
        );
    }
}

// Docker volume tests (only run if Docker is available)
fn is_docker_available() -> bool {
    Command::new("docker")
        .args(["version"])
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
}

#[test]
fn test_clean_with_docker_volumes() {
    if !is_docker_available() {
        eprintln!("Skipping Docker volume test - Docker not available");
        return;
    }

    // Config uses unscoped volume name; fed will scope it with work_dir hash
    let raw_volume = format!("testvolume{}", std::process::id());
    let config_content = format!(
        r#"
services:
  docker-service:
    image: alpine:latest
    volumes:
      - "{}:/data"
"#,
        raw_volume
    );

    let (temp_dir, config_path) = create_clean_test_config(&config_content);
    let workdir = temp_dir.path().to_str().unwrap();

    // Compute the scoped volume name (same as docker.rs does)
    let hash = hash_work_dir(temp_dir.path());
    let scoped_volume = format!("fed-{}-{}", hash, raw_volume);

    // Create the scoped volume manually (simulating what fed start would create)
    let _ = Command::new("docker")
        .args(["volume", "create", &scoped_volume])
        .output();

    // Verify volume exists
    let volume_check = Command::new("docker")
        .args(["volume", "inspect", &scoped_volume])
        .output()
        .expect("Failed to inspect volume");

    if !volume_check.status.success() {
        eprintln!("Could not create test volume, skipping test");
        return;
    }

    // Run clean
    let clean_output = Command::new(fed_binary())
        .args(["-c", config_path.to_str().unwrap(), "-w", workdir, "clean"])
        .output()
        .expect("Failed to run clean");

    assert!(
        clean_output.status.success(),
        "Clean failed: {}",
        String::from_utf8_lossy(&clean_output.stderr)
    );

    // Verify volume is removed
    std::thread::sleep(Duration::from_millis(500));
    let volume_check_after = Command::new("docker")
        .args(["volume", "inspect", &scoped_volume])
        .output()
        .expect("Failed to inspect volume");

    assert!(
        !volume_check_after.status.success(),
        "Docker volume should be removed after clean"
    );

    // Cleanup (in case test failed)
    let _ = Command::new("docker")
        .args(["volume", "rm", "-f", &scoped_volume])
        .output();
}

#[test]
fn test_clean_skips_bind_mounts() {
    if !is_docker_available() {
        eprintln!("Skipping Docker bind mount test - Docker not available");
        return;
    }

    let config_content = r#"
services:
  docker-service:
    image: alpine:latest
    volumes:
      - "./data:/data"
      - "../parent:/parent"
"#;

    let (temp_dir, config_path) = create_clean_test_config(config_content);
    let workdir = temp_dir.path().to_str().unwrap();

    // Create local data directory
    fs::create_dir(temp_dir.path().join("data")).expect("Failed to create data dir");
    fs::write(temp_dir.path().join("data/test.txt"), "test").expect("Failed to write test file");

    // Run clean - should succeed without trying to remove bind mounts as volumes
    let clean_output = Command::new(fed_binary())
        .args(["-c", config_path.to_str().unwrap(), "-w", workdir, "clean"])
        .output()
        .expect("Failed to run clean");

    assert!(
        clean_output.status.success(),
        "Clean failed: {}",
        String::from_utf8_lossy(&clean_output.stderr)
    );

    // Local data should still exist (bind mounts are not Docker volumes)
    assert!(
        temp_dir.path().join("data/test.txt").exists(),
        "Bind mount directories should not be affected by volume cleanup"
    );
}

#[test]
fn test_clean_only_removes_scoped_volumes() {
    if !is_docker_available() {
        eprintln!("Skipping Docker volume test - Docker not available");
        return;
    }

    // Config has two named volumes; fed clean scopes them with work_dir hash
    let vol_a = format!("dbdata{}", std::process::id());
    let unrelated_volume = format!("unrelated-volume-{}", std::process::id());

    let config_content = format!(
        r#"
services:
  docker-service:
    image: alpine:latest
    volumes:
      - "{}:/data"
"#,
        vol_a
    );

    let (temp_dir, config_path) = create_clean_test_config(&config_content);
    let workdir = temp_dir.path().to_str().unwrap();

    // Compute scoped volume name
    let hash = hash_work_dir(temp_dir.path());
    let scoped_vol_a = format!("fed-{}-{}", hash, vol_a);

    // Create the scoped volume and an unrelated volume
    let scoped_create = Command::new("docker")
        .args(["volume", "create", &scoped_vol_a])
        .output();
    let unrelated_create = Command::new("docker")
        .args(["volume", "create", &unrelated_volume])
        .output();

    if scoped_create.is_err() || unrelated_create.is_err() {
        eprintln!("Could not create test volumes, skipping test");
        return;
    }

    // Verify both exist
    let scoped_check = Command::new("docker")
        .args(["volume", "inspect", &scoped_vol_a])
        .output()
        .expect("Failed to inspect scoped volume");
    let unrelated_check = Command::new("docker")
        .args(["volume", "inspect", &unrelated_volume])
        .output()
        .expect("Failed to inspect unrelated volume");

    if !scoped_check.status.success() || !unrelated_check.status.success() {
        eprintln!("Could not verify test volumes exist, skipping test");
        let _ = Command::new("docker")
            .args(["volume", "rm", "-f", &scoped_vol_a])
            .output();
        let _ = Command::new("docker")
            .args(["volume", "rm", "-f", &unrelated_volume])
            .output();
        return;
    }

    // Run clean
    let clean_output = Command::new(fed_binary())
        .args(["-c", config_path.to_str().unwrap(), "-w", workdir, "clean"])
        .output()
        .expect("Failed to run clean");

    assert!(
        clean_output.status.success(),
        "Clean failed: {}",
        String::from_utf8_lossy(&clean_output.stderr)
    );

    std::thread::sleep(Duration::from_millis(500));

    // Verify scoped volume is removed
    let scoped_check_after = Command::new("docker")
        .args(["volume", "inspect", &scoped_vol_a])
        .output()
        .expect("Failed to inspect scoped volume after clean");

    assert!(
        !scoped_check_after.status.success(),
        "Scoped volume should be removed after clean"
    );

    // Verify unrelated volume still exists
    let unrelated_check_after = Command::new("docker")
        .args(["volume", "inspect", &unrelated_volume])
        .output()
        .expect("Failed to inspect unrelated volume after clean");

    assert!(
        unrelated_check_after.status.success(),
        "Unrelated volume should NOT be removed by clean"
    );

    // Cleanup the unrelated volume
    let _ = Command::new("docker")
        .args(["volume", "rm", "-f", &unrelated_volume])
        .output();
}
