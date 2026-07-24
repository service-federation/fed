//! Integration tests for `fed login/logout/whoami/link/secrets` surfaces
//! that don't need a network.

use std::process::Command;
use tempfile::TempDir;

fn fed_binary() -> &'static str {
    env!("CARGO_BIN_EXE_fed")
}

/// whoami without credentials says so and exits 0 (informational, not an error).
#[test]
fn test_whoami_signed_out() {
    let tmp = TempDir::new().unwrap();
    let output = Command::new(fed_binary())
        .args(["whoami"])
        .env("HOME", tmp.path()) // no ~/.fed/credentials
        .env_remove("FED_TOKEN")
        .output()
        .unwrap();
    assert!(output.status.success());
    let combined = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        combined.contains("fed login"),
        "should hint at fed login: {combined}"
    );
}

/// link with an explicit target writes .fed/cloud.yaml without needing auth.
#[test]
fn test_link_writes_cloud_yaml() {
    let tmp = TempDir::new().unwrap();
    let output = Command::new(fed_binary())
        .args([
            "--workdir",
            tmp.path().to_str().unwrap(),
            "link",
            "acme/web",
        ])
        .env("HOME", tmp.path())
        .env_remove("FED_TOKEN")
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let written = std::fs::read_to_string(tmp.path().join(".fed/cloud.yaml")).unwrap();
    assert!(written.contains("org: acme"));
    assert!(written.contains("project: web"));
    assert!(written.contains("secret_cache: file"));

    // fed self-manages .fed/.gitignore: everything ignored except cloud.yaml
    // (and the .gitignore itself).
    let gitignore = std::fs::read_to_string(tmp.path().join(".fed/.gitignore")).unwrap();
    assert_eq!(gitignore, "*\n!cloud.yaml\n!.gitignore\n");
}

#[test]
fn cloud_config_memory_policy_removes_and_refuses_the_file_cache() {
    let tmp = TempDir::new().unwrap();
    std::fs::create_dir_all(tmp.path().join(".fed")).unwrap();
    std::fs::write(
        tmp.path().join(".fed/cloud.yaml"),
        "org: acme\nproject: web\nsecret_cache: memory\n",
    )
    .unwrap();
    std::fs::write(
        tmp.path().join(".fed/secrets.cache.env"),
        "API_KEY=must_not_be_used\n",
    )
    .unwrap();
    std::fs::write(
        tmp.path().join("fed.yaml"),
        "parameters:\n  API_KEY:\n    type: secret\n    source: manual\nservices:\n  app:\n    process: echo ok\n    environment:\n      API_KEY: '{{API_KEY}}'\nentrypoint: app\n",
    )
    .unwrap();

    let config_path = tmp.path().join("fed.yaml");
    let output = Command::new(fed_binary())
        .args([
            "--workdir",
            tmp.path().to_str().unwrap(),
            "--config",
            config_path.to_str().unwrap(),
            "--offline",
            "start",
            "--dry-run",
        ])
        .env("HOME", tmp.path())
        .env_remove("FED_TOKEN")
        .output()
        .unwrap();

    let combined = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(!output.status.success(), "{combined}");
    assert!(
        !tmp.path().join(".fed/secrets.cache.env").exists(),
        "cloud-config memory policy must remove and bypass an existing file cache: {combined}"
    );
    assert!(combined.contains("API_KEY"), "{combined}");
    assert!(!combined.contains("must_not_be_used"), "{combined}");
}

/// link rejects malformed targets.
#[test]
fn test_link_rejects_bad_target() {
    let tmp = TempDir::new().unwrap();
    let output = Command::new(fed_binary())
        .args([
            "--workdir",
            tmp.path().to_str().unwrap(),
            "link",
            "not-a-path",
        ])
        .env("HOME", tmp.path())
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("org/project"));
}

/// secrets ls without login fails with the login hint.
#[test]
fn test_secrets_ls_requires_login() {
    let tmp = TempDir::new().unwrap();
    let output = Command::new(fed_binary())
        .args(["--workdir", tmp.path().to_str().unwrap(), "secrets", "ls"])
        .env("HOME", tmp.path())
        .env_remove("FED_TOKEN")
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("fed login"));
}

/// logout when not signed in is a no-op, not an error.
#[test]
fn test_logout_signed_out() {
    let tmp = TempDir::new().unwrap();
    let output = Command::new(fed_binary())
        .args(["logout"])
        .env("HOME", tmp.path())
        .env_remove("FED_TOKEN")
        .output()
        .unwrap();
    assert!(output.status.success());
}
