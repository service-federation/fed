//! Integration tests for `run:` oneshot services.
//!
//! A oneshot is a fifth service type: a command that executes to completion
//! during startup, after its dependencies are healthy, gating its dependents on
//! that completion. It runs on EVERY `fed start` (its command is expected to be
//! idempotent, e.g. `prisma db push`).
//!
//! The driving real-world bug (Plenora): install/migrate were attached to the
//! LAST service in the graph and ran just-in-time before that service spawned,
//! so earlier services booted against an un-migrated database. A oneshot node
//! that completes before its dependents fixes this.

use std::fs;
use std::process::Command;
use std::time::Duration;

use fed::service::Status;
use fed::{Orchestrator, Parser};
use tempfile::tempdir;

fn fed_binary() -> String {
    env!("CARGO_BIN_EXE_fed").to_string()
}

fn fed_stop(config_path: &std::path::Path, workdir: &str) {
    Command::new(fed_binary())
        .args(["-c", config_path.to_str().unwrap(), "-w", workdir, "stop"])
        .env("FED_NON_INTERACTIVE", "1")
        .output()
        .ok();
}

/// (a) `fed start consumer` succeeds and the oneshot ran before consumer spawned.
///
/// `schema` (oneshot) appends a line to a file. `consumer` FAILS to start unless
/// that file exists (`test -f FILE && exec sleep 30`). So `fed start consumer`
/// succeeding is proof the oneshot completed before consumer spawned.
///
/// (b) A second start re-runs the oneshot — the file ends up with two lines.
#[test]
fn test_oneshot_runs_before_dependent_and_reruns_every_start() {
    let temp = tempdir().expect("temp dir");
    let config_path = temp.path().join("service-federation.yaml");
    let workdir = temp.path().to_str().unwrap();
    let marker = temp.path().join("schema-ran.log");

    let config = format!(
        r#"
services:
  base:
    process: "sleep 30"
  schema:
    run: "echo ran >> {marker}"
    depends_on:
      - base
  consumer:
    process: "test -f {marker} && exec sleep 30"
    depends_on:
      - schema
    startup_message: "consumer is up"
"#,
        marker = marker.display()
    );
    fs::write(&config_path, &config).expect("write config");

    // --- First start ---
    let out = Command::new(fed_binary())
        .args([
            "-c",
            config_path.to_str().unwrap(),
            "-w",
            workdir,
            "start",
            "consumer",
        ])
        .env("FED_NON_INTERACTIVE", "1")
        .output()
        .expect("run fed start");

    println!("start#1 stdout:\n{}", String::from_utf8_lossy(&out.stdout));
    println!("start#1 stderr:\n{}", String::from_utf8_lossy(&out.stderr));

    assert!(
        out.status.success(),
        "fed start consumer must succeed — consumer only starts if the oneshot ran first.\nstderr:\n{}",
        String::from_utf8_lossy(&out.stderr)
    );

    let lines_after_first = fs::read_to_string(&marker)
        .expect("oneshot must have written the marker file")
        .lines()
        .count();
    assert_eq!(
        lines_after_first, 1,
        "oneshot should have run exactly once during the first start"
    );

    // --- Second start: oneshot must run again (idempotent re-run) ---
    let out2 = Command::new(fed_binary())
        .args([
            "-c",
            config_path.to_str().unwrap(),
            "-w",
            workdir,
            "start",
            "consumer",
        ])
        .env("FED_NON_INTERACTIVE", "1")
        .output()
        .expect("run fed start #2");

    println!("start#2 stdout:\n{}", String::from_utf8_lossy(&out2.stdout));
    println!("start#2 stderr:\n{}", String::from_utf8_lossy(&out2.stderr));
    assert!(
        out2.status.success(),
        "second fed start consumer must succeed too.\nstderr:\n{}",
        String::from_utf8_lossy(&out2.stderr)
    );

    let lines_after_second = fs::read_to_string(&marker)
        .expect("marker file should still exist")
        .lines()
        .count();
    assert_eq!(
        lines_after_second, 2,
        "a oneshot must re-run on EVERY fed start (expected two lines, got {})",
        lines_after_second
    );

    fed_stop(&config_path, workdir);
}

/// (c) A oneshot whose run command fails aborts startup, and the error names the
/// oneshot service.
#[test]
fn test_oneshot_failure_aborts_start_with_service_name() {
    let temp = tempdir().expect("temp dir");
    let config_path = temp.path().join("service-federation.yaml");
    let workdir = temp.path().to_str().unwrap();

    let config = r#"
services:
  base:
    process: "sleep 30"
  schema:
    run: "exit 1"
    depends_on:
      - base
  consumer:
    process: "sleep 30"
    depends_on:
      - schema
    startup_message: "consumer is up"
"#;
    fs::write(&config_path, config).expect("write config");

    let out = Command::new(fed_binary())
        .args([
            "-c",
            config_path.to_str().unwrap(),
            "-w",
            workdir,
            "start",
            "consumer",
        ])
        .env("FED_NON_INTERACTIVE", "1")
        .output()
        .expect("run fed start");

    let combined = format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    println!("failing-oneshot output:\n{}", combined);

    assert!(
        !out.status.success(),
        "fed start must fail when a oneshot's run command exits non-zero"
    );
    assert!(
        combined.contains("schema"),
        "the startup error must name the failing oneshot service 'schema'. Got:\n{}",
        combined
    );

    fed_stop(&config_path, workdir);
}

// ---------------------------------------------------------------------------
// (d) Validation
// ---------------------------------------------------------------------------

/// `run` is mutually exclusive with the other type-defining fields.
#[test]
fn test_run_plus_process_is_rejected() {
    let yaml = r#"
services:
  bad:
    run: "echo hi"
    process: "sleep 30"
"#;
    let config = Parser::new().parse_config(yaml).expect("parse");
    let err = config
        .validate()
        .expect_err("run + process must be rejected")
        .to_string();
    assert!(
        err.contains("multiple type-defining fields")
            && err.contains("run")
            && err.contains("process"),
        "error should call out the run/process conflict, got: {err}"
    );
}

/// A oneshot's completion IS its readiness — a healthcheck is contradictory.
#[test]
fn test_run_plus_healthcheck_is_rejected() {
    let yaml = r#"
services:
  schema:
    run: "echo hi"
    healthcheck:
      command: "true"
"#;
    let config = Parser::new().parse_config(yaml).expect("parse");
    let err = config
        .validate()
        .expect_err("run + healthcheck must be rejected")
        .to_string();
    assert!(
        err.contains("schema") && err.to_lowercase().contains("healthcheck"),
        "error should reject a healthcheck on a oneshot, got: {err}"
    );
}

/// A oneshot runs once to completion — a restart policy is contradictory.
#[test]
fn test_run_plus_restart_is_rejected() {
    let yaml = r#"
services:
  schema:
    run: "echo hi"
    restart: "always"
"#;
    let config = Parser::new().parse_config(yaml).expect("parse");
    let err = config
        .validate()
        .expect_err("run + restart must be rejected")
        .to_string();
    assert!(
        err.contains("schema") && err.to_lowercase().contains("restart"),
        "error should reject a restart policy on a oneshot, got: {err}"
    );
}

/// A lone `run:` service is a valid, complete service definition.
#[test]
fn test_lone_run_is_a_valid_service_type() {
    let yaml = r#"
services:
  schema:
    run: "echo hi"
"#;
    let config = Parser::new().parse_config(yaml).expect("parse");
    assert!(
        config.validate().is_ok(),
        "a service with only `run:` should be a valid oneshot"
    );
}

// ---------------------------------------------------------------------------
// (e) Concurrency: a shared oneshot runs exactly once per startup
// ---------------------------------------------------------------------------

/// Two services depend on the same oneshot. Started concurrently, the second
/// must wait for the first oneshot execution to finish and must NOT re-run it
/// within the same startup — the run command appends one line, and we assert
/// exactly one line after both chains complete.
#[tokio::test]
async fn test_shared_oneshot_runs_once_under_concurrent_starts() {
    let temp = tempdir().expect("temp dir");
    let workdir = temp.path().to_path_buf();
    let marker = temp.path().join("schema-ran.log");

    let yaml = format!(
        r#"
services:
  base:
    process: "sleep 30"
  schema:
    run: "echo ran >> {marker}"
    depends_on:
      - base
  svc1:
    process: "sleep 30"
    depends_on:
      - schema
  svc2:
    process: "sleep 30"
    depends_on:
      - schema
"#,
        marker = marker.display()
    );

    let config = Parser::new().parse_config(&yaml).expect("parse");
    let orch_temp = tempdir().unwrap();
    let mut orch = Orchestrator::new(config, orch_temp.path().to_path_buf())
        .await
        .unwrap();
    orch.set_work_dir(workdir).await.expect("set work dir");
    orch.set_auto_resolve_conflicts(true);
    orch.initialize().await.expect("initialize");

    // Start both dependent chains concurrently. Both walk through `schema`.
    let (r1, r2) = tokio::join!(orch.start("svc1"), orch.start("svc2"));
    r1.expect("svc1 chain should start");
    r2.expect("svc2 chain should start");

    let lines = fs::read_to_string(&marker)
        .expect("oneshot should have written the marker")
        .lines()
        .count();
    assert_eq!(
        lines, 1,
        "a shared oneshot must run exactly once per startup even under concurrent \
         dependents (expected 1 line, got {})",
        lines
    );

    // Both dependents should be up, and the oneshot should read as Completed.
    assert!(
        orch.is_service_running("svc1").await,
        "svc1 should be running"
    );
    assert!(
        orch.is_service_running("svc2").await,
        "svc2 should be running"
    );
    let status = orch.get_status().await;
    assert_eq!(
        status.get("schema").copied(),
        Some(Status::Completed),
        "the oneshot should report Completed after running"
    );

    orch.stop_all().await.ok();
    // Give processes a moment to actually exit before the temp dir is dropped.
    tokio::time::sleep(Duration::from_millis(50)).await;
}
