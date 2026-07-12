//! Integration tests for hook-only services and `migrate:` semantics (fed 6.0).
//!
//! fed 6.0 collapsed three overlapping hooks into two:
//!   * `install:` — once per scope, marker-gated (unchanged).
//!   * `migrate:` — runs on EVERY start, after dependencies are healthy and
//!     before the service starts / counts as ready. No marker, no fingerprint;
//!     idempotency is the documented contract (e.g. `prisma db push`).
//!
//! A service with `install:` and/or `migrate:` but NO process/image/gradle/
//! compose field is a *hook-only service* — the oneshot node. It reuses the
//! oneshot machinery: it completes when its hooks finish, shows as `Completed`,
//! gates its dependents on that completion, and a hook failure aborts `fed start`
//! naming the node. Concurrent dependents get one execution per startup.
//!
//! `run:` was removed in 6.0 — a config declaring it fails validation.
//!
//! The driving real-world bug (Plenora): install/migrate were attached to the
//! LAST service in the graph and ran just-in-time before that service spawned,
//! so earlier services booted against an un-migrated database. A hook-only node
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

/// (a) `fed start consumer` succeeds and the hook-only node's `migrate:` ran
/// before consumer spawned.
///
/// `schema` (hook-only: only `migrate:`) appends a line to a file. `consumer`
/// FAILS to start unless that file exists (`test -f FILE && exec sleep 30`). So
/// `fed start consumer` succeeding is proof the migrate completed before consumer
/// spawned.
///
/// (b) A second start re-runs `migrate:` — the file ends up with two lines.
/// (Converted from `test_oneshot_runs_before_dependent_and_reruns_every_start`:
/// the ordering + every-start contract is identical, now carried by `migrate:`
/// on a hook-only node instead of `run:`.)
#[test]
fn test_hookonly_migrate_runs_before_dependent_and_reruns_every_start() {
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
    migrate: "echo ran >> {marker}"
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
        "fed start consumer must succeed — consumer only starts if migrate ran first.\nstderr:\n{}",
        String::from_utf8_lossy(&out.stderr)
    );

    let lines_after_first = fs::read_to_string(&marker)
        .expect("migrate must have written the marker file")
        .lines()
        .count();
    assert_eq!(
        lines_after_first, 1,
        "migrate should have run exactly once during the first start"
    );

    // --- Second start: migrate must run again (idempotent re-run) ---
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
        "migrate must re-run on EVERY fed start (expected two lines, got {})",
        lines_after_second
    );

    fed_stop(&config_path, workdir);
}

/// `migrate:` on a normal process service runs after deps are healthy and BEFORE
/// that service's own process spawns. `api`'s process refuses to start unless the
/// migrate marker exists, so a successful start proves the ordering.
#[test]
fn test_migrate_runs_before_own_process_spawns() {
    let temp = tempdir().expect("temp dir");
    let config_path = temp.path().join("service-federation.yaml");
    let workdir = temp.path().to_str().unwrap();
    let marker = temp.path().join("api-migrated.log");

    let config = format!(
        r#"
services:
  base:
    process: "sleep 30"
  api:
    process: "test -f {marker} && exec sleep 30"
    migrate: "echo migrated >> {marker}"
    depends_on:
      - base
    startup_message: "api is up"
"#,
        marker = marker.display()
    );
    fs::write(&config_path, &config).expect("write config");

    let out = Command::new(fed_binary())
        .args([
            "-c",
            config_path.to_str().unwrap(),
            "-w",
            workdir,
            "start",
            "api",
        ])
        .env("FED_NON_INTERACTIVE", "1")
        .output()
        .expect("run fed start");

    println!("stdout:\n{}", String::from_utf8_lossy(&out.stdout));
    println!("stderr:\n{}", String::from_utf8_lossy(&out.stderr));
    assert!(
        out.status.success(),
        "fed start api must succeed — api's process only spawns if its migrate ran first.\nstderr:\n{}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(
        marker.exists(),
        "migrate should have written its marker before the process spawned"
    );

    fed_stop(&config_path, workdir);
}

/// A hook-only node whose `migrate:` command fails aborts startup, and the error
/// names the node. (Converted from `test_oneshot_failure_aborts_start_with_service_name`.)
#[test]
fn test_hookonly_migrate_failure_aborts_start_with_service_name() {
    let temp = tempdir().expect("temp dir");
    let config_path = temp.path().join("service-federation.yaml");
    let workdir = temp.path().to_str().unwrap();

    let config = r#"
services:
  base:
    process: "sleep 30"
  schema:
    migrate: "exit 1"
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
    println!("failing-migrate output:\n{}", combined);

    assert!(
        !out.status.success(),
        "fed start must fail when a hook-only node's migrate command exits non-zero"
    );
    assert!(
        combined.contains("schema"),
        "the startup error must name the failing node 'schema'. Got:\n{}",
        combined
    );

    fed_stop(&config_path, workdir);
}

// ---------------------------------------------------------------------------
// (d) Validation
// ---------------------------------------------------------------------------

/// `run:` was removed in 6.0 — any config declaring it fails validation with the
/// migration guidance. (Converted from `test_run_plus_process_is_rejected` and
/// `test_lone_run_is_a_valid_service_type`, which encoded `run:` as a live type.)
#[test]
fn test_run_field_is_rejected_in_6_0() {
    // Lone `run:` — previously a valid oneshot type.
    let yaml = r#"
services:
  schema:
    run: "echo hi"
"#;
    let config = Parser::new().parse_config(yaml).expect("parse");
    let err = config
        .validate()
        .expect_err("run: must be rejected in 6.0")
        .to_string();
    assert!(
        err.contains("run: was removed in 6.0"),
        "error should explain run: was removed in 6.0, got: {err}"
    );
    assert!(
        err.contains("migrate:"),
        "error should point users at migrate:, got: {err}"
    );

    // `run:` alongside a process is likewise rejected with the same guidance
    // (previously this produced a "multiple type-defining fields" message).
    let yaml2 = r#"
services:
  bad:
    run: "echo hi"
    process: "sleep 30"
"#;
    let config2 = Parser::new().parse_config(yaml2).expect("parse");
    let err2 = config2
        .validate()
        .expect_err("run + process must be rejected")
        .to_string();
    assert!(
        err2.contains("run: was removed in 6.0"),
        "run + process should also surface the 6.0 removal error, got: {err2}"
    );
}

/// A hook-only node completes to signal readiness — a healthcheck is
/// contradictory. (Converted from `test_run_plus_healthcheck_is_rejected`.)
#[test]
fn test_hookonly_plus_healthcheck_is_rejected() {
    let yaml = r#"
services:
  schema:
    migrate: "echo hi"
    healthcheck:
      command: "true"
"#;
    let config = Parser::new().parse_config(yaml).expect("parse");
    let err = config
        .validate()
        .expect_err("hook-only + healthcheck must be rejected")
        .to_string();
    assert!(
        err.contains("schema") && err.to_lowercase().contains("healthcheck"),
        "error should reject a healthcheck on a hook-only node, got: {err}"
    );
}

/// A hook-only node runs its hooks to completion — a restart policy is
/// contradictory. (Converted from `test_run_plus_restart_is_rejected`.)
#[test]
fn test_hookonly_plus_restart_is_rejected() {
    let yaml = r#"
services:
  schema:
    migrate: "echo hi"
    restart: "always"
"#;
    let config = Parser::new().parse_config(yaml).expect("parse");
    let err = config
        .validate()
        .expect_err("hook-only + restart must be rejected")
        .to_string();
    assert!(
        err.contains("schema") && err.to_lowercase().contains("restart"),
        "error should reject a restart policy on a hook-only node, got: {err}"
    );
}

/// A service with only `migrate:` is a valid, complete hook-only node.
/// (Converted from `test_lone_run_is_a_valid_service_type`.)
#[test]
fn test_lone_migrate_is_a_valid_service_type() {
    let yaml = r#"
services:
  schema:
    migrate: "echo hi"
"#;
    let config = Parser::new().parse_config(yaml).expect("parse");
    assert!(
        config.validate().is_ok(),
        "a service with only `migrate:` should be a valid hook-only node"
    );
}

/// A service with only `install:` is a valid, complete hook-only node.
#[test]
fn test_lone_install_is_a_valid_service_type() {
    let yaml = r#"
services:
  deps:
    install: "echo hi"
"#;
    let config = Parser::new().parse_config(yaml).expect("parse");
    assert!(
        config.validate().is_ok(),
        "a service with only `install:` should be a valid hook-only node"
    );
}

/// A service with neither hooks nor a type-defining field is still rejected, and
/// the message now mentions hook-only services.
#[test]
fn test_empty_service_still_rejected() {
    let yaml = r#"
services:
  empty: {}
"#;
    let config = Parser::new().parse_config(yaml).expect("parse");
    let err = config
        .validate()
        .expect_err("a service with no type and no hooks must be rejected")
        .to_string();
    assert!(
        err.contains("empty") && err.contains("no type defined"),
        "error should reject the empty service by name, got: {err}"
    );
    assert!(
        err.contains("install") || err.contains("migrate"),
        "the no-type message should mention hook-only services (install/migrate), got: {err}"
    );
}

// ---------------------------------------------------------------------------
// (e) install-only hook nodes: once per scope, but the node still completes
// ---------------------------------------------------------------------------

/// A hook-only node with only `install:` runs its install once per scope (the
/// marker suppresses re-runs), yet the node still *completes* on every start so
/// its dependents proceed — including the second start where install is skipped.
#[test]
fn test_hookonly_install_only_completes_and_gates_dependent() {
    let temp = tempdir().expect("temp dir");
    let config_path = temp.path().join("service-federation.yaml");
    let workdir = temp.path().to_str().unwrap();
    let marker = temp.path().join("install-ran.log");

    let config = format!(
        r#"
services:
  base:
    process: "sleep 30"
  deps:
    install: "echo ran >> {marker}"
    depends_on:
      - base
  consumer:
    process: "sleep 30"
    depends_on:
      - deps
    startup_message: "consumer is up"
"#,
        marker = marker.display()
    );
    fs::write(&config_path, &config).expect("write config");

    let start = || {
        Command::new(fed_binary())
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
            .expect("run fed start")
    };

    // First start: install runs once, consumer comes up behind the completed node.
    let out1 = start();
    assert!(
        out1.status.success(),
        "first fed start consumer must succeed.\nstderr:\n{}",
        String::from_utf8_lossy(&out1.stderr)
    );
    assert_eq!(
        fs::read_to_string(&marker).unwrap().lines().count(),
        1,
        "install should have run exactly once"
    );

    // Second start: install is marker-gated (skipped), but the node still
    // completes immediately and consumer starts again.
    let out2 = start();
    assert!(
        out2.status.success(),
        "second fed start consumer must succeed — the install-only node completes even when the install is skipped.\nstderr:\n{}",
        String::from_utf8_lossy(&out2.stderr)
    );
    assert_eq!(
        fs::read_to_string(&marker).unwrap().lines().count(),
        1,
        "install must NOT re-run — it is once per scope (still one line)"
    );

    fed_stop(&config_path, workdir);
}

// ---------------------------------------------------------------------------
// (f) Concurrency: a shared hook-only node runs its hooks exactly once per startup
// ---------------------------------------------------------------------------

/// Two services depend on the same hook-only node. Started concurrently, the
/// second must wait for the first execution to finish and must NOT re-run the
/// migrate within the same startup — the migrate appends one line, and we assert
/// exactly one line after both chains complete.
/// (Converted from `test_shared_oneshot_runs_once_under_concurrent_starts`.)
#[tokio::test]
async fn test_shared_hookonly_migrate_runs_once_under_concurrent_starts() {
    let temp = tempdir().expect("temp dir");
    let workdir = temp.path().to_path_buf();
    let marker = temp.path().join("schema-ran.log");

    let yaml = format!(
        r#"
services:
  base:
    process: "sleep 30"
  schema:
    migrate: "echo ran >> {marker}"
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
        .expect("migrate should have written the marker")
        .lines()
        .count();
    assert_eq!(
        lines, 1,
        "a shared hook-only node's migrate must run exactly once per startup even \
         under concurrent dependents (expected 1 line, got {})",
        lines
    );

    // Both dependents should be up, and the node should read as Completed.
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
        "the hook-only node should report Completed after running"
    );

    orch.stop_all().await.ok();
    // Give processes a moment to actually exit before the temp dir is dropped.
    tokio::time::sleep(Duration::from_millis(50)).await;
}
