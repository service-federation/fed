//! Schema-shape tests for `fed status --json`'s enriched per-service object
//! (03-status-json.md): `schema_version`, `health`, `service_type`, `pid`,
//! `container_id`, `started_at`, `uptime_seconds`, `ports`, `startup_message`.
//!
//! `pid`/`container_id`/`started_at`/`uptime_seconds` are inherently
//! non-deterministic per test run, so these assert *shape* via
//! `serde_json::Value`, not literal fixture diffing — a future maintainer
//! should not try to force a byte-exact comparison here.
//!
//! `tests/cli_subcommands_test.rs::test_status_json_output` already covers
//! the loose "is this valid JSON" baseline and is intentionally left
//! untouched (additive-only requirement) — these tests live in their own
//! file rather than extending that one.

use std::fs;
use std::process::Command;
use std::time::Duration;
use tempfile::TempDir;

fn fed_binary() -> String {
    env!("CARGO_BIN_EXE_fed").to_string()
}

fn fed(temp_dir: &TempDir, args: &[&str]) -> std::process::Output {
    Command::new(fed_binary())
        .arg("-w")
        .arg(temp_dir.path())
        .arg("-c")
        .arg(temp_dir.path().join("fed.yaml"))
        .args(args)
        .current_dir(temp_dir.path())
        .env("FED_NON_INTERACTIVE", "1")
        .output()
        .expect("Failed to run fed")
}

fn combined(output: &std::process::Output) -> String {
    format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    )
}

fn status_json(temp_dir: &TempDir) -> serde_json::Value {
    let out = fed(temp_dir, &["status", "--json"]);
    assert!(
        out.status.success(),
        "status --json failed: {}",
        combined(&out)
    );
    serde_json::from_str(&String::from_utf8_lossy(&out.stdout))
        .unwrap_or_else(|e| panic!("status --json did not emit valid JSON: {e}\n{out:?}"))
}

/// Process service, never-started service, and a hook-only (oneshot) service
/// in one project, exercising test-plan items 1, 4, 5, 6 together.
fn write_project(temp_dir: &TempDir) {
    let config = r#"
services:
  migrate:
    install: "echo installed"

  app:
    process: "sleep 30"
    startup_message: "http://localhost:1234"
    depends_on:
      - migrate

  never-started:
    process: "sleep 30"
"#;
    fs::write(temp_dir.path().join("fed.yaml"), config).expect("write config");
}

#[test]
fn status_json_schema_process_oneshot_and_never_started() {
    let temp_dir = TempDir::new().unwrap();
    write_project(&temp_dir);

    let start = fed(&temp_dir, &["start", "app"]);
    assert!(start.status.success(), "start failed: {}", combined(&start));

    // Give the health/status machinery a moment to settle before reading.
    std::thread::sleep(Duration::from_millis(500));

    let data = status_json(&temp_dir);

    // --- item 1: started process service ---
    let app = &data["app"];
    assert_eq!(app["schema_version"], 1);
    let app_status = app["status"].as_str().unwrap();
    assert!(
        app_status == "running" || app_status == "healthy",
        "expected app status running/healthy, got {app_status}"
    );
    let app_health = app["health"].as_str().unwrap();
    assert!(
        [
            "healthy",
            "unhealthy",
            "starting",
            "stopping",
            "stopped",
            "unknown"
        ]
        .contains(&app_health),
        "unexpected health bucket: {app_health}"
    );
    assert_eq!(app["service_type"], "process");
    assert!(
        app["pid"].as_u64().is_some_and(|p| p > 0),
        "expected positive pid, got {:?}",
        app["pid"]
    );
    assert!(app["container_id"].is_null());
    let started_at = app["started_at"].as_str().expect("started_at present");
    chrono::DateTime::parse_from_rfc3339(started_at).expect("started_at must be RFC3339");
    assert!(
        app["uptime_seconds"].as_i64().is_some_and(|u| u >= 0),
        "expected non-negative uptime_seconds, got {:?}",
        app["uptime_seconds"]
    );
    assert!(app["ports"].is_object());
    assert_eq!(app["startup_message"], "http://localhost:1234");

    // --- item 5: configured-but-never-started service ---
    let never = &data["never-started"];
    assert_eq!(never["schema_version"], 1);
    assert_eq!(never["status"], "stopped");
    assert_eq!(never["health"], "stopped");
    assert_eq!(
        never["service_type"], "process",
        "service_type must come from config even with no persisted ServiceState row"
    );
    assert!(never["pid"].is_null());
    assert!(never["container_id"].is_null());
    assert!(never["started_at"].is_null());
    assert!(never["uptime_seconds"].is_null());
    assert_eq!(never["ports"], serde_json::json!({}));
    assert!(never["startup_message"].is_null());

    // --- item 4: oneshot/Completed shape ---
    let migrate = &data["migrate"];
    assert_eq!(migrate["schema_version"], 1);
    assert_eq!(migrate["status"], "completed");
    assert_eq!(migrate["health"], "healthy");
    assert_eq!(migrate["service_type"], "oneshot");
    assert!(migrate["pid"].is_null());
    assert!(migrate["container_id"].is_null());
    assert!(
        migrate["started_at"].as_str().is_some(),
        "oneshot started_at should be non-null"
    );
    assert!(
        migrate["uptime_seconds"].as_i64().is_some_and(|u| u >= 0),
        "oneshot uptime_seconds must be non-negative, not null (uniform-uptime rationale)"
    );

    // --- item 1 (continued): after stop, enrichment fields go back to null ---
    let stop = fed(&temp_dir, &["stop"]);
    assert!(stop.status.success(), "stop failed: {}", combined(&stop));

    let data_after_stop = status_json(&temp_dir);
    let app_after = &data_after_stop["app"];
    assert_eq!(app_after["status"], "stopped");
    assert_eq!(app_after["health"], "stopped");
    assert!(app_after["pid"].is_null());
    assert!(app_after["container_id"].is_null());
    assert!(app_after["started_at"].is_null());
    assert!(app_after["uptime_seconds"].is_null());
}

/// Cross-process regression test for compose `container_id` (task
/// requirement 3): a separate `fed` process must be able to read the
/// compose service's container id via `status --json`, not just the process
/// that started it. Mirrors `compose_cross_process_test.rs`'s pattern.
#[test]
#[cfg_attr(not(feature = "docker-tests"), ignore)] // Requires Docker (CI: cargo test --features docker-tests)
fn compose_service_container_id_visible_in_status_json_across_processes() {
    let temp_dir = TempDir::new().unwrap();

    let compose = r#"
services:
  kv:
    image: redis:7-alpine
"#;
    fs::write(temp_dir.path().join("docker-compose.yml"), compose).unwrap();
    let fed_yaml = r#"
services:
  kv:
    compose_file: ./docker-compose.yml
    compose_service: kv
"#;
    fs::write(temp_dir.path().join("fed.yaml"), fed_yaml).unwrap();

    let start = fed(&temp_dir, &["start", "kv"]);
    assert!(start.status.success(), "start failed: {}", combined(&start));

    // A brand-new fed process (separate `status --json` invocation) must see
    // the persisted container id.
    let data = status_json(&temp_dir);
    let kv = &data["kv"];
    assert_eq!(kv["service_type"], "docker-compose");
    let container_id = kv["container_id"]
        .as_str()
        .expect("compose container_id must be visible across process boundary");
    assert!(!container_id.is_empty());

    // Cross-check the exact container_id returned by `status --json` directly
    // via `docker inspect` (rather than a host-wide `docker ps` name-substring
    // search, which is fragile if unrelated containers from other test runs
    // happen to share the "-kv-" substring).
    let inspect = Command::new("docker")
        .args([
            "inspect",
            "--format",
            "{{.Name}} {{.State.Running}}",
            container_id,
        ])
        .output()
        .expect("docker inspect failed");
    assert!(
        inspect.status.success(),
        "docker inspect {container_id} failed: {}",
        String::from_utf8_lossy(&inspect.stderr)
    );
    let inspect_out = String::from_utf8_lossy(&inspect.stdout);
    assert!(
        inspect_out.contains("kv") && inspect_out.contains("true"),
        "container {container_id} from status --json is not the running kv container: {inspect_out}"
    );

    let stop = fed(&temp_dir, &["stop"]);
    assert!(stop.status.success(), "stop failed: {}", combined(&stop));
}
