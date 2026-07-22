//! Integration test for the stale-grace-period mitigation
//! (`07-supervisor.md` Design §3, Phase 4): a concurrent `fed` command's
//! `mark_dead_services` container-liveness check must not permanently stale
//! a native-restart-enabled Docker service's row while the container is
//! momentarily not running (Docker's own restart-backoff window) — and once
//! the container is back up, the row must reconcile to healthy instead of
//! staying hidden from `get_services()`.
//!
//! This drives the exact SQL/liveness path `SqliteStateTracker` uses against
//! a *real* Docker container, but drives the container's stop/start
//! transitions directly (rather than racing Docker's own restart-policy
//! timer) so the test is deterministic in CI. The pure counter mechanics
//! (exactly how many consecutive failures it takes to actually go stale) are
//! covered by the non-Docker unit tests in
//! `src/state/sqlite/service_crud.rs` (`STALE_GRACE_THRESHOLD`), which have
//! direct access to that private constant; this test only needs to prove
//! the end-to-end reconciliation property against a real container.

use fed::config::ServiceType;
use fed::service::Status;
use fed::state::{DesiredState, ServiceState, SqliteStateTracker};
use std::collections::HashMap;
use std::time::Duration;

fn is_docker_available() -> bool {
    std::process::Command::new("docker")
        .arg("version")
        .output()
        .map(|output| output.status.success())
        .unwrap_or(false)
}

macro_rules! require_docker {
    () => {
        if !is_docker_available() {
            eprintln!("Skipping test: Docker not available");
            return;
        }
    };
}

fn docker(args: &[&str]) -> std::process::Output {
    std::process::Command::new("docker")
        .args(args)
        .output()
        .expect("failed to run docker")
}

fn native_restart_service_state(container_id: &str) -> ServiceState {
    ServiceState {
        id: "flaky-container".to_string(),
        status: Status::Running,
        service_type: ServiceType::Docker,
        pid: None,
        container_id: Some(container_id.to_string()),
        started_at: chrono::Utc::now(),
        external_repo: None,
        namespace: "root".to_string(),
        restart_count: 0,
        last_restart_at: None,
        consecutive_failures: 0,
        port_allocations: HashMap::new(),
        startup_message: None,
        desired_state: DesiredState::Running,
        native_restart_enabled: true,
    }
}

#[tokio::test]
#[cfg_attr(not(feature = "docker-tests"), ignore)] // Requires Docker
async fn native_restart_service_reconciles_instead_of_staying_stale() {
    require_docker!();

    let container_name = "fed-test-stale-grace";
    docker(&["rm", "-f", container_name]);

    // A real, long-lived container — its own process never exits on its
    // own in this test; we drive "not running right now" and "running
    // again" directly via `docker stop`/`docker start`, standing in for
    // Docker's own native-restart backoff window (Design §3's residual
    // race).
    let run = docker(&[
        "run",
        "-d",
        "--name",
        container_name,
        "alpine:latest",
        "sleep",
        "300",
    ]);
    assert!(
        run.status.success(),
        "docker run failed: {}",
        String::from_utf8_lossy(&run.stderr)
    );
    let container_id = String::from_utf8_lossy(&run.stdout).trim().to_string();

    let temp_dir = tempfile::tempdir().unwrap();
    let mut tracker = SqliteStateTracker::new(temp_dir.path().to_path_buf())
        .await
        .expect("create tracker");
    tracker.initialize().await.expect("initialize tracker");

    tracker
        .register_service(native_restart_service_state(&container_id))
        .await
        .expect("register service");

    // Baseline: container is genuinely running, must not be touched.
    let marked = tracker
        .mark_dead_services()
        .await
        .expect("mark_dead_services");
    assert!(
        marked.is_empty(),
        "a live container must never be marked stale"
    );
    assert_eq!(tracker.get_services().await.len(), 1);

    // Simulate Docker's own restart-backoff window: the container is
    // genuinely not running right now (from a concurrent `fed` command's
    // point of view), same signal `mark_dead_services`'s
    // `is_container_running` check reacts to.
    let stop = docker(&["stop", "-t", "1", container_name]);
    assert!(stop.status.success(), "docker stop failed");

    // A couple of liveness checks while it's down: within the grace
    // period, the row must stay visible — this is the direct regression
    // test for the state-reconciliation gap (a naive one-shot check would
    // mark it stale on the very first of these).
    for attempt in 1..=2 {
        let marked = tracker
            .mark_dead_services()
            .await
            .expect("mark_dead_services while container is down");
        assert!(
            marked.is_empty(),
            "attempt {attempt}: native-restart-enabled service must not be marked stale \
             this quickly — it should get a grace period, not a one-shot check"
        );
        assert_eq!(
            tracker.get_services().await.len(),
            1,
            "attempt {attempt}: service must still be visible via get_services() during \
             the grace period"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    // Docker's own restart succeeds (simulated here by us starting the same
    // container back up, standing in for `--restart unless-stopped`
    // bringing it back).
    let start = docker(&["start", container_name]);
    assert!(start.status.success(), "docker start failed");
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Reconciliation: the very next liveness check must see it running
    // again and leave the row alone — status reconciles, not permanently
    // hidden.
    let marked = tracker
        .mark_dead_services()
        .await
        .expect("mark_dead_services after docker restarted the container");
    assert!(
        marked.is_empty(),
        "container is running again — must not be (or stay) marked stale"
    );
    let services = tracker.get_services().await;
    assert_eq!(
        services.len(),
        1,
        "service row must have survived the whole backoff window and still be visible"
    );
    let state = services
        .get("flaky-container")
        .expect("service must still be present");
    assert!(state.native_restart_enabled);

    docker(&["rm", "-f", container_name]);
}
