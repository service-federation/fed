//! Integration tests for the `fed supervise` daemon (`07-supervisor.md`
//! Design §1/§5/§6/§7, Phase 3: the daemon itself).
//!
//! These drive the real `fed` binary across separate process invocations —
//! exactly the cross-process contract the supervisor exists to coordinate
//! through SQLite and `.fed/supervisor.lock`, not through any in-memory
//! object. Health checks tick every 5s +/- jitter (`monitoring.rs`), so these
//! tests use generous, bounded polling loops rather than fixed sleeps tuned
//! to exact tick timing.

use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{Duration, Instant};

fn fed_binary() -> String {
    env!("CARGO_BIN_EXE_fed").to_string()
}

fn create_test_config(temp_dir: &tempfile::TempDir, content: &str) -> PathBuf {
    let config_path = temp_dir.path().join("fed.yaml");
    fs::write(&config_path, content).expect("Failed to write test config");
    config_path
}

fn run_fed(config_path: &Path, workdir: &Path, args: &[&str]) -> std::process::Output {
    Command::new(fed_binary())
        .arg("-c")
        .arg(config_path)
        .arg("-w")
        .arg(workdir)
        .args(args)
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

fn is_pid_alive(pid: u32) -> bool {
    Command::new("kill")
        .args(["-0", &pid.to_string()])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

fn kill9(pid: u32) {
    let _ = Command::new("kill").args(["-9", &pid.to_string()]).status();
}

fn sighup(pid: u32) {
    let _ = Command::new("kill")
        .args(["-HUP", &pid.to_string()])
        .status();
}

fn wait_for_pid_dead(pid: u32, timeout: Duration) -> bool {
    let start = Instant::now();
    while start.elapsed() < timeout {
        if !is_pid_alive(pid) {
            return true;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    !is_pid_alive(pid)
}

/// Read `.fed/supervisor.lock`'s PID marker directly (format: "<pid>
/// fed-supervisor\n", written by `orchestrator::supervisor::try_acquire`).
/// Does NOT attempt to distinguish live-vs-stale here (unlike
/// `live_supervisor_pid` in the library) — callers combine this with
/// `is_pid_alive` themselves, since the point of several of these tests is
/// observing the transition between the two.
fn read_supervisor_lock_pid(workdir: &Path) -> Option<u32> {
    let contents = fs::read_to_string(workdir.join(".fed").join("supervisor.lock")).ok()?;
    contents.split_whitespace().next()?.parse().ok()
}

/// Poll until `.fed/supervisor.lock` names a PID that is actually alive,
/// i.e. a supervisor has spawned and acquired the lock.
fn wait_for_live_supervisor(workdir: &Path, timeout: Duration) -> u32 {
    let start = Instant::now();
    loop {
        if let Some(pid) = read_supervisor_lock_pid(workdir)
            && is_pid_alive(pid)
        {
            return pid;
        }
        assert!(
            start.elapsed() < timeout,
            "no live supervisor appeared for {:?} within {:?}",
            workdir,
            timeout
        );
        std::thread::sleep(Duration::from_millis(100));
    }
}

/// Read `.fed/lock.db` directly for polling, rather than shelling out to
/// `fed debug state --json`. This matters, not just style: every `fed`
/// command's `.initialize()` runs `mark_dead_services` unconditionally, and
/// a plain process service under supervision is briefly, genuinely dead
/// between the test explicitly killing its PID and the supervisor's own
/// restart landing — a *second* concurrent `fed` command (this test's own
/// polling, if it shelled out) that happened to run its `.initialize()` in
/// that exact window would mark the row 'stale', permanently hiding it from
/// every future `get_services()`-based read (this is the generalized,
/// non-Docker-specific version of the same race Design §3 documents and
/// explicitly accepts for Docker's native-restart backoff window — nothing
/// in the ordinary crash-restart path un-stales a row once an unrelated
/// command's `mark_dead_services` pass catches it mid-restart). A direct,
/// read-only DB query has no such side effect, matching the pattern already
/// established in `tests/desired_state_test.rs`.
fn open_lock_db(workdir: &Path) -> Option<rusqlite::Connection> {
    rusqlite::Connection::open(workdir.join(".fed/lock.db")).ok()
}

fn service_pid(workdir: &Path, service: &str) -> Option<u32> {
    let conn = open_lock_db(workdir)?;
    conn.query_row(
        "SELECT pid FROM services WHERE id = ?1",
        rusqlite::params![service],
        |row| row.get::<_, Option<u32>>(0),
    )
    .ok()
    .flatten()
}

fn restart_count(workdir: &Path, service: &str) -> u64 {
    let Some(conn) = open_lock_db(workdir) else {
        return 0;
    };
    conn.query_row(
        "SELECT restart_count FROM services WHERE id = ?1",
        rusqlite::params![service],
        |row| row.get::<_, u32>(0),
    )
    .map(|c| c as u64)
    .unwrap_or(0)
}

/// Poll `restart_count` until it exceeds `above`. `record_restart` and the
/// batched `restart_count` column increment happen as two separate steps
/// inside `execute_health_check_cycle` (the PID update lands first) — a
/// single read taken right after observing a new PID can still catch the
/// window before the count itself is incremented, so callers that need
/// both must poll rather than read once.
fn wait_for_restart_count_above(
    workdir: &Path,
    service: &str,
    above: u64,
    timeout: Duration,
) -> u64 {
    let start = Instant::now();
    loop {
        let count = restart_count(workdir, service);
        if count > above {
            return count;
        }
        assert!(
            start.elapsed() < timeout,
            "restart_count for '{}' did not exceed {} within {:?} (last seen: {})",
            service,
            above,
            timeout,
            count
        );
        std::thread::sleep(Duration::from_millis(300));
    }
}

/// A long-running service that never exits on its own — it only "crashes"
/// when the test explicitly kills its PID. `restart: always` so the
/// supervisor should always bring it back regardless of failure count.
const STEADY_ALWAYS_CONFIG: &str = r#"
services:
  steady:
    process: sleep 300
    restart: always
"#;

/// Two independent long-running, `restart: always` services. Used by the
/// partial-stop resurrection test: stopping only one of them must leave the
/// other's presence in the supervisor's scope, so the daemon has a reason to
/// stay alive throughout (a single-service project would have the
/// supervisor correctly self-exit once its only service is stopped, which
/// would defeat the point of that test).
const TWO_STEADY_ALWAYS_CONFIG: &str = r#"
services:
  steady:
    process: sleep 301
    restart: always
  keepalive:
    process: sleep 302
    restart: always
"#;

/// Plain `fed start` of a `restart: always` service must spawn a
/// supervisor daemon (`spawn_if_needed`, wired into `start.rs`'s non-watch
/// branch) — nothing supervises a backgrounded service otherwise.
#[test]
fn test_plain_start_spawns_supervisor_for_restart_always_service() {
    let temp_dir = tempfile::tempdir().unwrap();
    let workdir = temp_dir.path();
    let config_path = create_test_config(&temp_dir, STEADY_ALWAYS_CONFIG);

    let start = run_fed(&config_path, workdir, &["start", "steady"]);
    assert!(start.status.success(), "start failed: {}", combined(&start));

    let pid = wait_for_live_supervisor(workdir, Duration::from_secs(10));
    assert!(is_pid_alive(pid));

    // Cleanup: stop quiesces the daemon too (best-effort).
    let _ = run_fed(&config_path, workdir, &["stop"]);
}

/// The direct regression test for hole #2 (attach-and-reconcile) combined
/// with the phase-3 acceptance test: `kill -9` the supervisor, verify a
/// subsequent `fed start` respawns it, and that the respawned daemon
/// resumes real supervision (a service killed *after* respawn gets
/// restarted). Also verifies `fed status` never spawns anything along the
/// way (Design's scaled-back self-heal promise).
#[test]
fn test_kill9_supervisor_then_fed_start_respawns_and_resumes_supervision() {
    let temp_dir = tempfile::tempdir().unwrap();
    let workdir = temp_dir.path();
    let config_path = create_test_config(&temp_dir, STEADY_ALWAYS_CONFIG);

    let start = run_fed(&config_path, workdir, &["start", "steady"]);
    assert!(start.status.success(), "start failed: {}", combined(&start));

    let supervisor_pid_1 = wait_for_live_supervisor(workdir, Duration::from_secs(10));

    // Kill the daemon itself (not the service) — simulates OOM/`kill -9`,
    // the crash-of-supervisor case Design explicitly accepts as a gap until
    // the next `fed start`/`fed restart`.
    kill9(supervisor_pid_1);
    assert!(
        wait_for_pid_dead(supervisor_pid_1, Duration::from_secs(5)),
        "supervisor pid {} should be dead after kill -9",
        supervisor_pid_1
    );

    // `fed status` must never respawn a supervisor — strictly read-only,
    // per the scaled-back self-heal promise.
    let status = run_fed(&config_path, workdir, &["status"]);
    assert!(
        status.status.success(),
        "status failed: {}",
        combined(&status)
    );
    assert!(
        read_supervisor_lock_pid(workdir).is_none_or(|p| !is_pid_alive(p)),
        "fed status must not respawn the supervisor"
    );

    // `fed start` of the already-running service DOES respawn — this is
    // the self-heal promise's actual owner.
    let start2 = run_fed(&config_path, workdir, &["start", "steady"]);
    assert!(
        start2.status.success(),
        "second start failed: {}",
        combined(&start2)
    );

    let supervisor_pid_2 = wait_for_live_supervisor(workdir, Duration::from_secs(10));
    assert_ne!(
        supervisor_pid_1, supervisor_pid_2,
        "the respawned supervisor should be a genuinely new process"
    );

    // Now prove the respawned daemon actually resumed supervision: kill the
    // service's real process and confirm it comes back with a new PID.
    let service_pid_before =
        service_pid(workdir, "steady").expect("steady should have a tracked pid");
    kill9(service_pid_before);

    let deadline = Instant::now() + Duration::from_secs(30);
    let mut restarted_pid = None;
    while Instant::now() < deadline {
        if let Some(pid) = service_pid(workdir, "steady")
            && pid != service_pid_before
            && is_pid_alive(pid)
        {
            restarted_pid = Some(pid);
            break;
        }
        std::thread::sleep(Duration::from_millis(300));
    }
    assert!(
        restarted_pid.is_some(),
        "respawned supervisor should have restarted 'steady' after its process was killed \
         (service kept its supervision across the supervisor's own kill -9 + respawn)"
    );

    let _ = run_fed(&config_path, workdir, &["stop"]);
}

/// The direct regression test for hole #1: a service the user explicitly
/// `fed stop`'d must never be resurrected by a *still-alive* supervisor.
/// Without gating the restart decision on the persisted `desired_state`
/// (rather than the supervisor's own in-process manager status, which a
/// separate `fed stop` invocation never touches), this would fail.
#[test]
fn test_supervisor_never_resurrects_desired_state_stopped_service() {
    let temp_dir = tempfile::tempdir().unwrap();
    let workdir = temp_dir.path();
    // Two independent restart:always services: stopping only one must
    // still leave the other in the supervisor's scope, so the daemon has a
    // reason to stay alive for the whole test — a single-service project
    // would have it correctly self-exit once its only service is stopped
    // (see Design's per-tick self-exit check), which isn't what this test
    // is checking.
    let config_path = create_test_config(&temp_dir, TWO_STEADY_ALWAYS_CONFIG);

    let start = run_fed(&config_path, workdir, &["start", "steady", "keepalive"]);
    assert!(start.status.success(), "start failed: {}", combined(&start));

    let supervisor_pid = wait_for_live_supervisor(workdir, Duration::from_secs(10));

    // Partial (per-service) stop: kills the real process and writes
    // desired_state = Stopped, but does NOT tear down the supervisor
    // (`07-supervisor.md`: only whole-project stop does that) — the
    // supervisor stays alive and ticking the whole time this test runs,
    // since 'keepalive' remains desired-running throughout.
    let stop = run_fed(&config_path, workdir, &["stop", "steady"]);
    assert!(stop.status.success(), "stop failed: {}", combined(&stop));

    assert!(
        is_pid_alive(supervisor_pid),
        "a partial stop must not tear down the supervisor — other supervised \
         services (or a future one) may still need it"
    );

    // Give the still-alive supervisor several health-check ticks (5s +/-
    // jitter each) to (incorrectly) resurrect the service, if the
    // desired_state gate were missing or broken.
    std::thread::sleep(Duration::from_secs(18));

    assert!(
        is_pid_alive(supervisor_pid),
        "supervisor should still be alive and well after the wait"
    );

    // 'steady' runs `sleep 301` specifically (distinct from 'keepalive's
    // `sleep 302`) so this check can't accidentally pass because the
    // *other*, still-running service happens to match the same pattern.
    let no_stray_process = Command::new("pgrep")
        .args(["-f", "sleep 301"])
        .output()
        .map(|o| !o.status.success()) // pgrep exits 1 when nothing matches
        .unwrap_or(true);
    assert!(
        no_stray_process,
        "a 'sleep 301' process is still running — the supervisor resurrected \
         a service the user explicitly stopped"
    );

    assert!(
        service_pid(workdir, "steady").is_none(),
        "'steady' should remain unregistered after fed stop, not reappear"
    );
}

/// The direct regression test for hole #5 (daemonization): a `SIGHUP`
/// (simulated terminal close) must not kill the supervisor, and it must
/// keep restarting a crashed service afterward.
///
/// Uses the same "long-running, kill its PID explicitly" shape as the
/// kill-9-respawn test rather than a self-crashing process: a process that
/// exits and restarts every ~1-2s on its own spends much of its time
/// genuinely dead, which races against `mark_dead_services` (any concurrent
/// `fed` command, including this test's own polling reads, can observe
/// "not running right now" and mark the row stale) — a real, accepted
/// residual risk (`07-supervisor.md` Design §3's rationale, which applies
/// generally, not just to Docker), but not what *this* test is trying to
/// verify. A steady process that only "crashes" when the test explicitly
/// kills it sidesteps that race entirely.
#[test]
fn test_supervisor_survives_sighup_and_keeps_restarting() {
    let temp_dir = tempfile::tempdir().unwrap();
    let workdir = temp_dir.path();
    let config_path = create_test_config(&temp_dir, STEADY_ALWAYS_CONFIG);

    let start = run_fed(&config_path, workdir, &["start", "steady"]);
    assert!(start.status.success(), "start failed: {}", combined(&start));

    let supervisor_pid = wait_for_live_supervisor(workdir, Duration::from_secs(10));

    // Crash it once so there's a baseline restart_count, and to prove the
    // daemon is actually functioning before we touch it with SIGHUP.
    let pid_before_hup = service_pid(workdir, "steady").expect("steady should have a tracked pid");
    kill9(pid_before_hup);

    let deadline = Instant::now() + Duration::from_secs(20);
    let mut pid_after_first_restart = None;
    while Instant::now() < deadline {
        if let Some(pid) = service_pid(workdir, "steady")
            && pid != pid_before_hup
            && is_pid_alive(pid)
        {
            pid_after_first_restart = Some(pid);
            break;
        }
        std::thread::sleep(Duration::from_millis(300));
    }
    let pid_after_first_restart =
        pid_after_first_restart.expect("expected 'steady' to be restarted before sending SIGHUP");
    let baseline = wait_for_restart_count_above(workdir, "steady", 0, Duration::from_secs(10));

    sighup(supervisor_pid);
    std::thread::sleep(Duration::from_secs(1));
    assert!(
        is_pid_alive(supervisor_pid),
        "supervisor pid {} must survive SIGHUP (simulated terminal close)",
        supervisor_pid
    );

    // Confirm it keeps functioning afterward, not just alive: crash it a
    // second time and verify the (same, still-alive) supervisor process
    // brings it back again.
    kill9(pid_after_first_restart);

    let deadline = Instant::now() + Duration::from_secs(20);
    let mut recovered = false;
    while Instant::now() < deadline {
        if let Some(pid) = service_pid(workdir, "steady")
            && pid != pid_after_first_restart
            && is_pid_alive(pid)
        {
            recovered = true;
            break;
        }
        std::thread::sleep(Duration::from_millis(300));
    }
    assert!(
        recovered,
        "supervisor should still be restarting 'steady' after SIGHUP"
    );
    wait_for_restart_count_above(workdir, "steady", baseline, Duration::from_secs(10));
    assert!(
        is_pid_alive(supervisor_pid),
        "supervisor should still be the same live process throughout"
    );

    kill9(supervisor_pid);
    let _ = run_fed(&config_path, workdir, &["stop"]);
}

/// Single-instance enforcement (hole #4's locking half, daemon side):
/// spawning several `fed supervise` processes concurrently against the
/// same workspace must leave exactly one alive, holding
/// `.fed/supervisor.lock`; the rest lose the race and exit quietly
/// (`try_acquire` failing is not an error from `run_supervise`'s point of
/// view — it's the expected outcome of two spawners racing).
#[test]
fn test_supervisor_single_instance_under_concurrent_spawns() {
    let temp_dir = tempfile::tempdir().unwrap();
    let workdir = temp_dir.path();
    let config_path = create_test_config(&temp_dir, STEADY_ALWAYS_CONFIG);

    // Register 'steady' as desired-running first (via a normal `fed
    // start`), then kill the one supervisor that spawned so the race below
    // starts from a clean slate. Without a desired-running supervised
    // service already on record, every racing `fed supervise` would see
    // "nothing to supervise" on its very first tick (`tokio::time::interval`
    // fires immediately) and self-exit regardless of who won the lock —
    // that's a correct daemon behavior, but it would make this test
    // vacuous (everyone would "lose", proving nothing about the lock).
    let start = run_fed(&config_path, workdir, &["start", "steady"]);
    assert!(start.status.success(), "start failed: {}", combined(&start));
    let initial_supervisor = wait_for_live_supervisor(workdir, Duration::from_secs(10));
    kill9(initial_supervisor);
    assert!(
        wait_for_pid_dead(initial_supervisor, Duration::from_secs(5)),
        "initial supervisor should be dead before racing new ones"
    );

    const N: usize = 5;
    let mut children: Vec<std::process::Child> = (0..N)
        .map(|_| {
            Command::new(fed_binary())
                .arg("-c")
                .arg(&config_path)
                .arg("-w")
                .arg(workdir)
                .arg("supervise")
                .env("FED_NON_INTERACTIVE", "1")
                .stdout(std::process::Stdio::null())
                .stderr(std::process::Stdio::null())
                .spawn()
                .expect("failed to spawn fed supervise")
        })
        .collect();

    // Give every contender a chance to race for the lock and the losers a
    // chance to exit.
    std::thread::sleep(Duration::from_secs(3));

    let mut alive_pids: Vec<u32> = Vec::new();
    for child in &mut children {
        if let Ok(None) = child.try_wait() {
            alive_pids.push(child.id());
        }
    }

    assert_eq!(
        alive_pids.len(),
        1,
        "expected exactly one fed supervise process to survive the race, got {}: {:?}",
        alive_pids.len(),
        alive_pids
    );

    // The lock file's own PID marker must agree with the one survivor.
    let lock_pid = read_supervisor_lock_pid(workdir);
    assert_eq!(
        lock_pid,
        Some(alive_pids[0]),
        "the lock file's PID marker should name the one live supervisor"
    );

    for child in &mut children {
        let _ = child.kill();
        let _ = child.wait();
    }
}

/// Lock non-monopolization test (hole #4's locking half, from the client
/// side this time — `test_supervisor_single_instance_under_concurrent_spawns`
/// above covers the daemon's own `.fed/supervisor.lock` enforcement; this
/// covers the *other* lock, `.fed/.lock`, which the supervisor must never
/// touch at all per `SqliteStateTracker::new_for_supervisor`). A live
/// supervisor holding only `supervisor.lock` must never cause a routine `fed
/// status`/`fed logs` invocation to print the "another fed instance ...
/// proceeding anyway" warning (`src/state/sqlite/mod.rs`'s
/// `try_acquire_lock`, exercised generically — without a supervisor
/// involved — by `tests/lock_warning_integration_test.rs`). Without the
/// supervisor's own unlocked tracker construction, every one of these calls
/// would degrade into that warning path forever, exactly the constant-noise
/// regression Design §1 calls out.
#[test]
fn test_live_supervisor_never_triggers_lock_contention_warning() {
    const WARNING_TEXT: &str = "Another fed instance (PID";

    let temp_dir = tempfile::tempdir().unwrap();
    let workdir = temp_dir.path();
    let config_path = create_test_config(&temp_dir, STEADY_ALWAYS_CONFIG);

    let start = run_fed(&config_path, workdir, &["start", "steady"]);
    assert!(start.status.success(), "start failed: {}", combined(&start));

    let supervisor_pid = wait_for_live_supervisor(workdir, Duration::from_secs(10));

    // Several `fed status`/`fed logs` invocations in a loop while the
    // supervisor stays alive throughout (holding only `supervisor.lock`).
    for i in 0..5 {
        assert!(
            is_pid_alive(supervisor_pid),
            "supervisor should still be alive for the whole loop (iteration {})",
            i
        );

        let status = run_fed(&config_path, workdir, &["status"]);
        let status_text = combined(&status);
        assert!(status.status.success(), "status failed: {}", status_text);
        assert!(
            !status_text.contains(WARNING_TEXT),
            "iteration {}: fed status warned about a competing fed instance \
             while only the supervisor (holding a separate lock) was alive:\n{}",
            i,
            status_text
        );

        let logs = run_fed(&config_path, workdir, &["logs", "steady"]);
        let logs_text = combined(&logs);
        assert!(
            !logs_text.contains(WARNING_TEXT),
            "iteration {}: fed logs warned about a competing fed instance \
             while only the supervisor (holding a separate lock) was alive:\n{}",
            i,
            logs_text
        );
    }

    let _ = run_fed(&config_path, workdir, &["stop"]);
}

/// Phase 5 (integration and surfacing) full-cycle soak test: start a
/// `restart: always` service, crash it repeatedly and confirm supervised
/// recovery each time, then run a whole-project `fed stop` and confirm the
/// daemon actually **exits** (not just that resurrection is prevented,
/// which `test_supervisor_never_resurrects_desired_state_stopped_service`
/// above already covers using a *second* service kept alive on purpose so
/// the daemon has a reason to stay up) — here there is nothing left
/// supervised and desired-running after `fed stop`, so the per-tick
/// self-exit check (`Orchestrator::any_supervised_service_desired_running`)
/// combined with `stop.rs`'s `signal_stop_and_wait` teardown call should
/// bring the whole daemon down, with no resurrection afterward.
/// Soak-private fixture: `sleep 307` is used by no other test in the
/// workspace, so the trailing pgrep can't see a parallel sibling's
/// process (the earlier `sleep 300`/`sleep 301` collision taught this).
const SOAK_CONFIG: &str = r#"
services:
  steady:
    process: sleep 307
    restart: always
"#;

#[test]
fn test_full_cycle_soak_start_crash_loop_stop_daemon_exits_no_resurrection() {
    let temp_dir = tempfile::tempdir().unwrap();
    let workdir = temp_dir.path();
    let config_path = create_test_config(&temp_dir, SOAK_CONFIG);

    let start = run_fed(&config_path, workdir, &["start", "steady"]);
    assert!(start.status.success(), "start failed: {}", combined(&start));

    let supervisor_pid = wait_for_live_supervisor(workdir, Duration::from_secs(10));

    // Crash the service twice in a row, confirming supervised recovery
    // (a new, live pid and an incremented restart_count) each time before
    // crashing it again.
    let mut last_restart_count = 0;
    for round in 0..2 {
        let pid_before = service_pid(workdir, "steady")
            .unwrap_or_else(|| panic!("steady should have a tracked pid (round {})", round));
        kill9(pid_before);

        let deadline = Instant::now() + Duration::from_secs(20);
        let mut recovered_pid = None;
        while Instant::now() < deadline {
            if let Some(pid) = service_pid(workdir, "steady")
                && pid != pid_before
                && is_pid_alive(pid)
            {
                recovered_pid = Some(pid);
                break;
            }
            std::thread::sleep(Duration::from_millis(300));
        }
        assert!(
            recovered_pid.is_some(),
            "supervisor should have restarted 'steady' after crash round {}",
            round
        );
        last_restart_count = wait_for_restart_count_above(
            workdir,
            "steady",
            last_restart_count,
            Duration::from_secs(10),
        );
    }

    // Whole-project `fed stop`: quiesces (writes desired_state=Stopped for
    // the whole batch before killing anything), kills the real process, and
    // — since nothing supervised remains desired-running — tears the daemon
    // itself down.
    let stop = run_fed(&config_path, workdir, &["stop"]);
    assert!(stop.status.success(), "stop failed: {}", combined(&stop));

    assert!(
        wait_for_pid_dead(supervisor_pid, Duration::from_secs(15)),
        "the supervisor daemon should exit once whole-project `fed stop` \
         leaves nothing supervised desired-running"
    );
    assert!(
        read_supervisor_lock_pid(workdir).is_none_or(|p| !is_pid_alive(p)),
        "supervisor.lock should not name a live process after full teardown"
    );

    // Give a would-be (but now-dead) supervisor several health-check
    // intervals to (incorrectly) resurrect the service, if daemon exit or
    // the desired_state gate were broken.
    std::thread::sleep(Duration::from_secs(18));

    assert!(
        service_pid(workdir, "steady").is_none(),
        "'steady' must remain stopped and unregistered after the full cycle"
    );
    let no_stray_process = Command::new("pgrep")
        .args(["-f", "sleep 307"])
        .output()
        .map(|o| !o.status.success()) // pgrep exits 1 when nothing matches
        .unwrap_or(true);
    assert!(
        no_stray_process,
        "a 'sleep 307' process is still running after full teardown; the \
         service was resurrected"
    );
    assert!(
        read_supervisor_lock_pid(workdir).is_none_or(|p| !is_pid_alive(p)),
        "no supervisor should have respawned itself during the wait"
    );
}

/// The direct regression test for the timing half of hole #4 (Design §6):
/// starting `fed start --watch` in a directory with a live background
/// supervisor must tear that daemon down *before* the foreground
/// orchestrator's own in-process monitoring loop starts (the pre-flight
/// handoff in `main.rs`, run before `Orchestrator::builder()...build()`) —
/// never racing two live monitoring loops over the same services — and
/// once the foreground `--watch` session ends (here, abruptly, simulating a
/// closed terminal), the daemon does not auto-resume: only the next `fed
/// start`/`fed restart` respawns it, per the scaled-back self-heal promise.
#[test]
fn test_watch_handoff_stops_background_supervisor_and_resumes_after() {
    let temp_dir = tempfile::tempdir().unwrap();
    let workdir = temp_dir.path();
    let config_path = create_test_config(&temp_dir, STEADY_ALWAYS_CONFIG);

    let start = run_fed(&config_path, workdir, &["start", "steady"]);
    assert!(start.status.success(), "start failed: {}", combined(&start));

    let background_supervisor_pid = wait_for_live_supervisor(workdir, Duration::from_secs(10));

    // Start `fed start --watch` in the background — in real usage this
    // blocks in the foreground; here it's a spawned child under our
    // control so the test can observe both sides of the handoff.
    let mut watch_child = Command::new(fed_binary())
        .arg("-c")
        .arg(&config_path)
        .arg("-w")
        .arg(workdir)
        .args(["start", "--watch", "steady"])
        .env("FED_NON_INTERACTIVE", "1")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .expect("failed to spawn fed start --watch");

    // The pre-flight handoff must SIGTERM the background daemon before
    // `--watch`'s own orchestrator ever starts monitoring — the exact
    // window Design §6 closes (it used to be possible only after
    // `run_watch_mode` was reached, by which point `initialize()` had
    // already started the foreground loop).
    assert!(
        wait_for_pid_dead(background_supervisor_pid, Duration::from_secs(10)),
        "the background supervisor should be torn down by the watch \
         pre-flight handoff"
    );
    assert!(
        matches!(watch_child.try_wait(), Ok(None)),
        "the --watch process itself should still be running"
    );

    // The pre-flight handoff (this test's own polling of the *old*
    // supervisor's liveness) and `--watch`'s own internal build/attach
    // (which independently waits on the same handoff via a different
    // liveness primitive, `live_supervisor_pid`'s flock check rather than
    // `kill -0`) are two separate observers of the same event and are not
    // guaranteed to settle at the exact same instant. A short buffer here
    // lets `--watch`'s own `Orchestrator::initialize()`/`create_services()`
    // finish attaching to the still-alive 'steady' *before* this test
    // crashes it — otherwise a crash landing in that narrow window would be
    // absorbed by the plain `start steady` CLI argument's own "start what
    // was named" semantics (a fresh register, not a monitoring-loop
    // restart), which is a real but orthogonal pre-existing race (the same
    // `mark_dead_services` staleness race Design's "Attach/self-heal
    // reality" section discusses), not what this test exists to check.
    std::thread::sleep(Duration::from_secs(3));

    // Prove there's exactly one live monitor, not a race window with two:
    // crash the service once and confirm exactly one restart is recorded —
    // a second, concurrent monitoring loop racing the same crash would
    // double-count this.
    let baseline = restart_count(workdir, "steady");
    let pid_before = service_pid(workdir, "steady").expect("steady should have a tracked pid");
    kill9(pid_before);
    let after_one_restart =
        wait_for_restart_count_above(workdir, "steady", baseline, Duration::from_secs(20));
    // Give a hypothetical second (buggy) monitor a further full health-check
    // tick to also act, then confirm the count didn't move again.
    std::thread::sleep(Duration::from_secs(7));
    assert_eq!(
        restart_count(workdir, "steady"),
        after_one_restart,
        "exactly one monitoring loop should be active during --watch — a \
         second restart in the same window would mean two loops raced"
    );

    // End the foreground `--watch` session uncleanly (SIGKILL, simulating a
    // closed terminal rather than a graceful Ctrl+C) — the service itself
    // must survive (it's a real, independently-detached process the watch
    // session was only observing, not one it owns), and nothing auto-
    // respawns supervision once the session is gone.
    let watch_child_pid = watch_child.id();
    kill9(watch_child_pid);
    let _ = watch_child.wait();

    let service_pid_after_watch_exit = service_pid(workdir, "steady")
        .expect("'steady' should still be running after --watch exits");
    assert!(
        is_pid_alive(service_pid_after_watch_exit),
        "the service must survive the --watch session ending"
    );
    assert!(
        read_supervisor_lock_pid(workdir).is_none_or(|p| !is_pid_alive(p)),
        "no supervisor should be left running immediately after --watch exits"
    );

    // The next plain `fed start` (the service is already running) is what
    // respawns supervision — the scaled-back self-heal promise.
    let start2 = run_fed(&config_path, workdir, &["start", "steady"]);
    assert!(
        start2.status.success(),
        "second start failed: {}",
        combined(&start2)
    );
    let respawned_supervisor_pid = wait_for_live_supervisor(workdir, Duration::from_secs(10));
    assert_ne!(
        background_supervisor_pid, respawned_supervisor_pid,
        "the respawned supervisor should be a new process"
    );

    // Confirm it actually resumed real supervision, not just that the lock
    // file was re-acquired.
    kill9(service_pid_after_watch_exit);
    let deadline = Instant::now() + Duration::from_secs(20);
    let mut recovered = false;
    while Instant::now() < deadline {
        if let Some(pid) = service_pid(workdir, "steady")
            && pid != service_pid_after_watch_exit
            && is_pid_alive(pid)
        {
            recovered = true;
            break;
        }
        std::thread::sleep(Duration::from_millis(300));
    }
    assert!(
        recovered,
        "respawned supervisor should resume restarting 'steady'"
    );

    let _ = run_fed(&config_path, workdir, &["stop"]);
}
