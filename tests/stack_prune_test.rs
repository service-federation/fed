//! `fed prune` and the leak handling around `isolated: true` scripts.
//!
//! Two kinds of test. The process-only ones drive the real binary against a
//! script that depends on a plain process service, which exercises the same
//! teardown and registry paths as Docker services without needing Docker.
//! The Docker one builds throwaway stacks and runs `fed prune` against them;
//! it points `FED_STACK_REGISTRY_DIR` at a temp directory, so every other
//! stack on the machine is "owner unknown" to it and is never touched.

#![cfg(unix)]

use fed::stack_registry::StackRegistry;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output, Stdio};
use std::time::{Duration, Instant};

fn fed(workdir: &Path, registry: &Path) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_fed"));
    command
        .arg("-c")
        .arg(workdir.join("fed.yaml"))
        .arg("-w")
        .arg(workdir)
        .env("FED_NON_INTERACTIVE", "1")
        .env(StackRegistry::DIR_ENV, registry);
    command
}

fn wait_until(what: &str, timeout: Duration, mut done: impl FnMut() -> bool) {
    let start = Instant::now();
    while !done() {
        assert!(start.elapsed() < timeout, "timed out waiting for {what}");
        std::thread::sleep(Duration::from_millis(100));
    }
}

/// Pids of processes whose command line contains `needle`.
fn pids_running(needle: &str) -> Vec<u32> {
    let out = Command::new("pgrep").args(["-f", needle]).output().unwrap();
    String::from_utf8_lossy(&out.stdout)
        .lines()
        .filter_map(|l| l.trim().parse().ok())
        .collect()
}

struct Project {
    dir: tempfile::TempDir,
    registry: tempfile::TempDir,
    marker: String,
}

impl Project {
    /// A project whose isolated script runs `sleep <marker>` against a worker
    /// that runs `sleep <marker + 1>`, so each test finds only its own processes.
    /// The script backgrounds its `sleep` under the shell, so ending the shell
    /// alone would leave it running.
    fn new(marker: u32) -> Self {
        let dir = tempfile::tempdir().unwrap();
        fs::write(
            dir.path().join("fed.yaml"),
            format!(
                r#"
services:
  worker:
    process: sleep {worker}
scripts:
  slow:
    isolated: true
    depends_on: [worker]
    # Like a test runner: the shell is not the process that does the work.
    script: |
      sleep {marker} &
      wait
"#,
                worker = marker + 1
            ),
        )
        .unwrap();
        Self {
            dir,
            registry: tempfile::tempdir().unwrap(),
            marker: marker.to_string(),
        }
    }

    fn worker_needle(&self) -> String {
        format!("sleep {}", self.marker.parse::<u32>().unwrap() + 1)
    }

    fn registry(&self) -> StackRegistry {
        StackRegistry::at(self.registry.path().to_path_buf())
    }

    /// The records of script runs. (The project's own checkout scope is also
    /// registered by the parent `fed`, which is right and not what is under test.)
    fn script_runs(&self) -> Vec<fed::stack_registry::StackEntry> {
        self.registry()
            .entries()
            .into_iter()
            .filter(|e| e.kind == fed::stack_registry::StackKind::Script)
            .collect()
    }

    fn run_script(&self) -> Child {
        let child = fed(self.dir.path(), self.registry.path())
            .arg("slow")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        // Up when the script runs, which is after the worker started.
        wait_until(
            "the isolated script to run",
            Duration::from_secs(60),
            || !pids_running(&format!("sleep {}", self.marker)).is_empty(),
        );
        child
    }
}

impl Drop for Project {
    /// A failed assertion must not leave sleeping processes behind.
    fn drop(&mut self) {
        for needle in [
            format!("^sleep {}", self.marker),
            format!("^{}", self.worker_needle()),
        ] {
            let _ = Command::new("pkill").args(["-f", &needle]).status();
        }
    }
}

fn signal(child: &Child, sig: nix::sys::signal::Signal) {
    nix::sys::signal::kill(nix::unistd::Pid::from_raw(child.id() as i32), sig).unwrap();
}

#[test]
fn a_running_script_holds_its_scope_and_sigterm_cleans_everything_up() {
    let project = Project::new(47_110);
    let mut run = project.run_script();

    let entries = project.script_runs();
    assert_eq!(entries.len(), 1, "the run registers its scope: {entries:?}");
    assert!(entries[0].scope.starts_with("iso-"));
    assert!(
        project.registry().script_run_alive(&entries[0].scope),
        "the owner holds the lock while it runs"
    );
    assert!(!pids_running(&project.worker_needle()).is_empty());

    // What an agent's command timeout, `kill`, or a closing terminal sends.
    signal(&run, nix::sys::signal::Signal::SIGTERM);
    wait_until("fed to exit after SIGTERM", Duration::from_secs(60), || {
        run.try_wait().unwrap().is_some()
    });

    assert!(
        pids_running(&project.worker_needle()).is_empty(),
        "the isolated stack must be torn down, not left running"
    );
    assert!(
        pids_running(&format!("sleep {}", project.marker)).is_empty(),
        "the script itself is stopped"
    );
    assert!(
        project.script_runs().is_empty(),
        "a run that cleaned up leaves no record: {:?}",
        project.script_runs()
    );
}

#[test]
fn a_sigkilled_script_run_is_recognisable_afterwards() {
    let project = Project::new(47_120);
    let mut run = project.run_script();
    let scope = project.script_runs()[0].scope.clone();

    // Nothing can catch SIGKILL, so nothing cleans up. The leak has to be
    // provable from outside: the lock is gone, the record is not.
    signal(&run, nix::sys::signal::Signal::SIGKILL);
    run.wait().unwrap();

    let registry = project.registry();
    assert!(
        !registry.script_run_alive(&scope),
        "the kernel dropped the lock"
    );
    let dead = registry.dead_script_runs();
    assert_eq!(dead.len(), 1, "{dead:?}");
    assert_eq!(dead[0].scope, scope);
    assert_eq!(
        dead[0].workdir,
        fs::canonicalize(project.dir.path()).unwrap()
    );

    // The stack the dead run started is still up, which is the leak.
    assert!(!pids_running(&project.worker_needle()).is_empty());
    let _ = Command::new("pkill")
        .args(["-f", &project.worker_needle()])
        .status();
    let _ = Command::new("pkill")
        .args(["-f", &format!("sleep {}", project.marker)])
        .status();
}

#[test]
fn a_run_on_a_machine_without_docker_leaves_no_record() {
    // Process-only projects need no container runtime. A run there cannot have
    // left anything in Docker, so neither a clean exit nor a later sweep may
    // keep a record that makes every following command look for Docker again.
    let project = Project::new(47_140);
    let mut run = fed(project.dir.path(), project.registry.path())
        .arg("slow")
        .env("FED_CONTAINER_RUNTIME", "/nonexistent/docker")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    wait_until(
        "the isolated script to run",
        Duration::from_secs(60),
        || !pids_running(&format!("sleep {}", project.marker)).is_empty(),
    );
    signal(&run, nix::sys::signal::Signal::SIGTERM);
    run.wait().unwrap();
    assert!(
        project.script_runs().is_empty(),
        "{:?}",
        project.script_runs()
    );

    // A SIGKILLed run is swept by the next command instead.
    let mut run = fed(project.dir.path(), project.registry.path())
        .arg("slow")
        .env("FED_CONTAINER_RUNTIME", "/nonexistent/docker")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    wait_until("the run to register", Duration::from_secs(60), || {
        !project.script_runs().is_empty()
    });
    signal(&run, nix::sys::signal::Signal::SIGKILL);
    run.wait().unwrap();
    assert_eq!(project.registry().dead_script_runs().len(), 1);
    let stopped = fed(project.dir.path(), project.registry.path())
        .arg("stop")
        .env("FED_CONTAINER_RUNTIME", "/nonexistent/docker")
        .output()
        .unwrap();
    assert!(
        stopped.status.success(),
        "{}",
        String::from_utf8_lossy(&stopped.stderr)
    );
    assert!(
        project.script_runs().is_empty(),
        "{:?}",
        project.script_runs()
    );
}

#[test]
fn two_runs_of_the_same_script_do_not_share_a_scope() {
    let project = Project::new(47_130);
    let mut first = project.run_script();
    let mut second = project.run_script();
    wait_until("both runs to register", Duration::from_secs(60), || {
        project.script_runs().len() == 2
    });
    let scopes: std::collections::HashSet<_> =
        project.script_runs().into_iter().map(|e| e.scope).collect();
    assert_eq!(scopes.len(), 2);
    for run in [&mut first, &mut second] {
        signal(run, nix::sys::signal::Signal::SIGTERM);
        run.wait().unwrap();
    }
    assert!(project.script_runs().is_empty());
}

// ── Docker ────────────────────────────────────────────────────────────────

const MANAGED: &str = "com.service-federation.managed=true";

fn docker(args: &[&str]) -> Output {
    Command::new("docker").args(args).output().expect("docker")
}

fn docker_available() -> bool {
    Command::new("docker")
        .arg("info")
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
}

fn container_exists(name: &str) -> bool {
    !docker(&["ps", "-a", "-q", "-f", &format!("name=^{name}$")])
        .stdout
        .is_empty()
}

fn volume_exists(name: &str) -> bool {
    docker(&["volume", "inspect", name]).status.success()
}

/// An exited container and a volume named for `scope`, both labeled as fed's.
fn make_stack(scope: &str) -> (String, String) {
    let (container, volume) = (format!("fed-{scope}-db"), format!("fed-{scope}-data"));
    assert!(
        docker(&["volume", "create", "--label", MANAGED, &volume])
            .status
            .success()
    );
    let created = docker(&[
        "create",
        "--name",
        &container,
        "--label",
        MANAGED,
        "-v",
        &format!("{volume}:/data"),
        "alpine:latest",
        "true",
    ]);
    assert!(
        created.status.success(),
        "{}",
        String::from_utf8_lossy(&created.stderr)
    );
    (container, volume)
}

fn prune(registry: &Path, args: &[&str]) -> String {
    let out = Command::new(env!("CARGO_BIN_EXE_fed"))
        .arg("prune")
        .args(args)
        .env(StackRegistry::DIR_ENV, registry)
        .env("FED_NON_INTERACTIVE", "1")
        .stdin(Stdio::null())
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    )
}

#[test]
#[cfg_attr(not(feature = "docker-tests"), ignore)] // Requires Docker
fn prune_removes_only_what_it_can_prove_abandoned() {
    if !docker_available() {
        eprintln!("skipping: Docker not available");
        return;
    }
    let tmp = tempfile::tempdir().unwrap();
    let registry_dir: PathBuf = tmp.path().join("stacks");
    let registry = StackRegistry::at(registry_dir.clone());
    let id = || format!("{:08x}", rand::random::<u32>());

    // 1. A checkout that was deleted without `fed stop`.
    let gone_dir = tmp.path().join("deleted-checkout");
    fs::create_dir(&gone_dir).unwrap();
    let gone_scope = format!("iso-{}", id());
    registry.register_checkout(&gone_dir, &gone_scope).unwrap();
    fs::remove_dir(&gone_dir).unwrap();
    let gone = make_stack(&gone_scope);

    // 2. A stopped stack in a checkout that still exists: keep its database.
    let live_dir = tmp.path().join("live-checkout");
    fs::create_dir(&live_dir).unwrap();
    let live_scope = fed::service::hash_work_dir(&live_dir);
    registry.register_checkout(&live_dir, &live_scope).unwrap();
    let live = make_stack(&live_scope);

    // 3. A stack made by a fed that predates the registry.
    let legacy = make_stack(&format!("iso-{}", id()));

    // 4. An isolated script run whose process was SIGKILLed.
    let run_scope = format!("iso-{}", id());
    drop(
        registry
            .begin_script_run(tmp.path(), &run_scope, true)
            .unwrap(),
    );
    let run = make_stack(&run_scope);

    let report = prune(&registry_dir, &[]);
    assert!(report.contains("checkout directory deleted"), "{report}");
    assert!(report.contains(&gone_scope), "{report}");
    assert!(
        report.contains("deleted-checkout"),
        "the report names the path: {report}"
    );
    assert!(
        report.contains("isolated script run whose process is dead"),
        "{report}"
    );
    assert!(report.contains(&run_scope), "{report}");
    assert!(
        !report.contains(&live_scope),
        "a stopped stack in a live checkout is never listed for removal: {report}"
    );
    assert!(report.contains("stack stopped"), "{report}");
    assert!(report.contains("no owner on record"), "{report}");
    for name in [&gone.0, &live.0, &legacy.0, &run.0] {
        assert!(container_exists(name), "a dry run removes nothing: {name}");
    }

    let removed = prune(&registry_dir, &["--force"]);
    assert!(
        removed.contains("Removed 2 container(s) and 2 volume(s)"),
        "{removed}"
    );
    for (container, volume) in [&gone, &run] {
        assert!(!container_exists(container), "{container} is abandoned");
        assert!(!volume_exists(volume), "{volume} is abandoned");
    }
    for (container, volume) in [&live, &legacy] {
        assert!(container_exists(container), "{container} must survive");
        assert!(volume_exists(volume), "{volume} must survive");
    }
    assert!(
        registry.get(&gone_scope).is_none(),
        "removed stacks are forgotten"
    );
    assert!(registry.get(&live_scope).is_some());

    // `--adopt ... --unowned` is deliberately not exercised here: against a real
    // Docker daemon it would also remove every other stopped stack on the machine
    // that is not in this test's registry. Its rules are unit-tested in
    // `stack_cleanup`.
    docker(&["rm", "-f", "-v", &legacy.0]);
    docker(&["volume", "rm", "-f", &legacy.1]);
    docker(&["rm", "-f", "-v", &live.0]);
    docker(&["volume", "rm", "-f", &live.1]);
}
