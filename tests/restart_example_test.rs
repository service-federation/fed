use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};

use tempfile::tempdir;

fn fed_binary() -> &'static str {
    env!("CARGO_BIN_EXE_fed")
}

fn run_fed(workdir: &Path, args: &[&str]) -> Output {
    Command::new(fed_binary())
        .arg("-c")
        .arg(workdir.join("fed.yaml"))
        .arg("-w")
        .arg(workdir)
        .args(args)
        .env("FED_NON_INTERACTIVE", "1")
        .output()
        .expect("run fed")
}

struct RunningExample {
    workdir: PathBuf,
}

impl Drop for RunningExample {
    fn drop(&mut self) {
        let _ = run_fed(&self.workdir, &["stop"]);
    }
}

fn assert_success(action: &str, output: &Output) {
    assert!(
        output.status.success(),
        "{action} failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

fn events_by_generation(path: &Path) -> HashMap<u32, Vec<String>> {
    fs::read_to_string(path)
        .expect("example must write its event log")
        .lines()
        .map(|line| {
            let (generation, service) = line.split_once(' ').expect("generation service event");
            (
                generation.parse::<u32>().expect("numeric generation"),
                service.to_string(),
            )
        })
        .fold(HashMap::new(), |mut generations, (generation, service)| {
            generations.entry(generation).or_default().push(service);
            generations
        })
}

fn assert_generation_order(events: &[String]) {
    let expected = [
        "datastore",
        "schema-ready",
        "stream-ready",
        "worker-ready",
        "application-ready",
        "web",
    ];
    assert_eq!(
        events.len(),
        expected.len(),
        "every node must run exactly once"
    );
    for name in expected {
        assert_eq!(
            events.iter().filter(|event| event.as_str() == name).count(),
            1,
            "{name} must run exactly once; got {events:?}"
        );
    }

    let position = |name: &str| {
        events
            .iter()
            .position(|event| event == name)
            .expect("expected service event")
    };
    assert!(position("datastore") < position("schema-ready"));
    assert!(position("datastore") < position("stream-ready"));
    assert!(position("schema-ready") < position("worker-ready"));
    assert!(position("schema-ready") < position("application-ready"));
    assert!(position("stream-ready") < position("application-ready"));
    assert!(position("worker-ready") < position("web"));
    assert!(position("application-ready") < position("web"));
}

#[test]
fn targeted_restart_replays_the_real_example_in_dependency_order() {
    let temp = tempdir().expect("temp dir");
    let source = Path::new(env!("CARGO_MANIFEST_DIR")).join("examples/restart-diamond/fed.yaml");
    fs::copy(source, temp.path().join("fed.yaml")).expect("copy example config");
    let _cleanup = RunningExample {
        workdir: temp.path().to_path_buf(),
    };

    assert_success("start", &run_fed(temp.path(), &["start"]));
    assert_success("restart", &run_fed(temp.path(), &["restart", "datastore"]));

    let generations = events_by_generation(&temp.path().join(".runtime/events"));
    assert_eq!(
        generations.len(),
        2,
        "start and restart must make two generations"
    );
    assert_generation_order(&generations[&1]);
    assert_generation_order(&generations[&2]);

    let status = run_fed(temp.path(), &["status", "--json"]);
    assert_success("status", &status);
    let status = String::from_utf8_lossy(&status.stdout);
    assert!(status.contains("\"schema-ready\""));
    assert!(status.contains("\"stream-ready\""));
    assert!(status.contains("completed"));
}
