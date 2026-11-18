//! A started stack keeps the variant it was started with.
//!
//! Drives the real binary against process services, so the whole path is
//! exercised: `--variant` on `fed start`, the state database, the session
//! file, a later start or restart *without* the flag, and the commands that
//! say otherwise.

use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};

const FED_YAML: &str = r#"
services:
  presentation:
    default_variant: image
    variants:
      source:
        process: sleep 300
      image:
        process: sleep 301
  next:
    process: sleep 302
    depends_on: [presentation]
  analytics:
    default_variant: image
    variants:
      source:
        process: sleep 303
      image:
        process: sleep 304
"#;

fn run_fed(workdir: &Path, args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_fed"))
        .arg("-c")
        .arg(workdir.join("fed.yaml"))
        .arg("-w")
        .arg(workdir)
        .args(args)
        .env("FED_NON_INTERACTIVE", "1")
        .output()
        .expect("run fed")
}

fn ok(workdir: &Path, args: &[&str]) -> String {
    let output = run_fed(workdir, args);
    assert!(
        output.status.success(),
        "fed {args:?} failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    )
}

fn variant_of(workdir: &Path, service: &str) -> Option<String> {
    // stdout only: anything fed logs to stderr (a warning, an INFO line) must
    // not end up inside the JSON document.
    let output = run_fed(workdir, &["status", "--json"]);
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let status: serde_json::Value = serde_json::from_slice(&output.stdout).expect("status json");
    status[service]["variant"].as_str().map(String::from)
}

struct Stack {
    dir: tempfile::TempDir,
}

impl Stack {
    fn new() -> Self {
        let dir = tempfile::tempdir().unwrap();
        fs::write(dir.path().join("fed.yaml"), FED_YAML).unwrap();
        Self { dir }
    }
    fn path(&self) -> PathBuf {
        self.dir.path().to_path_buf()
    }
}

impl Drop for Stack {
    fn drop(&mut self) {
        let _ = run_fed(self.dir.path(), &["stop"]);
    }
}

#[test]
fn start_next_after_one_service_stopped_keeps_the_variant_chosen_at_start() {
    let stack = Stack::new();
    let dir = stack.path();

    ok(&dir, &["start", "--variant", "presentation:source", "next"]);
    assert_eq!(variant_of(&dir, "presentation").as_deref(), Some("source"));

    // `next` and `presentation` stop individually (a failing dependency, an
    // agent's `fed stop presentation`) while the stack as a whole is still up.
    // Their rows are forgotten, so only the remembered choice is left to
    // bring `presentation` back as `source` when `next` pulls it in.
    ok(&dir, &["stop", "next", "presentation"]);
    let started = ok(&dir, &["start", "next"]);
    assert_eq!(
        variant_of(&dir, "presentation").as_deref(),
        Some("source"),
        "start output:\n{started}"
    );
    assert!(
        started.contains("earlier 'fed start --variant'"),
        "the start must say why presentation is not the default: {started}"
    );
}

#[test]
fn a_whole_stack_stop_ends_the_remembered_choice() {
    let stack = Stack::new();
    let dir = stack.path();

    ok(&dir, &["start", "--variant", "presentation:source", "next"]);
    ok(&dir, &["stop"]);
    assert!(!dir.join(".fed/variants.session.yaml").exists());
    ok(&dir, &["start", "next"]);
    assert_eq!(variant_of(&dir, "presentation").as_deref(), Some("image"));
}

#[test]
fn restart_without_the_flag_keeps_the_running_variant() {
    let stack = Stack::new();
    let dir = stack.path();

    ok(&dir, &["start", "--variant", "presentation:source", "next"]);
    ok(&dir, &["restart", "presentation"]);
    assert_eq!(variant_of(&dir, "presentation").as_deref(), Some("source"));
}

#[test]
fn restart_keeps_the_running_variant_of_a_stack_started_before_it_was_remembered() {
    let stack = Stack::new();
    let dir = stack.path();

    ok(&dir, &["start", "--variant", "presentation:source", "next"]);
    // A stack started by a fed that did not write the session file.
    let _ = fs::remove_file(dir.join(".fed/variants.session.yaml"));
    ok(&dir, &["restart", "presentation"]);
    assert_eq!(variant_of(&dir, "presentation").as_deref(), Some("source"));
}

#[test]
fn a_new_flag_replaces_the_choice_and_variant_clear_forgets_it() {
    let stack = Stack::new();
    let dir = stack.path();

    ok(&dir, &["start", "--variant", "presentation:source", "next"]);
    ok(&dir, &["stop", "next", "presentation"]);
    ok(&dir, &["start", "--variant", "presentation:image", "next"]);
    assert_eq!(variant_of(&dir, "presentation").as_deref(), Some("image"));

    ok(&dir, &["stop", "next", "presentation"]);
    ok(&dir, &["start", "--variant", "presentation:source", "next"]);
    ok(&dir, &["stop", "next", "presentation"]);
    ok(&dir, &["variant", "clear"]);
    ok(&dir, &["start", "next"]);
    assert_eq!(
        variant_of(&dir, "presentation").as_deref(),
        Some("image"),
        "`fed variant clear` is the way back to default_variant"
    );
}

#[test]
fn a_dry_run_remembers_nothing() {
    let stack = Stack::new();
    let dir = stack.path();

    ok(
        &dir,
        &[
            "start",
            "--dry-run",
            "--variant",
            "presentation:source",
            "next",
        ],
    );
    assert!(!dir.join(".fed/variants.session.yaml").exists());
}

#[test]
fn variant_set_overrides_only_what_it_names() {
    let stack = Stack::new();
    let dir = stack.path();
    let session = dir.join(".fed/variants.session.yaml");

    ok(&dir, &["start", "--variant", "presentation:source", "next"]);
    assert!(
        fs::read_to_string(&session)
            .unwrap()
            .contains("presentation: source")
    );

    // A pin for another service leaves this one's remembered choice alone.
    ok(&dir, &["variant", "set", "analytics:source"]);
    let after_unrelated = fs::read_to_string(&session).unwrap_or_default();
    assert!(
        after_unrelated.contains("presentation: source"),
        "{after_unrelated}"
    );

    ok(&dir, &["variant", "set", "presentation:image"]);
    assert!(
        !session.exists(),
        "the remembered pin it supersedes is gone, and with it the file"
    );
    let unset = ok(&dir, &["variant", "unset", "presentation"]);
    assert!(unset.contains("keeps the variant it runs as"), "{unset}");
}
