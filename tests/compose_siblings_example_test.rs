use std::fs;
use std::path::Path;
use std::process::Stdio;
use std::process::{Command, Output};
use std::thread;
use std::time::{Duration, Instant};

use tempfile::TempDir;

fn fed_binary() -> &'static str {
    env!("CARGO_BIN_EXE_fed")
}

fn copy_example(temp: &TempDir) {
    let source = Path::new(env!("CARGO_MANIFEST_DIR")).join("examples/compose-siblings");
    for name in ["fed.yaml", "compose.yaml"] {
        fs::copy(source.join(name), temp.path().join(name)).expect("copy Compose example");
    }
}

fn fed(temp: &TempDir, args: &[&str]) -> Output {
    Command::new(fed_binary())
        .arg("-c")
        .arg(temp.path().join("fed.yaml"))
        .arg("-w")
        .arg(temp.path())
        .args(args)
        .env("FED_NON_INTERACTIVE", "1")
        .output()
        .expect("run fed")
}

fn project_name(compose_file: &Path) -> String {
    let canonical = fs::canonicalize(compose_file).expect("canonical Compose path");
    let mut hash = 2_166_136_261_u32;
    for &byte in canonical.as_os_str().as_encoded_bytes() {
        hash ^= byte as u32;
        hash = hash.wrapping_mul(16_777_619);
    }
    format!("fed-{:04x}", hash & 0xffff)
}

fn compose(temp: &TempDir, args: &[&str]) -> Output {
    Command::new("docker")
        .args(["compose", "-f"])
        .arg(temp.path().join("compose.yaml"))
        .args(["-p", &project_name(&temp.path().join("compose.yaml"))])
        .args(args)
        // Compose interpolates on every command. Fed supplies the real resolved
        // values; direct inspection commands only need valid parse-time values.
        .env("DATABASE_PORT", "15432")
        .env("CACHE_PORT", "16379")
        .output()
        .expect("run docker compose")
}

fn assert_success(action: &str, output: &Output) {
    assert!(
        output.status.success(),
        "{action} failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

fn container_id(temp: &TempDir, service: &str) -> String {
    let output = compose(temp, &["ps", "-q", service]);
    assert_success("compose ps", &output);
    String::from_utf8_lossy(&output.stdout).trim().to_string()
}

fn wait_for_example_database(container: &str) {
    let start = Instant::now();
    while start.elapsed() < Duration::from_secs(30) {
        if Command::new("docker")
            .args([
                "exec", container, "psql", "-U", "example", "-d", "example", "-Atc", "SELECT 1",
            ])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .map(|status| status.success())
            .unwrap_or(false)
        {
            return;
        }
        thread::sleep(Duration::from_millis(250));
    }
    panic!("the configured Postgres database did not become queryable");
}

fn psql(container: &str, sql: &str) -> Output {
    Command::new("docker")
        .args([
            "exec", container, "psql", "-U", "example", "-d", "example", "-Atc", sql,
        ])
        .output()
        .expect("run psql")
}

struct Cleanup<'a>(&'a TempDir);

impl Drop for Cleanup<'_> {
    fn drop(&mut self) {
        let _ = compose(self.0, &["down", "-v", "--remove-orphans"]);
    }
}

#[test]
#[cfg_attr(not(feature = "docker-tests"), ignore)] // Requires Docker (CI: cargo test --features docker-tests)
fn compose_siblings_keep_container_identity_and_data() {
    let temp = TempDir::new().expect("temp project");
    copy_example(&temp);
    let _cleanup = Cleanup(&temp);

    assert_success("start database", &fed(&temp, &["start", "database"]));
    let database_id = container_id(&temp, "database");
    assert!(!database_id.is_empty(), "database container must exist");
    wait_for_example_database(&database_id);

    let create = psql(
        &database_id,
        "CREATE TABLE notes(body text); INSERT INTO notes VALUES ('preserved');",
    );
    assert_success("create durable Postgres data", &create);

    assert_success("start cache", &fed(&temp, &["start", "cache"]));
    assert_eq!(
        container_id(&temp, "database"),
        database_id,
        "starting a sibling must not recreate the database"
    );

    assert_success("stop cache", &fed(&temp, &["stop", "cache"]));
    assert!(container_id(&temp, "cache").is_empty());
    assert_eq!(container_id(&temp, "database"), database_id);

    let query = psql(&database_id, "SELECT body FROM notes;");
    assert_success("read durable Postgres data", &query);
    assert_eq!(String::from_utf8_lossy(&query.stdout).trim(), "preserved");

    assert_success("stop database", &fed(&temp, &["stop", "database"]));
    assert!(container_id(&temp, "database").is_empty());
}

#[test]
#[cfg_attr(not(feature = "docker-tests"), ignore)]
fn complete_compose_file_expands_and_starts_in_one_fed_invocation() {
    let temp = TempDir::new().expect("temp project");
    copy_example(&temp);
    let _cleanup = Cleanup(&temp);

    assert_success(
        "start expanded Compose project",
        &fed(&temp, &["start", "database", "cache"]),
    );
    let database = container_id(&temp, "database");
    let cache = container_id(&temp, "cache");
    assert!(!database.is_empty());
    assert!(!cache.is_empty());
    wait_for_example_database(&database);
    assert_success(
        "read imported service logs from a fresh process",
        &fed(&temp, &["logs", "database"]),
    );
    assert_success("stop expanded Compose project", &fed(&temp, &["stop"]));
    assert!(container_id(&temp, "database").is_empty());
    assert!(container_id(&temp, "cache").is_empty());
}
