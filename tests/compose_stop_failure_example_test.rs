use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::process::{Command, Output};

use tempfile::TempDir;

fn fed_binary() -> &'static str {
    env!("CARGO_BIN_EXE_fed")
}

fn copy_example(temp: &TempDir) {
    let source = Path::new(env!("CARGO_MANIFEST_DIR")).join("examples/compose-stop-failure");
    for name in ["fed.yaml", "compose.yaml", "runtime-wrapper.sh"] {
        fs::copy(source.join(name), temp.path().join(name)).expect("copy failure example");
    }
    let wrapper = temp.path().join("runtime-wrapper.sh");
    let mut permissions = fs::metadata(&wrapper).unwrap().permissions();
    permissions.set_mode(0o755);
    fs::set_permissions(wrapper, permissions).unwrap();
}

fn fed(temp: &TempDir, args: &[&str], fail_rm: bool) -> Output {
    let mut command = Command::new(fed_binary());
    command
        .arg("-c")
        .arg(temp.path().join("fed.yaml"))
        .arg("-w")
        .arg(temp.path())
        .args(args)
        .env("FED_NON_INTERACTIVE", "1");
    if fail_rm {
        command
            .env(
                "FED_CONTAINER_RUNTIME",
                temp.path().join("runtime-wrapper.sh"),
            )
            .env("FED_EXAMPLE_FAIL_COMPOSE_RM", "1");
    }
    command.output().expect("run fed")
}

fn state_row_exists(temp: &TempDir) -> bool {
    let connection = rusqlite::Connection::open(temp.path().join(".fed/lock.db")).unwrap();
    connection
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM services WHERE id = 'cache')",
            [],
            |row| row.get(0),
        )
        .unwrap()
}

fn container_is_running(temp: &TempDir) -> bool {
    let compose_file = fs::canonicalize(temp.path().join("compose.yaml")).unwrap();
    let mut hash = 2_166_136_261_u32;
    for &byte in compose_file.as_os_str().as_encoded_bytes() {
        hash ^= byte as u32;
        hash = hash.wrapping_mul(16_777_619);
    }
    let project = format!("fed-{:04x}", hash & 0xffff);
    Command::new("docker")
        .args(["compose", "-f"])
        .arg(compose_file)
        .args(["-p", &project, "ps", "-q", "cache"])
        .output()
        .map(|output| output.status.success() && !output.stdout.is_empty())
        .unwrap_or(false)
}

#[test]
#[cfg_attr(not(feature = "docker-tests"), ignore)] // Requires Docker (CI: cargo test --features docker-tests)
fn failed_compose_stop_is_nonzero_and_keeps_recoverable_state() {
    let temp = TempDir::new().expect("temp project");
    copy_example(&temp);

    let start = fed(&temp, &["start", "cache"], false);
    assert!(
        start.status.success(),
        "start failed: {}",
        String::from_utf8_lossy(&start.stderr)
    );

    let failed_stop = fed(&temp, &["stop", "cache"], true);
    assert!(
        !failed_stop.status.success(),
        "injected stop failure must be surfaced"
    );
    assert!(
        String::from_utf8_lossy(&failed_stop.stderr).contains("injected compose rm failure"),
        "the actionable Compose error must be preserved"
    );
    assert!(
        state_row_exists(&temp),
        "Fed must retain state after an unverified stop"
    );
    assert!(
        container_is_running(&temp),
        "the fixture must prove the container is still live"
    );

    let retry = fed(&temp, &["stop", "cache"], false);
    assert!(
        retry.status.success(),
        "stop retry failed: {}",
        String::from_utf8_lossy(&retry.stderr)
    );
    assert!(
        !state_row_exists(&temp),
        "verified cleanup should unregister state"
    );
    assert!(
        !container_is_running(&temp),
        "verified cleanup should remove the container"
    );
}
