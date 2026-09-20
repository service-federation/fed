//! Real HTTP consumers catch supervisor/port/variant regressions that a
//! healthcheck consisting of `true` cannot reveal.
mod support;

#[test]
fn variant_stack_walkthrough() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    let example = root.join("examples/variant-stack");
    let yaml = std::fs::read_to_string(example.join("fed.yaml")).unwrap();
    support::parse_checked(&yaml);
    let output = std::process::Command::new("python3")
        .arg(example.join("verify.py"))
        .arg("--fed")
        .arg(env!("CARGO_BIN_EXE_fed"))
        .output()
        .expect("the variant-stack walkthrough requires Python 3.10+");
    assert!(
        output.status.success(),
        "variant-stack walkthrough failed:\n{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

// Keep the socket handoff separate from the library tests that stress the
// ephemeral-port allocator concurrently. This exercises its real consumer.
#[tokio::test]
async fn supervisor_releases_port_reservations_for_recovering_services() {
    let temp_dir = tempfile::tempdir().unwrap();
    let config = support::parse_checked(
        r#"
parameters:
  RECOVERY_LISTENER_PORT: { type: port }
services: {}
"#,
    );
    let orchestrator = fed::Orchestrator::builder()
        .config(config)
        .work_dir(temp_dir.path().to_path_buf())
        .supervisor_attach(true)
        .build()
        .await
        .unwrap();
    let port: u16 = orchestrator.get_resolved_parameters()["RECOVERY_LISTENER_PORT"]
        .parse()
        .unwrap();
    let listener = std::net::TcpListener::bind(("127.0.0.1", port))
        .expect("a recovering service must be able to bind its resolved port");
    orchestrator.stop_monitoring_only().await;
    drop(listener);
}
