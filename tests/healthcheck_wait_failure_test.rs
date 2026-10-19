//! A service that stops while fed waits for its healthcheck: the error says
//! why, with the last lines the service wrote.

use fed::error::Error;

mod support;

/// The error of a failed start, whether it came alone or with others.
fn start_failure_message(error: &Error) -> String {
    match error {
        Error::ServiceStartFailed(_, message) => message.clone(),
        Error::Multiple(errors) => errors.iter().map(start_failure_message).collect(),
        other => panic!("expected a start failure, got {other:?}"),
    }
}

#[tokio::test]
async fn a_service_that_stops_during_the_healthcheck_wait_shows_its_last_output() {
    // It outlives fed's startup crash check (300 ms), so it dies while fed is
    // waiting on a healthcheck that never passes.
    let config = support::parse_checked(
        r#"
services:
  db-client:
    process: "sh -c 'sleep 1; echo starting; echo could not resolve db.internal >&2; exit 1'"
    healthcheck:
      command: "false"
      timeout: 30s
entrypoint: db-client
"#,
    );
    let work_dir = tempfile::tempdir().unwrap();
    let mut orchestrator =
        support::new_orchestrator_for_test(config, work_dir.path().to_path_buf())
            .await
            .unwrap();
    orchestrator.initialize().await.unwrap();

    let started = std::time::Instant::now();
    let error = orchestrator.start_all().await.unwrap_err();
    let message = start_failure_message(&error);
    orchestrator.cleanup().await;

    assert!(
        message.contains("during healthcheck wait"),
        "unexpected error: {message}"
    );
    assert!(
        message.contains("could not resolve db.internal"),
        "the error must carry the service's last output: {message}"
    );
    assert!(
        started.elapsed() < std::time::Duration::from_secs(20),
        "fed must report the death, not wait out the healthcheck timeout"
    );
}

#[tokio::test]
#[cfg_attr(not(feature = "docker-tests"), ignore)] // Requires Docker
async fn a_container_that_stops_during_the_healthcheck_wait_shows_its_last_output() {
    let config = support::parse_checked(
        r#"
services:
  db-client:
    image: alpine:latest
    command: ["sh", "-c", "sleep 2; echo could not resolve db.internal >&2; exit 1"]
    healthcheck:
      command: "false"
      timeout: 60s
entrypoint: db-client
"#,
    );
    let work_dir = tempfile::tempdir().unwrap();
    let mut orchestrator =
        support::new_orchestrator_for_test(config, work_dir.path().to_path_buf())
            .await
            .unwrap();
    orchestrator.initialize().await.unwrap();

    let error = orchestrator.start_all().await.unwrap_err();
    let message = start_failure_message(&error);
    orchestrator.cleanup().await;

    assert!(
        message.contains("container stopped during healthcheck wait"),
        "unexpected error: {message}"
    );
    assert!(
        message.contains("could not resolve db.internal"),
        "the error must carry the container's last output: {message}"
    );
}
