#![cfg(unix)]

mod support;

use fed::Status;
use fed::config::ServiceType;
use fed::state::{ServiceState, StateTracker};
use std::process::Stdio;
use std::time::Duration;

struct Reaper(tokio::task::JoinHandle<std::process::ExitStatus>);

impl Drop for Reaper {
    fn drop(&mut self) {
        self.0.abort();
    }
}

async fn run_isolate(
    config_path: &std::path::Path,
    work_dir: &std::path::Path,
    operation: &str,
    force: bool,
) -> std::process::Output {
    let mut command = tokio::process::Command::new(env!("CARGO_BIN_EXE_fed"));
    command
        .arg("-c")
        .arg(config_path)
        .arg("-w")
        .arg(work_dir)
        .args(["isolate", operation])
        .stdin(Stdio::null())
        .kill_on_drop(true);
    if force {
        command.arg("--force");
    }
    tokio::time::timeout(Duration::from_secs(20), command.output())
        .await
        .expect("isolation command timed out")
        .unwrap()
}

/// An unhealthy service is still alive, and a stopping service may still own
/// its ports. Isolation changes must use the same active-state definition as
/// the stop command, rather than abandoning either process in the old scope.
#[tokio::test]
async fn isolation_changes_require_stopping_failing_and_stopping_services() {
    for operation in ["enable", "disable", "rotate"] {
        for status in [Status::Failing, Status::Stopping] {
            let temp = tempfile::tempdir().unwrap();
            let config_path = temp.path().join("fed.yaml");
            let yaml = "services:\n  worker:\n    process: sleep 300\n";
            support::parse_checked(yaml);
            std::fs::write(&config_path, yaml).unwrap();

            // Model a running process whose latest persisted observation is
            // Failing/Stopping, without depending on health-check timing.
            let mut process = tokio::process::Command::new("sleep")
                .arg("300")
                .process_group(0)
                .kill_on_drop(true)
                .spawn()
                .unwrap();
            let pid = process.id().unwrap();
            let mut reaper = Reaper(tokio::spawn(async move { process.wait().await.unwrap() }));
            let mut tracker = StateTracker::new(temp.path().to_path_buf()).await.unwrap();
            tracker.initialize().await.unwrap();
            let mut state =
                ServiceState::new("worker".to_owned(), ServiceType::Process, "root".to_owned());
            state.status = status;
            state.pid = Some(pid);
            tracker.register_service(state).await.unwrap();
            let was_enabled = operation != "enable";
            if was_enabled {
                tracker
                    .set_isolation_mode(true, Some("iso-original".to_owned()))
                    .await
                    .unwrap();
            }
            drop(tracker);

            let output = run_isolate(&config_path, temp.path(), operation, false).await;
            assert!(
                !output.status.success(),
                "isolate {operation} must require confirmation for {status}: {} {}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
            assert!(
                String::from_utf8_lossy(&output.stderr)
                    .contains("Aborted: services must be stopped first")
            );
            assert!(
                !reaper.0.is_finished(),
                "confirmation must precede stopping"
            );
            let tracker = StateTracker::new(temp.path().to_path_buf()).await.unwrap();
            assert_eq!(
                tracker.get_isolation_mode().await,
                (was_enabled, was_enabled.then(|| "iso-original".to_owned()))
            );
            drop(tracker);

            let output = run_isolate(&config_path, temp.path(), operation, true).await;
            assert!(
                output.status.success(),
                "isolate {operation} --force failed for {status}: {} {}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
            let exit = tokio::time::timeout(Duration::from_secs(5), &mut reaper.0)
                .await
                .expect("forced isolation change must stop the old process")
                .unwrap();
            assert!(!exit.success());
            let tracker = StateTracker::new(temp.path().to_path_buf()).await.unwrap();
            assert!(!tracker.get_services().await.contains_key("worker"));
            assert_eq!(tracker.get_isolation_mode().await.0, operation != "disable");
        }
    }
}
