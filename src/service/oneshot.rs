use super::{ServiceManager, Status};
use crate::config::Service as ServiceConfig;
use crate::error::{Error, Result};
use async_trait::async_trait;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use tokio::process::Command;

/// Oneshot service manager (`run:`).
///
/// Executes a shell command to completion during startup. There is no
/// long-running process: exit 0 marks the node satisfied ([`Status::Completed`])
/// and lets dependents proceed, while a non-zero exit is a startup error. The
/// command re-runs on every `fed start`, so it must be idempotent
/// (e.g. `prisma db push`).
pub struct OneshotService {
    name: String,
    /// The resolved `run:` command.
    run_command: String,
    /// Absolute working directory the command runs in.
    cwd: PathBuf,
    /// Resolved environment for the command.
    environment: HashMap<String, String>,
    status: Status,
    /// Whether the `run:` command executed successfully in THIS `fed` process.
    ///
    /// Managers are rebuilt fresh each `fed` invocation, so this is `false` at
    /// the start of every `fed start`/`fed restart` — the oneshot runs every
    /// invocation. Within a single startup it is the concurrency-dedup signal:
    /// a second dependent that reaches a shared oneshot sees `has_run` and skips
    /// re-executing (it only gets here after the first execution finished).
    has_run: bool,
}

impl OneshotService {
    pub fn new(
        name: String,
        config: ServiceConfig,
        environment: HashMap<String, String>,
        work_dir: String,
    ) -> Self {
        let cwd = match config.cwd {
            Some(ref c) => {
                let p = Path::new(c);
                if p.is_absolute() {
                    p.to_path_buf()
                } else {
                    Path::new(&work_dir).join(c)
                }
            }
            None => PathBuf::from(&work_dir),
        };
        Self {
            run_command: config.run.clone().unwrap_or_default(),
            name,
            cwd,
            environment,
            status: Status::Stopped,
            has_run: false,
        }
    }

    /// Whether the `run:` command already executed successfully this session.
    pub fn has_run(&self) -> bool {
        self.has_run
    }

    /// Restore a persisted status for display (e.g. `Completed` from a previous
    /// session's state tracker). Deliberately does NOT set `has_run`, so the
    /// oneshot still re-runs on the next `fed start`.
    pub fn restore_status(&mut self, status: Status) {
        self.status = status;
    }
}

#[async_trait]
impl ServiceManager for OneshotService {
    async fn start(&mut self) -> Result<()> {
        self.status = Status::Starting;
        tracing::info!(
            "Running oneshot command for service '{}': {}",
            self.name,
            self.run_command
        );

        // Stream output like install/migrate commands do (inherit stdio).
        let result = Command::new("sh")
            .arg("-ec")
            .arg(&self.run_command)
            .current_dir(&self.cwd)
            .envs(&self.environment)
            .stdin(Stdio::inherit())
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit())
            .status()
            .await;

        let exit = match result {
            Ok(exit) => exit,
            Err(e) => {
                self.status = Status::Failing;
                return Err(Error::Process(format!(
                    "Failed to execute run command for oneshot service '{}': {}",
                    self.name, e
                )));
            }
        };

        if !exit.success() {
            self.status = Status::Failing;
            let code = exit
                .code()
                .map(|c| c.to_string())
                .unwrap_or_else(|| "signal".to_string());
            return Err(Error::Process(format!(
                "Oneshot service '{}' run command failed (exit {})",
                self.name, code
            )));
        }

        self.status = Status::Completed;
        self.has_run = true;
        tracing::info!("Oneshot service '{}' completed", self.name);
        Ok(())
    }

    async fn stop(&mut self) -> Result<()> {
        // Nothing to kill — a oneshot has no running process. Stopping clears the
        // completion so a subsequent start re-runs it.
        self.status = Status::Stopped;
        self.has_run = false;
        Ok(())
    }

    async fn kill(&mut self) -> Result<()> {
        self.status = Status::Stopped;
        self.has_run = false;
        Ok(())
    }

    async fn health(&self) -> Result<bool> {
        Ok(self.status == Status::Completed)
    }

    fn status(&self) -> Status {
        self.status
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn oneshot(name: &str, run: &str, dir: &std::path::Path) -> OneshotService {
        let config = ServiceConfig {
            run: Some(run.to_string()),
            ..Default::default()
        };
        OneshotService::new(
            name.to_string(),
            config,
            HashMap::new(),
            dir.to_string_lossy().to_string(),
        )
    }

    #[tokio::test]
    async fn completes_and_marks_has_run() {
        let dir = tempfile::tempdir().unwrap();
        let marker = dir.path().join("ran");
        let mut svc = oneshot("schema", &format!("touch {}", marker.display()), dir.path());

        assert_eq!(svc.status(), Status::Stopped);
        assert!(!svc.has_run());

        svc.start().await.expect("oneshot should succeed");

        assert_eq!(svc.status(), Status::Completed);
        assert!(svc.has_run());
        assert!(marker.exists(), "run command should have executed");
    }

    #[tokio::test]
    async fn failing_run_returns_error_naming_service() {
        let dir = tempfile::tempdir().unwrap();
        let mut svc = oneshot("migrate", "exit 3", dir.path());

        let err = svc.start().await.expect_err("non-zero exit is an error");
        assert!(err.to_string().contains("migrate"));
        assert_eq!(svc.status(), Status::Failing);
        assert!(!svc.has_run());
    }

    #[tokio::test]
    async fn stop_is_a_clean_noop_that_clears_completion() {
        let dir = tempfile::tempdir().unwrap();
        let mut svc = oneshot("schema", "true", dir.path());
        svc.start().await.unwrap();
        assert!(svc.has_run());

        svc.stop().await.expect("stop must be a graceful no-op");
        assert_eq!(svc.status(), Status::Stopped);
        assert!(!svc.has_run());
    }

    #[tokio::test]
    async fn restore_status_does_not_set_has_run() {
        let dir = tempfile::tempdir().unwrap();
        let mut svc = oneshot("schema", "true", dir.path());
        svc.restore_status(Status::Completed);
        assert_eq!(svc.status(), Status::Completed);
        assert!(
            !svc.has_run(),
            "restored completion must not suppress the next re-run"
        );
    }
}
