use super::HealthChecker;
use crate::docker::DockerClient;
use crate::error::Result;
use async_trait::async_trait;
use std::collections::HashMap;
use std::process::Stdio;
use std::time::Duration;
use tokio::process::Command;

/// Command-based health checker
pub struct CommandChecker {
    command: String,
    args: Vec<String>,
    timeout: Duration,
    environment: HashMap<String, String>,
}

impl CommandChecker {
    pub fn new(command: String, args: Vec<String>, timeout: Duration) -> Self {
        Self {
            command,
            args,
            timeout,
            environment: HashMap::new(),
        }
    }

    pub fn with_environment(
        command: String,
        args: Vec<String>,
        timeout: Duration,
        environment: HashMap<String, String>,
    ) -> Self {
        Self {
            command,
            args,
            timeout,
            environment,
        }
    }
}

#[async_trait]
impl HealthChecker for CommandChecker {
    async fn check(&self) -> Result<bool> {
        let mut command = Command::new(&self.command);
        command
            .args(&self.args)
            .envs(&self.environment)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            // `timeout` cancels by dropping the status future. Tokio leaves
            // children running on drop unless this is set.
            .kill_on_drop(true);
        let result = tokio::time::timeout(self.timeout, command.status()).await;

        match result {
            Ok(Ok(status)) => Ok(status.success()),
            Ok(Err(_)) | Err(_) => Ok(false),
        }
    }

    fn timeout(&self) -> Duration {
        self.timeout
    }
}

/// Docker command-based health checker - runs commands inside a Docker container
pub struct DockerCommandChecker {
    container_name: String,
    command: String,
    timeout: Duration,
}

impl DockerCommandChecker {
    pub fn new(container_name: String, command: String, timeout: Duration) -> Self {
        Self {
            container_name,
            command,
            timeout,
        }
    }
}

#[async_trait]
impl HealthChecker for DockerCommandChecker {
    async fn check(&self) -> Result<bool> {
        match DockerClient::new()
            .exec_sh(&self.container_name, &self.command, self.timeout)
            .await
        {
            Ok(output) => Ok(output.status.success()),
            Err(_) => Ok(false),
        }
    }

    fn timeout(&self) -> Duration {
        self.timeout
    }
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use nix::sys::signal::{Signal, kill};
    use nix::unistd::Pid;

    async fn wait_for_exit(pid: Pid) -> bool {
        for _ in 0..40 {
            if kill(pid, None).is_err() {
                return true;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        false
    }

    #[tokio::test]
    async fn timed_out_command_healthcheck_kills_child() {
        let temp = tempfile::tempdir().unwrap();
        let pid_path = temp.path().join("healthcheck.pid");
        let checker = CommandChecker::new(
            "sh".into(),
            vec![
                "-c".into(),
                r#"echo $$ > "$1"; exec sleep 30"#.into(),
                "fed-healthcheck".into(),
                pid_path.to_string_lossy().into_owned(),
            ],
            Duration::from_millis(500),
        );

        assert!(!checker.check().await.unwrap());

        let pid: i32 = std::fs::read_to_string(&pid_path)
            .expect("healthcheck command should record its pid before timing out")
            .trim()
            .parse()
            .unwrap();
        let pid = Pid::from_raw(pid);
        let exited = wait_for_exit(pid).await;
        if !exited {
            let _ = kill(pid, Signal::SIGKILL);
        }
        assert!(exited, "timed-out healthcheck child was left running");
    }
}
