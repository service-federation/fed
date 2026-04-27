use super::{BaseService, ServiceManager, Status};
use crate::config::Service as ServiceConfig;
use crate::error::{validate_pid, Error, Result};
use async_trait::async_trait;
use nix::sys::signal::{self, Signal};
use nix::unistd::Pid;
use parking_lot::RwLock;
use std::collections::{HashMap, VecDeque};
use std::path::Path;
use std::process::Stdio;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::{Child, Command};
use tokio::sync::Mutex;
use tokio::time::timeout;

/// Gradle task service manager
pub struct GradleService {
    name: String,
    base: Arc<RwLock<BaseService>>,
    config: ServiceConfig,
    tasks: Vec<String>, // Multiple tasks for grouped execution
    process: Arc<Mutex<Option<Child>>>,
    logs: Arc<Mutex<VecDeque<String>>>,
    /// Process ID for state persistence and restore.
    pid: parking_lot::Mutex<Option<u32>>,
}

impl GradleService {
    pub fn new(
        name: String,
        config: ServiceConfig,
        environment: HashMap<String, String>,
        work_dir: String,
    ) -> Self {
        let tasks = vec![config.gradle_task.clone().unwrap_or_default()];
        Self {
            name: name.clone(),
            base: Arc::new(RwLock::new(BaseService::new(name, environment, work_dir))),
            config,
            tasks,
            process: Arc::new(Mutex::new(None)),
            logs: Arc::new(Mutex::new(VecDeque::new())),
            pid: parking_lot::Mutex::new(None),
        }
    }

    /// Create a grouped Gradle service that runs multiple tasks together
    pub fn new_grouped(
        name: String,
        configs: Vec<ServiceConfig>,
        environment: HashMap<String, String>,
        work_dir: String,
    ) -> Self {
        let tasks: Vec<String> = configs
            .iter()
            .filter_map(|c| c.gradle_task.clone())
            .collect();

        // Use the first config as base, but override with merged environment
        let config = configs.first().cloned().unwrap_or_default();

        Self {
            name: name.clone(),
            base: Arc::new(RwLock::new(BaseService::new(name, environment, work_dir))),
            config,
            tasks,
            process: Arc::new(Mutex::new(None)),
            logs: Arc::new(Mutex::new(VecDeque::new())),
            pid: parking_lot::Mutex::new(None),
        }
    }

    /// Set the PID for a restored Gradle service.
    /// Called during state restoration to reconnect to a running Gradle process.
    pub fn set_pid(&self, pid: u32) {
        *self.pid.lock() = Some(pid);
        let mut base = self.base.write();
        base.set_status(Status::Running);
    }

    async fn spawn_gradle_process(&self) -> Result<Child> {
        if self.tasks.is_empty() || self.tasks.iter().all(|t| t.is_empty()) {
            return Err(Error::Config("No gradle tasks specified".to_string()));
        }

        let base = self.base.read();
        let work_dir = if let Some(ref cwd) = self.config.cwd {
            // Check if cwd is already an absolute path
            let cwd_path = Path::new(cwd);
            if cwd_path.is_absolute() {
                cwd.clone()
            } else {
                format!("{}/{}", base.work_dir, cwd)
            }
        } else {
            base.work_dir.clone()
        };

        // Check for gradlew wrapper, fall back to gradle command
        let gradlew_path = format!("{}/gradlew", work_dir);
        let gradle_cmd = if Path::new(&gradlew_path).exists() {
            "./gradlew"
        } else {
            // Verify gradle is available on PATH before attempting to use it
            if which::which("gradle").is_err() {
                return Err(Error::ServiceStartFailed(
                    base.name.clone(),
                    format!(
                        "Neither ./gradlew nor gradle command found. \
                        Either add a gradlew wrapper to '{}' or install gradle and add it to PATH.",
                        work_dir
                    ),
                ));
            }
            "gradle"
        };

        let mut cmd = Command::new(gradle_cmd);
        // Add all tasks as arguments
        for task in &self.tasks {
            if !task.is_empty() {
                cmd.arg(task);
            }
        }
        cmd.current_dir(&work_dir)
            .envs(&base.environment)
            // Markers for the recursion check in main.rs.
            .env("FED_SPAWNED_BY_SERVICE", &base.name)
            .env("FED_SPAWNED_FROM_WORKSPACE", &work_dir)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .process_group(0); // Create new process group for proper cleanup of gradle daemons

        let mut child = cmd.spawn().map_err(|e| {
            Error::ServiceStartFailed(
                base.name.clone(),
                format!("Failed to start gradle task: {}", e),
            )
        })?;

        // Spawn log capture tasks
        if let Some(stdout) = child.stdout.take() {
            let logs = self.logs.clone();
            tokio::spawn(async move {
                let reader = BufReader::new(stdout);
                let mut lines = reader.lines();
                while let Ok(Some(line)) = lines.next_line().await {
                    let mut logs = logs.lock().await;
                    logs.push_back(line);
                    if logs.len() > 1000 {
                        logs.pop_front();
                    }
                }
            });
        }

        if let Some(stderr) = child.stderr.take() {
            let logs = self.logs.clone();
            tokio::spawn(async move {
                let reader = BufReader::new(stderr);
                let mut lines = reader.lines();
                while let Ok(Some(line)) = lines.next_line().await {
                    let mut logs = logs.lock().await;
                    logs.push_back(format!("[stderr] {}", line));
                    if logs.len() > 1000 {
                        logs.pop_front();
                    }
                }
            });
        }

        Ok(child)
    }
}

#[async_trait]
impl ServiceManager for GradleService {
    async fn start(&mut self) -> Result<()> {
        {
            let mut base = self.base.write();
            if base.status == Status::Running || base.status == Status::Healthy {
                return Ok(());
            }
            base.set_status(Status::Starting);
        }

        // Start the gradle task
        let child = self.spawn_gradle_process().await?;
        if let Some(raw_pid) = child.id() {
            *self.pid.lock() = Some(raw_pid);
        }
        *self.process.lock().await = Some(child);

        {
            let mut base = self.base.write();
            base.set_status(Status::Running);
        }

        Ok(())
    }

    async fn stop(&mut self) -> Result<()> {
        {
            let mut base = self.base.write();
            if base.status == Status::Stopped {
                return Ok(());
            }
            base.set_status(Status::Stopping);
        }

        let mut process = self.process.lock().await;
        if let Some(child) = process.as_mut() {
            if let Some(raw_pid) = child.id() {
                // Validate PID: rejects 0, 1, and values > i32::MAX
                let pid = validate_pid(raw_pid, &self.name)?;

                // Send SIGTERM to the process group (negative PID)
                // This ensures gradle daemons and child processes also receive the signal
                let pgid = Pid::from_raw(-(pid.as_raw()));

                // Try to signal the process group first
                let signal_result = signal::kill(pgid, Signal::SIGTERM)
                    .or_else(|_| signal::kill(pid, Signal::SIGTERM)); // Fallback to just the process

                if let Ok(()) = signal_result {
                    // Wait up to 10 seconds for graceful shutdown
                    let wait_result = timeout(Duration::from_secs(10), child.wait()).await;

                    match wait_result {
                        Ok(Ok(_)) => {
                            // Process exited gracefully
                            tracing::debug!("Gradle process {} exited gracefully", self.name);
                        }
                        Ok(Err(e)) => {
                            tracing::warn!("Error waiting for gradle process {}: {}", self.name, e);
                            // Try SIGKILL to process group as fallback
                            let _ = signal::kill(pgid, Signal::SIGKILL)
                                .or_else(|_| signal::kill(pid, Signal::SIGKILL));
                            let _ = timeout(Duration::from_secs(2), child.wait()).await;
                        }
                        Err(_) => {
                            // Timeout - process didn't exit, force kill
                            tracing::warn!(
                                "Gradle process {} did not exit after SIGTERM, sending SIGKILL",
                                self.name
                            );
                            let _ = signal::kill(pgid, Signal::SIGKILL)
                                .or_else(|_| signal::kill(pid, Signal::SIGKILL));
                            let _ = timeout(Duration::from_secs(2), child.wait()).await;
                        }
                    }
                } else {
                    // SIGTERM failed, try SIGKILL directly
                    let _ = child.kill().await;
                    let _ = timeout(Duration::from_secs(2), child.wait()).await;
                }
            } else {
                // Process already dead, just reap
                let _ = timeout(Duration::from_secs(1), child.wait()).await;
            }
        }
        *process = None;

        {
            let mut base = self.base.write();
            base.set_status(Status::Stopped);
        }

        Ok(())
    }

    async fn kill(&mut self) -> Result<()> {
        self.stop().await
    }

    async fn health(&self) -> Result<bool> {
        let mut process = self.process.lock().await;
        if let Some(child) = process.as_mut() {
            // Check if process has exited (reaps zombie if needed)
            match child.try_wait() {
                Ok(Some(_status)) => Ok(false), // Process exited
                Ok(None) => Ok(true),           // Still running
                Err(_) => Ok(false),            // Error checking status
            }
        } else {
            Ok(false)
        }
    }

    fn status(&self) -> Status {
        self.base.read().status
    }

    fn name(&self) -> &str {
        &self.name
    }

    async fn logs(&self, tail: Option<usize>) -> Result<Vec<String>> {
        let logs = self.logs.lock().await;
        if let Some(n) = tail {
            Ok(logs.iter().rev().take(n).rev().cloned().collect())
        } else {
            Ok(logs.iter().cloned().collect())
        }
    }
    fn get_pid(&self) -> Option<u32> {
        *self.pid.lock()
    }

    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
}
