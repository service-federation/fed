use super::{BaseService, LogCapture, OutputMode, ServiceManager, Status};
use crate::config::Service as ServiceConfig;
use crate::error::{validate_pid, validate_pid_for_check, Error, Result};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use nix::sys::signal::{self, killpg, Signal};
use parking_lot::RwLock;
use std::collections::HashMap;
use std::process::Stdio;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::process::{Child, Command};

/// Health check cache TTL: 500ms - short enough to be responsive but reduces load
const HEALTH_CACHE_TTL: Duration = Duration::from_millis(500);

/// Synchronous mutex for fields that are never held across await points.
/// Uses parking_lot for better performance in uncontended cases.
///
/// This type alias makes it explicit that we're using a synchronous mutex
/// (parking_lot) rather than an async mutex (tokio::sync::Mutex), which
/// is important for understanding the concurrency model.
type SyncMutex<T> = parking_lot::Mutex<T>;

/// Process-based service manager
///
/// # Mutex Type Selection
///
/// This struct deliberately uses different mutex types based on access patterns:
///
/// - **`parking_lot::RwLock`/`Mutex`**: For synchronous, CPU-bound operations that
///   never cross `.await` boundaries. These are faster for short critical sections.
///
/// - **`tokio::sync::Mutex`**: For async operations that may hold the lock across
///   `.await` points. Using parking_lot here would block the tokio runtime.
///
/// This design prevents deadlocks while maximizing performance.
pub struct ProcessService {
    name: String,
    /// Service state (status, environment, work_dir).
    /// Uses parking_lot::RwLock - access is brief and synchronous.
    base: Arc<RwLock<BaseService>>,
    config: ServiceConfig,
    /// Child process handle.
    /// Uses tokio::sync::Mutex - operations (wait, kill) are async.
    process: Arc<tokio::sync::Mutex<Option<Child>>>,
    /// Output capture mode.
    output_mode: OutputMode,
    /// Process ID for signal operations.
    /// Uses SyncMutex (parking_lot::Mutex) - quick synchronous access, never held across await.
    pid: Arc<SyncMutex<Option<u32>>>,
    /// Log capture and management (encapsulates log buffer, file path, tasks, and shutdown).
    log_capture: LogCapture,
    /// Cached health check result with timestamp.
    /// Uses tokio::sync::Mutex - serializes async health checks to prevent duplicate work.
    health_cache: Arc<tokio::sync::Mutex<(Option<bool>, Instant)>>,
    /// Grace period for graceful shutdown before SIGKILL
    grace_period: Duration,
    /// When the process was started (used to detect PID reuse in detached stop)
    started_at: Arc<SyncMutex<Option<DateTime<Utc>>>>,
}

impl ProcessService {
    pub fn new(
        name: String,
        config: ServiceConfig,
        environment: HashMap<String, String>,
        work_dir: String,
        output_mode: OutputMode,
        log_file_path: Option<std::path::PathBuf>,
    ) -> Self {
        let grace_period = config.get_grace_period();
        Self {
            name: name.clone(),
            base: Arc::new(RwLock::new(BaseService::new(
                name.clone(),
                environment,
                work_dir,
            ))),
            config,
            process: Arc::new(tokio::sync::Mutex::new(None)),
            output_mode,
            pid: Arc::new(SyncMutex::new(None)),
            log_capture: LogCapture::new(name, log_file_path, output_mode),
            health_cache: Arc::new(tokio::sync::Mutex::new((None, Instant::now()))),
            grace_period,
            started_at: Arc::new(SyncMutex::new(None)),
        }
    }

    /// Restore PID from state (used when reattaching to detached processes)
    pub fn set_pid(&self, pid: u32) {
        self.set_pid_with_start_time(pid, None);
    }

    /// Restore PID from state with a known start time (used when reattaching to detached processes).
    /// The start time enables PID reuse detection during stop.
    pub fn set_pid_with_start_time(&self, pid: u32, started_at: Option<DateTime<Utc>>) {
        if let Err(e) = self.validate_and_store_pid(pid) {
            tracing::error!("Failed to restore PID for '{}': {}", self.name, e);
            return;
        }
        *self.started_at.lock() = started_at;
        // Also set status to Running since we're restoring an existing process
        let mut base = self.base.write();
        base.set_status(Status::Running);
    }

    /// Validates that a PID can be safely converted to i32 for Unix signal operations
    /// Returns an error if PID exceeds i32::MAX
    fn validate_and_store_pid(&self, pid: u32) -> Result<()> {
        if pid > i32::MAX as u32 {
            return Err(Error::Validation(format!(
                "Service '{}': PID {} exceeds i32::MAX, cannot be used for signal operations on this platform",
                self.name, pid
            )));
        }
        if pid == 0 {
            return Err(Error::Validation(format!(
                "Service '{}': PID cannot be 0",
                self.name
            )));
        }
        *self.pid.lock() = Some(pid);
        Ok(())
    }

    async fn spawn_process(&self) -> Result<Child> {
        let process_cmd = self
            .config
            .process
            .as_ref()
            .ok_or_else(|| Error::Config("No process command specified".to_string()))?;

        // Extract values from base lock before any async operations
        let (work_dir, environment, service_name) = {
            let base = self.base.read();
            let work_dir = if let Some(ref cwd) = self.config.cwd {
                // Check if cwd is already an absolute path
                let cwd_path = std::path::Path::new(cwd);
                if cwd_path.is_absolute() {
                    cwd.clone()
                } else {
                    format!("{}/{}", base.work_dir, cwd)
                }
            } else {
                base.work_dir.clone()
            };
            (work_dir, base.environment.clone(), base.name.clone())
        };

        tracing::debug!(
            "Spawning process for '{}' in work_dir: {:?}, command: {:?}, output_mode: {}",
            service_name,
            work_dir,
            process_cmd,
            self.output_mode
        );

        let mut cmd = Command::new("/bin/bash");

        // NOTE: We do NOT shell-escape the process command here because:
        // 1. The command comes from service-federation.yaml (trusted config)
        // 2. We pass it to bash -c which expects a shell command string to parse
        // 3. Shell-escaping would break commands with quotes/args (e.g., `echo "hello"`
        //    becomes `'echo "hello"'` which bash treats as a literal filename)
        //
        // The config file is the trust boundary - if an attacker can modify it,
        // they already have write access to the project.

        // In File mode, wrap command with nohup to truly detach from terminal
        let final_cmd = if self.output_mode.is_file() {
            // PID Capture Strategy:
            // The inner bash first prints its own PID ($$) to stdout, then redirects stdout/stderr
            // to the log file. This ensures we capture the PID of the actual long-running process,
            // not a wrapper shell that might exit immediately.
            //
            // Previous approaches using 'exec $cmd' failed because exec only replaces the shell
            // with the FIRST command in a multi-line script. When that command terminates,
            // the shell exits and we capture a stale PID.
            //
            // This approach works correctly for all cases:
            // - Single-line commands (e.g., sleep 300)
            // - Multi-line scripts (e.g., YAML | blocks with multiple commands)
            // - Commands with output that needs logging

            // Redirect to log file if available, otherwise to /dev/null
            // Note: Log path is escaped since it may contain spaces
            let redirect_target = if let Some(log_path) = self.log_capture.log_file_path() {
                use shell_escape::escape;
                // Use >> for append mode to preserve logs across restarts
                let escaped_path = escape(log_path.to_string_lossy());
                format!(">> {}", escaped_path)
            } else {
                "> /dev/null".to_string()
            };

            // Bash prints its PID, redirects I/O, then runs the user's commands.
            // Note: The PGID may differ from the PID due to how nohup/backgrounding works.
            // The stop() method looks up the actual PGID to ensure all children are killed.
            format!(
                "nohup bash -c 'echo $$; exec 1{} 2>&1; {}' &",
                redirect_target, process_cmd
            )
        } else {
            // Non-detached mode: pass command directly to bash -c
            format!("exec {}", process_cmd)
        };

        cmd.arg("-ec")
            .arg(&final_cmd)
            .current_dir(&work_dir)
            .envs(&environment)
            // Markers for the recursion check in main.rs. Service name
            // is identity for the error message; workspace path lets
            // the child fed distinguish same-workspace recursion (real
            // problem) from cross-workspace invocation (legitimate).
            .env("FED_SPAWNED_BY_SERVICE", &service_name)
            .env("FED_SPAWNED_FROM_WORKSPACE", &work_dir);

        // Configure stdio based on output mode
        match self.output_mode {
            OutputMode::File => {
                // File mode: capture stdout to read the background PID
                cmd.stdout(Stdio::piped())
                    .stderr(Stdio::null())
                    .stdin(Stdio::null());
            }
            OutputMode::Captured => {
                // Captured mode: capture logs via pipes to memory buffer
                cmd.stdout(Stdio::piped()).stderr(Stdio::piped());
            }
            OutputMode::Passthrough => {
                // Passthrough mode: inherit parent's stdio for direct visibility
                cmd.stdout(Stdio::inherit())
                    .stderr(Stdio::inherit())
                    .stdin(Stdio::inherit());
            }
        }

        cmd.kill_on_drop(false) // Don't kill on drop to support detach mode
            .process_group(0); // Create new process group for proper signal handling

        // Apply resource limits on Unix systems
        #[cfg(unix)]
        if let Some(ref resources) = self.config.resources {
            // CRITICAL: Parse ALL resource limits BEFORE fork() to ensure async-signal-safety.
            // String parsing (parse_memory_to_bytes) is NOT async-signal-safe and must happen here.
            let mut parsed_limits = resource_limits::ParsedResourceLimits::from_config(resources);

            // Validate limits against system hard limits BEFORE fork.
            // If limits exceed the hard limit, they will be capped and warnings logged.
            let validation = parsed_limits.validate_against_system();

            // Log warnings for any capped limits so users understand what happened
            if let Some(resource_limits::LimitValidationResult::Capped {
                requested,
                hard_limit,
            }) = validation.memory
            {
                tracing::warn!(
                    "Service '{}': Requested memory limit {} exceeds system hard limit {}, capping to hard limit",
                    service_name,
                    resource_limits::format_bytes(requested),
                    resource_limits::format_bytes(hard_limit)
                );
            }

            if let Some(resource_limits::LimitValidationResult::Capped {
                requested,
                hard_limit,
            }) = validation.nofile
            {
                tracing::warn!(
                    "Service '{}': Requested nofile limit {} exceeds system hard limit {}, capping to hard limit",
                    service_name, requested, hard_limit
                );
            }

            #[cfg(target_os = "linux")]
            if let Some(resource_limits::LimitValidationResult::Capped {
                requested,
                hard_limit,
            }) = validation.pids
            {
                tracing::warn!(
                    "Service '{}': Requested pids limit {} exceeds system hard limit {}, capping to hard limit",
                    service_name, requested, hard_limit
                );
            }

            // SAFETY: pre_exec is unsafe because the closure runs after fork() but before exec()
            // in the child process. During this window, only async-signal-safe functions can be called.
            //
            // Safety invariants upheld:
            // 1. parsed_limits contains only Copy types (Option<u64>) - no heap allocations
            // 2. All string parsing happened BEFORE this point (in from_config)
            // 3. The closure only calls setrlimit() which is async-signal-safe per POSIX
            // 4. No heap allocation, no locks, no I/O in the closure itself
            // 5. The closure is FnMut + Send which is required for pre_exec
            //
            // See: https://man7.org/linux/man-pages/man7/signal-safety.7.html
            unsafe {
                #[allow(unused_imports)]
                use std::os::unix::process::CommandExt;
                cmd.pre_exec(move || parsed_limits.apply());
            }
        }

        let mut child = cmd.spawn().map_err(|e| {
            tracing::error!(
                "Failed to spawn process for '{}': {} (work_dir: {:?})",
                service_name,
                e,
                work_dir
            );
            Error::ServiceStartFailed(service_name.clone(), e.to_string())
        })?;

        // Spawn log capture tasks only in Captured mode.
        // In File mode, we need stdout to read the background PID first.
        // In Passthrough mode, output goes directly to parent (no capture needed).
        if self.output_mode.is_captured() {
            self.log_capture
                .spawn_capture_tasks(child.stdout.take(), child.stderr.take())
                .await;
        }

        Ok(child)
    }
}

#[async_trait]
impl ServiceManager for ProcessService {
    #[tracing::instrument(skip(self), fields(service.name = %self.name))]
    async fn start(&mut self) -> Result<()> {
        {
            let mut base = self.base.write();
            if base.status == Status::Running || base.status == Status::Healthy {
                return Ok(());
            }
            base.set_status(Status::Starting);
        }

        // Note: Install command is handled by orchestrator.run_install_if_needed()
        // before calling start(). Don't duplicate that logic here.

        // Start the process
        let mut child = self.spawn_process().await?;

        // Record start time for PID reuse detection in detached stop
        *self.started_at.lock() = Some(Utc::now());

        if self.output_mode.is_file() {
            // In File mode: Read the background PID from stdout
            if let Some(stdout) = child.stdout.take() {
                use tokio::io::AsyncBufReadExt;
                let mut reader = tokio::io::BufReader::new(stdout);
                let mut pid_str = String::new();

                // Read PID with timeout - the wrapper script should print the PID immediately
                // after spawning the process, so 3s is more than enough
                let read_result =
                    tokio::time::timeout(Duration::from_secs(3), reader.read_line(&mut pid_str))
                        .await;

                match read_result {
                    Ok(Ok(bytes_read)) => {
                        tracing::debug!(
                            "Read {} bytes from stdout for '{}': '{}'",
                            bytes_read,
                            self.name,
                            pid_str.trim()
                        );
                        if let Ok(bg_pid) = pid_str.trim().parse::<u32>() {
                            if let Err(e) = self.validate_and_store_pid(bg_pid) {
                                tracing::error!(
                                    "Invalid PID {} for '{}': {}",
                                    bg_pid,
                                    self.name,
                                    e
                                );
                            } else {
                                tracing::debug!(
                                    "Captured and stored background PID {} for '{}'",
                                    bg_pid,
                                    self.name
                                );
                            }
                        } else {
                            tracing::warn!(
                                "Failed to parse background PID for '{}': got '{}'",
                                self.name,
                                pid_str.trim()
                            );
                        }
                    }
                    Ok(Err(e)) => {
                        tracing::warn!("Failed to read background PID for '{}': {}", self.name, e);
                    }
                    Err(_) => {
                        tracing::warn!(
                            "Timeout reading background PID for '{}' - process may be slow to start",
                            self.name
                        );
                    }
                }
            }

            // Wait for bash wrapper to exit
            let _ = child.wait().await;

            // DX improvement: In detached mode, wait briefly and check if process crashed
            // This catches immediate startup failures (missing env vars, syntax errors, etc.)
            tokio::time::sleep(Duration::from_millis(500)).await;

            let is_alive = self.health().await.unwrap_or(false);
            if !is_alive {
                // Process crashed during startup - read logs from file
                let log_preview = if let Some(ref log_path) = self.log_capture.log_file_path() {
                    match tokio::fs::read_to_string(log_path).await {
                        Ok(contents) => {
                            let lines: Vec<&str> = contents.lines().collect();
                            let last_lines: Vec<&str> =
                                lines.iter().rev().take(15).rev().cloned().collect();
                            if last_lines.is_empty() {
                                "Log file is empty (process may have crashed before writing output)"
                                    .to_string()
                            } else {
                                format!(
                                    "Last {} lines from logs:\n{}",
                                    last_lines.len(),
                                    last_lines.join("\n")
                                )
                            }
                        }
                        Err(_) => "Could not read log file".to_string(),
                    }
                } else {
                    "No log file available".to_string()
                };

                let error_msg = format!(
                    "Service '{}' crashed immediately after starting.\n\n{}",
                    self.name, log_preview
                );

                // Store error message for visibility
                {
                    let mut base = self.base.write();
                    base.set_error(error_msg.clone());
                    base.set_status(Status::Failing);
                }

                return Err(Error::ServiceStartFailed(self.name.clone(), error_msg));
            }
        } else {
            // In interactive mode: Store Child for log capture and proper cleanup
            // Also store PID
            if let Some(pid) = child.id() {
                if let Err(e) = self.validate_and_store_pid(pid) {
                    tracing::error!("Invalid PID {} for '{}': {}", pid, self.name, e);
                }
            }
            *self.process.lock().await = Some(child);
        }

        {
            let mut base = self.base.write();
            base.set_status(Status::Running);
        }

        // DX improvement: Wait briefly to catch immediate crashes and capture startup logs
        // This helps detect processes that fail within the first few hundred milliseconds
        // (Only in Captured/Passthrough modes - File mode has its own crash detection above)
        if !self.output_mode.is_file() {
            tokio::time::sleep(Duration::from_millis(300)).await;

            // Check if the process is still alive
            let is_alive = self.health().await.unwrap_or(false);
            if !is_alive {
                // Process crashed during startup - capture logs for error reporting
                let logs = self.logs(Some(20)).await.unwrap_or_default();
                let log_preview = if logs.is_empty() {
                    "No logs captured (process may have crashed too quickly)".to_string()
                } else {
                    let preview_lines = logs.iter().take(10).cloned().collect::<Vec<_>>();
                    format!(
                        "Last {} lines:\n{}",
                        preview_lines.len(),
                        preview_lines.join("\n")
                    )
                };

                let error_msg = format!("Process crashed during startup. {}", log_preview);

                // Store error message for visibility
                {
                    let mut base = self.base.write();
                    base.set_error(error_msg.clone());
                }

                return Err(Error::ServiceStartFailed(self.name.clone(), error_msg));
            }
        }

        Ok(())
    }

    #[tracing::instrument(skip(self), fields(service.name = %self.name))]
    async fn stop(&mut self) -> Result<()> {
        {
            let mut base = self.base.write();
            if base.status == Status::Stopped {
                return Ok(());
            }
            base.set_status(Status::Stopping);
        }

        // If we have a Child handle (interactive mode), use it for clean shutdown
        let mut process = self.process.lock().await;
        if let Some(child) = process.as_mut() {
            if let Some(raw_pid) = child.id() {
                // Validate PID: rejects 0, 1, and values > i32::MAX
                let pid = validate_pid(raw_pid, &self.name)?;

                // Send SIGTERM to process group first, fall back to individual process
                // Use killpg() for process group signaling (more explicit than negative PID)
                let signal_result =
                    killpg(pid, Signal::SIGTERM).or_else(|_| signal::kill(pid, Signal::SIGTERM));

                if signal_result.is_ok() {
                    // Wait for graceful shutdown using configured grace period
                    match tokio::time::timeout(self.grace_period, child.wait()).await {
                        Ok(Ok(_)) => {
                            tracing::debug!("Process {} exited gracefully", self.name);
                        }
                        Ok(Err(e)) => {
                            tracing::warn!("Error waiting for process {}: {}", self.name, e);
                            let _ = killpg(pid, Signal::SIGKILL)
                                .or_else(|_| signal::kill(pid, Signal::SIGKILL));
                            let _ =
                                tokio::time::timeout(Duration::from_secs(2), child.wait()).await;
                        }
                        Err(_) => {
                            tracing::warn!(
                                "Process {} did not exit after SIGTERM (grace period: {:?}), sending SIGKILL",
                                self.name,
                                self.grace_period
                            );
                            let _ = killpg(pid, Signal::SIGKILL)
                                .or_else(|_| signal::kill(pid, Signal::SIGKILL));
                            let _ =
                                tokio::time::timeout(Duration::from_secs(2), child.wait()).await;
                        }
                    }
                }
            }
            *process = None;
        } else {
            // Detached mode - only have a PID, no Child handle
            drop(process);

            let pid_opt = *self.pid.lock();
            if let Some(pid_val) = pid_opt {
                // Validate PID: rejects 0, 1, and values > i32::MAX
                let pid = validate_pid(pid_val, &self.name)?;

                // Check for PID reuse before sending signals to avoid killing the wrong process
                let started_at = *self.started_at.lock();
                if let Some(expected_start) = started_at {
                    if !crate::error::validate_pid_start_time(pid_val, expected_start) {
                        tracing::warn!(
                            "PID {} for service '{}' was reused by another process, skipping kill",
                            pid_val,
                            self.name
                        );
                        *self.pid.lock() = None;

                        // Shutdown log capture tasks
                        self.log_capture.shutdown().await;
                        *self.health_cache.lock().await = (None, Instant::now());

                        let mut base = self.base.write();
                        base.set_status(Status::Stopped);
                        return Ok(());
                    }
                }

                // Try to get the actual process group ID (PGID) of the process.
                // This is important because the PGID may differ from the PID if the process
                // was reparented or created without setsid. Using the correct PGID ensures
                // we kill all child processes, not just the leader.
                let pgid = Self::get_process_group(pid_val).unwrap_or(pid);

                // Send SIGTERM to process group first
                let signal_result = killpg(pgid, Signal::SIGTERM);
                let signal_result = if signal_result.is_err() {
                    // Process group kill failed, try individual process
                    tracing::debug!(
                        "killpg failed for PGID {} (service: {}), trying individual PID",
                        pgid.as_raw(),
                        self.name
                    );
                    signal::kill(pid, Signal::SIGTERM)
                } else {
                    signal_result
                };

                if signal_result.is_ok() {
                    // Poll for process exit using configured grace period
                    let poll_interval = Duration::from_millis(100);
                    let poll_count =
                        (self.grace_period.as_millis() / poll_interval.as_millis()).max(1) as u64;
                    for _ in 0..poll_count {
                        tokio::time::sleep(poll_interval).await;
                        if signal::kill(pid, None).is_err() {
                            break;
                        }
                    }

                    // If still running, send SIGKILL to process group
                    if signal::kill(pid, None).is_ok() {
                        tracing::warn!(
                            "Process {} did not exit after SIGTERM (grace period: {:?}), sending SIGKILL",
                            self.name,
                            self.grace_period
                        );
                        let _ = killpg(pgid, Signal::SIGKILL)
                            .or_else(|_| signal::kill(pid, Signal::SIGKILL));
                        tokio::time::sleep(Duration::from_millis(500)).await;
                    }
                } else {
                    tracing::warn!(
                        "Failed to send signal to PID {} (service: {})",
                        pid_val,
                        self.name
                    );
                }

                *self.pid.lock() = None;
            }
        }

        // Shutdown log capture tasks
        self.log_capture.shutdown().await;

        // Clear health check cache to prevent stale "healthy" status after stop
        *self.health_cache.lock().await = (None, Instant::now());

        {
            let mut base = self.base.write();
            base.set_status(Status::Stopped);
        }

        Ok(())
    }

    async fn kill(&mut self) -> Result<()> {
        // Immediate SIGKILL without SIGTERM first (unlike stop())
        {
            let mut base = self.base.write();
            if base.status == Status::Stopped {
                return Ok(());
            }
            base.set_status(Status::Stopping);
        }

        let pid_opt = *self.pid.lock();

        if let Some(pid_val) = pid_opt {
            // Validate PID: rejects 0, 1, and values > i32::MAX
            let pid = validate_pid(pid_val, &self.name)?;

            // Send SIGKILL immediately (no SIGTERM first)
            // Use killpg() for process group signaling
            let _ = killpg(pid, Signal::SIGKILL).or_else(|_| signal::kill(pid, Signal::SIGKILL));

            // Brief wait for process to be reaped
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        // If we have a Child handle (interactive mode), wait for it to exit
        // IMPORTANT: Wait for child BEFORE clearing PID to ensure accurate state tracking
        let mut process = self.process.lock().await;
        if let Some(child) = process.as_mut() {
            let _ = child.wait().await;
        }
        *process = None;

        // Now that the process is confirmed dead, clear PID
        *self.pid.lock() = None;

        // Shutdown log capture tasks
        self.log_capture.shutdown().await;

        // Clear health check cache to prevent stale "healthy" status after kill
        *self.health_cache.lock().await = (None, Instant::now());

        {
            let mut base = self.base.write();
            base.set_status(Status::Stopped);
        }

        Ok(())
    }

    async fn health(&self) -> Result<bool> {
        // Lock cache for the entire operation to serialize health checks.
        // This prevents multiple concurrent threads from all performing expensive
        // health checks when the cache expires.
        let mut cache = self.health_cache.lock().await;

        // Check if cached result is still valid
        if let (Some(result), timestamp) = *cache {
            if timestamp.elapsed() < HEALTH_CACHE_TTL {
                return Ok(result);
            }
        }

        // Cache expired or empty - perform actual health check while holding lock.
        // This serializes health checks but prevents duplicate work.
        let result = self.check_health_internal().await;

        // Update cache with new result
        if let Ok(healthy) = result {
            *cache = (Some(healthy), Instant::now());

            // Update status if process has exited - this ensures TUI sees the change immediately
            if !healthy {
                let current_status = self.base.read().status;
                if current_status == Status::Running || current_status == Status::Healthy {
                    let mut base = self.base.write();
                    base.set_status(Status::Failing);
                }
            }

            Ok(healthy)
        } else {
            result
        }
    }

    fn status(&self) -> Status {
        self.base.read().status
    }

    fn name(&self) -> &str {
        &self.name
    }

    async fn logs(&self, tail: Option<usize>) -> Result<Vec<String>> {
        self.log_capture.read_logs(tail).await
    }

    fn get_pid(&self) -> Option<u32> {
        // Return stored PID (works for both detached and interactive mode)
        *self.pid.lock()
    }

    fn get_last_error(&self) -> Option<String> {
        self.base.read().last_error.clone()
    }

    async fn get_resource_usage(&self) -> Option<super::ResourceUsage> {
        // Only query resources if we have a PID
        let pid = self.get_pid()?;

        // Query resource usage using platform-specific methods
        Some(super::ResourceUsage::query(pid).await)
    }

    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

// Private helper implementation for ProcessService
impl ProcessService {
    async fn check_health_internal(&self) -> Result<bool> {
        // DX improvement: In Captured/Passthrough mode, prefer Child handle check to properly detect defunct/zombie processes
        // Child::try_wait() is more reliable as it reaps zombies and detects actual process state
        if !self.output_mode.is_file() {
            let mut process = self.process.lock().await;
            if let Some(child) = process.as_mut() {
                // Check if process has exited (reaps zombie/defunct processes)
                match child.try_wait() {
                    Ok(Some(_status)) => return Ok(false), // Process exited
                    Ok(None) => return Ok(true),           // Still running
                    Err(_) => return Ok(false),            // Error checking status
                }
            }
        }

        // For File mode or when Child handle is unavailable, check PID
        // Improved approach: Use /proc/[pid]/status on Linux (container-safe), fallback to signal check
        let pid_val = *self.pid.lock(); // Extract value before await
        tracing::trace!(
            "Health check for '{}': stored PID = {:?}",
            self.name,
            pid_val
        );
        if let Some(pid_val) = pid_val {
            // Validate PID for read-only check (allows PID 1, rejects 0 and >i32::MAX)
            let Some(pid) = validate_pid_for_check(pid_val) else {
                tracing::error!("PID {} is invalid, cannot check health", pid_val);
                return Ok(false); // Treat as unhealthy
            };
            // First check if process exists with signal(0)
            if signal::kill(pid, None).is_err() {
                return Ok(false); // Process doesn't exist
            }

            // Process exists - check if it's defunct/zombie using /proc (most reliable, container-safe)
            #[cfg(target_os = "linux")]
            {
                let proc_status_path = format!("/proc/{}/status", pid_val);
                if let Ok(status_content) = tokio::fs::read_to_string(&proc_status_path).await {
                    // Look for "State:" line which contains single letter state code
                    for line in status_content.lines() {
                        if line.starts_with("State:") {
                            // Format: "State:\tZ (zombie)" or "State:\tT (stopped)"
                            let state_char = line.chars().find(|c| c.is_alphabetic());
                            if let Some(state) = state_char {
                                match state {
                                    'Z' | 'T' | 'X' | 'x' => return Ok(false), // Zombie, stopped, or dead
                                    'R' | 'S' | 'D' | 'W' => return Ok(true), // Running, sleeping, disk sleep, paging
                                    _ => {} // Unknown state, continue with fallback
                                }
                            }
                            break;
                        }
                    }
                }
            }

            // Fallback to ps command only on non-Linux or if /proc unavailable
            #[cfg(not(target_os = "linux"))]
            {
                let output = tokio::process::Command::new("ps")
                    .args(["-p", &pid_val.to_string(), "-o", "stat="])
                    .output()
                    .await;

                if let Ok(output) = output {
                    if output.status.success() {
                        let stat = String::from_utf8_lossy(&output.stdout).trim().to_string();
                        // Process state codes: Z = zombie, T = stopped
                        if stat.starts_with('Z') || stat.starts_with('T') {
                            return Ok(false); // Defunct/zombie or stopped
                        }
                        return Ok(true); // Process is alive and running
                    }
                }
            }

            // Fallback: if all checks pass (signal check succeeded), assume running
            return Ok(true);
        }

        Ok(false)
    }

    /// Get the process group ID (PGID) for a given PID.
    ///
    /// This is used for proper signal delivery in detached mode, where the PGID
    /// may differ from the PID if the process was reparented or started without setsid.
    #[cfg(unix)]
    fn get_process_group(pid: u32) -> Option<nix::unistd::Pid> {
        use nix::unistd::{getpgid, Pid};

        // Validate PID can be converted to i32 safely
        if pid == 0 || pid > i32::MAX as u32 {
            return None;
        }

        let nix_pid = Pid::from_raw(pid as i32);

        // getpgid() is a direct syscall wrapper - no subprocess overhead
        match getpgid(Some(nix_pid)) {
            Ok(pgid) => {
                // Validate PGID (must be > 0)
                if pgid.as_raw() <= 0 {
                    return None;
                }
                Some(pgid)
            }
            Err(e) => {
                tracing::debug!(
                    "Failed to get PGID for PID {}: {} (process may have exited)",
                    pid,
                    e
                );
                None
            }
        }
    }

    #[cfg(not(unix))]
    fn get_process_group(_pid: u32) -> Option<nix::unistd::Pid> {
        None
    }
}

#[cfg(unix)]
/// Helper functions for setting process resource limits on Unix systems
pub mod resource_limits {
    use crate::config::ResourceLimits;
    use nix::sys::resource::{getrlimit, setrlimit, Resource};

    /// Result of validating a single resource limit against system hard limits.
    #[derive(Debug, Clone, PartialEq, Eq)]
    pub enum LimitValidationResult {
        /// Requested value is within the system hard limit.
        Valid,
        /// Requested value exceeds hard limit and was capped.
        Capped {
            /// The originally requested limit value.
            requested: u64,
            /// The system hard limit that will be used instead.
            hard_limit: u64,
        },
        /// Could not determine system hard limit (e.g., getrlimit failed).
        Unknown,
    }

    /// Results of validating all resource limits against system hard limits.
    #[derive(Debug, Clone, Default)]
    pub struct ResourceLimitValidation {
        /// Validation result for memory limit (RLIMIT_AS).
        pub memory: Option<LimitValidationResult>,
        /// Validation result for pids limit (RLIMIT_NPROC) - Linux only.
        pub pids: Option<LimitValidationResult>,
        /// Validation result for nofile limit (RLIMIT_NOFILE).
        pub nofile: Option<LimitValidationResult>,
    }

    /// Pre-parsed resource limits containing only Copy types.
    /// This struct is safe to use inside pre_exec because it contains no heap allocations.
    #[derive(Clone, Copy)]
    #[allow(dead_code)] // pids_limit is Linux-only, unused on macOS
    pub struct ParsedResourceLimits {
        /// Memory limit in bytes (RLIMIT_AS)
        pub memory_bytes: Option<u64>,
        /// Max number of processes/threads (RLIMIT_NPROC) - Linux only
        pub pids_limit: Option<u64>,
        /// Max open file descriptors (RLIMIT_NOFILE)
        pub nofile_limit: Option<u64>,
        /// Whether to strictly enforce limits (fail on error vs. warn)
        pub strict_limits: bool,
    }

    impl ParsedResourceLimits {
        /// Parse resource limits from config BEFORE fork().
        /// This performs all string parsing so the closure only needs to call setrlimit().
        pub fn from_config(resources: &ResourceLimits) -> Self {
            Self {
                memory_bytes: resources
                    .memory
                    .as_ref()
                    .and_then(|m| parse_memory_to_bytes(m)),
                pids_limit: resources.pids.map(|p| p as u64),
                nofile_limit: resources.nofile.map(|n| n as u64),
                strict_limits: resources.strict_limits,
            }
        }

        /// Validate requested limits against system hard limits.
        ///
        /// This method queries the current system hard limits using `getrlimit()` and
        /// compares them to the requested limits. If a requested limit exceeds the
        /// system hard limit, the limit is capped to the hard limit value.
        ///
        /// This validation should be called BEFORE fork() so that appropriate warnings
        /// can be logged. The `apply()` method will use the (potentially capped) values.
        ///
        /// # Returns
        ///
        /// A `ResourceLimitValidation` struct containing the validation result for each
        /// limit type. Each result indicates whether the limit was valid, capped, or
        /// could not be validated.
        pub fn validate_against_system(&mut self) -> ResourceLimitValidation {
            let mut validation = ResourceLimitValidation::default();

            // Validate memory limit (RLIMIT_AS - address space)
            if let Some(requested) = self.memory_bytes {
                validation.memory = Some(match getrlimit(Resource::RLIMIT_AS) {
                    Ok((_, hard)) => {
                        if requested > hard {
                            self.memory_bytes = Some(hard);
                            LimitValidationResult::Capped {
                                requested,
                                hard_limit: hard,
                            }
                        } else {
                            LimitValidationResult::Valid
                        }
                    }
                    Err(_) => LimitValidationResult::Unknown,
                });
            }

            // Validate nofile limit (RLIMIT_NOFILE - open file descriptors)
            if let Some(requested) = self.nofile_limit {
                validation.nofile = Some(match getrlimit(Resource::RLIMIT_NOFILE) {
                    Ok((_, hard)) => {
                        if requested > hard {
                            self.nofile_limit = Some(hard);
                            LimitValidationResult::Capped {
                                requested,
                                hard_limit: hard,
                            }
                        } else {
                            LimitValidationResult::Valid
                        }
                    }
                    Err(_) => LimitValidationResult::Unknown,
                });
            }

            // Validate pids limit (RLIMIT_NPROC - max processes) - Linux only
            #[cfg(target_os = "linux")]
            if let Some(requested) = self.pids_limit {
                validation.pids = Some(match getrlimit(Resource::RLIMIT_NPROC) {
                    Ok((_, hard)) => {
                        if requested > hard {
                            self.pids_limit = Some(hard);
                            LimitValidationResult::Capped {
                                requested,
                                hard_limit: hard,
                            }
                        } else {
                            LimitValidationResult::Valid
                        }
                    }
                    Err(_) => LimitValidationResult::Unknown,
                });
            }

            validation
        }

        /// Apply pre-parsed resource limits using setrlimit.
        ///
        /// If `strict_limits` is false (default), logs warnings and continues on error.
        /// If `strict_limits` is true, returns error immediately on any setrlimit failure.
        ///
        /// SAFETY: This function is async-signal-safe because it only calls setrlimit()
        /// with pre-computed numeric values. No string parsing, heap allocation, or I/O.
        /// The eprintln! calls are technically not async-signal-safe, but are used only
        /// in non-strict mode where startup failure is acceptable.
        pub fn apply(self) -> std::io::Result<()> {
            // Helper macro to handle setrlimit with strict_limits flag
            macro_rules! apply_limit {
                ($resource:expr, $limit:expr, $name:expr) => {
                    if let Err(e) = setrlimit($resource, $limit, $limit) {
                        if self.strict_limits {
                            return Err(std::io::Error::other(format!(
                                "Failed to set {} limit: {}",
                                $name, e
                            )));
                        } else {
                            // Note: eprintln! is not async-signal-safe but acceptable here
                            // since we're in non-strict mode (startup failure acceptable)
                            eprintln!("Warning: Failed to set {} limit: {}", $name, e);
                        }
                    }
                };
            }

            // Set memory limit (RLIMIT_AS - address space)
            if let Some(bytes) = self.memory_bytes {
                apply_limit!(Resource::RLIMIT_AS, bytes, "memory");
            }

            // Set max number of processes/threads (RLIMIT_NPROC)
            // Note: RLIMIT_NPROC is available on Linux but not all Unix systems
            #[cfg(target_os = "linux")]
            if let Some(limit) = self.pids_limit {
                apply_limit!(Resource::RLIMIT_NPROC, limit, "pids");
            }

            // Set max open file descriptors (RLIMIT_NOFILE)
            if let Some(limit) = self.nofile_limit {
                apply_limit!(Resource::RLIMIT_NOFILE, limit, "nofile");
            }

            Ok(())
        }
    }

    /// Parse memory string (e.g., "512m", "2g") to bytes
    ///
    /// Returns `None` for:
    /// - Invalid format or unrecognized suffix
    /// - Negative values
    /// - Values that would overflow u64
    pub fn parse_memory_to_bytes(memory: &str) -> Option<u64> {
        // Find where suffix starts using char_indices for UTF-8 safety
        // (chars().position() returns character index, not byte index)
        let suffix_start = memory
            .char_indices()
            .find(|(_, c)| !c.is_ascii_digit() && *c != '.')
            .map(|(byte_idx, _)| byte_idx)
            .unwrap_or(memory.len());

        let num_part = &memory[..suffix_start];
        let suffix = &memory[suffix_start..];

        // Parse numeric part
        let value: f64 = num_part.parse().ok()?;

        // Reject negative values (would produce undefined behavior when cast to u64)
        if value < 0.0 {
            return None;
        }

        // Convert based on suffix
        let bytes = match suffix.to_lowercase().as_str() {
            "" | "b" => value,
            "k" | "kb" => value * 1024.0,
            "m" | "mb" => value * 1024.0 * 1024.0,
            "g" | "gb" => value * 1024.0 * 1024.0 * 1024.0,
            "t" | "tb" => value * 1024.0 * 1024.0 * 1024.0 * 1024.0,
            _ => return None,
        };

        // Check for overflow before casting (u64::MAX is ~18.4 exabytes)
        if bytes > u64::MAX as f64 {
            return None;
        }

        Some(bytes as u64)
    }

    /// Format a byte count as a human-readable string (e.g., "512.0 MB", "2.5 GB").
    ///
    /// This function converts raw byte counts into human-friendly formats,
    /// using binary prefixes (KiB, MiB, GiB, TiB) for consistency with
    /// how memory is typically measured in computing contexts.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// assert_eq!(format_bytes(1024), "1.0 KB");
    /// assert_eq!(format_bytes(1024 * 1024), "1.0 MB");
    /// assert_eq!(format_bytes(2 * 1024 * 1024 * 1024), "2.0 GB");
    /// ```
    pub fn format_bytes(bytes: u64) -> String {
        const KB: u64 = 1024;
        const MB: u64 = KB * 1024;
        const GB: u64 = MB * 1024;
        const TB: u64 = GB * 1024;

        if bytes >= TB {
            format!("{:.1} TB", bytes as f64 / TB as f64)
        } else if bytes >= GB {
            format!("{:.1} GB", bytes as f64 / GB as f64)
        } else if bytes >= MB {
            format!("{:.1} MB", bytes as f64 / MB as f64)
        } else if bytes >= KB {
            format!("{:.1} KB", bytes as f64 / KB as f64)
        } else {
            format!("{} bytes", bytes)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ========================================================================
    // Process Group (PGID) Lookup Tests
    // ========================================================================

    #[test]
    #[cfg(unix)]
    fn test_get_process_group_current_process() {
        // We can reliably get the PGID for our own process
        use nix::unistd::getpid;
        let current_pid = getpid().as_raw() as u32;
        let pgid = ProcessService::get_process_group(current_pid);
        assert!(pgid.is_some(), "Should get PGID for current process");
        // PGID should be positive
        assert!(pgid.unwrap().as_raw() > 0, "PGID should be positive");
    }

    #[test]
    #[cfg(unix)]
    fn test_get_process_group_pid_zero() {
        // PID 0 should return None (invalid)
        let pgid = ProcessService::get_process_group(0);
        assert!(pgid.is_none(), "PID 0 should return None");
    }

    #[test]
    #[cfg(unix)]
    fn test_get_process_group_pid_max() {
        // PID > i32::MAX should return None (invalid)
        let pgid = ProcessService::get_process_group(i32::MAX as u32 + 1);
        assert!(pgid.is_none(), "PID > i32::MAX should return None");
    }

    #[test]
    #[cfg(unix)]
    fn test_get_process_group_nonexistent_pid() {
        // Spawn a short-lived process and wait for it to finish, giving us a
        // PID that is guaranteed to be dead without relying on magic constants.
        let mut child = std::process::Command::new("true").spawn().unwrap();
        let dead_pid = child.id();
        child.wait().unwrap();

        let pgid = ProcessService::get_process_group(dead_pid);
        // This should return None gracefully (ESRCH error)
        assert!(
            pgid.is_none(),
            "Non-existent PID should return None gracefully"
        );
    }

    // ========================================================================
    // Memory Parsing Tests
    // ========================================================================

    #[test]
    #[cfg(unix)]
    fn test_parse_memory_bytes() {
        assert_eq!(resource_limits::parse_memory_to_bytes("1024"), Some(1024));
        assert_eq!(resource_limits::parse_memory_to_bytes("1024b"), Some(1024));
    }

    #[test]
    #[cfg(unix)]
    fn test_parse_memory_kilobytes() {
        assert_eq!(resource_limits::parse_memory_to_bytes("1k"), Some(1024));
        assert_eq!(resource_limits::parse_memory_to_bytes("1kb"), Some(1024));
    }

    #[test]
    #[cfg(unix)]
    fn test_parse_memory_megabytes() {
        assert_eq!(
            resource_limits::parse_memory_to_bytes("1m"),
            Some(1024 * 1024)
        );
        assert_eq!(
            resource_limits::parse_memory_to_bytes("512mb"),
            Some(512 * 1024 * 1024)
        );
    }

    #[test]
    #[cfg(unix)]
    fn test_parse_memory_gigabytes() {
        assert_eq!(
            resource_limits::parse_memory_to_bytes("1g"),
            Some(1024 * 1024 * 1024)
        );
        assert_eq!(
            resource_limits::parse_memory_to_bytes("2gb"),
            Some(2 * 1024 * 1024 * 1024)
        );
    }

    #[test]
    #[cfg(unix)]
    fn test_parse_memory_rejects_negative_values() {
        // SF-00017: Negative values should return None, not wrap around
        assert_eq!(resource_limits::parse_memory_to_bytes("-512m"), None);
        assert_eq!(resource_limits::parse_memory_to_bytes("-1g"), None);
        assert_eq!(resource_limits::parse_memory_to_bytes("-100"), None);
    }

    #[test]
    #[cfg(unix)]
    fn test_parse_memory_rejects_overflow_values() {
        // SF-00015: Values that would overflow u64 should return None
        // u64::MAX is ~18.4 exabytes (~16.7 million TB)
        // Values >= 17,000,000 TB will overflow
        assert_eq!(resource_limits::parse_memory_to_bytes("20000000tb"), None);
        assert_eq!(
            resource_limits::parse_memory_to_bytes("999999999999tb"),
            None
        );

        // Large but valid values should still work
        // 1000000tb = ~1.1 exabytes, well under u64::MAX
        assert!(resource_limits::parse_memory_to_bytes("1000000tb").is_some());
    }

    #[test]
    #[cfg(unix)]
    fn test_parse_memory_handles_invalid_suffix() {
        assert_eq!(resource_limits::parse_memory_to_bytes("512xyz"), None);
        assert_eq!(resource_limits::parse_memory_to_bytes("100pb"), None); // petabytes not supported
    }

    // ========================================================================
    // Resource Limit Validation Tests
    // ========================================================================

    #[test]
    #[cfg(unix)]
    fn test_resource_limit_validation_caps_excessive_memory() {
        use resource_limits::{LimitValidationResult, ParsedResourceLimits};

        // Request an impossibly high memory limit that will definitely exceed the hard limit
        let mut limits = ParsedResourceLimits {
            memory_bytes: Some(u64::MAX),
            pids_limit: None,
            nofile_limit: None,
            strict_limits: false,
        };

        let validation = limits.validate_against_system();

        // Memory should be capped (unless the system has unlimited memory, which is very unlikely)
        match validation.memory {
            Some(LimitValidationResult::Capped {
                requested,
                hard_limit,
            }) => {
                assert_eq!(requested, u64::MAX);
                // After validation, the limit should be capped to the hard limit
                assert_eq!(limits.memory_bytes, Some(hard_limit));
                assert!(hard_limit < u64::MAX);
            }
            Some(LimitValidationResult::Valid) => {
                // This would mean the system has unlimited memory - very unlikely but possible
                assert_eq!(limits.memory_bytes, Some(u64::MAX));
            }
            Some(LimitValidationResult::Unknown) => {
                // getrlimit failed - acceptable on some systems
            }
            None => panic!("Expected Some validation result for memory"),
        }
    }

    #[test]
    #[cfg(unix)]
    fn test_resource_limit_validation_accepts_reasonable_limits() {
        use resource_limits::{LimitValidationResult, ParsedResourceLimits};

        // Request very small, reasonable limits that should always be valid
        let mut limits = ParsedResourceLimits {
            memory_bytes: Some(1024), // 1 KB - trivially small
            pids_limit: None,
            nofile_limit: Some(10), // 10 file descriptors - very small
            strict_limits: false,
        };

        let validation = limits.validate_against_system();

        // These small limits should be valid (not capped)
        match validation.memory {
            Some(LimitValidationResult::Valid) => {
                assert_eq!(limits.memory_bytes, Some(1024));
            }
            Some(LimitValidationResult::Capped { .. }) => {
                panic!("1 KB memory limit should not be capped");
            }
            Some(LimitValidationResult::Unknown) | None => {
                // Acceptable - getrlimit may not work on all systems
            }
        }

        match validation.nofile {
            Some(LimitValidationResult::Valid) => {
                assert_eq!(limits.nofile_limit, Some(10));
            }
            Some(LimitValidationResult::Capped { .. }) => {
                panic!("10 file descriptor limit should not be capped");
            }
            Some(LimitValidationResult::Unknown) | None => {
                // Acceptable - getrlimit may not work on all systems
            }
        }
    }

    #[test]
    #[cfg(unix)]
    fn test_resource_limit_validation_caps_excessive_nofile() {
        use resource_limits::{LimitValidationResult, ParsedResourceLimits};

        // Request an impossibly high nofile limit
        let mut limits = ParsedResourceLimits {
            memory_bytes: None,
            pids_limit: None,
            nofile_limit: Some(u64::MAX),
            strict_limits: false,
        };

        let validation = limits.validate_against_system();

        match validation.nofile {
            Some(LimitValidationResult::Capped {
                requested,
                hard_limit,
            }) => {
                assert_eq!(requested, u64::MAX);
                assert_eq!(limits.nofile_limit, Some(hard_limit));
                assert!(hard_limit < u64::MAX);
            }
            Some(LimitValidationResult::Valid) => {
                // Very unlikely - would mean unlimited file descriptors
            }
            Some(LimitValidationResult::Unknown) | None => {
                // Acceptable on some systems
            }
        }
    }

    #[test]
    #[cfg(unix)]
    fn test_resource_limit_validation_no_limits_set() {
        use resource_limits::ParsedResourceLimits;

        // No limits set - validation should return None for all fields
        let mut limits = ParsedResourceLimits {
            memory_bytes: None,
            pids_limit: None,
            nofile_limit: None,
            strict_limits: false,
        };

        let validation = limits.validate_against_system();

        assert!(validation.memory.is_none());
        assert!(validation.nofile.is_none());
        // pids is only validated on Linux, so we don't check it here
    }

    // ========================================================================
    // format_bytes Tests
    // ========================================================================

    #[test]
    #[cfg(unix)]
    fn test_format_bytes_bytes() {
        assert_eq!(resource_limits::format_bytes(0), "0 bytes");
        assert_eq!(resource_limits::format_bytes(100), "100 bytes");
        assert_eq!(resource_limits::format_bytes(1023), "1023 bytes");
    }

    #[test]
    #[cfg(unix)]
    fn test_format_bytes_kilobytes() {
        assert_eq!(resource_limits::format_bytes(1024), "1.0 KB");
        assert_eq!(resource_limits::format_bytes(2048), "2.0 KB");
        assert_eq!(resource_limits::format_bytes(1536), "1.5 KB"); // 1.5 KB
    }

    #[test]
    #[cfg(unix)]
    fn test_format_bytes_megabytes() {
        assert_eq!(resource_limits::format_bytes(1024 * 1024), "1.0 MB");
        assert_eq!(resource_limits::format_bytes(512 * 1024 * 1024), "512.0 MB");
    }

    #[test]
    #[cfg(unix)]
    fn test_format_bytes_gigabytes() {
        assert_eq!(resource_limits::format_bytes(1024 * 1024 * 1024), "1.0 GB");
        assert_eq!(
            resource_limits::format_bytes(2 * 1024 * 1024 * 1024),
            "2.0 GB"
        );
    }

    #[test]
    #[cfg(unix)]
    fn test_format_bytes_terabytes() {
        assert_eq!(
            resource_limits::format_bytes(1024 * 1024 * 1024 * 1024),
            "1.0 TB"
        );
    }
}
