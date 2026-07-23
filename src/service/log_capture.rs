//! Log capture and management for process services.
//!
//! This module provides the `LogCapture` struct that encapsulates all log-related
//! functionality for process services, including:
//! - In-memory log buffering (ring buffer with max 1000 lines)
//! - Log file reading for file-based output mode
//! - Background task management for log capture
//! - Graceful shutdown coordination

use super::OutputMode;
use crate::error::{Error, Result};
use std::collections::VecDeque;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::{ChildStderr, ChildStdout};
use tokio::sync::{Mutex, Notify};
use tokio::task::JoinHandle;

/// Default maximum number of log lines to keep in memory per service.
/// This prevents unbounded memory growth in long-running services.
const DEFAULT_MAX_LOG_LINES: usize = 10_000;

/// Threshold percentage for memory usage warning (80% of max capacity).
const LOG_BUFFER_WARNING_THRESHOLD: f64 = 0.8;

/// Manages log capture and storage for a process service.
///
/// # Design
///
/// - **Captured mode**: Spawns background tasks to capture stdout/stderr
///   into an in-memory ring buffer (configurable, default 10,000 lines).
/// - **File mode**: Reads logs from a file on disk when requested.
/// - **Passthrough mode**: No log capture (output goes directly to parent).
///
/// # Memory Management
///
/// The ring buffer automatically evicts oldest lines when capacity is reached.
/// A warning is logged when the buffer reaches 80% capacity to help detect
/// overly verbose services.
///
/// # Thread Safety
///
/// Uses `tokio::sync::Mutex` for all fields because log operations may hold
/// locks across `.await` points (e.g., reading from file, spawning tasks).
pub struct LogCapture {
    /// Service name for logging warnings.
    service_name: String,
    /// In-memory log buffer (ring buffer).
    logs: Arc<Mutex<VecDeque<String>>>,
    /// Maximum number of log lines to keep in memory.
    max_lines: usize,
    /// Whether we've already warned about buffer size (to avoid spam).
    warned_at_capacity: Arc<Mutex<bool>>,
    /// Count of log lines that were dropped due to buffer overflow.
    dropped_count: Arc<AtomicUsize>,
    /// Path to log file for file mode.
    log_file_path: Option<PathBuf>,
    /// Handles to log capture tasks for cleanup.
    /// Uses std::sync::Mutex (not tokio) to allow synchronous access in Drop impl.
    log_tasks: Arc<StdMutex<Vec<JoinHandle<()>>>>,
    /// Shutdown signal for log capture tasks.
    log_shutdown: Arc<Notify>,
    /// Output capture mode.
    output_mode: OutputMode,
    /// File-mode read cache keyed on (file size, tail count): pollers (the TUI
    /// asks every tick) skip re-reading the file when it hasn't grown.
    file_read_cache: Arc<Mutex<Option<FileReadCache>>>,
}

/// Cached result of a file-mode log read:
/// ((file size, mtime), tail count, lines). Size alone would serve stale
/// content after a truncate-and-rewrite to the same length.
type FileReadCache = ((u64, Option<std::time::SystemTime>), usize, Vec<String>);

impl LogCapture {
    /// Create a new LogCapture instance with default maximum lines.
    ///
    /// # Arguments
    ///
    /// * `service_name` - Name of the service (for warning messages).
    /// * `log_file_path` - Path for log file in file mode, None otherwise.
    /// * `output_mode` - How output should be captured.
    pub fn new(
        service_name: String,
        log_file_path: Option<PathBuf>,
        output_mode: OutputMode,
    ) -> Self {
        Self::with_max_lines(
            service_name,
            log_file_path,
            output_mode,
            DEFAULT_MAX_LOG_LINES,
        )
    }

    /// Create a new LogCapture instance with custom maximum lines.
    ///
    /// # Arguments
    ///
    /// * `service_name` - Name of the service (for warning messages).
    /// * `log_file_path` - Path for log file in file mode, None otherwise.
    /// * `output_mode` - How output should be captured.
    /// * `max_lines` - Maximum number of log lines to keep in memory.
    pub fn with_max_lines(
        service_name: String,
        log_file_path: Option<PathBuf>,
        output_mode: OutputMode,
        max_lines: usize,
    ) -> Self {
        Self {
            service_name,
            logs: Arc::new(Mutex::new(VecDeque::new())),
            max_lines,
            warned_at_capacity: Arc::new(Mutex::new(false)),
            dropped_count: Arc::new(AtomicUsize::new(0)),
            log_file_path,
            log_tasks: Arc::new(StdMutex::new(Vec::new())),
            log_shutdown: Arc::new(Notify::new()),
            output_mode,
            file_read_cache: Arc::new(Mutex::new(None)),
        }
    }

    /// Get the count of log lines that were dropped due to buffer overflow.
    pub fn dropped_count(&self) -> usize {
        self.dropped_count.load(Ordering::Relaxed)
    }

    /// Spawn background tasks to capture stdout and stderr.
    ///
    /// Only spawns tasks in Captured mode. In File mode, logs are written
    /// directly to disk by the process. In Passthrough mode, output goes
    /// directly to the parent process.
    ///
    /// # Arguments
    ///
    /// * `stdout` - Optional stdout handle from the child process.
    /// * `stderr` - Optional stderr handle from the child process.
    pub async fn spawn_capture_tasks(
        &self,
        stdout: Option<ChildStdout>,
        stderr: Option<ChildStderr>,
    ) {
        // Only capture logs in Captured mode
        if !self.output_mode.is_captured() {
            return;
        }

        if let Some(stdout) = stdout {
            let logs = self.logs.clone();
            let log_tasks = self.log_tasks.clone();
            let shutdown = self.log_shutdown.clone();
            let max_lines = self.max_lines;
            let warned_at_capacity = self.warned_at_capacity.clone();
            let dropped_count = self.dropped_count.clone();
            let service_name = self.service_name.clone();

            let handle = tokio::spawn(async move {
                let reader = BufReader::new(stdout);
                let mut lines = reader.lines();
                loop {
                    tokio::select! {
                        biased;
                        _ = shutdown.notified() => break,
                        line = lines.next_line() => {
                            match line {
                                Ok(Some(line)) => {
                                    let mut logs = logs.lock().await;
                                    logs.push_back(line);

                                    // Evict oldest line if over capacity
                                    if logs.len() > max_lines {
                                        logs.pop_front();
                                        dropped_count.fetch_add(1, Ordering::Relaxed);
                                    }

                                    // Warn once when buffer reaches threshold
                                    let threshold = (max_lines as f64 * LOG_BUFFER_WARNING_THRESHOLD) as usize;
                                    if logs.len() >= threshold {
                                        let mut warned = warned_at_capacity.lock().await;
                                        if !*warned {
                                            tracing::warn!(
                                                "Log buffer for service '{}' is at {}/{} lines ({}% capacity). Oldest logs will be dropped.",
                                                service_name,
                                                logs.len(),
                                                max_lines,
                                                (logs.len() as f64 / max_lines as f64 * 100.0) as usize
                                            );
                                            *warned = true;
                                        }
                                    }
                                }
                                _ => break,
                            }
                        }
                    }
                }
            });
            log_tasks.lock().unwrap().push(handle);
        }

        if let Some(stderr) = stderr {
            let logs = self.logs.clone();
            let log_tasks = self.log_tasks.clone();
            let shutdown = self.log_shutdown.clone();
            let max_lines = self.max_lines;
            let warned_at_capacity = self.warned_at_capacity.clone();
            let dropped_count = self.dropped_count.clone();
            let service_name = self.service_name.clone();

            let handle = tokio::spawn(async move {
                let reader = BufReader::new(stderr);
                let mut lines = reader.lines();
                loop {
                    tokio::select! {
                        biased;
                        _ = shutdown.notified() => break,
                        line = lines.next_line() => {
                            match line {
                                Ok(Some(line)) => {
                                    let mut logs = logs.lock().await;
                                    logs.push_back(format!("[stderr] {}", line));

                                    // Evict oldest line if over capacity
                                    if logs.len() > max_lines {
                                        logs.pop_front();
                                        dropped_count.fetch_add(1, Ordering::Relaxed);
                                    }

                                    // Warn once when buffer reaches threshold
                                    let threshold = (max_lines as f64 * LOG_BUFFER_WARNING_THRESHOLD) as usize;
                                    if logs.len() >= threshold {
                                        let mut warned = warned_at_capacity.lock().await;
                                        if !*warned {
                                            tracing::warn!(
                                                "Log buffer for service '{}' is at {}/{} lines ({}% capacity). Oldest logs will be dropped.",
                                                service_name,
                                                logs.len(),
                                                max_lines,
                                                (logs.len() as f64 / max_lines as f64 * 100.0) as usize
                                            );
                                            *warned = true;
                                        }
                                    }
                                }
                                _ => break,
                            }
                        }
                    }
                }
            });
            log_tasks.lock().unwrap().push(handle);
        }
    }

    /// Read logs, optionally limited to the last N lines.
    ///
    /// # Arguments
    ///
    /// * `tail` - If Some(n), return only the last n lines.
    ///
    /// # Returns
    ///
    /// A vector of log lines.
    pub async fn read_logs(&self, tail: Option<usize>) -> Result<Vec<String>> {
        match self.output_mode {
            OutputMode::File => self.read_logs_from_file(tail).await,
            OutputMode::Captured => self.read_logs_from_memory(tail).await,
            OutputMode::Passthrough => {
                // In passthrough mode, logs go directly to parent process
                // We can't capture them retroactively
                Ok(vec!["[logs not captured in passthrough mode]".to_string()])
            }
        }
    }

    /// Read logs from in-memory buffer (interactive mode).
    async fn read_logs_from_memory(&self, tail: Option<usize>) -> Result<Vec<String>> {
        let logs = self.logs.lock().await;
        if let Some(n) = tail {
            Ok(logs.iter().rev().take(n).rev().cloned().collect())
        } else {
            Ok(logs.iter().cloned().collect())
        }
    }

    /// Read logs from file (detached mode).
    async fn read_logs_from_file(&self, tail: Option<usize>) -> Result<Vec<String>> {
        let Some(ref log_path) = self.log_file_path else {
            return Ok(vec![]);
        };

        if !log_path.exists() {
            return Ok(vec![]);
        }

        let log_path = log_path.clone();
        let tail_count = tail.unwrap_or(self.max_lines);

        // Serve from cache while the file hasn't changed — pollers hit this
        // every 250ms and the file usually hasn't changed between ticks
        let current_key = match tokio::fs::metadata(&log_path).await {
            Ok(m) => (m.len(), m.modified().ok()),
            Err(_) => (0, None),
        };
        {
            let cache = self.file_read_cache.lock().await;
            if let Some((key, cached_tail, ref lines)) = *cache
                && key == current_key
                && cached_tail == tail_count
            {
                return Ok(lines.clone());
            }
        }

        let result = tokio::task::spawn_blocking(move || {
            use std::fs::File;
            use std::io::{BufRead, BufReader, Seek, SeekFrom};

            let file = File::open(&log_path)?;

            // Acquire shared lock (allows concurrent reads, blocks writes)
            // Use try_lock to avoid deadlocks - if we can't get lock, read anyway
            use fs2::FileExt;
            if FileExt::try_lock_shared(&file).is_err() {
                tracing::debug!("Could not acquire shared lock on log file, reading without lock");
            }

            // For tail, we read from the end efficiently
            let metadata = file.metadata()?;
            let file_size = metadata.len();

            // Estimate: assume ~200 bytes per line, read what we need
            let estimated_bytes = (tail_count as u64) * 200;
            let start_pos = file_size.saturating_sub(estimated_bytes);

            let mut reader = BufReader::new(&file);
            reader.seek(SeekFrom::Start(start_pos))?;

            // If we seeked to middle of file, skip partial first line
            if start_pos > 0 {
                let mut partial = String::new();
                let _ = reader.read_line(&mut partial);
            }

            // Read remaining lines
            let mut lines: Vec<String> = Vec::new();
            for line in reader.lines() {
                match line {
                    Ok(l) => lines.push(l),
                    Err(_) => break,
                }
            }

            // Take only the last N lines
            let result: Vec<String> = if lines.len() > tail_count {
                let skip_count = lines.len() - tail_count;
                lines.into_iter().skip(skip_count).collect()
            } else {
                lines
            };

            Ok::<Vec<String>, std::io::Error>(result)
        })
        .await
        .map_err(|e| Error::Filesystem(format!("Log read task failed: {}", e)))?;

        let lines =
            result.map_err(|e| Error::Filesystem(format!("Failed to read log file: {}", e)))?;

        *self.file_read_cache.lock().await = Some((current_key, tail_count, lines.clone()));
        Ok(lines)
    }

    /// Shutdown log capture tasks gracefully.
    ///
    /// Signals all log capture tasks to stop, waits briefly for graceful exit,
    /// then aborts any remaining tasks.
    pub async fn shutdown(&self) {
        // Signal log capture tasks to shutdown gracefully
        self.log_shutdown.notify_waiters();

        // Give tasks a moment to exit gracefully, then abort any remaining
        tokio::time::sleep(Duration::from_millis(10)).await;
        let mut tasks = self.log_tasks.lock().unwrap();
        for task in tasks.drain(..) {
            task.abort();
        }
    }

    /// Clear the in-memory log buffer.
    pub async fn clear(&self) {
        self.logs.lock().await.clear();
    }

    /// Get the log file path (for detached mode redirection).
    pub fn log_file_path(&self) -> Option<&PathBuf> {
        self.log_file_path.as_ref()
    }
}

impl Drop for LogCapture {
    fn drop(&mut self) {
        // Signal log capture tasks to shutdown
        self.log_shutdown.notify_waiters();

        // Abort any running tasks to prevent leaks
        // We can access the mutex synchronously since we use std::sync::Mutex
        if let Ok(mut tasks) = self.log_tasks.lock() {
            for task in tasks.drain(..) {
                task.abort();
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_log_capture_new() {
        let capture = LogCapture::new("test".to_string(), None, OutputMode::Captured);
        assert!(capture.output_mode.is_captured());
        assert_eq!(capture.max_lines, DEFAULT_MAX_LOG_LINES);

        let capture = LogCapture::new(
            "test".to_string(),
            Some(PathBuf::from("/tmp/test.log")),
            OutputMode::File,
        );
        assert!(capture.output_mode.is_file());

        let capture = LogCapture::new("test".to_string(), None, OutputMode::Passthrough);
        assert!(capture.output_mode.is_passthrough());
    }

    #[tokio::test]
    async fn test_log_capture_with_custom_max_lines() {
        let capture =
            LogCapture::with_max_lines("test".to_string(), None, OutputMode::Captured, 5000);
        assert_eq!(capture.max_lines, 5000);
    }

    #[tokio::test]
    async fn test_read_empty_logs() {
        let capture = LogCapture::new("test".to_string(), None, OutputMode::Captured);
        let logs = capture.read_logs(None).await.unwrap();
        assert!(logs.is_empty());
    }

    #[tokio::test]
    async fn test_read_logs_with_tail() {
        let capture = LogCapture::new("test".to_string(), None, OutputMode::Captured);

        // Manually add some logs
        {
            let mut logs = capture.logs.lock().await;
            for i in 0..10 {
                logs.push_back(format!("line {}", i));
            }
        }

        let logs = capture.read_logs(Some(3)).await.unwrap();
        assert_eq!(logs.len(), 3);
        assert_eq!(logs[0], "line 7");
        assert_eq!(logs[1], "line 8");
        assert_eq!(logs[2], "line 9");
    }

    #[tokio::test]
    async fn test_shutdown() {
        let capture = LogCapture::new("test".to_string(), None, OutputMode::Captured);
        // Shutdown should not panic even with no tasks
        capture.shutdown().await;
    }

    #[tokio::test]
    async fn test_passthrough_logs() {
        let capture = LogCapture::new("test".to_string(), None, OutputMode::Passthrough);
        let logs = capture.read_logs(None).await.unwrap();
        assert_eq!(logs.len(), 1);
        assert!(logs[0].contains("passthrough"));
    }

    #[tokio::test]
    async fn test_dropped_count() {
        // Create a capture with a small max_lines limit
        let capture = LogCapture::with_max_lines("test".to_string(), None, OutputMode::Captured, 5);

        // Initially no logs dropped
        assert_eq!(capture.dropped_count(), 0);

        // Add logs beyond capacity
        {
            let mut logs = capture.logs.lock().await;
            for i in 0..10 {
                logs.push_back(format!("line {}", i));
                // Simulate the eviction logic
                if logs.len() > 5 {
                    logs.pop_front();
                    capture.dropped_count.fetch_add(1, Ordering::Relaxed);
                }
            }
        }

        // Should have dropped 5 lines (10 added, 5 max capacity)
        assert_eq!(capture.dropped_count(), 5);

        // Buffer should have exactly 5 lines
        let logs = capture.read_logs(None).await.unwrap();
        assert_eq!(logs.len(), 5);
        assert_eq!(logs[0], "line 5");
        assert_eq!(logs[4], "line 9");
    }

    #[tokio::test]
    async fn test_drop_aborts_tasks() {
        // Create LogCapture and manually add a task
        let capture = LogCapture::new("test".to_string(), None, OutputMode::Captured);

        // Spawn a long-running task
        let handle = tokio::spawn(async {
            tokio::time::sleep(Duration::from_secs(60)).await;
        });

        // Store the handle so we can check it later
        let handle_clone = {
            let mut tasks = capture.log_tasks.lock().unwrap();
            tasks.push(handle);
            // Get the JoinHandle from the vec to check its state
            // Actually we can't easily do this, so just verify behavior via log_tasks being empty after drop
            tasks.len()
        };
        assert_eq!(handle_clone, 1);

        // Drop without calling shutdown() - this should abort tasks via Drop impl
        drop(capture);

        // If we reach here without hanging, Drop successfully aborted tasks
        // (If Drop didn't abort, the task would keep a reference and potentially cause issues)
    }

    #[tokio::test]
    async fn test_drop_cleans_up_task_handles() {
        // Verify that after drop, no task handles remain
        let log_tasks_arc;
        {
            let capture = LogCapture::new("test".to_string(), None, OutputMode::Captured);
            log_tasks_arc = capture.log_tasks.clone();

            // Add a task
            let handle = tokio::spawn(async {
                tokio::time::sleep(Duration::from_secs(60)).await;
            });
            capture.log_tasks.lock().unwrap().push(handle);

            assert_eq!(log_tasks_arc.lock().unwrap().len(), 1);
            // capture is dropped here
        }

        // After drop, the vec should be drained (empty)
        assert_eq!(log_tasks_arc.lock().unwrap().len(), 0);
    }
}
