use std::fmt;
use tracing::{Event, Subscriber};
use tracing_subscriber::fmt::format::Writer;
use tracing_subscriber::fmt::{FmtContext, FormatEvent, FormatFields};
use tracing_subscriber::registry::LookupSpan;

/// Abstraction over user-facing output.
///
/// Command modules use this trait instead of `println!`/`eprintln!` so that
/// output can be suppressed in TUI mode (which renders its own UI) or
/// redirected to JSON in a future machine-readable mode.
pub trait UserOutput: Send + Sync {
    /// Informational status message (e.g., "Stopping all services...")
    fn status(&self, message: &str);

    /// Success message (e.g., "All services started successfully!")
    fn success(&self, message: &str);

    /// Warning message (e.g., "Failed to clean orphaned containers")
    fn warning(&self, message: &str);

    /// Error message (e.g., "Error starting dependency 'foo'")
    fn error(&self, message: &str);

    /// Inline progress (no trailing newline). Call `finish_progress` after.
    fn progress(&self, message: &str);

    /// Finish an inline progress line with a result (appended to the
    /// progress message).
    fn finish_progress(&self, result: &str);

    /// Finish an inline progress line by replacing it with a complete line.
    fn finish_progress_with(&self, line: &str) {
        self.finish_progress(line);
    }

    /// Clear an inline progress line without printing anything in its place.
    fn clear_progress(&self) {}

    /// A blank line separator.
    fn blank(&self);
}

/// Standard CLI output — writes to stdout/stderr with optional ANSI colors.
///
/// Progress lines and blocking output are routed through [`fed::progress`]
/// so in-place progress, tracing events, and hook output never interleave
/// mid-line.
pub struct CliOutput {
    is_tty: bool,
    /// The message passed to the last `progress()` call, so `finish_progress`
    /// can rebuild the complete line (progress + result) for the final print.
    pending_progress: std::sync::Mutex<String>,
}

impl CliOutput {
    pub fn new(is_tty: bool) -> Self {
        Self {
            is_tty,
            pending_progress: std::sync::Mutex::new(String::new()),
        }
    }
}

impl UserOutput for CliOutput {
    fn status(&self, message: &str) {
        fed::progress::println_above(message);
    }

    fn success(&self, message: &str) {
        if self.is_tty {
            fed::progress::println_above(&format!("\x1b[32m{}\x1b[0m", message));
        } else {
            fed::progress::println_above(message);
        }
    }

    fn warning(&self, message: &str) {
        if self.is_tty {
            fed::progress::eprintln_above(&format!("\x1b[33m{}\x1b[0m", message));
        } else {
            fed::progress::eprintln_above(message);
        }
    }

    fn error(&self, message: &str) {
        if self.is_tty {
            fed::progress::eprintln_above(&format!("\x1b[31m{}\x1b[0m", message));
        } else {
            fed::progress::eprintln_above(message);
        }
    }

    fn progress(&self, message: &str) {
        *self.pending_progress.lock().unwrap() = message.to_string();
        fed::progress::set_line(message);
    }

    fn finish_progress(&self, result: &str) {
        let base = std::mem::take(&mut *self.pending_progress.lock().unwrap());
        fed::progress::finish_line(&format!("{}{}", base, result));
    }

    fn finish_progress_with(&self, line: &str) {
        self.pending_progress.lock().unwrap().clear();
        fed::progress::finish_line(line);
    }

    fn clear_progress(&self) {
        self.pending_progress.lock().unwrap().clear();
        fed::progress::clear_line();
    }

    fn blank(&self) {
        fed::progress::println_above("");
    }
}

// ── Custom tracing formatter for CLI output ─────────────────────────

/// Extracts the `message` field from a tracing event.
struct MessageVisitor {
    message: String,
}

impl MessageVisitor {
    fn new() -> Self {
        Self {
            message: String::new(),
        }
    }
}

impl tracing::field::Visit for MessageVisitor {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn fmt::Debug) {
        if field.name() == "message" {
            self.message = format!("{:?}", value);
        }
    }

    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        if field.name() == "message" {
            self.message = value.to_string();
        }
    }
}

/// Pretty tracing formatter for CLI output.
///
/// **TTY mode** — no timestamps, no module paths, no spans:
/// ```text
/// ⚠ Another fed instance (PID 66233) is modifying this workspace.
///   Waiting for healthcheck on 'redis' (timeout: 5s)
/// ✓ Service 'redis' is healthy
/// ✗ Failed to stop dependent service 'api': timeout
/// ```
///
/// **Non-TTY mode** — short timestamp + level, no ANSI:
/// ```text
/// [19:55:40 WARN] Another fed instance is modifying this workspace.
/// [19:55:50 INFO] Waiting for healthcheck on 'redis' (timeout: 5s)
/// ```
pub struct CliFormatter {
    is_tty: bool,
}

impl CliFormatter {
    pub fn new(is_tty: bool) -> Self {
        Self { is_tty }
    }
}

impl<S, N> FormatEvent<S, N> for CliFormatter
where
    S: Subscriber + for<'a> LookupSpan<'a>,
    N: for<'a> FormatFields<'a> + 'static,
{
    fn format_event(
        &self,
        _ctx: &FmtContext<'_, S, N>,
        mut writer: Writer<'_>,
        event: &Event<'_>,
    ) -> fmt::Result {
        let metadata = event.metadata();
        let level = *metadata.level();

        let mut visitor = MessageVisitor::new();
        event.record(&mut visitor);
        let msg = &visitor.message;

        if self.is_tty {
            // Route through the shared progress coordinator: if an in-place
            // progress line is pending (fed start), the event prints above it
            // instead of splicing into it.
            let styled = match level {
                tracing::Level::ERROR => format!("\x1b[31m✗\x1b[0m {}", msg),
                tracing::Level::WARN => format!("\x1b[33m⚠\x1b[0m {}", msg),
                tracing::Level::INFO => {
                    if msg.ends_with("is healthy") {
                        format!("\x1b[32m✓\x1b[0m {}", msg)
                    } else {
                        format!("  {}", msg)
                    }
                }
                tracing::Level::DEBUG | tracing::Level::TRACE => {
                    format!("\x1b[2m  {}\x1b[0m", msg)
                }
            };
            fed::progress::eprintln_above(&styled);
            return Ok(());
        }
        {
            let now = chrono::Local::now().format("%H:%M:%S");
            let level_str = match level {
                tracing::Level::ERROR => "ERROR",
                tracing::Level::WARN => "WARN",
                tracing::Level::INFO => "INFO",
                tracing::Level::DEBUG => "DEBUG",
                tracing::Level::TRACE => "TRACE",
            };
            write!(writer, "[{} {}] {}", now, level_str, msg)?;
        }

        writeln!(writer)
    }
}
