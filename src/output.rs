use std::fmt;
use std::io::Write;
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

    /// Finish an inline progress line with a result.
    fn finish_progress(&self, result: &str);

    /// A blank line separator.
    fn blank(&self);
}

/// Standard CLI output — writes to stdout/stderr with optional ANSI colors.
pub struct CliOutput {
    is_tty: bool,
}

impl CliOutput {
    pub fn new(is_tty: bool) -> Self {
        Self { is_tty }
    }
}

impl UserOutput for CliOutput {
    fn status(&self, message: &str) {
        println!("{}", message);
    }

    fn success(&self, message: &str) {
        if self.is_tty {
            println!("\x1b[32m{}\x1b[0m", message);
        } else {
            println!("{}", message);
        }
    }

    fn warning(&self, message: &str) {
        if self.is_tty {
            eprintln!("\x1b[33m{}\x1b[0m", message);
        } else {
            eprintln!("{}", message);
        }
    }

    fn error(&self, message: &str) {
        if self.is_tty {
            eprintln!("\x1b[31m{}\x1b[0m", message);
        } else {
            eprintln!("{}", message);
        }
    }

    fn progress(&self, message: &str) {
        print!("{}", message);
        std::io::stdout().flush().ok();
    }

    fn finish_progress(&self, result: &str) {
        println!("{}", result);
    }

    fn blank(&self) {
        println!();
    }
}

/// Test double that records everything a flow would show the user, so tests
/// can assert both on what IS printed and — for secret material like tokens,
/// exchange codes, and callback URLs — on what is NOT.
#[cfg(test)]
pub struct RecordingOutput {
    lines: std::sync::Mutex<Vec<String>>,
}

#[cfg(test)]
impl RecordingOutput {
    pub fn new() -> Self {
        Self {
            lines: std::sync::Mutex::new(Vec::new()),
        }
    }

    /// Everything recorded, joined with newlines — one haystack for
    /// "contains"/"does not contain" assertions across all output kinds.
    pub fn combined(&self) -> String {
        self.lines.lock().unwrap().join("\n")
    }
}

#[cfg(test)]
impl UserOutput for RecordingOutput {
    fn status(&self, message: &str) {
        self.lines.lock().unwrap().push(message.to_string());
    }

    fn success(&self, message: &str) {
        self.lines.lock().unwrap().push(message.to_string());
    }

    fn warning(&self, message: &str) {
        self.lines.lock().unwrap().push(message.to_string());
    }

    fn error(&self, message: &str) {
        self.lines.lock().unwrap().push(message.to_string());
    }

    fn progress(&self, message: &str) {
        self.lines.lock().unwrap().push(message.to_string());
    }

    fn finish_progress(&self, result: &str) {
        self.lines.lock().unwrap().push(result.to_string());
    }

    fn blank(&self) {
        self.lines.lock().unwrap().push(String::new());
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
            match level {
                tracing::Level::ERROR => {
                    write!(writer, "\x1b[31m✗\x1b[0m {}", msg)?;
                }
                tracing::Level::WARN => {
                    write!(writer, "\x1b[33m⚠\x1b[0m {}", msg)?;
                }
                tracing::Level::INFO => {
                    if msg.ends_with("is healthy") {
                        write!(writer, "\x1b[32m✓\x1b[0m {}", msg)?;
                    } else {
                        write!(writer, "  {}", msg)?;
                    }
                }
                tracing::Level::DEBUG => {
                    write!(writer, "\x1b[2m  {}\x1b[0m", msg)?;
                }
                tracing::Level::TRACE => {
                    write!(writer, "\x1b[2m  {}\x1b[0m", msg)?;
                }
            }
        } else {
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
