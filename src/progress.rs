//! Coordinated single-line terminal progress.
//!
//! During `fed start`, three writers share one terminal: the command's own
//! progress lines (stdout), tracing events rendered by `CliFormatter`
//! (stderr), and lifecycle hook output (`install:`/`migrate:` commands).
//! Historically the progress line was left dangling (`print!` without a
//! newline) while the other two printed straight through it, producing
//! fragments like a lone " ready" after a healthcheck message.
//!
//! This module owns the notion of a single *pending* progress line. All
//! terminal writers route through it:
//!
//! - [`set_line`] draws or replaces the pending line in place (`\r` + clear)
//! - [`set_detail`] appends live detail (e.g. "healthcheck 12s/60s") to it
//! - [`finish_line`] replaces the pending line with a final full line
//! - [`println_above`] / [`eprintln_above`] print a complete line while a
//!   pending line is up: clear, print, redraw
//!
//! On a non-TTY stdout the pending line is suppressed entirely — only
//! complete lines are printed, which is what CI logs want. [`set_silent`]
//! additionally suppresses all output for full-screen TUI mode.

use std::io::{IsTerminal, Write};
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};

struct Pending {
    base: String,
    detail: String,
}

static PENDING: Mutex<Option<Pending>> = Mutex::new(None);
static SILENT: AtomicBool = AtomicBool::new(false);

fn stdout_is_tty() -> bool {
    std::io::stdout().is_terminal()
}

/// Suppress all coordinated output (used by the TUI, which owns the screen).
pub fn set_silent(silent: bool) {
    SILENT.store(silent, Ordering::SeqCst);
}

fn is_silent() -> bool {
    SILENT.load(Ordering::SeqCst)
}

/// Whether an in-place progress line is currently pending.
///
/// Callers that would otherwise be silent during a long wait (no pending
/// line to attach detail to) can use this to fall back to a plain log line.
pub fn has_pending() -> bool {
    PENDING.lock().unwrap().is_some()
}

fn redraw(out: &mut impl Write, pending: &Pending) {
    if pending.detail.is_empty() {
        let _ = write!(out, "{}", pending.base);
    } else {
        let _ = write!(out, "{}  \x1b[2m{}\x1b[0m", pending.base, pending.detail);
    }
}

/// Draw or replace the in-place pending progress line (no trailing newline).
///
/// No-op on non-TTY stdout: the eventual [`finish_line`] prints the complete
/// line instead.
pub fn set_line(line: &str) {
    if is_silent() || !stdout_is_tty() {
        return;
    }
    let mut p = PENDING.lock().unwrap();
    let mut out = std::io::stdout().lock();
    if p.is_some() {
        let _ = write!(out, "\r\x1b[2K");
    }
    let pending = Pending {
        base: line.to_string(),
        detail: String::new(),
    };
    redraw(&mut out, &pending);
    let _ = out.flush();
    *p = Some(pending);
}

/// Update the dimmed live-detail suffix of the pending line, if one is up.
///
/// Used for liveness during long waits ("healthcheck 12s/60s"). No-op when
/// nothing is pending, so library code can call it unconditionally.
pub fn set_detail(detail: &str) {
    if is_silent() {
        return;
    }
    let mut p = PENDING.lock().unwrap();
    if let Some(ref mut pending) = *p {
        pending.detail = detail.to_string();
        let mut out = std::io::stdout().lock();
        let _ = write!(out, "\r\x1b[2K");
        redraw(&mut out, pending);
        let _ = out.flush();
    }
}

/// Replace the pending line (if any) with a final, complete line.
pub fn finish_line(line: &str) {
    if is_silent() {
        return;
    }
    let mut p = PENDING.lock().unwrap();
    let mut out = std::io::stdout().lock();
    if p.is_some() {
        let _ = write!(out, "\r\x1b[2K");
    }
    let _ = writeln!(out, "{}", line);
    let _ = out.flush();
    *p = None;
}

/// Clear the pending line without printing anything in its place.
pub fn clear_line() {
    if is_silent() {
        return;
    }
    let mut p = PENDING.lock().unwrap();
    if p.is_some() {
        let mut out = std::io::stdout().lock();
        let _ = write!(out, "\r\x1b[2K");
        let _ = out.flush();
    }
    *p = None;
}

/// Print a complete line to stdout, above any pending progress line.
pub fn println_above(text: &str) {
    if is_silent() {
        return;
    }
    let p = PENDING.lock().unwrap();
    let mut out = std::io::stdout().lock();
    if let Some(ref pending) = *p {
        let _ = write!(out, "\r\x1b[2K");
        let _ = writeln!(out, "{}", text);
        redraw(&mut out, pending);
    } else {
        let _ = writeln!(out, "{}", text);
    }
    let _ = out.flush();
}

/// Print a complete line to stderr, above any pending progress line.
///
/// The pending line lives on stdout; both streams share the terminal, so the
/// clear/redraw happens on stdout around the stderr write, with flushes
/// ordering the interleave.
pub fn eprintln_above(text: &str) {
    if is_silent() {
        return;
    }
    let p = PENDING.lock().unwrap();
    if p.is_some() {
        let mut out = std::io::stdout().lock();
        let _ = write!(out, "\r\x1b[2K");
        let _ = out.flush();
        eprintln!("{}", text);
        if let Some(ref pending) = *p {
            redraw(&mut out, pending);
        }
        let _ = out.flush();
    } else {
        eprintln!("{}", text);
    }
}

/// Print one line of lifecycle hook output (install/migrate/build commands),
/// dimmed and prefixed with the service name so it reads as subordinate to
/// fed's own progress.
pub fn hook_output_line(service: &str, line: &str) {
    if is_silent() {
        return;
    }
    if stdout_is_tty() {
        println_above(&format!("  \x1b[2m{} │ {}\x1b[0m", service, line));
    } else {
        println_above(&format!("  {} │ {}", service, line));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn silent_mode_suppresses_pending_state() {
        set_silent(true);
        set_line("should not stick");
        assert!(PENDING.lock().unwrap().is_none());
        set_silent(false);
    }

    #[test]
    fn finish_clears_pending() {
        // Non-TTY in tests: set_line is a no-op, finish_line still prints
        set_line("pending");
        finish_line("done");
        assert!(PENDING.lock().unwrap().is_none());
    }
}
