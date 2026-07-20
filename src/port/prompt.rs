use crate::error::Result;
use crate::port::PortConflict;
use std::io::{Write, stdin, stdout};

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum PortConflictAction {
    KillAndRetry,
    Retry,
    Ignore,
    Abort,
}

/// Handle a port conflict interactively (if TTY) or with an error message
pub fn handle_port_conflict(
    port: u16,
    param_name: &str,
    alternative_port: u16,
    conflict: &PortConflict,
) -> Result<PortConflictAction> {
    if is_interactive() {
        prompt_user(port, param_name, alternative_port, conflict)
    } else {
        // Non-interactive: auto-fallback to alternative port (dynamic allocation)
        Ok(PortConflictAction::Ignore)
    }
}

/// Check if running in interactive TTY
fn is_interactive() -> bool {
    use std::io::IsTerminal;
    if std::env::var_os("FED_NON_INTERACTIVE").is_some() {
        return false;
    }
    // Cargo test binaries run from target/*/deps/ — never interactive
    if let Ok(exe) = std::env::current_exe()
        && let Some(path) = exe.to_str()
        && (path.contains("/deps/") || path.contains("\\deps\\"))
    {
        return false;
    }
    stdin().is_terminal() && stdout().is_terminal()
}

/// Show interactive prompt and return user's choice
fn prompt_user(
    port: u16,
    param_name: &str,
    alternative_port: u16,
    conflict: &PortConflict,
) -> Result<PortConflictAction> {
    println!();
    println!("⚠️  Port {} ({}) is in use", port, param_name);
    println!();

    if !conflict.processes.is_empty() {
        println!(
            "Process{} using port:",
            if conflict.processes.len() > 1 {
                "es"
            } else {
                ""
            }
        );
        for process in &conflict.processes {
            println!("  PID:     {}", process.pid);
            println!("  Process: {}", process.name);
            if let Some(ref cmd) = process.command {
                println!("  Command: {}", cmd);
            }
            if conflict.processes.len() > 1 {
                println!();
            }
        }
        println!();
    }

    println!("Options:");
    println!("  [k] Kill existing process and retry");
    println!("  [r] Retry (process may have exited)");
    println!(
        "  [i] Ignore and use random port (currently: {})",
        alternative_port
    );
    println!("  [Esc/q] Abort startup");
    println!();
    print!("Your choice: ");
    stdout().flush().ok();

    loop {
        use crossterm::event::{Event, KeyCode, KeyEvent, read};

        if let Ok(Event::Key(KeyEvent { code, .. })) = read() {
            match code {
                KeyCode::Char('k') | KeyCode::Char('K') => {
                    println!("k");
                    return Ok(PortConflictAction::KillAndRetry);
                }
                KeyCode::Char('r') | KeyCode::Char('R') => {
                    println!("r");
                    return Ok(PortConflictAction::Retry);
                }
                KeyCode::Char('i') | KeyCode::Char('I') => {
                    println!("i");
                    return Ok(PortConflictAction::Ignore);
                }
                KeyCode::Char('q') | KeyCode::Char('Q') | KeyCode::Esc => {
                    println!("q");
                    return Ok(PortConflictAction::Abort);
                }
                _ => {
                    // Invalid input, continue waiting
                }
            }
        }
    }
}
