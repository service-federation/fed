//! Terminal handover for `fed start -i`.
//!
//! The foreground service runs in its own process group, like every other
//! process service, so `fed stop`, orphan cleanup, and signal forwarding
//! all address the service's group and never fed's own. What makes it
//! interactive is that its group is made the terminal's foreground group:
//! it can read the tty without SIGTTIN, and Ctrl+C or Ctrl+Z from the
//! keyboard reach it alone. This is the arrangement a shell sets up for a
//! job, and the one `sudo` sets up for the command it runs.
//!
//! fed, now a background group on its own terminal, waits with job control:
//! when the child stops (Ctrl+Z, or a tty read while backgrounded) fed
//! takes the terminal back and stops itself, so the user's shell reports
//! the job as suspended; when the shell continues fed, fed hands the
//! terminal back and continues the child. Nothing here is async: the wait
//! is a blocking `waitpid` loop meant for a blocking thread.

use std::io::{self, IsTerminal};
use std::os::fd::{AsRawFd, BorrowedFd, RawFd};
use std::os::unix::process::ExitStatusExt;
use std::process::ExitStatus;

use nix::sys::signal::{SigHandler, Signal, kill, killpg, signal};
use nix::sys::wait::{WaitPidFlag, WaitStatus, waitpid};
use nix::unistd::{Pid, getpgrp, getpid, tcgetpgrp, tcsetpgrp};

/// The terminal a foreground service can be handed: fed's stdin, when it
/// is one. Without a terminal there is nothing to hand over and the child
/// simply runs in its own group with fed's stdio.
pub fn controlling_tty() -> Option<RawFd> {
    let stdin = io::stdin();
    stdin.is_terminal().then_some(stdin.as_raw_fd())
}

/// Make the calling process's group the terminal's foreground group.
///
/// Runs in the child between fork and exec, after std has put it in its
/// own group, so it only calls async-signal-safe functions. SIGTTOU is
/// ignored around the call because the child is a background group until
/// the call succeeds, and reset afterwards so the program starts with the
/// default disposition.
pub fn claim_terminal_in_child(tty: RawFd) -> io::Result<()> {
    let fd = unsafe { BorrowedFd::borrow_raw(tty) };
    // SAFETY: plain disposition changes for the current process, which is
    // the freshly forked child about to exec.
    let previous = unsafe { signal(Signal::SIGTTOU, SigHandler::SigIgn) }?;
    let claimed = tcsetpgrp(fd, getpgrp());
    unsafe { signal(Signal::SIGTTOU, previous) }?;
    claimed?;
    Ok(())
}

/// Wait for the foreground child, forwarding stop and continue between it
/// and the shell that owns fed, and return its exit status. The terminal,
/// if any, belongs to fed again when this returns.
pub fn wait_with_job_control(child: Pid, tty: Option<RawFd>) -> io::Result<ExitStatus> {
    let own_group = getpgrp();
    let tty = tty.map(|raw| unsafe { BorrowedFd::borrow_raw(raw) });

    // fed is a background group while the child holds the terminal, so its
    // own tcsetpgrp calls would stop it with SIGTTOU. Ignoring the signal
    // lets them proceed; fed has no other use for it.
    // SAFETY: disposition change for the current process only.
    unsafe { signal(Signal::SIGTTOU, SigHandler::SigIgn) }?;

    let status = loop {
        match waitpid(child, Some(WaitPidFlag::WUNTRACED))? {
            WaitStatus::Exited(_, code) => break ExitStatus::from_raw(code << 8),
            WaitStatus::Signaled(_, sig, core_dumped) => {
                break ExitStatus::from_raw(sig as i32 | if core_dumped { 0x80 } else { 0 });
            }
            WaitStatus::Stopped(_, _) => {
                // Take the terminal back and stop with the child, so the
                // shell sees its job (fed) suspended and can resume it.
                if let Some(fd) = tty {
                    let _ = tcsetpgrp(fd, own_group);
                }
                let _ = kill(getpid(), Signal::SIGSTOP);
                // Continued by the shell. Hand the terminal back only if
                // the shell gave it to fed (`fg`); after `bg` the child
                // stays a background group and stops again on its next
                // tty read, which lands back here.
                if let Some(fd) = tty
                    && tcgetpgrp(fd).ok() == Some(own_group)
                {
                    let _ = tcsetpgrp(fd, child);
                }
                let _ = killpg(child, Signal::SIGCONT);
            }
            _ => continue,
        }
    };

    if let Some(fd) = tty {
        let _ = tcsetpgrp(fd, own_group);
    }
    Ok(status)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::process::CommandExt;

    // The children are reaped by `wait_with_job_control` itself, which is
    // the point of the test.
    #[allow(clippy::zombie_processes)]
    #[test]
    fn wait_reports_exit_codes_and_signals() {
        let child = std::process::Command::new("sh")
            .args(["-c", "exit 7"])
            .process_group(0)
            .spawn()
            .expect("spawn");
        let status = wait_with_job_control(Pid::from_raw(child.id() as i32), None).unwrap();
        assert_eq!(status.code(), Some(7));

        let child = std::process::Command::new("sleep")
            .arg("30")
            .process_group(0)
            .spawn()
            .expect("spawn");
        let pid = Pid::from_raw(child.id() as i32);
        kill(pid, Signal::SIGTERM).unwrap();
        let status = wait_with_job_control(pid, None).unwrap();
        assert_eq!(status.code(), None);
        assert_eq!(status.signal(), Some(Signal::SIGTERM as i32));
    }
}
