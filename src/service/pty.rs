//! Pty allocation and pty-backed process spawning.
//!
//! Used by the `fed host` process, which owns a service's pty for the
//! service's whole lifetime so that clients can attach to it later.

use crate::error::{Error, Result};
use nix::libc;
use std::io;
use std::os::fd::{AsRawFd, OwnedFd, RawFd};
use std::os::unix::process::CommandExt;
use std::path::Path;
use std::process::{Child, Command, Stdio};

// TIOCSCTTY takes an int argument (0 = do not steal the terminal from
// another session). The `_bad` variant is for ioctls whose request code is
// a plain constant rather than one nix can compute from a type.
nix::ioctl_write_int_bad!(set_controlling_terminal, libc::TIOCSCTTY);

/// A process running on a pty, plus the master side of that pty.
pub struct PtyChild {
    pub child: Child,
    pub master: OwnedFd,
}

/// Run `command` through `bash -ec 'exec <command>'` on a fresh pty.
///
/// The child becomes a session leader with the pty slave as its controlling
/// terminal, so programs that check `isatty()` or want job control see a
/// real terminal. The slave is closed in the parent on the way out: while
/// any process holds it open, reads on the master block forever instead of
/// reporting the child's exit.
pub fn spawn_on_pty(command: &str, work_dir: &Path) -> Result<PtyChild> {
    let pty = nix::pty::openpty(None, None)
        .map_err(|e| Error::Process(format!("openpty failed: {}", e)))?;
    let master_raw: RawFd = pty.master.as_raw_fd();
    let slave_raw: RawFd = pty.slave.as_raw_fd();

    let mut cmd = Command::new("/bin/bash");
    cmd.arg("-ec").arg(format!("exec {}", command));
    cmd.current_dir(work_dir);
    // The pre_exec hook below replaces all three of these with the slave.
    cmd.stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());

    // Everything in here runs between fork and exec, so it must stay
    // async-signal-safe: raw syscalls only, no allocation.
    unsafe {
        cmd.pre_exec(move || {
            if libc::setsid() == -1 {
                return Err(io::Error::last_os_error());
            }
            set_controlling_terminal(slave_raw, 0)
                .map_err(|e| io::Error::from_raw_os_error(e as i32))?;
            for target in [libc::STDIN_FILENO, libc::STDOUT_FILENO, libc::STDERR_FILENO] {
                if libc::dup2(slave_raw, target) == -1 {
                    return Err(io::Error::last_os_error());
                }
            }
            if slave_raw > libc::STDERR_FILENO {
                libc::close(slave_raw);
            }
            libc::close(master_raw);
            Ok(())
        });
    }

    let child = cmd
        .spawn()
        .map_err(|e| Error::Process(format!("failed to spawn '{}' on a pty: {}", command, e)))?;

    drop(pty.slave);

    Ok(PtyChild {
        child,
        master: pty.master,
    })
}
