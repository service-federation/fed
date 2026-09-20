//! Pty allocation and pty-backed process spawning.
//!
//! Used by the `fed host` process, which owns a service's pty for the
//! service's whole lifetime so that clients can attach to it later.

use crate::config::ResourceLimits;
use crate::error::{Error, Result};
use nix::libc;
use std::collections::HashMap;
use std::io;
use std::os::fd::{AsRawFd, OwnedFd, RawFd};
use std::os::unix::process::CommandExt;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};

use super::resource_limits::ParsedResourceLimits;

// TIOCSCTTY takes an int argument (0 = do not steal the terminal from
// another session). The `_bad` variants are for ioctls whose request code is
// a plain constant rather than one nix can compute from a type.
nix::ioctl_write_int_bad!(set_controlling_terminal, libc::TIOCSCTTY);
nix::ioctl_write_ptr_bad!(set_winsize, libc::TIOCSWINSZ, libc::winsize);

/// Everything the host needs to put one service on a pty.
///
/// `fed start` resolves all of it and sends it to the host as a launch spec,
/// so the host never loads config and never reads the vault.
pub struct PtyLaunch {
    /// The `process:` string, run as `bash -ec <command>`.
    pub command: String,
    /// Already resolved against the work dir.
    pub cwd: PathBuf,
    /// The service's resolved environment, markers excluded.
    pub environment: HashMap<String, String>,
    /// Value for `FED_SPAWNED_BY_SERVICE`, the service's name.
    pub service: String,
    /// Value for `FED_SPAWNED_FROM_WORKSPACE`, the workspace path.
    pub workspace: PathBuf,
    /// Applied with `setrlimit` in the child, parsed before the fork.
    pub resources: Option<ResourceLimits>,
}

/// A process running on a pty, plus the master side of that pty.
pub struct PtyChild {
    /// The service process. Its pid is the one fed stores in state.
    pub child: Child,
    /// Read for the service's output, write for its input.
    pub master: OwnedFd,
}

/// Run `spec.command` through `bash -ec <command>` on a fresh pty.
///
/// Bash gets the command as written. A single simple command is exec'd in
/// place, so the pid is the program itself; a compound command or script
/// keeps bash as the session leader with the program as its child, and the
/// session is what `fed stop` signals. An explicit `exec` prefix would run
/// only the first command of `a && b`.
///
/// The child becomes a session leader with the pty slave as its controlling
/// terminal, so programs that check `isatty()` or want job control see a
/// real terminal. The slave is closed in the parent on the way out: while
/// any process holds it open, reads on the master block forever instead of
/// reporting the child's exit.
pub fn spawn_on_pty(spec: &PtyLaunch) -> Result<PtyChild> {
    // String parsing is not async-signal-safe, so the limits are resolved to
    // numbers here, before the fork. See `ParsedResourceLimits::apply`.
    let limits = spec.resources.as_ref().map(|resources| {
        let mut parsed = ParsedResourceLimits::from_config(resources);
        let capped = parsed.validate_against_system();
        warn_about_capped_limits(&spec.service, &capped);
        parsed
    });

    let pty = nix::pty::openpty(None, None)
        .map_err(|e| Error::Process(format!("openpty failed: {}", e)))?;
    let master_raw: RawFd = pty.master.as_raw_fd();
    let slave_raw: RawFd = pty.slave.as_raw_fd();

    let mut cmd = Command::new("/bin/bash");
    cmd.arg("-ec").arg(&spec.command);
    cmd.current_dir(&spec.cwd);
    cmd.envs(&spec.environment)
        // Markers for the recursion check in main.rs. Service name is
        // identity for the error message; workspace path lets the child fed
        // distinguish same-workspace recursion from a cross-workspace call.
        .env("FED_SPAWNED_BY_SERVICE", &spec.service)
        .env("FED_SPAWNED_FROM_WORKSPACE", &spec.workspace);
    // The pre_exec hook below replaces all three of these with the slave.
    cmd.stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());

    // SAFETY: the closure runs after fork() but before exec() in the child of
    // what is, in fed, always a multithreaded process — a thread other than
    // the one that forked may hold the allocator lock, and the child can
    // never release it, so allocating here can deadlock the child. Everything
    // below is a raw syscall over `Copy` values captured by the closure.
    // `ParsedResourceLimits::apply` holds to the same rule; see its doc
    // comment for the parts POSIX does not guarantee.
    //
    // See: https://man7.org/linux/man-pages/man7/signal-safety.7.html
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
            if let Some(limits) = limits {
                limits.apply()?;
            }
            Ok(())
        });
    }

    let child = cmd.spawn().map_err(|e| {
        Error::Process(format!(
            "failed to spawn '{}' on a pty: {}",
            spec.command, e
        ))
    })?;

    drop(pty.slave);

    Ok(PtyChild {
        child,
        master: pty.master,
    })
}

/// Tell the pty how large the attached terminal is, so `stty size` inside the
/// service is right and full-screen programs redraw at the new size.
pub fn set_window_size(master: &impl AsRawFd, cols: u16, rows: u16) -> Result<()> {
    let size = libc::winsize {
        ws_row: rows,
        ws_col: cols,
        ws_xpixel: 0,
        ws_ypixel: 0,
    };
    // SAFETY: `size` is a fully initialised `winsize` that outlives the call,
    // and the fd is borrowed for the duration of the ioctl.
    unsafe { set_winsize(master.as_raw_fd(), &size) }
        .map_err(|e| Error::Process(format!("resizing the pty failed: {}", e)))?;
    Ok(())
}

fn warn_about_capped_limits(
    service: &str,
    validation: &super::resource_limits::ResourceLimitValidation,
) {
    use super::resource_limits::LimitValidationResult::Capped;

    if let Some(Capped {
        requested,
        hard_limit,
    }) = validation.memory
    {
        tracing::warn!(
            "Service '{}': Requested memory limit {} exceeds system hard limit {}, capping to hard limit",
            service,
            super::resource_limits::format_bytes(requested),
            super::resource_limits::format_bytes(hard_limit)
        );
    }
    if let Some(Capped {
        requested,
        hard_limit,
    }) = validation.nofile
    {
        tracing::warn!(
            "Service '{}': Requested nofile limit {} exceeds system hard limit {}, capping to hard limit",
            service,
            requested,
            hard_limit
        );
    }
    #[cfg(target_os = "linux")]
    if let Some(Capped {
        requested,
        hard_limit,
    }) = validation.pids
    {
        tracing::warn!(
            "Service '{}': Requested pids limit {} exceeds system hard limit {}, capping to hard limit",
            service,
            requested,
            hard_limit
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Read;

    /// Reads the master until the pty hangs up. Linux reports the hangup as
    /// `EIO` rather than a zero-length read, so any error ends the read too.
    fn read_master_to_eof(master: OwnedFd) -> Vec<u8> {
        let mut file = std::fs::File::from(master);
        let mut out = Vec::new();
        let mut buf = [0u8; 4096];
        loop {
            match file.read(&mut buf) {
                Ok(0) | Err(_) => break,
                Ok(n) => out.extend_from_slice(&buf[..n]),
            }
        }
        out
    }

    fn launch(command: &str, cwd: PathBuf) -> PtyLaunch {
        PtyLaunch {
            command: command.to_string(),
            cwd,
            environment: HashMap::new(),
            service: "pty-test".to_string(),
            workspace: PathBuf::from("/tmp"),
            resources: None,
        }
    }

    // Requires a pty. The ubuntu and macos CI runners have one.
    #[test]
    fn the_child_runs_on_a_terminal() {
        let mut spawned = spawn_on_pty(&launch("tty", std::env::temp_dir())).unwrap();
        let output = String::from_utf8_lossy(&read_master_to_eof(spawned.master)).into_owned();
        let status = spawned.child.wait().unwrap();

        assert!(status.success(), "`tty` reported no terminal: {}", output);
        assert!(
            output.contains("/dev/"),
            "expected a terminal device path, got {:?}",
            output
        );
    }

    // Requires a pty.
    #[test]
    fn the_child_starts_in_the_given_cwd() {
        let dir = tempfile::tempdir().unwrap();
        let expected = dir.path().canonicalize().unwrap();
        let mut spawned = spawn_on_pty(&launch("pwd", expected.clone())).unwrap();
        let output = String::from_utf8_lossy(&read_master_to_eof(spawned.master)).into_owned();
        spawned.child.wait().unwrap();

        assert!(
            output.contains(expected.to_str().unwrap()),
            "expected {} in the output, got {:?}",
            expected.display(),
            output
        );
    }

    // Requires a pty.
    #[test]
    fn the_child_receives_the_environment_and_the_markers() {
        // macOS `printenv` takes one name, so ask for each on its own.
        let mut spec = launch(
            "bash -c 'printenv FED_PTY_TEST; printenv FED_SPAWNED_BY_SERVICE'",
            std::env::temp_dir(),
        );
        spec.environment
            .insert("FED_PTY_TEST".to_string(), "marker-7".to_string());
        let mut spawned = spawn_on_pty(&spec).unwrap();
        let output = String::from_utf8_lossy(&read_master_to_eof(spawned.master)).into_owned();
        spawned.child.wait().unwrap();

        assert!(output.contains("marker-7"), "got {:?}", output);
        assert!(output.contains("pty-test"), "got {:?}", output);
    }

    // Requires a pty. The parent's copy of the slave is closed inside
    // `spawn_on_pty`; while any process holds it open, this read never ends.
    #[test]
    fn the_master_reports_eof_once_the_child_is_gone() {
        let mut spawned = spawn_on_pty(&launch("echo done", std::env::temp_dir())).unwrap();
        let output = read_master_to_eof(spawned.master);
        let status = spawned.child.wait().unwrap();

        assert!(status.success());
        assert!(
            String::from_utf8_lossy(&output).contains("done"),
            "the last bytes must arrive before the hangup, got {:?}",
            String::from_utf8_lossy(&output)
        );
    }

    // Requires a pty. With an `exec` prefix bash would replace itself with
    // `true` and the echo would never run.
    #[test]
    fn a_compound_command_runs_past_its_first_command() {
        let mut spawned =
            spawn_on_pty(&launch("true && echo marker-9", std::env::temp_dir())).unwrap();
        let output = String::from_utf8_lossy(&read_master_to_eof(spawned.master)).into_owned();
        let status = spawned.child.wait().unwrap();

        assert!(status.success());
        assert!(output.contains("marker-9"), "got {:?}", output);
    }

    // Requires a pty.
    #[test]
    fn the_pty_reports_the_size_it_was_given() {
        let mut spawned = spawn_on_pty(&launch(
            "bash -c 'read -r _ignored; stty size'",
            std::env::temp_dir(),
        ))
        .unwrap();
        set_window_size(&spawned.master, 120, 40).unwrap();
        // The child blocks on `read` until the resize has landed.
        nix::unistd::write(&spawned.master, b"go\n").unwrap();

        let output = String::from_utf8_lossy(&read_master_to_eof(spawned.master)).into_owned();
        spawned.child.wait().unwrap();

        assert!(output.contains("40 120"), "got {:?}", output);
    }
}
