//! Launch and cleanup of detached terminal hosts.
use crate::attach::launch::{HostEvent, LaunchSpec};
use crate::error::{Error, Result, validate_pid};
use chrono::{DateTime, Utc};
use nix::libc;
use nix::sys::signal::{Signal, kill, killpg};
use std::path::Path;
use std::process::Stdio;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, Command};

struct PendingHost {
    child: Child,
    service_pid: Option<u32>,
    armed: bool,
}

impl Drop for PendingHost {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        if let Some(pid) = self
            .service_pid
            .and_then(|pid| validate_pid(pid, "hosted service").ok())
        {
            let _ = killpg(pid, Signal::SIGKILL);
        }
        if let Some(pid) = self
            .child
            .id()
            .and_then(|pid| validate_pid(pid, "host").ok())
        {
            let _ = kill(pid, Signal::SIGTERM);
        }
    }
}

/// Launch the host and keep ownership until its startup crash window passes.
pub async fn launch(spec: &LaunchSpec) -> Result<(u32, u32)> {
    let failure = |message: String| Error::ServiceStartFailed(spec.service.clone(), message);
    let mut command = Command::new(std::env::current_exe()?);
    command
        .arg("--workdir")
        .arg(&spec.work_dir)
        .arg("host")
        .arg(&spec.service)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null());
    // SAFETY: setsid is async-signal-safe and the closure does not allocate.
    unsafe {
        command.pre_exec(|| {
            if libc::setsid() == -1 {
                return Err(std::io::Error::last_os_error());
            }
            Ok(())
        });
    }
    let mut pending = PendingHost {
        child: command.spawn().map_err(|e| failure(e.to_string()))?,
        service_pid: None,
        armed: true,
    };
    let host_pid = pending
        .child
        .id()
        .ok_or_else(|| failure("Host has no PID".into()))?;
    let stdin = pending.child.stdin.take().unwrap();
    let stdout = pending.child.stdout.take().unwrap();
    let mut reader = BufReader::new(stdout);
    let line = spec.to_line()?;
    let handshake = async {
        let mut stdin = stdin;
        stdin.write_all(line.as_bytes()).await?;
        stdin.shutdown().await?;
        drop(stdin);
        read_event(&mut reader).await
    };
    let event = tokio::time::timeout(Duration::from_secs(5), handshake)
        .await
        .map_err(|_| failure("Timed out waiting for terminal host".into()))?
        .map_err(|e| failure(e.to_string()))?;
    let pid = match event {
        HostEvent::Ready { pid } => {
            validate_pid(pid, &spec.service)?;
            pid
        }
        HostEvent::Failed { error } => return Err(failure(error)),
        HostEvent::Exited { .. } => {
            return Err(failure("Host exited before its startup handshake".into()));
        }
    };
    pending.service_pid = Some(pid);
    let mut event = tokio::time::timeout(Duration::from_millis(500), read_event(&mut reader)).await;
    if event.is_err() && !alive(pid) {
        event = tokio::time::timeout(Duration::from_millis(2500), read_event(&mut reader)).await;
    }
    match event {
        Ok(Ok(HostEvent::Exited { status })) => {
            let status = if libc::WIFEXITED(status) {
                libc::WEXITSTATUS(status)
            } else {
                128 + libc::WTERMSIG(status)
            };
            let log = tokio::fs::read_to_string(&spec.log_path)
                .await
                .unwrap_or_default();
            let preview = log
                .lines()
                .rev()
                .take(15)
                .collect::<Vec<_>>()
                .into_iter()
                .rev()
                .collect::<Vec<_>>()
                .join("\n");
            return Err(failure(format!(
                "Service '{}' exited with code {} right after starting.\n\n{}",
                spec.service,
                status,
                if preview.is_empty() {
                    "Log file is empty."
                } else {
                    &preview
                }
            )));
        }
        Ok(Ok(_)) => return Err(failure("Unexpected terminal host startup event".into())),
        Ok(Err(e)) => {
            return Err(failure(format!(
                "Terminal host closed its startup pipe: {e}"
            )));
        }
        Err(_) => {
            if !alive(pid) || !alive(host_pid) {
                return Err(failure(format!(
                    "Service '{}' exited right after starting before its host reported an exit status.",
                    spec.service
                )));
            }
        }
    }
    pending.armed = false;
    Ok((pid, host_pid))
}

async fn read_event(reader: &mut BufReader<tokio::process::ChildStdout>) -> Result<HostEvent> {
    let mut line = String::new();
    let bytes = reader.take(64 * 1024).read_line(&mut line).await?;
    if bytes == 0 || !line.ends_with('\n') {
        return Err(Error::Validation(
            "Incomplete terminal host handshake".into(),
        ));
    }
    HostEvent::from_line(&line)
}

fn alive(pid: u32) -> bool {
    validate_pid(pid, "host").is_ok_and(|pid| kill(pid, None).is_ok())
}

async fn is_host(pid: u32, service: &str, started_at: DateTime<Utc>) -> bool {
    if !alive(pid) || !crate::error::validate_pid_start_time(pid, started_at) {
        return false;
    }
    let Ok(output) = Command::new("ps")
        .args(["-ww", "-p", &pid.to_string(), "-o", "command="])
        .output()
        .await
    else {
        return false;
    };
    let command = String::from_utf8_lossy(&output.stdout);
    let command = command.trim();
    let Some((executable, _)) = command.split_once(" --workdir ") else {
        return false;
    };
    Path::new(executable)
        .file_name()
        .is_some_and(|name| name == "fed")
        && command.ends_with(&format!(" host {service}"))
}

/// Reap a host only when the recorded PID still identifies a fed host.
pub async fn reap_host(
    service: &str,
    host_pid: Option<u32>,
    socket: Option<&Path>,
    started_at: DateTime<Utc>,
) {
    if let Some(pid) = host_pid {
        for _ in 0..20 {
            if !alive(pid) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        if is_host(pid, service, started_at).await {
            let _ = kill(validate_pid(pid, "host").unwrap(), Signal::SIGTERM);
            for _ in 0..20 {
                if !alive(pid) {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            if is_host(pid, service, started_at).await {
                let _ = kill(validate_pid(pid, "host").unwrap(), Signal::SIGKILL);
                for _ in 0..10 {
                    if !alive(pid) {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            }
        }
    }
    if let Some(socket) = socket {
        remove_dead_socket(socket);
    }
}

/// Remove sockets only when connecting proves there is no listener.
pub fn remove_dead_socket(socket: &Path) {
    use std::os::unix::fs::{FileTypeExt, OpenOptionsExt};
    let Ok(lock) = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .mode(0o600)
        .open(socket.with_extension("sock.lock"))
    else {
        return;
    };
    if fs2::FileExt::try_lock_exclusive(&lock).is_err() {
        return;
    }
    if !std::fs::symlink_metadata(socket).is_ok_and(|m| m.file_type().is_socket()) {
        return;
    }
    if let Err(error) = std::os::unix::net::UnixStream::connect(socket)
        && matches!(
            error.kind(),
            std::io::ErrorKind::ConnectionRefused | std::io::ErrorKind::NotFound
        )
    {
        let _ = std::fs::remove_file(socket);
    }
}

pub fn sweep_dead_sockets(work_dir: &Path) {
    if let Ok(entries) = std::fs::read_dir(crate::fed_dir::fed_dir(work_dir).join("attach")) {
        for entry in entries.flatten() {
            if entry
                .path()
                .extension()
                .is_some_and(|extension| extension == "sock")
            {
                remove_dead_socket(&entry.path());
            }
        }
    }
}
