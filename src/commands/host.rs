//! `fed host <service>`: the per-service pty host.
//!
//! Spike-only. The host owns one service's pty for the service's whole
//! lifetime: it tees everything the service writes to the service's log file
//! and to every attached client, and forwards client input back to the pty.
//! It exits when the service exits.
//!
//! Detachment mechanics mirror `fed supervise` (`spawn_if_needed` there):
//! fed's own binary, SIGHUP ignored in `main`, spawned into its own process
//! group with null stdio.

use fed::config::Config;
use fed::fed_dir::attach_socket_path;
use fed::service::pty::spawn_on_pty;
use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::os::unix::fs::PermissionsExt;
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// How long to let the output pump drain the pty after the service exits.
const DRAIN_TIMEOUT: Duration = Duration::from_secs(2);

type Clients = Arc<Mutex<Vec<Arc<UnixStream>>>>;

pub fn run_host(config: &Config, work_dir: &Path, service: &str) -> anyhow::Result<()> {
    let command = config
        .services
        .get(service)
        .ok_or_else(|| anyhow::anyhow!("unknown service '{}'", service))?
        .process
        .clone()
        .ok_or_else(|| {
            anyhow::anyhow!("'{}' is not a process service, so it has no pty", service)
        })?;

    let log = open_log(work_dir, service)?;
    let socket_path = attach_socket_path(work_dir, service);
    let listener = bind_socket(&socket_path)?;
    tracing::info!(
        "fed host {}: listening on {}",
        service,
        socket_path.display()
    );

    let pty = spawn_on_pty(&command, work_dir).inspect_err(|_| {
        // Nothing will ever answer on a socket whose service never started.
        let _ = std::fs::remove_file(&socket_path);
    })?;
    let mut child = pty.child;
    let master = Arc::new(File::from(pty.master));
    let clients: Clients = Arc::new(Mutex::new(Vec::new()));

    let (drained_tx, drained_rx) = std::sync::mpsc::channel::<()>();
    std::thread::spawn({
        let master = Arc::clone(&master);
        let clients = Arc::clone(&clients);
        move || {
            pump_pty_output(&master, log, &clients);
            let _ = drained_tx.send(());
        }
    });
    std::thread::spawn({
        let master = Arc::clone(&master);
        let clients = Arc::clone(&clients);
        move || accept_clients(listener, &master, &clients)
    });

    let status = child.wait();
    // The service is gone, but its last bytes may still be in the pty
    // buffer, and a client may not have been handed them yet.
    let _ = drained_rx.recv_timeout(DRAIN_TIMEOUT);
    let _ = std::fs::remove_file(&socket_path);
    tracing::info!(
        "fed host {}: service exited ({:?}), exiting",
        service,
        status
    );

    // Client threads are blocked in `read`, and there is nothing left for
    // them to read into; exiting the process is the whole shutdown.
    std::process::exit(0);
}

/// The same append-mode file the nohup wrapper writes today, so `fed logs`
/// keeps working for a hosted service.
fn open_log(work_dir: &Path, service: &str) -> anyhow::Result<File> {
    let dir = fed::fed_dir::fed_dir(work_dir).join("logs");
    std::fs::create_dir_all(&dir)?;
    let sanitized: String = service
        .chars()
        .map(|c| {
            if c.is_alphanumeric() || c == '-' || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect();
    Ok(OpenOptions::new()
        .create(true)
        .append(true)
        .open(dir.join(format!("{}.log", sanitized)))?)
}

fn bind_socket(path: &Path) -> anyhow::Result<UnixListener> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
        std::fs::set_permissions(parent, std::fs::Permissions::from_mode(0o700))?;
    }
    if path.exists() {
        // A live host owns its socket; only a dead one's leftovers may go.
        if UnixStream::connect(path).is_ok() {
            anyhow::bail!("a host is already listening on {}", path.display());
        }
        std::fs::remove_file(path)?;
    }
    let listener = UnixListener::bind(path)
        .map_err(|e| anyhow::anyhow!("failed to bind {}: {}", path.display(), e))?;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))?;
    Ok(listener)
}

/// Read the pty until the service closes it, teeing to the log and to every
/// connected client. The log write comes first so that any byte a client has
/// seen is already on disk.
fn pump_pty_output(master: &File, mut log: File, clients: &Clients) {
    let mut buf = [0u8; 4096];
    loop {
        let n = match (&mut &*master).read(&mut buf) {
            Ok(0) => break,
            Ok(n) => n,
            // The slave's last holder exiting surfaces as EIO on Linux.
            Err(_) => break,
        };
        let chunk = &buf[..n];
        let _ = log.write_all(chunk);
        let _ = log.flush();
        broadcast(clients, chunk);
    }
}

fn broadcast(clients: &Clients, chunk: &[u8]) {
    let mut guard = clients.lock().expect("clients mutex");
    guard.retain(|client| (&mut &**client).write_all(chunk).is_ok());
}

fn accept_clients(listener: UnixListener, master: &Arc<File>, clients: &Clients) {
    for stream in listener.incoming() {
        let Ok(stream) = stream else { continue };
        let stream = Arc::new(stream);
        clients
            .lock()
            .expect("clients mutex")
            .push(Arc::clone(&stream));
        std::thread::spawn({
            let master = Arc::clone(master);
            move || {
                let mut buf = [0u8; 4096];
                loop {
                    match (&mut &*stream).read(&mut buf) {
                        Ok(0) | Err(_) => break,
                        Ok(n) => {
                            if (&mut &*master).write_all(&buf[..n]).is_err() {
                                break;
                            }
                        }
                    }
                }
            }
        });
    }
}
