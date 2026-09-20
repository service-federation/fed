//! `fed host <service>`: the process that owns a tty service's terminal.
//!
//! `fed start` resolves the command, the cwd, the environment and the two
//! paths, spawns this process and writes one launch spec line to its stdin.
//! The host puts the service on a pseudo-terminal, writes everything the
//! service prints to the service's log file, and serves the attach socket so
//! that `fed attach` can show the same output and type into it.
//!
//! Everything is resolved by the spawner: the host loads no config, reads no
//! vault and opens no state database.

use anyhow::Context;
use fed::attach::launch::{HostEvent, LaunchSpec};
use fed::attach::protocol::{FLAG_NO_STDIN, Frame, FrameReader, ProtocolError, VERSION, encode};
use fed::attach::scrollback::Scrollback;
use fed::service::{PtyLaunch, set_window_size, spawn_on_pty};
use std::fs::{File, OpenOptions, Permissions};
use std::io::{self, BufRead, IsTerminal, Read, Write};
use std::net::Shutdown;
use std::os::unix::fs::{FileTypeExt, PermissionsExt};
use std::os::unix::net::{UnixListener, UnixStream};
use std::os::unix::process::ExitStatusExt;
use std::path::Path;
use std::process;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};

/// What a host started from a terminal prints: there is no launch spec on
/// its stdin to read.
const BARE_INVOCATION: &str =
    "fed host is started by fed start and reads its launch spec from stdin.";

/// How long the output pump may keep reading the pty after the service has
/// exited.
const DRAIN_TIMEOUT: Duration = Duration::from_secs(2);

/// Maximum time a client writer spends blocked in a socket write.
const CLIENT_WRITE_TIMEOUT: Duration = Duration::from_secs(5);

/// How much output the pump reads at a time.
const READ_CHUNK: usize = 4096;

static TERMINATE: AtomicBool = AtomicBool::new(false);

extern "C" fn request_termination(_: i32) {
    TERMINATE.store(true, Ordering::Relaxed);
}

/// Own `service`'s pseudo-terminal until it exits.
///
/// The launch spec arrives on stdin as one JSON line. Everything that can go
/// wrong before the service runs becomes a [`HostEvent::Failed`] line on
/// stdout and exit 1, because `fed start` is still reading there and turns
/// that text into its own start failure.
pub fn run_host(service: &str) -> anyhow::Result<()> {
    let Some(spec) = read_launch_spec().unwrap_or_else(|error| give_up(error)) else {
        eprintln!("{}", BARE_INVOCATION);
        process::exit(1);
    };

    // Register before spawning so cancellation cannot orphan a child whose
    // Ready event has not reached the caller yet.
    unsafe {
        nix::sys::signal::signal(
            nix::sys::signal::Signal::SIGTERM,
            nix::sys::signal::SigHandler::Handler(request_termination),
        )?;
    }
    let (listener, _socket_lock) =
        bind_socket(&spec.socket_path).unwrap_or_else(|error| give_up(error));
    let log = open_log(&spec.log_path)
        .unwrap_or_else(|error| give_up_and_unlink(&spec.socket_path, error));

    let launch = PtyLaunch {
        command: spec.command.clone(),
        cwd: spec.cwd.clone(),
        environment: spec.environment.clone(),
        service: spec.service.clone(),
        workspace: spec.work_dir.clone(),
        resources: spec.resources.clone(),
    };
    let pty =
        spawn_on_pty(&launch).unwrap_or_else(|error| give_up_and_unlink(&spec.socket_path, error));
    let mut child = pty.child;
    let master = Arc::new(File::from(pty.master));
    let pid = child.id();

    emit(&HostEvent::Ready { pid });
    tracing::info!(
        "fed host {}: pid {} is on a pty, listening on {}",
        service,
        pid,
        spec.socket_path.display()
    );

    let session = Arc::new(Mutex::new(Session::default()));
    let (drained_tx, drained_rx) = mpsc::channel();
    std::thread::spawn({
        let master = Arc::clone(&master);
        let session = Arc::clone(&session);
        move || {
            pump_output(&master, log, &session);
            let _ = drained_tx.send(());
        }
    });
    std::thread::spawn({
        let master = Arc::clone(&master);
        let session = Arc::clone(&session);
        move || accept_clients(listener, &master, &session)
    });

    let mut terminating = None;
    let status = loop {
        if let Some(status) = child.try_wait().context("waiting for the hosted service")? {
            break status.into_raw();
        }
        if TERMINATE.load(Ordering::Relaxed) {
            let signal = match terminating {
                None => {
                    terminating = Some(Instant::now());
                    Some(nix::sys::signal::Signal::SIGTERM)
                }
                Some(start) if start.elapsed() >= DRAIN_TIMEOUT => {
                    Some(nix::sys::signal::Signal::SIGKILL)
                }
                _ => None,
            };
            if let Some(signal) = signal {
                let _ = nix::sys::signal::killpg(nix::unistd::Pid::from_raw(pid as i32), signal);
            }
        }
        std::thread::sleep(Duration::from_millis(10));
    };
    if TERMINATE.load(Ordering::Relaxed) {
        // A shell can exit before a descendant that ignored TERM.
        let _ = nix::sys::signal::killpg(
            nix::unistd::Pid::from_raw(pid as i32),
            nix::sys::signal::Signal::SIGKILL,
        );
    }
    // Report the status before draining: a descendant may hold the slave
    // beyond the caller's startup crash window. A closed stdout is normal.
    emit(&HostEvent::Exited { status });

    // The service is gone, but its last bytes can still sit in the pty
    // buffer. They reach the log and the clients only once the master
    // reports end of file, so the exit frame waits for the pump.
    let _ = drained_rx.recv_timeout(DRAIN_TIMEOUT);

    let completions = {
        let mut session = lock(&session);
        session.exited = Some(status);
        session.broadcast(&Frame::Exit { status });
        session
            .clients
            .iter()
            .map(|client| Arc::clone(&client.done))
            .collect::<Vec<_>>()
    };
    let deadline = Instant::now() + DRAIN_TIMEOUT;
    while completions.iter().any(|done| !done.load(Ordering::Acquire)) && Instant::now() < deadline
    {
        std::thread::sleep(Duration::from_millis(10));
    }
    let _ = std::fs::remove_file(&spec.socket_path);
    tracing::info!(
        "fed host {}: the service exited (status {}), exiting",
        service,
        status
    );

    // The client threads are blocked reading sockets that nothing will write
    // to again. Exiting the process is the whole shutdown.
    process::exit(0);
}

/// A bounded output queue keeps a slow peer out of the pty pump.
struct Client {
    out: mpsc::SyncSender<Vec<u8>>,
    socket: UnixStream,
    done: Arc<AtomicBool>,
}

impl Client {
    fn send(&self, frame: &Frame) -> io::Result<()> {
        self.send_bytes(&encode(frame))
    }

    fn send_bytes(&self, bytes: &[u8]) -> io::Result<()> {
        self.out.try_send(bytes.to_vec()).map_err(|_| {
            let _ = self.socket.shutdown(Shutdown::Both);
            io::Error::new(io::ErrorKind::BrokenPipe, "attach client is not reading")
        })
    }
}

fn client_writer(mut stream: UnixStream, frames: mpsc::Receiver<Vec<u8>>, done: Arc<AtomicBool>) {
    for bytes in frames {
        if stream.write_all(&bytes).is_err() || matches!(bytes.first(), Some(6 | 7)) {
            break;
        }
    }
    let _ = stream.shutdown(Shutdown::Both);
    done.store(true, Ordering::Release);
}

/// The scrollback ring and the connected clients, under one lock.
///
/// A joining client receives the ring and enters the list in the same
/// critical section, so it neither misses nor repeats a chunk that the
/// output pump wrote in between.
#[derive(Default)]
struct Session {
    scrollback: Scrollback,
    clients: Vec<Arc<Client>>,
    exited: Option<i32>,
}

impl Session {
    /// Send one frame to everyone, dropping each client whose write fails.
    fn broadcast(&mut self, frame: &Frame) {
        let bytes = encode(frame);
        self.clients
            .retain(|client| client.send_bytes(&bytes).is_ok());
    }
}

/// A client thread that panics leaves the ring and the list usable. The
/// alternative is taking the whole host, and with it the service, down.
fn lock(session: &Mutex<Session>) -> MutexGuard<'_, Session> {
    session
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// Read the one line `fed start` writes before it closes the host's stdin.
fn read_launch_spec() -> anyhow::Result<Option<LaunchSpec>> {
    if io::stdin().is_terminal() {
        return Ok(None);
    }
    let mut line = String::new();
    io::stdin().lock().read_line(&mut line)?;
    if line.trim().is_empty() {
        return Ok(None);
    }
    Ok(Some(
        LaunchSpec::from_line(&line).context("reading the host launch spec")?,
    ))
}

/// Write one line of the launch handshake to stdout.
fn emit(event: &HostEvent) {
    let Ok(line) = event.to_line() else { return };
    let mut stdout = io::stdout();
    let _ = stdout.write_all(line.as_bytes());
    let _ = stdout.flush();
}

/// Report a failure that happened before the service started, and stop.
fn give_up(error: impl std::fmt::Display) -> ! {
    emit(&HostEvent::Failed {
        error: error.to_string(),
    });
    process::exit(1);
}

/// The same, once the socket exists: nothing will ever answer on it.
fn give_up_and_unlink(socket_path: &Path, error: impl std::fmt::Display) -> ! {
    let _ = std::fs::remove_file(socket_path);
    give_up(error)
}

/// Take the attach socket, or refuse when a live host already has it.
///
/// A host that was killed leaves its socket file behind, and the next start
/// has to be able to bind. Connecting to it tells the two cases apart: a
/// live host accepts, a leftover refuses.
fn bind_socket(path: &Path) -> anyhow::Result<(UnixListener, File)> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(|e| {
            anyhow::anyhow!("creating the directory {} failed: {}", parent.display(), e)
        })?;
        std::fs::set_permissions(parent, Permissions::from_mode(0o700)).map_err(|e| {
            anyhow::anyhow!("setting the mode of {} failed: {}", parent.display(), e)
        })?;
    }
    // Keep the lock inode: unlinking it would let concurrent hosts lock
    // different files while replacing the same socket.
    let ownership = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(path.with_extension("sock.lock"))?;
    fs2::FileExt::try_lock_exclusive(&ownership)
        .context("another fed host owns this attach socket")?;
    if let Ok(metadata) = std::fs::symlink_metadata(path) {
        anyhow::ensure!(
            metadata.file_type().is_socket(),
            "{} is not a socket",
            path.display()
        );
        match UnixStream::connect(path) {
            Ok(_) => anyhow::bail!(
                "another fed host is already listening on {}",
                path.display()
            ),
            Err(error)
                if matches!(
                    error.kind(),
                    io::ErrorKind::ConnectionRefused | io::ErrorKind::NotFound
                ) => {}
            Err(error) => return Err(error).context("checking the existing attach socket"),
        }
        std::fs::remove_file(path)?;
    }
    let listener = UnixListener::bind(path)
        .map_err(|e| anyhow::anyhow!("binding {} failed: {}", path.display(), e))?;
    if let Err(error) = std::fs::set_permissions(path, Permissions::from_mode(0o600)) {
        let _ = std::fs::remove_file(path);
        return Err(error).context("setting the attach socket permissions");
    }
    Ok((listener, ownership))
}

/// The same append-mode file the nohup path writes, so `fed logs` works the
/// same for a hosted service.
fn open_log(path: &Path) -> anyhow::Result<File> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(|e| {
            anyhow::anyhow!("creating the directory {} failed: {}", parent.display(), e)
        })?;
    }
    OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .map_err(|e| anyhow::anyhow!("opening the log file {} failed: {}", path.display(), e))
}

/// Read the pty until the service closes it.
///
/// Append each chunk before broadcasting it. A failed log sink is disabled
/// after one diagnostic; attached clients can still receive the output.
fn pump_output(master: &File, log: File, session: &Mutex<Session>) {
    let mut log = Some(log);
    let mut buf = [0u8; READ_CHUNK];
    loop {
        let chunk = match (&mut &*master).read(&mut buf) {
            Ok(0) => break,
            Ok(n) => &buf[..n],
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            // Linux reports the slave's last holder exiting as EIO.
            Err(error) if error.raw_os_error() == Some(nix::libc::EIO) => break,
            Err(error) => {
                tracing::error!(%error, "reading the hosted terminal failed; stopping the service");
                TERMINATE.store(true, Ordering::Relaxed);
                break;
            }
        };
        if let Some(file) = log.as_mut()
            && let Err(error) = file.write_all(chunk)
        {
            tracing::error!(%error, "writing the service log failed; logging is disabled for this host");
            log = None;
        }

        let mut session = lock(session);
        session.scrollback.push(chunk);
        session.broadcast(&Frame::Output(chunk.to_vec()));
    }
}

/// Give every connection its own thread.
fn accept_clients(listener: UnixListener, master: &Arc<File>, session: &Arc<Mutex<Session>>) {
    for stream in listener.incoming() {
        let Ok(stream) = stream else { continue };
        let master = Arc::clone(master);
        let session = Arc::clone(session);
        std::thread::spawn(move || serve_client(stream, &master, &session));
    }
}

/// One attach session: a hello, the scrollback, then keystrokes and resizes
/// until the client goes away.
fn serve_client(stream: UnixStream, master: &File, session: &Mutex<Session>) {
    let _ = stream.set_write_timeout(Some(CLIENT_WRITE_TIMEOUT));
    let Ok(write_half) = stream.try_clone() else {
        return;
    };
    let (out, receiver) = mpsc::sync_channel(256);
    let done = Arc::new(AtomicBool::new(false));
    let Ok(socket) = stream.try_clone() else {
        return;
    };
    let client = Arc::new(Client {
        out,
        socket,
        done: Arc::clone(&done),
    });
    std::thread::spawn(move || client_writer(write_half, receiver, done));
    let mut frames = FrameReader::new(stream);

    let Some(hello) = greet(&mut frames, &client) else {
        return;
    };
    let _ = set_window_size(master, hello.cols, hello.rows);
    {
        let mut session = lock(session);
        let recent = session.scrollback.contents();
        if client.send(&Frame::Scrollback(recent)).is_err() {
            return;
        }
        if let Some(status) = session.exited {
            let _ = client.send(&Frame::Exit { status });
            return;
        }
        session.clients.push(Arc::clone(&client));
    }

    let mut forwards_input = hello.flags & FLAG_NO_STDIN == 0;
    loop {
        match frames.read_frame() {
            Ok(Frame::Input(bytes)) => {
                if forwards_input && (&mut &*master).write_all(&bytes).is_err() {
                    // The slave may have closed while input was blocked.
                    // Keep receiving the final output and Exit frame.
                    forwards_input = false;
                }
            }
            Ok(Frame::Resize { cols, rows }) => {
                let _ = set_window_size(master, cols, rows);
            }
            Ok(_) => {
                tracing::debug!("an attach client sent a frame only a host sends, closing it");
                break;
            }
            Err(ProtocolError::Closed) => break,
            Err(e) => {
                tracing::debug!("an attach connection failed: {}", e);
                break;
            }
        }
    }

    lock(session)
        .clients
        .retain(|other| !Arc::ptr_eq(other, &client));
}

/// What a client says about its terminal when it connects.
struct Hello {
    cols: u16,
    rows: u16,
    flags: u8,
}

/// Read the first frame, which must be a hello from a client that speaks
/// this version of the protocol.
fn greet(frames: &mut FrameReader<UnixStream>, client: &Client) -> Option<Hello> {
    match frames.read_frame() {
        Ok(Frame::Hello {
            version,
            cols,
            rows,
            flags,
        }) if version == VERSION => Some(Hello { cols, rows, flags }),
        Ok(Frame::Hello { version, .. }) => {
            let _ = client.send(&Frame::Error(format!(
                "the host speaks version {} of the attach protocol and this client speaks {}",
                VERSION, version
            )));
            None
        }
        _ => None,
    }
}
