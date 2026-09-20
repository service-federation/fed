use std::io::{IsTerminal, Read, Write};
use std::net::Shutdown;
use std::os::unix::net::UnixStream;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{Context, bail};
use crossterm::terminal::{disable_raw_mode, enable_raw_mode, size};
use fed::attach::detach::{DetachDetector, DetachKeys};
use fed::attach::protocol::{FLAG_NO_STDIN, Frame, FrameReader, VERSION, encode};
use rusqlite::OptionalExtension;
use tokio::signal::unix::{SignalKind, signal};

struct TerminalGuard;
impl Drop for TerminalGuard {
    fn drop(&mut self) {
        let _ = disable_raw_mode();
    }
}

enum Event {
    Frame(Frame),
    Detached,
    Failed(String),
}

type Writer = Arc<Mutex<UnixStream>>;

fn send(writer: &Writer, frame: Frame) -> anyhow::Result<()> {
    writer
        .lock()
        .map_err(|_| anyhow::anyhow!("attach writer panicked"))?
        .write_all(&encode(&frame))?;
    Ok(())
}

/// Attach using persisted state without loading config or resolving secrets.
pub async fn run_attach(
    work_dir: PathBuf,
    service: &str,
    no_stdin: bool,
    detach_keys: &str,
) -> anyhow::Result<i32> {
    let keys = DetachKeys::parse(detach_keys)?;
    let not_running = || {
        anyhow::anyhow!("Service '{service}' is not running. Start it with: fed start {service}")
    };
    if !work_dir.join(".fed/lock.db").exists() {
        return Err(not_running());
    }
    let connection = tokio_rusqlite::Connection::open_with_flags(
        work_dir.join(".fed/lock.db"),
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
    )
    .await?;
    let id = service.to_owned();
    let row = connection.call(move |db| {
        let has_socket: bool = db.query_row(
            "SELECT EXISTS(SELECT 1 FROM pragma_table_info('services') WHERE name = 'attach_socket')",
            [], |row| row.get(0),
        )?;
        let query = if has_socket {
            "SELECT status, attach_socket FROM services WHERE id = ?1"
        } else {
            "SELECT status, NULL FROM services WHERE id = ?1"
        };
        Ok(db.query_row(query, [id], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, Option<String>>(1)?))
        }).optional()?)
    }).await?;
    connection.close().await?;
    let (status, socket) = row.ok_or_else(not_running)?;
    if !matches!(
        status.as_str(),
        "running" | "healthy" | "failing" | "starting"
    ) {
        return Err(not_running());
    }
    let socket = socket.ok_or_else(|| anyhow::anyhow!(
        "Service '{service}' is not attachable. Give it `tty: true` in fed.yaml to run it under a terminal, or follow its output with: fed logs -f {service}"
    ))?;
    let stream = UnixStream::connect(socket).map_err(|_| anyhow::anyhow!(
        "Service '{service}' has no host to attach to. Its state is stale; restart it with: fed restart {service}"
    ))?;
    stream.set_read_timeout(Some(Duration::from_secs(10)))?;
    stream.set_write_timeout(Some(Duration::from_secs(2)))?;
    let writer = Arc::new(Mutex::new(stream.try_clone()?));
    // Register handlers before changing terminal attributes.
    let mut terminate = signal(SignalKind::terminate())?;
    let mut hangup = signal(SignalKind::hangup())?;
    let mut interrupt = signal(SignalKind::interrupt())?;
    let mut resize = signal(SignalKind::window_change())?;
    let previous_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        let _ = disable_raw_mode();
        previous_hook(info);
    }));
    if !no_stdin && !std::io::stdin().is_terminal() {
        bail!("fed attach needs a terminal on stdin; use --no-stdin to follow output.");
    }
    if no_stdin {
        eprintln!("Attached to {service}. Stop following with Ctrl+C.");
    } else {
        eprintln!(
            "Attached to {service}. Detach with {}.",
            detach_keys.replace(',', " ")
        );
        enable_raw_mode().context("could not put the terminal in raw mode")?;
    }
    let terminal = TerminalGuard;
    let (cols, rows) = size().unwrap_or((80, 24));
    send(
        &writer,
        Frame::Hello {
            version: VERSION,
            cols,
            rows,
            flags: if no_stdin { FLAG_NO_STDIN } else { 0 },
        },
    )?;
    let (tx, mut input_rx) = tokio::sync::mpsc::unbounded_channel();
    let (output_tx, mut rx) = tokio::sync::mpsc::channel(16);
    std::thread::spawn(move || {
        let timeout_socket = stream.try_clone().ok();
        let mut reader = FrameReader::new(stream);
        loop {
            match reader.read_frame() {
                Ok(frame) => {
                    if matches!(frame, Frame::Scrollback(_))
                        && let Some(socket) = &timeout_socket
                    {
                        let _ = socket.set_read_timeout(None);
                    }
                    let done = matches!(frame, Frame::Exit { .. } | Frame::Error(_));
                    if output_tx.blocking_send(Event::Frame(frame)).is_err() || done {
                        break;
                    }
                }
                Err(error) => {
                    let _ = output_tx.blocking_send(Event::Failed(error.to_string()));
                    break;
                }
            }
        }
    });
    if !no_stdin {
        let input_writer = writer.clone();
        std::thread::spawn(move || {
            let mut detector = DetachDetector::new(keys);
            let mut buffer = [0; 4096];
            loop {
                match std::io::stdin().read(&mut buffer) {
                    Ok(0) => {
                        let _ = tx.send(Event::Detached);
                        break;
                    }
                    Ok(count) => {
                        let scan = detector.scan(&buffer[..count]);
                        if !scan.forward.is_empty()
                            && let Err(error) = send(&input_writer, Frame::Input(scan.forward))
                        {
                            let _ = tx.send(Event::Failed(error.to_string()));
                            break;
                        }
                        if scan.detach {
                            let _ = tx.send(Event::Detached);
                            break;
                        }
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
                    Err(error) => {
                        let _ = tx.send(Event::Failed(error.to_string()));
                        break;
                    }
                }
            }
        });
    }
    let result = async {
        loop {
            tokio::select! {
                biased;
                _ = terminate.recv() => return Ok(1),
                _ = hangup.recv() => return Ok(1),
                _ = interrupt.recv() => return Ok(if no_stdin { 0 } else { 1 }),
                _ = resize.recv() => {
                    if let Ok((cols, rows)) = size() { send(&writer, Frame::Resize { cols, rows })?; }
                }
                Some(event) = input_rx.recv() => match event {
                    Event::Detached => return Ok(0),
                    Event::Failed(error) => bail!(error),
                    Event::Frame(_) => unreachable!("input pump only sends control events"),
                },
                event = rx.recv() => match event {
                    Some(Event::Frame(Frame::Output(bytes) | Frame::Scrollback(bytes))) => {
                        let mut stdout = std::io::stdout().lock();
                        stdout.write_all(&bytes)?;
                        stdout.flush()?;
                    }
                    Some(Event::Frame(Frame::Exit { status })) => {
                        let code = exit_code(status);
                        drop(terminal);
                        eprintln!("{service} exited (code {code})");
                        return Ok(code);
                    }
                    Some(Event::Detached) => return Ok(0),
                    Some(Event::Failed(error) | Event::Frame(Frame::Error(error))) => bail!(error),
                    Some(Event::Frame(_)) => bail!("unexpected frame from attach host"),
                    None => bail!("the attach connection closed"),
                }
            }
        }
    }.await;
    if let Ok(socket) = writer.lock() {
        let _ = socket.shutdown(Shutdown::Both);
    }
    result
}

fn exit_code(status: i32) -> i32 {
    if nix::libc::WIFEXITED(status) {
        nix::libc::WEXITSTATUS(status)
    } else if nix::libc::WIFSIGNALED(status) {
        128 + nix::libc::WTERMSIG(status)
    } else {
        1
    }
}
