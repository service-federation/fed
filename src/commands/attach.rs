use std::io::{IsTerminal, Read, Write};
use std::net::Shutdown;
use std::os::fd::AsFd;
use std::os::unix::net::UnixStream;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
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

enum WriteRequest {
    Frame(Frame),
    Detach,
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
    let mut writer = stream.try_clone()?;
    let shutdown = stream.try_clone()?;
    let mut stdout = std::fs::File::from(std::io::stdout().as_fd().try_clone_to_owned()?);
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
    let (events, mut controls) = tokio::sync::mpsc::unbounded_channel();
    let (input, outgoing) = std::sync::mpsc::sync_channel(16);
    let dimensions = Arc::new(AtomicU32::new((u32::from(cols) << 16) | u32::from(rows)));
    let writer_dimensions = dimensions.clone();
    let writer_events = events.clone();
    std::thread::spawn(move || {
        let result = (|| -> anyhow::Result<()> {
            writer.write_all(&encode(&Frame::Hello {
                version: VERSION,
                cols,
                rows,
                flags: if no_stdin { FLAG_NO_STDIN } else { 0 },
            }))?;
            let mut previous_size = (u32::from(cols) << 16) | u32::from(rows);
            loop {
                let current_size = writer_dimensions.load(Ordering::Relaxed);
                if current_size != previous_size {
                    writer.write_all(&encode(&Frame::Resize {
                        cols: (current_size >> 16) as u16,
                        rows: current_size as u16,
                    }))?;
                    previous_size = current_size;
                }
                match outgoing.recv_timeout(Duration::from_millis(50)) {
                    Ok(WriteRequest::Frame(frame)) => writer.write_all(&encode(&frame))?,
                    Ok(WriteRequest::Detach) => {
                        let _ = writer_events.send(Event::Detached);
                        return Ok(());
                    }
                    Err(std::sync::mpsc::RecvTimeoutError::Timeout) => continue,
                    Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => return Ok(()),
                }
            }
        })();
        if let Err(error) = result {
            let _ = writer_events.send(Event::Failed(error.to_string()));
        }
    });
    let (output_tx, mut output_rx) = tokio::sync::mpsc::channel(16);
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
    let output_events = events.clone();
    std::thread::spawn(move || {
        while let Some(event) = output_rx.blocking_recv() {
            match event {
                Event::Frame(Frame::Output(bytes) | Frame::Scrollback(bytes)) => {
                    if let Err(error) = stdout.write_all(&bytes).and_then(|()| stdout.flush()) {
                        let _ = output_events.send(Event::Failed(error.to_string()));
                        return;
                    }
                }
                // Delivery acknowledges that every preceding output frame was flushed.
                event => {
                    let _ = output_events.send(event);
                    return;
                }
            }
        }
        let _ = output_events.send(Event::Failed("the attach connection closed".into()));
    });
    if !no_stdin {
        let input = input.clone();
        std::thread::spawn(move || {
            let mut detector = DetachDetector::new(keys);
            let mut buffer = [0; 4096];
            loop {
                match std::io::stdin().read(&mut buffer) {
                    Ok(0) => {
                        let _ = input.send(WriteRequest::Detach);
                        break;
                    }
                    Ok(count) => {
                        let scan = detector.scan(&buffer[..count]);
                        if !scan.forward.is_empty()
                            && input
                                .send(WriteRequest::Frame(Frame::Input(scan.forward)))
                                .is_err()
                        {
                            break;
                        }
                        if scan.detach {
                            let _ = input.send(WriteRequest::Detach);
                            break;
                        }
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
                    Err(error) => {
                        let _ = events.send(Event::Failed(error.to_string()));
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
                    if let Ok((cols, rows)) = size() {
                        dimensions.store((u32::from(cols) << 16) | u32::from(rows), Ordering::Relaxed);
                    }
                }
                event = controls.recv() => match event {
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
    let _ = shutdown.shutdown(Shutdown::Both);
    drop(input);
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
