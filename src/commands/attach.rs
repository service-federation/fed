//! `fed attach <service>`: connect this terminal to a hosted service.
//!
//! Prototype only. Puts the terminal in raw mode, pumps raw bytes to and from
//! the host's unix socket, and detaches on Ctrl-P Ctrl-Q.

use crossterm::terminal::{disable_raw_mode, enable_raw_mode};
use fed::fed_dir::attach_socket_path;
use std::io::{Read, Write};
use std::os::unix::net::UnixStream;
use std::path::Path;

const CTRL_P: u8 = 0x10;
const CTRL_Q: u8 = 0x11;

pub fn run_attach(work_dir: &Path, service: &str) -> anyhow::Result<()> {
    let socket_path = attach_socket_path(work_dir, service);
    let socket = match UnixStream::connect(&socket_path) {
        Ok(socket) => socket,
        Err(e) => {
            eprintln!(
                "'{}' is not attachable: nothing is listening on {} ({}). Start its host with `fed host {}`.",
                service,
                socket_path.display(),
                e,
                service
            );
            std::process::exit(1);
        }
    };

    enter_raw_mode()?;
    eprintln!("attached to {}, detach with ctrl-p ctrl-q\r", service);

    let reader = socket.try_clone()?;
    std::thread::spawn(move || {
        pump_to_stdout(reader);
        // The host is gone, so there is nothing left to send it either.
        leave_raw_mode();
        std::process::exit(0);
    });

    pump_stdin(&socket);
    leave_raw_mode();
    Ok(())
}

fn pump_to_stdout(mut socket: UnixStream) {
    let mut buf = [0u8; 4096];
    let mut stdout = std::io::stdout();
    loop {
        match socket.read(&mut buf) {
            Ok(0) | Err(_) => break,
            Ok(n) => {
                if stdout.write_all(&buf[..n]).is_err() || stdout.flush().is_err() {
                    break;
                }
            }
        }
    }
}

/// Forward stdin until the detach sequence arrives, the terminal closes, or
/// the host goes away.
fn pump_stdin(socket: &UnixStream) {
    let mut detector = DetachDetector::default();
    let mut buf = [0u8; 1024];
    let mut stdin = std::io::stdin();
    loop {
        let n = match stdin.read(&mut buf) {
            Ok(0) | Err(_) => break,
            Ok(n) => n,
        };
        let scan = detector.scan(&buf[..n]);
        if !scan.forward.is_empty() && (&mut &*socket).write_all(&scan.forward).is_err() {
            break;
        }
        if scan.detach {
            let _ = socket.shutdown(std::net::Shutdown::Both);
            break;
        }
    }
}

fn enter_raw_mode() -> anyhow::Result<()> {
    enable_raw_mode()?;
    // A panic between here and `leave_raw_mode` would otherwise leave the
    // user's terminal with no echo and no line editing.
    let previous = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        leave_raw_mode();
        previous(info);
    }));
    Ok(())
}

fn leave_raw_mode() {
    let _ = disable_raw_mode();
}

/// Watches the input stream for Ctrl-P Ctrl-Q.
///
/// A Ctrl-P is held back until the next byte decides whether it was a detach
/// prefix; if it was not, both bytes are forwarded in order, so a program
/// that uses Ctrl-P itself still receives it.
#[derive(Default)]
struct DetachDetector {
    saw_prefix: bool,
}

struct Scan {
    forward: Vec<u8>,
    detach: bool,
}

impl DetachDetector {
    fn scan(&mut self, input: &[u8]) -> Scan {
        let mut forward = Vec::with_capacity(input.len() + 1);
        for &byte in input {
            match (self.saw_prefix, byte) {
                (true, CTRL_Q) => {
                    self.saw_prefix = false;
                    return Scan {
                        forward,
                        detach: true,
                    };
                }
                (true, CTRL_P) => forward.push(CTRL_P),
                (true, other) => {
                    self.saw_prefix = false;
                    forward.push(CTRL_P);
                    forward.push(other);
                }
                (false, CTRL_P) => self.saw_prefix = true,
                (false, other) => forward.push(other),
            }
        }
        Scan {
            forward,
            detach: false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plain_input_passes_through() {
        let mut d = DetachDetector::default();
        let scan = d.scan(b"marker-7\n");
        assert_eq!(scan.forward, b"marker-7\n");
        assert!(!scan.detach);
    }

    #[test]
    fn detach_sequence_is_swallowed() {
        let mut d = DetachDetector::default();
        let scan = d.scan(&[b'a', CTRL_P, CTRL_Q, b'b']);
        assert_eq!(scan.forward, b"a");
        assert!(scan.detach);
    }

    #[test]
    fn detach_sequence_split_across_reads() {
        let mut d = DetachDetector::default();
        let first = d.scan(&[CTRL_P]);
        assert!(first.forward.is_empty());
        assert!(!first.detach);
        assert!(d.scan(&[CTRL_Q]).detach);
    }

    #[test]
    fn partial_match_is_forwarded_in_order() {
        let mut d = DetachDetector::default();
        let scan = d.scan(&[CTRL_P, b'x']);
        assert_eq!(scan.forward, vec![CTRL_P, b'x']);
        assert!(!scan.detach);
    }

    #[test]
    fn repeated_prefix_forwards_all_but_the_last() {
        let mut d = DetachDetector::default();
        let scan = d.scan(&[CTRL_P, CTRL_P, CTRL_P]);
        assert_eq!(scan.forward, vec![CTRL_P, CTRL_P]);
        assert!(!scan.detach);
        assert!(d.scan(&[CTRL_Q]).detach);
    }
}
