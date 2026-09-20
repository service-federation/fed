//! The frames that travel over the attach socket.
//!
//! ```text
//! frame := kind u8, len u32 le, payload[len]
//! kind  := 1 Hello      client -> host   { version u8, cols u16, rows u16, flags u8 }
//!          2 Input      client -> host   bytes
//!          3 Output     host -> client   bytes
//!          4 Resize     client -> host   { cols u16, rows u16 }
//!          5 Scrollback host -> client   bytes, once, right after Hello
//!          6 Exit       host -> client   { status i32 }
//!          7 Error      host -> client   utf-8 text
//! ```
//!
//! All integers are little endian. The host and the client normally come from
//! the same binary, but a host outlives an upgrade, so [`Frame::Hello`]
//! carries a version and a host that receives a different one answers
//! [`Frame::Error`] and closes.

use std::io::Read;
use thiserror::Error;

/// The protocol this binary speaks.
pub const VERSION: u8 = 1;

/// Longest payload a frame may carry. A larger one is a protocol error.
pub const MAX_PAYLOAD_LEN: usize = 1024 * 1024;

/// Bit 0 of [`Frame::Hello`]'s flags: the client does not forward its stdin.
pub const FLAG_NO_STDIN: u8 = 1;

mod kind {
    pub const HELLO: u8 = 1;
    pub const INPUT: u8 = 2;
    pub const OUTPUT: u8 = 3;
    pub const RESIZE: u8 = 4;
    pub const SCROLLBACK: u8 = 5;
    pub const EXIT: u8 = 6;
    pub const ERROR: u8 = 7;
}

/// One message on the attach socket.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Frame {
    /// First frame of a session, from the client.
    Hello {
        /// The client's protocol version.
        version: u8,
        /// Terminal width in columns.
        cols: u16,
        /// Terminal height in rows.
        rows: u16,
        /// See [`FLAG_NO_STDIN`].
        flags: u8,
    },
    /// Keystrokes, from the client.
    Input(Vec<u8>),
    /// Service output, from the host.
    Output(Vec<u8>),
    /// A new terminal size, from the client.
    Resize {
        /// Terminal width in columns.
        cols: u16,
        /// Terminal height in rows.
        rows: u16,
    },
    /// Recent output, sent once right after [`Frame::Hello`].
    Scrollback(Vec<u8>),
    /// The service is gone. The host closes after this.
    Exit {
        /// The raw `waitpid` status, so a signal survives.
        status: i32,
    },
    /// The host refuses the session. It closes after this.
    Error(String),
}

/// Why a byte stream is not a sequence of frames.
#[derive(Debug, Error)]
pub enum ProtocolError {
    /// The peer closed the connection between two frames.
    #[error("the attach connection closed")]
    Closed,
    /// The peer closed the connection part way through a frame.
    #[error("the attach connection closed in the middle of a frame")]
    Truncated,
    /// A length header is over [`MAX_PAYLOAD_LEN`].
    #[error("a frame claims {len} bytes, over the {MAX_PAYLOAD_LEN} byte limit")]
    TooLarge {
        /// The length the header claimed.
        len: u32,
    },
    /// A kind byte that version 1 does not define.
    #[error("frame kind {kind} is not part of version {VERSION} of the attach protocol")]
    UnknownKind {
        /// The kind byte received.
        kind: u8,
    },
    /// A fixed-size frame arrived with fewer bytes than its fields need.
    #[error("frame kind {kind} carries {len} bytes, too few for its fields")]
    ShortPayload {
        /// The kind byte received.
        kind: u8,
        /// How many payload bytes arrived.
        len: usize,
    },
    /// An [`Frame::Error`] payload that is not utf-8.
    #[error("an error frame carries bytes that are not utf-8")]
    NotUtf8,
    /// The socket itself failed.
    #[error("reading the attach connection failed: {0}")]
    Io(#[from] std::io::Error),
}

/// Turn a frame into the bytes that go on the wire.
pub fn encode(frame: &Frame) -> Vec<u8> {
    let (kind, payload) = match frame {
        Frame::Hello {
            version,
            cols,
            rows,
            flags,
        } => {
            let mut payload = Vec::with_capacity(6);
            payload.push(*version);
            payload.extend_from_slice(&cols.to_le_bytes());
            payload.extend_from_slice(&rows.to_le_bytes());
            payload.push(*flags);
            (kind::HELLO, payload)
        }
        Frame::Input(bytes) => (kind::INPUT, bytes.clone()),
        Frame::Output(bytes) => (kind::OUTPUT, bytes.clone()),
        Frame::Resize { cols, rows } => {
            let mut payload = Vec::with_capacity(4);
            payload.extend_from_slice(&cols.to_le_bytes());
            payload.extend_from_slice(&rows.to_le_bytes());
            (kind::RESIZE, payload)
        }
        Frame::Scrollback(bytes) => (kind::SCROLLBACK, bytes.clone()),
        Frame::Exit { status } => (kind::EXIT, status.to_le_bytes().to_vec()),
        Frame::Error(text) => (kind::ERROR, text.as_bytes().to_vec()),
    };

    let mut out = Vec::with_capacity(5 + payload.len());
    out.push(kind);
    out.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    out.extend_from_slice(&payload);
    out
}

/// Reads frames off a byte stream, one at a time.
pub struct FrameReader<R> {
    stream: R,
}

impl<R: Read> FrameReader<R> {
    /// Wrap a stream. Nothing is read until [`FrameReader::read_frame`].
    pub fn new(stream: R) -> Self {
        Self { stream }
    }

    /// Read the next frame.
    ///
    /// A clean end of stream gives [`ProtocolError::Closed`]. Every other
    /// error means the connection cannot be used any further, so callers
    /// close it.
    pub fn read_frame(&mut self) -> Result<Frame, ProtocolError> {
        let mut header = [0u8; 5];
        match self.stream.read(&mut header[..1])? {
            0 => return Err(ProtocolError::Closed),
            _ => self.fill(&mut header[1..])?,
        }

        let kind = header[0];
        let len = u32::from_le_bytes([header[1], header[2], header[3], header[4]]);
        if len as usize > MAX_PAYLOAD_LEN {
            return Err(ProtocolError::TooLarge { len });
        }
        if !matches!(
            kind,
            kind::HELLO
                | kind::INPUT
                | kind::OUTPUT
                | kind::RESIZE
                | kind::SCROLLBACK
                | kind::EXIT
                | kind::ERROR
        ) {
            return Err(ProtocolError::UnknownKind { kind });
        }

        let mut payload = vec![0u8; len as usize];
        self.fill(&mut payload)?;
        decode(kind, payload)
    }

    /// Read exactly `buf.len()` bytes, or report a truncated frame.
    fn fill(&mut self, buf: &mut [u8]) -> Result<(), ProtocolError> {
        match self.stream.read_exact(buf) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                Err(ProtocolError::Truncated)
            }
            Err(e) => Err(ProtocolError::Io(e)),
        }
    }
}

fn decode(kind: u8, payload: Vec<u8>) -> Result<Frame, ProtocolError> {
    let short = |len: usize| ProtocolError::ShortPayload { kind, len };
    match kind {
        kind::HELLO => {
            if payload.len() < 6 {
                return Err(short(payload.len()));
            }
            Ok(Frame::Hello {
                version: payload[0],
                cols: u16::from_le_bytes([payload[1], payload[2]]),
                rows: u16::from_le_bytes([payload[3], payload[4]]),
                flags: payload[5],
            })
        }
        kind::INPUT => Ok(Frame::Input(payload)),
        kind::OUTPUT => Ok(Frame::Output(payload)),
        kind::RESIZE => {
            if payload.len() < 4 {
                return Err(short(payload.len()));
            }
            Ok(Frame::Resize {
                cols: u16::from_le_bytes([payload[0], payload[1]]),
                rows: u16::from_le_bytes([payload[2], payload[3]]),
            })
        }
        kind::SCROLLBACK => Ok(Frame::Scrollback(payload)),
        kind::EXIT => {
            if payload.len() < 4 {
                return Err(short(payload.len()));
            }
            Ok(Frame::Exit {
                status: i32::from_le_bytes([payload[0], payload[1], payload[2], payload[3]]),
            })
        }
        kind::ERROR => String::from_utf8(payload)
            .map(Frame::Error)
            .map_err(|_| ProtocolError::NotUtf8),
        _ => Err(ProtocolError::UnknownKind { kind }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn read_one(bytes: &[u8]) -> Result<Frame, ProtocolError> {
        FrameReader::new(bytes).read_frame()
    }

    fn any_frame() -> impl Strategy<Value = Frame> {
        let payload = prop::collection::vec(any::<u8>(), 0..512);
        prop_oneof![
            (any::<u8>(), any::<u16>(), any::<u16>(), any::<u8>()).prop_map(
                |(version, cols, rows, flags)| Frame::Hello {
                    version,
                    cols,
                    rows,
                    flags
                }
            ),
            payload.clone().prop_map(Frame::Input),
            payload.clone().prop_map(Frame::Output),
            (any::<u16>(), any::<u16>()).prop_map(|(cols, rows)| Frame::Resize { cols, rows }),
            payload.prop_map(Frame::Scrollback),
            any::<i32>().prop_map(|status| Frame::Exit { status }),
            ".*".prop_map(Frame::Error),
        ]
    }

    proptest! {
        #[test]
        fn every_frame_survives_a_round_trip(frame in any_frame()) {
            prop_assert_eq!(read_one(&encode(&frame)).unwrap(), frame);
        }
    }

    #[test]
    fn several_frames_decode_in_order() {
        let sent = vec![
            Frame::Hello {
                version: VERSION,
                cols: 80,
                rows: 24,
                flags: FLAG_NO_STDIN,
            },
            Frame::Input(b"ls\n".to_vec()),
            Frame::Exit { status: 256 },
        ];
        let mut wire = Vec::new();
        for frame in &sent {
            wire.extend_from_slice(&encode(frame));
        }

        let mut reader = FrameReader::new(&wire[..]);
        for frame in &sent {
            assert_eq!(&reader.read_frame().unwrap(), frame);
        }
        assert!(matches!(reader.read_frame(), Err(ProtocolError::Closed)));
    }

    #[test]
    fn a_length_over_the_cap_is_rejected() {
        let mut wire = vec![3u8];
        wire.extend_from_slice(&((MAX_PAYLOAD_LEN as u32) + 1).to_le_bytes());
        match read_one(&wire) {
            Err(ProtocolError::TooLarge { len }) => {
                assert_eq!(len as usize, MAX_PAYLOAD_LEN + 1)
            }
            other => panic!("expected an oversize frame error, got {:?}", other),
        }
    }

    #[test]
    fn an_unknown_kind_is_rejected() {
        let wire = [9u8, 0, 0, 0, 0];
        match read_one(&wire) {
            Err(ProtocolError::UnknownKind { kind }) => assert_eq!(kind, 9),
            other => panic!("expected an unknown kind error, got {:?}", other),
        }
    }

    #[test]
    fn a_payload_too_short_for_its_kind_is_rejected() {
        let wire = [1u8, 2, 0, 0, 0, 7, 7];
        match read_one(&wire) {
            Err(ProtocolError::ShortPayload { kind, len }) => {
                assert_eq!((kind, len), (1, 2))
            }
            other => panic!("expected a short payload error, got {:?}", other),
        }
    }

    #[test]
    fn a_stream_cut_inside_a_frame_is_reported_as_truncated() {
        let full = encode(&Frame::Output(b"hello".to_vec()));
        for cut in 1..full.len() {
            assert!(
                matches!(read_one(&full[..cut]), Err(ProtocolError::Truncated)),
                "a stream cut after {} bytes must read as truncated",
                cut
            );
        }
    }
}
