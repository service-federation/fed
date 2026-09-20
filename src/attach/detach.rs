//! The two-key sequence that ends an attach session.
//!
//! A client sends everything the user types to the host, except this
//! sequence. Ctrl-P Ctrl-Q by default, the same pair docker uses.

use thiserror::Error;

/// The two keys that detach, as the bytes a terminal sends for them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DetachKeys {
    first: u8,
    second: u8,
}

/// Why a `--detach-keys` value was refused.
#[derive(Debug, Error, PartialEq, Eq)]
#[error("--detach-keys needs two ctrl keys, like ctrl-p,ctrl-q. It received '{input}'.")]
pub struct DetachKeysError {
    /// What the user passed.
    pub input: String,
}

impl Default for DetachKeys {
    fn default() -> Self {
        Self {
            first: ctrl(b'p'),
            second: ctrl(b'q'),
        }
    }
}

impl DetachKeys {
    /// Read a pair like `ctrl-p,ctrl-q`.
    ///
    /// Only `ctrl-<a-z>` pairs are accepted. docker allows more, but every
    /// other form it takes maps to a byte a terminal rarely sends, and a
    /// wrong guess here silently swallows a keystroke.
    pub fn parse(input: &str) -> Result<Self, DetachKeysError> {
        let refuse = || DetachKeysError {
            input: input.to_string(),
        };
        let (first, second) = input.split_once(',').ok_or_else(refuse)?;
        Ok(Self {
            first: parse_ctrl_key(first).ok_or_else(refuse)?,
            second: parse_ctrl_key(second).ok_or_else(refuse)?,
        })
    }
}

/// `ctrl-p` gives 0x10, the byte a terminal sends for Ctrl-P.
fn parse_ctrl_key(key: &str) -> Option<u8> {
    let letter = key.strip_prefix("ctrl-")?.as_bytes();
    match letter {
        [c] if c.is_ascii_lowercase() => Some(ctrl(*c)),
        _ => None,
    }
}

const fn ctrl(letter: u8) -> u8 {
    letter - b'a' + 1
}

/// Watches the input stream for the detach sequence.
///
/// The first key is held back until the next byte decides whether it was a
/// detach prefix; if it was not, both bytes are forwarded in order, so a
/// program that uses Ctrl-P itself still receives it.
#[derive(Debug, Default)]
pub struct DetachDetector {
    keys: DetachKeys,
    saw_prefix: bool,
}

/// What to do with one read from the terminal.
#[derive(Debug, PartialEq, Eq)]
pub struct Scan {
    /// The bytes to send to the host, in the order they were typed.
    pub forward: Vec<u8>,
    /// The user asked to detach. Bytes typed after the sequence are dropped.
    pub detach: bool,
}

impl DetachDetector {
    /// Watch for `keys` instead of the default pair.
    pub fn new(keys: DetachKeys) -> Self {
        Self {
            keys,
            saw_prefix: false,
        }
    }

    /// Split one read into the bytes the host should receive and whether the
    /// user detached.
    pub fn scan(&mut self, input: &[u8]) -> Scan {
        let mut forward = Vec::with_capacity(input.len() + 1);
        for &byte in input {
            match (self.saw_prefix, byte) {
                (true, b) if b == self.keys.second => {
                    self.saw_prefix = false;
                    return Scan {
                        forward,
                        detach: true,
                    };
                }
                (true, b) if b == self.keys.first => forward.push(self.keys.first),
                (true, b) => {
                    self.saw_prefix = false;
                    forward.push(self.keys.first);
                    forward.push(b);
                }
                (false, b) if b == self.keys.first => self.saw_prefix = true,
                (false, b) => forward.push(b),
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

    const CTRL_P: u8 = 0x10;
    const CTRL_Q: u8 = 0x11;

    #[test]
    fn plain_input_passes_through() {
        let mut detector = DetachDetector::default();
        let scan = detector.scan(b"marker-7\n");
        assert_eq!(scan.forward, b"marker-7\n");
        assert!(!scan.detach);
    }

    #[test]
    fn the_detach_sequence_is_swallowed() {
        let mut detector = DetachDetector::default();
        let scan = detector.scan(&[b'a', CTRL_P, CTRL_Q, b'b']);
        assert_eq!(scan.forward, b"a");
        assert!(scan.detach);
    }

    #[test]
    fn the_detach_sequence_survives_a_split_read() {
        let mut detector = DetachDetector::default();
        let first = detector.scan(&[CTRL_P]);
        assert!(first.forward.is_empty());
        assert!(!first.detach);
        assert!(detector.scan(&[CTRL_Q]).detach);
    }

    #[test]
    fn a_partial_match_is_forwarded_in_order() {
        let mut detector = DetachDetector::default();
        let scan = detector.scan(&[CTRL_P, b'x']);
        assert_eq!(scan.forward, vec![CTRL_P, b'x']);
        assert!(!scan.detach);
    }

    #[test]
    fn a_repeated_prefix_forwards_all_but_the_last() {
        let mut detector = DetachDetector::default();
        let scan = detector.scan(&[CTRL_P, CTRL_P, CTRL_P]);
        assert_eq!(scan.forward, vec![CTRL_P, CTRL_P]);
        assert!(!scan.detach);
        assert!(detector.scan(&[CTRL_Q]).detach);
    }

    #[test]
    fn a_custom_pair_replaces_the_default_one() {
        let keys = DetachKeys::parse("ctrl-a,ctrl-d").unwrap();
        let mut detector = DetachDetector::new(keys);

        assert_eq!(
            detector.scan(&[CTRL_P, CTRL_Q]).forward,
            vec![CTRL_P, CTRL_Q]
        );
        let scan = detector.scan(&[b'z', ctrl(b'a'), ctrl(b'd')]);
        assert_eq!(scan.forward, b"z");
        assert!(scan.detach);
    }

    #[test]
    fn the_default_pair_is_ctrl_p_ctrl_q() {
        assert_eq!(
            DetachKeys::parse("ctrl-p,ctrl-q").unwrap(),
            DetachKeys::default()
        );
    }

    #[test]
    fn anything_but_two_ctrl_letters_is_refused() {
        for input in [
            "",
            "ctrl-p",
            "ctrl-p,",
            "ctrl-p,ctrl-q,ctrl-r",
            "ctrl-P,ctrl-Q",
            "ctrl-1,ctrl-q",
            "ctrl-pp,ctrl-q",
            "p,q",
            "^P,^Q",
            "a-ctrl,b-ctrl",
        ] {
            assert_eq!(
                DetachKeys::parse(input),
                Err(DetachKeysError {
                    input: input.to_string()
                }),
                "{:?} must be refused",
                input
            );
        }
    }

    #[test]
    fn the_refusal_shows_the_accepted_form() {
        let message = DetachKeys::parse("ctrl-p").unwrap_err().to_string();
        assert_eq!(
            message,
            "--detach-keys needs two ctrl keys, like ctrl-p,ctrl-q. It received 'ctrl-p'."
        );
    }
}
