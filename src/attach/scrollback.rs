//! The recent output a client receives when it connects.
//!
//! The host keeps the last [`DEFAULT_CAPACITY`] bytes the service printed in
//! a ring and sends them as one `Scrollback` frame, so an attaching terminal
//! is not blank until the service prints again.

/// How much output the host keeps. A full-screen program redraws often, so
/// this is a few screens rather than a session.
pub const DEFAULT_CAPACITY: usize = 64 * 1024;

/// A fixed-size byte ring. Once it is full, the oldest bytes are overwritten.
#[derive(Debug)]
pub struct Scrollback {
    buffer: Vec<u8>,
    /// Where the next byte is written.
    head: usize,
    /// How many bytes are held, at most `buffer.len()`.
    len: usize,
}

impl Default for Scrollback {
    fn default() -> Self {
        Self::with_capacity(DEFAULT_CAPACITY)
    }
}

impl Scrollback {
    /// Keep the last `capacity` bytes.
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            buffer: vec![0; capacity],
            head: 0,
            len: 0,
        }
    }

    /// How many bytes are held.
    pub fn len(&self) -> usize {
        self.len
    }

    /// Nothing has been pushed, or the capacity is zero.
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Add output. A push larger than the capacity keeps its tail.
    pub fn push(&mut self, data: &[u8]) {
        let capacity = self.buffer.len();
        if capacity == 0 {
            return;
        }
        let data = if data.len() > capacity {
            &data[data.len() - capacity..]
        } else {
            data
        };

        let mut written = 0;
        while written < data.len() {
            let room = capacity - self.head;
            let take = room.min(data.len() - written);
            self.buffer[self.head..self.head + take]
                .copy_from_slice(&data[written..written + take]);
            self.head = (self.head + take) % capacity;
            written += take;
        }
        self.len = (self.len + data.len()).min(capacity);
    }

    /// The bytes held, oldest first.
    pub fn contents(&self) -> Vec<u8> {
        if self.len < self.buffer.len() {
            return self.buffer[..self.len].to_vec();
        }
        let mut out = Vec::with_capacity(self.len);
        out.extend_from_slice(&self.buffer[self.head..]);
        out.extend_from_slice(&self.buffer[..self.head]);
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_fresh_ring_is_empty() {
        let ring = Scrollback::default();
        assert!(ring.is_empty());
        assert!(ring.contents().is_empty());
    }

    #[test]
    fn output_that_fits_comes_back_whole() {
        let mut ring = Scrollback::with_capacity(16);
        ring.push(b"abc");
        ring.push(b"de");
        assert_eq!(ring.contents(), b"abcde");
        assert_eq!(ring.len(), 5);
    }

    #[test]
    fn a_push_larger_than_the_capacity_keeps_its_tail() {
        let mut ring = Scrollback::with_capacity(4);
        ring.push(b"0123456789");
        assert_eq!(ring.contents(), b"6789");
        assert_eq!(ring.len(), 4);
    }

    #[test]
    fn many_small_pushes_keep_the_last_bytes() {
        let mut ring = Scrollback::with_capacity(4);
        for byte in b"0123456789" {
            ring.push(&[*byte]);
        }
        assert_eq!(ring.contents(), b"6789");
    }

    #[test]
    fn a_push_that_wraps_stays_in_order() {
        let mut ring = Scrollback::with_capacity(5);
        ring.push(b"abc");
        ring.push(b"defg");
        assert_eq!(ring.contents(), b"cdefg");
    }

    #[test]
    fn the_default_capacity_is_64_kib() {
        let mut ring = Scrollback::default();
        ring.push(&vec![b'x'; DEFAULT_CAPACITY + 10]);
        assert_eq!(ring.len(), 64 * 1024);
    }

    #[test]
    fn a_zero_capacity_ring_holds_nothing() {
        let mut ring = Scrollback::with_capacity(0);
        ring.push(b"abc");
        assert!(ring.contents().is_empty());
    }
}
