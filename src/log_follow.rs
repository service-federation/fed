//! Follow retained log files by identity and byte offset, including repeated lines.

use fs2::FileExt;
use std::fs::{self, File};
use std::io::{self, Read, Seek, SeekFrom};
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};

const READ_BUDGET: usize = 2 * 1024 * 1024;
const PARTIAL_LIMIT: usize = 64 * 1024;

/// One polling result. A reader cannot recover history already removed by retention.
pub struct FollowBatch {
    pub lines: Vec<String>,
    pub history_lost: bool,
}

struct Segment {
    path: PathBuf,
    identity: (u64, u64),
    len: u64,
}

/// Keeps an open handle to the last segment so rotation cannot reuse its inode.
pub struct FileFollower {
    path: PathBuf,
    cursor: Option<(File, (u64, u64), u64)>,
    pending: Vec<u8>,
    initial_tail: usize,
    reported_failure: Option<String>,
}

impl FileFollower {
    pub fn new(path: PathBuf, initial_tail: usize) -> Self {
        Self {
            path,
            cursor: None,
            pending: Vec::new(),
            initial_tail,
            reported_failure: None,
        }
    }

    /// Read a bounded amount while holding the writer's stable rotation lock.
    pub fn poll(&mut self) -> io::Result<FollowBatch> {
        let mut lock_path = self.path.as_os_str().to_os_string();
        lock_path.push(".lock");
        let lock = match File::open(PathBuf::from(lock_path)) {
            Ok(file) => {
                FileExt::lock_shared(&file)?;
                Some(file)
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => None,
            Err(error) => return Err(error),
        };
        let result = self.read_segments();
        drop(lock);
        let mut batch = result?;
        let mut error_path = self.path.as_os_str().to_os_string();
        error_path.push(".error");
        match File::open(PathBuf::from(error_path)) {
            Ok(file) => {
                let mut bytes = Vec::new();
                file.take(4096).read_to_end(&mut bytes)?;
                let message = String::from_utf8_lossy(&bytes).trim_end().to_owned();
                if self.reported_failure.as_ref() != Some(&message) {
                    batch.lines.push(message.clone());
                    self.reported_failure = Some(message);
                }
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => self.reported_failure = None,
            Err(error) => return Err(error),
        }
        Ok(batch)
    }

    fn read_segments(&mut self) -> io::Result<FollowBatch> {
        let segments = segments(&self.path)?;
        if segments.is_empty() {
            return Ok(FollowBatch {
                lines: Vec::new(),
                history_lost: false,
            });
        }
        let initial = self.cursor.is_none();
        let mut history_lost = false;
        let (start, offset) = match &self.cursor {
            Some((_, identity, offset)) => {
                match segments.iter().position(|s| s.identity == *identity) {
                    Some(index) if *offset <= segments[index].len => (index, *offset),
                    _ => {
                        history_lost = true;
                        self.pending.clear();
                        (0, 0)
                    }
                }
            }
            None => {
                let mut budget = READ_BUDGET as u64;
                let mut start = 0;
                let mut offset = 0;
                for (index, segment) in segments.iter().enumerate().rev() {
                    start = index;
                    if segment.len > budget {
                        offset = segment.len - budget;
                        break;
                    }
                    budget -= segment.len;
                }
                (start, offset)
            }
        };
        let mut remaining = READ_BUDGET;
        let mut bytes = Vec::new();
        for (index, segment) in segments.iter().enumerate().skip(start) {
            let offset = if index == start { offset } else { 0 };
            let count = (segment.len - offset).min(remaining as u64);
            let mut file = File::open(&segment.path)?;
            file.seek(SeekFrom::Start(offset))?;
            let before = bytes.len();
            file.by_ref().take(count).read_to_end(&mut bytes)?;
            let read = bytes.len() - before;
            self.cursor = Some((file, segment.identity, offset + read as u64));
            remaining -= read;
            if remaining == 0 {
                break;
            }
        }
        let mut truncated_line = false;
        if (initial && offset > 0) || (history_lost && segments[start].path != self.path) {
            // The bounded initial window can begin inside a line or a UTF-8 scalar.
            // After lost history, the oldest retained backup has no known line boundary either.
            match bytes.iter().position(|byte| *byte == b'\n') {
                Some(index) if index + 1 < bytes.len() => {
                    bytes.drain(..=index);
                }
                _ => truncated_line = !bytes.is_empty(),
            }
        }
        let mut lines = self.consume(&bytes);
        if initial && lines.len() > self.initial_tail {
            lines.drain(..lines.len() - self.initial_tail);
        }
        if truncated_line && (!initial || self.initial_tail > 0) {
            lines.insert(0, crate::log_rotation::TRUNCATED_LINE_MARKER.to_owned());
        }
        Ok(FollowBatch {
            lines,
            history_lost,
        })
    }

    fn consume(&mut self, bytes: &[u8]) -> Vec<String> {
        let mut lines = Vec::new();
        for byte in bytes {
            if *byte == b'\n' {
                if self.pending.last() == Some(&b'\r') {
                    self.pending.pop();
                }
                lines.push(String::from_utf8_lossy(&self.pending).into_owned());
                self.pending.clear();
            } else {
                self.pending.push(*byte);
                if self.pending.len() >= PARTIAL_LIMIT {
                    // Bound memory even for a service that never writes a newline.
                    // Leave any incomplete UTF-8 scalar for the next chunk.
                    let split = match std::str::from_utf8(&self.pending) {
                        Err(error) if error.error_len().is_none() => error.valid_up_to(),
                        _ => self.pending.len(),
                    };
                    lines.push(String::from_utf8_lossy(&self.pending[..split]).into_owned());
                    self.pending.drain(..split);
                }
            }
        }
        lines
    }
}

fn segments(path: &Path) -> io::Result<Vec<Segment>> {
    let Some(name) = path.file_name() else {
        return Ok(Vec::new());
    };
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    let entries = match fs::read_dir(parent) {
        Ok(entries) => entries,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => return Err(error),
    };
    use std::os::unix::ffi::OsStrExt;
    let mut prefix = name.as_bytes().to_vec();
    prefix.push(b'.');
    let mut paths = Vec::new();
    for entry in entries {
        let entry = entry?;
        let file_name = entry.file_name();
        let index = if file_name == name {
            0
        } else {
            let Some(suffix) = file_name.as_bytes().strip_prefix(prefix.as_slice()) else {
                continue;
            };
            let Some(index) = std::str::from_utf8(suffix)
                .ok()
                .and_then(|s| s.parse::<usize>().ok())
            else {
                continue;
            };
            if index == 0 {
                continue;
            }
            index
        };
        paths.push((index, entry.path()));
    }
    paths.sort_by_key(|(index, _)| std::cmp::Reverse(*index));
    paths
        .into_iter()
        .map(|(_, path)| {
            let metadata = fs::metadata(&path)?;
            Ok(Segment {
                path,
                identity: (metadata.dev(), metadata.ino()),
                len: metadata.len(),
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn follows_identical_lines_across_rotation_and_split_utf8() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("service.log");
        fs::write(&path, b"old\nsame\n").unwrap();
        let mut follower = FileFollower::new(path.clone(), 1);
        assert_eq!(follower.poll().unwrap().lines, ["same"]);
        fs::rename(&path, dir.path().join("service.log.1")).unwrap();
        fs::write(&path, b"same\n\xc3").unwrap();
        assert_eq!(follower.poll().unwrap().lines, ["same"]);
        let mut file = fs::OpenOptions::new().append(true).open(&path).unwrap();
        file.write_all(b"\xa6\n").unwrap();
        assert_eq!(follower.poll().unwrap().lines, ["æ"]);
        assert!(follower.poll().unwrap().lines.is_empty());
    }

    #[test]
    fn reports_retention_overtaking_reader() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("service.log");
        fs::write(&path, b"before\n").unwrap();
        let mut follower = FileFollower::new(path.clone(), 10);
        follower.poll().unwrap();
        fs::remove_file(&path).unwrap();
        fs::write(&path, b"after\n").unwrap();
        let batch = follower.poll().unwrap();
        assert!(batch.history_lost);
        assert_eq!(batch.lines, ["after"]);
    }

    #[test]
    fn skips_the_unknown_line_boundary_after_history_expires() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("service.log");
        fs::write(&path, b"before\n").unwrap();
        let mut follower = FileFollower::new(path.clone(), 10);
        follower.poll().unwrap();
        fs::remove_file(&path).unwrap();
        fs::write(dir.path().join("service.log.1"), b"fragment\nretained\n").unwrap();
        fs::write(&path, b"new\n").unwrap();
        let batch = follower.poll().unwrap();
        assert!(batch.history_lost);
        assert_eq!(batch.lines, ["retained", "new"]);
    }

    #[test]
    fn drains_multiple_rotations_without_repeating_history() {
        use crate::config::LoggingConfig;
        use crate::log_rotation::RotatingWriter;

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("service.log");
        let mut writer = RotatingWriter::open(
            &path,
            LoggingConfig {
                max_size: 7,
                max_files: 5,
            },
        )
        .unwrap();
        writer.write_all(b"first\n").unwrap();
        let mut follower = FileFollower::new(path, 0);
        assert!(follower.poll().unwrap().lines.is_empty());
        writer.write_all(b"same\nsame\nsame\n").unwrap();
        let batch = follower.poll().unwrap();
        assert!(!batch.history_lost);
        assert_eq!(batch.lines, ["same", "same", "same"]);
        assert!(follower.poll().unwrap().lines.is_empty());
    }

    #[test]
    fn bounds_unterminated_lines_and_recovers_from_truncation() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("service.log");
        fs::write(&path, vec![b'x'; PARTIAL_LIMIT + 10]).unwrap();
        let mut follower = FileFollower::new(path.clone(), 10);
        let batch = follower.poll().unwrap();
        assert_eq!(batch.lines.len(), 1);
        assert_eq!(batch.lines[0].len(), PARTIAL_LIMIT);
        assert_eq!(follower.pending.len(), 10);
        fs::write(&path, b"new\n").unwrap();
        let batch = follower.poll().unwrap();
        assert!(batch.history_lost);
        assert_eq!(batch.lines, ["new"]);
    }

    #[test]
    fn displays_a_bounded_suffix_of_a_line_larger_than_the_read_budget() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("service.log");
        fs::write(&path, vec![b'x'; READ_BUDGET + 10]).unwrap();
        let mut follower = FileFollower::new(path, 10);
        let batch = follower.poll().unwrap();
        assert_eq!(batch.lines[0], crate::log_rotation::TRUNCATED_LINE_MARKER);
        assert!(batch.lines.len() > 1);
        assert!(
            batch.lines[1..]
                .iter()
                .all(|line| line.len() <= PARTIAL_LIMIT)
        );
        assert!(follower.pending.len() < PARTIAL_LIMIT);
    }

    #[test]
    fn reports_writer_failure_once_even_without_a_log_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("service.log");
        let mut follower = FileFollower::new(path.clone(), 10);
        crate::log_rotation::record_failure(&path, &io::Error::other("disk full"));
        assert_eq!(
            follower.poll().unwrap().lines,
            ["fed log writer failed: disk full"]
        );
        assert!(follower.poll().unwrap().lines.is_empty());
        crate::log_rotation::clear_failure(&path);
        assert!(follower.poll().unwrap().lines.is_empty());
    }
}
