//! Bounded, shared log files owned by fed.
//!
//! Every append reopens the active path under a stable sidecar lock. This is
//! deliberate: a long-lived writer must not keep writing to a renamed backup
//! after another fed process rotates the same log.

use crate::config::LoggingConfig;
use fs2::FileExt;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

const READ_LIMIT: u64 = 8 * 1024 * 1024;
pub const TRUNCATED_LINE_MARKER: &str = "[fed: earlier bytes of this log line omitted]";

pub struct RotatingWriter {
    path: PathBuf,
    lock: File,
    limits: LoggingConfig,
}

impl RotatingWriter {
    pub fn open(path: impl Into<PathBuf>, limits: LoggingConfig) -> io::Result<Self> {
        limits
            .validate()
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error.to_string()))?;
        let path = path.into();
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).map_err(|error| {
                io::Error::new(
                    error.kind(),
                    format!("creating log directory {}: {error}", parent.display()),
                )
            })?;
        }
        let lock = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(false)
            .open(sidecar(&path, ".lock"))?;
        let mut writer = Self { path, lock, limits };
        writer.with_lock(|writer| writer.prepare())?;
        Ok(writer)
    }

    fn with_lock<T>(&mut self, f: impl FnOnce(&mut Self) -> io::Result<T>) -> io::Result<T> {
        self.lock.lock_exclusive()?;
        let result = f(self);
        let unlock = FileExt::unlock(&self.lock);
        unlock?;
        result
    }

    fn prepare(&mut self) -> io::Result<()> {
        self.prune()?;
        // Existing logs can predate this policy. Trim each retained file
        // without ever loading its old contents into memory.
        trim(&self.path, self.limits.max_size)?;
        for index in 1..self.limits.max_files {
            trim(&backup(&self.path, index), self.limits.max_size)?;
        }
        OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)?;
        Ok(())
    }

    fn prune(&self) -> io::Result<()> {
        let Some(parent) = self.path.parent() else {
            return Ok(());
        };
        let Some(name) = self.path.file_name() else {
            return Ok(());
        };
        let mut prefix = name.to_os_string();
        prefix.push(".");
        for entry in fs::read_dir(parent)? {
            let entry = entry?;
            if let Some(suffix) = entry
                .file_name()
                .as_encoded_bytes()
                .strip_prefix(prefix.as_encoded_bytes())
                && let Ok(index) = std::str::from_utf8(suffix).unwrap_or("").parse::<usize>()
                && index >= self.limits.max_files
            {
                fs::remove_file(entry.path())?;
            }
        }
        Ok(())
    }

    fn rotate(&self) -> io::Result<()> {
        let oldest = self.limits.max_files - 1;
        if oldest > 0 {
            remove_if_exists(&backup(&self.path, oldest))?;
            for index in (1..oldest).rev() {
                rename_if_exists(&backup(&self.path, index), &backup(&self.path, index + 1))?;
            }
            rename_if_exists(&self.path, &backup(&self.path, 1))?;
        } else {
            remove_if_exists(&self.path)?;
        }
        Ok(())
    }

    fn append(&mut self, mut bytes: &[u8]) -> io::Result<()> {
        while !bytes.is_empty() {
            let mut file = OpenOptions::new()
                .create(true)
                .append(true)
                .open(&self.path)?;
            let size = file.metadata()?.len();
            if size >= self.limits.max_size {
                self.rotate()?;
                continue;
            }
            let count = (self.limits.max_size - size).min(bytes.len() as u64) as usize;
            file.write_all(&bytes[..count])?;
            bytes = &bytes[count..];
        }
        Ok(())
    }
}

impl Write for RotatingWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.lock.lock_exclusive()?;
        let result = self.append(buf);
        let unlock = FileExt::unlock(&self.lock);
        result?;
        unlock?;
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn sidecar(path: &Path, suffix: &str) -> PathBuf {
    let mut value = path.as_os_str().to_os_string();
    value.push(suffix);
    PathBuf::from(value)
}

fn backup(path: &Path, index: usize) -> PathBuf {
    sidecar(path, &format!(".{index}"))
}

/// A short, persistent diagnostic for a detached sink with no terminal.
pub fn record_failure(path: &Path, error: &io::Error) {
    let _ = fs::write(
        sidecar(path, ".error"),
        format!("fed log writer failed: {error}\n"),
    );
}

pub fn clear_failure(path: &Path) {
    let _ = remove_if_exists(&sidecar(path, ".error"));
}

fn trim(path: &Path, max_size: u64) -> io::Result<()> {
    let metadata = match fs::metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error),
    };
    if metadata.len() <= max_size {
        return Ok(());
    }
    let temp = sidecar(path, ".trim");
    let mut input = File::open(path)?;
    input.seek(SeekFrom::End(-(max_size as i64)))?;
    let mut output = File::create(&temp)?;
    io::copy(&mut input.take(max_size), &mut output)?;
    output.sync_all()?;
    fs::rename(temp, path)
}

fn remove_if_exists(path: &Path) -> io::Result<()> {
    match fs::remove_file(path) {
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        result => result,
    }
}

fn rename_if_exists(from: &Path, to: &Path) -> io::Result<()> {
    match fs::rename(from, to) {
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        result => result,
    }
}

/// Read the newest lines across all retained files without loading old giant
/// files into memory. A line crossing a rotation boundary is joined first.
pub fn tail(path: &Path, limits: LoggingConfig, count: usize) -> io::Result<Vec<String>> {
    if count == 0 {
        return Ok(Vec::new());
    }
    let guard = match File::open(sidecar(path, ".lock")) {
        Ok(file) => {
            FileExt::lock_shared(&file)?;
            Some(file)
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => None,
        Err(error) => return Err(error),
    };
    let mut chunks = Vec::new();
    let mut total_bytes = 0;
    let mut line_breaks = 0;
    let mut clipped = false;
    let mut enough_lines = false;
    for index in 0..limits.max_files {
        let candidate = if index == 0 {
            path.to_owned()
        } else {
            backup(path, index)
        };
        let mut file = match File::open(candidate) {
            Ok(file) => file,
            Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
            Err(error) => return Err(error),
        };
        let mut position = file.metadata()?.len();
        while position > 0 {
            let available = READ_LIMIT.saturating_sub(total_bytes);
            if available == 0 {
                clipped = true;
                break;
            }
            let take = position.min(available).min(8192);
            position -= take;
            file.seek(SeekFrom::Start(position))?;
            let mut chunk = vec![0; take as usize];
            file.read_exact(&mut chunk)?;
            line_breaks += chunk.iter().filter(|&&byte| byte == b'\n').count();
            total_bytes += take;
            chunks.push(chunk);
            if line_breaks > count {
                clipped = position > 0;
                enough_lines = true;
                break;
            }
        }
        if clipped || enough_lines {
            break;
        }
    }
    let mut bytes = Vec::with_capacity(total_bytes as usize);
    for chunk in chunks.into_iter().rev() {
        bytes.extend_from_slice(&chunk);
    }
    let decoded = String::from_utf8_lossy(&bytes);
    let text = if clipped {
        match decoded.split_once('\n') {
            Some((_, rest)) if !rest.is_empty() => rest.to_owned(),
            _ => {
                // A single huge unterminated line still deserves a useful,
                // bounded preview instead of disappearing entirely.
                let suffix = &bytes[bytes.len().saturating_sub(64 * 1024)..];
                format!(
                    "{TRUNCATED_LINE_MARKER} {}",
                    String::from_utf8_lossy(suffix)
                )
            }
        }
    } else {
        decoded.into_owned()
    };
    let lines = text
        .lines()
        .rev()
        .take(count)
        .map(str::to_owned)
        .collect::<Vec<_>>()
        .into_iter()
        .rev()
        .collect();
    drop(guard);
    let mut lines: Vec<String> = lines;
    if let Ok(error) = fs::read_to_string(sidecar(path, ".error")) {
        lines.push(error.trim_end().to_owned());
    }
    Ok(lines)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn limits(max_size: u64, max_files: usize) -> LoggingConfig {
        LoggingConfig {
            max_size,
            max_files,
        }
    }

    #[test]
    fn rotates_and_reads_split_lines_in_order() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("service.log");
        let mut writer = RotatingWriter::open(&path, limits(8, 3)).unwrap();
        writer.write_all("αβγ\nline-two\n".as_bytes()).unwrap();
        assert_eq!(tail(&path, limits(8, 3), 10).unwrap(), ["αβγ", "line-two"]);
        writer.write_all(b"third\n").unwrap();
        assert_eq!(tail(&path, limits(8, 3), 2).unwrap(), ["line-two", "third"]);
        assert!(fs::metadata(&path).unwrap().len() <= 8);
        assert!(fs::metadata(backup(&path, 1)).unwrap().len() <= 8);
        assert!(fs::metadata(backup(&path, 2)).unwrap().len() <= 8);
    }

    #[test]
    fn trims_old_logs_and_prunes_backups_when_policy_tightens() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("service.log");
        fs::write(&path, b"0123456789abcdefghij").unwrap();
        fs::write(backup(&path, 1), b"abcdefghijklmnopqrst").unwrap();
        fs::write(backup(&path, 4), b"old").unwrap();
        RotatingWriter::open(&path, limits(6, 2)).unwrap();
        assert_eq!(fs::read(&path).unwrap(), b"efghij");
        assert_eq!(fs::read(backup(&path, 1)).unwrap(), b"opqrst");
        assert!(!backup(&path, 4).exists());
    }

    #[test]
    fn reports_inaccessible_destination() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("directory.log");
        fs::create_dir(&path).unwrap();
        assert!(RotatingWriter::open(path, limits(8, 3)).is_err());
    }

    #[test]
    fn interleaved_writers_reopen_active_file_after_rotation() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("tui.log");
        let mut first = RotatingWriter::open(&path, limits(4, 3)).unwrap();
        let mut second = RotatingWriter::open(&path, limits(4, 3)).unwrap();
        first.write_all(b"abcd").unwrap();
        second.write_all(b"efgh").unwrap();
        first.write_all(b"ijkl").unwrap();
        assert_eq!(fs::read(&path).unwrap(), b"ijkl");
        assert_eq!(fs::read(backup(&path, 1)).unwrap(), b"efgh");
        assert_eq!(fs::read(backup(&path, 2)).unwrap(), b"abcd");
        assert!(!backup(&path, 3).exists());
    }

    #[test]
    fn one_file_policy_displaces_old_bytes() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("one.log");
        let mut writer = RotatingWriter::open(&path, limits(4, 1)).unwrap();
        writer.write_all(b"abcdefgh").unwrap();
        assert_eq!(fs::read(&path).unwrap(), b"efgh");
        assert!(!backup(&path, 1).exists());
    }

    #[test]
    fn clipped_unterminated_line_keeps_bounded_suffix() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("huge.log");
        let file = File::create(&path).unwrap();
        file.set_len(READ_LIMIT + 1).unwrap();
        let lines = tail(&path, limits(READ_LIMIT + 1, 1), 1).unwrap();
        assert_eq!(lines.len(), 1);
        assert!(lines[0].starts_with(TRUNCATED_LINE_MARKER));
        assert!(lines[0].len() <= 64 * 1024 + TRUNCATED_LINE_MARKER.len() + 1);
    }

    #[test]
    fn clipped_line_ending_at_window_boundary_keeps_suffix() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("huge.log");
        let mut file = File::create(&path).unwrap();
        file.set_len(READ_LIMIT + 1).unwrap();
        file.seek(SeekFrom::End(-1)).unwrap();
        file.write_all(b"\n").unwrap();
        let lines = tail(&path, limits(READ_LIMIT + 1, 1), 1).unwrap();
        assert_eq!(lines.len(), 1);
        assert!(lines[0].starts_with(TRUNCATED_LINE_MARKER));
    }
}
