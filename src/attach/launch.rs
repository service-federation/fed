//! The handshake between `fed start` and a host process.
//!
//! `fed start` resolves parameters, secrets, cwd and environment, writes one
//! [`LaunchSpec`] line to the host's stdin and closes it. The host answers
//! with [`HostEvent`] lines on its stdout. Both sides use one JSON object per
//! line, so a reader can take them with `BufRead::read_line`.

use crate::config::ResourceLimits;
use crate::error::Result;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;

/// Everything a host needs to run one service. Nothing here is looked up
/// again: the host never loads config, never reads the vault and never opens
/// the state database.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LaunchSpec {
    /// The service's name, as written in `fed.yaml`.
    pub service: String,
    /// The `process:` string, run as `bash -ec <command>`.
    pub command: String,
    /// The workspace root, the directory that holds `fed.yaml`. The service
    /// receives it as `FED_SPAWNED_FROM_WORKSPACE`, and `cwd` may sit below
    /// it.
    pub work_dir: PathBuf,
    /// Already resolved against the work dir.
    pub cwd: PathBuf,
    /// The service's resolved environment.
    pub environment: HashMap<String, String>,
    /// `.fed/logs/<service>.log`, opened for append.
    pub log_path: PathBuf,
    /// Where the host listens for clients.
    pub socket_path: PathBuf,
    /// Parsed in the host before the fork.
    pub resources: Option<ResourceLimits>,
}

/// What a host reports back to `fed start` on its stdout.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HostEvent {
    /// The service is running on the pty.
    Ready {
        /// The service's pid, not the host's.
        pid: u32,
    },
    /// The service exited while `fed start` was still reading, during its
    /// crash window.
    Exited {
        /// The raw `waitpid` status, so a signal survives.
        status: i32,
    },
    /// The host gave up before the service started. Never sent after
    /// [`HostEvent::Ready`].
    Failed {
        /// Text for `fed start` to show the user.
        error: String,
    },
}

impl LaunchSpec {
    /// One JSON object and the newline that ends it.
    pub fn to_line(&self) -> Result<String> {
        Ok(format!("{}\n", serde_json::to_string(self)?))
    }

    /// Read one line written by [`LaunchSpec::to_line`]. Trailing whitespace
    /// is ignored, so a line that still carries its newline works.
    pub fn from_line(line: &str) -> Result<Self> {
        Ok(serde_json::from_str(line.trim_end())?)
    }
}

impl HostEvent {
    /// One JSON object and the newline that ends it.
    pub fn to_line(&self) -> Result<String> {
        Ok(format!("{}\n", serde_json::to_string(self)?))
    }

    /// Read one line written by [`HostEvent::to_line`].
    pub fn from_line(line: &str) -> Result<Self> {
        Ok(serde_json::from_str(line.trim_end())?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn spec() -> LaunchSpec {
        LaunchSpec {
            service: "repl".to_string(),
            command: "node".to_string(),
            work_dir: PathBuf::from("/w"),
            cwd: PathBuf::from("/w/api"),
            environment: HashMap::from([("PORT".to_string(), "8080".to_string())]),
            log_path: PathBuf::from("/w/.fed/logs/repl.log"),
            socket_path: PathBuf::from("/tmp/fed-abc/repl.sock"),
            resources: None,
        }
    }

    #[test]
    fn a_launch_spec_survives_a_round_trip() {
        let line = spec().to_line().unwrap();
        assert!(line.ends_with('\n'), "each spec is one line");
        assert_eq!(line.matches('\n').count(), 1);

        let back = LaunchSpec::from_line(&line).unwrap();
        assert_eq!(back.service, "repl");
        assert_eq!(back.work_dir, PathBuf::from("/w"));
        assert_eq!(back.cwd, PathBuf::from("/w/api"));
        assert_eq!(back.environment["PORT"], "8080");
        assert_eq!(back.socket_path, PathBuf::from("/tmp/fed-abc/repl.sock"));
    }

    #[test]
    fn every_host_event_survives_a_round_trip() {
        for event in [
            HostEvent::Ready { pid: 4242 },
            HostEvent::Exited { status: 256 },
            HostEvent::Failed {
                error: "the socket is taken".to_string(),
            },
        ] {
            let line = event.to_line().unwrap();
            assert_eq!(HostEvent::from_line(&line).unwrap(), event);
        }
    }

    #[test]
    fn a_failed_line_parses() {
        let event = HostEvent::from_line(r#"{"failed":{"error":"openpty failed"}}"#).unwrap();
        assert_eq!(
            event,
            HostEvent::Failed {
                error: "openpty failed".to_string()
            }
        );
    }

    #[test]
    fn a_ready_line_parses() {
        assert_eq!(
            HostEvent::from_line(r#"{"ready":{"pid":7}}"#).unwrap(),
            HostEvent::Ready { pid: 7 }
        );
    }
}
