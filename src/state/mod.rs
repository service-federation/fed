//! Persistent state management for service federation.
//!
//! This module provides SQLite-backed state tracking for:
//!
//! - Service lifecycle state (PIDs, container IDs, status)
//! - Port allocations across sessions
//! - Restart counts and failure tracking
//!
//! # Architecture
//!
//! State is persisted in a SQLite database (`.fed/lock.db`) with WAL mode enabled
//! for crash recovery. This ensures that service state survives process restarts
//! and allows multiple `fed` instances to coordinate.
//!
//! # Example
//!
//! ```ignore
//! use fed::StateTracker;
//!
//! let tracker = StateTracker::new(work_dir).await?;
//! tracker.initialize().await?;
//!
//! // Register a running service
//! let state = ServiceState::new("my-service".into(), "Process".into(), "root".into());
//! tracker.register_service(state).await;
//! ```

mod sqlite;
mod types;

pub use sqlite::SqliteStateTracker;
pub use types::{DesiredState, LockFile, RegistrationOutcome, ServiceState};

/// Primary state tracker type, backed by SQLite.
///
/// This is an alias for [`SqliteStateTracker`] for backwards compatibility.
pub type StateTracker = SqliteStateTracker;

/// How long a `Starting` row with no PID/container yet is exempt from
/// liveness sweeps (`mark_dead_services`).
///
/// That shape is exactly what a live concurrent start looks like while it
/// runs install/migrate hooks — another `fed` process initializing must not
/// mark it stale, or a start waiting on that attempt
/// (`Orchestrator::await_concurrent_start`, which uses this same constant as
/// its wait deadline) would misread the winner as failed. Only a row stuck
/// in `Starting` past this window — a starter that died without cleanup —
/// is treated as dead.
pub const STARTING_STALE_GRACE: std::time::Duration = std::time::Duration::from_secs(180);
