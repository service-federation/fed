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
