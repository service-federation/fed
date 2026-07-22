#![allow(unused_assignments)]

//! # Service Federation
//!
//! A service orchestration tool for managing complex service dependencies in development environments.
//!
//! ## Features
//!
//! - **Service Orchestration**: Start, stop, and manage services with dependency-aware ordering
//! - **Multiple Service Types**: Support for process-based, Docker, Docker Compose, and Gradle services
//! - **Template Resolution**: Dynamic variable substitution in configuration using `{{variable}}` syntax
//! - **Port Allocation**: Automatic port allocation with TOCTOU race prevention
//! - **Isolation**: Directory-scoped isolation with stable port allocation across restarts
//! - **Cancellation Support**: Graceful cancellation of in-progress operations via `CancellationToken`
//! - **Configurable Timeouts**: Per-operation timeouts for startup and stop operations
//! - **TUI Interface**: Interactive terminal UI for monitoring and control
//! - **Watch Mode**: Automatic service restart on file changes
//!
//! ## Quick Start
//!
//! ```no_run
//! use fed::{Orchestrator, Parser};
//!
//! # async fn example() -> Result<(), fed::Error> {
//! // Load and parse configuration
//! let parser = Parser::new();
//! let config = parser.load_config("fed.yaml")?;
//!
//! // Create and initialize orchestrator
//! let mut orchestrator = Orchestrator::new(config, std::path::PathBuf::from(".")).await?;
//! orchestrator.initialize().await?;
//!
//! // Start all services (respects dependency order)
//! orchestrator.start_all().await?;
//!
//! // Cleanup when done
//! orchestrator.cleanup().await;
//! # Ok(())
//! # }
//! ```
//!
//! ## Concurrency Model
//!
//! The orchestrator supports concurrent access:
//! - Most methods take `&self` instead of `&mut self`
//! - Operations can be cancelled via [`Orchestrator::cancel_operations`]
//! - Timeouts prevent hanging on stuck services
//! - Cleanup runs exactly once even with concurrent calls

pub mod cloud;
pub mod config;
pub mod dependency;
pub mod docker;
pub mod error;
pub mod fed_dir;
pub mod fsutil;
pub mod healthcheck;
pub mod lock_order;
pub mod markers;
pub mod orchestrator;
pub mod package;
pub mod parameter;
pub mod port;
pub mod service;
pub mod state;
pub mod tui;
pub mod watch;

// Re-export commonly used types
pub use config::{Config, Parser, RestartPolicy};
pub use error::{Error, Result};
pub use orchestrator::{Orchestrator, RunContext};
pub use package::{PackageLoader, PackageResolver, ServiceMerger};
pub use service::{OutputMode, ServiceManager, Status};
pub use state::StateTracker;
pub use watch::WatchMode;
