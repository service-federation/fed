//! Service management implementations for different service types.
//!
//! This module provides the [`ServiceManager`] trait and implementations for:
//!
//! - **Process services** ([`ProcessService`]): Standard process-based services
//! - **Docker services** ([`DockerService`]): Services running in Docker containers
//! - **Docker Compose services** ([`DockerComposeService`]): Multi-container Docker applications
//! - **Gradle services** ([`GradleService`]): Java/Kotlin services using Gradle
//! - **External services** ([`ExternalService`]): Services from external repositories
//!
//! # Example
//!
//! ```ignore
//! use fed::service::{ServiceManager, Status};
//!
//! async fn check_service(service: &dyn ServiceManager) {
//!     if service.status() == Status::Running {
//!         println!("Service {} is running", service.name());
//!     }
//! }
//! ```

mod compose;
mod docker;
mod external;
mod gradle;
mod log_capture;
mod oneshot;
mod process;
mod resources;
mod types;

pub use compose::*;
pub(crate) use docker::docker_container_name;
pub(crate) use docker::fnv1a_32;
pub use docker::hash_work_dir;
pub(crate) use docker::sanitize_container_name_component;
pub use docker::*;
pub use external::*;
pub use gradle::*;
pub use log_capture::*;
pub use oneshot::*;
pub use process::*;
pub use resources::*;
pub use types::*;
