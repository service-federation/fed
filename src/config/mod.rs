// Allow deprecated PackageAuth re-export for backward compatibility
#![allow(deprecated)]

//! Configuration parsing and types.
//!
//! This module provides the configuration system for service-federation,
//! organized into focused submodules:
//!
//! - `types` - Core config structure (`Config`, `PackageReference`, etc.)
//! - `service` - Service configuration (`Service`, `ServiceType`)
//! - `health` - Health check config (`HealthCheck`, `RestartPolicy`)
//! - `resources` - Resource limits (`ResourceLimits`)
//! - `dependency` - Dependencies (`Dependency`, `DependsOn`, `Metadata`)
//! - `parameter` - Variables/parameters (`Parameter`)
//! - `script` - Script configuration (`Script`)
//! - `parser` - YAML config parsing
//! - `defaults` - The top-level `defaults:` block
//! - `validation` - Config validation
//! - `variants` - Per-service implementation variants

pub mod discovery;
pub mod env_loader;

pub mod defaults;
mod dependency;
mod duration;
mod health;
mod parameter;
mod parser;
mod resources;
mod script;
mod service;
mod types;
mod validation;
pub mod variants;

// Re-export all types for backward compatibility
pub use dependency::*;
pub use duration::*;
pub use health::*;
pub use parameter::*;
pub use parser::*;
pub use resources::*;
pub use script::*;
pub use service::*;
pub use types::*;
pub use validation::*;
