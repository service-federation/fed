//! File watching and automatic service restart functionality.
//!
//! This module provides file system monitoring to automatically restart services
//! when their source files change. This is useful during development to avoid
//! manual restarts after code changes.
//!
//! # Usage
//!
//! ```ignore
//! use fed::{WatchMode, Config};
//!
//! let watch = WatchMode::new(&config, &work_dir)?;
//! while let Some(event) = watch.next_event().await {
//!     println!("Service {} needs restart", event.service_name);
//! }
//! ```
//!
//! # Configuration
//!
//! Services can specify custom watch paths in their configuration:
//!
//! ```yaml
//! services:
//!   my-service:
//!     process: "npm run dev"
//!     watch:
//!       - src/
//!       - package.json
//! ```

use crate::config::Config;
use crate::error::Result;
use notify_debouncer_full::{
    DebounceEventResult, Debouncer, FileIdMap, new_debouncer,
    notify::{RecursiveMode, Watcher},
};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;

/// File change event with affected service
#[derive(Debug, Clone)]
pub struct FileChangeEvent {
    pub service_name: String,
    pub changed_paths: Vec<PathBuf>,
}

/// Watch mode manager
pub struct WatchMode {
    _debouncer: Debouncer<RecommendedWatcher, FileIdMap>,
    event_rx: mpsc::UnboundedReceiver<FileChangeEvent>,
}

use notify_debouncer_full::notify::RecommendedWatcher;

impl WatchMode {
    /// Create a new watch mode instance
    ///
    /// This sets up file watchers for all services with:
    /// - Their working directory (cwd)
    /// - Custom watch paths specified in the service config
    pub fn new(config: &Config, work_dir: &Path) -> Result<Self> {
        let (event_tx, event_rx) = mpsc::unbounded_channel();

        // Map of paths to service names for quick lookup
        let mut path_to_services: HashMap<PathBuf, Vec<String>> = HashMap::new();

        // Collect all paths to watch per service
        for (service_name, service) in &config.services {
            let mut paths_to_watch = Vec::new();

            // Watch the service's working directory if it exists
            if let Some(cwd) = &service.cwd {
                let cwd_path = work_dir.join(cwd);
                if cwd_path.exists() {
                    paths_to_watch.push(cwd_path);
                }
            }

            // Watch custom paths specified in the service config
            for watch_path in &service.watch {
                let watch_path = work_dir.join(watch_path);
                if watch_path.exists() {
                    paths_to_watch.push(watch_path);
                }
            }

            // Register paths for this service
            for path in paths_to_watch {
                path_to_services
                    .entry(path.clone())
                    .or_default()
                    .push(service_name.clone());
            }
        }

        // Wrap in Arc to avoid expensive clone (only Arc pointer is cloned)
        let path_to_services = Arc::new(path_to_services);

        // Create the debounced file watcher
        let event_tx_clone = event_tx.clone();
        let path_to_services_clone = Arc::clone(&path_to_services);

        // Use 500ms debounce to handle editors with format-on-save that
        // may save the file twice in quick succession (save + format)
        let mut debouncer = new_debouncer(
            Duration::from_millis(500),
            None,
            move |result: DebounceEventResult| {
                match result {
                    Ok(events) => {
                        // Group events by service
                        let mut service_events: HashMap<String, Vec<PathBuf>> = HashMap::new();

                        for event in events {
                            for path in &event.paths {
                                // Canonicalize to resolve symlinks
                                let canonical_path =
                                    path.canonicalize().unwrap_or_else(|_| path.clone());

                                // Find which services this path belongs to
                                for (watch_path, services) in path_to_services_clone.iter() {
                                    // Canonicalize watch path too for consistent comparison
                                    let canonical_watch = watch_path
                                        .canonicalize()
                                        .unwrap_or_else(|_| watch_path.clone());

                                    if canonical_path.starts_with(&canonical_watch) {
                                        // Skip ignored paths
                                        if should_ignore_path(path) {
                                            continue;
                                        }

                                        for service_name in services {
                                            service_events
                                                .entry(service_name.clone())
                                                .or_default()
                                                .push(path.clone());
                                        }
                                    }
                                }
                            }
                        }

                        // Send events for each affected service
                        for (service_name, changed_paths) in service_events {
                            let _ = event_tx_clone.send(FileChangeEvent {
                                service_name,
                                changed_paths,
                            });
                        }
                    }
                    Err(errors) => {
                        tracing::warn!("Watch error: {:?}", errors);
                    }
                }
            },
        )
        .map_err(|e| {
            crate::error::Error::Filesystem(format!("Failed to create file watcher: {}", e))
        })?;

        // Add all paths to the watcher
        for path in path_to_services.keys() {
            debouncer
                .watcher()
                .watch(path, RecursiveMode::Recursive)
                .map_err(|e| {
                    crate::error::Error::Filesystem(format!(
                        "Failed to watch path {}: {}",
                        path.display(),
                        e
                    ))
                })?;

            tracing::debug!("Watching path: {}", path.display());
        }

        Ok(Self {
            _debouncer: debouncer,
            event_rx,
        })
    }

    /// Receive the next file change event
    pub async fn next_event(&mut self) -> Option<FileChangeEvent> {
        self.event_rx.recv().await
    }
}

/// Check if a path should be ignored (common build artifacts, dependencies, etc.)
fn should_ignore_path(path: &Path) -> bool {
    let ignored_names = [
        "node_modules",
        ".git",
        "target",
        "dist",
        "build",
        ".next",
        ".nuxt",
        "__pycache__",
        ".pytest_cache",
        ".venv",
        "venv",
        ".DS_Store",
    ];

    path.components().any(|component| {
        let name = component.as_os_str().to_string_lossy();
        ignored_names.contains(&name.as_ref())
            || [".log", ".swp", ".swo", "~"]
                .iter()
                .any(|suffix| name.ends_with(suffix))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_should_ignore_path() {
        assert!(should_ignore_path(Path::new("/foo/node_modules/bar")));
        assert!(should_ignore_path(Path::new("/foo/.git/config")));
        assert!(should_ignore_path(Path::new("/foo/target/debug/bar")));
        assert!(!should_ignore_path(Path::new("/foo/src/main.rs")));
        assert!(!should_ignore_path(Path::new("/foo/package.json")));
    }

    #[test]
    fn ignore_patterns_match_names_and_suffixes() {
        for path in [
            "/foo/app.log",
            "/foo/.main.rs.swp",
            "/foo/.main.rs.swo",
            "/foo/main.rs~",
            "/foo/.next/server/app.js",
        ] {
            assert!(should_ignore_path(Path::new(path)), "{path}");
        }
        for path in [
            "/foo/.github/workflows/ci.yml",
            "/foo/src/rebuild.rs",
            "/foo/build-tools/config.json",
            "/foo/targeting/main.rs",
            "/foo/src/distinct.rs",
            "/foo/app.log.rs",
        ] {
            assert!(!should_ignore_path(Path::new(path)), "{path}");
        }
    }

    #[tokio::test]
    async fn changes_in_similarly_named_source_paths_emit_events() {
        let temp = tempfile::tempdir().unwrap();
        let source_dir = temp.path().join("build-tools");
        std::fs::create_dir(&source_dir).unwrap();
        let source = source_dir.join("rebuild.rs");
        std::fs::write(&source, "before").unwrap();
        let mut config = Config::default();
        config.services.insert(
            "app".into(),
            crate::config::Service {
                watch: vec!["build-tools".into()],
                ..Default::default()
            },
        );
        let mut watcher = WatchMode::new(&config, temp.path()).unwrap();
        std::fs::write(&source, "after").unwrap();
        let source = source.canonicalize().unwrap();
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let event = watcher.next_event().await.unwrap();
                assert_eq!(event.service_name, "app");
                // macOS can report /private/var for a fixture under /var.
                // Other events may arrive before the changed source file.
                if event
                    .changed_paths
                    .iter()
                    .any(|path| path.canonicalize().ok().as_ref() == Some(&source))
                {
                    break;
                }
            }
        })
        .await
        .expect("source changes should reach the watch restart loop");
    }
}
