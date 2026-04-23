use crate::orchestrator::Orchestrator;
use crate::service::Status;
use crossterm::event::{KeyCode, KeyEvent, KeyModifiers, MouseEvent, MouseEventKind};
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;

const LOG_BUFFER_SIZE: usize = 1000;

/// Case-insensitive ASCII substring search without allocation.
fn contains_ci(haystack: &str, needle: &[u8]) -> bool {
    haystack
        .as_bytes()
        .windows(needle.len())
        .any(|window| window.eq_ignore_ascii_case(needle))
}

#[derive(Debug, Clone)]
pub struct StatusMessage {
    pub text: String,
    pub level: StatusLevel,
    pub expires_at: Instant,
}

#[derive(Debug, Clone, Copy)]
pub enum StatusLevel {
    Info,
    Success,
    Warning,
    Error,
}

pub struct App {
    /// Shared orchestrator
    pub orchestrator: Arc<RwLock<Orchestrator>>,

    /// Current view
    pub view: View,

    /// Services list (cached)
    pub services: Vec<ServiceInfo>,

    /// Selected service index
    pub selected_service: Option<usize>,

    /// Log buffers per service
    pub log_buffers: HashMap<String, VecDeque<LogLine>>,

    /// Last seen log count per service (for deduplication)
    log_seen_count: HashMap<String, usize>,

    /// Previous service statuses (for detecting status changes)
    previous_status: HashMap<String, Status>,

    /// Current filter
    pub filter: String,

    /// Follow logs per service
    pub follow_logs: HashMap<String, bool>,

    /// Show help
    pub show_help: bool,

    /// Log scroll position per service
    pub log_scroll: HashMap<String, usize>,

    /// Log level filter per service (None = show all)
    pub log_level_filter: HashMap<String, LogLevel>,

    /// Search query per service
    pub log_search: HashMap<String, String>,

    /// Whether we're in search input mode
    pub search_mode: bool,

    /// Current search input buffer
    pub search_input: String,

    /// Terminal size
    pub terminal_width: u16,
    pub terminal_height: u16,

    /// Cached dependency graph (for synchronous drawing functions)
    pub dep_graph_cache: crate::dependency::Graph,

    /// Cached resolved parameters (for synchronous drawing functions)
    pub parameters_cache: HashMap<String, String>,

    /// Status message (transient)
    pub status_message: Option<StatusMessage>,

    /// Whether watch mode is enabled
    pub watch_mode_enabled: bool,

    /// Last restart time per service (for debouncing watch mode restarts)
    last_restart: HashMap<String, Instant>,

    /// Selected node index in dependency graph view
    pub graph_selected: usize,

    /// Selected parameter index in parameters view
    pub params_selected: usize,

    /// Filter for parameters view
    pub params_filter: String,

    /// Whether we're in filter input mode for params view
    pub params_filter_mode: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub enum View {
    Dashboard,
    ServiceDetails(String),
    Logs(String),
    DependencyGraph,
    Parameters,
}

#[derive(Debug, Clone)]
pub struct ServiceInfo {
    pub name: String,
    pub namespace: String,
    pub status: Status,
    pub service_type: String,
    pub port: Option<u16>,
    pub started_at: Option<chrono::DateTime<chrono::Utc>>,
    pub health_error: Option<String>,
}

#[derive(Debug, Clone)]
pub struct LogLine {
    pub timestamp: chrono::DateTime<chrono::Utc>,
    pub service: String,
    pub level: LogLevel,
    pub message: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum LogLevel {
    Debug,
    Info,
    Warning,
    Error,
}

impl LogLevel {
    /// Check if this log level should be shown given a minimum filter level
    pub fn passes_filter(&self, min_level: Option<&LogLevel>) -> bool {
        match min_level {
            None => true, // No filter, show all
            Some(min) => self >= min,
        }
    }

    /// Get display name for the filter
    pub fn filter_name(&self) -> &'static str {
        match self {
            LogLevel::Debug => "Debug+",
            LogLevel::Info => "Info+",
            LogLevel::Warning => "Warn+",
            LogLevel::Error => "Errors",
        }
    }
}

impl App {
    pub fn new(orchestrator: Orchestrator) -> Self {
        // Get dependency graph and parameters before wrapping in Arc<RwLock>
        let dep_graph = orchestrator.get_dependency_graph().clone();
        let parameters = orchestrator.get_resolved_parameters_owned();

        let orchestrator = Arc::new(RwLock::new(orchestrator));

        Self {
            orchestrator,
            view: View::Dashboard,
            services: Vec::new(),
            selected_service: Some(0),
            log_buffers: HashMap::new(),
            log_seen_count: HashMap::new(),
            previous_status: HashMap::new(),
            filter: String::new(),
            follow_logs: HashMap::new(),
            show_help: false,
            log_scroll: HashMap::new(),
            log_level_filter: HashMap::new(),
            log_search: HashMap::new(),
            search_mode: false,
            search_input: String::new(),
            terminal_width: 80,
            terminal_height: 24,
            dep_graph_cache: dep_graph,
            parameters_cache: parameters,
            status_message: None,
            watch_mode_enabled: false,
            last_restart: HashMap::new(),
            graph_selected: 0,
            params_selected: 0,
            params_filter: String::new(),
            params_filter_mode: false,
        }
    }

    /// Get reference to the orchestrator's work directory
    pub fn orchestrator(&self) -> std::sync::Arc<tokio::sync::RwLock<Orchestrator>> {
        self.orchestrator.clone()
    }

    /// Set whether watch mode is enabled
    pub fn set_watch_mode_enabled(&mut self, enabled: bool) {
        self.watch_mode_enabled = enabled;
    }

    /// Handle file change event from watch mode
    pub async fn handle_file_change(
        &mut self,
        event: crate::watch::FileChangeEvent,
    ) -> anyhow::Result<()> {
        let service_name = event.service_name.clone();
        let file_count = event.changed_paths.len();

        // Debounce: skip if service was restarted within the last 2 seconds
        const DEBOUNCE_DURATION: Duration = Duration::from_secs(2);
        if let Some(last) = self.last_restart.get(&service_name) {
            if last.elapsed() < DEBOUNCE_DURATION {
                tracing::debug!(
                    "Skipping restart of '{}' - debounced ({:?} since last restart)",
                    service_name,
                    last.elapsed()
                );
                return Ok(());
            }
        }

        // Record restart time
        self.last_restart
            .insert(service_name.clone(), Instant::now());

        // Show status message
        let msg = format!(
            "🔄 {} file(s) changed in '{}', restarting...",
            file_count, service_name
        );
        self.set_status(&msg, StatusLevel::Info, 3);

        // Restart the service
        let result = {
            let orchestrator = self.orchestrator.write().await;
            // Try to stop, but continue even if it fails
            let _ = orchestrator.stop(&service_name).await;
            orchestrator.start(&service_name).await
        };

        match result {
            Ok(_) => {
                let msg = format!("✓ '{}' restarted successfully", service_name);
                self.set_status(&msg, StatusLevel::Success, 3);
            }
            Err(e) => {
                let msg = format!("✗ Failed to restart '{}': {}", service_name, e);
                self.set_status(&msg, StatusLevel::Error, 5);
            }
        }

        Ok(())
    }

    /// Handle keyboard input
    pub async fn handle_key(&mut self, key: KeyEvent) -> anyhow::Result<bool> {
        // Global shortcuts
        if key.modifiers.contains(KeyModifiers::CONTROL) && key.code == KeyCode::Char('c') {
            if self.search_mode {
                self.exit_search_mode(false);
                return Ok(true);
            }
            return Ok(false); // Quit
        }

        // Handle search mode input
        if self.search_mode {
            match key.code {
                KeyCode::Enter => {
                    self.exit_search_mode(true);
                }
                KeyCode::Esc => {
                    self.exit_search_mode(false);
                }
                KeyCode::Backspace => {
                    self.search_input.pop();
                }
                KeyCode::Char(c) => {
                    self.search_input.push(c);
                }
                _ => {}
            }
            return Ok(true);
        }

        // Handle params filter mode input
        if self.params_filter_mode {
            match key.code {
                KeyCode::Enter | KeyCode::Esc => {
                    self.params_filter_mode = false;
                }
                KeyCode::Backspace => {
                    self.params_filter.pop();
                }
                KeyCode::Char(c) => {
                    self.params_filter.push(c);
                }
                _ => {}
            }
            return Ok(true);
        }

        match key.code {
            KeyCode::Char('q') if !self.show_help => return Ok(false), // Quit
            KeyCode::Char('?') => {
                self.show_help = !self.show_help;
                return Ok(true);
            }
            KeyCode::Esc => {
                if self.show_help {
                    self.show_help = false;
                } else {
                    self.view = View::Dashboard;
                }
                return Ok(true);
            }
            _ => {}
        }

        if self.show_help {
            return Ok(true); // Ignore other keys in help mode
        }

        // View-specific handlers
        match &self.view {
            View::Dashboard => self.handle_dashboard_key(key).await?,
            View::ServiceDetails(_) => self.handle_details_key(key).await?,
            View::Logs(_) => self.handle_logs_key(key).await?,
            View::DependencyGraph => self.handle_graph_key(key).await?,
            View::Parameters => self.handle_params_key(key).await?,
        }

        Ok(true)
    }

    /// Handle mouse input
    pub fn handle_mouse(&mut self, mouse: MouseEvent) {
        match mouse.kind {
            MouseEventKind::ScrollUp => {
                // Scroll up in logs view, or select previous service in dashboard
                match &self.view {
                    View::Logs(service) => {
                        // Disable follow when scrolling up
                        let service = service.clone();
                        self.follow_logs.insert(service, false);
                        self.scroll_logs_up();
                    }
                    View::Dashboard => {
                        self.select_previous_service();
                    }
                    View::ServiceDetails(_) => {
                        // Could add scroll for details view later
                    }
                    _ => {}
                }
            }
            MouseEventKind::ScrollDown => {
                // Scroll down in logs view, or select next service in dashboard
                match &self.view {
                    View::Logs(_) => {
                        self.scroll_logs_down();
                    }
                    View::Dashboard => {
                        self.select_next_service();
                    }
                    View::ServiceDetails(_) => {
                        // Could add scroll for details view later
                    }
                    _ => {}
                }
            }
            _ => {}
        }
    }

    async fn handle_dashboard_key(&mut self, key: KeyEvent) -> anyhow::Result<()> {
        match key.code {
            // Navigation
            KeyCode::Up | KeyCode::Char('k') => self.select_previous_service(),
            KeyCode::Down | KeyCode::Char('j') => self.select_next_service(),

            // Actions
            KeyCode::Enter => self.view_service_details()?,
            KeyCode::Char(' ') => self.toggle_service().await?,
            KeyCode::Char('r') => self.restart_service().await?,
            KeyCode::Char('s') => self.stop_all_services().await?,
            KeyCode::Char('S') => self.start_all_services().await?,

            // View switches
            KeyCode::Char('d') => self.view_service_details()?,
            KeyCode::Char('l') => self.view_logs()?,
            KeyCode::Char('g') => self.view = View::DependencyGraph,
            KeyCode::Char('p') => self.view = View::Parameters,

            // Other - toggle follow for selected service
            KeyCode::Char('f') => {
                if let Some(idx) = self.selected_service {
                    if let Some(service) = self.services.get(idx) {
                        let name = service.name.clone();
                        let current = self.is_following(&name);
                        self.follow_logs.insert(name, !current);
                    }
                }
            }

            _ => {}
        }
        Ok(())
    }

    async fn handle_details_key(&mut self, key: KeyEvent) -> anyhow::Result<()> {
        let service_name = if let View::ServiceDetails(name) = &self.view {
            name.clone()
        } else {
            return Ok(());
        };

        match key.code {
            KeyCode::Char('s') => {
                // Toggle start/stop
                if let Some(service) = self.services.iter().find(|s| s.name == service_name) {
                    let status = service.status;
                    let result = {
                        let orch = self.orchestrator.write().await;
                        match status {
                            Status::Running | Status::Healthy | Status::Failing => {
                                orch.stop(&service_name).await
                            }
                            Status::Stopped => orch.start(&service_name).await,
                            _ => Ok(()),
                        }
                    };
                    if let Err(e) = result {
                        self.set_status(
                            &format!("Failed to toggle '{}': {}", service_name, e),
                            StatusLevel::Error,
                            5,
                        );
                    }
                }
            }
            KeyCode::Char('r') => {
                // Restart service
                let result = {
                    let orch = self.orchestrator.write().await;
                    let _ = orch.stop(&service_name).await;
                    orch.start(&service_name).await
                };
                if let Err(e) = result {
                    self.set_status(
                        &format!("Failed to restart '{}': {}", service_name, e),
                        StatusLevel::Error,
                        5,
                    );
                }
            }
            KeyCode::Char('l') => {
                // View logs
                self.view = View::Logs(service_name);
            }
            _ => {}
        }
        Ok(())
    }

    async fn handle_logs_key(&mut self, key: KeyEvent) -> anyhow::Result<()> {
        match key.code {
            KeyCode::Char('f') => self.toggle_follow(),
            KeyCode::Up | KeyCode::Char('k') => self.scroll_logs_up(),
            KeyCode::Down | KeyCode::Char('j') => self.scroll_logs_down(),
            KeyCode::PageUp => self.scroll_logs_page_up(),
            KeyCode::PageDown => self.scroll_logs_page_down(),
            KeyCode::Char('g') => self.scroll_logs_top(),
            KeyCode::Char('G') => self.scroll_logs_bottom(),
            // Log level filtering
            KeyCode::Char('1') => self.set_log_filter(None), // All
            KeyCode::Char('2') => self.set_log_filter(Some(LogLevel::Debug)), // Debug+
            KeyCode::Char('3') => self.set_log_filter(Some(LogLevel::Info)), // Info+
            KeyCode::Char('4') => self.set_log_filter(Some(LogLevel::Warning)), // Warning+
            KeyCode::Char('5') => self.set_log_filter(Some(LogLevel::Error)), // Error only
            KeyCode::Tab => self.cycle_log_filter(),
            // Search
            KeyCode::Char('/') => self.enter_search_mode(),
            KeyCode::Char('c') => self.clear_search(),
            // Additional features
            KeyCode::Char('C') => self.clear_logs(),
            KeyCode::Char('e') => self.jump_to_next_error(),
            KeyCode::Char('E') => self.jump_to_prev_error(),
            _ => {}
        }
        Ok(())
    }

    async fn handle_graph_key(&mut self, key: KeyEvent) -> anyhow::Result<()> {
        let node_count = self.services.len();
        if node_count == 0 {
            return Ok(());
        }

        match key.code {
            // Navigation
            KeyCode::Up | KeyCode::Char('k') if self.graph_selected > 0 => {
                self.graph_selected -= 1;
            }
            KeyCode::Down | KeyCode::Char('j') if self.graph_selected + 1 < node_count => {
                self.graph_selected += 1;
            }
            KeyCode::Home | KeyCode::Char('g') => {
                self.graph_selected = 0;
            }
            KeyCode::End | KeyCode::Char('G') => {
                self.graph_selected = node_count.saturating_sub(1);
            }

            // Service control - get selected service name
            KeyCode::Char(' ') | KeyCode::Enter => {
                // Toggle start/stop for selected service
                if let Some(service) = self.services.get(self.graph_selected) {
                    let service_name = service.name.clone();
                    let status = service.status;
                    let result = {
                        let orch = self.orchestrator.write().await;
                        match status {
                            Status::Running | Status::Healthy => orch.stop(&service_name).await,
                            Status::Stopped | Status::Failing => orch.start(&service_name).await,
                            _ => Ok(()),
                        }
                    };
                    if let Err(e) = result {
                        self.set_status(
                            &format!("Failed to toggle '{}': {}", service_name, e),
                            StatusLevel::Error,
                            5,
                        );
                    }
                }
            }
            KeyCode::Char('r') => {
                // Restart selected service
                if let Some(service) = self.services.get(self.graph_selected) {
                    let service_name = service.name.clone();
                    let result = {
                        let orch = self.orchestrator.write().await;
                        let _ = orch.stop(&service_name).await;
                        orch.start(&service_name).await
                    };
                    if let Err(e) = result {
                        self.set_status(
                            &format!("Failed to restart '{}': {}", service_name, e),
                            StatusLevel::Error,
                            5,
                        );
                    }
                }
            }
            KeyCode::Char('l') => {
                // View logs for selected service
                if let Some(service) = self.services.get(self.graph_selected) {
                    self.view = View::Logs(service.name.clone());
                }
            }
            KeyCode::Char('d') => {
                // View details for selected service
                if let Some(service) = self.services.get(self.graph_selected) {
                    self.view = View::ServiceDetails(service.name.clone());
                }
            }

            _ => {}
        }
        Ok(())
    }

    /// Get the currently selected service name in graph view
    pub fn get_graph_selected_service(&self) -> Option<&str> {
        self.services
            .get(self.graph_selected)
            .map(|s| s.name.as_str())
    }

    async fn handle_params_key(&mut self, key: KeyEvent) -> anyhow::Result<()> {
        let filtered_params = self.get_filtered_params();
        let param_count = filtered_params.len();

        match key.code {
            // Navigation
            KeyCode::Up | KeyCode::Char('k') if self.params_selected > 0 => {
                self.params_selected -= 1;
            }
            KeyCode::Down | KeyCode::Char('j')
                if param_count > 0 && self.params_selected + 1 < param_count =>
            {
                self.params_selected += 1;
            }
            KeyCode::Home | KeyCode::Char('g') => {
                self.params_selected = 0;
            }
            KeyCode::End | KeyCode::Char('G') => {
                self.params_selected = param_count.saturating_sub(1);
            }
            KeyCode::PageUp => {
                self.params_selected = self.params_selected.saturating_sub(10);
            }
            KeyCode::PageDown if param_count > 0 => {
                self.params_selected = (self.params_selected + 10).min(param_count - 1);
            }

            // Filter
            KeyCode::Char('/') => {
                self.params_filter_mode = true;
            }
            KeyCode::Char('c') => {
                // Clear filter
                self.params_filter.clear();
                self.params_selected = 0;
            }

            // Copy value to clipboard (via OSC 52 escape sequence)
            KeyCode::Char('y') | KeyCode::Enter => {
                if let Some((key, value)) = filtered_params.get(self.params_selected) {
                    // Use OSC 52 escape sequence for clipboard copy
                    // This works in most modern terminals
                    let encoded = base64_encode(value.as_bytes());
                    print!("\x1b]52;c;{}\x07", encoded);
                    self.set_status(
                        &format!("Copied '{}' value to clipboard", key),
                        StatusLevel::Success,
                        3,
                    );
                }
            }

            // Copy key=value
            KeyCode::Char('Y') => {
                if let Some((key, value)) = filtered_params.get(self.params_selected) {
                    let full = format!("{}={}", key, value);
                    let encoded = base64_encode(full.as_bytes());
                    print!("\x1b]52;c;{}\x07", encoded);
                    self.set_status(
                        &format!("Copied '{}=...' to clipboard", key),
                        StatusLevel::Success,
                        3,
                    );
                }
            }

            _ => {}
        }
        Ok(())
    }

    /// Get filtered parameters list
    pub fn get_filtered_params(&self) -> Vec<(&String, &String)> {
        let filter_lower = self.params_filter.to_lowercase();
        self.parameters_cache
            .iter()
            .filter(|(k, v)| {
                if filter_lower.is_empty() {
                    true
                } else {
                    k.to_lowercase().contains(&filter_lower)
                        || v.to_lowercase().contains(&filter_lower)
                }
            })
            .collect()
    }

    /// Called on each tick (e.g., every 250ms)
    pub async fn on_tick(&mut self) -> anyhow::Result<()> {
        // Clear expired status messages
        if let Some(ref msg) = self.status_message {
            if Instant::now() > msg.expires_at {
                self.status_message = None;
            }
        }

        // Refresh service status
        self.refresh_services().await?;

        // Fetch new logs
        self.fetch_logs().await?;

        Ok(())
    }

    pub fn on_resize(&mut self, width: u16, height: u16) {
        self.terminal_width = width;
        self.terminal_height = height;
    }

    async fn refresh_services(&mut self) -> anyhow::Result<()> {
        // Acquire all needed data in a single scope, then drop locks before processing
        let (status_map, service_states) = {
            let orch = self.orchestrator.read().await;
            let status_map = orch.get_status().await;
            let state_tracker = orch.state_tracker.read().await;

            // Extract all service states we need while holding the lock
            let mut service_states: HashMap<String, _> = HashMap::new();
            for name in status_map.keys() {
                if let Some(state) = state_tracker.get_service(name).await {
                    service_states.insert(
                        name.clone(),
                        (
                            state.namespace.clone(),
                            state.service_type.to_string(),
                            state.port_allocations.values().next().copied(),
                            state.started_at,
                        ),
                    );
                }
            }

            // Drop locks by moving out of the scope
            (status_map, service_states)
        };

        // Process data without holding any locks
        self.services = status_map
            .into_iter()
            .map(|(name, status)| {
                // Detect status change from stopped to starting/running (restart)
                if let Some(&prev_status) = self.previous_status.get(&name) {
                    if matches!(prev_status, Status::Stopped | Status::Failing)
                        && matches!(status, Status::Starting | Status::Running | Status::Healthy)
                    {
                        // Service is restarting, reset log counter
                        self.log_seen_count.insert(name.clone(), 0);
                    }
                }

                // Update previous status
                self.previous_status.insert(name.clone(), status);

                let (namespace, service_type, port, started_at) =
                    service_states.get(&name).cloned().unwrap_or((
                        "root".to_string(),
                        "Unknown".to_string(),
                        None,
                        chrono::Utc::now(),
                    ));

                ServiceInfo {
                    name: name.clone(),
                    namespace,
                    status,
                    service_type,
                    port,
                    started_at: Some(started_at),
                    health_error: None,
                }
            })
            .collect();

        // Sort by name for consistent display
        self.services.sort_by(|a, b| a.name.cmp(&b.name));

        Ok(())
    }

    async fn fetch_logs(&mut self) -> anyhow::Result<()> {
        // Only fetch logs for the service currently being viewed to avoid
        // spawning docker/compose subprocesses for ALL services every tick
        let service_to_fetch = match &self.view {
            View::Logs(name) | View::ServiceDetails(name) => Some(name.clone()),
            View::Dashboard => {
                // In dashboard, fetch logs for selected service only
                self.selected_service
                    .and_then(|idx| self.services.get(idx))
                    .map(|s| s.name.clone())
            }
            _ => None,
        };

        if let Some(service_name) = service_to_fetch {
            let logs = {
                let orch = self.orchestrator.read().await;
                orch.get_logs(&service_name, Some(50))
                    .await
                    .unwrap_or_default()
            };

            let buffer = self
                .log_buffers
                .entry(service_name.clone())
                .or_insert_with(|| VecDeque::with_capacity(LOG_BUFFER_SIZE));

            // Only append new logs (those we haven't seen before)
            let prev_count = self.log_seen_count.get(&service_name).copied().unwrap_or(0);
            if logs.len() > prev_count {
                // Skip the logs we've already seen and add only new ones
                for line in logs.iter().skip(prev_count) {
                    let log_line = LogLine {
                        timestamp: chrono::Utc::now(),
                        service: service_name.clone(),
                        level: Self::parse_log_level(line),
                        message: line.clone(),
                    };

                    buffer.push_back(log_line);
                    if buffer.len() > LOG_BUFFER_SIZE {
                        buffer.pop_front();
                    }
                }
                // Update the seen count for this service
                self.log_seen_count.insert(service_name.clone(), logs.len());
            }
        }

        Ok(())
    }

    fn parse_log_level(line: &str) -> LogLevel {
        if contains_ci(line, b"error") || contains_ci(line, b"err]") {
            LogLevel::Error
        } else if contains_ci(line, b"warn") {
            LogLevel::Warning
        } else if contains_ci(line, b"debug") {
            LogLevel::Debug
        } else {
            LogLevel::Info
        }
    }

    fn select_next_service(&mut self) {
        if let Some(selected) = self.selected_service {
            if selected + 1 < self.services.len() {
                self.selected_service = Some(selected + 1);
            }
        } else if !self.services.is_empty() {
            self.selected_service = Some(0);
        }
    }

    fn select_previous_service(&mut self) {
        if let Some(selected) = self.selected_service {
            if selected > 0 {
                self.selected_service = Some(selected - 1);
            }
        }
    }

    fn view_service_details(&mut self) -> anyhow::Result<()> {
        if let Some(idx) = self.selected_service {
            if let Some(service) = self.services.get(idx) {
                self.view = View::ServiceDetails(service.name.clone());
            }
        }
        Ok(())
    }

    fn view_logs(&mut self) -> anyhow::Result<()> {
        if let Some(idx) = self.selected_service {
            if let Some(service) = self.services.get(idx) {
                self.view = View::Logs(service.name.clone());
            }
        }
        Ok(())
    }

    async fn toggle_service(&mut self) -> anyhow::Result<()> {
        if let Some(idx) = self.selected_service {
            if let Some(service) = self.services.get(idx) {
                let name = service.name.clone();
                let status = service.status;
                let result = {
                    let orch = self.orchestrator.write().await;
                    match status {
                        Status::Running | Status::Healthy => orch.stop(&name).await,
                        Status::Stopped => orch.start(&name).await,
                        _ => Ok(()),
                    }
                };
                if let Err(e) = result {
                    self.set_status(
                        &format!("Failed to toggle '{}': {}", name, e),
                        StatusLevel::Error,
                        5,
                    );
                }
            }
        }
        Ok(())
    }

    async fn restart_service(&mut self) -> anyhow::Result<()> {
        if let Some(idx) = self.selected_service {
            if let Some(service) = self.services.get(idx) {
                let name = service.name.clone();
                let result = {
                    let orch = self.orchestrator.write().await;
                    let _ = orch.stop(&name).await;
                    orch.start(&name).await
                };
                if let Err(e) = result {
                    self.set_status(
                        &format!("Failed to restart '{}': {}", name, e),
                        StatusLevel::Error,
                        5,
                    );
                }
            }
        }
        Ok(())
    }

    async fn stop_all_services(&mut self) -> anyhow::Result<()> {
        let orch = self.orchestrator.write().await;
        orch.stop_all().await?;
        Ok(())
    }

    async fn start_all_services(&mut self) -> anyhow::Result<()> {
        // Count stopped services
        let stopped: Vec<_> = self
            .services
            .iter()
            .filter(|s| matches!(s.status, Status::Stopped | Status::Failing))
            .collect();

        if stopped.is_empty() {
            self.set_status("All services already running", StatusLevel::Info, 3);
            return Ok(());
        }

        self.set_status(
            &format!("Starting {} services...", stopped.len()),
            StatusLevel::Info,
            30,
        );

        let result = {
            let orch = self.orchestrator.write().await;
            orch.start_all().await
        };

        match result {
            Ok(()) => self.set_status("All services started", StatusLevel::Success, 5),
            Err(e) => self.set_status(&format!("Failed: {}", e), StatusLevel::Error, 10),
        }

        Ok(())
    }

    /// Get the current service name if viewing logs
    fn current_log_service(&self) -> Option<&str> {
        if let View::Logs(ref name) = self.view {
            Some(name)
        } else {
            None
        }
    }

    /// Check if following logs for a service (defaults to true)
    pub fn is_following(&self, service: &str) -> bool {
        *self.follow_logs.get(service).unwrap_or(&true)
    }

    /// Toggle follow mode for current service
    fn toggle_follow(&mut self) {
        if let Some(service) = self.current_log_service() {
            let service = service.to_string();
            let current = self.is_following(&service);
            self.follow_logs.insert(service, !current);
        }
    }

    /// Get scroll position for a service (defaults to 0)
    pub fn get_scroll(&self, service: &str) -> usize {
        *self.log_scroll.get(service).unwrap_or(&0)
    }

    fn scroll_logs_up(&mut self) {
        if let Some(service) = self.current_log_service() {
            let service = service.to_string();
            let current = self.get_scroll(&service);
            if current > 0 {
                self.log_scroll.insert(service, current - 1);
            }
        }
    }

    fn scroll_logs_down(&mut self) {
        if let Some(service) = self.current_log_service() {
            let service = service.to_string();
            let current = self.get_scroll(&service);
            self.log_scroll.insert(service, current + 1);
        }
    }

    fn scroll_logs_page_up(&mut self) {
        if let Some(service) = self.current_log_service() {
            let service = service.to_string();
            let current = self.get_scroll(&service);
            self.log_scroll.insert(service, current.saturating_sub(10));
        }
    }

    fn scroll_logs_page_down(&mut self) {
        if let Some(service) = self.current_log_service() {
            let service = service.to_string();
            let current = self.get_scroll(&service);
            self.log_scroll.insert(service, current + 10);
        }
    }

    fn scroll_logs_top(&mut self) {
        if let Some(service) = self.current_log_service() {
            let service = service.to_string();
            self.log_scroll.insert(service, 0);
        }
    }

    fn scroll_logs_bottom(&mut self) {
        if let Some(service) = self.current_log_service() {
            let service = service.to_string();
            self.log_scroll.insert(service, usize::MAX);
        }
    }

    /// Get log level filter for a service (None means show all)
    pub fn get_log_filter(&self, service: &str) -> Option<&LogLevel> {
        self.log_level_filter.get(service)
    }

    /// Get search query for a service
    pub fn get_search(&self, service: &str) -> Option<&String> {
        self.log_search.get(service).filter(|s| !s.is_empty())
    }

    /// Enter search mode
    fn enter_search_mode(&mut self) {
        self.search_mode = true;
        // Pre-fill with existing search for this service
        if let Some(service) = self.current_log_service() {
            self.search_input = self.log_search.get(service).cloned().unwrap_or_default();
        }
    }

    /// Exit search mode and apply/cancel search
    fn exit_search_mode(&mut self, apply: bool) {
        self.search_mode = false;
        if apply {
            if let Some(service) = self.current_log_service() {
                let service = service.to_string();
                if self.search_input.is_empty() {
                    self.log_search.remove(&service);
                } else {
                    self.log_search.insert(service, self.search_input.clone());
                }
            }
        }
        self.search_input.clear();
    }

    /// Clear search for current service
    fn clear_search(&mut self) {
        if let Some(service) = self.current_log_service() {
            let service = service.to_string();
            self.log_search.remove(&service);
        }
    }

    /// Clear logs for current service
    fn clear_logs(&mut self) {
        if let Some(service) = self.current_log_service() {
            let service = service.to_string();
            self.log_buffers.remove(&service);
            self.log_seen_count.remove(&service);
            self.log_scroll.remove(&service);
        }
    }

    /// Jump to next error in logs
    fn jump_to_next_error(&mut self) {
        if let Some(service) = self.current_log_service() {
            let service = service.to_string();
            if let Some(buffer) = self.log_buffers.get(&service) {
                let current_scroll = self.get_scroll(&service);
                let level_filter = self.get_log_filter(&service);
                let search_query = self.get_search(&service).map(|s| s.to_lowercase());

                // Find filtered logs and their indices
                let filtered_with_idx: Vec<_> = buffer
                    .iter()
                    .enumerate()
                    .filter(|(_, log)| log.level.passes_filter(level_filter))
                    .filter(|(_, log)| {
                        search_query
                            .as_ref()
                            .map(|q| log.message.to_lowercase().contains(q))
                            .unwrap_or(true)
                    })
                    .collect();

                // Find next error after current scroll position
                for (filtered_idx, (_, log)) in filtered_with_idx.iter().enumerate() {
                    if filtered_idx > current_scroll && log.level == LogLevel::Error {
                        self.log_scroll.insert(service.clone(), filtered_idx);
                        self.follow_logs.insert(service, false);
                        return;
                    }
                }

                // Wrap around to find first error
                for (filtered_idx, (_, log)) in filtered_with_idx.iter().enumerate() {
                    if log.level == LogLevel::Error {
                        self.log_scroll.insert(service.clone(), filtered_idx);
                        self.follow_logs.insert(service, false);
                        return;
                    }
                }
            }
        }
    }

    /// Jump to previous error in logs
    fn jump_to_prev_error(&mut self) {
        if let Some(service) = self.current_log_service() {
            let service = service.to_string();
            if let Some(buffer) = self.log_buffers.get(&service) {
                let current_scroll = self.get_scroll(&service);
                let level_filter = self.get_log_filter(&service);
                let search_query = self.get_search(&service).map(|s| s.to_lowercase());

                // Find filtered logs and their indices
                let filtered_with_idx: Vec<_> = buffer
                    .iter()
                    .enumerate()
                    .filter(|(_, log)| log.level.passes_filter(level_filter))
                    .filter(|(_, log)| {
                        search_query
                            .as_ref()
                            .map(|q| log.message.to_lowercase().contains(q))
                            .unwrap_or(true)
                    })
                    .collect();

                // Find previous error before current scroll position
                for (filtered_idx, (_, log)) in filtered_with_idx.iter().enumerate().rev() {
                    if filtered_idx < current_scroll && log.level == LogLevel::Error {
                        self.log_scroll.insert(service.clone(), filtered_idx);
                        self.follow_logs.insert(service, false);
                        return;
                    }
                }

                // Wrap around to find last error
                for (filtered_idx, (_, log)) in filtered_with_idx.iter().enumerate().rev() {
                    if log.level == LogLevel::Error {
                        self.log_scroll.insert(service.clone(), filtered_idx);
                        self.follow_logs.insert(service, false);
                        return;
                    }
                }
            }
        }
    }

    /// Set log level filter for current service
    fn set_log_filter(&mut self, level: Option<LogLevel>) {
        if let Some(service) = self.current_log_service() {
            let service = service.to_string();
            if let Some(level) = level {
                self.log_level_filter.insert(service, level);
            } else {
                self.log_level_filter.remove(&service);
            }
        }
    }

    /// Cycle through log level filters: All -> Debug -> Info -> Warning -> Error -> All
    fn cycle_log_filter(&mut self) {
        if let Some(service) = self.current_log_service() {
            let service = service.to_string();
            let next = match self.log_level_filter.get(&service) {
                None => Some(LogLevel::Debug),
                Some(LogLevel::Debug) => Some(LogLevel::Info),
                Some(LogLevel::Info) => Some(LogLevel::Warning),
                Some(LogLevel::Warning) => Some(LogLevel::Error),
                Some(LogLevel::Error) => None,
            };
            if let Some(level) = next {
                self.log_level_filter.insert(service, level);
            } else {
                self.log_level_filter.remove(&service);
            }
        }
    }

    /// Set a status message that will expire after the given duration
    pub fn set_status(&mut self, text: &str, level: StatusLevel, duration_secs: u64) {
        self.status_message = Some(StatusMessage {
            text: text.to_string(),
            level,
            expires_at: Instant::now() + Duration::from_secs(duration_secs),
        });
    }
}

/// Simple base64 encoding for OSC 52 clipboard support
fn base64_encode(data: &[u8]) -> String {
    const ALPHABET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut result = String::new();

    for chunk in data.chunks(3) {
        let b0 = chunk[0] as usize;
        let b1 = chunk.get(1).copied().unwrap_or(0) as usize;
        let b2 = chunk.get(2).copied().unwrap_or(0) as usize;

        result.push(ALPHABET[b0 >> 2] as char);
        result.push(ALPHABET[((b0 & 0x03) << 4) | (b1 >> 4)] as char);

        if chunk.len() > 1 {
            result.push(ALPHABET[((b1 & 0x0f) << 2) | (b2 >> 6)] as char);
        } else {
            result.push('=');
        }

        if chunk.len() > 2 {
            result.push(ALPHABET[b2 & 0x3f] as char);
        } else {
            result.push('=');
        }
    }

    result
}
