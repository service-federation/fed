use crate::error::Result;
use std::collections::HashMap;

/// Unified port storage abstraction.
///
/// The resolver uses this trait to read/write port allocations without
/// knowing the backend. This eliminates the dual-cache priority chain
/// that previously caused ports to survive a `fed ports reset`.
///
/// # Implementations
///
/// - [`SqlitePortStore`] — wraps ports loaded from the `persisted_ports` table
/// - [`NoopPortStore`] — for isolated mode (test scripts); discards everything
pub trait PortStore: Send + Sync {
    /// Look up a previously allocated port for a parameter.
    fn get_port(&self, param_name: &str) -> Option<u16>;

    /// Save a port allocation. Called after a port is resolved (whether from
    /// cache, default, or random allocation).
    fn save_port(&mut self, param_name: &str, port: u16) -> Result<()>;

    /// Get all stored port allocations.
    fn get_all_ports(&self) -> HashMap<String, u16>;
}

/// Port store backed by ports loaded from SQLite `persisted_ports` table.
///
/// Reads from an in-memory `HashMap` loaded at init time. Writes accumulate
/// in memory — the caller is responsible for flushing back to SQLite via
/// `save_port_resolutions` on the state tracker (this happens in
/// `Orchestrator::initialize`).
pub struct SqlitePortStore {
    ports: HashMap<String, u16>,
}

impl SqlitePortStore {
    pub fn new(ports: HashMap<String, u16>) -> Self {
        Self { ports }
    }
}

impl PortStore for SqlitePortStore {
    fn get_port(&self, param_name: &str) -> Option<u16> {
        self.ports.get(param_name).copied()
    }

    fn save_port(&mut self, param_name: &str, port: u16) -> Result<()> {
        self.ports.insert(param_name.to_string(), port);
        Ok(())
    }

    fn get_all_ports(&self) -> HashMap<String, u16> {
        self.ports.clone()
    }
}

/// No-op port store for isolated mode (test scripts).
///
/// Always returns `None` for lookups and discards saves. This forces
/// fresh random port allocation for every parameter, giving each
/// isolated script its own port space.
pub struct NoopPortStore;

impl PortStore for NoopPortStore {
    fn get_port(&self, _param_name: &str) -> Option<u16> {
        None
    }

    fn save_port(&mut self, _param_name: &str, _port: u16) -> Result<()> {
        Ok(())
    }

    fn get_all_ports(&self) -> HashMap<String, u16> {
        HashMap::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_noop_store_always_empty() {
        let mut store = NoopPortStore;
        assert!(store.get_port("any").is_none());
        store.save_port("any", 8080).unwrap();
        assert!(store.get_port("any").is_none());
        assert!(store.get_all_ports().is_empty());
    }

    #[test]
    fn test_sqlite_store_read_write() {
        let mut ports = HashMap::new();
        ports.insert("db_port".to_string(), 5432u16);
        let mut store = SqlitePortStore::new(ports);

        assert_eq!(store.get_port("db_port"), Some(5432));
        assert_eq!(store.get_port("unknown"), None);

        store.save_port("web_port", 3000).unwrap();
        assert_eq!(store.get_port("web_port"), Some(3000));

        let all = store.get_all_ports();
        assert_eq!(all.len(), 2);
        assert_eq!(all["db_port"], 5432);
        assert_eq!(all["web_port"], 3000);
    }
}
