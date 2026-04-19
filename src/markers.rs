use crate::error::{Error, Result};
use std::fs;
use std::path::{Path, PathBuf};
use std::time::SystemTime;

/// Sanitize service name for safe filesystem usage.
///
/// Prevents path traversal attacks by:
/// - Rejecting empty names
/// - Rejecting names with path separators (/ or \)
/// - Rejecting names starting with dots (., ..)
/// - Replacing remaining special characters with underscores
fn sanitize_service_name_for_path(service_name: &str) -> Result<String> {
    if service_name.is_empty() {
        return Err(Error::Config("Service name cannot be empty".to_string()));
    }

    if service_name.contains('/') || service_name.contains('\\') {
        return Err(Error::Config(format!(
            "Service name '{}' contains path separators",
            service_name
        )));
    }

    if service_name.starts_with('.') {
        return Err(Error::Config(format!(
            "Service name '{}' cannot start with a dot",
            service_name
        )));
    }

    let sanitized: String = service_name
        .chars()
        .map(|c| match c {
            c if c.is_alphanumeric() || c == '-' || c == '_' => c,
            _ => '_',
        })
        .collect();

    Ok(sanitized)
}

/// Sanitize an isolation id for safe filesystem usage. Same rules as service names.
fn sanitize_isolation_id_for_path(id: &str) -> Result<String> {
    if id.is_empty() {
        return Err(Error::Config("Isolation ID cannot be empty".to_string()));
    }
    if id.contains('/') || id.contains('\\') {
        return Err(Error::Config(format!(
            "Isolation ID '{}' contains path separators",
            id
        )));
    }
    if id.starts_with('.') {
        return Err(Error::Config(format!(
            "Isolation ID '{}' cannot start with a dot",
            id
        )));
    }
    Ok(id
        .chars()
        .map(|c| match c {
            c if c.is_alphanumeric() || c == '-' || c == '_' => c,
            _ => '_',
        })
        .collect())
}

fn fed_home() -> Result<PathBuf> {
    let home = dirs::home_dir()
        .ok_or_else(|| Error::Config("Could not determine home directory".to_string()))?;
    Ok(home.join(".fed"))
}

/// Get the installed directory for a (work_dir, isolation_id) scope.
///
/// Non-isolated markers live at `~/.fed/installed/<hash>/` (unchanged layout).
/// Isolated markers live at `~/.fed/isolated/installed/<hash>/<id>/` — a disjoint
/// tree so `clear_all_*` on either scope cannot touch the other.
fn scoped_installed_dir(work_dir: &Path, isolation_id: Option<&str>) -> Result<PathBuf> {
    let hash = crate::service::hash_work_dir(work_dir);
    let base = fed_home()?;
    Ok(match isolation_id {
        None => base.join("installed").join(hash),
        Some(id) => {
            let sanitized = sanitize_isolation_id_for_path(id)?;
            base.join("isolated")
                .join("installed")
                .join(hash)
                .join(sanitized)
        }
    })
}

/// Get the migrated directory for a (work_dir, isolation_id) scope. See
/// [`scoped_installed_dir`] for the layout rationale.
fn scoped_migrated_dir(work_dir: &Path, isolation_id: Option<&str>) -> Result<PathBuf> {
    let hash = crate::service::hash_work_dir(work_dir);
    let base = fed_home()?;
    Ok(match isolation_id {
        None => base.join("migrated").join(hash),
        Some(id) => {
            let sanitized = sanitize_isolation_id_for_path(id)?;
            base.join("isolated")
                .join("migrated")
                .join(hash)
                .join(sanitized)
        }
    })
}

fn write_marker(dir: PathBuf, service_name: &str, kind: &str) -> Result<()> {
    fs::create_dir_all(&dir)
        .map_err(|e| Error::Filesystem(format!("Failed to create {} directory: {}", kind, e)))?;
    let sanitized = sanitize_service_name_for_path(service_name)?;
    let marker_file = dir.join(sanitized);
    let timestamp = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_else(|_| std::time::Duration::from_secs(0))
        .as_secs();
    fs::write(&marker_file, timestamp.to_string())
        .map_err(|e| Error::Filesystem(format!("Failed to create {} marker: {}", kind, e)))?;
    Ok(())
}

fn marker_exists(dir: PathBuf, service_name: &str) -> Result<bool> {
    let sanitized = sanitize_service_name_for_path(service_name)?;
    Ok(dir.join(sanitized).exists())
}

fn remove_marker(dir: PathBuf, service_name: &str, kind: &str) -> Result<()> {
    let sanitized = sanitize_service_name_for_path(service_name)?;
    let marker_file = dir.join(sanitized);
    match fs::remove_file(&marker_file) {
        Ok(_) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(Error::Filesystem(format!(
            "Failed to remove {} marker: {}",
            kind, e
        ))),
    }
}

fn remove_dir(dir: PathBuf, kind: &str) -> Result<()> {
    match fs::remove_dir_all(&dir) {
        Ok(_) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(Error::Filesystem(format!(
            "Failed to remove {} markers: {}",
            kind, e
        ))),
    }
}

/// Lifecycle markers for install/migrate state tracking.
///
/// Markers are scoped by `(work_dir, isolation_id)`. A `None` isolation id
/// refers to shared (non-isolated) containers; a `Some(id)` refers to the
/// ephemeral containers created for that isolation session. The two scopes
/// live in disjoint filesystem subtrees so operations on one never touch the
/// other.
pub struct LifecycleMarkers {
    work_dir: PathBuf,
    isolation_id: Option<String>,
}

impl LifecycleMarkers {
    /// Create a markers context scoped to a work directory and optional isolation id.
    ///
    /// Pass `None` for the isolation id to operate on the shared (non-isolated)
    /// marker namespace used by default `fed start`. Pass `Some(id)` to operate
    /// on an isolation session's namespace — ephemeral children created by
    /// `isolated: true` scripts, or the persisted isolation session set via
    /// `fed isolate enable`.
    pub fn new(work_dir: PathBuf, isolation_id: Option<String>) -> Self {
        Self {
            work_dir,
            isolation_id,
        }
    }

    fn installed_dir(&self) -> Result<PathBuf> {
        scoped_installed_dir(&self.work_dir, self.isolation_id.as_deref())
    }

    fn migrated_dir(&self) -> Result<PathBuf> {
        scoped_migrated_dir(&self.work_dir, self.isolation_id.as_deref())
    }

    /// Check if a service has been installed in this scope.
    pub fn is_installed(&self, service_name: &str) -> Result<bool> {
        marker_exists(self.installed_dir()?, service_name)
    }

    /// Mark a service as installed in this scope.
    pub fn mark_installed(&self, service_name: &str) -> Result<()> {
        write_marker(self.installed_dir()?, service_name, "installed")
    }

    /// Clear install state for a service in this scope.
    pub fn clear_installed(&self, service_name: &str) -> Result<()> {
        remove_marker(self.installed_dir()?, service_name, "install")
    }

    /// Check if a service has been migrated in this scope.
    pub fn is_migrated(&self, service_name: &str) -> Result<bool> {
        marker_exists(self.migrated_dir()?, service_name)
    }

    /// Mark a service as migrated in this scope.
    pub fn mark_migrated(&self, service_name: &str) -> Result<()> {
        write_marker(self.migrated_dir()?, service_name, "migrated")
    }

    /// Clear migrate state for a service in this scope.
    pub fn clear_migrated(&self, service_name: &str) -> Result<()> {
        remove_marker(self.migrated_dir()?, service_name, "migrate")
    }

    /// Clear all install markers in this scope.
    ///
    /// Only affects the current `(work_dir, isolation_id)` tuple — markers in
    /// other isolation scopes are untouched.
    pub fn clear_all_installed(&self) -> Result<()> {
        remove_dir(self.installed_dir()?, "install")
    }

    /// Clear all migrate markers in this scope.
    ///
    /// Only affects the current `(work_dir, isolation_id)` tuple — markers in
    /// other isolation scopes are untouched.
    pub fn clear_all_migrated(&self) -> Result<()> {
        remove_dir(self.migrated_dir()?, "migrate")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn shared(dir: &tempfile::TempDir) -> LifecycleMarkers {
        LifecycleMarkers::new(dir.path().to_path_buf(), None)
    }

    fn isolated(dir: &tempfile::TempDir, id: &str) -> LifecycleMarkers {
        LifecycleMarkers::new(dir.path().to_path_buf(), Some(id.to_string()))
    }

    #[test]
    fn test_lifecycle_markers_shared_mode() {
        let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
        let ctx = shared(&temp_dir);

        let service_name = "test-service-shared";
        let _ = ctx.clear_installed(service_name);

        assert!(!ctx.is_installed(service_name).unwrap());
        ctx.mark_installed(service_name).unwrap();
        assert!(ctx.is_installed(service_name).unwrap());
        ctx.clear_installed(service_name).unwrap();
        assert!(!ctx.is_installed(service_name).unwrap());
    }

    #[test]
    fn test_shared_markers_isolated_by_work_dir() {
        let dir_a = tempfile::tempdir().unwrap();
        let dir_b = tempfile::tempdir().unwrap();
        let ctx_a = shared(&dir_a);
        let ctx_b = shared(&dir_b);

        let svc = "test-isolated-service";
        let _ = ctx_a.clear_installed(svc);
        let _ = ctx_b.clear_installed(svc);

        ctx_a.mark_installed(svc).unwrap();
        assert!(ctx_a.is_installed(svc).unwrap());
        assert!(!ctx_b.is_installed(svc).unwrap());

        let _ = ctx_a.clear_installed(svc);
    }

    #[test]
    fn test_lifecycle_markers_shared_migrate_tracking() {
        let temp_dir = tempfile::tempdir().unwrap();
        let ctx = shared(&temp_dir);

        let svc = "test-service-migrate";
        let _ = ctx.clear_migrated(svc);

        assert!(!ctx.is_migrated(svc).unwrap());
        ctx.mark_migrated(svc).unwrap();
        assert!(ctx.is_migrated(svc).unwrap());
        ctx.clear_migrated(svc).unwrap();
        assert!(!ctx.is_migrated(svc).unwrap());
    }

    #[test]
    fn test_shared_migrate_markers_isolated_by_work_dir() {
        let dir_a = tempfile::tempdir().unwrap();
        let dir_b = tempfile::tempdir().unwrap();
        let ctx_a = shared(&dir_a);
        let ctx_b = shared(&dir_b);

        let svc = "test-isolated-migrate";
        let _ = ctx_a.clear_migrated(svc);
        let _ = ctx_b.clear_migrated(svc);

        ctx_a.mark_migrated(svc).unwrap();
        assert!(ctx_a.is_migrated(svc).unwrap());
        assert!(!ctx_b.is_migrated(svc).unwrap());

        let _ = ctx_a.clear_migrated(svc);
    }

    #[test]
    fn test_clear_all_installed_removes_all_markers() {
        let temp_dir = tempfile::tempdir().unwrap();
        let ctx = shared(&temp_dir);

        ctx.mark_installed("svc-a").unwrap();
        ctx.mark_installed("svc-b").unwrap();
        ctx.mark_installed("svc-c").unwrap();
        assert!(ctx.is_installed("svc-a").unwrap());
        assert!(ctx.is_installed("svc-b").unwrap());
        assert!(ctx.is_installed("svc-c").unwrap());

        ctx.clear_all_installed().unwrap();
        assert!(!ctx.is_installed("svc-a").unwrap());
        assert!(!ctx.is_installed("svc-b").unwrap());
        assert!(!ctx.is_installed("svc-c").unwrap());
    }

    #[test]
    fn test_clear_all_migrated_removes_all_markers() {
        let temp_dir = tempfile::tempdir().unwrap();
        let ctx = shared(&temp_dir);

        ctx.mark_migrated("svc-a").unwrap();
        ctx.mark_migrated("svc-b").unwrap();
        ctx.mark_migrated("svc-c").unwrap();
        assert!(ctx.is_migrated("svc-a").unwrap());
        assert!(ctx.is_migrated("svc-b").unwrap());
        assert!(ctx.is_migrated("svc-c").unwrap());

        ctx.clear_all_migrated().unwrap();
        assert!(!ctx.is_migrated("svc-a").unwrap());
        assert!(!ctx.is_migrated("svc-b").unwrap());
        assert!(!ctx.is_migrated("svc-c").unwrap());
    }

    #[test]
    fn test_clear_all_is_idempotent_on_empty() {
        let temp_dir = tempfile::tempdir().unwrap();
        let ctx = shared(&temp_dir);
        ctx.clear_all_installed().unwrap();
        ctx.clear_all_migrated().unwrap();
    }

    /// Shared and isolated scopes for the same work_dir must not share state.
    /// This is the whole point of scoping — a script with `isolated: true`
    /// should see an empty marker namespace regardless of what the shared
    /// namespace contains, and clearing the isolation scope should never
    /// touch the shared scope.
    #[test]
    fn test_shared_and_isolated_scopes_are_disjoint() {
        let temp_dir = tempfile::tempdir().unwrap();
        let shared_ctx = shared(&temp_dir);
        let iso_ctx = isolated(&temp_dir, "iso-test1234");

        shared_ctx.mark_installed("api").unwrap();
        shared_ctx.mark_migrated("api").unwrap();

        // Isolated scope must be empty even though shared has markers
        assert!(!iso_ctx.is_installed("api").unwrap());
        assert!(!iso_ctx.is_migrated("api").unwrap());

        // Writing to isolated scope must not leak back to shared scope
        iso_ctx.mark_installed("api").unwrap();
        iso_ctx.mark_migrated("api").unwrap();
        assert!(shared_ctx.is_installed("api").unwrap());
        assert!(shared_ctx.is_migrated("api").unwrap());

        // Clearing isolated scope must not touch shared scope — this is the
        // regression guard: previously `clear_all_*` on work_dir wiped every
        // marker in that work_dir, so an isolated cleanup would also wipe
        // the parent's shared markers.
        iso_ctx.clear_all_installed().unwrap();
        iso_ctx.clear_all_migrated().unwrap();
        assert!(shared_ctx.is_installed("api").unwrap());
        assert!(shared_ctx.is_migrated("api").unwrap());
        assert!(!iso_ctx.is_installed("api").unwrap());
        assert!(!iso_ctx.is_migrated("api").unwrap());

        // Cleanup
        let _ = shared_ctx.clear_installed("api");
        let _ = shared_ctx.clear_migrated("api");
    }

    /// Two different isolation ids on the same work_dir must not share state.
    #[test]
    fn test_distinct_isolation_ids_are_disjoint() {
        let temp_dir = tempfile::tempdir().unwrap();
        let a = isolated(&temp_dir, "iso-aaaaaaaa");
        let b = isolated(&temp_dir, "iso-bbbbbbbb");

        a.mark_installed("api").unwrap();
        assert!(a.is_installed("api").unwrap());
        assert!(!b.is_installed("api").unwrap());

        a.clear_all_installed().unwrap();
        assert!(!a.is_installed("api").unwrap());
    }

    #[test]
    fn test_sanitize_isolation_id_rejects_path_traversal() {
        let temp_dir = tempfile::tempdir().unwrap();
        let ctx = isolated(&temp_dir, "../evil");
        // Any path-returning operation should error rather than resolve outside the scope root
        assert!(ctx.is_installed("any").is_err());
    }
}
