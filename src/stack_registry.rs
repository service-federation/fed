//! Which checkout owns which Docker stack.
//!
//! Docker names a stack's containers and volumes `fed-<scope>-<name>`, where
//! `<scope>` is the checkout's isolation id (`iso-<8 hex>`) or, without
//! isolation, an 8-hex hash of the checkout's path. Neither can be turned back
//! into a path, so once a checkout is deleted nothing says the stack is
//! abandoned rather than merely stopped. This registry is that record: one
//! small file per scope under `~/.fed/stacks/`, written by every fed command
//! that builds an orchestrator, mapping the scope to the checkout directory.
//!
//! It is deliberately outside Docker. A stopped stack keeps its database
//! volume but loses its containers, and volumes made before this registry
//! existed carry no label that could be added later, so the only durable place
//! to remember "this volume belongs to /path/to/checkout" is a file fed owns.
//!
//! A scope with no entry is *unknown*, never abandoned: resources created by an
//! fed that predates the registry stay untouched until the checkout is
//! started or stopped by this version (which writes the entry) or the user
//! names the directories that hold their checkouts (`fed prune --adopt`).
//!
//! Two kinds of entry:
//!
//! * [`StackKind::Checkout`]: a stack that belongs to a directory. Live while
//!   the directory exists and, for an isolated stack, its `.fed/lock.db` still
//!   names this isolation id.
//! * [`StackKind::Script`]: the throwaway stack of one `isolated: true` script
//!   run. Its owner is a process, and a process can die without cleaning up
//!   (SIGKILL, a power cut), so liveness is an exclusive `flock` the running fed
//!   holds on `<scope>.lock` for the whole run. The kernel drops the lock when
//!   the process dies, whatever way it dies, so a lock anyone can take means the
//!   run is over. No pid is compared, so pid reuse cannot fake a live owner.

use fs2::FileExt;
use serde::{Deserialize, Serialize};
use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};

/// What a registry entry's scope belongs to.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum StackKind {
    /// A checkout's own stack (shared scope or `fed isolate enable`).
    Checkout,
    /// One `isolated: true` script run.
    Script,
}

/// One scope's record.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StackEntry {
    pub scope: String,
    pub kind: StackKind,
    pub workdir: PathBuf,
    pub fed_version: String,
    /// Unix seconds.
    pub first_seen: i64,
    /// Unix seconds; refreshed at most once an hour.
    pub last_seen: i64,
    /// The fed process that owns a script run (informational: liveness is the lock).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pid: Option<u32>,
}

/// How a checkout stands relative to a registered scope.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CheckoutState {
    /// The directory exists and still uses this scope.
    Current,
    /// The directory is gone (and its parent is there, so this is not an
    /// unmounted drive).
    Gone,
    /// The directory exists but its isolation id is no longer this one
    /// (`fed isolate rotate` or `disable`, or the path was reused).
    Superseded { current: Option<String> },
    /// Something prevents a verdict. Never a reason to delete.
    Unverifiable(String),
}

/// A scope fed can put in a file name and recognise in a Docker name:
/// `iso-<8 lowercase hex>` or `<8 lowercase hex>`.
pub fn is_valid_scope(scope: &str) -> bool {
    let hex = scope.strip_prefix("iso-").unwrap_or(scope);
    hex.len() == 8
        && hex
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

/// The scope a `fed-<scope>-<rest>` Docker name belongs to, if it has one.
pub fn scope_of_docker_name(name: &str) -> Option<String> {
    let rest = name.strip_prefix("fed-")?;
    let len = if rest.starts_with("iso-") { 12 } else { 8 };
    let scope = rest.get(..len)?;
    (is_valid_scope(scope) && rest[len..].starts_with('-')).then(|| scope.to_string())
}

fn now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

/// The registry directory.
#[derive(Debug, Clone)]
pub struct StackRegistry {
    dir: PathBuf,
}

impl StackRegistry {
    /// Environment variable that moves the registry, for tests and for anyone
    /// who keeps `~/.fed` somewhere else.
    pub const DIR_ENV: &'static str = "FED_STACK_REGISTRY_DIR";

    /// `~/.fed/stacks` (or `$FED_STACK_REGISTRY_DIR`), or `None` when there is
    /// no home directory.
    pub fn default_location() -> Option<Self> {
        if let Some(dir) = std::env::var_os(Self::DIR_ENV).filter(|d| !d.is_empty()) {
            return Some(Self::at(PathBuf::from(dir)));
        }
        dirs::home_dir().map(|home| Self::at(home.join(".fed").join("stacks")))
    }

    pub fn at(dir: PathBuf) -> Self {
        Self { dir }
    }

    fn entry_path(&self, scope: &str) -> PathBuf {
        self.dir.join(format!("{scope}.json"))
    }

    fn lock_path(&self, scope: &str) -> PathBuf {
        self.dir.join(format!("{scope}.lock"))
    }

    fn write_entry(&self, entry: &StackEntry) -> io::Result<()> {
        fs::create_dir_all(&self.dir)?;
        let path = self.entry_path(&entry.scope);
        let tmp = path.with_extension(format!("json.{:016x}.tmp", rand::random::<u64>()));
        let result = (|| {
            let mut file = OpenOptions::new().write(true).create_new(true).open(&tmp)?;
            file.write_all(&serde_json::to_vec_pretty(entry).map_err(io::Error::other)?)?;
            file.sync_all()?;
            fs::rename(&tmp, &path)
        })();
        if result.is_err() {
            let _ = fs::remove_file(&tmp);
        }
        result
    }

    /// The entry for `scope`, if there is a readable one.
    pub fn get(&self, scope: &str) -> Option<StackEntry> {
        if !is_valid_scope(scope) {
            return None;
        }
        let bytes = fs::read(self.entry_path(scope)).ok()?;
        serde_json::from_slice(&bytes).ok()
    }

    /// Every readable entry. A file that does not parse is skipped, not fatal.
    pub fn entries(&self) -> Vec<StackEntry> {
        let Ok(read) = fs::read_dir(&self.dir) else {
            return Vec::new();
        };
        let mut entries: Vec<StackEntry> = read
            .filter_map(Result::ok)
            .filter(|e| e.path().extension().is_some_and(|x| x == "json"))
            .filter_map(|e| serde_json::from_slice(&fs::read(e.path()).ok()?).ok())
            .collect();
        entries.sort_by(|a, b| a.scope.cmp(&b.scope));
        entries
    }

    /// Record that `scope` belongs to the checkout at `workdir`.
    ///
    /// Cheap enough for every command: an unchanged entry seen in the last hour
    /// is not rewritten. A script run's entry is never replaced by this.
    pub fn register_checkout(&self, workdir: &Path, scope: &str) -> io::Result<()> {
        if !is_valid_scope(scope) {
            return Ok(());
        }
        let workdir = fs::canonicalize(workdir).unwrap_or_else(|_| workdir.to_path_buf());
        let now = now();
        let first_seen = match self.get(scope) {
            Some(existing) if existing.kind == StackKind::Script => return Ok(()),
            Some(existing) if existing.workdir == workdir && now - existing.last_seen < 3600 => {
                return Ok(());
            }
            Some(existing) => existing.first_seen,
            None => now,
        };
        self.write_entry(&StackEntry {
            scope: scope.to_string(),
            kind: StackKind::Checkout,
            workdir,
            fed_version: env!("CARGO_PKG_VERSION").to_string(),
            first_seen,
            last_seen: now,
            pid: None,
        })
    }

    /// Claim `scope` for one `isolated: true` script run: take the run's lock,
    /// then record the entry. Call before the first container is created.
    pub fn begin_script_run(&self, workdir: &Path, scope: &str) -> io::Result<ScriptRun> {
        if !is_valid_scope(scope) {
            return Err(io::Error::other(format!("invalid stack scope '{scope}'")));
        }
        fs::create_dir_all(&self.dir)?;
        let lock = OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(self.lock_path(scope))?;
        lock.try_lock_exclusive()?;
        let workdir = fs::canonicalize(workdir).unwrap_or_else(|_| workdir.to_path_buf());
        let now = now();
        self.write_entry(&StackEntry {
            scope: scope.to_string(),
            kind: StackKind::Script,
            workdir,
            fed_version: env!("CARGO_PKG_VERSION").to_string(),
            first_seen: now,
            last_seen: now,
            pid: Some(std::process::id()),
        })?;
        Ok(ScriptRun {
            registry: self.clone(),
            scope: scope.to_string(),
            _lock: lock,
        })
    }

    /// Whether some live process still holds the run lock for `scope`.
    ///
    /// A missing lock file means nobody does: the run's process never got that
    /// far or something removed the file. Either way there is no owner.
    pub fn script_run_alive(&self, scope: &str) -> bool {
        if !is_valid_scope(scope) {
            return false;
        }
        let Ok(file) = File::open(self.lock_path(scope)) else {
            return false;
        };
        match file.try_lock_exclusive() {
            Ok(()) => {
                let _ = file.unlock();
                false
            }
            // Held by someone else, or unknowable: either way not provably dead.
            Err(_) => true,
        }
    }

    /// Forget `scope`: its entry and its lock file.
    pub fn remove(&self, scope: &str) {
        if is_valid_scope(scope) {
            let _ = fs::remove_file(self.entry_path(scope));
            let _ = fs::remove_file(self.lock_path(scope));
        }
    }

    /// Entries of script runs that no live process owns any more.
    pub fn dead_script_runs(&self) -> Vec<StackEntry> {
        self.entries()
            .into_iter()
            .filter(|e| e.kind == StackKind::Script && !self.script_run_alive(&e.scope))
            .collect()
    }

    /// Register every checkout found under `root` (see [`find_checkouts`]):
    /// its shared scope, and its current isolation id when it has one. Returns
    /// the checkouts found with the scopes registered for each.
    pub fn adopt(&self, root: &Path, max_depth: usize) -> Vec<(PathBuf, Vec<String>)> {
        let mut adopted = Vec::new();
        for dir in find_checkouts(root, max_depth) {
            let mut scopes = vec![crate::service::hash_work_dir(&dir)];
            if let Ok(Some(id)) = read_isolation_id(&dir)
                && is_valid_scope(&id)
            {
                scopes.push(id);
            }
            scopes.retain(|scope| self.register_checkout(&dir, scope).is_ok());
            adopted.push((dir, scopes));
        }
        adopted
    }
}

/// The lock a running `isolated: true` script holds. Dropping it without
/// [`ScriptRun::finish_clean`] releases the lock but keeps the entry, which is
/// what marks the run as one whose cleanup may be unfinished.
#[derive(Debug)]
pub struct ScriptRun {
    registry: StackRegistry,
    scope: String,
    _lock: File,
}

impl ScriptRun {
    pub fn scope(&self) -> &str {
        &self.scope
    }

    /// Cleanup is verified complete: forget the run.
    pub fn finish_clean(self) {
        self.registry.remove(&self.scope);
    }
}

/// The isolation id in `<workdir>/.fed/lock.db`, read without taking the
/// checkout's lock: `Ok(None)` when isolation is disabled.
pub fn read_isolation_id(workdir: &Path) -> Result<Option<String>, String> {
    let db = crate::fed_dir::fed_dir(workdir).join("lock.db");
    if !db.exists() {
        return Err(format!("{} does not exist", db.display()));
    }
    let conn = rusqlite::Connection::open_with_flags(
        &db,
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .map_err(|e| format!("opening {}: {e}", db.display()))?;
    conn.busy_timeout(std::time::Duration::from_secs(2)).ok();
    let setting = |key: &str| -> Result<Option<String>, String> {
        match conn.query_row(
            "SELECT value FROM project_settings WHERE key = ?1",
            [key],
            |row| row.get::<_, String>(0),
        ) {
            Ok(v) => Ok(Some(v)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(format!("reading {}: {e}", db.display())),
        }
    };
    if setting("isolation_enabled")?.as_deref() != Some("true") {
        return Ok(None);
    }
    setting("isolation_id")
}

/// Where a registered scope stands now.
pub fn checkout_state(entry: &StackEntry) -> CheckoutState {
    let dir = &entry.workdir;
    if !dir.exists() {
        return match dir.parent() {
            Some(parent) if parent.exists() => CheckoutState::Gone,
            _ => CheckoutState::Unverifiable(format!(
                "{} and its parent directory are both missing (an unmounted drive?)",
                dir.display()
            )),
        };
    }
    if entry.scope.starts_with("iso-") {
        match read_isolation_id(dir) {
            Ok(Some(id)) if id == entry.scope => CheckoutState::Current,
            Ok(current) => CheckoutState::Superseded { current },
            Err(e) => CheckoutState::Unverifiable(e),
        }
    } else if crate::service::hash_work_dir(dir) == entry.scope {
        CheckoutState::Current
    } else {
        CheckoutState::Unverifiable(format!(
            "{} no longer hashes to {}",
            dir.display(),
            entry.scope
        ))
    }
}

/// Directories under `root` (at most `max_depth` levels down) that hold a fed
/// state database, i.e. checkouts fed has run in. Symlinks are not followed and
/// dependency and build directories are skipped.
pub fn find_checkouts(root: &Path, max_depth: usize) -> Vec<PathBuf> {
    const SKIP: &[&str] = &["node_modules", ".git", "target", ".next", ".cache", ".fed"];
    let mut found = Vec::new();
    let mut stack = vec![(root.to_path_buf(), 0usize)];
    while let Some((dir, depth)) = stack.pop() {
        if crate::fed_dir::fed_dir(&dir).join("lock.db").is_file() {
            found.push(dir.clone());
        }
        if depth >= max_depth {
            continue;
        }
        let Ok(read) = fs::read_dir(&dir) else {
            continue;
        };
        for entry in read.filter_map(Result::ok) {
            let Ok(kind) = entry.file_type() else {
                continue;
            };
            let name = entry.file_name();
            if kind.is_dir() && !SKIP.iter().any(|s| name == *s) {
                stack.push((entry.path(), depth + 1));
            }
        }
    }
    found.sort();
    found
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scopes_are_recognised_in_docker_names() {
        assert_eq!(
            scope_of_docker_name("fed-iso-2a09648b-redis").as_deref(),
            Some("iso-2a09648b")
        );
        assert_eq!(
            scope_of_docker_name("fed-0a1b2c3d-postgres").as_deref(),
            Some("0a1b2c3d")
        );
        assert_eq!(
            scope_of_docker_name("fed-iso-2a09648b-pg-data").as_deref(),
            Some("iso-2a09648b")
        );
        for not_ours in [
            "fed-backup",
            "fed-iso-2a09648b",
            "fed-iso-2A09648B-redis",
            "fed-iso-2a09648-redis",
            "redis",
            "fed-0a1b2c3dx-redis",
            "fed-iso-é-redis",
        ] {
            assert_eq!(scope_of_docker_name(not_ours), None, "{not_ours}");
        }
    }

    #[test]
    fn scopes_that_would_escape_the_directory_are_rejected() {
        assert!(!is_valid_scope("../../etc"));
        assert!(!is_valid_scope("iso-../../x"));
        assert!(is_valid_scope("iso-deadbeef"));
        assert!(is_valid_scope("00d5fbcb"));
    }

    #[test]
    fn a_checkout_registers_once_and_is_not_rewritten_within_the_hour() {
        let tmp = tempfile::tempdir().unwrap();
        let registry = StackRegistry::at(tmp.path().join("stacks"));
        let work = tempfile::tempdir().unwrap();
        registry
            .register_checkout(work.path(), "iso-deadbeef")
            .unwrap();
        let first = registry.get("iso-deadbeef").unwrap();
        assert_eq!(first.kind, StackKind::Checkout);
        assert_eq!(first.workdir, fs::canonicalize(work.path()).unwrap());

        let path = registry.entry_path("iso-deadbeef");
        let before = fs::metadata(&path).unwrap().modified().unwrap();
        registry
            .register_checkout(work.path(), "iso-deadbeef")
            .unwrap();
        assert_eq!(fs::metadata(&path).unwrap().modified().unwrap(), before);

        // The same scope seen from another path (a moved checkout) is rewritten.
        let other = tempfile::tempdir().unwrap();
        registry
            .register_checkout(other.path(), "iso-deadbeef")
            .unwrap();
        let moved = registry.get("iso-deadbeef").unwrap();
        assert_eq!(moved.workdir, fs::canonicalize(other.path()).unwrap());
        assert_eq!(moved.first_seen, first.first_seen);
    }

    #[test]
    fn a_live_script_run_cannot_be_overwritten_by_a_checkout_registration() {
        let tmp = tempfile::tempdir().unwrap();
        let registry = StackRegistry::at(tmp.path().to_path_buf());
        let work = tempfile::tempdir().unwrap();
        let run = registry
            .begin_script_run(work.path(), "iso-cafef00d")
            .unwrap();
        registry
            .register_checkout(work.path(), "iso-cafef00d")
            .unwrap();
        assert_eq!(
            registry.get("iso-cafef00d").unwrap().kind,
            StackKind::Script
        );
        drop(run);
    }

    #[test]
    fn a_script_run_is_alive_exactly_while_its_lock_is_held() {
        let tmp = tempfile::tempdir().unwrap();
        let registry = StackRegistry::at(tmp.path().to_path_buf());
        let work = tempfile::tempdir().unwrap();

        assert!(
            !registry.script_run_alive("iso-cafef00d"),
            "no run, no owner"
        );
        let run = registry
            .begin_script_run(work.path(), "iso-cafef00d")
            .unwrap();
        assert!(registry.script_run_alive("iso-cafef00d"));
        assert!(registry.dead_script_runs().is_empty());
        assert!(
            registry
                .begin_script_run(work.path(), "iso-cafef00d")
                .is_err(),
            "two runs cannot claim one scope"
        );

        // Dropping without finish_clean is what a crash looks like from outside:
        // the lock goes, the entry stays.
        drop(run);
        assert!(!registry.script_run_alive("iso-cafef00d"));
        let dead = registry.dead_script_runs();
        assert_eq!(dead.len(), 1);
        assert_eq!(dead[0].scope, "iso-cafef00d");
        assert_eq!(dead[0].pid, Some(std::process::id()));

        let run = registry
            .begin_script_run(work.path(), "iso-cafef00d")
            .unwrap();
        run.finish_clean();
        assert!(registry.get("iso-cafef00d").is_none());
        assert!(registry.dead_script_runs().is_empty());
    }

    #[test]
    fn a_checkout_directory_is_current_gone_or_unverifiable() {
        let tmp = tempfile::tempdir().unwrap();
        let work = tmp.path().join("checkout");
        fs::create_dir(&work).unwrap();
        let scope = crate::service::hash_work_dir(&work);
        let entry = |workdir: &Path, scope: &str| StackEntry {
            scope: scope.to_string(),
            kind: StackKind::Checkout,
            workdir: workdir.to_path_buf(),
            fed_version: "8.3.1".into(),
            first_seen: 0,
            last_seen: 0,
            pid: None,
        };

        assert_eq!(
            checkout_state(&entry(&work, &scope)),
            CheckoutState::Current
        );
        assert!(matches!(
            checkout_state(&entry(&work, "deadbeef")),
            CheckoutState::Unverifiable(_)
        ));
        // An isolated stack whose checkout has no state database cannot be judged.
        assert!(matches!(
            checkout_state(&entry(&work, "iso-deadbeef")),
            CheckoutState::Unverifiable(_)
        ));

        fs::remove_dir(&work).unwrap();
        assert_eq!(checkout_state(&entry(&work, &scope)), CheckoutState::Gone);
        assert!(
            matches!(
                checkout_state(&entry(&tmp.path().join("no/such/parent/checkout"), &scope)),
                CheckoutState::Unverifiable(_)
            ),
            "a missing parent may be an unmounted drive, not a deleted checkout"
        );
    }

    fn write_lock_db(workdir: &Path, enabled: bool, id: Option<&str>) {
        let dir = workdir.join(".fed");
        fs::create_dir_all(&dir).unwrap();
        let conn = rusqlite::Connection::open(dir.join("lock.db")).unwrap();
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS project_settings (key TEXT PRIMARY KEY, value TEXT, updated_at TEXT);",
        )
        .unwrap();
        conn.execute(
            "INSERT OR REPLACE INTO project_settings (key, value) VALUES ('isolation_enabled', ?1)",
            [enabled.to_string()],
        )
        .unwrap();
        if let Some(id) = id {
            conn.execute(
                "INSERT OR REPLACE INTO project_settings (key, value) VALUES ('isolation_id', ?1)",
                [id],
            )
            .unwrap();
        }
    }

    #[test]
    fn an_isolated_scope_is_superseded_when_the_checkout_moved_on() {
        let tmp = tempfile::tempdir().unwrap();
        let work = tmp.path().join("checkout");
        fs::create_dir(&work).unwrap();
        write_lock_db(&work, true, Some("iso-11111111"));
        let entry = |scope: &str| StackEntry {
            scope: scope.to_string(),
            kind: StackKind::Checkout,
            workdir: work.clone(),
            fed_version: "8.4.0".into(),
            first_seen: 0,
            last_seen: 0,
            pid: None,
        };
        assert_eq!(
            checkout_state(&entry("iso-11111111")),
            CheckoutState::Current
        );
        assert_eq!(
            checkout_state(&entry("iso-22222222")),
            CheckoutState::Superseded {
                current: Some("iso-11111111".into())
            }
        );
        write_lock_db(&work, false, None);
        assert_eq!(
            checkout_state(&entry("iso-11111111")),
            CheckoutState::Superseded { current: None },
            "`fed isolate disable` retires the id"
        );
    }

    #[test]
    fn adopting_a_root_registers_the_checkouts_under_it_and_skips_dependencies() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join("worktrees");
        let a = root.join("proj/a");
        let b = root.join("proj/b");
        let junk = root.join("proj/a/node_modules/pkg");
        for dir in [&a, &b, &junk] {
            fs::create_dir_all(dir).unwrap();
        }
        write_lock_db(&a, true, Some("iso-aaaaaaaa"));
        write_lock_db(&b, false, None);
        write_lock_db(&junk, true, Some("iso-bbbbbbbb"));

        let registry = StackRegistry::at(tmp.path().join("stacks"));
        let adopted = registry.adopt(&root, 4);
        assert_eq!(
            adopted.len(),
            2,
            "node_modules is not a checkout: {adopted:?}"
        );
        assert!(registry.get("iso-aaaaaaaa").is_some());
        assert!(registry.get("iso-bbbbbbbb").is_none());
        assert!(registry.get(&crate::service::hash_work_dir(&a)).is_some());
        assert!(registry.get(&crate::service::hash_work_dir(&b)).is_some());

        assert_eq!(registry.adopt(&root, 1).len(), 0, "depth is bounded");
    }
}
