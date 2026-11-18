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
use std::collections::HashSet;
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

/// A directory that uses a scope, with the device its parent was on when it
/// registered (Unix), so a later "the directory is gone" can be told apart
/// from "the drive it lives on is not mounted".
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Claim {
    pub path: PathBuf,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_dev: Option<u64>,
}

/// One scope's record.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StackEntry {
    pub scope: String,
    pub kind: StackKind,
    /// The directory that registered the scope most recently.
    pub workdir: PathBuf,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_dev: Option<u64>,
    /// Other directories that registered the same scope and still existed when
    /// `workdir` took over: a copied checkout carries its `.fed/lock.db`, and
    /// so its isolation id, with it. The stack is abandoned only when every
    /// claim is.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub also: Vec<Claim>,
    pub fed_version: String,
    /// Unix seconds.
    pub first_seen: i64,
    /// Unix seconds; refreshed at most once an hour.
    pub last_seen: i64,
    /// The fed process that owns a script run (informational: liveness is the lock).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pid: Option<u32>,
    /// Whether a script run's stack can have anything in Docker. A run of a
    /// project without Docker-backed services has nothing for a sweep to clean.
    #[serde(default = "yes")]
    pub uses_docker: bool,
}

fn yes() -> bool {
    true
}

impl StackEntry {
    /// Every directory that claims this scope, most recent first.
    pub fn claims(&self) -> Vec<Claim> {
        let mut claims = vec![Claim {
            path: self.workdir.clone(),
            parent_dev: self.parent_dev,
        }];
        claims.extend(self.also.iter().cloned());
        claims
    }
}

/// The device of `path`'s parent directory, where the platform has one.
fn parent_device(path: &Path) -> Option<u64> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        path.parent()
            .and_then(|parent| fs::metadata(parent).ok())
            .map(|m| m.dev())
    }
    #[cfg(not(unix))]
    {
        let _ = path;
        None
    }
}

/// Whether `path` exists, without following a final symlink, and without
/// reading "permission denied" as "does not exist".
fn path_presence(path: &Path) -> io::Result<bool> {
    match fs::symlink_metadata(path) {
        Ok(_) => Ok(true),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(e) => Err(e),
    }
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
    /// is not rewritten. A script run's entry is never replaced by this. When
    /// another directory that still exists already claims the scope (a copied
    /// checkout), both claims are kept.
    pub fn register_checkout(&self, workdir: &Path, scope: &str) -> io::Result<()> {
        if !is_valid_scope(scope) {
            return Ok(());
        }
        let workdir = fs::canonicalize(workdir).unwrap_or_else(|_| workdir.to_path_buf());
        let now = now();
        let (first_seen, also) = match self.get(scope) {
            Some(existing) if existing.kind == StackKind::Script => return Ok(()),
            Some(existing) if existing.workdir == workdir && now - existing.last_seen < 3600 => {
                return Ok(());
            }
            Some(existing) => {
                let mut seen = HashSet::new();
                seen.insert(workdir.clone());
                let also = existing
                    .claims()
                    .into_iter()
                    .filter(|claim| seen.insert(claim.path.clone()))
                    .filter(|claim| matches!(path_presence(&claim.path), Ok(true) | Err(_)))
                    .collect();
                (existing.first_seen, also)
            }
            None => (now, Vec::new()),
        };
        self.write_entry(&StackEntry {
            scope: scope.to_string(),
            kind: StackKind::Checkout,
            parent_dev: parent_device(&workdir),
            workdir,
            also,
            fed_version: env!("CARGO_PKG_VERSION").to_string(),
            first_seen,
            last_seen: now,
            pid: None,
            uses_docker: true,
        })
    }

    /// Claim `scope` for one `isolated: true` script run: take the run's lock,
    /// then record the entry. Call before the first container is created.
    ///
    /// `uses_docker` says whether the run's stack can put anything in Docker.
    pub fn begin_script_run(
        &self,
        workdir: &Path,
        scope: &str,
        uses_docker: bool,
    ) -> io::Result<ScriptRun> {
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
            parent_dev: parent_device(&workdir),
            workdir,
            also: Vec::new(),
            fed_version: env!("CARGO_PKG_VERSION").to_string(),
            first_seen: now,
            last_seen: now,
            pid: Some(std::process::id()),
            uses_docker,
        })?;
        Ok(ScriptRun {
            registry: self.clone(),
            scope: scope.to_string(),
            uses_docker,
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
    ///
    /// Runs on every orchestrated command, so it looks only at the `.lock`
    /// files (one per script run, however many checkouts are registered).
    pub fn dead_script_runs(&self) -> Vec<StackEntry> {
        let Ok(read) = fs::read_dir(&self.dir) else {
            return Vec::new();
        };
        let mut dead: Vec<StackEntry> = read
            .filter_map(Result::ok)
            .filter_map(|e| {
                let name = e.file_name();
                let scope = name.to_str()?.strip_suffix(".lock")?.to_string();
                is_valid_scope(&scope).then_some(scope)
            })
            .filter(|scope| !self.script_run_alive(scope))
            .filter_map(|scope| self.get(&scope))
            .filter(|e| e.kind == StackKind::Script)
            .collect();
        dead.sort_by(|a, b| a.scope.cmp(&b.scope));
        dead
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
    uses_docker: bool,
    _lock: File,
}

impl ScriptRun {
    pub fn scope(&self) -> &str {
        &self.scope
    }

    pub fn uses_docker(&self) -> bool {
        self.uses_docker
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

/// Where a registered scope stands now: the best state of any directory that
/// claims it. One current claim keeps the stack; it is gone only when every
/// claim is.
pub fn checkout_state(entry: &StackEntry) -> CheckoutState {
    let states: Vec<CheckoutState> = entry
        .claims()
        .iter()
        .map(|claim| claim_state(&entry.scope, claim))
        .collect();
    if states.contains(&CheckoutState::Current) {
        return CheckoutState::Current;
    }
    if let Some(unverifiable) = states
        .iter()
        .find(|s| matches!(s, CheckoutState::Unverifiable(_)))
    {
        return unverifiable.clone();
    }
    if let Some(superseded) = states
        .iter()
        .find(|s| matches!(s, CheckoutState::Superseded { .. }))
    {
        return superseded.clone();
    }
    CheckoutState::Gone
}

fn claim_state(scope: &str, claim: &Claim) -> CheckoutState {
    let dir = &claim.path;
    match path_presence(dir) {
        Ok(true) => {}
        Ok(false) => {
            let Some(parent) = dir.parent() else {
                return CheckoutState::Unverifiable(format!("{} has no parent", dir.display()));
            };
            return match path_presence(parent) {
                Ok(true) => match (claim.parent_dev, parent_device(dir)) {
                    (Some(then), Some(now)) if then != now => CheckoutState::Unverifiable(format!(
                        "{} is now on a different filesystem than when fed last saw {} \
                             (an unmounted drive?)",
                        parent.display(),
                        dir.display()
                    )),
                    _ => CheckoutState::Gone,
                },
                Ok(false) => CheckoutState::Unverifiable(format!(
                    "{} and its parent directory are both missing (an unmounted drive?)",
                    dir.display()
                )),
                Err(e) => CheckoutState::Unverifiable(format!("{}: {e}", parent.display())),
            };
        }
        Err(e) => return CheckoutState::Unverifiable(format!("{}: {e}", dir.display())),
    }
    if scope.starts_with("iso-") {
        match read_isolation_id(dir) {
            Ok(Some(id)) if id == scope => CheckoutState::Current,
            Ok(current) => CheckoutState::Superseded { current },
            Err(e) => CheckoutState::Unverifiable(e),
        }
    } else if crate::service::hash_work_dir(dir) == scope {
        CheckoutState::Current
    } else {
        CheckoutState::Unverifiable(format!("{} no longer hashes to {}", dir.display(), scope))
    }
}

/// Directories under `root` (at most `max_depth` levels down) that hold a fed
/// state database, i.e. checkouts fed has run in. Symlinked directories are
/// followed (a checkout is often reached through one) but each real directory
/// is visited once; dependency and build directories are skipped.
pub fn find_checkouts(root: &Path, max_depth: usize) -> Vec<PathBuf> {
    const SKIP: &[&str] = &["node_modules", ".git", "target", ".next", ".cache", ".fed"];
    let mut found = Vec::new();
    let mut visited: HashSet<PathBuf> = HashSet::new();
    let root = fs::canonicalize(root).unwrap_or_else(|_| root.to_path_buf());
    let mut stack = vec![(root, 0usize)];
    while let Some((dir, depth)) = stack.pop() {
        if !visited.insert(dir.clone()) {
            continue;
        }
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
            if SKIP.iter().any(|s| name == *s) {
                continue;
            }
            let is_dir = kind.is_dir() || (kind.is_symlink() && entry.path().is_dir());
            if is_dir && let Ok(real) = fs::canonicalize(entry.path()) {
                stack.push((real, depth + 1));
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
            .begin_script_run(work.path(), "iso-cafef00d", true)
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
            .begin_script_run(work.path(), "iso-cafef00d", true)
            .unwrap();
        assert!(registry.script_run_alive("iso-cafef00d"));
        assert!(registry.dead_script_runs().is_empty());
        assert!(
            registry
                .begin_script_run(work.path(), "iso-cafef00d", true)
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
            .begin_script_run(work.path(), "iso-cafef00d", true)
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
            parent_dev: None,
            also: Vec::new(),
            uses_docker: true,
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
            parent_dev: None,
            also: Vec::new(),
            uses_docker: true,
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

    #[test]
    fn a_copied_checkout_keeps_the_stack_while_either_copy_exists() {
        let tmp = tempfile::tempdir().unwrap();
        let registry = StackRegistry::at(tmp.path().join("stacks"));
        let original = tmp.path().join("original");
        let copy = tmp.path().join("copy");
        for dir in [&original, &copy] {
            fs::create_dir(dir).unwrap();
            write_lock_db(dir, true, Some("iso-cccccccc"));
        }
        registry
            .register_checkout(&original, "iso-cccccccc")
            .unwrap();
        registry.register_checkout(&copy, "iso-cccccccc").unwrap();
        let entry = registry.get("iso-cccccccc").unwrap();
        assert_eq!(entry.claims().len(), 2, "{entry:?}");

        fs::remove_dir_all(&copy).unwrap();
        assert_eq!(
            checkout_state(&registry.get("iso-cccccccc").unwrap()),
            CheckoutState::Current,
            "deleting the copy must not abandon the original's stack"
        );
        fs::remove_dir_all(&original).unwrap();
        assert_eq!(
            checkout_state(&registry.get("iso-cccccccc").unwrap()),
            CheckoutState::Gone
        );
    }

    #[test]
    fn a_claim_whose_parent_moved_to_another_filesystem_cannot_be_judged() {
        // An unmounted drive leaves its empty mount point behind: the
        // directory is missing but its parent exists, now on the root
        // filesystem instead of the drive's.
        let tmp = tempfile::tempdir().unwrap();
        let gone = tmp.path().join("checkout");
        let mut entry = StackEntry {
            scope: "iso-dddddddd".into(),
            kind: StackKind::Checkout,
            workdir: gone.clone(),
            parent_dev: parent_device(&gone),
            also: Vec::new(),
            fed_version: "x".into(),
            first_seen: 0,
            last_seen: 0,
            pid: None,
            uses_docker: true,
        };
        assert_eq!(checkout_state(&entry), CheckoutState::Gone);
        if let Some(dev) = entry.parent_dev {
            entry.parent_dev = Some(dev.wrapping_add(1));
            assert!(matches!(
                checkout_state(&entry),
                CheckoutState::Unverifiable(_)
            ));
        }
    }

    #[test]
    fn adopt_follows_a_symlinked_checkout_once() {
        let tmp = tempfile::tempdir().unwrap();
        let real = tmp.path().join("data/proj");
        fs::create_dir_all(&real).unwrap();
        write_lock_db(&real, false, None);
        let root = tmp.path().join("code");
        fs::create_dir(&root).unwrap();
        #[cfg(unix)]
        std::os::unix::fs::symlink(&real, root.join("proj")).unwrap();
        #[cfg(unix)]
        std::os::unix::fs::symlink(&root, root.join("loop")).unwrap();
        #[cfg(unix)]
        assert_eq!(
            find_checkouts(&root, 4),
            vec![fs::canonicalize(&real).unwrap()]
        );
    }

    #[test]
    fn only_lock_files_are_read_to_find_dead_runs() {
        let tmp = tempfile::tempdir().unwrap();
        let registry = StackRegistry::at(tmp.path().to_path_buf());
        let work = tempfile::tempdir().unwrap();
        for i in 0..50 {
            registry
                .register_checkout(work.path(), &format!("{i:08x}"))
                .unwrap();
        }
        drop(
            registry
                .begin_script_run(work.path(), "iso-eeeeeeee", false)
                .unwrap(),
        );
        let dead = registry.dead_script_runs();
        assert_eq!(dead.len(), 1);
        assert!(!dead[0].uses_docker);
    }
}
