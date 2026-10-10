//! `fed remote`: disposable machines in Service Federation Cloud that run a
//! checkout's fed stack.
//!
//! The cloud API lives in `fed::cloud::remote`. This module holds what runs on
//! this machine: the SSH keys and state under `~/.fed/remote/<id>/`, the shared
//! SSH connection, the file copy with rsync and the port forwards.
//!
//! Tokens never appear in a process argument. The cloud token travels in HTTP
//! headers, and a vault token travels to the machine over SSH stdin.

use crate::cli::RemoteCommands;
use crate::output::UserOutput;
use anyhow::{Context, Result, anyhow, bail};
use fed::cloud::{self, remote as api};
use serde::{Deserialize, Serialize};
use std::ffi::OsString;
use std::os::unix::fs::{DirBuilderExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::process::{ExitStatus, Stdio};
use std::time::{Duration, Instant};
use tokio::process::Command;

/// How long `fed remote up` waits for SSH to answer.
const SSH_WAIT: Duration = Duration::from_secs(300);
/// How long an environment lives without an SSH session.
const IDLE_MINUTES: u32 = 5;
/// How long an environment lives in any case.
const LIFETIME_HOURS: u32 = 6;
/// Remote port P is forwarded from local P + PORT_OFFSET, so the same stack can
/// run on this machine at the same time.
const PORT_OFFSET: u16 = 10_000;
/// How long `connect` waits before it reports the forwards as up. A forward
/// whose local port is taken fails within this time.
const FORWARD_SETTLE: Duration = Duration::from_secs(1);
/// rsync's exit status when some files vanished during the copy. The rest
/// were copied.
const RSYNC_VANISHED: i32 = 24;
/// rsync gives up when no data moves for this many seconds.
const RSYNC_IO_TIMEOUT: u32 = 120;

const STATE_FILE: &str = "env.json";
/// `fed remote up` builds a new environment's state in `.new-<random>` and
/// renames it to the id once the cloud has answered.
const STAGING_PREFIX: &str = ".new-";
/// A staging directory older than this belongs to an `up` that died. It may
/// hold a host private key, so the next `up` deletes it.
const STALE_STAGING: Duration = Duration::from_secs(15 * 60);
/// The list of files the last push copied, on the machine, relative to the
/// workspace. fed never copies `.fed/` there except `.fed/cloud.yaml`, so
/// the list is not overwritten by a push.
const PUSH_MANIFEST: &str = ".fed/remote-push-files";

// ── Local state (~/.fed/remote/<id>/) ────────────────────────────────

/// What this machine remembers about one environment, in `env.json`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct EnvState {
    pub id: String,
    pub name: String,
    pub org: String,
    pub project: String,
    pub ip: String,
    pub port: u16,
    pub deadline: String,
}

impl EnvState {
    fn project_ref(&self) -> api::ProjectRef {
        api::ProjectRef {
            org: self.org.clone(),
            project: self.project.clone(),
        }
    }

    fn in_project(&self, project: &api::ProjectRef) -> bool {
        self.org == project.org && self.project == project.project
    }
}

/// The vault token minted for one workspace, in `tokens/<workspace>.json`.
/// Only the id: the token itself lives on the remote machine.
#[derive(Debug, Serialize, Deserialize)]
struct TokenRecord {
    id: String,
    org: String,
    project: String,
}

fn state_root() -> Result<PathBuf> {
    let home = dirs::home_dir().context("cannot find the home directory")?;
    Ok(home.join(".fed").join("remote"))
}

/// Create `dir` (and missing parents) and make sure only the owner can enter it.
pub(crate) fn create_private_dir(dir: &Path) -> Result<()> {
    if let Some(parent) = dir.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("creating {}", parent.display()))?;
    }
    match std::fs::DirBuilder::new().mode(0o700).create(dir) {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {}
        Err(e) => return Err(e).with_context(|| format!("creating {}", dir.display())),
    }
    let meta =
        std::fs::symlink_metadata(dir).with_context(|| format!("reading {}", dir.display()))?;
    if !meta.is_dir() {
        bail!("{} is not a directory", dir.display());
    }
    std::fs::set_permissions(dir, std::fs::Permissions::from_mode(0o700))
        .with_context(|| format!("restricting {}", dir.display()))?;
    Ok(())
}

fn write_private(path: &Path, contents: &[u8]) -> Result<()> {
    fed::fsutil::write_owner_only_atomic(path, contents, false)?;
    Ok(())
}

fn read_state(dir: &Path) -> Option<EnvState> {
    let raw = std::fs::read_to_string(dir.join(STATE_FILE)).ok()?;
    serde_json::from_str(&raw).ok()
}

/// Every environment this machine has keys for, with its state directory.
fn local_environments(root: &Path) -> Vec<(PathBuf, EnvState)> {
    let Ok(entries) = std::fs::read_dir(root) else {
        return Vec::new();
    };
    let mut found: Vec<_> = entries
        .flatten()
        .filter(|e| !e.file_name().to_string_lossy().starts_with('.'))
        .map(|e| e.path())
        .filter_map(|dir| read_state(&dir).map(|state| (dir, state)))
        .collect();
    found.sort_by(|a, b| a.1.name.cmp(&b.1.name).then(a.1.id.cmp(&b.1.id)));
    found
}

/// Delete the staging directories of `up` runs that died more than
/// `STALE_STAGING` ago. A younger one may belong to an `up` still running.
fn remove_stale_staging(root: &Path, now: std::time::SystemTime) {
    let Ok(entries) = std::fs::read_dir(root) else {
        return;
    };
    for entry in entries.flatten() {
        if !entry
            .file_name()
            .to_string_lossy()
            .starts_with(STAGING_PREFIX)
        {
            continue;
        }
        let old = entry
            .metadata()
            .and_then(|m| m.modified())
            .ok()
            .and_then(|modified| now.duration_since(modified).ok())
            .is_some_and(|age| age > STALE_STAGING);
        if old {
            let _ = std::fs::remove_dir_all(entry.path());
        }
    }
}

/// What the local state knows about an environment name.
#[derive(Debug)]
pub(crate) enum Lookup {
    Found(PathBuf, EnvState),
    Missing,
    /// Several environments by that name, and the checkout's link does not
    /// pick one.
    Ambiguous(Vec<(PathBuf, EnvState)>),
}

/// The environment called `name`. When several have that name, the
/// checkout's link decides.
pub(crate) fn lookup(root: &Path, name: &str, project: Option<&api::ProjectRef>) -> Lookup {
    let mut matches: Vec<_> = local_environments(root)
        .into_iter()
        .filter(|(_, s)| s.name == name)
        .collect();
    if matches.len() > 1
        && let Some(project) = project
    {
        let linked: Vec<_> = matches
            .iter()
            .filter(|(_, s)| s.in_project(project))
            .cloned()
            .collect();
        if !linked.is_empty() {
            matches = linked;
        }
    }
    match matches.len() {
        0 => Lookup::Missing,
        1 => {
            let (dir, state) = matches.remove(0);
            Lookup::Found(dir, state)
        }
        _ => Lookup::Ambiguous(matches),
    }
}

fn no_such_environment(name: &str) -> anyhow::Error {
    anyhow!(
        "this machine has no environment called {name}. See `fed remote ls`, or create it with `fed remote up {name}`."
    )
}

/// The environment called `name`. When the name is ambiguous, environments
/// the cloud no longer has are forgotten first.
async fn find_environment(
    root: &Path,
    name: &str,
    project: Option<&api::ProjectRef>,
) -> Result<(PathBuf, EnvState)> {
    match lookup(root, name, project) {
        Lookup::Found(dir, state) => return Ok((dir, state)),
        Lookup::Missing => return Err(no_such_environment(name)),
        Lookup::Ambiguous(found) => {
            if let Ok(creds) = credentials() {
                let states: Vec<EnvState> = found.into_iter().map(|(_, s)| s).collect();
                forget_deleted(&creds, root, &states).await;
            }
        }
    }
    match lookup(root, name, project) {
        Lookup::Found(dir, state) => Ok((dir, state)),
        Lookup::Missing => Err(no_such_environment(name)),
        Lookup::Ambiguous(_) => bail!(
            "several environments are called {name}. Run this from a checkout linked to the project of the one you mean."
        ),
    }
}

/// Forget every local environment in the projects of `states` that the cloud
/// no longer lists. A project whose list fails is left alone.
async fn forget_deleted(creds: &cloud::Credentials, root: &Path, states: &[EnvState]) {
    let mut projects: Vec<api::ProjectRef> = Vec::new();
    for state in states {
        let project = state.project_ref();
        if !projects.contains(&project) {
            projects.push(project);
        }
    }
    for project in projects {
        let Ok(list) = api::list_environments(creds, &project).await else {
            continue;
        };
        for (dir, state) in local_environments(root) {
            if state.in_project(&project) && !list.iter().any(|e| e.id == state.id) {
                forget(Some(creds), &dir, &state).await;
            }
        }
    }
}

/// Remove this machine's state for an environment: revoke its vault tokens
/// where the cloud still knows them, close the shared connection and delete
/// the directory. Every step is best effort. A token left behind expires at
/// the environment's deadline.
async fn forget(creds: Option<&cloud::Credentials>, dir: &Path, state: &EnvState) {
    if let Some(creds) = creds
        && let Ok(entries) = std::fs::read_dir(dir.join("tokens"))
    {
        for entry in entries.flatten() {
            let _ = revoke_token_file(creds, &entry.path()).await;
        }
    }
    let control = control_path(dir, &state.id);
    if control.exists() {
        close_connection(&control, &state.ip).await;
    }
    let _ = std::fs::remove_dir_all(dir);
}

// ── Pure helpers ──────────────────────────────────────────────────────

/// Workspace names on the remote machine: the folder under `/srv/`. They go
/// into shell commands and paths there, so only a safe set is allowed.
pub(crate) fn valid_workspace_name(name: &str) -> bool {
    let bytes = name.as_bytes();
    matches!(bytes.first(), Some(c) if c.is_ascii_alphanumeric())
        && bytes.len() <= 64
        && bytes
            .iter()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, b'.' | b'_' | b'-'))
}

const INVALID_WORKSPACE_NAME: &str =
    "Use 1 to 64 letters, digits, '.', '_' and '-', starting with a letter or digit.";

/// The workspace name: `--as`, or else the checkout's folder name.
fn workspace_name(checkout: &Path, given: Option<&str>) -> Result<String> {
    if let Some(name) = given {
        return checked_workspace(name);
    }
    let folder = checkout
        .file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_default();
    if !valid_workspace_name(&folder) {
        bail!(
            "the checkout folder `{folder}` cannot be a workspace name. Pass one with --as. {INVALID_WORKSPACE_NAME}"
        );
    }
    Ok(folder)
}

fn checked_workspace(name: &str) -> Result<String> {
    if !valid_workspace_name(name) {
        bail!("`{name}` cannot be a workspace name. {INVALID_WORKSPACE_NAME}");
    }
    Ok(name.to_string())
}

/// Whether a path from `git ls-files` goes to the remote machine. Never
/// anything in `.git`, and from `.fed/` only the root `.fed/cloud.yaml`, the
/// committed link to the team vault.
pub(crate) fn keep_path(path: &[u8]) -> bool {
    if path.is_empty() {
        return false;
    }
    if path == b".fed/cloud.yaml" {
        return true;
    }
    let mut components = path.split(|c| *c == b'/');
    let last = components.next_back();
    let dirs_ok = components.all(|c| c != b".git" && c != b".fed");
    dirs_ok && last != Some(b".git")
}

/// Filter the NUL-separated output of `git ls-files -z`.
pub(crate) fn filter_file_list(raw: &[u8]) -> Vec<Vec<u8>> {
    raw.split(|c| *c == 0)
        .filter(|p| keep_path(p))
        .map(<[u8]>::to_vec)
        .collect()
}

/// The submodule paths in the output of `git ls-files -z --stage`. A
/// submodule is an entry with mode 160000.
pub(crate) fn submodule_paths(staged: &[u8]) -> Vec<Vec<u8>> {
    staged
        .split(|c| *c == 0)
        .filter_map(|entry| {
            let tab = entry.iter().position(|c| *c == b'\t')?;
            entry
                .starts_with(b"160000 ")
                .then(|| entry[tab + 1..].to_vec())
        })
        .collect()
}

/// The line for the environment's `known_hosts`. The IP and port are in the
/// `[host]:port` form ssh uses for a port other than 22. The key comment is
/// left out.
pub(crate) fn known_hosts_line(ip: &str, port: u16, host_public_key: &str) -> Result<String> {
    let mut fields = host_public_key.split_whitespace();
    let (Some(kind), Some(key)) = (fields.next(), fields.next()) else {
        bail!("the host public key is malformed");
    };
    if ip.is_empty() || ip.contains(|c: char| c.is_whitespace() || c == '[' || c == ']') {
        bail!("Service Federation Cloud sent a malformed IP address `{ip}` for the environment.");
    }
    // ssh looks up port 22 by the bare address.
    if port == 22 {
        return Ok(format!("{ip} {kind} {key}\n"));
    }
    Ok(format!("[{ip}]:{port} {kind} {key}\n"))
}

/// The local port that forwards to remote port `remote`.
pub(crate) fn local_port(remote: u16) -> u16 {
    remote.checked_add(PORT_OFFSET).unwrap_or(remote)
}

/// The ports in the JSON of `fed ports list --json` (`{"PARAM": port}`), each
/// with the parameters that use it, sorted by port.
pub(crate) fn parse_ports(json: &str) -> Result<Vec<(u16, Vec<String>)>> {
    let map: std::collections::BTreeMap<String, u16> = serde_json::from_str(json.trim())
        .context("`fed ports list --json` on the environment printed something unexpected")?;
    let mut by_port: std::collections::BTreeMap<u16, Vec<String>> = Default::default();
    for (param, port) in map {
        by_port.entry(port).or_default().push(param);
    }
    Ok(by_port.into_iter().collect())
}

/// Seconds from `now` until `deadline`, as a vault token lifetime. Capped at
/// the cloud's maximum.
pub(crate) fn token_ttl(deadline: &str, now: chrono::DateTime<chrono::Utc>) -> Result<i64> {
    let deadline = chrono::DateTime::parse_from_rfc3339(deadline).with_context(|| {
        format!("Service Federation Cloud sent a malformed deadline `{deadline}`")
    })?;
    let left = (deadline.with_timezone(&chrono::Utc) - now).num_seconds();
    if left < api::MIN_TOKEN_TTL {
        bail!(
            "the environment has less than a minute left. Create a new one with `fed remote up`."
        );
    }
    Ok(left.min(api::MAX_TOKEN_TTL))
}

fn deadline_passed(deadline: &str, now: chrono::DateTime<chrono::Utc>) -> bool {
    chrono::DateTime::parse_from_rfc3339(deadline)
        .is_ok_and(|d| d.with_timezone(&chrono::Utc) <= now)
}

fn local_time(deadline: &str) -> String {
    chrono::DateTime::parse_from_rfc3339(deadline)
        .map(|d| {
            d.with_timezone(&chrono::Local)
                .format("%Y-%m-%d %H:%M")
                .to_string()
        })
        .unwrap_or_else(|_| deadline.to_string())
}

fn minutes_left(deadline: &str, now: chrono::DateTime<chrono::Utc>) -> String {
    match chrono::DateTime::parse_from_rfc3339(deadline) {
        Ok(d) => format!(
            "{}m",
            (d.with_timezone(&chrono::Utc) - now).num_minutes().max(0)
        ),
        Err(_) => "-".to_string(),
    }
}

/// What fed prints when an environment the user still has keys for is gone.
/// Someone may have deleted it, so the expiry is one of two reasons.
pub(crate) fn deleted_message(name: &str) -> String {
    format!(
        "{name} no longer exists. It was deleted, or it expired ({IDLE_MINUTES} idle minutes or {LIFETIME_HOURS} hours). Create it again with `fed remote up {name}`."
    )
}

/// The `X.Y.Z` in `fed X.Y.Z`, or in a bare `X.Y.Z`. A pre-release such as
/// `8.4.0-rc.1` has no comparable version and gives `None`.
pub(crate) fn parse_version(text: &str) -> Option<[u64; 3]> {
    let word = text.split_whitespace().last()?;
    let word = word.strip_prefix('v').unwrap_or(word);
    let mut parts = word.split('.').map(|p| p.parse::<u64>().ok());
    let version = [parts.next()??, parts.next()??, parts.next()??];
    parts.next().is_none().then_some(version)
}

fn version_string(v: [u64; 3]) -> String {
    format!("{}.{}.{}", v[0], v[1], v[2])
}

/// The shell command that runs `command` (the words after `--`) on the
/// machine. In a workspace it runs in `/srv/<ws>`, as `start` does, so fed
/// there finds the workspace's vault token.
pub(crate) fn remote_command(env: &str, ws: Option<&str>, command: &[String]) -> String {
    let words = shell_words::join(command);
    match ws {
        None => words,
        Some(ws) => {
            let missing = shell_words::quote(&format!(
                "/srv/{ws} does not exist on {env}. Copy this checkout there with `fed remote push {env}`."
            ))
            .into_owned();
            format!("cd /srv/{ws} 2>/dev/null || {{ echo {missing} >&2; exit 1; }}; {words}")
        }
    }
}

/// The shell command for `fed remote ssh` without a command: a login shell,
/// in `/srv/<ws>` when that exists.
pub(crate) fn remote_shell(ws: Option<&str>) -> Option<String> {
    ws.map(|ws| format!("cd /srv/{ws} 2>/dev/null; exec \"${{SHELL:-/bin/sh}}\" -l"))
}

/// The script that deletes from `/srv/<ws>` the files the previous push
/// copied and this one does not. Its stdin is this push's NUL-separated file
/// list, which it keeps in `PUSH_MANIFEST` for the next push. It deletes only
/// files fed copied, so build output and fed's own state stay. A path whose
/// folder resolves outside the workspace is skipped.
pub(crate) fn push_cleanup_script(ws: &str) -> String {
    format!(
        r#"set -e
cd /srv/{ws}
mkdir -p .fed
new=$(mktemp .fed/remote-push.XXXXXX)
trap 'rm -f "$new"' EXIT
LC_ALL=C sort -z -u > "$new"
if [ -f {PUSH_MANIFEST} ]; then
  LC_ALL=C sort -z -u {PUSH_MANIFEST} | LC_ALL=C comm -z -23 - "$new" | xargs -0 -r sh -c '
    for p; do
      d=$(readlink -f -- "$(dirname -- "$p")") || continue
      case "$d/" in /srv/{ws}/*) ;; *) continue ;; esac
      if [ -d "$p" ] && [ ! -L "$p" ]; then continue; fi
      rm -f -- "$p"
      rmdir -p -- "$(dirname -- "$p")" 2>/dev/null || true
    done' sh
fi
mv -f "$new" {PUSH_MANIFEST}
"#
    )
}

/// Exit status of `install_fed_script` when the release does not exist.
const NO_RELEASE: i32 = 3;

/// The script that installs fed `version` from its GitHub release on the
/// machine. The wrapper in /usr/local/sbin runs /usr/local/bin/fed, so that
/// is the file it replaces. It checks the archive against the release's
/// SHA-256 file.
pub(crate) fn install_fed_script(version: [u64; 3]) -> String {
    let version = version_string(version);
    format!(
        r#"set -e
case $(uname -m) in
  x86_64) t=x86_64-unknown-linux-gnu ;;
  aarch64) t=aarch64-unknown-linux-gnu ;;
  *) exit 4 ;;
esac
url=https://github.com/service-federation/fed/releases/download/v{version}/fed-$t.tar.xz
d=$(mktemp -d)
trap 'rm -rf "$d"' EXIT
cd "$d"
code=$(curl -sSL --retry 2 -o fed.tar.xz.sha256 -w '%{{http_code}}' "$url.sha256") || exit 5
if [ "$code" = 404 ]; then exit {NO_RELEASE}; fi
if [ "$code" != 200 ]; then exit 5; fi
curl -fsSL --retry 2 -o fed.tar.xz "$url"
echo "$(cut -d ' ' -f 1 fed.tar.xz.sha256)  fed.tar.xz" | sha256sum -c --status
tar -xJf fed.tar.xz
bin=$(find . -type f -name fed | head -n 1)
[ -n "$bin" ]
install -m 0755 "$bin" /usr/local/bin/fed.new
mv -f /usr/local/bin/fed.new /usr/local/bin/fed
"#
    )
}

/// Where the shared SSH connection listens. ssh first creates the socket under
/// a name 17 bytes longer and then renames it, so that name has to fit in a
/// unix socket path too (103 bytes on macOS). When the state directory is too
/// deep, the socket goes to a short directory under /tmp owned by this user.
pub(crate) fn control_path(state_dir: &Path, id: &str) -> PathBuf {
    let limit = fed::fed_dir::max_socket_path_len() - 17;
    let in_state = state_dir.join("ssh.sock");
    if in_state.as_os_str().len() <= limit {
        return in_state;
    }
    // FNV-1a: stable across Rust versions, so every fed build finds the
    // same socket.
    let hash = id.bytes().fold(0xcbf2_9ce4_8422_2325_u64, |h, b| {
        (h ^ u64::from(b)).wrapping_mul(0x0000_0100_0000_01b3)
    });
    let uid = unsafe { nix::libc::geteuid() };
    PathBuf::from("/tmp")
        .join(format!("fed-remote-{uid}"))
        .join(format!("{hash:016x}"))
}

/// The short socket directory under /tmp has to be ours and private, or another
/// user could listen on our socket path.
fn prepare_control_dir(path: &Path, state_dir: &Path) -> Result<()> {
    let Some(dir) = path.parent() else {
        return Ok(());
    };
    if dir == state_dir {
        return Ok(());
    }
    create_private_dir(dir)?;
    use std::os::unix::fs::MetadataExt;
    let meta = std::fs::symlink_metadata(dir)?;
    if meta.uid() != unsafe { nix::libc::geteuid() } || meta.mode() & 0o077 != 0 {
        bail!(
            "{} is not private to this user. Remove it and try again.",
            dir.display()
        );
    }
    Ok(())
}

// ── Tools on this machine ─────────────────────────────────────────────

fn find_program(name: &str) -> Option<PathBuf> {
    let path = std::env::var_os("PATH")?;
    std::env::split_paths(&path)
        .map(|dir| dir.join(name))
        .find(|candidate| {
            std::fs::metadata(candidate)
                .is_ok_and(|m| m.is_file() && m.permissions().mode() & 0o111 != 0)
        })
}

/// macOS ships /usr/bin/rsync as a switch between samba rsync and openrsync.
/// This picks samba rsync. Other rsync builds ignore it.
const CHOSEN_RSYNC: (&str, &str) = ("CHOSEN_RSYNC", "rsync_samba");

/// Whether `rsync --version` printed by openrsync. openrsync never finishes a
/// copy with `--files-from`.
pub(crate) fn is_openrsync(version_output: &str) -> bool {
    version_output.to_ascii_lowercase().contains("openrsync")
}

fn rsync() -> Command {
    let mut cmd = Command::new("rsync");
    cmd.env(CHOSEN_RSYNC.0, CHOSEN_RSYNC.1);
    cmd
}

/// Check that `ssh`, `rsync` and, for `up`, `ssh-keygen` are on PATH and
/// work. `command` is the fed command, for the message.
async fn check_tools(command: &str, keygen: bool) -> Result<()> {
    let mut needed = vec!["ssh", "rsync"];
    if keygen {
        needed.insert(1, "ssh-keygen");
    }
    let missing: Vec<&str> = needed
        .iter()
        .copied()
        .filter(|name| find_program(name).is_none())
        .collect();
    if !missing.is_empty() {
        bail!(
            "{command} needs {}, which this machine does not have. Install OpenSSH and rsync, then try again.",
            missing.join(" and ")
        );
    }
    let ssh = Command::new("ssh")
        .arg("-V")
        .stdin(Stdio::null())
        .output()
        .await
        .context("running ssh -V")?;
    if !ssh.status.success() {
        bail!(
            "{command} needs a working ssh, but `ssh -V` failed ({}).",
            ssh.status
        );
    }
    let out = rsync()
        .arg("--version")
        .stdin(Stdio::null())
        .output()
        .await
        .context("running rsync --version")?;
    let version = String::from_utf8_lossy(&out.stdout);
    if is_openrsync(&version) {
        bail!(
            "{command} needs the original rsync, but the rsync on PATH is openrsync, which hangs on the file list fed sends. Install rsync with `brew install rsync` (or your package manager), then try again."
        );
    }
    if !out.status.success() {
        bail!(
            "{command} needs a working rsync, but `rsync --version` failed ({}).",
            out.status
        );
    }
    Ok(())
}

// ── SSH ───────────────────────────────────────────────────────────────

/// One environment this machine can reach over SSH.
struct Remote {
    dir: PathBuf,
    state: EnvState,
    control: PathBuf,
}

impl Remote {
    fn new(dir: PathBuf, state: EnvState) -> Result<Self> {
        let control = control_path(&dir, &state.id);
        prepare_control_dir(&control, &dir)?;
        Ok(Self {
            dir,
            state,
            control,
        })
    }

    fn target(&self) -> String {
        format!("root@{}", self.state.ip)
    }

    fn name(&self) -> &str {
        &self.state.name
    }

    /// The options every ssh call uses. ControlMaster and ControlPath are added
    /// per call, because ssh keeps the first value it gets for an option.
    fn base_args(&self) -> Vec<OsString> {
        ssh_base_args(&self.dir, self.state.port)
    }

    /// ssh through the shared connection, or a new one when it is closed.
    fn shared_args(&self) -> Vec<OsString> {
        let mut args = self.base_args();
        args.extend(opt("ControlMaster=no"));
        args.extend(opt_path("ControlPath=", &self.control));
        args
    }

    fn ssh(&self) -> Command {
        let mut cmd = Command::new("ssh");
        cmd.args(self.shared_args());
        cmd
    }

    /// Hold the environment's `ssh.lock` until the returned file is dropped.
    /// The file is opened close-on-exec, so ssh does not inherit the lock.
    async fn lock(&self) -> Result<std::fs::File> {
        let path = self.dir.join("ssh.lock");
        tokio::task::spawn_blocking(move || -> Result<std::fs::File> {
            let file = std::fs::OpenOptions::new()
                .create(true)
                .truncate(false)
                .write(true)
                .open(&path)
                .with_context(|| format!("opening {}", path.display()))?;
            fs2::FileExt::lock_exclusive(&file)
                .with_context(|| format!("locking {}", path.display()))?;
            Ok(file)
        })
        .await?
    }

    /// Open the shared connection unless it is already open. It is detached
    /// from every stream: started by an ordinary command, it would keep that
    /// command's output open and hang whoever reads it. Its stderr goes to
    /// `ssh.log`, for the error when it fails. It is a logged-in session, so
    /// it keeps the machine awake until 60 s after the last use.
    ///
    /// The check and the start run under `ssh.lock`. Two commands that both
    /// start a connection would leave one as a plain session with no
    /// ControlPersist limit, which keeps the machine awake for good.
    async fn share_connection(&self) -> Result<()> {
        let _lock = self.lock().await?;
        let open = Command::new("ssh")
            .args(self.shared_args())
            .args(["-O", "check"])
            .arg(self.target())
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .await
            .map(|s| s.success())
            .unwrap_or(false);
        if open {
            return Ok(());
        }
        // A socket left by a crashed master would make the new one fail to
        // listen and stay up as a plain session with no ControlPersist limit.
        let _ = std::fs::remove_file(&self.control);
        let log_path = self.dir.join("ssh.log");
        let log = std::fs::File::create(&log_path)
            .with_context(|| format!("creating {}", log_path.display()))?;
        let mut cmd = Command::new("ssh");
        cmd.args(self.base_args());
        // With its streams closed, a prompt would wait forever.
        cmd.args(opt("BatchMode=yes"));
        cmd.args(opt("ControlMaster=yes"));
        cmd.args(opt("ControlPersist=60"));
        cmd.args(opt_path("ControlPath=", &self.control));
        cmd.args(["-f", "-N"]).arg(self.target());
        let status = cmd
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(log)
            .status()
            .await
            .context("running ssh")?;
        if status.success() {
            // A machine whose `fed remote up` was interrupted may still be
            // setting up. Each new connection waits for that, which on a set-up
            // machine returns at once.
            let _ = self
                .ssh()
                .arg(self.target())
                .arg("cloud-init status --wait >/dev/null 2>&1")
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status()
                .await;
            return Ok(());
        }
        let log = std::fs::read_to_string(&log_path).unwrap_or_default();
        let detail = log
            .lines()
            .rfind(|l| !l.trim().is_empty())
            .map(|l| format!(" ssh said: {}", l.trim()))
            .unwrap_or_default();
        Err(self
            .failure(
                format!(
                    "cannot connect to {} over SSH ({status}).{detail}",
                    self.name()
                ),
                ssh_lost(status),
            )
            .await)
    }

    /// The error for a failed ssh or rsync call. When the connection was
    /// `lost`, the cloud list says whether the environment is gone, and a gone
    /// one is forgotten.
    async fn failure(&self, message: String, lost: bool) -> anyhow::Error {
        if lost && let Some(gone) = self.deleted().await {
            return gone;
        }
        anyhow!(message)
    }

    /// When the cloud no longer lists this environment, forget it and return
    /// the error that says so. `None` when it is listed, or the list fails.
    async fn deleted(&self) -> Option<anyhow::Error> {
        let creds = credentials().ok()?;
        let list = api::list_environments(&creds, &self.state.project_ref())
            .await
            .ok()?;
        if list.iter().any(|e| e.id == self.state.id) {
            return None;
        }
        forget(Some(&creds), &self.dir, &self.state).await;
        Some(anyhow!(deleted_message(self.name())))
    }

    /// Run `command` on the machine with this terminal's streams.
    async fn run(&self, command: &str) -> Result<ExitStatus> {
        self.share_connection().await?;
        self.ssh()
            .arg(self.target())
            .arg(command)
            .stdin(Stdio::null())
            .status()
            .await
            .context("running ssh")
    }

    async fn run_checked(&self, command: &str, what: &str) -> Result<()> {
        let status = self.run(command).await?;
        if !status.success() {
            return Err(self
                .failure(
                    format!("{what} failed on {} ({status}).", self.name()),
                    ssh_lost(status),
                )
                .await);
        }
        Ok(())
    }

    /// Run `command` on the machine and return what it printed.
    async fn output(&self, command: &str, what: &str) -> Result<String> {
        self.share_connection().await?;
        let out = self
            .ssh()
            .arg(self.target())
            .arg(command)
            .stdin(Stdio::null())
            .stderr(Stdio::inherit())
            .output()
            .await
            .context("running ssh")?;
        if !out.status.success() {
            return Err(self
                .failure(
                    format!("{what} failed on {} ({}).", self.name(), out.status),
                    ssh_lost(out.status),
                )
                .await);
        }
        Ok(String::from_utf8_lossy(&out.stdout).into_owned())
    }

    /// Run `command` on the machine with `input` on its stdin.
    async fn run_with_input(&self, command: &str, input: &[u8], what: &str) -> Result<()> {
        use tokio::io::AsyncWriteExt;
        self.share_connection().await?;
        let mut child = self
            .ssh()
            .arg(self.target())
            .arg(command)
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .spawn()
            .context("running ssh")?;
        let written = match child.stdin.take() {
            Some(mut stdin) => {
                let result = stdin.write_all(input).await;
                drop(stdin);
                result
            }
            None => Ok(()),
        };
        // When ssh stops early, its status says why. The broken pipe does not.
        let status = child.wait().await?;
        if !status.success() {
            return Err(self
                .failure(
                    format!("{what} failed on {} ({status}).", self.name()),
                    ssh_lost(status),
                )
                .await);
        }
        written.with_context(|| format!("{what} on {}", self.name()))?;
        Ok(())
    }

    /// Whether SSH answers, without the shared connection.
    async fn answers(&self) -> bool {
        Command::new("ssh")
            .args(self.base_args())
            .args(opt("BatchMode=yes"))
            .args(opt("ControlMaster=no"))
            .args(opt("ControlPath=none"))
            .arg(self.target())
            .arg("true")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .await
            .map(|s| s.success())
            .unwrap_or(false)
    }
}

/// Whether ssh lost or never got its connection: it exits with 255 then.
fn ssh_lost(status: ExitStatus) -> bool {
    status.code() == Some(255)
}

/// Ask the shared connection at `control` to end.
async fn close_connection(control: &Path, ip: &str) {
    let _ = Command::new("ssh")
        .args(["-F", "none"])
        .args(opt_path("ControlPath=", control))
        .args(["-O", "exit"])
        .arg(format!("root@{ip}"))
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .await;
}

fn opt(value: &str) -> [OsString; 2] {
    ["-o".into(), value.into()]
}

/// An ssh `-o` option with a path value. ssh splits option values on spaces
/// (UserKnownHostsFile takes a list), so a path with a space is double-quoted.
fn opt_path(key: &str, path: &Path) -> [OsString; 2] {
    let mut value = OsString::from(key);
    if path.as_os_str().to_string_lossy().contains(' ') {
        value.push("\"");
        value.push(path);
        value.push("\"");
    } else {
        value.push(path);
    }
    ["-o".into(), value]
}

/// The ssh options for every call to an environment. `-F none` keeps the
/// user's ~/.ssh/config out: everything the connection needs is here.
pub(crate) fn ssh_base_args(state_dir: &Path, port: u16) -> Vec<OsString> {
    let mut args: Vec<OsString> = vec!["-F".into(), "none".into()];
    args.push("-i".into());
    args.push(state_dir.join("client_key").into());
    args.push("-p".into());
    args.push(port.to_string().into());
    args.extend(opt("IdentitiesOnly=yes"));
    args.extend(opt_path(
        "UserKnownHostsFile=",
        &state_dir.join("known_hosts"),
    ));
    args.extend(opt("StrictHostKeyChecking=yes"));
    args.extend(opt("ConnectTimeout=10"));
    args.extend(opt("ServerAliveInterval=15"));
    args
}

/// The `-e` value for rsync: ssh with `args`. rsync splits it into words
/// itself and knows quotes but not backslashes, so an argument with a `'`
/// cannot be passed.
pub(crate) fn rsync_shell(args: Vec<OsString>) -> Result<String> {
    let words = std::iter::once(OsString::from("ssh"))
        .chain(args)
        .map(|a| match a.into_string() {
            Ok(a) if !a.contains('\'') => Ok(a),
            Ok(a) => Err(anyhow!(
                "fed remote push cannot copy files while the path `{a}` contains a ' character, because rsync cannot pass it to ssh. Rename the folder, or move your home directory to a path without '."
            )),
            Err(a) => Err(anyhow!(
                "fed remote push cannot copy files while the path `{}` is not valid UTF-8, because rsync cannot pass it to ssh.",
                a.to_string_lossy()
            )),
        })
        .collect::<Result<Vec<String>>>()?;
    Ok(shell_words::join(&words))
}

// ── Checkout ──────────────────────────────────────────────────────────

fn checkout_root(workdir: Option<PathBuf>) -> Result<PathBuf> {
    let dir = match workdir {
        Some(d) => d,
        None => std::env::current_dir()?,
    };
    let out = std::process::Command::new("git")
        .arg("-C")
        .arg(&dir)
        .args(["rev-parse", "--show-toplevel"])
        .stderr(Stdio::null())
        .output()
        .context("running git")?;
    if !out.status.success() {
        bail!("{} is not in a git checkout.", dir.display());
    }
    let root = String::from_utf8_lossy(&out.stdout).trim_end().to_string();
    Ok(PathBuf::from(root))
}

fn git_output(root: &Path, args: &[&str]) -> Result<Vec<u8>> {
    let out = std::process::Command::new("git")
        .arg("-C")
        .arg(root)
        .args(args)
        .output()
        .context("running git")?;
    if !out.status.success() {
        bail!(
            "git {} failed in {}: {}",
            args.join(" "),
            root.display(),
            String::from_utf8_lossy(&out.stderr).trim()
        );
    }
    Ok(out.stdout)
}

/// What `fed remote push` copies, as paths relative to the checkout.
struct PushList {
    /// Tracked and untracked-but-not-ignored files, filtered by `keep_path`,
    /// and only the ones that exist (a deleted tracked file would fail the
    /// copy).
    files: Vec<Vec<u8>>,
    /// Submodules, which are left out: rsync would only make their folders.
    submodules: Vec<Vec<u8>>,
}

fn files_to_push(root: &Path) -> Result<PushList> {
    use std::os::unix::ffi::OsStrExt;
    let listed = git_output(root, &["ls-files", "-z", "-co", "--exclude-standard"])?;
    let submodules = submodule_paths(&git_output(root, &["ls-files", "-z", "--stage"])?);
    let files = filter_file_list(&listed)
        .into_iter()
        .filter(|p| !submodules.contains(p))
        .filter(|p| std::fs::symlink_metadata(root.join(std::ffi::OsStr::from_bytes(p))).is_ok())
        .collect();
    Ok(PushList { files, submodules })
}

// ── Commands ──────────────────────────────────────────────────────────

fn credentials() -> Result<cloud::Credentials> {
    cloud::load_credentials().context("you are not signed in. Run `fed login`.")
}

fn linked_project(root: &Path) -> Option<api::ProjectRef> {
    cloud::load_link(root).map(|l| api::ProjectRef {
        org: l.org,
        project: l.project,
    })
}

fn required_project(root: &Path) -> Result<api::ProjectRef> {
    linked_project(root)
        .context("this checkout is not linked to a project. Run `fed link org/project`.")
}

pub async fn run_remote(
    cmd: &RemoteCommands,
    workdir: Option<PathBuf>,
    out: &dyn UserOutput,
) -> Result<()> {
    let root = state_root()?;
    match cmd {
        RemoteCommands::Up { name, kind } => {
            let checkout = checkout_root(workdir)?;
            check_tools("fed remote up", true).await?;
            up(&root, &checkout, name.as_deref(), kind.as_deref(), out).await
        }
        RemoteCommands::Ls => {
            let checkout = checkout_root(workdir)?;
            ls(&root, &checkout, out).await
        }
        RemoteCommands::Push { name, workspace } => {
            let checkout = checkout_root(workdir)?;
            let ws = workspace_name(&checkout, workspace.as_deref())?;
            check_tools("fed remote push", false).await?;
            let remote = open(&root, name, linked_project(&checkout).as_ref(), out).await?;
            push(&remote, &checkout, &ws, out).await
        }
        RemoteCommands::Start { name, workspace } => {
            let checkout = checkout_root(workdir)?;
            let ws = workspace_name(&checkout, workspace.as_deref())?;
            check_tools("fed remote start", false).await?;
            let remote = open(&root, name, linked_project(&checkout).as_ref(), out).await?;
            push(&remote, &checkout, &ws, out).await?;
            match_fed_version(&remote, out).await?;
            if let Some(project) = linked_project(&checkout) {
                give_vault_token(&remote, &project, &ws, out).await?;
            }
            remote
                .run_checked(&format!("cd /srv/{ws} && fed start"), "fed start")
                .await?;
            out.success(&format!(
                "Started /srv/{ws} on {name}. Next: `fed remote connect {name}`"
            ));
            Ok(())
        }
        RemoteCommands::Connect { name, workspace } => {
            let checkout = checkout_root(workdir)?;
            let ws = workspace_name(&checkout, workspace.as_deref())?;
            let remote = open(&root, name, linked_project(&checkout).as_ref(), out).await?;
            connect(&remote, &ws, out).await
        }
        RemoteCommands::Ssh {
            name,
            workspace,
            command,
        } => {
            let checkout = checkout_root(workdir).ok();
            let project = checkout.as_deref().and_then(linked_project);
            let ws = match (workspace, &checkout) {
                (Some(ws), _) => Some(checked_workspace(ws)?),
                (None, Some(checkout)) => workspace_name(checkout, None).ok(),
                (None, None) => None,
            };
            let remote = open(&root, name, project.as_ref(), out).await?;
            let code = ssh(&remote, ws.as_deref(), command).await?;
            std::process::exit(code);
        }
        RemoteCommands::Down { name } => {
            let project = checkout_root(workdir).ok().and_then(|c| linked_project(&c));
            down(&root, name, project.as_ref(), out).await
        }
    }
}

/// The environment called `name`, ready for SSH. One past its deadline is
/// checked against the cloud first, so the user gets the reason instead of
/// an SSH timeout.
async fn open(
    root: &Path,
    name: &str,
    project: Option<&api::ProjectRef>,
    out: &dyn UserOutput,
) -> Result<Remote> {
    let (dir, state) = find_environment(root, name, project).await?;
    if let Some(project) = project {
        note_other_project(&state, project, out);
    }
    let remote = Remote::new(dir, state)?;
    if deadline_passed(&remote.state.deadline, chrono::Utc::now())
        && let Some(gone) = remote.deleted().await
    {
        return Err(gone);
    }
    Ok(remote)
}

async fn keygen(path: &Path, comment: &str) -> Result<()> {
    let status = Command::new("ssh-keygen")
        .args(["-q", "-t", "ed25519", "-N", "", "-C", comment, "-f"])
        .arg(path)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .status()
        .await
        .context("running ssh-keygen (it comes with OpenSSH)")?;
    if !status.success() {
        bail!("ssh-keygen failed ({status}).");
    }
    Ok(())
}

fn read_trimmed(path: &Path) -> Result<String> {
    Ok(std::fs::read_to_string(path)
        .with_context(|| format!("reading {}", path.display()))?
        .trim_end()
        .to_string())
}

async fn up(
    root: &Path,
    checkout: &Path,
    name: Option<&str>,
    kind: Option<&str>,
    out: &dyn UserOutput,
) -> Result<()> {
    let creds = credentials()?;
    let project = required_project(checkout)?;
    let name = match name {
        Some(n) => n.to_string(),
        None => format!("env-{}", chrono::Local::now().format("%H%M%S")),
    };
    if !api::valid_environment_name(&name) {
        bail!(api::INVALID_ENVIRONMENT_NAME);
    }

    // Once tokio listens for SIGINT, Ctrl-C no longer ends the process. One
    // listener for the whole command, so every step after this sees Ctrl-C.
    use tokio::signal::unix::{SignalKind, signal};
    let mut interrupt = signal(SignalKind::interrupt()).context("listening for Ctrl-C")?;

    create_private_dir(root)?;
    remove_stale_staging(root, std::time::SystemTime::now());
    let staging = root.join(format!("{STAGING_PREFIX}{:016x}", rand::random::<u64>()));
    create_private_dir(&staging)?;
    let created = create(&creds, &project, &staging, &name, kind, &mut interrupt, out).await;
    let env = match created {
        Ok(env) => env,
        Err(e) => {
            let _ = std::fs::remove_dir_all(&staging);
            return Err(e);
        }
    };

    // env.json goes in before the keys move to their final place, so
    // `fed remote down` can find the environment whatever fails after this.
    if !api::valid_id(&env.id) {
        let _ = std::fs::remove_dir_all(&staging);
        bail!(
            "Service Federation Cloud sent an invalid id for {name}. Delete it in the dashboard."
        );
    }
    let state = EnvState {
        id: env.id.clone(),
        name: name.clone(),
        org: project.org.clone(),
        project: project.project.clone(),
        ip: env.ip.clone().unwrap_or_default(),
        port: env.port.unwrap_or(0),
        deadline: env.deadline.clone(),
    };
    write_private(
        &staging.join(STATE_FILE),
        serde_json::to_string_pretty(&state)?.as_bytes(),
    )?;
    let dir = root.join(&env.id);
    std::fs::rename(&staging, &dir)
        .with_context(|| format!("moving the keys to {}", dir.display()))?;

    let finish = async {
        // The cloud only gives out a name that no active environment has, so
        // an older environment by this name is gone.
        for (old_dir, old) in local_environments(root) {
            if old.name == name && old.in_project(&project) && old.id != env.id {
                forget(Some(&creds), &old_dir, &old).await;
            }
        }
        let (Some(ip), Some(port)) = (env.ip.clone(), env.port) else {
            bail!(
                "Service Federation Cloud has no address for {name} yet. Delete it with `fed remote down {name}` and try again."
            );
        };
        let host_public = read_trimmed(&dir.join("host_key.pub"))?;
        write_private(
            &dir.join("known_hosts"),
            known_hosts_line(&ip, port, &host_public)?.as_bytes(),
        )?;
        let remote = Remote::new(dir.clone(), state.clone())?;
        wait_until_ready(&remote, &ip, port, out).await?;
        Ok((ip, port))
    };
    let (ip, port) = tokio::select! {
        biased;
        _ = interrupt.recv() => {
            out.clear_progress();
            bail!(
                "Stopped waiting for {name}. It keeps starting up, and deletes itself {IDLE_MINUTES} minutes after the last SSH session ends. Continue with `fed remote start {name}`, or delete it with `fed remote down {name}`."
            );
        }
        ready = finish => ready?,
    };

    out.success(&format!("{name} is ready at {ip} (SSH port {port})."));
    out.status(&format!(
        "It deletes itself {IDLE_MINUTES} minutes after the last SSH session ends, and at {} in any case.",
        local_time(&env.deadline)
    ));
    out.status(&format!("Next: `fed remote start {name}`"));
    Ok(())
}

async fn wait_until_ready(
    remote: &Remote,
    ip: &str,
    port: u16,
    out: &dyn UserOutput,
) -> Result<()> {
    let name = remote.name();
    out.progress(&format!("Waiting for {name} to answer SSH ({ip}:{port}) "));
    let start = Instant::now();
    while !remote.answers().await {
        if start.elapsed() > SSH_WAIT {
            out.finish_progress("timed out");
            if let Some(gone) = remote.deleted().await {
                return Err(gone);
            }
            bail!(
                "{name} does not answer SSH after {} minutes. Delete it with `fed remote down {name}` and try again.",
                SSH_WAIT.as_secs() / 60
            );
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
    out.finish_progress(&format!("after {}s", start.elapsed().as_secs()));
    remote
        .run_checked("cloud-init status --wait >/dev/null", "cloud-init")
        .await
}

/// Make the keys in `staging` and ask the cloud for the environment. The host
/// private key is only needed for the request, so it is deleted after it.
///
/// When the answer does not come within `api::CREATE_TIMEOUT`, or the user
/// presses Ctrl-C, the request is dropped and the cloud list says what
/// exists. This machine never got the address, so an environment that was
/// made anyway can only be deleted.
async fn create(
    creds: &cloud::Credentials,
    project: &api::ProjectRef,
    staging: &Path,
    name: &str,
    kind: Option<&str>,
    interrupt: &mut tokio::signal::unix::Signal,
    out: &dyn UserOutput,
) -> Result<api::Environment> {
    keygen(
        &staging.join("client_key"),
        &format!("fed-remote {name} client"),
    )
    .await?;
    keygen(
        &staging.join("host_key"),
        &format!("fed-remote {name} host"),
    )
    .await?;
    let client_public = read_trimmed(&staging.join("client_key.pub"))?;
    let host_public = read_trimmed(&staging.join("host_key.pub"))?;
    let host_private = std::fs::read_to_string(staging.join("host_key"))?;
    out.progress(&format!(
        "Creating {name} ({}) in {project} ",
        kind.unwrap_or(api::DEFAULT_TYPE)
    ));
    let body = api::CreateEnvironment {
        name,
        kind,
        client_public_key: &client_public,
        host_private_key: &host_private,
        host_public_key: &host_public,
    };
    let stopped = tokio::select! {
        biased;
        _ = interrupt.recv() => "you pressed Ctrl-C",
        _ = tokio::time::sleep(api::CREATE_TIMEOUT) => "it took more than 5 minutes",
        result = api::create_environment(creds, project, &body) => {
            let _ = std::fs::remove_file(staging.join("host_key"));
            return match result {
                Ok(env) => {
                    out.finish_progress("done");
                    Ok(env)
                }
                Err(e) => {
                    out.clear_progress();
                    Err(e.into())
                }
            };
        }
    };
    let _ = std::fs::remove_file(staging.join("host_key"));
    out.clear_progress();
    out.status("Checking whether it exists. Press Ctrl-C again to skip.");
    let check = tokio::select! {
        biased;
        _ = interrupt.recv() => None,
        listed = tokio::time::timeout(
            Duration::from_secs(20),
            api::list_environments(creds, project),
        ) => listed.ok().and_then(|l| l.ok()),
    };
    match check {
        Some(list) if list.iter().any(|e| e.name == name) => bail!(
            "fed stopped waiting for {name} because {stopped}. {name} exists in {project}, but this machine has no keys for it. Delete it with `fed remote down {name}`, then create it again."
        ),
        Some(_) => bail!(
            "fed stopped waiting for {name} because {stopped}. {project} has no environment called {name} yet. If it shows up in `fed remote ls`, delete it with `fed remote down {name}`."
        ),
        None => bail!(
            "fed stopped waiting for {name} because {stopped}, and did not check whether it exists. If it shows up in `fed remote ls`, delete it with `fed remote down {name}`."
        ),
    }
}

/// The caller's user id, to tell their environments from others' in an org
/// admin's list. Only asked for when the list has owners.
async fn my_user_id(creds: &cloud::Credentials, list: &[api::Environment]) -> Option<String> {
    if list.iter().all(|e| e.owner.is_none()) {
        return None;
    }
    cloud::whoami(creds).await.ok()?.user.id
}

async fn ls(root: &Path, checkout: &Path, out: &dyn UserOutput) -> Result<()> {
    let creds = credentials()?;
    let project = required_project(checkout)?;
    let list = api::list_environments(&creds, &project).await?;

    // The cloud deletes idle environments without telling us. Forget their
    // keys, in this project and in every other one this machine has keys for.
    for (dir, state) in local_environments(root) {
        if state.in_project(&project) && !list.iter().any(|e| e.id == state.id) {
            forget(Some(&creds), &dir, &state).await;
        }
    }
    let others: Vec<EnvState> = local_environments(root)
        .into_iter()
        .map(|(_, s)| s)
        .filter(|s| !s.in_project(&project))
        .collect();
    forget_deleted(&creds, root, &others).await;

    if list.is_empty() {
        out.status(&format!(
            "No remote environments in {project}. Create one with `fed remote up NAME`."
        ));
        return Ok(());
    }
    let me = my_user_id(&creds, &list).await;
    let show_owner = list.iter().any(|e| !e.is_owned_by(me.as_deref()));
    let now = chrono::Utc::now();
    let mut header = vec!["NAME", "TYPE", "STATE", "ADDRESS", "LEFT"];
    if show_owner {
        header.push("OWNER");
    }
    let header: Vec<String> = header.into_iter().map(String::from).collect();
    let rows: Vec<Vec<String>> = list
        .iter()
        .map(|e| {
            let mut row = vec![
                e.name.clone(),
                e.kind.clone(),
                e.state.clone(),
                match (&e.ip, e.port) {
                    (Some(ip), Some(port)) => format!("{ip}:{port}"),
                    _ => "-".to_string(),
                },
                minutes_left(&e.deadline, now),
            ];
            if show_owner {
                row.push(match &e.owner {
                    _ if e.is_owned_by(me.as_deref()) => "you".to_string(),
                    Some(owner) => owner.name.clone().unwrap_or_else(|| owner.id.clone()),
                    None => "-".to_string(),
                });
            }
            row
        })
        .collect();
    let mut widths: Vec<usize> = header.iter().map(String::len).collect();
    for row in &rows {
        for (w, cell) in widths.iter_mut().zip(row) {
            *w = (*w).max(cell.len());
        }
    }
    for row in std::iter::once(&header).chain(&rows) {
        let line: Vec<String> = row
            .iter()
            .zip(&widths)
            .map(|(cell, w)| format!("{cell:w$}"))
            .collect();
        out.status(line.join("  ").trim_end());
    }
    Ok(())
}

/// Copy the checkout to `/srv/<ws>`, then delete there the files an earlier
/// push copied that the checkout no longer has.
async fn push(remote: &Remote, checkout: &Path, ws: &str, out: &dyn UserOutput) -> Result<()> {
    use std::os::unix::ffi::OsStrExt;
    let PushList { files, submodules } = files_to_push(checkout)?;
    if !submodules.is_empty() {
        let names: Vec<String> = submodules
            .iter()
            .map(|p| String::from_utf8_lossy(p).into_owned())
            .collect();
        out.warning(&format!(
            "fed remote push skips git submodules. These do not reach {}: {}",
            remote.name(),
            names.join(", ")
        ));
    }
    let shell = rsync_shell(remote.shared_args())?;
    remote
        .run_checked(&format!("mkdir -p /srv/{ws}"), "creating the workspace")
        .await?;
    let count = match files.len() {
        1 => "1 file".to_string(),
        n => format!("{n} files"),
    };
    out.status(&format!("Pushing {count} to {}:/srv/{ws}", remote.name()));

    let mut list = Vec::new();
    for path in &files {
        list.extend_from_slice(std::ffi::OsStr::from_bytes(path).as_bytes());
        list.push(0);
    }
    let mut source = checkout.as_os_str().to_os_string();
    source.push("/");
    let mut child = rsync()
        .args(["-a", "--from0", "--files-from=-"])
        .arg(format!("--timeout={RSYNC_IO_TIMEOUT}"))
        .arg("-e")
        .arg(shell)
        .arg(source)
        .arg(format!("{}:/srv/{ws}/", remote.target()))
        .stdin(Stdio::piped())
        .spawn()
        .context("running rsync")?;
    let written = {
        use tokio::io::AsyncWriteExt;
        let mut stdin = child.stdin.take().context("rsync stdin")?;
        let result = stdin.write_all(&list).await;
        drop(stdin);
        result
    };
    // When rsync stops early, its status says why. The broken pipe does not.
    let status = child.wait().await?;
    if status.code() == Some(RSYNC_VANISHED) {
        out.warning(&format!(
            "Some files were deleted while fed copied them to {}. Push again to copy the checkout as it is now.",
            remote.name()
        ));
    } else if !status.success() {
        // rsync exits with 255 when ssh does, and with 12 when the connection
        // drops in the middle of the copy.
        let lost = matches!(status.code(), Some(12 | 255));
        return Err(remote
            .failure(
                format!("rsync to {} failed ({status}).", remote.name()),
                lost,
            )
            .await);
    }
    written.context("sending the file list to rsync")?;
    remote
        .run_with_input(&push_cleanup_script(ws), &list, "removing deleted files")
        .await
}

/// Make fed on the machine at least as new as this one. The machine image
/// has a fixed fed version. A newer released version is installed from its
/// GitHub release. A version with no release, such as a build from source,
/// gets a warning.
async fn match_fed_version(remote: &Remote, out: &dyn UserOutput) -> Result<()> {
    let name = remote.name();
    let Some(local) = parse_version(env!("CARGO_PKG_VERSION")) else {
        return Ok(());
    };
    let printed = remote.output("fed --version", "fed --version").await?;
    let Some(theirs) = parse_version(&printed) else {
        out.warning(&format!(
            "fed on {name} printed an unexpected version ({}). Commands there may behave differently from this fed.",
            printed.trim()
        ));
        return Ok(());
    };
    if theirs >= local {
        return Ok(());
    }
    let (local_s, theirs_s) = (version_string(local), version_string(theirs));
    out.status(&format!(
        "Installing fed {local_s} on {name}, which has fed {theirs_s}"
    ));
    let status = remote.run(&install_fed_script(local)).await?;
    match status.code() {
        Some(0) => Ok(()),
        Some(NO_RELEASE) => {
            out.warning(&format!(
                "fed {local_s} has no release to install on {name}, so commands there run fed {theirs_s}."
            ));
            Ok(())
        }
        _ => {
            out.warning(&format!(
                "installing fed {local_s} on {name} failed ({status}), so commands there run fed {theirs_s}."
            ));
            Ok(())
        }
    }
}

/// Give the workspace a vault token: read access to the linked project's
/// vault until the environment's deadline. The cloud revokes it when the
/// environment ends. The new token is in place before the previous one for
/// the workspace is revoked, so a failure leaves the workspace with a token.
async fn give_vault_token(
    remote: &Remote,
    project: &api::ProjectRef,
    ws: &str,
    out: &dyn UserOutput,
) -> Result<()> {
    let creds =
        credentials().context("this checkout uses the team vault. Run `fed login` first.")?;
    let base = cloud::cloud_base_url(&creds.url)?;
    let ttl = token_ttl(&remote.state.deadline, chrono::Utc::now())?;

    let tokens = remote.dir.join("tokens");
    create_private_dir(&tokens)?;
    let record_path = tokens.join(format!("{ws}.json"));

    let label = format!("fed remote {}/{ws}", remote.name());
    let token = api::mint_env_token(&creds, project, &label, ttl, Some(&remote.state.id)).await?;
    let url = base.as_str().trim_end_matches('/');
    let command = format!(
        "umask 077 && mkdir -p /run/fedenv/tokens && cat > /run/fedenv/tokens/{ws} && printf '%s\\n' {} > /run/fedenv/cloud-url",
        shell_words::quote(url)
    );
    if let Err(e) = remote
        .run_with_input(&command, token.token.as_bytes(), "storing the vault token")
        .await
    {
        let _ = api::revoke_env_token(&creds, project, &token.id).await;
        return Err(e);
    }

    if let Err(e) = revoke_token_file(&creds, &record_path).await {
        out.warning(&format!(
            "Could not revoke the previous vault token for /srv/{ws}: {e}. It expires at the environment's deadline."
        ));
    }
    let record = TokenRecord {
        id: token.id.clone(),
        org: project.org.clone(),
        project: project.project.clone(),
    };
    write_private(&record_path, serde_json::to_string(&record)?.as_bytes())?;
    out.status(&format!(
        "Gave /srv/{ws} read access to {project}'s vault for {}m",
        ttl / 60
    ));
    Ok(())
}

/// Revoke the token in a record file and delete the file. Without a readable
/// record there is nothing to revoke.
async fn revoke_token_file(creds: &cloud::Credentials, path: &Path) -> Result<()> {
    let Some(record) = std::fs::read_to_string(path)
        .ok()
        .and_then(|raw| serde_json::from_str::<TokenRecord>(&raw).ok())
    else {
        return Ok(());
    };
    let project = api::ProjectRef {
        org: record.org,
        project: record.project,
    };
    api::revoke_env_token(creds, &project, &record.id).await?;
    let _ = std::fs::remove_file(path);
    Ok(())
}

/// `fed remote ssh`: a shell, or the command after `--`, in the workspace.
/// Returns ssh's exit code.
async fn ssh(remote: &Remote, ws: Option<&str>, command: &[String]) -> Result<i32> {
    remote.share_connection().await?;
    let mut ssh = remote.ssh();
    if command.is_empty() {
        ssh.arg("-t").arg(remote.target());
        if let Some(shell) = remote_shell(ws) {
            ssh.arg(shell);
        }
    } else {
        ssh.arg(remote.target())
            .arg(remote_command(remote.name(), ws, command));
    }
    let status = ssh.status().await.context("running ssh")?;
    if ssh_lost(status)
        && let Some(gone) = remote.deleted().await
    {
        return Err(gone);
    }
    Ok(status.code().unwrap_or(1))
}

async fn connect(remote: &Remote, ws: &str, out: &dyn UserOutput) -> Result<()> {
    let json = remote
        .output(
            &format!("cd /srv/{ws} && fed ports list --json"),
            "fed ports list",
        )
        .await?;
    let ports = parse_ports(&json)?;
    let name = remote.name();
    if ports.is_empty() {
        bail!(
            "/srv/{ws} on {name} has no ports to forward. Its services publish none, or `fed remote start {name}` has not run."
        );
    }

    // One listener per signal for the whole command, so a signal between two
    // rounds is kept. Each forward is a logged-in session, so a forward left
    // behind by `kill` or a closed terminal would keep the VM awake.
    use tokio::signal::unix::{SignalKind, signal};
    let mut interrupt = signal(SignalKind::interrupt()).context("listening for Ctrl-C")?;
    let mut terminate = signal(SignalKind::terminate()).context("listening for SIGTERM")?;
    let mut hangup = signal(SignalKind::hangup()).context("listening for SIGHUP")?;

    // One ssh per port, outside the shared connection, so one failed forward
    // does not stop the others. Each runs in its own process group, so Ctrl-C
    // in the terminal reaches only fed, which then ends them.
    let mut forwards = Vec::new();
    for (port, params) in &ports {
        let local = local_port(*port);
        let child = Command::new("ssh")
            .args(remote.base_args())
            .args(opt("BatchMode=yes"))
            .args(opt("ControlMaster=no"))
            .args(opt("ControlPath=none"))
            .args(opt("ExitOnForwardFailure=yes"))
            .arg("-N")
            .arg("-L")
            .arg(format!("{local}:127.0.0.1:{port}"))
            .arg(remote.target())
            .stdin(Stdio::null())
            .process_group(0)
            .kill_on_drop(true)
            .spawn()
            .context("running ssh")?;
        forwards.push((local, *port, params.join(", "), child));
    }

    let stop_requested = tokio::select! {
        biased;
        _ = interrupt.recv() => true,
        _ = terminate.recv() => true,
        _ = hangup.recv() => true,
        _ = tokio::time::sleep(FORWARD_SETTLE) => false,
    };
    if !stop_requested {
        let mut alive = Vec::new();
        for (local, port, params, mut child) in forwards {
            match child.try_wait() {
                Ok(None) => {
                    out.status(&format!("  localhost:{local} -> {name}:{port}  ({params})"));
                    alive.push((local, child));
                }
                Ok(Some(status)) => out.warning(&format!(
                    "The forward from localhost:{local} to {name}:{port} stopped ({status})."
                )),
                Err(e) => out.warning(&format!(
                    "The forward from localhost:{local} to {name}:{port} failed: {e}"
                )),
            }
        }
        let mut forwards = alive;
        if forwards.is_empty() {
            return Err(every_forward_stopped(remote).await);
        }
        out.status(&format!(
            "Connected. Press Ctrl-C to disconnect. {name} deletes itself {IDLE_MINUTES} minutes after the last SSH session ends."
        ));
        loop {
            let stopped = {
                let waits = forwards
                    .iter_mut()
                    .map(|(_, child)| Box::pin(child.wait()))
                    .collect::<Vec<_>>();
                tokio::select! {
                    biased;
                    _ = interrupt.recv() => None,
                    _ = terminate.recv() => None,
                    _ = hangup.recv() => None,
                    (status, index, _) = futures::future::select_all(waits) => Some((
                        index,
                        status.map(|s| s.to_string()).unwrap_or_else(|e| e.to_string()),
                    )),
                }
            };
            let Some((index, status)) = stopped else {
                break;
            };
            let (local, _) = forwards.remove(index);
            out.warning(&format!(
                "The forward from localhost:{local} stopped ({status})."
            ));
            if forwards.is_empty() {
                return Err(every_forward_stopped(remote).await);
            }
        }
        for (_, child) in &mut forwards {
            let _ = child.kill().await;
        }
    } else {
        for (_, _, _, child) in &mut forwards {
            let _ = child.kill().await;
        }
    }
    out.status("Disconnected.");
    Ok(())
}

async fn every_forward_stopped(remote: &Remote) -> anyhow::Error {
    if let Some(gone) = remote.deleted().await {
        return gone;
    }
    anyhow!("every port forward to {} stopped.", remote.name())
}

/// The caller's environment called `name` in `project`, from the cloud list.
/// For one created on another machine. An org admin's list also holds other
/// people's environments, and only the caller's own count.
async fn cloud_environment(
    creds: &cloud::Credentials,
    project: &api::ProjectRef,
    name: &str,
) -> Result<Option<EnvState>> {
    let list = api::list_environments(creds, project).await?;
    let me = my_user_id(creds, &list).await;
    Ok(list
        .into_iter()
        .find(|e| e.name == name && e.is_owned_by(me.as_deref()))
        .map(|env| EnvState {
            id: env.id,
            name: env.name,
            org: project.org.clone(),
            project: project.project.clone(),
            ip: env.ip.unwrap_or_default(),
            port: env.port.unwrap_or(22),
            deadline: env.deadline,
        }))
}

/// Say so when a command acts on an environment outside the linked project.
fn note_other_project(state: &EnvState, linked: &api::ProjectRef, out: &dyn UserOutput) {
    if !state.in_project(linked) {
        out.status(&format!(
            "Using {} in {}. This checkout is linked to {linked}.",
            state.name,
            state.project_ref()
        ));
    }
}

async fn down(
    root: &Path,
    name: &str,
    project: Option<&api::ProjectRef>,
    out: &dyn UserOutput,
) -> Result<()> {
    let creds = credentials()?;
    let local = find_environment(root, name, project).await;
    // An environment in the linked project wins over one this machine has in
    // another project, also when only the cloud knows the linked one.
    let (dir, state) = match (local, project) {
        (Ok((dir, state)), None) => (Some(dir), state),
        (Ok((dir, state)), Some(project)) if state.in_project(project) => (Some(dir), state),
        (local, Some(project)) => match cloud_environment(&creds, project, name).await? {
            Some(state) => (None, state),
            None => match local {
                Ok((dir, state)) => {
                    note_other_project(&state, project, out);
                    (Some(dir), state)
                }
                Err(_) => bail!(
                    "you have no environment called {name} in {project}. See `fed remote ls`."
                ),
            },
        },
        (Err(not_found), None) => return Err(not_found),
    };

    if let Some(dir) = &dir {
        if let Ok(entries) = std::fs::read_dir(dir.join("tokens")) {
            for entry in entries.flatten() {
                if let Err(e) = revoke_token_file(&creds, &entry.path()).await {
                    out.warning(&format!(
                        "Could not revoke a vault token of {name}: {e}. It expires at the environment's deadline."
                    ));
                }
            }
        }
        if !state.ip.is_empty() {
            close_connection(&control_path(dir, &state.id), &state.ip).await;
        }
    }

    let deleted = api::delete_environment(&creds, &state.project_ref(), &state.id).await?;
    if let Some(dir) = &dir {
        std::fs::remove_dir_all(dir).with_context(|| format!("removing {}", dir.display()))?;
    }
    if deleted {
        out.success(&format!("Deleted {name}."));
    } else {
        out.success(&format!("{name} was already gone."));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn state(id: &str, name: &str, project: &str) -> EnvState {
        EnvState {
            id: id.into(),
            name: name.into(),
            org: "acme".into(),
            project: project.into(),
            ip: "203.0.113.7".into(),
            port: 23456,
            deadline: "2026-10-10T16:00:00Z".into(),
        }
    }

    fn save(root: &Path, state: &EnvState) -> PathBuf {
        let dir = root.join(&state.id);
        create_private_dir(&dir).unwrap();
        write_private(
            &dir.join(STATE_FILE),
            serde_json::to_string(state).unwrap().as_bytes(),
        )
        .unwrap();
        dir
    }

    fn mode(path: &Path) -> u32 {
        std::fs::metadata(path).unwrap().permissions().mode() & 0o777
    }

    #[test]
    fn file_list_drops_git_and_fed_but_keeps_the_cloud_link() {
        let raw = b"src/main.rs\0.gitignore\0.git/config\0sub/.git\0sub/.git/HEAD\0.fed/cloud.yaml\0.fed/.gitignore\0.fed/state.db\0sub/.fed/cloud.yaml\0.github/ci.yml\0.fedrc\0a.git/x\0";
        let kept = filter_file_list(raw);
        let kept: Vec<&[u8]> = kept.iter().map(Vec::as_slice).collect();
        assert_eq!(
            kept,
            vec![
                &b"src/main.rs"[..],
                b".gitignore",
                b".fed/cloud.yaml",
                b".github/ci.yml",
                b".fedrc",
                b"a.git/x",
            ]
        );
    }

    #[test]
    fn file_list_keeps_names_with_spaces_newlines_and_other_bytes() {
        let raw = b"my file.txt\0line\nbreak.txt\0caf\xc3\xa9\0raw\xff\0\0";
        let kept = filter_file_list(raw);
        assert_eq!(
            kept,
            vec![
                b"my file.txt".to_vec(),
                b"line\nbreak.txt".to_vec(),
                b"caf\xc3\xa9".to_vec(),
                b"raw\xff".to_vec(),
            ]
        );
    }

    #[test]
    fn files_to_push_reads_git_and_skips_deleted_files() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        let git = |args: &[&str]| {
            let ok = std::process::Command::new("git")
                .arg("-C")
                .arg(root)
                .args(args)
                .env("GIT_CONFIG_GLOBAL", "/dev/null")
                .output()
                .unwrap()
                .status
                .success();
            assert!(ok, "git {args:?}");
        };
        git(&["init", "-q"]);
        std::fs::write(root.join(".gitignore"), "ignored.txt\n").unwrap();
        std::fs::write(root.join("a b.txt"), "x").unwrap();
        std::fs::write(root.join("new\nline.txt"), "x").unwrap();
        std::fs::write(root.join("ignored.txt"), "x").unwrap();
        std::fs::write(root.join("gone.txt"), "x").unwrap();
        std::fs::create_dir_all(root.join(".fed")).unwrap();
        std::fs::write(root.join(".fed/cloud.yaml"), "org: a\nproject: b\n").unwrap();
        std::fs::write(root.join(".fed/state.db"), "x").unwrap();
        git(&["add", "gone.txt", ".gitignore"]);
        std::fs::remove_file(root.join("gone.txt")).unwrap();

        let mut files: Vec<String> = files_to_push(root)
            .unwrap()
            .files
            .into_iter()
            .map(|p| String::from_utf8(p).unwrap())
            .collect();
        files.sort();
        assert_eq!(
            files,
            vec![".fed/cloud.yaml", ".gitignore", "a b.txt", "new\nline.txt"]
        );
    }

    #[test]
    fn workspace_names_are_strict() {
        for good in ["app", "my-app", "app_2", "App.v2", &"a".repeat(64)] {
            assert!(valid_workspace_name(good), "{good}");
        }
        for bad in [
            "",
            ".",
            "..",
            ".hidden",
            "-x",
            "a/b",
            "a b",
            "a;b",
            "a$b",
            "a'b",
            "å",
            &"a".repeat(65),
        ] {
            assert!(!valid_workspace_name(bad), "{bad}");
        }
    }

    #[test]
    fn workspace_name_defaults_to_the_folder_and_rejects_a_bad_one() {
        assert_eq!(workspace_name(Path::new("/code/app"), None).unwrap(), "app");
        assert_eq!(
            workspace_name(Path::new("/code/app"), Some("bob")).unwrap(),
            "bob"
        );
        let err = workspace_name(Path::new("/code/my app"), None)
            .unwrap_err()
            .to_string();
        assert!(err.contains("--as"), "{err}");
        assert!(workspace_name(Path::new("/code/app"), Some("../x")).is_err());
    }

    #[test]
    fn known_hosts_uses_bracketed_ip_and_port_without_the_comment() {
        assert_eq!(
            known_hosts_line(
                "203.0.113.7",
                23456,
                "ssh-ed25519 AAAAC3Nz fed-remote box host"
            )
            .unwrap(),
            "[203.0.113.7]:23456 ssh-ed25519 AAAAC3Nz\n"
        );
        assert_eq!(
            known_hosts_line("2001:db8::1", 2222, "ssh-ed25519 AAAA\n").unwrap(),
            "[2001:db8::1]:2222 ssh-ed25519 AAAA\n"
        );
        assert_eq!(
            known_hosts_line("203.0.113.7", 22, "ssh-ed25519 AAAA").unwrap(),
            "203.0.113.7 ssh-ed25519 AAAA\n"
        );
        assert!(known_hosts_line("203.0.113.7", 1, "ssh-ed25519").is_err());
        assert!(known_hosts_line("1.2.3.4 evil", 1, "ssh-ed25519 AAAA").is_err());
        assert!(known_hosts_line("", 1, "ssh-ed25519 AAAA").is_err());
    }

    #[test]
    fn local_ports_are_offset_unless_that_overflows() {
        assert_eq!(local_port(5432), 15432);
        assert_eq!(local_port(18743), 28743);
        assert_eq!(local_port(55535), 65535);
        assert_eq!(local_port(55536), 55536);
        assert_eq!(local_port(65535), 65535);
    }

    #[test]
    fn ports_json_is_grouped_and_sorted_by_port() {
        let json = r#"{"WEB_PORT": 18080, "DB_PORT": 15432, "ALSO_WEB": 18080}"#;
        assert_eq!(
            parse_ports(json).unwrap(),
            vec![
                (15432, vec!["DB_PORT".to_string()]),
                (18080, vec!["ALSO_WEB".to_string(), "WEB_PORT".to_string()]),
            ]
        );
        assert_eq!(parse_ports("{}\n").unwrap(), vec![]);
        assert!(parse_ports("Port Allocations").is_err());
    }

    #[test]
    fn token_ttl_runs_to_the_deadline_within_the_cloud_range() {
        let now = chrono::DateTime::parse_from_rfc3339("2026-10-10T15:00:00Z")
            .unwrap()
            .with_timezone(&chrono::Utc);
        assert_eq!(token_ttl("2026-10-10T16:00:00Z", now).unwrap(), 3600);
        assert_eq!(token_ttl("2026-10-11T16:00:00Z", now).unwrap(), 21_600);
        assert_eq!(token_ttl("2026-10-10T15:01:00Z", now).unwrap(), 60);
        assert!(token_ttl("2026-10-10T15:00:59Z", now).is_err());
        assert!(token_ttl("2026-10-10T14:00:00Z", now).is_err());
        assert!(token_ttl("tomorrow", now).is_err());
    }

    #[test]
    fn ssh_args_carry_every_required_option_and_no_control_master() {
        // `-F none` comes first, so no option from ~/.ssh/config applies.
        let args: Vec<String> = ssh_base_args(Path::new("/s/env"), 23456)
            .into_iter()
            .map(|a| a.into_string().unwrap())
            .collect();
        assert_eq!(
            args,
            [
                "-F",
                "none",
                "-i",
                "/s/env/client_key",
                "-p",
                "23456",
                "-o",
                "IdentitiesOnly=yes",
                "-o",
                "UserKnownHostsFile=/s/env/known_hosts",
                "-o",
                "StrictHostKeyChecking=yes",
                "-o",
                "ConnectTimeout=10",
                "-o",
                "ServerAliveInterval=15",
            ]
        );
    }

    #[test]
    fn ssh_args_quote_paths_with_spaces() {
        let args: Vec<String> = ssh_base_args(Path::new("/Users/a b/env"), 1)
            .into_iter()
            .map(|a| a.into_string().unwrap())
            .collect();
        assert!(args.contains(&"UserKnownHostsFile=\"/Users/a b/env/known_hosts\"".to_string()));
        assert!(args.contains(&"/Users/a b/env/client_key".to_string()));
    }

    #[test]
    fn control_path_fits_a_unix_socket_with_ssh_temp_suffix() {
        let limit = fed::fed_dir::max_socket_path_len();
        let short = control_path(Path::new("/Users/a/.fed/remote/abc"), "abc");
        assert_eq!(short, Path::new("/Users/a/.fed/remote/abc/ssh.sock"));
        let deep = PathBuf::from(format!("/Users/{}/.fed/remote/id", "x".repeat(80)));
        let fallback = control_path(&deep, "0b6f0c1e-1111-2222-3333-444455556666");
        assert!(fallback.starts_with("/tmp"), "{}", fallback.display());
        assert!(fallback.as_os_str().len() + 17 <= limit);
        assert_ne!(fallback, control_path(&deep, "another-id"));
    }

    #[test]
    fn state_dirs_are_private() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join(".fed/remote");
        create_private_dir(&root).unwrap();
        assert_eq!(mode(&root), 0o700);
        std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o755)).unwrap();
        create_private_dir(&root).unwrap();
        assert_eq!(mode(&root), 0o700, "an existing loose dir is tightened");
        let dir = save(&root, &state("id-1", "box", "web"));
        assert_eq!(mode(&dir), 0o700);
        assert_eq!(mode(&dir.join(STATE_FILE)), 0o600);
    }

    #[test]
    fn environments_are_found_by_name_and_the_link_breaks_ties() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        save(root, &state("id-1", "box", "web"));
        save(root, &state("id-2", "box", "api"));
        save(root, &state("id-3", "other", "web"));
        std::fs::create_dir_all(root.join(".new-123")).unwrap();

        let found = |name, project: Option<&api::ProjectRef>| match lookup(root, name, project) {
            Lookup::Found(_, state) => Some(state.id),
            Lookup::Missing => None,
            Lookup::Ambiguous(all) => Some(format!("{} matches", all.len())),
        };
        assert_eq!(found("other", None).as_deref(), Some("id-3"));
        assert_eq!(found("box", None).as_deref(), Some("2 matches"));
        let api_project = api::ProjectRef {
            org: "acme".into(),
            project: "api".into(),
        };
        assert_eq!(found("box", Some(&api_project)).as_deref(), Some("id-2"));
        let elsewhere = api::ProjectRef {
            org: "acme".into(),
            project: "cli".into(),
        };
        assert_eq!(found("box", Some(&elsewhere)).as_deref(), Some("2 matches"));
        assert_eq!(found("nope", None), None);
    }

    #[test]
    fn submodules_are_found_by_their_mode() {
        let staged =
            b"100644 aaaa 0\tsrc/main.rs\x00160000 bbbb 0\tvendor/lib\x00120000 cccc 0\tlink\0";
        assert_eq!(submodule_paths(staged), vec![b"vendor/lib".to_vec()]);
        assert!(submodule_paths(b"").is_empty());
    }

    #[test]
    fn stale_staging_dirs_are_removed_and_fresh_ones_kept() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        let staging = root.join(".new-0123");
        std::fs::create_dir_all(&staging).unwrap();
        std::fs::write(staging.join("host_key"), "private").unwrap();
        let env = save(root, &state("id-1", "box", "web"));
        let now = std::time::SystemTime::now();
        remove_stale_staging(root, now);
        assert!(staging.exists(), "a staging dir from a running up stays");
        remove_stale_staging(root, now + STALE_STAGING + Duration::from_secs(60));
        assert!(!staging.exists());
        assert!(env.exists(), "environment state is not staging");
    }

    #[test]
    fn versions_parse_from_fed_version_output() {
        assert_eq!(parse_version("fed 8.3.0\n"), Some([8, 3, 0]));
        assert_eq!(parse_version("8.10.2"), Some([8, 10, 2]));
        assert_eq!(parse_version("v1.2.3"), Some([1, 2, 3]));
        assert_eq!(parse_version("fed 8.4.0-rc.1"), None);
        assert_eq!(parse_version("fed 8.4"), None);
        assert_eq!(parse_version("fed 1.2.3.4"), None);
        assert_eq!(parse_version(""), None);
        assert!(parse_version("fed 8.10.0") > parse_version("fed 8.9.9"));
    }

    #[test]
    fn install_script_downloads_the_release_and_checks_it() {
        let script = install_fed_script([8, 4, 0]);
        assert!(
            script.contains(
                "https://github.com/service-federation/fed/releases/download/v8.4.0/fed-$t.tar.xz"
            ),
            "{script}"
        );
        assert!(script.contains("sha256sum -c"), "{script}");
        assert!(script.contains("/usr/local/bin/fed"), "{script}");
        assert!(script.contains(&format!("exit {NO_RELEASE}")), "{script}");
    }

    #[test]
    fn openrsync_is_told_apart_from_rsync() {
        assert!(is_openrsync(
            "openrsync: protocol version 29\nrsync version 2.6.9 compatible\n"
        ));
        assert!(!is_openrsync(
            "rsync  version 3.2.7  protocol version 31\nCopyright (C) 1996-2022\n"
        ));
        assert!(!is_openrsync("rsync  version 2.6.9  protocol version 29\n"));
    }

    #[test]
    fn rsync_shell_quotes_spaces_and_refuses_a_quote() {
        let shell = rsync_shell(ssh_base_args(Path::new("/Users/a b/env"), 1)).unwrap();
        assert!(
            shell.starts_with("ssh -F none -i '/Users/a b/env/client_key'"),
            "{shell}"
        );
        let err = rsync_shell(ssh_base_args(Path::new("/Users/o'neil/env"), 1))
            .unwrap_err()
            .to_string();
        assert!(err.contains("contains a ' character"), "{err}");
    }

    #[test]
    fn ssh_commands_keep_their_quoting_and_run_in_the_workspace() {
        let words: Vec<String> = ["echo", "a b", "it's", "$HOME"].map(String::from).to_vec();
        assert_eq!(
            remote_command("box", None, &words),
            r#"echo 'a b' 'it'\''s' '$HOME'"#
        );
        let in_ws = remote_command("box", Some("app"), &words);
        assert!(
            in_ws.starts_with("cd /srv/app 2>/dev/null || { echo "),
            "{in_ws}"
        );
        assert!(
            in_ws.ends_with(r#"exit 1; }; echo 'a b' 'it'\''s' '$HOME'"#),
            "{in_ws}"
        );
        assert_eq!(remote_shell(None), None);
        assert_eq!(
            remote_shell(Some("app")).unwrap(),
            r#"cd /srv/app 2>/dev/null; exec "${SHELL:-/bin/sh}" -l"#
        );
    }

    /// The push cleanup runs with the real tools on a copy of a workspace:
    /// files the last push copied and this one does not are deleted, build
    /// output, fed's state and anything outside the workspace stay.
    #[test]
    fn push_cleanup_deletes_only_files_an_earlier_push_copied() {
        let comm_z = std::process::Command::new("comm")
            .args(["-z", "/dev/null", "/dev/null"])
            .output()
            .is_ok_and(|o| o.status.success());
        if !comm_z {
            eprintln!("skipped: this machine's comm has no -z");
            return;
        }
        let tmp = tempfile::tempdir().unwrap();
        // The script compares resolved paths, and /var is a symlink on macOS.
        let srv = tmp.path().canonicalize().unwrap().join("srv");
        let ws = srv.join("app");
        let outside = tmp.path().join("outside");
        std::fs::create_dir_all(ws.join("src/old")).unwrap();
        std::fs::create_dir_all(ws.join(".fed")).unwrap();
        std::fs::create_dir_all(ws.join("node_modules")).unwrap();
        std::fs::create_dir_all(&outside).unwrap();
        std::fs::create_dir_all(ws.join("deep/a/b")).unwrap();
        std::fs::create_dir_all(ws.join("deep/keep")).unwrap();
        for file in [
            "keep.txt",
            "gone.txt",
            "-n",
            "src/old/gone.rs",
            "deep/a/b/gone.txt",
            "deep/keep/kept.txt",
            "node_modules/x.js",
            ".fed/lock.db",
        ] {
            std::fs::write(ws.join(file), "x").unwrap();
        }
        std::fs::write(outside.join("victim"), "x").unwrap();
        std::os::unix::fs::symlink(&outside, ws.join("escape")).unwrap();
        std::fs::write(
            ws.join(PUSH_MANIFEST),
            b"keep.txt\0gone.txt\0-n\0src/old/gone.rs\0deep/a/b/gone.txt\0deep/keep/kept.txt\0escape/victim\0",
        )
        .unwrap();

        let script = push_cleanup_script("app").replace("/srv/", &format!("{}/", srv.display()));
        let mut child = std::process::Command::new("sh")
            .arg("-c")
            .arg(&script)
            .stdin(Stdio::piped())
            .spawn()
            .unwrap();
        use std::io::Write;
        child
            .stdin
            .take()
            .unwrap()
            .write_all(b"keep.txt\0deep/keep/kept.txt\0new.txt\0")
            .unwrap();
        assert!(child.wait().unwrap().success());

        assert!(ws.join("keep.txt").exists());
        assert!(!ws.join("gone.txt").exists());
        assert!(!ws.join("-n").exists(), "a name like an option is a name");
        assert!(!ws.join("src").exists(), "emptied folders go too");
        assert!(!ws.join("deep/a").exists());
        assert!(ws.join("deep/keep/kept.txt").exists());
        assert!(ws.join("node_modules/x.js").exists());
        assert!(ws.join(".fed/lock.db").exists());
        assert!(outside.join("victim").exists());
        assert_eq!(
            std::fs::read(ws.join(PUSH_MANIFEST)).unwrap(),
            b"deep/keep/kept.txt\0keep.txt\0new.txt\0"
        );
    }

    #[test]
    fn deleted_message_says_why_and_what_to_do() {
        assert_eq!(
            deleted_message("box"),
            "box no longer exists. It was deleted, or it expired (5 idle minutes or 6 hours). Create it again with `fed remote up box`."
        );
    }

    #[test]
    fn a_passed_deadline_is_noticed() {
        let now = chrono::DateTime::parse_from_rfc3339("2026-10-10T15:00:00Z")
            .unwrap()
            .with_timezone(&chrono::Utc);
        assert!(deadline_passed("2026-10-10T14:59:59Z", now));
        assert!(!deadline_passed("2026-10-10T15:00:01Z", now));
        assert!(!deadline_passed("garbage", now));
    }
}
