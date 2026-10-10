//! `fed remote` (beta, behind the `remote-beta` cargo feature): disposable
//! machines in Service Federation Cloud that run a checkout's fed stack.
//!
//! The cloud API lives in `fed::cloud::remote`. This module holds what runs on
//! this machine: the SSH keys and state under `~/.fed/remote/<id>/`, the shared
//! SSH connection, the file copy with rsync and the port forwards.
//!
//! Tokens never appear in a process argument. The cloud token travels in HTTP
//! headers, and an env token travels to the machine over SSH stdin.

use crate::cli::RemoteCommands;
use crate::output::UserOutput;
use anyhow::{Context, Result, bail};
use fed::cloud::{self, remote as api};
use serde::{Deserialize, Serialize};
use std::ffi::OsString;
use std::os::unix::fs::{DirBuilderExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::{Duration, Instant};
use tokio::process::Command;

/// How long `fed remote up` waits for SSH to answer.
const SSH_WAIT: Duration = Duration::from_secs(300);
/// How long an environment lives without an SSH session.
const IDLE_MINUTES: u32 = 5;
/// Remote port P is forwarded from local P + PORT_OFFSET, so the same stack can
/// run on this machine at the same time.
const PORT_OFFSET: u16 = 10_000;

const STATE_FILE: &str = "env.json";

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
}

/// The env token minted for one workspace, in `tokens/<workspace>.json`. Only
/// the id: the token itself lives on the remote machine.
#[derive(Debug, Serialize, Deserialize)]
struct TokenRecord {
    id: String,
    org: String,
    project: String,
}

fn state_root() -> Result<PathBuf> {
    let home = dirs::home_dir().context("cannot determine the home directory")?;
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
    found.sort_by(|a, b| a.1.name.cmp(&b.1.name));
    found
}

/// The environment called `name`. When several projects have one by that name,
/// the checkout's link decides.
pub(crate) fn find_environment(
    root: &Path,
    name: &str,
    project: Option<&api::ProjectRef>,
) -> Result<(PathBuf, EnvState)> {
    let mut matches: Vec<_> = local_environments(root)
        .into_iter()
        .filter(|(_, s)| s.name == name)
        .collect();
    if matches.len() > 1
        && let Some(project) = project
    {
        matches.retain(|(_, s)| s.org == project.org && s.project == project.project);
    }
    match matches.len() {
        0 => bail!(
            "this machine has no environment called {name} — see `fed remote ls`, or create it with `fed remote up {name}`"
        ),
        1 => Ok(matches.remove(0)),
        _ => bail!(
            "several projects have an environment called {name} — run this from a checkout linked to the one you mean"
        ),
    }
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
    "use 1 to 64 letters, digits, '.', '_' and '-', starting with a letter or digit";

/// The workspace name: `--as`, or else the checkout's folder name.
fn workspace_name(checkout: &Path, given: Option<&str>) -> Result<String> {
    if let Some(name) = given {
        if !valid_workspace_name(name) {
            bail!("invalid workspace name `{name}`: {INVALID_WORKSPACE_NAME}");
        }
        return Ok(name.to_string());
    }
    let folder = checkout
        .file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_default();
    if !valid_workspace_name(&folder) {
        bail!(
            "the checkout folder `{folder}` cannot be a workspace name ({INVALID_WORKSPACE_NAME}) — pass one with --as"
        );
    }
    Ok(folder)
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

/// Filter the NUL-separated output of `git ls-files -z`. The result is
/// NUL-terminated, for `rsync --from0 --files-from=-`.
pub(crate) fn filter_file_list(raw: &[u8]) -> Vec<Vec<u8>> {
    raw.split(|c| *c == 0)
        .filter(|p| keep_path(p))
        .map(<[u8]>::to_vec)
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
        bail!("cloud: the environment's IP address `{ip}` is malformed");
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
        .context("the remote `fed ports list --json` printed something unexpected")?;
    let mut by_port: std::collections::BTreeMap<u16, Vec<String>> = Default::default();
    for (param, port) in map {
        by_port.entry(port).or_default().push(param);
    }
    Ok(by_port.into_iter().collect())
}

/// Seconds from `now` until `deadline`, as an env token lifetime. Capped at
/// the cloud's maximum.
pub(crate) fn token_ttl(deadline: &str, now: chrono::DateTime<chrono::Utc>) -> Result<i64> {
    let deadline = chrono::DateTime::parse_from_rfc3339(deadline)
        .with_context(|| format!("cloud: the environment deadline `{deadline}` is malformed"))?;
    let left = (deadline.with_timezone(&chrono::Utc) - now).num_seconds();
    if left < api::MIN_TOKEN_TTL {
        bail!(
            "the environment has less than a minute left — create a new one with `fed remote up`"
        );
    }
    Ok(left.min(api::MAX_TOKEN_TTL))
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
            "{} is not private to this user — remove it and retry",
            dir.display()
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
    fn open(dir: PathBuf, state: EnvState) -> Result<Self> {
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

    /// Open the shared connection unless it is already open. It is detached
    /// from every stream: started by an ordinary command, it would keep that
    /// command's output open and hang whoever reads it. It is a logged-in
    /// session, so it keeps the machine awake until 60 s after the last use.
    async fn share_connection(&self) {
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
            return;
        }
        // A socket left by a crashed master would make the new one fail to
        // listen and stay up as a plain session with no ControlPersist limit.
        let _ = std::fs::remove_file(&self.control);
        let mut cmd = Command::new("ssh");
        cmd.args(self.base_args());
        // With its streams closed, a prompt would wait forever.
        cmd.args(opt("BatchMode=yes"));
        cmd.args(opt("ControlMaster=yes"));
        cmd.args(opt("ControlPersist=60"));
        cmd.args(opt_path("ControlPath=", &self.control));
        cmd.args(["-f", "-N"]).arg(self.target());
        let _ = cmd
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .await;
    }

    async fn close_connection(&self) {
        let _ = Command::new("ssh")
            .args(opt_path("ControlPath=", &self.control))
            .args(["-O", "exit"])
            .arg(self.target())
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .await;
    }

    /// Run `command` on the machine with this terminal's streams.
    async fn run(&self, command: &str) -> Result<std::process::ExitStatus> {
        self.share_connection().await;
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
            bail!("{what} failed on {} ({status})", self.state.name);
        }
        Ok(())
    }

    /// Run `command` on the machine and return what it printed.
    async fn output(&self, command: &str) -> Result<String> {
        self.share_connection().await;
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
            bail!("`{command}` failed on {} ({})", self.state.name, out.status);
        }
        Ok(String::from_utf8_lossy(&out.stdout).into_owned())
    }

    /// Run `command` on the machine with `input` on its stdin.
    async fn run_with_input(&self, command: &str, input: &[u8]) -> Result<()> {
        use tokio::io::AsyncWriteExt;
        self.share_connection().await;
        let mut child = self
            .ssh()
            .arg(self.target())
            .arg(command)
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .spawn()
            .context("running ssh")?;
        if let Some(mut stdin) = child.stdin.take() {
            stdin.write_all(input).await?;
            stdin.shutdown().await?;
        }
        let status = child.wait().await?;
        if !status.success() {
            bail!("writing to {} failed ({status})", self.state.name);
        }
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

/// The ssh options for every call to an environment.
pub(crate) fn ssh_base_args(state_dir: &Path, port: u16) -> Vec<OsString> {
    let mut args: Vec<OsString> = vec!["-i".into(), state_dir.join("client_key").into()];
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
        bail!("{} is not in a git checkout", dir.display());
    }
    let root = String::from_utf8_lossy(&out.stdout).trim_end().to_string();
    Ok(PathBuf::from(root))
}

/// The files `fed remote push` copies: tracked and untracked-but-not-ignored,
/// filtered by `keep_path`, and only the ones that exist (a deleted tracked
/// file would fail the copy).
fn files_to_push(root: &Path) -> Result<Vec<Vec<u8>>> {
    use std::os::unix::ffi::OsStrExt;
    let out = std::process::Command::new("git")
        .arg("-C")
        .arg(root)
        .args(["ls-files", "-z", "-co", "--exclude-standard"])
        .output()
        .context("running git ls-files")?;
    if !out.status.success() {
        bail!(
            "git ls-files failed in {}: {}",
            root.display(),
            String::from_utf8_lossy(&out.stderr).trim()
        );
    }
    Ok(filter_file_list(&out.stdout)
        .into_iter()
        .filter(|p| std::fs::symlink_metadata(root.join(std::ffi::OsStr::from_bytes(p))).is_ok())
        .collect())
}

// ── Commands ──────────────────────────────────────────────────────────

fn credentials() -> Result<cloud::Credentials> {
    cloud::load_credentials().context("not signed in — run `fed login`")
}

fn linked_project(root: &Path) -> Option<api::ProjectRef> {
    cloud::load_link(root).map(|l| api::ProjectRef {
        org: l.org,
        project: l.project,
    })
}

fn required_project(root: &Path) -> Result<api::ProjectRef> {
    linked_project(root).context("this checkout isn't linked — run `fed link org/project`")
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
            up(&root, &checkout, name.as_deref(), kind.as_deref(), out).await
        }
        RemoteCommands::Ls => {
            let checkout = checkout_root(workdir)?;
            ls(&root, &checkout, out).await
        }
        RemoteCommands::Push { name, workspace } => {
            let checkout = checkout_root(workdir)?;
            let ws = workspace_name(&checkout, workspace.as_deref())?;
            let remote = open(&root, name, linked_project(&checkout).as_ref())?;
            push(&remote, &checkout, &ws, out).await
        }
        RemoteCommands::Start { name, workspace } => {
            let checkout = checkout_root(workdir)?;
            let ws = workspace_name(&checkout, workspace.as_deref())?;
            let remote = open(&root, name, linked_project(&checkout).as_ref())?;
            push(&remote, &checkout, &ws, out).await?;
            if let Some(project) = linked_project(&checkout) {
                give_env_token(&remote, &project, &ws, out).await?;
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
            let remote = open(&root, name, linked_project(&checkout).as_ref())?;
            connect(&remote, &ws, out).await
        }
        RemoteCommands::Ssh { name, command } => {
            let project = checkout_root(workdir).ok().and_then(|c| linked_project(&c));
            let remote = open(&root, name, project.as_ref())?;
            remote.share_connection().await;
            let mut ssh = remote.ssh();
            if command.is_empty() {
                ssh.arg("-t");
            }
            let status = ssh
                .arg(remote.target())
                .args(command)
                .status()
                .await
                .context("running ssh")?;
            std::process::exit(status.code().unwrap_or(1));
        }
        RemoteCommands::Down { name } => {
            let project = checkout_root(workdir).ok().and_then(|c| linked_project(&c));
            down(&root, name, project.as_ref(), out).await
        }
    }
}

fn open(root: &Path, name: &str, project: Option<&api::ProjectRef>) -> Result<Remote> {
    let (dir, state) = find_environment(root, name, project)?;
    Remote::open(dir, state)
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
        bail!("ssh-keygen failed ({status})");
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

    create_private_dir(root)?;
    let staging = root.join(format!(".new-{:016x}", rand::random::<u64>()));
    create_private_dir(&staging)?;
    let created = create(&creds, &project, &staging, &name, kind, out).await;
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
        bail!("cloud: the new environment has an invalid id — remove it in the dashboard");
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
    let (Some(ip), Some(port)) = (env.ip.clone(), env.port) else {
        bail!(
            "cloud: {name} has no address yet — remove it with `fed remote down {name}` and try again"
        );
    };
    let host_public = read_trimmed(&dir.join("host_key.pub"))?;
    write_private(
        &dir.join("known_hosts"),
        known_hosts_line(&ip, port, &host_public)?.as_bytes(),
    )?;

    let remote = Remote::open(dir, state)?;
    out.progress(&format!("Waiting for {name} to answer SSH ({ip}:{port}) "));
    let start = Instant::now();
    while !remote.answers().await {
        if start.elapsed() > SSH_WAIT {
            out.finish_progress("timed out");
            bail!(
                "{name} does not answer SSH after {} minutes — remove it with `fed remote down {name}`",
                SSH_WAIT.as_secs() / 60
            );
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
    out.finish_progress(&format!("after {}s", start.elapsed().as_secs()));
    remote
        .run_checked("cloud-init status --wait >/dev/null", "cloud-init")
        .await?;

    out.success(&format!("{name} is ready at {ip} (SSH port {port})."));
    out.status(&format!(
        "It deletes itself {IDLE_MINUTES} minutes after the last SSH session ends, and at {} in any case.",
        local_time(&env.deadline)
    ));
    out.status(&format!("Next: `fed remote start {name}`"));
    Ok(())
}

/// Make the keys in `staging` and ask the cloud for the environment. The host
/// private key is only needed for the request, so it is deleted after it.
async fn create(
    creds: &cloud::Credentials,
    project: &api::ProjectRef,
    staging: &Path,
    name: &str,
    kind: Option<&str>,
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
    let result = api::create_environment(creds, project, &body).await;
    let _ = std::fs::remove_file(staging.join("host_key"));
    match result {
        Ok(env) => {
            out.finish_progress("done");
            Ok(env)
        }
        Err(e) => {
            out.clear_progress();
            Err(e.into())
        }
    }
}

async fn ls(root: &Path, checkout: &Path, out: &dyn UserOutput) -> Result<()> {
    let creds = credentials()?;
    let project = required_project(checkout)?;
    let list = api::list_environments(&creds, &project).await?;

    // The cloud deletes idle environments without telling us. Forget their keys.
    for (dir, state) in local_environments(root) {
        if state.org == project.org
            && state.project == project.project
            && !list.iter().any(|e| e.id == state.id)
        {
            let _ = std::fs::remove_dir_all(&dir);
        }
    }

    if list.is_empty() {
        out.status(&format!(
            "No remote environments in {project}. Create one with `fed remote up NAME`."
        ));
        return Ok(());
    }
    let now = chrono::Utc::now();
    let rows: Vec<[String; 5]> = list
        .iter()
        .map(|e| {
            [
                e.name.clone(),
                e.kind.clone(),
                e.state.clone(),
                match (&e.ip, e.port) {
                    (Some(ip), Some(port)) => format!("{ip}:{port}"),
                    _ => "-".to_string(),
                },
                minutes_left(&e.deadline, now),
            ]
        })
        .collect();
    let header = ["NAME", "TYPE", "STATE", "ADDRESS", "LEFT"].map(String::from);
    let mut widths = header.clone().map(|h| h.len());
    for row in &rows {
        for (w, cell) in widths.iter_mut().zip(row) {
            *w = (*w).max(cell.len());
        }
    }
    for row in std::iter::once(&header).chain(&rows) {
        let line: Vec<String> = row
            .iter()
            .zip(widths)
            .map(|(cell, w)| format!("{cell:w$}"))
            .collect();
        out.status(line.join("  ").trim_end());
    }
    Ok(())
}

async fn push(remote: &Remote, checkout: &Path, ws: &str, out: &dyn UserOutput) -> Result<()> {
    use std::os::unix::ffi::OsStrExt;
    let files = files_to_push(checkout)?;
    remote
        .run_checked(&format!("mkdir -p /srv/{ws}"), "creating the workspace")
        .await?;
    let count = match files.len() {
        1 => "1 file".to_string(),
        n => format!("{n} files"),
    };
    out.status(&format!(
        "Pushing {count} to {}:/srv/{ws}",
        remote.state.name
    ));

    let ssh_command: Vec<String> = std::iter::once(OsString::from("ssh"))
        .chain(remote.shared_args())
        .map(|a| match a.into_string() {
            // rsync splits this command itself and knows quotes but not
            // backslashes, so a quote in a path cannot be escaped.
            Ok(a) if !a.contains('\'') => Ok(a),
            _ => Err(anyhow::anyhow!(
                "the state directory path must be valid UTF-8 without a ' in it"
            )),
        })
        .collect::<Result<_>>()?;
    let mut source = checkout.as_os_str().to_os_string();
    source.push("/");
    let mut child = Command::new("rsync")
        .args(["-a", "--from0", "--files-from=-", "-e"])
        .arg(shell_words::join(&ssh_command))
        .arg(source)
        .arg(format!("{}:/srv/{ws}/", remote.target()))
        .stdin(Stdio::piped())
        .spawn()
        .context("running rsync")?;
    let written = {
        use tokio::io::AsyncWriteExt;
        let mut stdin = child.stdin.take().context("rsync stdin")?;
        let mut list = Vec::new();
        for path in &files {
            list.extend_from_slice(std::ffi::OsStr::from_bytes(path).as_bytes());
            list.push(0);
        }
        let result = stdin.write_all(&list).await;
        drop(stdin);
        result
    };
    // When rsync stops early, its status says why. The broken pipe does not.
    let status = child.wait().await?;
    if !status.success() {
        bail!("rsync to {} failed ({status})", remote.state.name);
    }
    written.context("sending the file list to rsync")?;
    Ok(())
}

/// Give the workspace an env token: read access to the linked project's vault
/// until the environment's deadline. The previous token for the workspace is
/// revoked first, so there is one per workspace.
async fn give_env_token(
    remote: &Remote,
    project: &api::ProjectRef,
    ws: &str,
    out: &dyn UserOutput,
) -> Result<()> {
    let creds =
        credentials().context("this checkout uses the team vault — run `fed login` first")?;
    let base = cloud::cloud_base_url(&creds.url)?;
    let ttl = token_ttl(&remote.state.deadline, chrono::Utc::now())?;

    let tokens = remote.dir.join("tokens");
    create_private_dir(&tokens)?;
    let record_path = tokens.join(format!("{ws}.json"));
    revoke_token_file(&creds, &record_path, out).await;

    let label = format!("fed remote {}/{ws}", remote.state.name);
    let token = api::mint_env_token(&creds, project, &label, ttl).await?;
    let record = TokenRecord {
        id: token.id.clone(),
        org: project.org.clone(),
        project: project.project.clone(),
    };
    write_private(&record_path, serde_json::to_string(&record)?.as_bytes())?;

    let url = base.as_str().trim_end_matches('/');
    let command = format!(
        "umask 077 && mkdir -p /run/fedenv/tokens && cat > /run/fedenv/tokens/{ws} && printf '%s\\n' {} > /run/fedenv/cloud-url",
        shell_words::quote(url)
    );
    remote
        .run_with_input(&command, token.token.as_bytes())
        .await?;
    out.status(&format!(
        "Gave /srv/{ws} read access to {project}'s vault for {}m",
        ttl / 60
    ));
    Ok(())
}

/// Revoke the token in a record file and delete the file. A failure is a
/// warning: the token expires at the environment's deadline anyway.
async fn revoke_token_file(creds: &cloud::Credentials, path: &Path, out: &dyn UserOutput) {
    let Some(record) = std::fs::read_to_string(path)
        .ok()
        .and_then(|raw| serde_json::from_str::<TokenRecord>(&raw).ok())
    else {
        return;
    };
    let project = api::ProjectRef {
        org: record.org,
        project: record.project,
    };
    match api::revoke_env_token(creds, &project, &record.id).await {
        Ok(()) => {
            let _ = std::fs::remove_file(path);
        }
        Err(e) => out.warning(&format!(
            "could not revoke env token {}: {e}. It expires at the environment's deadline.",
            record.id
        )),
    }
}

async fn connect(remote: &Remote, ws: &str, out: &dyn UserOutput) -> Result<()> {
    let json = remote
        .output(&format!("cd /srv/{ws} && fed ports list --json"))
        .await?;
    let ports = parse_ports(&json)?;
    let name = &remote.state.name;
    if ports.is_empty() {
        bail!("/srv/{ws} on {name} has no ports — run `fed remote start {name}` first");
    }

    // One ssh per port, outside the shared connection, so Ctrl-C ends them and
    // one failed forward does not stop the others.
    let mut forwards = Vec::new();
    for (port, params) in &ports {
        let local = local_port(*port);
        let child = Command::new("ssh")
            .args(remote.base_args())
            .args(opt("ControlMaster=no"))
            .args(opt("ControlPath=none"))
            .args(opt("ExitOnForwardFailure=yes"))
            .arg("-N")
            .arg("-L")
            .arg(format!("{local}:127.0.0.1:{port}"))
            .arg(remote.target())
            .stdin(Stdio::null())
            .kill_on_drop(true)
            .spawn()
            .context("running ssh")?;
        out.status(&format!(
            "  localhost:{local} -> {name}:{port}  ({})",
            params.join(", ")
        ));
        forwards.push((local, child));
    }
    out.status(&format!(
        "Connected. Ctrl-C to disconnect. {name} deletes itself {IDLE_MINUTES} minutes after the last SSH session ends."
    ));

    // One listener for the whole loop, so a Ctrl-C between two rounds is kept.
    let mut interrupt = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())
        .context("listening for Ctrl-C")?;
    loop {
        let stopped = {
            let waits = forwards
                .iter_mut()
                .map(|(_, child)| Box::pin(child.wait()))
                .collect::<Vec<_>>();
            tokio::select! {
                _ = interrupt.recv() => None,
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
            "the forward from localhost:{local} stopped ({status})"
        ));
        if forwards.is_empty() {
            bail!("every port forward to {name} stopped");
        }
    }
    for (_, child) in &mut forwards {
        let _ = child.kill().await;
    }
    out.status("Disconnected.");
    Ok(())
}

async fn down(
    root: &Path,
    name: &str,
    project: Option<&api::ProjectRef>,
    out: &dyn UserOutput,
) -> Result<()> {
    let creds = credentials()?;
    let (dir, state) = match find_environment(root, name, project) {
        Ok(found) => (Some(found.0), found.1),
        Err(not_found) => {
            // Created on another machine: the cloud still knows it by name.
            let Some(project) = project else {
                return Err(not_found);
            };
            let list = api::list_environments(&creds, project).await?;
            let Some(env) = list.into_iter().find(|e| e.name == name) else {
                bail!("{project} has no environment called {name} — see `fed remote ls`");
            };
            let state = EnvState {
                id: env.id,
                name: env.name,
                org: project.org.clone(),
                project: project.project.clone(),
                ip: env.ip.unwrap_or_default(),
                port: env.port.unwrap_or(22),
                deadline: env.deadline,
            };
            (None, state)
        }
    };

    if let Some(dir) = &dir {
        if let Ok(entries) = std::fs::read_dir(dir.join("tokens")) {
            for entry in entries.flatten() {
                revoke_token_file(&creds, &entry.path(), out).await;
            }
        }
        if !state.ip.is_empty()
            && let Ok(remote) = Remote::open(dir.clone(), state.clone())
        {
            remote.close_connection().await;
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
        let args: Vec<String> = ssh_base_args(Path::new("/s/env"), 23456)
            .into_iter()
            .map(|a| a.into_string().unwrap())
            .collect();
        assert_eq!(
            args,
            [
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

        let (_, found) = find_environment(root, "other", None).unwrap();
        assert_eq!(found.id, "id-3");
        let err = find_environment(root, "box", None).unwrap_err().to_string();
        assert!(err.contains("several projects"), "{err}");
        let api_project = api::ProjectRef {
            org: "acme".into(),
            project: "api".into(),
        };
        let (_, found) = find_environment(root, "box", Some(&api_project)).unwrap();
        assert_eq!(found.id, "id-2");
        let err = find_environment(root, "nope", None)
            .unwrap_err()
            .to_string();
        assert!(err.contains("fed remote up nope"), "{err}");
    }
}
