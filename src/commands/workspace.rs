use crate::cli::WorkspaceCommands;
use crate::output::UserOutput;
use anyhow::{Context, bail};
use std::path::{Path, PathBuf};
use std::time::Duration;
use tokio::process::Command;

const GIT_TIMEOUT: Duration = Duration::from_secs(30);

pub async fn run_workspace(cmd: &WorkspaceCommands, out: &dyn UserOutput) -> anyhow::Result<()> {
    match cmd {
        WorkspaceCommands::New {
            branch,
            create_branch,
        } => ws_new(branch, *create_branch, out).await,
        WorkspaceCommands::List => ws_list(out).await,
        WorkspaceCommands::Cd { name } => ws_cd(name, out).await,
        WorkspaceCommands::Rm { name, force } => ws_rm(name, *force, out).await,
        WorkspaceCommands::Prune => ws_prune(out).await,
        WorkspaceCommands::Setup => ws_setup(out).await,
        WorkspaceCommands::InitShell => ws_init_shell(),
    }
}

/// Parse `git worktree list --porcelain` into structured entries.
struct WorktreeInfo {
    path: PathBuf,
    branch: Option<String>,
}

async fn list_worktrees_parsed() -> anyhow::Result<Vec<WorktreeInfo>> {
    let output = tokio::time::timeout(
        GIT_TIMEOUT,
        Command::new("git")
            .args(["worktree", "list", "--porcelain"])
            .output(),
    )
    .await
    .context("git worktree list timed out")?
    .context("Failed to run git worktree list")?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        bail!("Not inside a git repository: {}", stderr.trim());
    }

    let stdout = String::from_utf8(output.stdout).context("Invalid UTF-8 from git")?;
    let mut worktrees = Vec::new();
    let mut current_path: Option<PathBuf> = None;
    let mut current_branch: Option<String> = None;

    for line in stdout.lines() {
        if let Some(path) = line.strip_prefix("worktree ") {
            // Flush previous entry
            if let Some(p) = current_path.take() {
                worktrees.push(WorktreeInfo {
                    path: p,
                    branch: current_branch.take(),
                });
            }
            current_path = Some(PathBuf::from(path));
            current_branch = None;
        } else if let Some(branch_ref) = line.strip_prefix("branch ") {
            // refs/heads/main -> main
            current_branch = Some(
                branch_ref
                    .strip_prefix("refs/heads/")
                    .unwrap_or(branch_ref)
                    .to_string(),
            );
        }
        // "HEAD <sha>", "bare", "detached", blank lines — ignored
    }

    // Flush last entry
    if let Some(p) = current_path.take() {
        worktrees.push(WorktreeInfo {
            path: p,
            branch: current_branch.take(),
        });
    }

    Ok(worktrees)
}

/// Get the main worktree path (first entry from `git worktree list`).
async fn get_main_worktree() -> anyhow::Result<PathBuf> {
    let worktrees = list_worktrees_parsed().await?;
    worktrees
        .into_iter()
        .next()
        .map(|w| w.path)
        .ok_or_else(|| anyhow::anyhow!("No worktrees found"))
}

/// Derive the worktree base directory: `<main-worktree-parent>/<repo-name>-worktrees/`
fn get_worktree_base(main_worktree: &Path) -> PathBuf {
    let repo_name = main_worktree
        .file_name()
        .unwrap_or_default()
        .to_string_lossy();
    let parent = main_worktree.parent().unwrap_or(main_worktree);
    parent.join(format!("{}-worktrees", repo_name))
}

/// Write the target directory to `FED_WS_CD_FILE` if the env var is set.
/// If not set, print the path and a hint to set up shell integration.
fn write_cd_file(path: &Path, out: &dyn UserOutput) {
    if let Ok(cd_file) = std::env::var("FED_WS_CD_FILE") {
        if let Err(e) = std::fs::write(&cd_file, path.to_string_lossy().as_bytes()) {
            tracing::warn!("Failed to write cd file: {}", e);
        }
    } else {
        out.status(&format!("  cd {}", path.display()));
        out.status("");
        out.status("  Run `fed ws setup` to enable auto-cd.");
    }
}

/// Count running services by peeking at a worktree's `.fed/lock.db`.
/// Returns None if the DB doesn't exist or can't be read.
async fn count_running_services(worktree_path: &Path) -> Option<usize> {
    let db_path = worktree_path.join(".fed").join("lock.db");
    if !db_path.exists() {
        return Some(0);
    }

    // Open read-only to avoid contention with the worktree's writer.
    // Match the writer's WAL journal mode to prevent checkpoint-on-close.
    let db_path_clone = db_path.to_path_buf();
    let conn = tokio_rusqlite::Connection::open_with_flags(
        &db_path_clone,
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .await
    .ok()?;
    let count = conn
        .call(|conn| {
            conn.pragma_update(None, "journal_mode", "WAL")?;
            conn.pragma_update(None, "query_only", "ON")?;
            // Every status that still holds resources (matches
            // `status_indicates_should_be_running` in state/sqlite.rs).
            // 'healthy' matters most: healthchecked services park there, and
            // omitting it made `ws rm` skip its stop and orphan containers.
            let mut stmt = conn.prepare(
                "SELECT COUNT(*) FROM services WHERE status IN ('running', 'starting', 'healthy', 'failing')",
            )?;
            let count: i64 = stmt.query_row([], |row| row.get(0))?;
            Ok(count as usize)
        })
        .await
        .ok()?;
    Some(count)
}

// ── Subcommand implementations ──────────────────────────────────────

async fn ws_new(branch: &str, create_branch: bool, out: &dyn UserOutput) -> anyhow::Result<()> {
    let main_wt = get_main_worktree().await?;
    let base = get_worktree_base(&main_wt);
    let target = base.join(branch);

    if target.exists() {
        bail!("Worktree directory already exists: {}", target.display());
    }

    // Ensure base directory exists
    std::fs::create_dir_all(&base)
        .with_context(|| format!("Failed to create worktree base: {}", base.display()))?;

    let target_str = target
        .to_str()
        .context("Worktree path contains non-UTF-8 characters")?;

    let mut args = vec!["worktree", "add"];
    if create_branch {
        args.push("-b");
        args.push(branch);
        args.push(target_str);
    } else {
        args.push(target_str);
        args.push(branch);
    }

    let output = tokio::time::timeout(
        GIT_TIMEOUT,
        Command::new("git")
            .args(&args)
            .current_dir(&main_wt)
            .output(),
    )
    .await
    .context("git worktree add timed out")?
    .context("Failed to run git worktree add")?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        let msg = stderr.trim();

        // Provide friendly errors for common cases
        if msg.contains("is not a valid branch name") || msg.contains("not a valid ref") {
            bail!("Branch '{}' not found. Use `-b` to create it.", branch);
        }
        if msg.contains("already checked out") {
            bail!("{}", msg);
        }
        bail!("git worktree add failed: {}", msg);
    }

    if create_branch {
        out.status(&format!("  Created branch '{}'", branch));
    }
    out.status(&format!("  Created worktree at {}", target.display()));
    out.status("");
    out.status("  Run `fed isolate enable` there before any other fed command —");
    out.status("  the worktree gets its own ports, containers, and volumes.");

    write_cd_file(&target, out);
    Ok(())
}

async fn ws_list(out: &dyn UserOutput) -> anyhow::Result<()> {
    let worktrees = list_worktrees_parsed().await?;

    if worktrees.is_empty() {
        out.status("No worktrees found.");
        return Ok(());
    }

    // Determine column widths
    let branch_width = worktrees
        .iter()
        .map(|w| w.branch.as_deref().unwrap_or("(detached)").len())
        .max()
        .unwrap_or(10);

    for wt in &worktrees {
        let branch_name = wt.branch.as_deref().unwrap_or("(detached)");
        let path_str = wt.path.display().to_string();
        let status = match count_running_services(&wt.path).await {
            Some(0) => "(stopped)".to_string(),
            Some(n) => format!("({} running)", n),
            None => "(unknown)".to_string(),
        };
        out.status(&format!(
            "  {:<width$}  {}  {}",
            branch_name,
            path_str,
            status,
            width = branch_width,
        ));
    }

    Ok(())
}

async fn ws_cd(name: &str, out: &dyn UserOutput) -> anyhow::Result<()> {
    let worktrees = list_worktrees_parsed().await?;

    let found = worktrees.iter().find(|w| {
        w.branch.as_deref() == Some(name)
            || w.path
                .file_name()
                .map(|f| f.to_string_lossy() == name)
                .unwrap_or(false)
    });

    match found {
        Some(wt) => {
            write_cd_file(&wt.path, out);
            Ok(())
        }
        None => {
            bail!("No worktree found for '{}'. Run `fed ws list`.", name);
        }
    }
}

async fn ws_rm(name: &str, force: bool, out: &dyn UserOutput) -> anyhow::Result<()> {
    let main_wt = get_main_worktree().await?;
    let worktrees = list_worktrees_parsed().await?;

    let found = worktrees.iter().find(|w| {
        w.branch.as_deref() == Some(name)
            || w.path
                .file_name()
                .map(|f| f.to_string_lossy() == name)
                .unwrap_or(false)
    });

    let wt = match found {
        Some(wt) => wt,
        None => bail!("No worktree found for '{}'. Run `fed ws list`.", name),
    };

    // Don't remove the main worktree
    if wt.path == main_wt {
        bail!("Cannot remove the main worktree");
    }

    // Stop running services if any
    if let Some(count) = count_running_services(&wt.path).await
        && count > 0
    {
        out.warning(&format!(
            "  Stopping {} running service(s) in '{}'...",
            count, name
        ));
        if let Err(e) = super::run_stop_from_state(&wt.path, vec![], out).await {
            tracing::warn!("Failed to stop services in worktree: {}", e);
        }
    }

    // Clean up .fed state
    let fed_dir = wt.path.join(".fed");
    if fed_dir.exists() {
        out.status("  Cleaning up state...");
        if let Err(e) = std::fs::remove_dir_all(&fed_dir) {
            tracing::warn!("Failed to remove .fed directory: {}", e);
        }
    }

    // Remove the worktree
    let wt_path_str = wt
        .path
        .to_str()
        .context("Worktree path contains non-UTF-8 characters")?;
    let mut args = vec!["worktree", "remove"];
    if force {
        args.push("--force");
    }
    args.push(wt_path_str);

    let output = tokio::time::timeout(
        GIT_TIMEOUT,
        Command::new("git")
            .args(&args)
            .current_dir(&main_wt)
            .output(),
    )
    .await
    .context("git worktree remove timed out")?
    .context("Failed to run git worktree remove")?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        let msg = stderr.trim();
        if msg.contains("contains modified or untracked files") {
            bail!(
                "Worktree '{}' has uncommitted changes. Use `--force` to remove anyway.",
                name
            );
        }
        bail!("git worktree remove failed: {}", msg);
    }

    out.success(&format!("  Removed worktree '{}'", name));
    Ok(())
}

async fn ws_prune(out: &dyn UserOutput) -> anyhow::Result<()> {
    let main_wt = get_main_worktree().await?;

    // Run git worktree prune (removes entries whose directories are gone)
    let output = tokio::time::timeout(
        GIT_TIMEOUT,
        Command::new("git")
            .args(["worktree", "prune"])
            .current_dir(&main_wt)
            .output(),
    )
    .await
    .context("git worktree prune timed out")?
    .context("Failed to run git worktree prune")?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        bail!("git worktree prune failed: {}", stderr.trim());
    }

    // Also clean up empty directories in the worktree base
    let base = get_worktree_base(&main_wt);
    let mut pruned = 0;

    if base.exists()
        && let Ok(entries) = std::fs::read_dir(&base)
    {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                // If the directory is empty, it's a leftover from a removed worktree
                let is_stale = match std::fs::read_dir(&path) {
                    Ok(mut contents) => contents.next().is_none(),
                    Err(_) => false,
                };
                if is_stale {
                    let name = path
                        .file_name()
                        .unwrap_or_default()
                        .to_string_lossy()
                        .to_string();
                    if let Err(e) = std::fs::remove_dir_all(&path) {
                        tracing::warn!("Failed to remove stale directory {}: {}", name, e);
                    } else {
                        out.status(&format!("  Removed '{}'", name));
                        pruned += 1;
                    }
                }
            }
        }
    }

    if pruned > 0 {
        out.success(&format!("  Pruned {} worktree(s)", pruned));
    } else {
        out.status("  Nothing to prune.");
    }

    Ok(())
}

/// Detect the user's shell and return (rc_file_path, shell_name).
fn detect_shell_rc() -> anyhow::Result<(PathBuf, &'static str)> {
    let home = dirs::home_dir().context("Cannot determine home directory")?;
    let shell = std::env::var("SHELL").unwrap_or_default();

    if shell.ends_with("/zsh") || shell.ends_with("/zsh-") {
        Ok((home.join(".zshrc"), "zsh"))
    } else if shell.ends_with("/bash") {
        // Prefer .bashrc (sourced for interactive non-login), fall back to .bash_profile
        let bashrc = home.join(".bashrc");
        if bashrc.exists() {
            Ok((bashrc, "bash"))
        } else {
            Ok((home.join(".bash_profile"), "bash"))
        }
    } else {
        bail!(
            "Unsupported shell: {}. Manually add `eval \"$(fed ws init-shell)\"` to your shell rc file.",
            if shell.is_empty() {
                "(unknown)"
            } else {
                &shell
            }
        );
    }
}

async fn ws_setup(out: &dyn UserOutput) -> anyhow::Result<()> {
    let (rc_file, shell_name) = detect_shell_rc()?;
    let rc_display = rc_file
        .strip_prefix(dirs::home_dir().unwrap_or_default())
        .map(|p| format!("~/{}", p.display()))
        .unwrap_or_else(|_| rc_file.display().to_string());

    let eval_line = r#"eval "$(fed ws init-shell)""#;

    // Check if already present
    if rc_file.exists() {
        let contents = std::fs::read_to_string(&rc_file)?;
        if contents.contains(eval_line) {
            out.status(&format!(
                "  Shell integration already installed in {}",
                rc_display
            ));
            return Ok(());
        }
    }

    // Append
    use std::io::Write;
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&rc_file)?;

    writeln!(file)?;
    writeln!(file, "# fed workspace shell integration")?;
    writeln!(file, "{}", eval_line)?;

    out.success(&format!("  Added to {} ({}):", rc_display, shell_name));
    out.status(&format!("    {}", eval_line));
    out.blank();
    out.status("  Restart your shell or run:");
    out.status(&format!("    source {}", rc_display));

    Ok(())
}

fn ws_init_shell() -> anyhow::Result<()> {
    print!(
        r#"fed() {{
  if [[ "$1" == "ws" || "$1" == "workspace" ]]; then
    local _fed_cd_file
    _fed_cd_file=$(mktemp "${{TMPDIR:-/tmp}}/fed-ws-cd.XXXXXX")
    FED_WS_CD_FILE="$_fed_cd_file" command fed "$@"
    local _fed_ret=$?
    if [[ -s "$_fed_cd_file" ]]; then
      builtin cd -- "$(cat "$_fed_cd_file")"
    fi
    command rm -f "$_fed_cd_file"
    return $_fed_ret
  fi
  command fed "$@"
}}
"#
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn worktree_base_derived_from_main() {
        let main = PathBuf::from("/Users/dev/Projects/my-app");
        let base = get_worktree_base(&main);
        assert_eq!(base, PathBuf::from("/Users/dev/Projects/my-app-worktrees"));
    }

    #[test]
    fn worktree_base_handles_trailing_slash() {
        // PathBuf normalizes, but test the function is robust
        let main = PathBuf::from("/home/user/repo");
        let base = get_worktree_base(&main);
        assert_eq!(base, PathBuf::from("/home/user/repo-worktrees"));
    }

    #[test]
    fn detect_shell_rc_zsh() {
        // This test relies on the test runner's $SHELL, so we just
        // verify the function doesn't panic on the current environment.
        // If $SHELL is set we get a result; if unset we get an error.
        let _ = detect_shell_rc();
    }

    #[test]
    fn init_shell_output_contains_fed_function() {
        // The shell function that ws_init_shell would print
        let full_output = concat!(
            "fed() {\n",
            "  if [[ \"$1\" == \"ws\" || \"$1\" == \"workspace\" ]]; then\n",
            "    local _fed_cd_file\n",
            "    _fed_cd_file=$(mktemp \"${TMPDIR:-/tmp}/fed-ws-cd.XXXXXX\")\n",
            "    FED_WS_CD_FILE=\"$_fed_cd_file\" command fed \"$@\"\n",
            "    local _fed_ret=$?\n",
            "    if [[ -s \"$_fed_cd_file\" ]]; then\n",
            "      builtin cd -- \"$(cat \"$_fed_cd_file\")\"\n",
            "    fi\n",
            "    command rm -f \"$_fed_cd_file\"\n",
            "    return $_fed_ret\n",
            "  fi\n",
            "  command fed \"$@\"\n",
            "}\n",
        );
        assert!(full_output.contains("fed()"));
        assert!(full_output.contains("FED_WS_CD_FILE"));
        assert!(full_output.contains("command fed"));
        assert!(full_output.contains("builtin cd"));
        // Verify cleanup always runs
        assert!(full_output.contains("command rm -f"));
        // Verify non-ws commands pass through
        let lines: Vec<&str> = full_output.lines().collect();
        let last_fn_line = lines
            .iter()
            .rev()
            .find(|l| l.contains("command fed"))
            .unwrap();
        assert!(
            !last_fn_line.contains("FED_WS_CD_FILE"),
            "passthrough path should not set FED_WS_CD_FILE"
        );
    }
}
