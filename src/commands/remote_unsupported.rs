//! `fed remote` on platforms other than macOS and Linux. The commands drive
//! OpenSSH and rsync with Unix sockets and process groups, which Windows
//! does not have.

use crate::cli::RemoteCommands;
use crate::output::UserOutput;
use std::path::PathBuf;

pub async fn run_remote(
    _cmd: &RemoteCommands,
    _workdir: Option<PathBuf>,
    _out: &dyn UserOutput,
) -> anyhow::Result<()> {
    anyhow::bail!("fed remote needs macOS or Linux. Run it from one of those, or from WSL.")
}
