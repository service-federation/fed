use crate::output::UserOutput;
use fed::{Error as FedError, Orchestrator};

pub async fn run_script(
    orchestrator: &mut Orchestrator,
    name: &str,
    extra_args: &[String],
    _verbose: bool,
    out: &dyn UserOutput,
) -> anyhow::Result<()> {
    let available_scripts = orchestrator.list_scripts();

    if available_scripts.is_empty() {
        out.status("No scripts defined in configuration");
        return Ok(());
    }

    if !available_scripts.contains(&name.to_string()) {
        // One rich error, printed once by main: did-you-mean + script list.
        let mut msg = super::suggest::with_did_you_mean(
            &format!("Script '{}' not found.", name),
            name,
            available_scripts.iter().map(String::as_str),
        );
        msg.push_str("\n\nAvailable scripts:");
        let mut names: Vec<_> = available_scripts.iter().collect();
        names.sort();
        for script in names {
            msg.push_str(&format!("\n  - {}", script));
        }
        return Err(anyhow::anyhow!(msg));
    }

    // Run interactively with stdin/stdout/stderr passthrough
    // This enables TUI apps, colors, and user interaction
    let status = orchestrator
        .run_script_interactive(name, extra_args)
        .await?;

    if !status.success() {
        let code = status.code().unwrap_or(1);
        return Err(FedError::ScriptFailed {
            name: name.to_string(),
            exit_code: code,
        }
        .into());
    }

    Ok(())
}
