use fed::{Orchestrator, config::Config};

pub async fn run_tui(
    orchestrator: Orchestrator,
    watch: bool,
    config: Option<&Config>,
) -> anyhow::Result<()> {
    if watch {
        fed::tui::run_with_watch(orchestrator, true, config).await?;
    } else {
        fed::tui::run(orchestrator).await?;
    }

    Ok(())
}
