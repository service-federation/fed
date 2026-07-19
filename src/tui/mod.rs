use crossterm::{
    event::{DisableMouseCapture, EnableMouseCapture},
    execute,
    terminal::{EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode},
};
use ratatui::{Terminal, backend::CrosstermBackend};
use std::io;

pub mod app;
pub mod events;
pub mod ui;
pub mod widgets;

use app::App;
use events::EventHandler;

/// Run the TUI application
pub async fn run(orchestrator: crate::Orchestrator) -> anyhow::Result<()> {
    run_with_watch(orchestrator, false, None).await
}

/// Run the TUI application with optional watch mode
pub async fn run_with_watch(
    orchestrator: crate::Orchestrator,
    watch_enabled: bool,
    config: Option<&crate::config::Config>,
) -> anyhow::Result<()> {
    run_internal(orchestrator, watch_enabled, config).await
}

async fn run_internal(
    mut orchestrator: crate::Orchestrator,
    watch_enabled: bool,
    config: Option<&crate::config::Config>,
) -> anyhow::Result<()> {
    // Install panic hook to restore terminal on panic
    let original_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |panic_info| {
        // Best-effort terminal restoration - restore BEFORE calling original hook
        // to ensure terminal is usable for error display
        let _ = disable_raw_mode();
        let _ = execute!(io::stdout(), LeaveAlternateScreen, DisableMouseCapture);
        let _ = execute!(io::stdout(), crossterm::cursor::Show);

        // Call the original panic hook
        original_hook(panic_info);
    }));

    // Enable auto-resolve for port conflicts to avoid interactive prompts bleeding through TUI
    orchestrator.set_auto_resolve_conflicts(true);

    // Mark startup complete so health monitoring works in TUI mode
    orchestrator.mark_startup_complete();

    // Setup terminal
    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen, EnableMouseCapture)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    // Get work_dir before wrapping orchestrator in App (avoids async lock issues)
    let work_dir = orchestrator.work_dir().to_path_buf();

    // Create app state
    let mut app = App::new(orchestrator);

    // Set up watch mode if enabled
    let watch_mode = if watch_enabled {
        if let Some(cfg) = config {
            match crate::watch::WatchMode::new(cfg, &work_dir) {
                Ok(wm) => {
                    app.set_watch_mode_enabled(true);
                    Some(wm)
                }
                Err(e) => {
                    tracing::warn!("Failed to start watch mode: {}", e);
                    None
                }
            }
        } else {
            None
        }
    } else {
        None
    };

    // Create event handler
    let events = EventHandler::new(250); // 250ms tick rate

    // Main loop
    let result = run_app(&mut terminal, &mut app, events, watch_mode).await;

    // Restore terminal - always cleanup even on error
    let cleanup_result = restore_terminal(&mut terminal);

    // Return the app result, but if cleanup failed and app succeeded, return cleanup error
    match (result, cleanup_result) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(e), _) => Err(e),
        (Ok(()), Err(e)) => Err(e.into()),
    }
}

/// Restore terminal to normal mode
fn restore_terminal<B: ratatui::backend::Backend + std::io::Write>(
    terminal: &mut Terminal<B>,
) -> io::Result<()> {
    disable_raw_mode()?;
    execute!(
        terminal.backend_mut(),
        LeaveAlternateScreen,
        DisableMouseCapture,
        crossterm::cursor::Show
    )?;
    Ok(())
}

async fn run_app<B: ratatui::backend::Backend>(
    terminal: &mut Terminal<B>,
    app: &mut App,
    mut events: EventHandler,
    mut watch_mode: Option<crate::watch::WatchMode>,
) -> anyhow::Result<()> {
    loop {
        // Draw UI
        terminal.draw(|f| ui::draw(f, app))?;

        // Handle events (with optional file watch events)
        tokio::select! {
            event = events.next() => {
                if let Some(event) = event {
                    match event {
                        events::Event::Tick => app.on_tick().await?,
                        events::Event::Key(key) => {
                            if !app.handle_key(key).await? {
                                break; // User quit
                            }
                        }
                        events::Event::Mouse(mouse) => {
                            app.handle_mouse(mouse);
                        }
                        events::Event::Resize(width, height) => {
                            app.on_resize(width, height);
                        }
                        events::Event::Shutdown => {
                            break; // Ctrl-C pressed
                        }
                    }
                }
            }
            file_event = async {
                if let Some(ref mut wm) = watch_mode {
                    wm.next_event().await
                } else {
                    std::future::pending::<Option<crate::watch::FileChangeEvent>>().await
                }
            } => {
                if let Some(event) = file_event {
                    // Handle file change - restart the affected service
                    // Don't propagate errors - just show status message
                    if let Err(e) = app.handle_file_change(event).await {
                        tracing::error!("Failed to handle file change: {}", e);
                    }
                }
            }
        }
    }

    // Clean up event handler task
    events.shutdown();

    Ok(())
}
