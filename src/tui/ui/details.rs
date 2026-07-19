use crate::service::Status;
use crate::tui::app::App;
use ratatui::{
    Frame,
    layout::{Constraint, Direction, Layout, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, List, ListItem, Paragraph, Wrap},
};

pub fn draw(f: &mut Frame, app: &App, area: Rect, service_name: &str) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3),  // Title
            Constraint::Min(10),    // Service info
            Constraint::Length(10), // Actions and logs
            Constraint::Length(1),  // Status bar
        ])
        .split(area);

    draw_title(f, service_name, chunks[0]);
    draw_service_info(f, app, service_name, chunks[1]);
    draw_actions_and_logs(f, app, service_name, chunks[2]);
    draw_status_bar(f, chunks[3]);
}

fn draw_title(f: &mut Frame, service_name: &str, area: Rect) {
    let text = vec![Line::from(vec![
        Span::styled("Service Details: ", Style::default().fg(Color::Cyan)),
        Span::styled(
            service_name,
            Style::default()
                .fg(Color::White)
                .add_modifier(Modifier::BOLD),
        ),
    ])];

    let block = Block::default()
        .borders(Borders::ALL)
        .border_style(Style::default().fg(Color::Blue));

    let paragraph = Paragraph::new(text).block(block);
    f.render_widget(paragraph, area);
}

fn draw_service_info(f: &mut Frame, app: &App, service_name: &str, area: Rect) {
    let service = app.services.iter().find(|s| s.name == service_name);

    let lines = if let Some(service) = service {
        let status_color = match service.status {
            Status::Running | Status::Healthy => Color::Green,
            Status::Completed => Color::Green,
            Status::Starting => Color::Yellow,
            Status::Failing => Color::Red,
            Status::Stopped => Color::DarkGray,
            Status::Stopping => Color::Yellow,
        };

        let status_icon = match service.status {
            Status::Running | Status::Healthy => "✓",
            Status::Completed => "✓",
            Status::Starting => "⋯",
            Status::Failing => "✗",
            Status::Stopped => "○",
            Status::Stopping => "⏸",
        };

        let started_at_str = service
            .started_at
            .map(|dt| dt.format("%Y-%m-%d %H:%M:%S UTC").to_string())
            .unwrap_or_else(|| "N/A".to_string());

        let uptime_str = service
            .started_at
            .map(|dt| {
                let now = chrono::Utc::now();
                let duration = now.signed_duration_since(dt);
                format_duration(duration)
            })
            .unwrap_or_else(|| "N/A".to_string());

        vec![
            Line::from(""),
            Line::from(vec![
                Span::styled("  Status:     ", Style::default().fg(Color::DarkGray)),
                Span::styled(
                    format!("{} {:?}", status_icon, service.status),
                    Style::default()
                        .fg(status_color)
                        .add_modifier(Modifier::BOLD),
                ),
            ]),
            Line::from(vec![
                Span::styled("  Type:       ", Style::default().fg(Color::DarkGray)),
                Span::styled(&service.service_type, Style::default().fg(Color::White)),
            ]),
            Line::from(vec![
                Span::styled("  Namespace:  ", Style::default().fg(Color::DarkGray)),
                Span::styled(&service.namespace, Style::default().fg(Color::White)),
            ]),
            Line::from(vec![
                Span::styled("  Port:       ", Style::default().fg(Color::DarkGray)),
                Span::styled(
                    service
                        .port
                        .map(|p| p.to_string())
                        .unwrap_or_else(|| "N/A".to_string()),
                    Style::default().fg(Color::Cyan),
                ),
            ]),
            Line::from(vec![
                Span::styled("  Started At: ", Style::default().fg(Color::DarkGray)),
                Span::styled(started_at_str, Style::default().fg(Color::White)),
            ]),
            Line::from(vec![
                Span::styled("  Uptime:     ", Style::default().fg(Color::DarkGray)),
                Span::styled(uptime_str, Style::default().fg(Color::White)),
            ]),
            Line::from(""),
            Line::from(vec![
                Span::styled("  Health:     ", Style::default().fg(Color::DarkGray)),
                if service.health_error.is_some() {
                    Span::styled("Unhealthy", Style::default().fg(Color::Red))
                } else if matches!(service.status, Status::Healthy) {
                    Span::styled("Healthy", Style::default().fg(Color::Green))
                } else {
                    Span::styled("N/A", Style::default().fg(Color::DarkGray))
                },
            ]),
        ]
    } else {
        vec![
            Line::from(""),
            Line::from(vec![Span::styled(
                "  Service not found",
                Style::default().fg(Color::Red),
            )]),
        ]
    };

    let block = Block::default()
        .title(" Information ")
        .borders(Borders::ALL)
        .border_style(Style::default().fg(Color::Blue));

    let paragraph = Paragraph::new(lines).block(block).wrap(Wrap { trim: true });
    f.render_widget(paragraph, area);
}

fn draw_actions_and_logs(f: &mut Frame, app: &App, service_name: &str, area: Rect) {
    let h_chunks = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Percentage(40), // Actions
            Constraint::Percentage(60), // Recent logs
        ])
        .split(area);

    draw_actions(f, app, service_name, h_chunks[0]);
    draw_recent_logs(f, app, service_name, h_chunks[1]);
}

fn draw_actions(f: &mut Frame, app: &App, service_name: &str, area: Rect) {
    let service = app.services.iter().find(|s| s.name == service_name);

    let items: Vec<ListItem> = if let Some(service) = service {
        let mut actions = vec![];

        match service.status {
            Status::Running | Status::Healthy => {
                actions.push(ListItem::new(Line::from(vec![
                    Span::styled("  [s] ", Style::default().fg(Color::Cyan)),
                    Span::raw("Stop service"),
                ])));
                actions.push(ListItem::new(Line::from(vec![
                    Span::styled("  [r] ", Style::default().fg(Color::Cyan)),
                    Span::raw("Restart service"),
                ])));
            }
            // A completed oneshot has no running process — offer a re-run (start).
            Status::Stopped | Status::Completed => {
                actions.push(ListItem::new(Line::from(vec![
                    Span::styled("  [s] ", Style::default().fg(Color::Cyan)),
                    Span::raw("Start service"),
                ])));
            }
            Status::Starting | Status::Stopping => {
                actions.push(ListItem::new(Line::from(vec![
                    Span::styled("  ", Style::default()),
                    Span::styled("Please wait...", Style::default().fg(Color::Yellow)),
                ])));
            }
            Status::Failing => {
                actions.push(ListItem::new(Line::from(vec![
                    Span::styled("  [s] ", Style::default().fg(Color::Cyan)),
                    Span::raw("Stop service"),
                ])));
                actions.push(ListItem::new(Line::from(vec![
                    Span::styled("  [r] ", Style::default().fg(Color::Cyan)),
                    Span::raw("Restart service"),
                ])));
            }
        }

        actions.push(ListItem::new(Line::from("")));
        actions.push(ListItem::new(Line::from(vec![
            Span::styled("  [l] ", Style::default().fg(Color::Cyan)),
            Span::raw("View full logs"),
        ])));
        actions.push(ListItem::new(Line::from(vec![
            Span::styled("  [Esc] ", Style::default().fg(Color::Cyan)),
            Span::raw("Back to dashboard"),
        ])));

        actions
    } else {
        vec![ListItem::new(Line::from("  No actions available"))]
    };

    let block = Block::default()
        .title(" Actions ")
        .borders(Borders::ALL)
        .border_style(Style::default().fg(Color::Blue));

    let list = List::new(items).block(block);
    f.render_widget(list, area);
}

fn draw_recent_logs(f: &mut Frame, app: &App, service_name: &str, area: Rect) {
    let lines = if let Some(buffer) = app.log_buffers.get(service_name) {
        buffer
            .iter()
            .rev()
            .take(8)
            .rev()
            .map(|log_line| {
                let level_color = match log_line.level {
                    crate::tui::app::LogLevel::Error => Color::Red,
                    crate::tui::app::LogLevel::Warning => Color::Yellow,
                    crate::tui::app::LogLevel::Info => Color::Green,
                    crate::tui::app::LogLevel::Debug => Color::DarkGray,
                };

                Line::from(vec![
                    Span::styled(
                        format!("{} ", log_line.timestamp.format("%H:%M:%S")),
                        Style::default().fg(Color::DarkGray),
                    ),
                    Span::styled(&log_line.message, Style::default().fg(level_color)),
                ])
            })
            .collect()
    } else {
        vec![Line::from("  No logs available")]
    };

    let block = Block::default()
        .title(" Recent Logs ")
        .borders(Borders::ALL)
        .border_style(Style::default().fg(Color::Blue));

    let paragraph = Paragraph::new(lines).block(block).wrap(Wrap { trim: true });
    f.render_widget(paragraph, area);
}

fn draw_status_bar(f: &mut Frame, area: Rect) {
    let shortcuts = vec![
        Span::styled("[s]", Style::default().fg(Color::Cyan)),
        Span::raw("tart/stop "),
        Span::styled("[r]", Style::default().fg(Color::Cyan)),
        Span::raw("estart "),
        Span::styled("[l]", Style::default().fg(Color::Cyan)),
        Span::raw("ogs "),
        Span::styled("[Esc]", Style::default().fg(Color::Cyan)),
        Span::raw(" back "),
        Span::styled("[q]", Style::default().fg(Color::Cyan)),
        Span::raw("uit"),
    ];

    let paragraph =
        Paragraph::new(Line::from(shortcuts)).style(Style::default().bg(Color::DarkGray));

    f.render_widget(paragraph, area);
}

fn format_duration(duration: chrono::Duration) -> String {
    let seconds = duration.num_seconds();
    if seconds < 60 {
        format!("{}s", seconds)
    } else if seconds < 3600 {
        format!("{}m {}s", seconds / 60, seconds % 60)
    } else if seconds < 86400 {
        format!("{}h {}m", seconds / 3600, (seconds % 3600) / 60)
    } else {
        format!("{}d {}h", seconds / 86400, (seconds % 86400) / 3600)
    }
}
