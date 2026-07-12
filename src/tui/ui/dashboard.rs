use crate::service::Status;
use crate::tui::app::{App, LogLevel, StatusLevel};
use ratatui::{
    layout::{Constraint, Direction, Layout, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, List, ListItem, Paragraph},
    Frame,
};

pub fn draw(f: &mut Frame, app: &App, area: Rect) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3), // Header
            Constraint::Min(10),   // Services list
            Constraint::Length(8), // Log panel
            Constraint::Length(1), // Status bar
        ])
        .split(area);

    draw_header(f, app, chunks[0]);
    draw_services_list(f, app, chunks[1]);
    draw_log_panel(f, app, chunks[2]);
    draw_status_bar(f, app, chunks[3]);
}

fn draw_header(f: &mut Frame, app: &App, area: Rect) {
    let running_count = app
        .services
        .iter()
        .filter(|s| matches!(s.status, Status::Running | Status::Healthy))
        .count();
    let total = app.services.len();

    let text = vec![
        Line::from(vec![
            Span::styled("Service Federation ", Style::default().fg(Color::Cyan)),
            Span::styled("v0.2.0", Style::default().fg(Color::DarkGray)),
        ]),
        Line::from(vec![
            Span::styled("🚀 Running: ", Style::default().fg(Color::Green)),
            Span::raw(format!("{}/{} ", running_count, total)),
            Span::styled("| Press ? for help", Style::default().fg(Color::DarkGray)),
        ]),
    ];

    let block = Block::default()
        .borders(Borders::ALL)
        .border_style(Style::default().fg(Color::Blue));

    let paragraph = Paragraph::new(text).block(block);
    f.render_widget(paragraph, area);
}

fn draw_services_list(f: &mut Frame, app: &App, area: Rect) {
    let items: Vec<ListItem> = app
        .services
        .iter()
        .enumerate()
        .map(|(idx, service)| {
            let status_icon = match service.status {
                Status::Running | Status::Healthy => "✓",
                Status::Completed => "✓",
                Status::Starting => "⋯",
                Status::Failing => "✗",
                Status::Stopped => "○",
                Status::Stopping => "⏸",
            };

            let status_color = match service.status {
                Status::Running | Status::Healthy => Color::Green,
                Status::Completed => Color::Green,
                Status::Starting => Color::Yellow,
                Status::Failing => Color::Red,
                Status::Stopped => Color::DarkGray,
                Status::Stopping => Color::Yellow,
            };

            let line = Line::from(vec![
                Span::styled(
                    format!(" {} ", status_icon),
                    Style::default().fg(status_color),
                ),
                Span::styled(
                    format!("{:<30}", service.name),
                    Style::default().fg(Color::White),
                ),
                Span::styled(
                    format!("{:<12}", format!("{:?}", service.status)),
                    Style::default().fg(status_color),
                ),
                Span::styled(
                    format!("{:<15}", service.service_type),
                    Style::default().fg(Color::DarkGray),
                ),
                Span::styled(
                    service.port.map(|p| format!(":{}", p)).unwrap_or_default(),
                    Style::default().fg(Color::Cyan),
                ),
            ]);

            let mut style = Style::default();
            if Some(idx) == app.selected_service {
                style = style.bg(Color::DarkGray).add_modifier(Modifier::BOLD);
            }

            ListItem::new(line).style(style)
        })
        .collect();

    let block = Block::default()
        .title(" Services (↑↓ navigate, Enter details) ")
        .borders(Borders::ALL)
        .border_style(Style::default().fg(Color::Blue));

    let list = List::new(items).block(block);
    f.render_widget(list, area);
}

fn draw_log_panel(f: &mut Frame, app: &App, area: Rect) {
    let mut lines = Vec::new();

    // Collect last 6 log lines across all services
    let mut all_logs: Vec<_> = app
        .log_buffers
        .values()
        .flat_map(|buffer| buffer.iter())
        .collect();

    // Sort by timestamp
    all_logs.sort_by_key(|log| log.timestamp);

    // Take last 6
    for log_line in all_logs.iter().rev().take(6).rev() {
        let level_style = match log_line.level {
            LogLevel::Error => Style::default().fg(Color::Red),
            LogLevel::Warning => Style::default().fg(Color::Yellow),
            LogLevel::Info => Style::default().fg(Color::Green),
            LogLevel::Debug => Style::default().fg(Color::DarkGray),
        };

        lines.push(Line::from(vec![
            Span::styled(
                format!("{} ", log_line.timestamp.format("%H:%M:%S")),
                Style::default().fg(Color::DarkGray),
            ),
            Span::styled(
                format!("[{}] ", log_line.service),
                Style::default().fg(Color::Cyan),
            ),
            Span::styled(&log_line.message, level_style),
        ]));
    }

    // Check follow status for selected service
    let is_following = app
        .selected_service
        .and_then(|idx| app.services.get(idx))
        .map(|s| app.is_following(&s.name))
        .unwrap_or(true);

    let block = Block::default()
        .title(format!(
            " Live Logs (l to expand, f to {}) ",
            if is_following { "pause" } else { "follow" }
        ))
        .borders(Borders::ALL)
        .border_style(Style::default().fg(Color::Blue));

    let paragraph = Paragraph::new(lines).block(block);
    f.render_widget(paragraph, area);
}

fn draw_status_bar(f: &mut Frame, app: &App, area: Rect) {
    // Show status message if present
    if let Some(ref msg) = app.status_message {
        let color = match msg.level {
            StatusLevel::Info => Color::Blue,
            StatusLevel::Success => Color::Green,
            StatusLevel::Warning => Color::Yellow,
            StatusLevel::Error => Color::Red,
        };

        let paragraph = Paragraph::new(msg.text.as_str()).style(
            Style::default()
                .fg(color)
                .add_modifier(Modifier::BOLD)
                .bg(Color::DarkGray),
        );

        f.render_widget(paragraph, area);
        return;
    }

    // Show shortcuts if no status message
    let shortcuts = vec![
        Span::styled("[r]", Style::default().fg(Color::Cyan)),
        Span::raw("estart "),
        Span::styled("[s]", Style::default().fg(Color::Cyan)),
        Span::raw("top "),
        Span::styled("[d]", Style::default().fg(Color::Cyan)),
        Span::raw("etails "),
        Span::styled("[l]", Style::default().fg(Color::Cyan)),
        Span::raw("ogs "),
        Span::styled("[g]", Style::default().fg(Color::Cyan)),
        Span::raw("raph "),
        Span::styled("[p]", Style::default().fg(Color::Cyan)),
        Span::raw("arams "),
        Span::styled("[q]", Style::default().fg(Color::Cyan)),
        Span::raw("uit"),
    ];

    let paragraph =
        Paragraph::new(Line::from(shortcuts)).style(Style::default().bg(Color::DarkGray));

    f.render_widget(paragraph, area);
}
