use crate::tui::app::{App, LogLevel};
use ratatui::{
    Frame,
    layout::{Constraint, Direction, Layout, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Paragraph, Wrap},
};

pub fn draw(f: &mut Frame, app: &App, area: Rect, service_name: &str) {
    // Add search bar if in search mode
    let constraints = if app.search_mode {
        vec![
            Constraint::Length(3), // Title
            Constraint::Min(10),   // Logs
            Constraint::Length(3), // Search bar
            Constraint::Length(1), // Status bar
        ]
    } else {
        vec![
            Constraint::Length(3), // Title
            Constraint::Min(10),   // Logs
            Constraint::Length(1), // Status bar
        ]
    };

    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints(constraints)
        .split(area);

    draw_title(f, app, service_name, chunks[0]);
    draw_logs(f, app, service_name, chunks[1]);

    if app.search_mode {
        draw_search_bar(f, app, chunks[2]);
        draw_status_bar(f, app, service_name, chunks[3]);
    } else {
        draw_status_bar(f, app, service_name, chunks[2]);
    }
}

fn draw_title(f: &mut Frame, app: &App, service_name: &str, area: Rect) {
    let is_following = app.is_following(service_name);
    let follow_status = if is_following {
        Span::styled(
            " [FOLLOWING]",
            Style::default()
                .fg(Color::Green)
                .add_modifier(Modifier::BOLD),
        )
    } else {
        Span::styled(
            " [PAUSED]",
            Style::default()
                .fg(Color::Yellow)
                .add_modifier(Modifier::BOLD),
        )
    };

    let filter_status = match app.get_log_filter(service_name) {
        None => Span::styled(" [All]", Style::default().fg(Color::DarkGray)),
        Some(level) => Span::styled(
            format!(" [{}]", level.filter_name()),
            Style::default()
                .fg(Color::Magenta)
                .add_modifier(Modifier::BOLD),
        ),
    };

    let search_status = match app.get_search(service_name) {
        None => Span::raw(""),
        Some(query) => Span::styled(
            format!(" 🔍\"{}\"", query),
            Style::default().fg(Color::Yellow),
        ),
    };

    let text = vec![Line::from(vec![
        Span::styled("Logs: ", Style::default().fg(Color::Cyan)),
        Span::styled(
            service_name,
            Style::default()
                .fg(Color::White)
                .add_modifier(Modifier::BOLD),
        ),
        follow_status,
        filter_status,
        search_status,
    ])];

    let block = Block::default()
        .borders(Borders::ALL)
        .border_style(Style::default().fg(Color::Blue));

    let paragraph = Paragraph::new(text).block(block);
    f.render_widget(paragraph, area);
}

fn draw_logs(f: &mut Frame, app: &App, service_name: &str, area: Rect) {
    let mut lines = Vec::new();
    let is_following = app.is_following(service_name);
    let scroll_pos = app.get_scroll(service_name);
    let level_filter = app.get_log_filter(service_name);
    let search_query = app.get_search(service_name).map(|s| s.to_lowercase());

    if let Some(buffer) = app.log_buffers.get(service_name) {
        // Filter logs by level and search query
        let filtered_logs: Vec<_> = buffer
            .iter()
            .filter(|log| log.level.passes_filter(level_filter))
            .filter(|log| {
                search_query
                    .as_ref()
                    .map(|q| log.message.to_lowercase().contains(q))
                    .unwrap_or(true)
            })
            .collect();

        let total_logs = filtered_logs.len();

        // Calculate visible range
        let visible_lines = (area.height.saturating_sub(2)) as usize; // Subtract 2 for borders

        let (start_idx, end_idx) = if is_following && total_logs > 0 {
            // In follow mode, show the last N lines
            let start = total_logs.saturating_sub(visible_lines);
            (start, total_logs)
        } else {
            // In scroll mode, respect scroll position
            let start = scroll_pos.min(total_logs.saturating_sub(1));
            let end = (start + visible_lines).min(total_logs);
            (start, end)
        };

        // Render logs
        for log_line in filtered_logs
            .iter()
            .skip(start_idx)
            .take(end_idx - start_idx)
        {
            let level_icon = match log_line.level {
                LogLevel::Error => "✗",
                LogLevel::Warning => "⚠",
                LogLevel::Info => "ℹ",
                LogLevel::Debug => "·",
            };

            let level_color = match log_line.level {
                LogLevel::Error => Color::Red,
                LogLevel::Warning => Color::Yellow,
                LogLevel::Info => Color::Green,
                LogLevel::Debug => Color::DarkGray,
            };

            // For now, just render the message as-is (wrapping handled by Paragraph widget)
            // TODO: Could implement custom wrapping for better control
            lines.push(Line::from(vec![
                Span::styled(
                    format!("{} ", log_line.timestamp.format("%H:%M:%S")),
                    Style::default().fg(Color::DarkGray),
                ),
                Span::styled(
                    format!("{} ", level_icon),
                    Style::default()
                        .fg(level_color)
                        .add_modifier(Modifier::BOLD),
                ),
                Span::styled(&log_line.message, Style::default().fg(level_color)),
            ]));
        }

        // Add scroll indicator if not at bottom
        if !is_following && total_logs > visible_lines && end_idx < total_logs {
            let remaining = total_logs - end_idx;
            lines.push(Line::from(""));
            lines.push(Line::from(vec![Span::styled(
                format!(
                    "  ↓ {} more lines (↓/j to scroll down, f to follow)",
                    remaining
                ),
                Style::default()
                    .fg(Color::Yellow)
                    .add_modifier(Modifier::ITALIC),
            )]));
        }
    } else {
        lines.push(Line::from(""));
        lines.push(Line::from(vec![Span::styled(
            "  No logs available for this service",
            Style::default().fg(Color::DarkGray),
        )]));
        lines.push(Line::from(""));
        lines.push(Line::from(vec![Span::styled(
            "  Logs will appear here as the service generates output.",
            Style::default().fg(Color::DarkGray),
        )]));
    }

    let block = Block::default()
        .title(format!(
            " Logs (showing {}) ",
            if is_following { "live" } else { "scrolled" }
        ))
        .borders(Borders::ALL)
        .border_style(Style::default().fg(Color::Blue));

    let paragraph = Paragraph::new(lines)
        .block(block)
        .wrap(Wrap { trim: false });

    f.render_widget(paragraph, area);
}

fn draw_search_bar(f: &mut Frame, app: &App, area: Rect) {
    let text = vec![Line::from(vec![
        Span::styled("Search: ", Style::default().fg(Color::Cyan)),
        Span::styled(
            &app.search_input,
            Style::default()
                .fg(Color::White)
                .add_modifier(Modifier::BOLD),
        ),
        Span::styled("█", Style::default().fg(Color::White)), // Cursor
    ])];

    let block = Block::default()
        .title(" Search (Enter to apply, Esc to cancel) ")
        .borders(Borders::ALL)
        .border_style(Style::default().fg(Color::Yellow));

    let paragraph = Paragraph::new(text).block(block);
    f.render_widget(paragraph, area);
}

fn draw_status_bar(f: &mut Frame, app: &App, service_name: &str, area: Rect) {
    let follow_key = if app.is_following(service_name) {
        "[f] pause"
    } else {
        "[f] follow"
    };

    let shortcuts = vec![
        Span::styled("[↑↓]", Style::default().fg(Color::Cyan)),
        Span::raw(" scroll "),
        Span::styled("[1-5]", Style::default().fg(Color::Cyan)),
        Span::raw(" filter "),
        Span::styled("[/]", Style::default().fg(Color::Cyan)),
        Span::raw(" search "),
        Span::styled("[e/E]", Style::default().fg(Color::Cyan)),
        Span::raw(" next/prev error "),
        Span::styled(follow_key, Style::default().fg(Color::Cyan)),
        Span::raw(" "),
        Span::styled("[Esc]", Style::default().fg(Color::Cyan)),
        Span::raw(" back"),
    ];

    let paragraph =
        Paragraph::new(Line::from(shortcuts)).style(Style::default().bg(Color::DarkGray));

    f.render_widget(paragraph, area);
}
