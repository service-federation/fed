use crate::tui::app::App;
use ratatui::{
    Frame,
    layout::{Constraint, Direction, Layout, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, List, ListItem, Paragraph},
};

pub fn draw(f: &mut Frame, app: &App, area: Rect) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3), // Title
            Constraint::Min(10),   // Parameters list
            Constraint::Length(1), // Status bar
        ])
        .split(area);

    draw_title(f, chunks[0]);
    draw_parameters(f, app, chunks[1]);
    draw_status_bar(f, chunks[2]);
}

fn draw_title(f: &mut Frame, area: Rect) {
    let text = vec![Line::from(vec![
        Span::styled(
            "Parameters",
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        ),
        Span::styled(" (resolved values)", Style::default().fg(Color::DarkGray)),
    ])];

    let block = Block::default()
        .borders(Borders::ALL)
        .border_style(Style::default().fg(Color::Blue));

    let paragraph = Paragraph::new(text).block(block);
    f.render_widget(paragraph, area);
}

fn draw_parameters(f: &mut Frame, app: &App, area: Rect) {
    // Render exactly the list the key handler indexes with params_selected —
    // same source, same filter, same order — so navigation and clipboard copy
    // always act on the highlighted row.
    let params = app.get_filtered_params();

    if params.is_empty() {
        let message = if app.params_filter.is_empty() {
            vec![
                Line::from(""),
                Line::from(vec![Span::styled(
                    "  No parameters defined",
                    Style::default().fg(Color::DarkGray),
                )]),
                Line::from(""),
                Line::from(vec![Span::styled(
                    "  Parameters are defined in your fed.yaml",
                    Style::default().fg(Color::DarkGray),
                )]),
                Line::from(vec![Span::styled(
                    "  under the 'parameters' section.",
                    Style::default().fg(Color::DarkGray),
                )]),
            ]
        } else {
            vec![
                Line::from(""),
                Line::from(vec![Span::styled(
                    format!("  No parameters match '{}'", app.params_filter),
                    Style::default().fg(Color::DarkGray),
                )]),
                Line::from(vec![Span::styled(
                    "  Press [c] to clear the filter",
                    Style::default().fg(Color::DarkGray),
                )]),
            ]
        };

        let block = Block::default()
            .title(" Parameter List ")
            .borders(Borders::ALL)
            .border_style(Style::default().fg(Color::Blue));

        let paragraph = Paragraph::new(message).block(block);
        f.render_widget(paragraph, area);
        return;
    }

    let selected = app.params_selected.min(params.len() - 1);

    let items: Vec<ListItem> = params
        .iter()
        .enumerate()
        .map(|(i, view)| {
            // Sensitivity was decided at the resolution boundary (declared
            // `type: secret` + transitive provenance); a sensitive view holds
            // no raw value, only the redacted display string.
            let display_value = view.value.display().to_string();

            let value_color = if view.value.is_sensitive() {
                Color::Red
            } else if display_value.is_empty() {
                Color::DarkGray
            } else {
                Color::Green
            };

            let is_selected = i == selected;
            let marker = if is_selected { "  ▶ " } else { "    " };
            let key_style = if is_selected {
                Style::default()
                    .fg(Color::Cyan)
                    .add_modifier(Modifier::BOLD)
            } else {
                Style::default().fg(Color::Cyan)
            };

            let line = Line::from(vec![
                Span::raw(marker),
                Span::styled(format!("{:<30}", view.name), key_style),
                Span::styled(" = ", Style::default().fg(Color::DarkGray)),
                Span::styled(display_value, Style::default().fg(value_color)),
            ]);

            if is_selected {
                ListItem::new(line).style(Style::default().bg(Color::Rgb(40, 40, 60)))
            } else {
                ListItem::new(line)
            }
        })
        .collect();

    let title = if app.params_filter.is_empty() {
        format!(" Parameter List ({}) ", params.len())
    } else {
        format!(
            " Parameter List ({} matching '{}') ",
            params.len(),
            app.params_filter
        )
    };

    let block = Block::default()
        .title(title)
        .borders(Borders::ALL)
        .border_style(Style::default().fg(Color::Blue));

    let mut state = ratatui::widgets::ListState::default();
    state.select(Some(selected));
    let list = List::new(items).block(block);
    f.render_stateful_widget(list, area, &mut state);
}

fn draw_status_bar(f: &mut Frame, area: Rect) {
    let shortcuts = vec![
        Span::styled("[j/k]", Style::default().fg(Color::Cyan)),
        Span::raw(" navigate "),
        Span::styled("[/]", Style::default().fg(Color::Cyan)),
        Span::raw(" filter "),
        Span::styled("[c]", Style::default().fg(Color::Cyan)),
        Span::raw(" clear "),
        Span::styled("[y]", Style::default().fg(Color::Cyan)),
        Span::raw(" copy value "),
        Span::styled("[Y]", Style::default().fg(Color::Cyan)),
        Span::raw(" copy key=value "),
        Span::styled("[Esc]", Style::default().fg(Color::Cyan)),
        Span::raw(" back "),
        Span::styled("[q]", Style::default().fg(Color::Cyan)),
        Span::raw("uit"),
    ];

    let paragraph =
        Paragraph::new(Line::from(shortcuts)).style(Style::default().bg(Color::DarkGray));

    f.render_widget(paragraph, area);
}
