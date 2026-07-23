use crate::tui::app::App;
use ratatui::{
    Frame,
    layout::{Constraint, Direction, Layout, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, List, ListItem, Paragraph},
};
use std::collections::{HashMap, HashSet};

pub fn draw(f: &mut Frame, app: &App, area: Rect) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3), // Title
            Constraint::Min(10),   // Graph visualization
            Constraint::Length(1), // Status bar
        ])
        .split(area);

    draw_title(f, chunks[0]);
    draw_graph(f, app, chunks[1]);
    draw_status_bar(f, chunks[2]);
}

fn draw_title(f: &mut Frame, area: Rect) {
    let text = vec![Line::from(vec![Span::styled(
        "Dependency Graph",
        Style::default()
            .fg(Color::Cyan)
            .add_modifier(Modifier::BOLD),
    )])];

    let block = Block::default()
        .borders(Borders::ALL)
        .border_style(Style::default().fg(Color::Blue));

    let paragraph = Paragraph::new(text).block(block);
    f.render_widget(paragraph, area);
}

fn draw_graph(f: &mut Frame, app: &App, area: Rect) {
    let dep_graph = &app.dep_graph_cache;
    let services = &app.services;

    // Build a map of service name to status for easy lookup
    let status_map: HashMap<String, crate::service::Status> = services
        .iter()
        .map(|s| (s.name.clone(), s.status))
        .collect();

    // Get all nodes sorted alphabetically
    let mut nodes: Vec<_> = dep_graph.nodes().iter().cloned().collect();
    nodes.sort();

    // Split the area into two columns: left for graph, right for legend
    let h_chunks = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Percentage(70), // Graph
            Constraint::Percentage(30), // Legend
        ])
        .split(area);

    // Draw the dependency tree
    draw_dependency_tree(f, &nodes, dep_graph, &status_map, h_chunks[0]);

    // Draw the legend
    draw_legend(f, &nodes, dep_graph, h_chunks[1]);
}

fn draw_dependency_tree(
    f: &mut Frame,
    nodes: &[String],
    dep_graph: &crate::dependency::Graph,
    status_map: &HashMap<String, crate::service::Status>,
    area: Rect,
) {
    let mut lines = Vec::new();

    // Group services by dependency level
    let levels = calculate_dependency_levels(nodes, dep_graph);

    // Get max level for display
    let max_level = levels.values().max().copied().unwrap_or(0);

    // Display by level
    for level in 0..=max_level {
        let level_nodes: Vec<_> = nodes
            .iter()
            .filter(|node| levels.get(*node).copied().unwrap_or(0) == level)
            .collect();

        if !level_nodes.is_empty() {
            // Level header
            lines.push(Line::from(vec![
                Span::styled(
                    format!("Level {} ", level),
                    Style::default()
                        .fg(Color::Yellow)
                        .add_modifier(Modifier::BOLD),
                ),
                Span::styled(
                    format!(
                        "({})",
                        if level == 0 {
                            "no dependencies"
                        } else {
                            "depends on previous"
                        }
                    ),
                    Style::default().fg(Color::DarkGray),
                ),
            ]));

            // Services at this level
            for node in level_nodes {
                let status_icon = if let Some(status) = status_map.get(node) {
                    match status {
                        crate::service::Status::Healthy | crate::service::Status::Completed => "✓",
                        // Up but health not verified — never rendered as ✓
                        crate::service::Status::Running => "●",
                        crate::service::Status::Starting => "⋯",
                        crate::service::Status::Failing => "✗",
                        crate::service::Status::Stopped => "○",
                        crate::service::Status::Stopping => "⏸",
                    }
                } else {
                    "?"
                };

                let status_color = if let Some(status) = status_map.get(node) {
                    match status {
                        crate::service::Status::Healthy | crate::service::Status::Completed => {
                            Color::Green
                        }
                        crate::service::Status::Running => Color::Cyan,
                        crate::service::Status::Starting => Color::Yellow,
                        crate::service::Status::Failing => Color::Red,
                        crate::service::Status::Stopped => Color::DarkGray,
                        crate::service::Status::Stopping => Color::Yellow,
                    }
                } else {
                    Color::DarkGray
                };

                // Get dependencies
                let deps = dep_graph.get_direct_dependencies(node);
                let deps_str = if deps.is_empty() {
                    String::new()
                } else {
                    format!("  → {}", deps.join(", "))
                };

                lines.push(Line::from(vec![
                    Span::raw("  "),
                    Span::styled(
                        format!("{} ", status_icon),
                        Style::default().fg(status_color),
                    ),
                    Span::styled(
                        node,
                        Style::default()
                            .fg(Color::White)
                            .add_modifier(Modifier::BOLD),
                    ),
                    Span::styled(deps_str, Style::default().fg(Color::Cyan)),
                ]));
            }

            lines.push(Line::from(""));
        }
    }

    if lines.is_empty() {
        lines.push(Line::from(""));
        lines.push(Line::from(vec![Span::styled(
            "  No services defined",
            Style::default().fg(Color::DarkGray),
        )]));
    }

    let block = Block::default()
        .title(" Dependency Tree (by level) ")
        .borders(Borders::ALL)
        .border_style(Style::default().fg(Color::Blue));

    let paragraph = Paragraph::new(lines).block(block);
    f.render_widget(paragraph, area);
}

fn draw_legend(f: &mut Frame, nodes: &[String], dep_graph: &crate::dependency::Graph, area: Rect) {
    let mut items = vec![
        ListItem::new(Line::from(vec![Span::styled(
            "Legend",
            Style::default()
                .fg(Color::Yellow)
                .add_modifier(Modifier::BOLD),
        )])),
        ListItem::new(Line::from("")),
    ];

    // Status icons
    items.push(ListItem::new(Line::from(vec![
        Span::styled("✓ ", Style::default().fg(Color::Green)),
        Span::raw("Running/Healthy"),
    ])));
    items.push(ListItem::new(Line::from(vec![
        Span::styled("⋯ ", Style::default().fg(Color::Yellow)),
        Span::raw("Starting"),
    ])));
    items.push(ListItem::new(Line::from(vec![
        Span::styled("✗ ", Style::default().fg(Color::Red)),
        Span::raw("Failing"),
    ])));
    items.push(ListItem::new(Line::from(vec![
        Span::styled("○ ", Style::default().fg(Color::DarkGray)),
        Span::raw("Stopped"),
    ])));
    items.push(ListItem::new(Line::from(vec![
        Span::styled("⏸ ", Style::default().fg(Color::Yellow)),
        Span::raw("Stopping"),
    ])));

    items.push(ListItem::new(Line::from("")));

    // Stats
    items.push(ListItem::new(Line::from(vec![
        Span::styled("Total Services: ", Style::default().fg(Color::DarkGray)),
        Span::styled(nodes.len().to_string(), Style::default().fg(Color::White)),
    ])));

    // Count dependencies
    let total_deps: usize = nodes
        .iter()
        .map(|node| dep_graph.get_direct_dependencies(node).len())
        .sum();

    items.push(ListItem::new(Line::from(vec![
        Span::styled("Dependencies: ", Style::default().fg(Color::DarkGray)),
        Span::styled(total_deps.to_string(), Style::default().fg(Color::White)),
    ])));

    // Check for cycles
    if dep_graph.has_cycle() {
        items.push(ListItem::new(Line::from("")));
        items.push(ListItem::new(Line::from(vec![
            Span::styled("⚠ ", Style::default().fg(Color::Red)),
            Span::styled(
                "Circular dependencies detected!",
                Style::default().fg(Color::Red),
            ),
        ])));
    }

    let block = Block::default()
        .title(" Info ")
        .borders(Borders::ALL)
        .border_style(Style::default().fg(Color::Blue));

    let list = List::new(items).block(block);
    f.render_widget(list, area);
}

fn calculate_dependency_levels(
    nodes: &[String],
    dep_graph: &crate::dependency::Graph,
) -> HashMap<String, usize> {
    let mut levels = HashMap::new();
    let mut visited = HashSet::new();

    // Start with nodes that have no dependencies
    let mut queue: Vec<(String, usize)> = nodes
        .iter()
        .filter(|node| dep_graph.get_direct_dependencies(node).is_empty())
        .map(|node| (node.clone(), 0))
        .collect();

    // BFS to assign levels
    while let Some((node, level)) = queue.pop() {
        if visited.contains(&node) {
            continue;
        }

        visited.insert(node.clone());
        levels.insert(node.clone(), level);

        // Add dependents at next level
        for dependent in dep_graph.get_dependents(&node) {
            if !visited.contains(&dependent) {
                queue.push((dependent, level + 1));
            }
        }
    }

    // Handle any remaining nodes (in case of cycles or disconnected components)
    for node in nodes {
        if !levels.contains_key(node) {
            levels.insert(node.clone(), 0);
        }
    }

    levels
}

fn draw_status_bar(f: &mut Frame, area: Rect) {
    let shortcuts = vec![
        Span::styled("[Esc]", Style::default().fg(Color::Cyan)),
        Span::raw(" back to dashboard "),
        Span::styled("[q]", Style::default().fg(Color::Cyan)),
        Span::raw("uit"),
    ];

    let paragraph =
        Paragraph::new(Line::from(shortcuts)).style(Style::default().bg(Color::DarkGray));

    f.render_widget(paragraph, area);
}
