//! UI rendering for the Amp CC TUI.

use ratatui::{
    Frame,
    layout::{Constraint, Direction, Layout, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, List, ListItem, ListState, Paragraph, Wrap},
};

use crate::app::{App, DataSource, InputMode};

/// Main draw function.
pub fn draw(f: &mut Frame, app: &App) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3), // Header
            Constraint::Min(0),    // Main content
            Constraint::Length(1), // Footer (status bar)
        ])
        .split(f.size());

    draw_header(f, app, chunks[0]);
    draw_main(f, app, chunks[1]);
    draw_footer(f, app, chunks[2]);
}

/// Draw the header with source information.
fn draw_header(f: &mut Frame, app: &App, area: Rect) {
    let source_style = match app.current_source {
        DataSource::Local => Style::default().fg(Color::Green),
        DataSource::Registry => Style::default().fg(Color::Cyan),
    };

    let text = vec![Line::from(vec![
        Span::styled("Amp CC", Style::default().add_modifier(Modifier::BOLD)),
        Span::raw(" | Source: "),
        Span::styled(app.current_source.as_str(), source_style),
        Span::raw(" | "),
        Span::styled(
            truncate_url(app.current_source_url(), 50),
            Style::default().fg(Color::DarkGray),
        ),
    ])];

    let block = Block::default().borders(Borders::ALL).title("Header");
    let paragraph = Paragraph::new(text).block(block);
    f.render_widget(paragraph, area);
}

/// Draw the main content area with sidebar and content.
fn draw_main(f: &mut Frame, app: &App, area: Rect) {
    let chunks = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Percentage(35), // Sidebar (slightly wider for tree)
            Constraint::Percentage(65), // Content
        ])
        .split(area);

    draw_sidebar(f, app, chunks[0]);
    draw_content(f, app, chunks[1]);
}

/// Draw the sidebar with dataset tree.
fn draw_sidebar(f: &mut Frame, app: &App, area: Rect) {
    let mut items: Vec<ListItem> = Vec::new();
    let mut selected_flat_index: Option<usize> = None;
    let mut current_flat_index = 0;

    for dataset in app.filtered_datasets.iter() {
        // Check if this dataset is selected
        if current_flat_index == app.selected_index {
            selected_flat_index = Some(items.len());
        }

        // Dataset line with expand/collapse indicator
        let expand_indicator = if dataset.expanded { "▾ " } else { "▸ " };
        let version_str = dataset
            .latest_version
            .as_ref()
            .map(|v| format!(" @{}", v))
            .unwrap_or_default();

        let dataset_line = Line::from(vec![
            Span::styled(expand_indicator, Style::default().fg(Color::DarkGray)),
            Span::styled(
                format!("{}/", dataset.namespace),
                Style::default().fg(Color::DarkGray),
            ),
            Span::styled(
                dataset.name.clone(),
                Style::default().add_modifier(Modifier::BOLD),
            ),
            Span::styled(version_str, Style::default().fg(Color::Cyan)),
        ]);

        items.push(ListItem::new(dataset_line));
        current_flat_index += 1;

        // If expanded, show versions
        if dataset.expanded {
            if let Some(versions) = &dataset.versions {
                for (version_idx, version) in versions.iter().enumerate() {
                    // Check if this version is selected
                    if current_flat_index == app.selected_index {
                        selected_flat_index = Some(items.len());
                    }

                    let status_style = match version.status.as_str() {
                        "published" | "active" => Style::default().fg(Color::Green),
                        "draft" => Style::default().fg(Color::Yellow),
                        "deprecated" => Style::default().fg(Color::Rgb(255, 165, 0)), // Orange
                        "archived" => Style::default().fg(Color::DarkGray),
                        _ => Style::default().fg(Color::Gray),
                    };

                    let latest_str = if version.is_latest { " (latest)" } else { "" };

                    let is_last = version_idx == versions.len() - 1;
                    let prefix = if is_last {
                        "  └── "
                    } else {
                        "  ├── "
                    };

                    let version_line = Line::from(vec![
                        Span::styled(prefix, Style::default().fg(Color::DarkGray)),
                        Span::raw(&version.version_tag),
                        Span::styled(latest_str, Style::default().fg(Color::Cyan)),
                        Span::raw(" ["),
                        Span::styled(&version.status, status_style),
                        Span::raw("]"),
                    ]);

                    items.push(ListItem::new(version_line));
                    current_flat_index += 1;
                }
            }
        }
    }

    let title = format!("Datasets ({})", app.filtered_datasets.len());
    let list = List::new(items)
        .block(Block::default().title(title).borders(Borders::ALL))
        .highlight_style(
            Style::default()
                .bg(Color::Blue)
                .add_modifier(Modifier::BOLD),
        )
        .highlight_symbol(">> ");

    let mut state = ListState::default();
    state.select(selected_flat_index);

    f.render_stateful_widget(list, area, &mut state);
}

/// Draw the content pane with manifest.
fn draw_content(f: &mut Frame, app: &App, area: Rect) {
    let block = Block::default().title("Manifest").borders(Borders::ALL);

    let content = if app.loading {
        "Loading...".to_string()
    } else if let Some(error) = &app.error_message {
        format!("Error: {}", error)
    } else if let Some(manifest) = &app.current_manifest {
        serde_json::to_string_pretty(manifest).unwrap_or_else(|_| "Invalid JSON".to_string())
    } else {
        "Select a dataset to view manifest...".to_string()
    };

    let text = Paragraph::new(content)
        .block(block)
        .wrap(Wrap { trim: true });
    f.render_widget(text, area);
}

/// Draw the footer with help text and loading indicator (right-aligned).
fn draw_footer(f: &mut Frame, app: &App, area: Rect) {
    // Build the left side (help text)
    let left_text = if matches!(app.input_mode, InputMode::Search) {
        format!(
            "Search: {}_ (Enter to confirm, Esc to cancel)",
            app.search_query
        )
    } else {
        let source_hint = match app.current_source {
            DataSource::Local => "[1]Local [2]Registry",
            DataSource::Registry => "[1]Local [2]Registry",
        };
        format!(
            "{} | '/' search | 'r' refresh | Enter expand | 'j/k' nav | 'q' quit",
            source_hint
        )
    };

    // Build the right side (loading indicator)
    let right_spans: Vec<Span> = if app.loading {
        let mut spans = Vec::new();
        if let Some(msg) = &app.loading_message {
            spans.push(Span::styled(
                format!("{} ", msg),
                Style::default().fg(Color::Yellow),
            ));
        }
        spans.push(Span::styled(
            format!("{}", app.spinner_char()),
            Style::default().fg(Color::Yellow),
        ));
        spans
    } else {
        vec![]
    };

    // Calculate widths for layout
    let right_width = if app.loading {
        let msg_len = app
            .loading_message
            .as_ref()
            .map(|m| m.len() + 1)
            .unwrap_or(0);
        (msg_len + 1) as u16 // message + space + spinner char
    } else {
        0
    };

    // Split area into left (flexible) and right (fixed width for spinner)
    let chunks = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Min(0),                                // Left side (help text)
            Constraint::Length(right_width.saturating_add(1)), // Right side (spinner + padding)
        ])
        .split(area);

    // Render left side
    let left_paragraph = Paragraph::new(left_text).style(Style::default().fg(Color::Gray));
    f.render_widget(left_paragraph, chunks[0]);

    // Render right side (loading indicator)
    if app.loading {
        let right_paragraph = Paragraph::new(Line::from(right_spans));
        f.render_widget(right_paragraph, chunks[1]);
    }
}

/// Truncate a URL to a maximum length.
fn truncate_url(url: &str, max_len: usize) -> String {
    if url.len() <= max_len {
        url.to_string()
    } else {
        format!("{}...", &url[..max_len - 3])
    }
}
