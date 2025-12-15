//! UI rendering for the Amp CC TUI.

use std::sync::LazyLock;

use ratatui::{
    Frame,
    layout::{Alignment, Constraint, Direction, Layout, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{
        Block, Borders, List, ListItem, ListState, Paragraph, Scrollbar, ScrollbarOrientation,
    },
};
use syntect::{easy::HighlightLines, highlighting::ThemeSet, parsing::SyntaxSet};

use crate::app::{ActivePane, App, DataSource, InputMode, InspectResult};

/// Lazily initialized syntax highlighting resources.
static SYNTAX_SET: LazyLock<SyntaxSet> = LazyLock::new(SyntaxSet::load_defaults_newlines);
static THEME_SET: LazyLock<ThemeSet> = LazyLock::new(ThemeSet::load_defaults);

/// ASCII art logo for splash screen (displayed when Header pane is focused).
const AMP_LOGO: &str = r#"
                    ▒█░                     
                   ▒███░                    
                  ▒█████░                   
                 ░▓██░██▓░                  
                ░▓██░ ░██▓░                 
               ░▓██░   ░██▓░                
              ░▓██▒     ░██▓░               
             ░▓██▒       ▒██▓░              
            ░▓██▒         ▒██▓░             
            ▓███▒░       ░▓███▒░            
           ▓██▓▓██▒░   ░▒██▓▓██▒            
          ▓██▒ ░▓███▒ ▒███▓░░▒██▒           
         ▓██▓   ░▓███████▓░  ░▓██▒          
        ▒██▓     ▒▓█████▓░     ▓██▒         
       ░░░░       ▒█████░       ░░░░        
                   ▒███░                    
                    ▒█░                     
                     ░                      

AMP Command & Control
The Graph
"#;

/// Main draw function.
pub fn draw(f: &mut Frame, app: &mut App) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3), // Header
            Constraint::Min(0),    // Main content
            Constraint::Length(1), // Footer (status bar)
        ])
        .split(f.area());

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

    let border_style = if app.active_pane == ActivePane::Header {
        Style::default().fg(Color::Cyan)
    } else {
        Style::default().fg(Color::DarkGray)
    };

    let block = Block::default()
        .borders(Borders::ALL)
        .title("Status")
        .border_style(border_style);
    let paragraph = Paragraph::new(text).block(block);
    f.render_widget(paragraph, area);
}

/// Draw the splash screen with logo (shown when Header is focused).
fn draw_splash(f: &mut Frame, app: &App, area: Rect) {
    let mut lines: Vec<Line> = Vec::new();

    // Calculate vertical padding for centering
    let logo_lines: Vec<&str> = AMP_LOGO.lines().collect();
    let content_height = logo_lines.len() + 8; // logo + spacing + info lines
    let vertical_padding = area.height.saturating_sub(content_height as u16) / 2;

    // Add vertical padding
    for _ in 0..vertical_padding {
        lines.push(Line::from(""));
    }

    // Add logo lines with cyan styling
    for line in logo_lines {
        lines.push(Line::from(Span::styled(
            line.to_string(),
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        )));
    }

    // Add spacing
    lines.push(Line::from(""));
    lines.push(Line::from(""));

    // Add version info
    lines.push(Line::from(vec![
        Span::styled("Version ", Style::default().fg(Color::DarkGray)),
        Span::styled(
            env!("CARGO_PKG_VERSION"),
            Style::default().fg(Color::Yellow),
        ),
    ]));

    // Add source info
    lines.push(Line::from(""));
    let source_style = match app.current_source {
        DataSource::Local => Style::default().fg(Color::Green),
        DataSource::Registry => Style::default().fg(Color::Cyan),
    };
    lines.push(Line::from(vec![
        Span::styled("Connected to: ", Style::default().fg(Color::DarkGray)),
        Span::styled(app.current_source.as_str(), source_style),
    ]));
    lines.push(Line::from(Span::styled(
        app.current_source_url().to_string(),
        Style::default().fg(Color::DarkGray),
    )));

    // Add navigation hint
    lines.push(Line::from(""));
    lines.push(Line::from(Span::styled(
        "Press Tab to navigate to datasets",
        Style::default().fg(Color::DarkGray),
    )));

    let block = Block::default()
        .borders(Borders::ALL)
        .title("Welcome")
        .border_style(Style::default().fg(Color::DarkGray));

    let paragraph = Paragraph::new(lines)
        .block(block)
        .alignment(Alignment::Center);

    f.render_widget(paragraph, area);
}

/// Draw the main content area with sidebar and content.
fn draw_main(f: &mut Frame, app: &mut App, area: Rect) {
    // Show splash screen when Header is focused
    if app.active_pane == ActivePane::Header {
        draw_splash(f, app, area);
        return;
    }

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
    let border_style = if app.active_pane == ActivePane::Sidebar {
        Style::default().fg(Color::Cyan)
    } else {
        Style::default().fg(Color::DarkGray)
    };
    let list = List::new(items)
        .block(
            Block::default()
                .title(title)
                .borders(Borders::ALL)
                .border_style(border_style),
        )
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

/// Highlight JSON with syntax coloring using syntect.
/// Returns a vector of Lines with colored spans.
fn highlight_json(json_str: &str) -> Vec<Line<'static>> {
    let syntax = SYNTAX_SET
        .find_syntax_by_extension("json")
        .unwrap_or_else(|| SYNTAX_SET.find_syntax_plain_text());
    let theme = &THEME_SET.themes["base16-ocean.dark"];
    let mut highlighter = HighlightLines::new(syntax, theme);

    let mut lines = Vec::new();
    for line in json_str.lines() {
        match highlighter.highlight_line(line, &SYNTAX_SET) {
            Ok(ranges) => {
                let spans: Vec<Span<'static>> = ranges
                    .into_iter()
                    .map(|(style, text)| {
                        // Convert syntect style to ratatui style
                        let ratatui_style = syntect_tui::translate_style(style).unwrap_or_default();
                        Span::styled(text.to_string(), ratatui_style)
                    })
                    .collect();
                lines.push(Line::from(spans));
            }
            Err(_) => {
                // Fallback to plain text on error
                lines.push(Line::from(line.to_string()));
            }
        }
    }
    lines
}

/// Draw the content pane with manifest and inspect.
fn draw_content(f: &mut Frame, app: &mut App, area: Rect) {
    // Split content vertically: Manifest (60%) | Inspect (40%)
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Percentage(60), // Manifest
            Constraint::Percentage(40), // Inspect
        ])
        .split(area);

    draw_manifest(f, app, chunks[0]);
    draw_inspect(f, app, chunks[1]);
}

/// Draw the manifest pane.
fn draw_manifest(f: &mut Frame, app: &mut App, area: Rect) {
    let border_style = if app.active_pane == ActivePane::Manifest {
        Style::default().fg(Color::Cyan)
    } else {
        Style::default().fg(Color::DarkGray)
    };
    let block = Block::default()
        .title("Manifest")
        .borders(Borders::ALL)
        .border_style(border_style);

    if app.loading {
        let text = Paragraph::new("Loading...").block(block);
        f.render_widget(text, area);
    } else if let Some(error) = &app.error_message {
        let text = Paragraph::new(format!("Error: {}", error))
            .block(block)
            .style(Style::default().fg(Color::Red));
        f.render_widget(text, area);
    } else if let Some(manifest) = &app.current_manifest {
        let json_str =
            serde_json::to_string_pretty(manifest).unwrap_or_else(|_| "Invalid JSON".to_string());
        let highlighted = highlight_json(&json_str);
        let line_count = highlighted.len();

        // Update content length for scroll bounds
        app.manifest_content_length = line_count;

        // Update scroll state with content length
        app.manifest_scroll_state = app.manifest_scroll_state.content_length(line_count);

        let text = Paragraph::new(highlighted)
            .block(block)
            .scroll((app.manifest_scroll, 0));
        f.render_widget(text, area);

        // Render scrollbar
        let scrollbar = Scrollbar::new(ScrollbarOrientation::VerticalRight);
        f.render_stateful_widget(scrollbar, area, &mut app.manifest_scroll_state);
    } else {
        let text = Paragraph::new("Select a dataset to view manifest...")
            .block(block)
            .style(Style::default().fg(Color::DarkGray));
        f.render_widget(text, area);
    }
}

/// Draw the inspect pane with schema information.
fn draw_inspect(f: &mut Frame, app: &mut App, area: Rect) {
    let border_style = if app.active_pane == ActivePane::Schema {
        Style::default().fg(Color::Cyan)
    } else {
        Style::default().fg(Color::DarkGray)
    };
    let block = Block::default()
        .title("Schema")
        .borders(Borders::ALL)
        .border_style(border_style);

    if app.loading {
        let content = Paragraph::new("Loading...").block(block);
        f.render_widget(content, area);
    } else if let Some(inspect) = &app.current_inspect {
        let lines = format_inspect_result(inspect);
        let line_count = lines.len();

        // Update content length for scroll bounds
        app.schema_content_length = line_count;

        // Update scroll state with content length
        app.schema_scroll_state = app.schema_scroll_state.content_length(line_count);

        let content = Paragraph::new(lines)
            .block(block)
            .scroll((app.schema_scroll, 0));
        f.render_widget(content, area);

        // Render scrollbar
        let scrollbar = Scrollbar::new(ScrollbarOrientation::VerticalRight);
        f.render_stateful_widget(scrollbar, area, &mut app.schema_scroll_state);
    } else if app.current_manifest.is_some() {
        let content = Paragraph::new("No schema information available")
            .block(block)
            .style(Style::default().fg(Color::DarkGray));
        f.render_widget(content, area);
    } else {
        let content = Paragraph::new("Select a dataset to view schema...")
            .block(block)
            .style(Style::default().fg(Color::DarkGray));
        f.render_widget(content, area);
    }
}

/// Format inspect result into styled lines.
fn format_inspect_result(inspect: &InspectResult) -> Vec<Line<'static>> {
    let mut lines = Vec::new();

    for table in &inspect.tables {
        // Table header
        lines.push(Line::from(vec![
            Span::styled(
                table.name.clone(),
                Style::default()
                    .fg(Color::Cyan)
                    .add_modifier(Modifier::BOLD),
            ),
            Span::styled(
                format!(" ({} columns)", table.columns.len()),
                Style::default().fg(Color::DarkGray),
            ),
        ]));

        // Columns
        for col in &table.columns {
            let nullable_str = if col.nullable { "" } else { " NOT NULL" };
            lines.push(Line::from(vec![
                Span::raw("  "),
                Span::styled(
                    format!("{:<24}", col.name),
                    Style::default().fg(Color::White),
                ),
                Span::styled(
                    format!("{:<20}", col.arrow_type),
                    Style::default().fg(Color::Yellow),
                ),
                Span::styled(nullable_str.to_string(), Style::default().fg(Color::Red)),
            ]));
        }

        // Empty line between tables
        lines.push(Line::from(""));
    }

    lines
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

    // Build the right side (loading indicator or error message)
    let (right_spans, right_width): (Vec<Span>, u16) = if app.loading {
        // Show loading spinner
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
        let msg_len = app
            .loading_message
            .as_ref()
            .map(|m| m.len() + 1)
            .unwrap_or(0);
        (spans, (msg_len + 2) as u16)
    } else if let Some(error) = &app.error_message {
        // Show error message in red
        let display_error = if error.len() > 50 {
            format!("{}...", &error[..47])
        } else {
            error.clone()
        };
        let spans = vec![Span::styled(
            format!("⚠ {}", display_error),
            Style::default().fg(Color::Red),
        )];
        let width = (display_error.len() + 3) as u16; // icon + space + message
        (spans, width)
    } else {
        (vec![], 0)
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

    // Render right side (loading indicator or error)
    if !right_spans.is_empty() {
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
