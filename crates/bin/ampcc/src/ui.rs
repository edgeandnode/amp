//! UI rendering for the Amp CC TUI.
//!
//! This module uses The Graph's official color palette for consistent branding.

use admin_client::{jobs::JobInfo, workers::WorkerDetailResponse};
use ratatui::{
    Frame,
    layout::{Alignment, Constraint, Direction, Layout, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{
        Block, Borders, Cell, List, ListItem, ListState, Paragraph, Row, Scrollbar,
        ScrollbarOrientation, Table, Wrap,
    },
};

use crate::app::{
    ActivePane, App, ContentView, DataSource, InputMode, InspectResult, QueryResults,
};

// ============================================================================
// The Graph Color Palette
// ============================================================================
//
// Official brand colors from The Graph's design system.
//
// ## Primary Colors
// - THE_GRAPH_PURPLE (#6F4CFF) - Main brand accent
// - GALAXY_DARK (#0C0A1D) - Deep background
// - LUNAR_GRAY (#494755) - Borders, secondary elements
// - SPACESUIT_WHITE (#F8F6FF) - Primary text
//
// ## Secondary Colors
// - ASTRO_BLUE (#4C66FF) - Selection highlights
// - GALACTIC_AQUA (#66D8FF) - Active/focused elements
// - STARFIELD_GREEN (#4BCA81) - Success states
// - NEBULA_PINK (#FF79C6) - Errors
// - SOLAR_YELLOW (#FFA801) - Warnings, drafts
// ============================================================================

/// The Graph brand colors and semantic UI theme.
///
/// This theme implements The Graph's official color palette, providing
/// consistent visual styling across the TUI application.
pub struct Theme;

impl Theme {
    // ========================================================================
    // Primary Colors
    // ========================================================================

    /// The Graph Purple - Main brand accent color.
    /// Hex: #6F4CFF
    pub const THE_GRAPH_PURPLE: Color = Color::Rgb(0x6F, 0x4C, 0xFF);

    /// Galaxy Dark - Deep space background.
    /// Hex: #0C0A1D
    ///
    /// Note: Not actively used as terminals provide their own background color.
    /// Kept for palette documentation completeness.
    #[allow(dead_code)]
    pub const GALAXY_DARK: Color = Color::Rgb(0x0C, 0x0A, 0x1D);

    /// Lunar Gray - Borders and dividers.
    /// Hex: #494755
    pub const LUNAR_GRAY: Color = Color::Rgb(0x49, 0x47, 0x55);

    /// Lunar Gray Light - Secondary text (lighter variant for readability).
    /// Hex: #8A8894
    pub const LUNAR_GRAY_LIGHT: Color = Color::Rgb(0x8A, 0x88, 0x94);

    /// Spacesuit White - Primary text color.
    /// Hex: #F8F6FF
    pub const SPACESUIT_WHITE: Color = Color::Rgb(0xF8, 0xF6, 0xFF);

    // ========================================================================
    // Secondary Colors
    // ========================================================================

    /// Astro Blue - Selection and highlight backgrounds.
    /// Hex: #4C66FF
    pub const ASTRO_BLUE: Color = Color::Rgb(0x4C, 0x66, 0xFF);

    /// Galactic Aqua - Active and focused elements.
    /// Hex: #66D8FF
    pub const GALACTIC_AQUA: Color = Color::Rgb(0x66, 0xD8, 0xFF);

    /// Starfield Green - Success and active states.
    /// Hex: #4BCA81
    pub const STARFIELD_GREEN: Color = Color::Rgb(0x4B, 0xCA, 0x81);

    /// Nebula Pink - Error states.
    /// Hex: #FF79C6
    pub const NEBULA_PINK: Color = Color::Rgb(0xFF, 0x79, 0xC6);

    /// Solar Yellow - Warnings, drafts, and loading states.
    /// Hex: #FFA801
    pub const SOLAR_YELLOW: Color = Color::Rgb(0xFF, 0xA8, 0x01);

    // ========================================================================
    // Semantic Styles
    // ========================================================================

    /// Style for focused/active pane borders.
    pub fn border_focused() -> Style {
        Style::default().fg(Self::GALACTIC_AQUA)
    }

    /// Style for unfocused pane borders.
    pub fn border_unfocused() -> Style {
        Style::default().fg(Self::LUNAR_GRAY)
    }

    /// Style for primary text.
    pub fn text_primary() -> Style {
        Style::default().fg(Self::SPACESUIT_WHITE)
    }

    /// Style for secondary/dimmed text.
    pub fn text_secondary() -> Style {
        Style::default().fg(Self::LUNAR_GRAY_LIGHT)
    }

    /// Style for brand accent (headers, titles).
    pub fn accent() -> Style {
        Style::default().fg(Self::THE_GRAPH_PURPLE)
    }

    /// Style for success states (published, active).
    pub fn status_success() -> Style {
        Style::default().fg(Self::STARFIELD_GREEN)
    }

    /// Style for warning states (draft, loading).
    pub fn status_warning() -> Style {
        Style::default().fg(Self::SOLAR_YELLOW)
    }

    /// Style for error states.
    pub fn status_error() -> Style {
        Style::default().fg(Self::NEBULA_PINK)
    }

    /// Style for archived/deprecated states.
    pub fn status_archived() -> Style {
        Style::default().fg(Self::LUNAR_GRAY)
    }

    /// Style for selected/highlighted items.
    pub fn selection() -> Style {
        Style::default()
            .bg(Self::ASTRO_BLUE)
            .add_modifier(Modifier::BOLD)
    }

    /// Style for local data source indicator.
    pub fn source_local() -> Style {
        Style::default().fg(Self::STARFIELD_GREEN)
    }

    /// Style for registry data source indicator.
    pub fn source_registry() -> Style {
        Style::default().fg(Self::GALACTIC_AQUA)
    }

    /// Style for version tags and labels.
    pub fn version_tag() -> Style {
        Style::default().fg(Self::GALACTIC_AQUA)
    }

    /// Style for type annotations in schema.
    pub fn type_annotation() -> Style {
        Style::default().fg(Self::SOLAR_YELLOW)
    }

    /// Style for constraint indicators (NOT NULL, etc.).
    pub fn constraint() -> Style {
        Style::default().fg(Self::NEBULA_PINK)
    }
}

// ============================================================================
// SQL Syntax Highlighting
// ============================================================================

/// SQL keywords for syntax highlighting.
const SQL_KEYWORDS: &[&str] = &[
    "SELECT",
    "FROM",
    "WHERE",
    "AND",
    "OR",
    "NOT",
    "IN",
    "LIKE",
    "JOIN",
    "LEFT",
    "RIGHT",
    "INNER",
    "OUTER",
    "ON",
    "ORDER",
    "BY",
    "ASC",
    "DESC",
    "GROUP",
    "HAVING",
    "LIMIT",
    "OFFSET",
    "INSERT",
    "INTO",
    "VALUES",
    "UPDATE",
    "SET",
    "DELETE",
    "CREATE",
    "TABLE",
    "INDEX",
    "DROP",
    "ALTER",
    "AS",
    "DISTINCT",
    "ALL",
    "UNION",
    "INTERSECT",
    "EXCEPT",
    "CASE",
    "WHEN",
    "THEN",
    "ELSE",
    "END",
    "NULL",
    "TRUE",
    "FALSE",
    "COUNT",
    "SUM",
    "AVG",
    "MIN",
    "MAX",
    "DESCRIBE",
    "IS",
    "BETWEEN",
    "EXISTS",
    "CROSS",
    "FULL",
    "NATURAL",
    "USING",
    "WITH",
    "RECURSIVE",
    "OVER",
    "PARTITION",
    "WINDOW",
    "ROWS",
    "RANGE",
    "UNBOUNDED",
    "PRECEDING",
    "FOLLOWING",
    "CURRENT",
    "ROW",
];

/// SQL token types for syntax highlighting.
#[derive(Debug, Clone)]
enum SqlToken {
    Keyword(String),
    String(String),
    Number(String),
    Operator(String),
    Identifier(String),
    Whitespace(String),
    Punctuation(String),
}

/// Tokenize SQL input for syntax highlighting.
fn tokenize_sql(input: &str) -> Vec<SqlToken> {
    let mut tokens = Vec::new();
    let mut chars = input.chars().peekable();

    while let Some(&ch) = chars.peek() {
        match ch {
            // Whitespace
            ' ' | '\t' | '\n' | '\r' => {
                let mut ws = String::new();
                while let Some(&c) = chars.peek() {
                    if c.is_whitespace() {
                        ws.push(chars.next().unwrap());
                    } else {
                        break;
                    }
                }
                tokens.push(SqlToken::Whitespace(ws));
            }
            // String literal (single quote)
            '\'' => {
                let quote = chars.next().unwrap();
                let mut s = String::from(quote);
                while let Some(&c) = chars.peek() {
                    s.push(chars.next().unwrap());
                    if c == '\'' {
                        break;
                    }
                }
                tokens.push(SqlToken::String(s));
            }
            // String literal (double quote)
            '"' => {
                let quote = chars.next().unwrap();
                let mut s = String::from(quote);
                while let Some(&c) = chars.peek() {
                    s.push(chars.next().unwrap());
                    if c == '"' {
                        break;
                    }
                }
                tokens.push(SqlToken::String(s));
            }
            // Number
            '0'..='9' => {
                let mut num = String::new();
                while let Some(&c) = chars.peek() {
                    if c.is_ascii_digit() || c == '.' {
                        num.push(chars.next().unwrap());
                    } else {
                        break;
                    }
                }
                tokens.push(SqlToken::Number(num));
            }
            // Operators
            '=' | '<' | '>' | '!' | '+' | '-' | '*' | '/' | '%' => {
                let mut op = String::new();
                op.push(chars.next().unwrap());
                // Handle two-character operators like !=, <=, >=, <>
                if let Some(&next_ch) = chars.peek()
                    && ((ch == '!' && next_ch == '=')
                        || (ch == '<' && (next_ch == '=' || next_ch == '>'))
                        || (ch == '>' && next_ch == '='))
                {
                    op.push(chars.next().unwrap());
                }
                tokens.push(SqlToken::Operator(op));
            }
            // Punctuation
            '(' | ')' | ',' | ';' | '.' | ':' => {
                tokens.push(SqlToken::Punctuation(chars.next().unwrap().to_string()));
            }
            // Word (keyword or identifier)
            _ if ch.is_alphabetic() || ch == '_' => {
                let mut word = String::new();
                while let Some(&c) = chars.peek() {
                    if c.is_alphanumeric() || c == '_' {
                        word.push(chars.next().unwrap());
                    } else {
                        break;
                    }
                }
                if SQL_KEYWORDS.contains(&word.to_uppercase().as_str()) {
                    tokens.push(SqlToken::Keyword(word));
                } else {
                    tokens.push(SqlToken::Identifier(word));
                }
            }
            // Other characters (backticks, brackets, etc.)
            _ => {
                tokens.push(SqlToken::Identifier(chars.next().unwrap().to_string()));
            }
        }
    }

    tokens
}

/// Get the style for a SQL token.
fn sql_token_style(token: &SqlToken) -> Style {
    match token {
        SqlToken::Keyword(_) => Theme::accent(),
        SqlToken::String(_) => Theme::status_success(),
        SqlToken::Number(_) => Theme::type_annotation(),
        SqlToken::Operator(_) => Theme::text_secondary(),
        SqlToken::Identifier(_) => Theme::text_primary(),
        SqlToken::Whitespace(_) => Style::default(),
        SqlToken::Punctuation(_) => Theme::text_secondary(),
    }
}

/// Get the text content of a SQL token.
fn sql_token_text(token: &SqlToken) -> &str {
    match token {
        SqlToken::Keyword(s)
        | SqlToken::String(s)
        | SqlToken::Number(s)
        | SqlToken::Operator(s)
        | SqlToken::Identifier(s)
        | SqlToken::Whitespace(s)
        | SqlToken::Punctuation(s) => s,
    }
}

/// Highlight SQL and insert cursor at specified position.
/// Returns spans for a single line with cursor indicator.
fn highlight_sql_with_cursor(line: &str, cursor_col: Option<usize>) -> Vec<Span<'static>> {
    let mut result = Vec::new();

    if let Some(col) = cursor_col {
        // Tokenize the entire line
        let tokens = tokenize_sql(line);

        let mut current_pos = 0;
        let mut cursor_inserted = false;

        for token in &tokens {
            let token_text = sql_token_text(token);
            let token_len = token_text.len();
            let token_end = current_pos + token_len;
            let style = sql_token_style(token);

            if !cursor_inserted && col >= current_pos && col < token_end {
                // Cursor is within this token
                let offset_in_token = col - current_pos;
                let (before, after) = token_text.split_at(offset_in_token);
                if !before.is_empty() {
                    result.push(Span::styled(before.to_string(), style));
                }
                result.push(Span::styled("_", Theme::status_warning()));
                cursor_inserted = true;
                if !after.is_empty() {
                    result.push(Span::styled(after.to_string(), style));
                }
            } else {
                result.push(Span::styled(token_text.to_string(), style));
            }

            current_pos = token_end;
        }

        // If cursor is at end of line
        if !cursor_inserted && col >= current_pos {
            result.push(Span::styled("_", Theme::status_warning()));
        }
    } else {
        // No cursor on this line - just highlight without cursor
        for token in tokenize_sql(line) {
            let style = sql_token_style(&token);
            let text = sql_token_text(&token).to_string();
            result.push(Span::styled(text, style));
        }
    }

    result
}

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
        DataSource::Local => Theme::source_local(),
        DataSource::Registry => Theme::source_registry(),
    };

    let text = vec![Line::from(vec![
        Span::styled("Amp CC", Theme::accent().add_modifier(Modifier::BOLD)),
        Span::raw(" | Source: "),
        Span::styled(app.current_source.as_str(), source_style),
        Span::raw(" | "),
        Span::styled(
            truncate_url(app.current_source_url(), 50),
            Theme::text_secondary(),
        ),
    ])];

    let border_style = if app.active_pane == ActivePane::Header {
        Theme::border_focused()
    } else {
        Theme::border_unfocused()
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

    // Add logo lines with The Graph Purple styling
    for line in logo_lines {
        lines.push(Line::from(Span::styled(
            line.to_string(),
            Theme::accent().add_modifier(Modifier::BOLD),
        )));
    }

    // Add spacing
    lines.push(Line::from(""));
    lines.push(Line::from(""));

    // Add version info
    lines.push(Line::from(vec![
        Span::styled("Version ", Theme::text_secondary()),
        Span::styled(env!("CARGO_PKG_VERSION"), Theme::version_tag()),
    ]));

    // Add source info
    lines.push(Line::from(""));
    let source_style = match app.current_source {
        DataSource::Local => Theme::source_local(),
        DataSource::Registry => Theme::source_registry(),
    };
    lines.push(Line::from(vec![
        Span::styled("Connected to: ", Theme::text_secondary()),
        Span::styled(app.current_source.as_str(), source_style),
    ]));
    lines.push(Line::from(Span::styled(
        app.current_source_url().to_string(),
        Theme::text_secondary(),
    )));

    // Add navigation hint
    lines.push(Line::from(""));
    lines.push(Line::from(Span::styled(
        "Press Tab to navigate to datasets",
        Theme::text_secondary(),
    )));

    let block = Block::default()
        .borders(Borders::ALL)
        .title("Welcome")
        .border_style(Theme::border_unfocused());

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

    // In Local mode, split sidebar into three sections
    if app.current_source == DataSource::Local {
        draw_sidebar_local(f, app, chunks[0]);
    } else {
        draw_sidebar(f, app, chunks[0]);
    }
    draw_content(f, app, chunks[1]);
}

/// Draw the sidebar with three sections for Local mode (Datasets, Jobs, Workers).
fn draw_sidebar_local(f: &mut Frame, app: &mut App, area: Rect) {
    // Split sidebar vertically into three sections
    let sidebar_chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Percentage(50), // Datasets
            Constraint::Percentage(25), // Jobs
            Constraint::Percentage(25), // Workers
        ])
        .split(area);

    draw_datasets_section(f, app, sidebar_chunks[0]);
    draw_jobs_section(f, app, sidebar_chunks[1]);
    draw_workers_section(f, app, sidebar_chunks[2]);
}

/// Draw the datasets section in the sidebar.
fn draw_datasets_section(f: &mut Frame, app: &App, area: Rect) {
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
            Span::styled(expand_indicator, Theme::text_secondary()),
            Span::styled(format!("{}/", dataset.namespace), Theme::text_secondary()),
            Span::styled(
                dataset.name.clone(),
                Theme::text_primary().add_modifier(Modifier::BOLD),
            ),
            Span::styled(version_str, Theme::version_tag()),
        ]);

        items.push(ListItem::new(dataset_line));
        current_flat_index += 1;

        // If expanded, show versions
        if dataset.expanded
            && let Some(versions) = &dataset.versions
        {
            for (version_idx, version) in versions.iter().enumerate() {
                // Check if this version is selected
                if current_flat_index == app.selected_index {
                    selected_flat_index = Some(items.len());
                }

                let status_style = match version.status.as_str() {
                    "published" | "active" => Theme::status_success(),
                    "draft" => Theme::status_warning(),
                    "deprecated" | "archived" => Theme::status_archived(),
                    _ => Theme::text_secondary(),
                };

                let latest_str = if version.is_latest { " (latest)" } else { "" };

                let is_last = version_idx == versions.len() - 1;
                let prefix = if is_last {
                    "  └── "
                } else {
                    "  ├── "
                };

                let version_line = Line::from(vec![
                    Span::styled(prefix, Theme::text_secondary()),
                    Span::raw(&version.version_tag),
                    Span::styled(latest_str, Theme::version_tag()),
                    Span::raw(" ["),
                    Span::styled(&version.status, status_style),
                    Span::raw("]"),
                ]);

                items.push(ListItem::new(version_line));
                current_flat_index += 1;
            }
        }
    }

    let title = format!("Datasets ({})", app.filtered_datasets.len());
    let border_style = if app.active_pane == ActivePane::Datasets {
        Theme::border_focused()
    } else {
        Theme::border_unfocused()
    };
    let list = List::new(items)
        .block(
            Block::default()
                .title(title)
                .borders(Borders::ALL)
                .border_style(border_style),
        )
        .highlight_style(Theme::selection())
        .highlight_symbol(">> ");

    let mut state = ListState::default();
    state.select(selected_flat_index);

    f.render_stateful_widget(list, area, &mut state);
}

/// Draw the jobs section in the sidebar.
fn draw_jobs_section(f: &mut Frame, app: &App, area: Rect) {
    let items: Vec<ListItem> = app
        .jobs
        .iter()
        .map(|job| {
            let status_icon = match job.status.to_lowercase().as_str() {
                "running" => "▶",
                "scheduled" => "◷",
                "completed" => "✓",
                "stopped" | "failed" => "✗",
                _ => "?",
            };
            let status_style = job_status_style(&job.status);

            let line = Line::from(vec![
                Span::styled(format!("{} ", status_icon), status_style),
                Span::styled(format!("Job {}", job.id), Theme::text_primary()),
                Span::styled(format!(" [{}]", job.status), status_style),
            ]);
            ListItem::new(line)
        })
        .collect();

    let title = format!("Jobs ({})", app.jobs.len());
    let border_style = if app.active_pane == ActivePane::Jobs {
        Theme::border_focused()
    } else {
        Theme::border_unfocused()
    };

    let list = List::new(items)
        .block(
            Block::default()
                .title(title)
                .borders(Borders::ALL)
                .border_style(border_style),
        )
        .highlight_style(Theme::selection())
        .highlight_symbol(">> ");

    let mut state = ListState::default();
    if !app.jobs.is_empty() {
        state.select(Some(app.selected_job_index));
    }

    f.render_stateful_widget(list, area, &mut state);
}

/// Draw the workers section in the sidebar.
fn draw_workers_section(f: &mut Frame, app: &App, area: Rect) {
    let items: Vec<ListItem> = app
        .workers
        .iter()
        .map(|worker| {
            let line = Line::from(vec![
                Span::styled("● ", Theme::status_success()),
                Span::styled(truncate_node_id(&worker.node_id, 25), Theme::text_primary()),
            ]);
            ListItem::new(line)
        })
        .collect();

    let title = format!("Workers ({})", app.workers.len());
    let border_style = if app.active_pane == ActivePane::Workers {
        Theme::border_focused()
    } else {
        Theme::border_unfocused()
    };

    let list = List::new(items)
        .block(
            Block::default()
                .title(title)
                .borders(Borders::ALL)
                .border_style(border_style),
        )
        .highlight_style(Theme::selection())
        .highlight_symbol(">> ");

    let mut state = ListState::default();
    if !app.workers.is_empty() {
        state.select(Some(app.selected_worker_index));
    }

    f.render_stateful_widget(list, area, &mut state);
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
            Span::styled(expand_indicator, Theme::text_secondary()),
            Span::styled(format!("{}/", dataset.namespace), Theme::text_secondary()),
            Span::styled(
                dataset.name.clone(),
                Theme::text_primary().add_modifier(Modifier::BOLD),
            ),
            Span::styled(version_str, Theme::version_tag()),
        ]);

        items.push(ListItem::new(dataset_line));
        current_flat_index += 1;

        // If expanded, show versions
        if dataset.expanded
            && let Some(versions) = &dataset.versions
        {
            for (version_idx, version) in versions.iter().enumerate() {
                // Check if this version is selected
                if current_flat_index == app.selected_index {
                    selected_flat_index = Some(items.len());
                }

                let status_style = match version.status.as_str() {
                    "published" | "active" => Theme::status_success(),
                    "draft" => Theme::status_warning(),
                    "deprecated" | "archived" => Theme::status_archived(),
                    _ => Theme::text_secondary(),
                };

                let latest_str = if version.is_latest { " (latest)" } else { "" };

                let is_last = version_idx == versions.len() - 1;
                let prefix = if is_last {
                    "  └── "
                } else {
                    "  ├── "
                };

                let version_line = Line::from(vec![
                    Span::styled(prefix, Theme::text_secondary()),
                    Span::raw(&version.version_tag),
                    Span::styled(latest_str, Theme::version_tag()),
                    Span::raw(" ["),
                    Span::styled(&version.status, status_style),
                    Span::raw("]"),
                ]);

                items.push(ListItem::new(version_line));
                current_flat_index += 1;
            }
        }
    }

    let title = format!("Datasets ({})", app.filtered_datasets.len());
    let border_style = if app.active_pane == ActivePane::Datasets {
        Theme::border_focused()
    } else {
        Theme::border_unfocused()
    };
    let list = List::new(items)
        .block(
            Block::default()
                .title(title)
                .borders(Borders::ALL)
                .border_style(border_style),
        )
        .highlight_style(Theme::selection())
        .highlight_symbol(">> ");

    let mut state = ListState::default();
    state.select(selected_flat_index);

    f.render_stateful_widget(list, area, &mut state);
}

/// Draw the content pane based on ContentView.
fn draw_content(f: &mut Frame, app: &mut App, area: Rect) {
    match &app.content_view {
        ContentView::Dataset => {
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
        ContentView::Job(job) => {
            draw_job_detail(f, app, job.clone(), area);
        }
        ContentView::Worker(worker) => {
            draw_worker_detail(f, app, worker.clone(), area);
        }
        ContentView::QueryResults => {
            // Dynamic height for query input: line count + 2 (borders), clamped to 3-10 lines
            let query_height = (app.query_line_count() as u16 + 2).clamp(3, 10);
            let chunks = Layout::default()
                .direction(Direction::Vertical)
                .constraints([
                    Constraint::Length(query_height), // Query input
                    Constraint::Min(0),               // Results
                ])
                .split(area);

            draw_query_input(f, app, chunks[0]);
            draw_query_results(f, app, chunks[1]);
        }
        ContentView::None => {
            draw_empty_content(f, area);
        }
    }
}

/// Draw empty content placeholder.
fn draw_empty_content(f: &mut Frame, area: Rect) {
    let block = Block::default()
        .title("Details")
        .borders(Borders::ALL)
        .border_style(Theme::border_unfocused());

    let text = Paragraph::new("Select an item to view details...")
        .block(block)
        .style(Theme::text_secondary())
        .alignment(Alignment::Center);

    f.render_widget(text, area);
}

/// Draw the SQL query input panel.
fn draw_query_input(f: &mut Frame, app: &App, area: Rect) {
    let border_style = if app.active_pane == ActivePane::Query {
        Theme::border_focused()
    } else {
        Theme::border_unfocused()
    };

    let line_count = app.query_line_count();
    let title = if app.input_mode == InputMode::Query {
        if let Some(idx) = app.query_history_index {
            format!(
                "SQL Query (history {}/{}) [Ctrl+Enter execute, Esc cancel]",
                idx + 1,
                app.query_history.len()
            )
        } else if !app.query_history.is_empty() {
            format!(
                "SQL Query ({} lines) [↑↓ nav, Enter newline, Ctrl+Enter execute]",
                line_count
            )
        } else {
            format!(
                "SQL Query ({} lines) [Enter newline, Ctrl+Enter execute]",
                line_count
            )
        }
    } else {
        "SQL Query (press Q to edit)".to_string()
    };

    let block = Block::default()
        .title(title)
        .borders(Borders::ALL)
        .border_style(border_style);

    // Build lines with SQL syntax highlighting and cursor rendering
    let lines: Vec<Line> = if app.input_mode == InputMode::Query {
        let query_lines = app.query_lines();
        query_lines
            .iter()
            .enumerate()
            .map(|(line_idx, line_text)| {
                let cursor_col = if line_idx == app.query_cursor.line {
                    Some(app.query_cursor.column.min(line_text.len()))
                } else {
                    None
                };
                Line::from(highlight_sql_with_cursor(line_text, cursor_col))
            })
            .collect()
    } else {
        // Not in query mode, show syntax highlighting without cursor
        app.query_lines()
            .iter()
            .map(|line| Line::from(highlight_sql_with_cursor(line, None)))
            .collect()
    };

    let paragraph = Paragraph::new(lines)
        .block(block)
        .scroll((app.query_input_scroll, 0));
    f.render_widget(paragraph, area);
}

/// Calculate column widths based on content, with min/max constraints.
///
/// Returns a vector of pixel widths for each column, auto-sized based on content
/// and scaled proportionally if total exceeds available width.
fn calculate_column_widths(results: &QueryResults, available_width: u16) -> Vec<u16> {
    let col_count = results.columns.len();
    if col_count == 0 {
        return vec![];
    }

    // Calculate max width needed for each column (considering headers)
    let mut max_widths: Vec<usize> = results.columns.iter().map(|c| c.len()).collect();

    // Check data rows for wider content
    for row in &results.rows {
        for (i, cell) in row.iter().enumerate() {
            if i < max_widths.len() {
                max_widths[i] = max_widths[i].max(cell.len());
            }
        }
    }

    // Apply min (5 chars) and max (50 chars) constraints
    const MIN_WIDTH: u16 = 5;
    const MAX_WIDTH: u16 = 50;

    let mut widths: Vec<u16> = max_widths
        .iter()
        .map(|&w| (w as u16).clamp(MIN_WIDTH, MAX_WIDTH))
        .collect();

    // Account for borders and spacing between columns (3 chars per column for " | ")
    let total_padding = (col_count as u16).saturating_mul(3);
    let usable = available_width.saturating_sub(total_padding);

    // Scale proportionally if total exceeds available width
    let total: u16 = widths.iter().sum();
    if total > usable && total > 0 {
        let scale = usable as f32 / total as f32;
        widths = widths
            .iter()
            .map(|&w| ((w as f32 * scale) as u16).max(MIN_WIDTH))
            .collect();
    }

    widths
}

/// Draw the SQL query results table.
fn draw_query_results(f: &mut Frame, app: &mut App, area: Rect) {
    let border_style = if app.active_pane == ActivePane::QueryResult {
        Theme::border_focused()
    } else {
        Theme::border_unfocused()
    };

    let Some(results) = &app.query_results else {
        let block = Block::default()
            .title("Query Results")
            .borders(Borders::ALL)
            .border_style(border_style);
        let text = Paragraph::new("No query executed yet. Press Q to enter a SQL query.")
            .block(block)
            .style(Theme::text_secondary());
        f.render_widget(text, area);
        return;
    };

    // Show error if present
    if let Some(error) = &results.error {
        let block = Block::default()
            .title("Query Error")
            .borders(Borders::ALL)
            .border_style(Theme::status_error());
        let text = Paragraph::new(error.as_str())
            .block(block)
            .style(Theme::status_error())
            .wrap(Wrap { trim: false });
        f.render_widget(text, area);
        return;
    }

    let title = format!(
        "Query Results ({} rows) - Press E to export",
        results.row_count
    );
    let block = Block::default()
        .title(title)
        .borders(Borders::ALL)
        .border_style(border_style);

    // Handle empty results
    if results.columns.is_empty() {
        let text = Paragraph::new("Query returned no columns.")
            .block(block)
            .style(Theme::text_secondary());
        f.render_widget(text, area);
        return;
    }

    // Calculate auto-sized column widths based on content
    // Account for 2 chars of border on each side of the table
    let available_width = area.width.saturating_sub(2);
    let col_widths = calculate_column_widths(results, available_width);

    // Build table header with column numbers
    let header_cells: Vec<Cell> = results
        .columns
        .iter()
        .enumerate()
        .map(|(idx, c)| {
            let col_num = idx + 1; // 1-indexed for user display
            let header_text = format!("[{}] {}", col_num, c);
            Cell::from(header_text).style(Theme::text_primary().add_modifier(Modifier::BOLD))
        })
        .collect();
    let header = Row::new(header_cells).height(1);

    // Build table rows with truncation based on calculated column widths
    let rows: Vec<Row> = results
        .rows
        .iter()
        .map(|row| {
            let cells: Vec<Cell> = row
                .iter()
                .enumerate()
                .map(|(col_idx, val)| {
                    // Get the calculated width for this column (default to 50 if out of bounds)
                    let max_width = col_widths.get(col_idx).copied().unwrap_or(50) as usize;
                    let display = if val.len() > max_width && max_width > 3 {
                        format!("{}...", &val[..max_width.saturating_sub(3)])
                    } else {
                        val.clone()
                    };
                    Cell::from(display)
                })
                .collect();
            Row::new(cells)
        })
        .collect();

    // Convert widths to constraints for the table
    let widths: Vec<Constraint> = col_widths.iter().map(|&w| Constraint::Length(w)).collect();

    let table = Table::new(rows, widths)
        .header(header)
        .block(block)
        .row_highlight_style(Theme::selection());

    // Update content length for scrolling
    app.query_content_length = results.rows.len();
    app.query_scroll_state = app.query_scroll_state.content_length(results.rows.len());

    // Render with scroll state
    let mut table_state = ratatui::widgets::TableState::default();
    table_state.select(Some(app.query_scroll as usize));

    f.render_stateful_widget(table, area, &mut table_state);

    // Draw scrollbar if content exceeds visible area
    let visible_rows = area.height.saturating_sub(3) as usize; // -3 for borders and header
    if results.rows.len() > visible_rows {
        let scrollbar = Scrollbar::new(ScrollbarOrientation::VerticalRight);
        let mut scrollbar_state = app
            .query_scroll_state
            .content_length(results.rows.len())
            .position(app.query_scroll as usize);
        f.render_stateful_widget(
            scrollbar,
            area.inner(ratatui::layout::Margin {
                vertical: 1,
                horizontal: 0,
            }),
            &mut scrollbar_state,
        );
    }
}

/// Format job descriptor fields as readable lines (generic key-value display).
fn format_descriptor_lines(descriptor: &serde_json::Value) -> Vec<Line<'static>> {
    let mut lines = Vec::new();
    format_json_value(descriptor, &mut lines, 1);
    lines
}

/// Format JSON value as key-value lines.
fn format_json_value(value: &serde_json::Value, lines: &mut Vec<Line<'static>>, indent: usize) {
    let prefix = "  ".repeat(indent);
    match value {
        serde_json::Value::Object(map) => {
            for (key, val) in map {
                match val {
                    serde_json::Value::Object(_) => {
                        lines.push(Line::from(Span::styled(
                            format!("{}{}", prefix, key),
                            Theme::text_secondary(),
                        )));
                        format_json_value(val, lines, indent + 1);
                    }
                    _ => {
                        let val_str = json_value_to_string(val);
                        lines.push(Line::from(vec![
                            Span::styled(format!("{}{}: ", prefix, key), Theme::text_secondary()),
                            Span::styled(val_str, Theme::text_primary()),
                        ]));
                    }
                }
            }
        }
        _ => {
            lines.push(Line::from(Span::styled(
                json_value_to_string(value),
                Theme::text_primary(),
            )));
        }
    }
}

/// Convert a JSON value to a display string.
fn json_value_to_string(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::String(s) => s.clone(),
        serde_json::Value::Number(n) => n.to_string(),
        serde_json::Value::Bool(b) => b.to_string(),
        serde_json::Value::Null => "null".to_string(),
        _ => value.to_string(),
    }
}

/// Draw job detail view.
fn draw_job_detail(f: &mut Frame, app: &mut App, job: JobInfo, area: Rect) {
    let border_style = if app.active_pane == ActivePane::Detail {
        Theme::border_focused()
    } else {
        Theme::border_unfocused()
    };

    let title = format!("Job: {}", job.id);
    let block = Block::default()
        .title(title)
        .borders(Borders::ALL)
        .border_style(border_style);

    // Build job details
    let mut lines: Vec<Line> = Vec::new();

    // Status with color
    let status_style = job_status_style(&job.status);
    lines.push(Line::from(vec![
        Span::styled("Status: ", Theme::text_secondary()),
        Span::styled(&job.status, status_style),
    ]));

    lines.push(Line::from(""));

    // Node ID
    lines.push(Line::from(vec![
        Span::styled("Node ID: ", Theme::text_secondary()),
        Span::styled(&job.node_id, Theme::text_primary()),
    ]));

    // Timestamps
    lines.push(Line::from(vec![
        Span::styled("Created: ", Theme::text_secondary()),
        Span::styled(&job.created_at, Theme::text_primary()),
    ]));
    lines.push(Line::from(vec![
        Span::styled("Updated: ", Theme::text_secondary()),
        Span::styled(&job.updated_at, Theme::text_primary()),
    ]));

    lines.push(Line::from(""));
    lines.push(Line::from(Span::styled(
        "Descriptor:",
        Theme::text_secondary(),
    )));
    lines.push(Line::from(""));

    // Descriptor fields in clean format
    lines.extend(format_descriptor_lines(&job.descriptor));

    // Update content length for scroll bounds
    app.detail_content_length = lines.len();
    app.detail_scroll_state = app.detail_scroll_state.content_length(lines.len());

    let paragraph = Paragraph::new(lines)
        .block(block)
        .scroll((app.detail_scroll, 0));
    f.render_widget(paragraph, area);

    // Render scrollbar
    let scrollbar = Scrollbar::new(ScrollbarOrientation::VerticalRight);
    f.render_stateful_widget(scrollbar, area, &mut app.detail_scroll_state);
}

/// Draw worker detail view.
fn draw_worker_detail(f: &mut Frame, app: &mut App, worker: WorkerDetailResponse, area: Rect) {
    let border_style = if app.active_pane == ActivePane::Detail {
        Theme::border_focused()
    } else {
        Theme::border_unfocused()
    };

    let title = format!("Worker: {}", truncate_node_id(&worker.node_id, 20));
    let block = Block::default()
        .title(title)
        .borders(Borders::ALL)
        .border_style(border_style);

    let lines: Vec<Line> = vec![
        // Node ID
        Line::from(vec![
            Span::styled("Node ID: ", Theme::text_secondary()),
            Span::styled(&worker.node_id, Theme::text_primary()),
        ]),
        Line::from(""),
        // Timestamps
        Line::from(vec![
            Span::styled("Created: ", Theme::text_secondary()),
            Span::styled(&worker.created_at, Theme::text_primary()),
        ]),
        Line::from(vec![
            Span::styled("Registered: ", Theme::text_secondary()),
            Span::styled(&worker.registered_at, Theme::text_primary()),
        ]),
        Line::from(vec![
            Span::styled("Last Heartbeat: ", Theme::text_secondary()),
            Span::styled(&worker.heartbeat_at, Theme::status_success()),
        ]),
        Line::from(""),
        Line::from(Span::styled("Build Information:", Theme::text_secondary())),
        Line::from(""),
        // Worker metadata
        Line::from(vec![
            Span::styled("  Version: ", Theme::text_secondary()),
            Span::styled(&worker.info.version, Theme::version_tag()),
        ]),
        Line::from(vec![
            Span::styled("  Commit SHA: ", Theme::text_secondary()),
            Span::styled(&worker.info.commit_sha, Theme::text_primary()),
        ]),
        Line::from(vec![
            Span::styled("  Commit Time: ", Theme::text_secondary()),
            Span::styled(&worker.info.commit_timestamp, Theme::text_primary()),
        ]),
        Line::from(vec![
            Span::styled("  Build Date: ", Theme::text_secondary()),
            Span::styled(&worker.info.build_date, Theme::text_primary()),
        ]),
    ];

    // Update content length for scroll bounds
    app.detail_content_length = lines.len();
    app.detail_scroll_state = app.detail_scroll_state.content_length(lines.len());

    let paragraph = Paragraph::new(lines)
        .block(block)
        .scroll((app.detail_scroll, 0));
    f.render_widget(paragraph, area);

    // Render scrollbar
    let scrollbar = Scrollbar::new(ScrollbarOrientation::VerticalRight);
    f.render_stateful_widget(scrollbar, area, &mut app.detail_scroll_state);
}

/// Get style for job status.
fn job_status_style(status: &str) -> Style {
    match status.to_lowercase().as_str() {
        "running" => Theme::status_success(),
        "scheduled" => Theme::status_warning(),
        "completed" => Theme::text_secondary(),
        "stopped" | "failed" => Theme::status_error(),
        _ => Theme::text_primary(),
    }
}

/// Truncate node ID for display.
fn truncate_node_id(node_id: &str, max_len: usize) -> String {
    if node_id.len() <= max_len {
        node_id.to_string()
    } else {
        format!("{}...", &node_id[..max_len - 3])
    }
}

/// Draw the manifest pane.
fn draw_manifest(f: &mut Frame, app: &mut App, area: Rect) {
    let border_style = if app.active_pane == ActivePane::Manifest {
        Theme::border_focused()
    } else {
        Theme::border_unfocused()
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
            .style(Theme::status_error());
        f.render_widget(text, area);
    } else if let Some(manifest) = &app.current_manifest {
        let lines = format_manifest(manifest);
        let line_count = lines.len();

        // Update content length for scroll bounds
        app.manifest_content_length = line_count;

        // Update scroll state with content length
        app.manifest_scroll_state = app.manifest_scroll_state.content_length(line_count);

        let text = Paragraph::new(lines)
            .block(block)
            .scroll((app.manifest_scroll, 0));
        f.render_widget(text, area);

        // Render scrollbar
        let scrollbar = Scrollbar::new(ScrollbarOrientation::VerticalRight);
        f.render_stateful_widget(scrollbar, area, &mut app.manifest_scroll_state);
    } else {
        let text = Paragraph::new("Select a dataset to view manifest...")
            .block(block)
            .style(Theme::text_secondary());
        f.render_widget(text, area);
    }
}

/// Draw the inspect pane with schema information.
fn draw_inspect(f: &mut Frame, app: &mut App, area: Rect) {
    let border_style = if app.active_pane == ActivePane::Schema {
        Theme::border_focused()
    } else {
        Theme::border_unfocused()
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
            .style(Theme::text_secondary());
        f.render_widget(content, area);
    } else {
        let content = Paragraph::new("Select a dataset to view schema...")
            .block(block)
            .style(Theme::text_secondary());
        f.render_widget(content, area);
    }
}

/// Remove common leading whitespace from SQL strings.
fn dedent_sql(sql: &str) -> String {
    let lines: Vec<&str> = sql.lines().collect();
    if lines.is_empty() {
        return String::new();
    }
    let min_indent = lines
        .iter()
        .skip(1)
        .filter(|line| !line.trim().is_empty())
        .map(|line| line.len() - line.trim_start().len())
        .min()
        .unwrap_or(0);
    let mut result = Vec::new();
    for (i, line) in lines.iter().enumerate() {
        if i == 0 {
            result.push(line.trim());
        } else if line.trim().is_empty() {
            result.push("");
        } else if let Some(trimmed) = line.get(min_indent..) {
            result.push(trimmed);
        } else {
            result.push(line.trim());
        }
    }
    result.join("\n").trim().to_string()
}

/// Format JSON manifest into readable lines.
fn format_manifest(manifest: &serde_json::Value) -> Vec<Line<'static>> {
    let mut lines = Vec::new();
    const SEPARATOR: &str = "────────────────────────────────────";

    // Kind
    if let Some(kind) = manifest.get("kind").and_then(|v| v.as_str()) {
        lines.push(Line::from(vec![
            Span::styled("Kind: ", Theme::text_secondary()),
            Span::styled(kind.to_string(), Theme::text_primary()),
        ]));
    }

    // Start Block (optional)
    if let Some(start_block) = manifest.get("start_block").and_then(|v| v.as_u64()) {
        lines.push(Line::from(vec![
            Span::styled("Start Block: ", Theme::text_secondary()),
            Span::styled(start_block.to_string(), Theme::text_primary()),
        ]));
    }

    lines.push(Line::from(""));

    // Dependencies
    lines.push(Line::from(vec![Span::styled(
        "Dependencies:",
        Theme::text_primary().add_modifier(Modifier::BOLD),
    )]));

    if let Some(deps) = manifest.get("dependencies").and_then(|v| v.as_object()) {
        if deps.is_empty() {
            lines.push(Line::from(vec![
                Span::raw("  "),
                Span::styled("(none)", Theme::text_secondary()),
            ]));
        } else {
            for (alias, reference) in deps {
                let ref_str = reference.as_str().unwrap_or("unknown");
                lines.push(Line::from(vec![
                    Span::raw("  "),
                    Span::styled(alias.clone(), Theme::text_primary()),
                    Span::styled(" → ", Theme::text_secondary()),
                    Span::styled(ref_str.to_string(), Theme::version_tag()),
                ]));
            }
        }
    } else {
        lines.push(Line::from(vec![
            Span::raw("  "),
            Span::styled("(none)", Theme::text_secondary()),
        ]));
    }

    lines.push(Line::from(""));

    // Tables
    lines.push(Line::from(vec![Span::styled(
        "Tables:",
        Theme::text_primary().add_modifier(Modifier::BOLD),
    )]));

    if let Some(tables) = manifest.get("tables").and_then(|v| v.as_object()) {
        if tables.is_empty() {
            lines.push(Line::from(vec![
                Span::raw("  "),
                Span::styled("(none)", Theme::text_secondary()),
            ]));
        } else {
            for (table_name, table_def) in tables {
                let network = table_def
                    .get("network")
                    .and_then(|v| v.as_str())
                    .unwrap_or("unknown");

                // Table header with name and network
                lines.push(Line::from(vec![
                    Span::raw("  "),
                    Span::styled("▸ ", Theme::text_secondary()),
                    Span::styled(
                        table_name.clone(),
                        Theme::border_focused().add_modifier(Modifier::BOLD),
                    ),
                    Span::styled(" (", Theme::text_secondary()),
                    Span::styled(network.to_string(), Theme::type_annotation()),
                    Span::styled(")", Theme::text_secondary()),
                ]));

                // SQL query
                if let Some(sql) = table_def
                    .get("input")
                    .and_then(|v| v.get("sql"))
                    .and_then(|v| v.as_str())
                {
                    let dedented_sql = dedent_sql(sql);
                    lines.push(Line::from(vec![
                        Span::raw("    "),
                        Span::styled(SEPARATOR, Theme::text_secondary()),
                    ]));
                    for sql_line in dedented_sql.lines() {
                        lines.push(Line::from(vec![
                            Span::raw("    "),
                            Span::styled(sql_line.to_string(), Theme::text_primary()),
                        ]));
                    }
                    lines.push(Line::from(vec![
                        Span::raw("    "),
                        Span::styled(SEPARATOR, Theme::text_secondary()),
                    ]));
                }
                lines.push(Line::from(""));
            }
        }
    } else {
        lines.push(Line::from(vec![
            Span::raw("  "),
            Span::styled("(none)", Theme::text_secondary()),
        ]));
    }

    // Functions
    lines.push(Line::from(vec![Span::styled(
        "Functions:",
        Theme::text_primary().add_modifier(Modifier::BOLD),
    )]));

    if let Some(functions) = manifest.get("functions").and_then(|v| v.as_object()) {
        if functions.is_empty() {
            lines.push(Line::from(vec![
                Span::raw("  "),
                Span::styled("(none)", Theme::text_secondary()),
            ]));
        } else {
            for func_name in functions.keys() {
                lines.push(Line::from(vec![
                    Span::raw("  "),
                    Span::styled("▸ ", Theme::text_secondary()),
                    Span::styled(func_name.clone(), Theme::text_primary()),
                ]));
            }
        }
    } else {
        lines.push(Line::from(vec![
            Span::raw("  "),
            Span::styled("(none)", Theme::text_secondary()),
        ]));
    }

    lines
}

/// Format inspect result into styled lines.
fn format_inspect_result(inspect: &InspectResult) -> Vec<Line<'static>> {
    let mut lines = Vec::new();

    for table in &inspect.tables {
        // Table header
        lines.push(Line::from(vec![
            Span::styled(
                table.name.clone(),
                Theme::border_focused().add_modifier(Modifier::BOLD),
            ),
            Span::styled(
                format!(" ({} columns)", table.columns.len()),
                Theme::text_secondary(),
            ),
        ]));

        // Columns
        for col in &table.columns {
            let nullable_str = if col.nullable { "" } else { " NOT NULL" };
            lines.push(Line::from(vec![
                Span::raw("  "),
                Span::styled(format!("{:<24}", col.name), Theme::text_primary()),
                Span::styled(format!("{:<32}", col.arrow_type), Theme::type_annotation()),
                Span::styled(nullable_str.to_string(), Theme::constraint()),
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
    } else if matches!(app.input_mode, InputMode::Query) {
        "Query: Type SQL, Ctrl+Enter to execute, Esc to cancel".to_string()
    } else {
        let source_hint = match app.current_source {
            DataSource::Local => "[1]Local [2]Registry",
            DataSource::Registry => "[1]Local [2]Registry",
        };

        // Context-sensitive keybindings based on active pane
        let context_keys = match app.active_pane {
            ActivePane::Jobs => "[s] Stop [d] Delete [r] Refresh",
            ActivePane::Workers => "[r] Refresh",
            ActivePane::Datasets => {
                if app.is_local() {
                    "[Enter] Expand [r] Refresh [Q] Query"
                } else {
                    "[Enter] Expand [r] Refresh"
                }
            }
            ActivePane::Manifest | ActivePane::Schema | ActivePane::Detail => "[Ctrl+u/d] Scroll",
            ActivePane::Header => "[Tab] Navigate",
            ActivePane::Query => {
                if app.query_history.is_empty() {
                    "[Ctrl+Enter] Execute [Esc] Cancel"
                } else {
                    "[↑↓] History [Ctrl+Enter] Execute [Esc] Cancel"
                }
            }
            ActivePane::QueryResult => "[E] Export [j/k] Scroll [Q] Edit query",
        };

        format!(
            "{} | {} | '/' search | 'j/k' nav | Tab cycle | 'q' quit",
            source_hint, context_keys
        )
    };

    // Build the right side (loading indicator, success message, or error message)
    let (right_spans, right_width): (Vec<Span>, u16) = if app.loading {
        // Show loading spinner
        let mut spans = Vec::new();
        if let Some(msg) = &app.loading_message {
            spans.push(Span::styled(format!("{} ", msg), Theme::status_warning()));
        }
        spans.push(Span::styled(
            format!("{}", app.spinner_char()),
            Theme::status_warning(),
        ));
        let msg_len = app
            .loading_message
            .as_ref()
            .map(|m| m.len() + 1)
            .unwrap_or(0);
        (spans, (msg_len + 2) as u16)
    } else if let Some(success) = &app.success_message {
        // Show success message (green)
        let display_success = if success.len() > 60 {
            format!("{}...", &success[..57])
        } else {
            success.clone()
        };
        let spans = vec![Span::styled(
            format!("✓ {}", display_success),
            Theme::status_success(),
        )];
        let width = (display_success.len() + 3) as u16; // icon + space + message
        (spans, width)
    } else if let Some(error) = &app.error_message {
        // Show error message
        let display_error = if error.len() > 50 {
            format!("{}...", &error[..47])
        } else {
            error.clone()
        };
        let spans = vec![Span::styled(
            format!("⚠ {}", display_error),
            Theme::status_error(),
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
    let left_paragraph = Paragraph::new(left_text).style(Theme::text_secondary());
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
