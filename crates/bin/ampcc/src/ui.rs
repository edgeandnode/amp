//! UI rendering for the Amp CC TUI.
//!
//! This module uses The Graph's official color palette for consistent branding.

use std::sync::LazyLock;

use admin_client::{jobs::JobInfo, workers::WorkerDetailResponse};
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

use crate::app::{ActivePane, App, ContentView, DataSource, InputMode, InspectResult};

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

    // Descriptor JSON with syntax highlighting
    let descriptor_str = serde_json::to_string_pretty(&job.descriptor)
        .unwrap_or_else(|_| "Invalid JSON".to_string());
    let highlighted = highlight_json(&descriptor_str);
    lines.extend(highlighted);

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
                Span::styled(format!("{:<20}", col.arrow_type), Theme::type_annotation()),
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
    } else {
        let source_hint = match app.current_source {
            DataSource::Local => "[1]Local [2]Registry",
            DataSource::Registry => "[1]Local [2]Registry",
        };

        // Context-sensitive keybindings based on active pane
        let context_keys = match app.active_pane {
            ActivePane::Jobs => "[s] Stop [d] Delete [r] Refresh",
            ActivePane::Workers => "[r] Refresh",
            ActivePane::Datasets => "[Enter] Expand [r] Refresh",
            ActivePane::Manifest | ActivePane::Schema | ActivePane::Detail => "[Ctrl+u/d] Scroll",
            ActivePane::Header => "[Tab] Navigate",
        };

        format!(
            "{} | {} | '/' search | 'j/k' nav | Tab cycle | 'q' quit",
            source_hint, context_keys
        )
    };

    // Build the right side (loading indicator or error message)
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
