//! Amp CC - Terminal UI for managing Amp datasets.

use std::{
    io,
    time::{Duration, Instant},
};

use admin_client::{
    jobs::JobInfo,
    workers::{WorkerDetailResponse, WorkerInfo},
};
use anyhow::Result;
use arrow::array::{
    Array, BinaryArray, BooleanArray, Float64Array, Int64Array, StringArray, UInt64Array,
};
use crossterm::{
    event::{
        self, DisableMouseCapture, EnableMouseCapture, Event, KeyCode, KeyModifiers, MouseEventKind,
    },
    execute,
    terminal::{EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode},
};
use futures::StreamExt;
use ratatui::{Terminal, backend::CrosstermBackend};
use tokio::sync::mpsc;
use worker::{job::JobId, node_id::NodeId};

mod app;
mod config;
mod registry;
mod ui;

use app::{
    ActivePane, App, ContentView, DataSource, InputMode, InspectResult, QueryResults,
    QUERY_TEMPLATES,
};
use ratatui::layout::{Constraint, Direction, Layout, Rect};

/// Auto-refresh interval for jobs/workers (10 seconds).
const REFRESH_INTERVAL: Duration = Duration::from_secs(10);

/// Events that can be sent from async tasks.
enum AppEvent {
    ManifestLoaded(Option<serde_json::Value>),
    JobsLoaded(Vec<JobInfo>),
    WorkersLoaded(Vec<WorkerInfo>),
    WorkerDetailLoaded(Option<WorkerDetailResponse>),
    JobStopped(Result<(), String>),
    JobDeleted(Result<(), String>),
    QueryCompleted(QueryResults),
    Error(String),
}

#[tokio::main]
async fn main() -> Result<()> {
    let config = config::Config::load()?;
    let mut app = App::new(config)?;

    // Load persisted history
    if let Err(e) = app.load_history() {
        eprintln!("Warning: could not load history: {}", e);
        // Continue anyway - not fatal
    }

    // Load favorites
    app.load_favorites();

    // Fetch initial datasets
    if let Err(e) = app.fetch_datasets().await {
        eprintln!("Failed to fetch datasets: {}", e);
        // Continue anyway to show UI
    }

    // If starting in Local mode, also fetch jobs and workers
    // (will be handled in run_app after channel is set up)

    // Setup terminal
    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen, EnableMouseCapture)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    // Run app
    let res = run_app(&mut terminal, &mut app).await;

    // Restore terminal
    disable_raw_mode()?;
    execute!(
        terminal.backend_mut(),
        LeaveAlternateScreen,
        DisableMouseCapture
    )?;
    terminal.show_cursor()?;

    if let Err(err) = res {
        println!("{:?}", err);
    }

    Ok(())
}

async fn run_app<B: ratatui::backend::Backend>(
    terminal: &mut Terminal<B>,
    app: &mut App,
) -> Result<()>
where
    B::Error: Send + Sync + 'static,
{
    let (tx, mut rx) = mpsc::channel::<AppEvent>(10);
    let tick_rate = std::time::Duration::from_millis(100);

    // Initial manifest fetch
    app.start_loading("Loading manifest...");
    spawn_fetch_manifest(app, tx.clone());

    // If in Local mode, also fetch jobs and workers
    if app.is_local() {
        spawn_fetch_jobs(app, tx.clone());
        spawn_fetch_workers(app, tx.clone());
    }

    loop {
        // Tick spinner animation (only when loading)
        if app.loading {
            app.tick_spinner();
            app.needs_redraw = true;
        }

        // Tick message expiration (for success messages)
        let had_message = app.success_message.is_some();
        app.tick_messages();
        if had_message && app.success_message.is_none() {
            app.needs_redraw = true;
        }

        // Auto-refresh jobs/workers in Local mode
        if app.is_local() && app.last_refresh.elapsed() >= REFRESH_INTERVAL {
            spawn_fetch_jobs(app, tx.clone());
            spawn_fetch_workers(app, tx.clone());
            app.last_refresh = Instant::now();
        }

        // Handle async updates
        if let Ok(event) = rx.try_recv() {
            match event {
                AppEvent::ManifestLoaded(manifest) => {
                    app.reset_scroll();
                    app.current_inspect = manifest.as_ref().and_then(InspectResult::from_manifest);
                    app.current_manifest = manifest;
                    app.content_view = ContentView::Dataset;
                    app.stop_loading();
                }
                AppEvent::JobsLoaded(jobs) => {
                    app.jobs = jobs;
                    // Reset selection if out of bounds
                    if app.selected_job_index >= app.jobs.len() && !app.jobs.is_empty() {
                        app.selected_job_index = app.jobs.len() - 1;
                    }
                }
                AppEvent::WorkersLoaded(workers) => {
                    app.workers = workers;
                    // Reset selection if out of bounds
                    if app.selected_worker_index >= app.workers.len() && !app.workers.is_empty() {
                        app.selected_worker_index = app.workers.len() - 1;
                    }
                }
                AppEvent::WorkerDetailLoaded(detail) => {
                    if let Some(worker_detail) = detail {
                        app.content_view = ContentView::Worker(worker_detail);
                        app.reset_scroll();
                    }
                    app.stop_loading();
                }
                AppEvent::JobStopped(result) => {
                    match result {
                        Ok(()) => {
                            // Refresh jobs list after stopping
                            spawn_fetch_jobs(app, tx.clone());
                        }
                        Err(msg) => {
                            app.error_message = Some(msg);
                        }
                    }
                    app.stop_loading();
                }
                AppEvent::JobDeleted(result) => {
                    match result {
                        Ok(()) => {
                            // Refresh jobs list after deleting
                            spawn_fetch_jobs(app, tx.clone());
                        }
                        Err(msg) => {
                            app.error_message = Some(msg);
                        }
                    }
                    app.stop_loading();
                }
                AppEvent::QueryCompleted(results) => {
                    app.query_results = Some(results);
                    app.query_scroll = 0;
                    app.query_scroll_state = ratatui::widgets::ScrollbarState::default();
                    app.content_view = ContentView::QueryResults;
                    app.active_pane = ActivePane::QueryResult;
                    app.input_mode = InputMode::Normal;
                    app.stop_loading();
                }
                AppEvent::Error(msg) => {
                    app.error_message = Some(msg);
                    app.stop_loading();
                }
            }
            app.needs_redraw = true;
        }

        // Handle Input
        if event::poll(tick_rate)? {
            match event::read()? {
                Event::Key(key) => {
                    // Handle template picker if open (modal popup)
                    if app.template_picker_open {
                        match key.code {
                            KeyCode::Up => {
                                if app.template_picker_index > 0 {
                                    app.template_picker_index -= 1;
                                }
                            }
                            KeyCode::Down => {
                                if app.template_picker_index < QUERY_TEMPLATES.len() - 1 {
                                    app.template_picker_index += 1;
                                }
                            }
                            KeyCode::Enter => {
                                // Apply selected template
                                let template = &QUERY_TEMPLATES[app.template_picker_index];
                                let resolved = app.resolve_template(template.pattern);
                                app.set_query_input(resolved);
                                app.template_picker_open = false;
                            }
                            KeyCode::Esc => {
                                app.template_picker_open = false;
                            }
                            _ => {}
                        }
                        continue;
                    }

                    // Handle favorites panel if open (modal popup)
                    if app.favorites_panel_open {
                        match key.code {
                            KeyCode::Up => {
                                if app.favorites_panel_index > 0 {
                                    app.favorites_panel_index -= 1;
                                }
                            }
                            KeyCode::Down => {
                                if app.favorites_panel_index < app.favorite_queries.len().saturating_sub(1) {
                                    app.favorites_panel_index += 1;
                                }
                            }
                            KeyCode::Enter => {
                                // Load selected favorite into query input
                                if let Some(query) = app.favorite_queries.get(app.favorites_panel_index).cloned() {
                                    app.set_query_input(query);
                                    app.favorites_panel_open = false;
                                }
                            }
                            KeyCode::Char('d') => {
                                // Delete the selected favorite
                                app.remove_favorite(app.favorites_panel_index);
                            }
                            KeyCode::Esc => {
                                app.favorites_panel_open = false;
                            }
                            _ => {}
                        }
                        continue;
                    }

                    match app.input_mode {
                        InputMode::Normal => {
                            match key.code {
                                KeyCode::Char('q') => app.quit(),
                                KeyCode::Char('c') if key.modifiers == KeyModifiers::CONTROL => {
                                    app.quit()
                                }

                                // Source switching
                                KeyCode::Char('1') => {
                                    let tx = tx.clone();
                                    app.start_loading("Switching source...");
                                    if let Err(e) = app.switch_source(DataSource::Local).await {
                                        let _ = tx.send(AppEvent::Error(e.to_string())).await;
                                    } else {
                                        app.start_loading("Loading manifest...");
                                        spawn_fetch_manifest(app, tx.clone());
                                        // Also fetch jobs and workers in Local mode
                                        spawn_fetch_jobs(app, tx.clone());
                                        spawn_fetch_workers(app, tx);
                                        app.last_refresh = Instant::now();
                                    }
                                }
                                KeyCode::Char('2') => {
                                    let tx = tx.clone();
                                    app.start_loading("Switching source...");
                                    if let Err(e) = app.switch_source(DataSource::Registry).await {
                                        let _ = tx.send(AppEvent::Error(e.to_string())).await;
                                    } else {
                                        app.start_loading("Loading manifest...");
                                        spawn_fetch_manifest(app, tx);
                                    }
                                }

                                // Search
                                KeyCode::Char('/') => {
                                    app.input_mode = InputMode::Search;
                                }

                                // Query mode (Q key) - only in Local mode
                                KeyCode::Char('Q') => {
                                    if app.is_local() {
                                        app.input_mode = InputMode::Query;
                                        app.active_pane = ActivePane::Query;
                                        app.content_view = ContentView::QueryResults;
                                        // Pre-populate with template if dataset selected and input empty
                                        if app.query_input.is_empty()
                                            && let Some((ns, name, _)) =
                                                app.get_selected_manifest_params()
                                        {
                                            app.set_query_input(format!(
                                                "SELECT * FROM {}.{} LIMIT 10",
                                                ns, name
                                            ));
                                        }
                                    }
                                }

                                // Refresh - context sensitive
                                KeyCode::Char('r') => {
                                    let tx = tx.clone();
                                    match app.active_pane {
                                        ActivePane::Datasets => {
                                            app.start_loading("Refreshing datasets...");
                                            if let Err(e) = app.fetch_datasets().await {
                                                let _ =
                                                    tx.send(AppEvent::Error(e.to_string())).await;
                                            } else {
                                                app.start_loading("Loading manifest...");
                                                spawn_fetch_manifest(app, tx);
                                            }
                                        }
                                        ActivePane::Jobs => {
                                            spawn_fetch_jobs(app, tx);
                                            app.last_refresh = Instant::now();
                                        }
                                        ActivePane::Workers => {
                                            spawn_fetch_workers(app, tx);
                                            app.last_refresh = Instant::now();
                                        }
                                        _ => {
                                            // Refresh everything
                                            app.start_loading("Refreshing...");
                                            if let Err(e) = app.fetch_datasets().await {
                                                let _ =
                                                    tx.send(AppEvent::Error(e.to_string())).await;
                                            } else {
                                                spawn_fetch_manifest(app, tx.clone());
                                                if app.is_local() {
                                                    spawn_fetch_jobs(app, tx.clone());
                                                    spawn_fetch_workers(app, tx);
                                                    app.last_refresh = Instant::now();
                                                }
                                            }
                                        }
                                    }
                                }

                                // Stop job (s key in Jobs pane)
                                KeyCode::Char('s') if app.active_pane == ActivePane::Jobs => {
                                    if let Some(job) = app.get_selected_job()
                                        && App::can_stop_job(&job.status)
                                    {
                                        let job_id = job.id;
                                        app.start_loading("Stopping job...");
                                        spawn_stop_job(app, job_id, tx.clone());
                                    }
                                }

                                // Delete job (d key, only if not Ctrl+d)
                                KeyCode::Char('d')
                                    if !key.modifiers.contains(KeyModifiers::CONTROL) =>
                                {
                                    if app.active_pane == ActivePane::Jobs
                                        && let Some(job) = app.get_selected_job()
                                        && App::is_job_terminal(&job.status)
                                    {
                                        let job_id = job.id;
                                        app.start_loading("Deleting job...");
                                        spawn_delete_job(app, job_id, tx.clone());
                                    }
                                }

                                // Navigation
                                KeyCode::Down | KeyCode::Char('j') => match app.active_pane {
                                    ActivePane::Datasets => {
                                        app.select_next();
                                        app.start_loading("Loading manifest...");
                                        spawn_fetch_manifest(app, tx.clone());
                                    }
                                    ActivePane::Jobs => {
                                        app.select_next_job();
                                        // Show job details in content pane
                                        if let Some(job) = app.get_selected_job().cloned() {
                                            app.content_view = ContentView::Job(job);
                                            app.reset_scroll();
                                        }
                                    }
                                    ActivePane::Workers => {
                                        app.select_next_worker();
                                        // Fetch and show worker details
                                        if let Some(node_id) =
                                            app.get_selected_worker().map(|w| w.node_id.clone())
                                        {
                                            app.start_loading("Loading worker details...");
                                            spawn_fetch_worker_detail(app, &node_id, tx.clone());
                                        }
                                    }
                                    _ => app.scroll_down(),
                                },
                                KeyCode::Up | KeyCode::Char('k') => match app.active_pane {
                                    ActivePane::Datasets => {
                                        app.select_previous();
                                        app.start_loading("Loading manifest...");
                                        spawn_fetch_manifest(app, tx.clone());
                                    }
                                    ActivePane::Jobs => {
                                        app.select_previous_job();
                                        // Show job details in content pane
                                        if let Some(job) = app.get_selected_job().cloned() {
                                            app.content_view = ContentView::Job(job);
                                            app.reset_scroll();
                                        }
                                    }
                                    ActivePane::Workers => {
                                        app.select_previous_worker();
                                        // Fetch and show worker details
                                        if let Some(node_id) =
                                            app.get_selected_worker().map(|w| w.node_id.clone())
                                        {
                                            app.start_loading("Loading worker details...");
                                            spawn_fetch_worker_detail(app, &node_id, tx.clone());
                                        }
                                    }
                                    _ => app.scroll_up(),
                                },

                                // Page navigation (vim-style)
                                KeyCode::Char('u')
                                    if key.modifiers.contains(KeyModifiers::CONTROL) =>
                                {
                                    app.page_up(10);
                                }
                                KeyCode::Char('d')
                                    if key.modifiers.contains(KeyModifiers::CONTROL) =>
                                {
                                    app.page_down(10);
                                }

                                // Expand/collapse or show details
                                KeyCode::Enter => {
                                    let tx = tx.clone();
                                    match app.active_pane {
                                        ActivePane::Datasets => {
                                            app.start_loading("Expanding...");
                                            if let Err(e) = app.toggle_expand().await {
                                                let _ =
                                                    tx.send(AppEvent::Error(e.to_string())).await;
                                            }
                                            app.stop_loading();
                                        }
                                        ActivePane::Jobs => {
                                            // Show job details in content pane and focus Detail
                                            if let Some(job) = app.get_selected_job().cloned() {
                                                app.content_view = ContentView::Job(job);
                                                app.reset_scroll();
                                                app.active_pane = ActivePane::Detail;
                                            }
                                        }
                                        ActivePane::Workers => {
                                            // Fetch worker details and focus Detail
                                            if let Some(node_id) =
                                                app.get_selected_worker().map(|w| w.node_id.clone())
                                            {
                                                app.start_loading("Loading worker details...");
                                                spawn_fetch_worker_detail(app, &node_id, tx);
                                                app.active_pane = ActivePane::Detail;
                                            }
                                        }
                                        _ => {}
                                    }
                                }

                                // Pane navigation
                                KeyCode::Tab => {
                                    let is_local = app.is_local();
                                    app.active_pane = app.active_pane.next(is_local);
                                }
                                KeyCode::BackTab => {
                                    let is_local = app.is_local();
                                    app.active_pane = app.active_pane.prev(is_local);
                                }

                                // Export query results to CSV (E key in QueryResult pane)
                                KeyCode::Char('E') | KeyCode::Char('e')
                                    if app.active_pane == ActivePane::QueryResult =>
                                {
                                    if let Some(results) = &app.query_results {
                                        match export_results_csv(results) {
                                            Ok(filename) => {
                                                app.set_success_message(format!(
                                                    "Exported {} rows to {}",
                                                    results.row_count, filename
                                                ));
                                            }
                                            Err(e) => {
                                                app.error_message =
                                                    Some(format!("Export failed: {}", e));
                                            }
                                        }
                                    } else {
                                        app.error_message =
                                            Some("No results to export".to_string());
                                    }
                                }

                                // Sort query results (s key in QueryResult pane)
                                KeyCode::Char('s')
                                    if app.active_pane == ActivePane::QueryResult
                                        && app.query_results.is_some() =>
                                {
                                    app.result_sort_pending = true;
                                }

                                // Clear sort (S key in QueryResult pane)
                                KeyCode::Char('S')
                                    if app.active_pane == ActivePane::QueryResult =>
                                {
                                    app.clear_sort();
                                }

                                // Column selection for sorting (1-9 keys when sort pending)
                                KeyCode::Char(c @ '1'..='9')
                                    if app.result_sort_pending
                                        && app.active_pane == ActivePane::QueryResult =>
                                {
                                    let col = c.to_digit(10).unwrap() as usize - 1; // 0-indexed
                                    if let Some(results) = &app.query_results {
                                        if col < results.columns.len() {
                                            app.toggle_sort(col);
                                        } else {
                                            app.result_sort_pending = false;
                                        }
                                    }
                                }

                                // Cancel sort mode with Escape
                                KeyCode::Esc if app.result_sort_pending => {
                                    app.result_sort_pending = false;
                                }

                                _ => {}
                            }
                        }
                        InputMode::Search => match key.code {
                            KeyCode::Enter => {
                                app.input_mode = InputMode::Normal;
                                app.start_loading("Loading manifest...");
                                spawn_fetch_manifest(app, tx.clone());
                            }
                            KeyCode::Esc => {
                                app.input_mode = InputMode::Normal;
                            }
                            KeyCode::Char(c) => {
                                app.search_query.push(c);
                                app.update_search();
                            }
                            KeyCode::Backspace => {
                                app.search_query.pop();
                                app.update_search();
                            }
                            _ => {}
                        },
                        InputMode::Query => match key.code {
                            KeyCode::Enter if key.modifiers.contains(KeyModifiers::CONTROL) => {
                                // Execute query with Ctrl+Enter
                                let sql = app.query_input.clone();
                                if !sql.trim().is_empty() {
                                    // Add to history (avoid consecutive duplicates)
                                    let trimmed = sql.trim().to_string();
                                    if app.query_history.last() != Some(&trimmed) {
                                        app.query_history.push(trimmed);
                                        // Cap history size at 100 entries
                                        const MAX_HISTORY: usize = 100;
                                        if app.query_history.len() > MAX_HISTORY {
                                            app.query_history.remove(0);
                                        }
                                    }
                                    // Reset history navigation state
                                    app.query_history_index = None;
                                    app.query_draft.clear();

                                    app.start_loading("Executing query...");
                                    spawn_execute_query(app, sql, tx.clone());
                                }
                            }
                            KeyCode::Enter => {
                                // Insert newline (plain Enter without Ctrl)
                                app.query_history_index = None;
                                app.insert_char('\n');
                            }
                            KeyCode::Esc => {
                                // Cancel query input, return to normal mode
                                app.input_mode = InputMode::Normal;
                                app.active_pane = ActivePane::Datasets;
                                // Restore previous content view if we have results
                                if app.query_results.is_none() {
                                    app.content_view = ContentView::Dataset;
                                }
                            }
                            KeyCode::Up => {
                                // First try to move cursor up in multiline input
                                if !app.cursor_up() {
                                    // At first line, navigate history
                                    if !app.query_history.is_empty() {
                                        match app.query_history_index {
                                            None => {
                                                // Save current input as draft, load most recent history
                                                app.query_draft = app.query_input.clone();
                                                let last_idx = app.query_history.len() - 1;
                                                app.query_history_index = Some(last_idx);
                                                app.set_query_input(
                                                    app.query_history[last_idx].clone(),
                                                );
                                            }
                                            Some(idx) if idx > 0 => {
                                                // Move to older entry
                                                let new_idx = idx - 1;
                                                app.query_history_index = Some(new_idx);
                                                app.set_query_input(
                                                    app.query_history[new_idx].clone(),
                                                );
                                            }
                                            Some(_) => {
                                                // Already at oldest entry, do nothing
                                            }
                                        }
                                    }
                                }
                            }
                            KeyCode::Down => {
                                // First try to move cursor down in multiline input
                                if !app.cursor_down() {
                                    // At last line, navigate history
                                    if let Some(idx) = app.query_history_index {
                                        if idx < app.query_history.len() - 1 {
                                            // Move to newer entry
                                            let new_idx = idx + 1;
                                            app.query_history_index = Some(new_idx);
                                            app.set_query_input(
                                                app.query_history[new_idx].clone(),
                                            );
                                        } else {
                                            // At newest entry, restore draft
                                            app.query_history_index = None;
                                            app.set_query_input(app.query_draft.clone());
                                        }
                                    }
                                }
                            }
                            KeyCode::Char('T') | KeyCode::Char('t')
                                if key.modifiers.is_empty()
                                    || key.modifiers == KeyModifiers::SHIFT =>
                            {
                                // Open template picker (only in query mode, only 'T' or 't')
                                app.template_picker_open = true;
                                app.template_picker_index = 0;
                            }
                            KeyCode::Char('f') if key.modifiers == KeyModifiers::CONTROL => {
                                // Open favorites panel with Ctrl+F
                                if !app.favorite_queries.is_empty() {
                                    app.favorites_panel_open = true;
                                    app.favorites_panel_index = 0;
                                }
                            }
                            KeyCode::Char('*') | KeyCode::Char('F')
                                if key.modifiers.is_empty()
                                    || key.modifiers == KeyModifiers::SHIFT =>
                            {
                                // Toggle favorite for current query
                                app.toggle_favorite();
                            }
                            KeyCode::Char(c) => {
                                // Reset history navigation on edit
                                app.query_history_index = None;
                                app.insert_char(c);
                            }
                            KeyCode::Backspace => {
                                // Reset history navigation on edit
                                app.query_history_index = None;
                                app.backspace();
                            }
                            KeyCode::Delete => {
                                // Reset history navigation on edit
                                app.query_history_index = None;
                                app.delete_char();
                            }
                            KeyCode::Left => {
                                app.cursor_left();
                            }
                            KeyCode::Right => {
                                app.cursor_right();
                            }
                            KeyCode::Home => {
                                app.cursor_home();
                            }
                            KeyCode::End => {
                                app.cursor_end();
                            }
                            _ => {}
                        },
                    }
                }
                Event::Mouse(mouse) => {
                    // TODO: refactor into components (test intersections)
                    if let MouseEventKind::Down(crossterm::event::MouseButton::Left) = mouse.kind {
                        // Calculate pane areas to determine which pane was clicked
                        let term_size = terminal.size()?;
                        let size = Rect::new(0, 0, term_size.width, term_size.height);

                        // Top-level layout: Header (3) | Main | Footer (1)
                        let main_chunks = Layout::default()
                            .direction(Direction::Vertical)
                            .constraints([
                                Constraint::Length(3),
                                Constraint::Min(0),
                                Constraint::Length(1),
                            ])
                            .split(size);

                        let header_area = main_chunks[0];
                        let main_area = main_chunks[1];

                        // Check which pane was clicked
                        let x = mouse.column;
                        let y = mouse.row;

                        // Check if header was clicked
                        if x >= header_area.x
                            && x < header_area.x + header_area.width
                            && y >= header_area.y
                            && y < header_area.y + header_area.height
                        {
                            app.active_pane = ActivePane::Header;
                        } else if app.active_pane == ActivePane::Header {
                            // If on splash and clicking main area, go to Datasets
                            if y >= main_area.y && y < main_area.y + main_area.height {
                                app.active_pane = ActivePane::Datasets;
                            }
                        } else {
                            // Normal pane detection
                            // Main layout: Sidebar (35%) | Content (65%)
                            let content_chunks = Layout::default()
                                .direction(Direction::Horizontal)
                                .constraints([
                                    Constraint::Percentage(35),
                                    Constraint::Percentage(65),
                                ])
                                .split(main_area);

                            let sidebar_area = content_chunks[0];
                            let content_area = content_chunks[1];

                            if x >= sidebar_area.x
                                && x < sidebar_area.x + sidebar_area.width
                                && y >= sidebar_area.y
                                && y < sidebar_area.y + sidebar_area.height
                            {
                                // Clicked in sidebar - determine which section
                                if app.is_local() {
                                    let sidebar_chunks = Layout::default()
                                        .direction(Direction::Vertical)
                                        .constraints([
                                            Constraint::Percentage(50), // Datasets
                                            Constraint::Percentage(25), // Jobs
                                            Constraint::Percentage(25), // Workers
                                        ])
                                        .split(sidebar_area);

                                    let datasets_area = sidebar_chunks[0];
                                    let jobs_area = sidebar_chunks[1];
                                    let workers_area = sidebar_chunks[2];

                                    if y >= datasets_area.y
                                        && y < datasets_area.y + datasets_area.height
                                    {
                                        app.active_pane = ActivePane::Datasets;
                                    } else if y >= jobs_area.y && y < jobs_area.y + jobs_area.height
                                    {
                                        app.active_pane = ActivePane::Jobs;
                                    } else if y >= workers_area.y
                                        && y < workers_area.y + workers_area.height
                                    {
                                        app.active_pane = ActivePane::Workers;
                                    }
                                } else {
                                    // Registry mode - only Datasets pane in sidebar
                                    app.active_pane = ActivePane::Datasets;
                                }
                            } else if x >= content_area.x
                                && x < content_area.x + content_area.width
                                && y >= content_area.y
                                && y < content_area.y + content_area.height
                            {
                                // Clicked in content area
                                match app.content_view {
                                    ContentView::Dataset => {
                                        let content_chunks = Layout::default()
                                            .direction(Direction::Vertical)
                                            .constraints([
                                                Constraint::Percentage(60), // Manifest
                                                Constraint::Percentage(40), // Schema
                                            ])
                                            .split(content_area);

                                        let manifest_area = content_chunks[0];
                                        let schema_area = content_chunks[1];

                                        if y >= manifest_area.y
                                            && y < manifest_area.y + manifest_area.height
                                        {
                                            app.active_pane = ActivePane::Manifest;
                                        } else if y >= schema_area.y
                                            && y < schema_area.y + schema_area.height
                                        {
                                            app.active_pane = ActivePane::Schema;
                                        }
                                    }
                                    ContentView::QueryResults => {
                                        // Dynamic height for query input
                                        let query_height =
                                            (app.query_line_count() as u16 + 2).clamp(3, 10);
                                        let query_chunks = Layout::default()
                                            .direction(Direction::Vertical)
                                            .constraints([
                                                Constraint::Length(query_height), // Query input
                                                Constraint::Min(0),               // Results
                                            ])
                                            .split(content_area);

                                        let query_input_area = query_chunks[0];
                                        let query_results_area = query_chunks[1];

                                        if y >= query_input_area.y
                                            && y < query_input_area.y + query_input_area.height
                                        {
                                            app.active_pane = ActivePane::Query;
                                        } else if y >= query_results_area.y
                                            && y < query_results_area.y + query_results_area.height
                                        {
                                            app.active_pane = ActivePane::QueryResult;
                                        }
                                    }
                                    ContentView::Job(_)
                                    | ContentView::Worker(_)
                                    | ContentView::None => {
                                        app.active_pane = ActivePane::Detail;
                                    }
                                }
                            }
                        }
                    }
                }
                _ => {}
            }
            app.needs_redraw = true;
        }

        // Only draw when needed (reduces CPU usage from ~20% to near 0% when idle)
        if app.needs_redraw {
            terminal.draw(|f| ui::draw(f, app))?;
            app.needs_redraw = false;
        }

        if app.should_quit {
            // Save history and favorites before quitting
            if let Err(e) = app.save_history() {
                eprintln!("Warning: could not save history: {}", e);
            }
            app.save_favorites();
            return Ok(());
        }
    }
}

fn spawn_fetch_manifest(app: &App, tx: mpsc::Sender<AppEvent>) {
    let Some((namespace, name, version)) = app.get_selected_manifest_params() else {
        return;
    };

    match app.current_source {
        DataSource::Local => {
            let client = app.local_client.clone();
            tokio::spawn(async move {
                use datasets_common::{reference::Reference, revision::Revision};
                let revision: Option<Revision> = version.parse().ok();
                match (namespace.parse(), name.parse(), revision) {
                    (Ok(ns), Ok(n), Some(rev)) => {
                        let reference = Reference::new(ns, n, rev);
                        match client.datasets().get_manifest(&reference).await {
                            Ok(manifest) => {
                                let _ = tx.send(AppEvent::ManifestLoaded(manifest)).await;
                            }
                            Err(e) => {
                                let _ = tx
                                    .send(AppEvent::Error(format!("Failed to fetch: {}", e)))
                                    .await;
                            }
                        }
                    }
                    _ => {
                        let _ = tx.send(AppEvent::ManifestLoaded(None)).await;
                    }
                }
            });
        }
        DataSource::Registry => {
            let client = app.registry_client.clone();
            tokio::spawn(async move {
                match client.get_manifest(&namespace, &name, &version).await {
                    Ok(manifest) => {
                        let _ = tx.send(AppEvent::ManifestLoaded(Some(manifest))).await;
                    }
                    Err(e) => {
                        let _ = tx
                            .send(AppEvent::Error(format!("Failed to fetch: {}", e)))
                            .await;
                    }
                }
            });
        }
    }
}

fn spawn_fetch_jobs(app: &App, tx: mpsc::Sender<AppEvent>) {
    let client = app.local_client.clone();
    tokio::spawn(async move {
        match client.jobs().list(None, None, None).await {
            Ok(response) => {
                let _ = tx.send(AppEvent::JobsLoaded(response.jobs)).await;
            }
            Err(e) => {
                let _ = tx
                    .send(AppEvent::Error(format!("Failed to fetch jobs: {}", e)))
                    .await;
            }
        }
    });
}

fn spawn_fetch_workers(app: &App, tx: mpsc::Sender<AppEvent>) {
    let client = app.local_client.clone();
    tokio::spawn(async move {
        match client.workers().list().await {
            Ok(response) => {
                let _ = tx.send(AppEvent::WorkersLoaded(response.workers)).await;
            }
            Err(e) => {
                let _ = tx
                    .send(AppEvent::Error(format!("Failed to fetch workers: {}", e)))
                    .await;
            }
        }
    });
}

fn spawn_fetch_worker_detail(app: &App, node_id: &str, tx: mpsc::Sender<AppEvent>) {
    let client = app.local_client.clone();
    let node_id: NodeId = match node_id.parse() {
        Ok(id) => id,
        Err(e) => {
            let tx_clone = tx.clone();
            tokio::spawn(async move {
                let _ = tx_clone
                    .send(AppEvent::Error(format!("Invalid node ID: {}", e)))
                    .await;
            });
            return;
        }
    };
    tokio::spawn(async move {
        match client.workers().get(&node_id).await {
            Ok(Some(detail)) => {
                let _ = tx.send(AppEvent::WorkerDetailLoaded(Some(detail))).await;
            }
            Ok(None) => {
                let _ = tx
                    .send(AppEvent::Error("Worker not found".to_string()))
                    .await;
            }
            Err(e) => {
                let _ = tx
                    .send(AppEvent::Error(format!(
                        "Failed to fetch worker details: {}",
                        e
                    )))
                    .await;
            }
        }
    });
}

fn spawn_stop_job(app: &App, job_id: JobId, tx: mpsc::Sender<AppEvent>) {
    let client = app.local_client.clone();
    tokio::spawn(async move {
        match client.jobs().stop(&job_id).await {
            Ok(()) => {
                let _ = tx.send(AppEvent::JobStopped(Ok(()))).await;
            }
            Err(e) => {
                let _ = tx
                    .send(AppEvent::JobStopped(Err(format!(
                        "Failed to stop job: {}",
                        e
                    ))))
                    .await;
            }
        }
    });
}

fn spawn_delete_job(app: &App, job_id: JobId, tx: mpsc::Sender<AppEvent>) {
    let client = app.local_client.clone();
    tokio::spawn(async move {
        match client.jobs().delete_by_id(&job_id).await {
            Ok(()) => {
                let _ = tx.send(AppEvent::JobDeleted(Ok(()))).await;
            }
            Err(e) => {
                let _ = tx
                    .send(AppEvent::JobDeleted(Err(format!(
                        "Failed to delete job: {}",
                        e
                    ))))
                    .await;
            }
        }
    });
}

fn spawn_execute_query(app: &App, sql: String, tx: mpsc::Sender<AppEvent>) {
    let query_url = app.config.local_query_url.clone();

    tokio::spawn(async move {
        let results = execute_query(&query_url, &sql).await;
        let _ = tx.send(AppEvent::QueryCompleted(results)).await;
    });
}

async fn execute_query(query_url: &str, sql: &str) -> QueryResults {
    // Connect to Flight service
    let mut client = match amp_client::AmpClient::from_endpoint(query_url).await {
        Ok(c) => c,
        Err(e) => {
            return QueryResults {
                columns: vec![],
                rows: vec![],
                row_count: 0,
                error: Some(format!("Connection failed: {}", e)),
            };
        }
    };

    // Execute query
    let mut stream = match client.query(sql).await {
        Ok(s) => s,
        Err(e) => {
            return QueryResults {
                columns: vec![],
                rows: vec![],
                row_count: 0,
                error: Some(format!("Query failed: {}", e)),
            };
        }
    };

    // Collect results
    let mut columns: Option<Vec<String>> = None;
    let mut rows: Vec<Vec<String>> = Vec::new();

    while let Some(batch_result) = stream.next().await {
        match batch_result {
            Ok(batch) => {
                // Extract column names from schema (once)
                if columns.is_none() {
                    columns = Some(
                        batch
                            .schema()
                            .fields()
                            .iter()
                            .map(|f| f.name().clone())
                            .collect(),
                    );
                }

                // Convert batch to rows
                for row_idx in 0..batch.num_rows() {
                    let row: Vec<String> = (0..batch.num_columns())
                        .map(|col_idx| format_array_value(batch.column(col_idx).as_ref(), row_idx))
                        .collect();
                    rows.push(row);
                }
            }
            Err(e) => {
                let row_count = rows.len();
                return QueryResults {
                    columns: columns.unwrap_or_default(),
                    rows,
                    row_count,
                    error: Some(format!("Fetch error: {}", e)),
                };
            }
        }
    }

    let row_count = rows.len();
    QueryResults {
        columns: columns.unwrap_or_default(),
        rows,
        row_count,
        error: None,
    }
}

fn format_array_value(array: &dyn Array, idx: usize) -> String {
    if array.is_null(idx) {
        return "NULL".to_string();
    }

    // Handle common types
    if let Some(arr) = array.as_any().downcast_ref::<StringArray>() {
        return arr.value(idx).to_string();
    }
    if let Some(arr) = array.as_any().downcast_ref::<Int64Array>() {
        return arr.value(idx).to_string();
    }
    if let Some(arr) = array.as_any().downcast_ref::<UInt64Array>() {
        return arr.value(idx).to_string();
    }
    if let Some(arr) = array.as_any().downcast_ref::<Float64Array>() {
        return format!("{:.6}", arr.value(idx));
    }
    if let Some(arr) = array.as_any().downcast_ref::<BooleanArray>() {
        return arr.value(idx).to_string();
    }
    // Binary types - show hex
    if let Some(arr) = array.as_any().downcast_ref::<BinaryArray>() {
        let bytes = arr.value(idx);
        if bytes.len() > 16 {
            return format!("0x{}...", hex::encode(&bytes[..16]));
        }
        return format!("0x{}", hex::encode(bytes));
    }

    // Fallback: use array_value_to_string from Arrow
    arrow::util::display::array_value_to_string(array, idx).unwrap_or_else(|_| "?".to_string())
}

/// Export query results to a CSV file.
///
/// Returns the filename on success, or an error message on failure.
fn export_results_csv(results: &QueryResults) -> Result<String, String> {
    let timestamp = chrono::Local::now().format("%Y%m%d_%H%M%S");
    let filename = format!("query_results_{}.csv", timestamp);

    let mut wtr =
        csv::Writer::from_path(&filename).map_err(|e| format!("Failed to create file: {}", e))?;

    // Write header
    wtr.write_record(&results.columns)
        .map_err(|e| format!("Failed to write header: {}", e))?;

    // Write rows
    for row in &results.rows {
        wtr.write_record(row)
            .map_err(|e| format!("Failed to write row: {}", e))?;
    }

    wtr.flush().map_err(|e| format!("Failed to flush: {}", e))?;

    Ok(filename)
}
