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
use crossterm::{
    event::{
        self, DisableMouseCapture, EnableMouseCapture, Event, KeyCode, KeyModifiers, MouseEventKind,
    },
    execute,
    terminal::{EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode},
};
use ratatui::{Terminal, backend::CrosstermBackend};
use tokio::sync::mpsc;
use worker::{job::JobId, node_id::NodeId};

mod app;
mod config;
mod registry;
mod ui;

use app::{ActivePane, App, ContentView, DataSource, InputMode, InspectResult};
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
    Error(String),
}

#[tokio::main]
async fn main() -> Result<()> {
    let config = config::Config::load()?;
    let mut app = App::new(config)?;

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
) -> Result<()> {
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

                                // Stop job (s key)
                                KeyCode::Char('s') => {
                                    if app.active_pane == ActivePane::Jobs
                                        && let Some(job) = app.get_selected_job()
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
