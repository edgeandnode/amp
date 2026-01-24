//! Amp CC - Terminal UI for managing Amp datasets.

use std::{
    io,
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::Result;
use crossterm::{
    event::{
        self, DisableMouseCapture, EnableMouseCapture, Event, KeyCode, KeyModifiers, MouseEventKind,
    },
    execute,
    terminal::{EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode},
};
use datasets_common::{name::Name, namespace::Namespace, revision::Revision};
use ratatui::{Terminal, backend::CrosstermBackend};
use reqwest::Client as HttpClient;
use tokio::sync::mpsc;

mod action;
mod amp_registry;
mod app;
mod auth;
mod config;
mod ui;
mod util;

use action::Action;
use amp_registry::AmpRegistryClient;
use app::{ActivePane, App, ContentView, DataSource, DeviceFlowStatus, InputMode};
use ratatui::layout::{Constraint, Direction, Layout, Rect};

/// Auto-refresh interval for jobs/workers (10 seconds).
const REFRESH_INTERVAL: Duration = Duration::from_secs(10);

/// Tick rate for UI updates.
const TICK_RATE: Duration = Duration::from_millis(100);

#[tokio::main]
async fn main() -> Result<()> {
    let config = config::Config::load()?;

    // Create shared HTTP client
    let http_client = HttpClient::new();

    // Create AMP registry client
    let amp_registry_client = Arc::new(AmpRegistryClient::new(
        http_client.clone(),
        &config.registry_url,
    ));

    // Create action channel
    let (action_tx, action_rx) = mpsc::unbounded_channel::<Action>();

    // Create app
    let mut app = App::new(config, action_tx.clone(), http_client, amp_registry_client)?;

    // Setup terminal
    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen, EnableMouseCapture)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    // Run app
    let res = run_app(&mut terminal, &mut app, action_tx, action_rx).await;

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
    action_tx: mpsc::UnboundedSender<Action>,
    mut action_rx: mpsc::UnboundedReceiver<Action>,
) -> Result<()>
where
    B::Error: Send + Sync + 'static,
{
    // Initial data loads
    let _ = action_tx.send(Action::RefreshDatasets);
    let _ = action_tx.send(Action::AuthCheckOnStartup);

    // If in Local mode, also fetch jobs and workers
    if app.is_local() {
        let _ = action_tx.send(Action::RefreshJobs);
        let _ = action_tx.send(Action::RefreshWorkers);
    }

    // Spawn async tasks for initial loads
    spawn_fetch_datasets(app);
    spawn_check_auth(app);
    if app.is_local() {
        spawn_fetch_jobs(app);
        spawn_fetch_workers(app);
    }

    let mut last_tick = Instant::now();

    loop {
        // Tick spinner animation (only when loading)
        if app.loading {
            app.tick_spinner();
            app.needs_redraw = true;
        }

        // Auto-refresh jobs/workers in Local mode
        if app.is_local() && app.last_refresh.elapsed() >= REFRESH_INTERVAL {
            spawn_fetch_jobs(app);
            spawn_fetch_workers(app);
            app.last_refresh = Instant::now();
        }

        // Draw if needed
        if app.needs_redraw {
            terminal.draw(|f| ui::draw(f, app))?;
            app.needs_redraw = false;
        }

        // Calculate timeout for event polling
        let timeout = TICK_RATE
            .checked_sub(last_tick.elapsed())
            .unwrap_or(Duration::ZERO);

        // Handle events using tokio::select!
        tokio::select! {
            // Check for terminal events
            _ = tokio::time::sleep(timeout) => {
                // Check for terminal events (non-blocking)
                if event::poll(Duration::ZERO)? {
                    let action = handle_terminal_event(app, terminal, event::read()?)?;
                    if action_tx.send(action).is_err() {
                        break;
                    }
                }

                // Tick
                if last_tick.elapsed() >= TICK_RATE {
                    last_tick = Instant::now();
                }
            }

            // Handle actions from channel
            Some(action) = action_rx.recv() => {
                // Handle actions that need to spawn async tasks
                handle_async_action(app, &action);

                // Handle state mutations
                app.handle_action(action);
            }
        }

        if !app.is_running() {
            break;
        }
    }

    Ok(())
}

/// Handle terminal events and convert to actions.
fn handle_terminal_event<B: ratatui::backend::Backend>(
    app: &mut App,
    terminal: &Terminal<B>,
    event: Event,
) -> Result<Action>
where
    B::Error: Send + Sync + 'static,
{
    match event {
        Event::Key(key) => Ok(handle_key_event(app, key)),
        Event::Mouse(mouse) => Ok(handle_mouse_event(app, terminal, mouse)?),
        _ => Ok(Action::None),
    }
}

/// Handle key events and return the corresponding action.
fn handle_key_event(app: &App, key: event::KeyEvent) -> Action {
    // If device flow modal is active, handle it first
    if let Some(ref flow) = app.auth_device_flow {
        return handle_device_flow_key(flow, key);
    }

    match app.input_mode {
        InputMode::Normal => handle_normal_mode_key(app, key),
        InputMode::Search => handle_search_mode_key(key),
    }
}

/// Handle key events when device flow modal is active.
fn handle_device_flow_key(flow: &app::DeviceFlowState, key: event::KeyEvent) -> Action {
    match key.code {
        KeyCode::Enter => {
            // Only confirm if awaiting confirmation
            if matches!(flow.status, DeviceFlowStatus::AwaitingConfirmation) {
                Action::AuthDeviceFlowConfirm
            } else {
                Action::None
            }
        }
        KeyCode::Esc => Action::AuthDeviceFlowCancel,
        _ => Action::None,
    }
}

/// Handle key events in normal mode.
fn handle_normal_mode_key(app: &App, key: event::KeyEvent) -> Action {
    match key.code {
        KeyCode::Char('q') => Action::Quit,
        KeyCode::Char('c') if key.modifiers == KeyModifiers::CONTROL => Action::Quit,

        // Source switching
        KeyCode::Char('1') => Action::SwitchToLocal,
        KeyCode::Char('2') => Action::SwitchToRegistry,

        // Search
        KeyCode::Char('/') => Action::EnterSearchMode,

        // Refresh
        KeyCode::Char('r') => match app.active_pane {
            ActivePane::Datasets => Action::RefreshDatasets,
            ActivePane::Jobs => Action::RefreshJobs,
            ActivePane::Workers => Action::RefreshWorkers,
            _ => Action::RefreshDatasets,
        },

        // Filter toggle (Registry mode only)
        KeyCode::Char('f') => Action::ToggleDatasetsFilter,

        // Stop job
        KeyCode::Char('s') => {
            if app.active_pane == ActivePane::Jobs
                && let Some(job) = app.get_selected_job()
                && App::can_stop_job(&job.status)
            {
                return Action::StopJob(job.id);
            }
            Action::None
        }

        // Delete job
        KeyCode::Char('d') if !key.modifiers.contains(KeyModifiers::CONTROL) => {
            if app.active_pane == ActivePane::Jobs
                && let Some(job) = app.get_selected_job()
                && App::is_job_terminal(&job.status)
            {
                return Action::DeleteJob(job.id);
            }
            Action::None
        }

        // Navigation
        KeyCode::Down | KeyCode::Char('j') => Action::NavigateDown,
        KeyCode::Up | KeyCode::Char('k') => Action::NavigateUp,

        // Page navigation
        KeyCode::Char('u') if key.modifiers.contains(KeyModifiers::CONTROL) => Action::PageUp(10),
        KeyCode::Char('d') if key.modifiers.contains(KeyModifiers::CONTROL) => Action::PageDown(10),

        // Expand/collapse or enter detail
        KeyCode::Enter => match app.active_pane {
            ActivePane::Datasets => Action::ToggleExpand,
            ActivePane::Jobs | ActivePane::Workers => Action::EnterDetail,
            _ => Action::None,
        },

        // Pane navigation
        KeyCode::Tab => Action::NextPane,
        KeyCode::BackTab => Action::PrevPane,

        // Auth - Ctrl+L toggles login/logout based on current state
        KeyCode::Char('l') if key.modifiers.contains(KeyModifiers::CONTROL) => {
            if app.auth_state.is_some() {
                Action::AuthLogout
            } else {
                Action::AuthLogin
            }
        }

        // Clear search (only when search is active)
        KeyCode::Esc => {
            if !app.search_query.is_empty() {
                Action::ClearSearch
            } else {
                Action::None
            }
        }

        _ => Action::None,
    }
}

/// Handle key events in search mode.
fn handle_search_mode_key(key: event::KeyEvent) -> Action {
    match key.code {
        KeyCode::Enter => Action::SearchSubmit,
        KeyCode::Esc => Action::ExitSearchMode,
        KeyCode::Char(c) => Action::SearchInput(c),
        KeyCode::Backspace => Action::SearchBackspace,
        _ => Action::None,
    }
}

/// Handle mouse events and return the corresponding action.
fn handle_mouse_event<B: ratatui::backend::Backend>(
    app: &mut App,
    terminal: &Terminal<B>,
    mouse: event::MouseEvent,
) -> Result<Action>
where
    B::Error: Send + Sync + 'static,
{
    if let MouseEventKind::Down(crossterm::event::MouseButton::Left) = mouse.kind {
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
            if y >= main_area.y && y < main_area.y + main_area.height {
                app.active_pane = ActivePane::Datasets;
            }
        } else {
            // Normal pane detection
            let content_chunks = Layout::default()
                .direction(Direction::Horizontal)
                .constraints([Constraint::Percentage(35), Constraint::Percentage(65)])
                .split(main_area);

            let sidebar_area = content_chunks[0];
            let content_area = content_chunks[1];

            if x >= sidebar_area.x
                && x < sidebar_area.x + sidebar_area.width
                && y >= sidebar_area.y
                && y < sidebar_area.y + sidebar_area.height
            {
                // Clicked in sidebar
                if app.is_local() {
                    let sidebar_chunks = Layout::default()
                        .direction(Direction::Vertical)
                        .constraints([
                            Constraint::Percentage(50),
                            Constraint::Percentage(25),
                            Constraint::Percentage(25),
                        ])
                        .split(sidebar_area);

                    let datasets_area = sidebar_chunks[0];
                    let jobs_area = sidebar_chunks[1];
                    let workers_area = sidebar_chunks[2];

                    if y >= datasets_area.y && y < datasets_area.y + datasets_area.height {
                        app.active_pane = ActivePane::Datasets;
                    } else if y >= jobs_area.y && y < jobs_area.y + jobs_area.height {
                        app.active_pane = ActivePane::Jobs;
                    } else if y >= workers_area.y && y < workers_area.y + workers_area.height {
                        app.active_pane = ActivePane::Workers;
                    }
                } else {
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
                            .constraints([Constraint::Percentage(60), Constraint::Percentage(40)])
                            .split(content_area);

                        let manifest_area = content_chunks[0];
                        let schema_area = content_chunks[1];

                        if y >= manifest_area.y && y < manifest_area.y + manifest_area.height {
                            app.active_pane = ActivePane::Manifest;
                        } else if y >= schema_area.y && y < schema_area.y + schema_area.height {
                            app.active_pane = ActivePane::Schema;
                        }
                    }
                    ContentView::Job(_) | ContentView::Worker(_) | ContentView::None => {
                        app.active_pane = ActivePane::Detail;
                    }
                }
            }
        }
    }

    Ok(Action::None)
}

/// Handle actions that need to spawn async tasks.
fn handle_async_action(app: &App, action: &Action) {
    match action {
        Action::RefreshDatasets => spawn_fetch_datasets(app),
        Action::LoadManifest => spawn_fetch_manifest(app),
        Action::RefreshJobs => spawn_fetch_jobs(app),
        Action::RefreshWorkers => spawn_fetch_workers(app),
        Action::StopJob(job_id) => spawn_stop_job(app, *job_id),
        Action::DeleteJob(job_id) => spawn_delete_job(app, *job_id),
        Action::LoadWorkerDetail(node_id) => spawn_fetch_worker_detail(app, node_id),
        Action::SwitchToLocal => spawn_switch_source(app, DataSource::Local),
        Action::SwitchToRegistry => spawn_switch_source(app, DataSource::Registry),
        Action::ToggleExpand => spawn_toggle_expand(app),
        Action::SearchDatasets => spawn_search_datasets(app),
        Action::AuthCheckOnStartup => spawn_check_auth(app),
        Action::AuthLogin => spawn_start_device_flow(app),
        Action::AuthDeviceFlowPoll {
            device_code,
            code_verifier,
            interval,
            is_first_poll,
        } => spawn_poll_device_token(app, device_code, code_verifier, *interval, *is_first_poll),
        _ => {}
    }
}

// ============================================================================
// Async task spawners
// ============================================================================

fn spawn_fetch_datasets(app: &App) {
    let tx = app.action_tx.clone();
    let local_client = app.local_client.clone();
    let registry_client = app.amp_registry_client.clone();
    let source = app.current_source;
    let filter = app.datasets_filter;
    let access_token = app.get_access_token();

    tokio::spawn(async move {
        let result = match source {
            DataSource::Local => App::fetch_local_datasets(&local_client)
                .await
                .map_err(|e| e.to_string()),
            DataSource::Registry => {
                App::fetch_registry_datasets(&registry_client, filter, access_token.as_deref())
                    .await
                    .map_err(|e| e.to_string())
            }
        };
        let _ = tx.send(Action::DatasetsLoaded(result));
    });
}

fn spawn_search_datasets(app: &App) {
    let tx = app.action_tx.clone();
    let registry_client = app.amp_registry_client.clone();
    let filter = app.datasets_filter;
    let access_token = app.get_access_token();
    let query = app.search_query.clone();

    tokio::spawn(async move {
        let result = App::search_registry_datasets(
            &registry_client,
            &query,
            filter,
            access_token.as_deref(),
        )
        .await
        .map_err(|e| e.to_string());
        let _ = tx.send(Action::DatasetsLoaded(result));
    });
}

fn spawn_fetch_manifest(app: &App) {
    let Some((namespace, name, version)) = app.get_selected_manifest_params() else {
        return;
    };

    let tx = app.action_tx.clone();
    let source = app.current_source;

    match source {
        DataSource::Local => {
            let client = app.local_client.clone();
            tokio::spawn(async move {
                use datasets_common::reference::Reference;
                let revision: Option<Revision> = version.parse().ok();
                match (namespace.parse(), name.parse(), revision) {
                    (Ok(ns), Ok(n), Some(rev)) => {
                        let reference = Reference::new(ns, n, rev);
                        match client.datasets().get_manifest(&reference).await {
                            Ok(manifest) => {
                                let _ = tx.send(Action::ManifestLoadedLocalAdminApi(manifest));
                            }
                            Err(e) => {
                                let _ = tx.send(Action::Error(format!("Failed to fetch: {}", e)));
                            }
                        }
                    }
                    _ => {
                        let _ = tx.send(Action::ManifestLoadedLocalAdminApi(None));
                    }
                }
            });
        }
        DataSource::Registry => {
            let client = app.amp_registry_client.clone();
            tokio::spawn(async move {
                let ns: Namespace = match namespace.parse() {
                    Ok(ns) => ns,
                    Err(e) => {
                        let _ = tx.send(Action::Error(format!("Invalid namespace: {}", e)));
                        return;
                    }
                };
                let n: Name = match name.parse() {
                    Ok(n) => n,
                    Err(e) => {
                        let _ = tx.send(Action::Error(format!("Invalid name: {}", e)));
                        return;
                    }
                };
                let rev: Revision = match version.parse() {
                    Ok(rev) => rev,
                    Err(e) => {
                        let _ = tx.send(Action::Error(format!("Invalid revision: {}", e)));
                        return;
                    }
                };

                match client.fetch_dataset_version_manifest(ns, n, rev).await {
                    Ok(manifest) => {
                        let _ = tx.send(Action::ManifestLoadedAmpRegistry(Ok(manifest)));
                    }
                    Err(e) => {
                        let _ = tx.send(Action::ManifestLoadedAmpRegistry(Err(format!(
                            "Failed to fetch: {}",
                            e
                        ))));
                    }
                }
            });
        }
    }
}

fn spawn_fetch_jobs(app: &App) {
    let tx = app.action_tx.clone();
    let client = app.local_client.clone();

    tokio::spawn(async move {
        match client.jobs().list(None, None, None).await {
            Ok(response) => {
                let _ = tx.send(Action::JobsLoaded(response.jobs));
            }
            Err(e) => {
                let _ = tx.send(Action::Error(format!("Failed to fetch jobs: {}", e)));
            }
        }
    });
}

fn spawn_fetch_workers(app: &App) {
    let tx = app.action_tx.clone();
    let client = app.local_client.clone();

    tokio::spawn(async move {
        match client.workers().list().await {
            Ok(response) => {
                let _ = tx.send(Action::WorkersLoaded(response.workers));
            }
            Err(e) => {
                let _ = tx.send(Action::Error(format!("Failed to fetch workers: {}", e)));
            }
        }
    });
}

fn spawn_fetch_worker_detail(app: &App, node_id: &str) {
    use worker::node_id::NodeId;

    let tx = app.action_tx.clone();
    let client = app.local_client.clone();
    let node_id: NodeId = match node_id.parse() {
        Ok(id) => id,
        Err(e) => {
            let tx_clone = tx.clone();
            tokio::spawn(async move {
                let _ = tx_clone.send(Action::Error(format!("Invalid node ID: {}", e)));
            });
            return;
        }
    };

    tokio::spawn(async move {
        match client.workers().get(&node_id).await {
            Ok(Some(detail)) => {
                let _ = tx.send(Action::WorkerDetailLoaded(Some(detail)));
            }
            Ok(None) => {
                let _ = tx.send(Action::Error("Worker not found".to_string()));
            }
            Err(e) => {
                let _ = tx.send(Action::Error(format!(
                    "Failed to fetch worker details: {}",
                    e
                )));
            }
        }
    });
}

fn spawn_stop_job(app: &App, job_id: worker::job::JobId) {
    let tx = app.action_tx.clone();
    let client = app.local_client.clone();

    tokio::spawn(async move {
        match client.jobs().stop(&job_id).await {
            Ok(()) => {
                let _ = tx.send(Action::JobStopped(Ok(())));
            }
            Err(e) => {
                let _ = tx.send(Action::JobStopped(Err(format!(
                    "Failed to stop job: {}",
                    e
                ))));
            }
        }
    });
}

fn spawn_delete_job(app: &App, job_id: worker::job::JobId) {
    let tx = app.action_tx.clone();
    let client = app.local_client.clone();

    tokio::spawn(async move {
        match client.jobs().delete_by_id(&job_id).await {
            Ok(()) => {
                let _ = tx.send(Action::JobDeleted(Ok(())));
            }
            Err(e) => {
                let _ = tx.send(Action::JobDeleted(Err(format!(
                    "Failed to delete job: {}",
                    e
                ))));
            }
        }
    });
}

fn spawn_switch_source(app: &App, source: DataSource) {
    let tx = app.action_tx.clone();

    tokio::spawn(async move {
        // Source switching is essentially just updating state
        // The actual dataset fetch will happen via SourceSwitched -> RefreshDatasets
        let _ = tx.send(Action::SourceSwitched(Ok(source)));
    });
}

fn spawn_toggle_expand(app: &App) {
    // Get the current selection info before spawning
    let (dataset_idx, version_idx) = {
        let mut current = 0usize;
        let mut result = None;
        for (idx, dataset) in app.filtered_datasets.iter().enumerate() {
            if current == app.selected_index {
                result = Some((idx, None::<usize>));
                break;
            }
            current += 1;
            if dataset.expanded
                && let Some(versions) = &dataset.versions
            {
                for v_idx in 0..versions.len() {
                    if current == app.selected_index {
                        result = Some((idx, Some(v_idx)));
                        break;
                    }
                    current += 1;
                }
            }
            if result.is_some() {
                break;
            }
        }
        match result {
            Some(r) => r,
            None => return,
        }
    };

    // Only toggle if we're on a dataset, not a version
    if version_idx.is_some() {
        return;
    }

    let dataset = match app.filtered_datasets.get(dataset_idx) {
        Some(d) => d,
        None => return,
    };

    // If already expanded or has versions, just send action to toggle
    if dataset.expanded || dataset.versions.is_some() {
        // Toggle will be handled synchronously in handle_action
        // For now, we don't need async for collapse
        return;
    }

    // This code path is reached when `dataset.versions` is None, meaning:
    // - Local mode: Admin API doesn't provide version details, only version strings
    // - Registry mode: Dataset had no versions in API response (edge case)
    //
    // We send empty versions to trigger expand. The VersionsLoaded action will
    // set `versions = Some(vec![])` and `expanded = true`.
    let tx = app.action_tx.clone();
    let _ = tx.send(Action::VersionsLoaded {
        dataset_index: dataset_idx,
        versions: Ok(Vec::new()),
    });
}

// ============================================================================
// Auth task spawners
// ============================================================================

fn spawn_check_auth(app: &App) {
    use crate::auth::{AuthClient, AuthStorage};

    let tx = app.action_tx.clone();
    let http_client = app.http_client.clone();
    let auth_url = app.config.auth_url.clone();

    tokio::spawn(async move {
        // Try to load auth from disk
        let Some(auth) = AuthStorage::load() else {
            let _ = tx.send(Action::AuthStateLoaded(None));
            return;
        };

        // Check if we need to refresh
        if AuthClient::needs_refresh(&auth) {
            let client = AuthClient::new(http_client, auth_url);
            match client.refresh_token(&auth).await {
                Ok(new_auth) => {
                    let _ = tx.send(Action::AuthRefreshComplete(Ok(new_auth)));
                }
                Err(e) => {
                    let _ = tx.send(Action::AuthRefreshComplete(Err(e.to_string())));
                }
            }
        } else {
            let _ = tx.send(Action::AuthStateLoaded(Some(auth)));
        }
    });
}

fn spawn_start_device_flow(app: &App) {
    use crate::auth::PkceDeviceFlowClient;

    let tx = app.action_tx.clone();
    let http_client = app.http_client.clone();
    let auth_url = app.config.auth_url.clone();

    tokio::spawn(async move {
        let client = PkceDeviceFlowClient::new(http_client, auth_url);

        match client.request_authorization().await {
            Ok(result) => {
                let _ = tx.send(Action::AuthDeviceFlowPending {
                    user_code: result.response.user_code,
                    verification_uri: result.response.verification_uri,
                    device_code: result.response.device_code,
                    code_verifier: result.code_verifier,
                    interval: result.response.interval,
                });
            }
            Err(e) => {
                let _ = tx.send(Action::AuthError(e.to_string()));
            }
        }
    });
}

fn spawn_poll_device_token(
    app: &App,
    device_code: &str,
    code_verifier: &str,
    interval: i64,
    is_first_poll: bool,
) {
    use chrono::Utc;

    use crate::auth::{AuthError, PkceDeviceFlowClient};

    let tx = app.action_tx.clone();
    let http_client = app.http_client.clone();
    let auth_url = app.config.auth_url.clone();
    let device_code = device_code.to_string();
    let code_verifier = code_verifier.to_string();

    tokio::spawn(async move {
        // Only wait for the polling interval on retries, not the first poll
        if !is_first_poll {
            tokio::time::sleep(std::time::Duration::from_secs(interval as u64)).await;
        }

        let client = PkceDeviceFlowClient::new(http_client, auth_url);

        match client.poll_for_token(&device_code, &code_verifier).await {
            Ok(Some(token_response)) => {
                // Success! Calculate expiry and send completion
                let now = Utc::now().timestamp();
                let expiry = now + token_response.expires_in;
                let auth = token_response.to_auth_storage(expiry);
                let _ = tx.send(Action::AuthDeviceFlowComplete(auth));
            }
            Ok(None) => {
                // Still pending, poll again (with delay on next iteration)
                let _ = tx.send(Action::AuthDeviceFlowPoll {
                    device_code,
                    code_verifier,
                    interval,
                    is_first_poll: false,
                });
            }
            Err(AuthError::DeviceTokenExpired) => {
                let _ = tx.send(Action::AuthError(
                    "Authentication timed out. Please try again.".to_string(),
                ));
            }
            Err(e) => {
                let _ = tx.send(Action::AuthError(e.to_string()));
            }
        }
    });
}
