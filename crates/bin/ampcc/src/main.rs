//! Amp CC - Terminal UI for managing Amp datasets.

use std::io;

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

mod app;
mod config;
mod registry;
mod ui;

use app::{ActivePane, App, DataSource, InputMode, InspectResult};
use ratatui::layout::{Constraint, Direction, Layout, Rect};

/// Events that can be sent from async tasks.
enum AppEvent {
    ManifestLoaded(Option<serde_json::Value>),
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

    loop {
        terminal.draw(|f| ui::draw(f, app))?;

        // Tick spinner animation
        app.tick_spinner();

        // Handle async updates
        if let Ok(event) = rx.try_recv() {
            match event {
                AppEvent::ManifestLoaded(manifest) => {
                    app.reset_scroll();
                    app.current_inspect = manifest.as_ref().and_then(InspectResult::from_manifest);
                    app.current_manifest = manifest;
                    app.stop_loading();
                }
                AppEvent::Error(msg) => {
                    app.error_message = Some(msg);
                    app.stop_loading();
                }
            }
        }

        // Handle Input
        if event::poll(tick_rate)? {
            match event::read()? {
                Event::Key(key) => {
                    match app.input_mode {
                        InputMode::Normal => {
                            match key.code {
                                KeyCode::Char('q') => app.quit(),

                                // Source switching
                                KeyCode::Char('1') => {
                                    let tx = tx.clone();
                                    app.start_loading("Switching source...");
                                    if let Err(e) = app.switch_source(DataSource::Local).await {
                                        let _ = tx.send(AppEvent::Error(e.to_string())).await;
                                    } else {
                                        app.start_loading("Loading manifest...");
                                        spawn_fetch_manifest(app, tx);
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

                                // Refresh
                                KeyCode::Char('r') => {
                                    let tx = tx.clone();
                                    app.start_loading("Refreshing...");
                                    if let Err(e) = app.fetch_datasets().await {
                                        let _ = tx.send(AppEvent::Error(e.to_string())).await;
                                    } else {
                                        app.start_loading("Loading manifest...");
                                        spawn_fetch_manifest(app, tx);
                                    }
                                }

                                // Navigation
                                KeyCode::Down | KeyCode::Char('j') => match app.active_pane {
                                    ActivePane::Sidebar => {
                                        app.select_next();
                                        app.start_loading("Loading manifest...");
                                        spawn_fetch_manifest(app, tx.clone());
                                    }
                                    _ => app.scroll_down(),
                                },
                                KeyCode::Up | KeyCode::Char('k') => match app.active_pane {
                                    ActivePane::Sidebar => {
                                        app.select_previous();
                                        app.start_loading("Loading manifest...");
                                        spawn_fetch_manifest(app, tx.clone());
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

                                // Expand/collapse
                                KeyCode::Enter => {
                                    let tx = tx.clone();
                                    app.start_loading("Expanding...");
                                    if let Err(e) = app.toggle_expand().await {
                                        let _ = tx.send(AppEvent::Error(e.to_string())).await;
                                    }
                                    app.stop_loading();
                                }

                                // Pane navigation
                                KeyCode::Tab => {
                                    app.active_pane = app.active_pane.next();
                                }
                                KeyCode::BackTab => {
                                    app.active_pane = app.active_pane.prev();
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

                        let main_area = main_chunks[1];

                        // Main layout: Sidebar (35%) | Content (65%)
                        let content_chunks = Layout::default()
                            .direction(Direction::Horizontal)
                            .constraints([Constraint::Percentage(35), Constraint::Percentage(65)])
                            .split(main_area);

                        let sidebar_area = content_chunks[0];
                        let content_area = content_chunks[1];

                        // Content layout: Manifest (60%) | Schema (40%)
                        let pane_chunks = Layout::default()
                            .direction(Direction::Vertical)
                            .constraints([Constraint::Percentage(60), Constraint::Percentage(40)])
                            .split(content_area);

                        let manifest_area = pane_chunks[0];
                        let schema_area = pane_chunks[1];

                        // Check which pane was clicked
                        let x = mouse.column;
                        let y = mouse.row;

                        if x >= sidebar_area.x
                            && x < sidebar_area.x + sidebar_area.width
                            && y >= sidebar_area.y
                            && y < sidebar_area.y + sidebar_area.height
                        {
                            app.active_pane = ActivePane::Sidebar;
                        } else if x >= manifest_area.x
                            && x < manifest_area.x + manifest_area.width
                            && y >= manifest_area.y
                            && y < manifest_area.y + manifest_area.height
                        {
                            app.active_pane = ActivePane::Manifest;
                        } else if x >= schema_area.x
                            && x < schema_area.x + schema_area.width
                            && y >= schema_area.y
                            && y < schema_area.y + schema_area.height
                        {
                            app.active_pane = ActivePane::Schema;
                        }
                    }
                }
                _ => {}
            }
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
