//! Amp CC - Terminal UI for managing Amp datasets.

use std::io;

use anyhow::Result;
use crossterm::{
    event::{self, Event, KeyCode},
    execute,
    terminal::{EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode},
};
use ratatui::{Terminal, backend::CrosstermBackend};
use tokio::sync::mpsc;

mod app;
mod config;
mod registry;
mod ui;

use app::{App, DataSource, InputMode, InspectResult};

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
    execute!(stdout, EnterAlternateScreen)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    // Run app
    let res = run_app(&mut terminal, &mut app).await;

    // Restore terminal
    disable_raw_mode()?;
    execute!(terminal.backend_mut(), LeaveAlternateScreen)?;
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
            if let Event::Key(key) = event::read()? {
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
                            KeyCode::Down | KeyCode::Char('j') => {
                                app.select_next();
                                app.start_loading("Loading manifest...");
                                spawn_fetch_manifest(app, tx.clone());
                            }
                            KeyCode::Up | KeyCode::Char('k') => {
                                app.select_previous();
                                app.start_loading("Loading manifest...");
                                spawn_fetch_manifest(app, tx.clone());
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
