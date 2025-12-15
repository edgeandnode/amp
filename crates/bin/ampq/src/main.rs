//! Amp TUI streaming client
//!
//! A terminal user interface for visualizing streaming data from Amp servers.
//! Displays real-time data updates, inserts, deletes, and reorg events.

use anyhow::Result;
use clap::Parser;

mod app;
mod stream;
mod ui;

use app::App;

/// Amp TUI streaming query client
#[derive(Parser, Debug, Clone)]
#[command(name = "ampq")]
#[command(about = "TUI client for streaming queries from Amp", long_about = None)]
struct Args {
    /// Arrow Flight endpoint URL
    #[arg(short, long, default_value = "http://localhost:1602")]
    endpoint: String,

    /// SQL query to execute
    #[arg(short, long)]
    query: Option<String>,

    /// Maximum number of rows to keep in buffer
    #[arg(short, long, default_value = "1000")]
    buffer_size: usize,

    /// Output JSONL file path (if specified, writes CDC events to file)
    #[arg(short, long)]
    output: Option<std::path::PathBuf>,

    /// LMDB database path for persistent state and batch storage
    #[arg(short, long)]
    lmdb: Option<std::path::PathBuf>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    // Set up terminal
    let mut terminal = ui::setup_terminal()?;

    // Create app state
    let mut app = App::new(args.clone());

    // Run the TUI application
    let result = run_app(&mut terminal, &mut app).await;

    // Restore terminal
    ui::restore_terminal(&mut terminal)?;

    result
}

async fn run_app(terminal: &mut ui::Terminal, app: &mut App) -> Result<()> {
    loop {
        // Draw the UI
        terminal.draw(|frame| ui::draw(frame, app))?;

        // Handle events
        if let Some(event) = ui::poll_event()?
            && !app.handle_event(event).await?
        {
            break;
        }

        // Update app state (e.g., check for new streaming data)
        app.update().await?;
    }

    Ok(())
}
