//! Application state management

use std::path::PathBuf;

use anyhow::Result;
use crossterm::event::{Event, KeyCode, KeyEvent};

use crate::stream::StreamHandler;

/// CLI arguments (stored for reconnection)
#[derive(Debug, Clone)]
pub struct AppArgs {
    pub endpoint: String,
    pub query: Option<String>,
    pub buffer_size: usize,
    pub output: Option<PathBuf>,
    pub lmdb: Option<PathBuf>,
}

impl From<crate::Args> for AppArgs {
    fn from(args: crate::Args) -> Self {
        Self {
            endpoint: args.endpoint,
            query: args.query,
            buffer_size: args.buffer_size,
            output: args.output,
            lmdb: args.lmdb,
        }
    }
}

/// Application state
pub struct App {
    /// CLI arguments (for reconnection)
    pub args: AppArgs,

    /// Current connection status
    pub status: ConnectionStatus,

    /// Data rows for display
    pub rows: Vec<Row>,

    /// Current vertical scroll offset (row index)
    pub scroll_offset: usize,

    /// Current horizontal scroll offset (column index)
    pub horizontal_scroll_offset: usize,

    /// Whether the stream is paused
    pub paused: bool,

    /// Whether to quit the application
    pub should_quit: bool,

    /// Stream handler for receiving data
    pub stream_handler: StreamHandler,

    /// Whether currently connected
    pub is_connected: bool,

    /// Whether to show the help panel
    pub show_help: bool,
}

/// Connection status
#[derive(Debug, Clone, PartialEq)]
#[allow(dead_code)]
pub enum ConnectionStatus {
    Disconnected,
    Connecting,
    Connected,
    Error(String),
}

/// A single row of data
#[derive(Debug, Clone, PartialEq)]
pub struct Row {
    /// Row data as key-value pairs
    pub data: Vec<(String, String)>,

    /// Row type (Insert, Delete, or Normal)
    pub row_type: RowType,

    /// Transaction ID this row belongs to (for CDC tracking)
    pub transaction_id: Option<String>,
}

/// Type of row
#[derive(Debug, Clone, PartialEq)]
#[allow(dead_code)]
pub enum RowType {
    Normal,
    Insert,
    Delete,
}

impl App {
    /// Create a new application instance
    pub fn new(args: impl Into<AppArgs>) -> Self {
        let args = args.into();

        // Create stream handler - start streaming if query is provided
        let (stream_handler, is_connected) = if args.query.is_some() {
            (
                StreamHandler::new_with_stream(
                    args.endpoint.clone(),
                    args.query.clone().unwrap(),
                    args.output.clone(),
                    args.lmdb.clone(),
                ),
                true,
            )
        } else {
            (StreamHandler::new(), false)
        };

        // Start with empty rows if streaming, otherwise show mock data
        let rows = if args.query.is_some() {
            Vec::new()
        } else {
            Self::generate_mock_data()
        };

        Self {
            args,
            status: ConnectionStatus::Disconnected,
            rows,
            scroll_offset: 0,
            horizontal_scroll_offset: 0,
            paused: false,
            should_quit: false,
            stream_handler,
            is_connected,
            show_help: false,
        }
    }

    /// Manually disconnect from the stream
    pub fn disconnect(&mut self) {
        tracing::info!("User requested disconnect");
        self.stream_handler = StreamHandler::new();
        self.is_connected = false;
        self.status = ConnectionStatus::Disconnected;
    }

    /// Manually reconnect to the stream
    pub fn reconnect(&mut self) {
        if let Some(ref query) = self.args.query {
            tracing::info!("User requested reconnect");
            self.stream_handler = StreamHandler::new_with_stream(
                self.args.endpoint.clone(),
                query.clone(),
                self.args.output.clone(),
                self.args.lmdb.clone(),
            );
            self.is_connected = true;
            self.status = ConnectionStatus::Connecting;
        } else {
            tracing::warn!("Cannot reconnect: no query specified");
        }
    }

    /// Generate mock data for initial display
    fn generate_mock_data() -> Vec<Row> {
        // Generate 100 rows of mock data for testing scrolling
        // Reversed so newest (highest block number) is at index 0
        (0..100)
            .rev()
            .map(|i| {
                let block_num = 12345678 + i;
                Row {
                    data: vec![
                        ("block_number".to_string(), block_num.to_string()),
                        (
                            "hash".to_string(),
                            format!("0x{:016x}...", block_num * 123456),
                        ),
                        (
                            "timestamp".to_string(),
                            format!("2024-01-15 10:{}:{:02}", 30 + i / 60, i % 60),
                        ),
                        ("gas_used".to_string(), format!("{}", 21000 + i * 1000)),
                        ("miner".to_string(), format!("0x{:040x}", i * 999999)),
                    ],
                    row_type: match i % 10 {
                        0 => RowType::Insert,
                        9 => RowType::Delete,
                        _ => RowType::Normal,
                    },
                    transaction_id: None,
                }
            })
            .collect()
    }

    /// Handle terminal events
    pub async fn handle_event(&mut self, event: Event) -> Result<bool> {
        if let Event::Key(key) = event {
            return Ok(self.handle_key_event(key));
        }
        Ok(true)
    }

    /// Handle keyboard input
    fn handle_key_event(&mut self, key: KeyEvent) -> bool {
        match key.code {
            KeyCode::Char('q') | KeyCode::Esc => {
                self.should_quit = true;
                false
            }
            KeyCode::Char(' ') => {
                self.paused = !self.paused;
                true
            }
            KeyCode::Char('d') => {
                // Disconnect
                self.disconnect();
                true
            }
            KeyCode::Char('r') => {
                // Reconnect
                self.reconnect();
                true
            }
            KeyCode::Char('?') | KeyCode::F(1) => {
                // Toggle help
                self.show_help = !self.show_help;
                true
            }
            // Vertical scrolling
            KeyCode::Down | KeyCode::Char('j') => {
                self.scroll_down();
                true
            }
            KeyCode::Up | KeyCode::Char('k') => {
                self.scroll_up();
                true
            }
            KeyCode::PageDown => {
                self.scroll_down_page();
                true
            }
            KeyCode::PageUp => {
                self.scroll_up_page();
                true
            }
            // Horizontal scrolling
            KeyCode::Left | KeyCode::Char('h') => {
                self.scroll_left();
                true
            }
            KeyCode::Right | KeyCode::Char('l') => {
                self.scroll_right();
                true
            }
            // Jump to edges
            KeyCode::Home => {
                self.scroll_offset = 0;
                self.horizontal_scroll_offset = 0;
                true
            }
            KeyCode::End => {
                self.scroll_to_end();
                true
            }
            _ => true,
        }
    }

    /// Scroll down by one row
    fn scroll_down(&mut self) {
        if self.scroll_offset < self.rows.len().saturating_sub(1) {
            self.scroll_offset += 1;
        }
    }

    /// Scroll up by one row
    fn scroll_up(&mut self) {
        self.scroll_offset = self.scroll_offset.saturating_sub(1);
    }

    /// Scroll down by a page
    fn scroll_down_page(&mut self) {
        self.scroll_offset = (self.scroll_offset + 10).min(self.rows.len().saturating_sub(1));
    }

    /// Scroll up by a page
    fn scroll_up_page(&mut self) {
        self.scroll_offset = self.scroll_offset.saturating_sub(10);
    }

    /// Scroll to the end
    fn scroll_to_end(&mut self) {
        self.scroll_offset = self.rows.len().saturating_sub(1);
    }

    /// Scroll left by one column
    fn scroll_left(&mut self) {
        self.horizontal_scroll_offset = self.horizontal_scroll_offset.saturating_sub(1);
    }

    /// Scroll right by one column
    fn scroll_right(&mut self) {
        let num_columns = self.get_columns().len();
        if num_columns > 0 && self.horizontal_scroll_offset < num_columns - 1 {
            self.horizontal_scroll_offset += 1;
        }
    }

    /// Update application state (called every frame)
    pub async fn update(&mut self) -> Result<()> {
        // Check for new stream messages
        while let Some(message) = self.stream_handler.try_recv() {
            use crate::stream::{StreamMessage, StreamStatus};

            match message {
                StreamMessage::Data(mut new_rows) => {
                    if !self.paused {
                        // Insert new rows at the beginning (newest first)
                        new_rows.reverse(); // Reverse so newest is first
                        self.rows.splice(0..0, new_rows);

                        // Trim buffer to max size if needed (remove oldest from end)
                        if self.rows.len() > self.args.buffer_size {
                            let new_len = self.args.buffer_size;
                            self.rows.truncate(new_len);
                            // No need to adjust scroll offset since we're trimming from the end
                        }
                    }
                }
                StreamMessage::Reorg {
                    transaction_ids,
                    message: _,
                } => {
                    if !self.paused {
                        // Mark all rows with matching transaction IDs as deleted (red)
                        for row in &mut self.rows {
                            if let Some(ref row_tx_id) = row.transaction_id
                                && transaction_ids.contains(row_tx_id)
                            {
                                row.row_type = RowType::Delete;
                            }
                        }
                    }
                }
                StreamMessage::StatusChanged(stream_status) => {
                    self.status = match stream_status {
                        StreamStatus::Connecting => ConnectionStatus::Connecting,
                        StreamStatus::Connected => ConnectionStatus::Connected,
                        StreamStatus::Disconnected => ConnectionStatus::Disconnected,
                        StreamStatus::Error(err) => ConnectionStatus::Error(err),
                    };
                }
                StreamMessage::Error(err) => {
                    self.status = ConnectionStatus::Error(err);
                }
            }
        }

        Ok(())
    }

    /// Get the column names from the first row
    pub fn get_columns(&self) -> Vec<String> {
        self.rows
            .first()
            .map(|row| row.data.iter().map(|(k, _)| k.clone()).collect())
            .unwrap_or_default()
    }

    /// Get visible column names based on horizontal scroll and available width
    pub fn get_visible_columns(&self, available_width: usize) -> Vec<(usize, String, usize)> {
        let all_columns = self.get_columns();
        if all_columns.is_empty() {
            return Vec::new();
        }

        let mut visible = Vec::new();
        let mut used_width = 0;
        let padding = 3; // Space for padding and borders

        // Start from horizontal scroll offset
        for (idx, col_name) in all_columns
            .iter()
            .enumerate()
            .skip(self.horizontal_scroll_offset)
        {
            // Calculate column width based on content
            let col_width = self.calculate_column_width(idx, col_name);

            if used_width + col_width + padding > available_width && !visible.is_empty() {
                break;
            }

            visible.push((idx, col_name.clone(), col_width));
            used_width += col_width + padding;
        }

        visible
    }

    /// Calculate the width needed for a column
    fn calculate_column_width(&self, col_idx: usize, col_name: &str) -> usize {
        let mut max_width = col_name.len();

        // Check the first 100 rows to determine width (for performance)
        for row in self.rows.iter().take(100) {
            if let Some((_, value)) = row.data.get(col_idx) {
                max_width = max_width.max(value.len());
            }
        }

        // Cap at reasonable max width
        max_width.min(50)
    }

    /// Get visible rows based on scroll offset
    pub fn get_visible_rows(&self, max_rows: usize) -> &[Row] {
        let start = self.scroll_offset;
        let end = (start + max_rows).min(self.rows.len());
        &self.rows[start..end]
    }
}
