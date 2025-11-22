//! Terminal UI rendering and event handling

use std::{
    io::{self, Stdout},
    time::Duration,
};

use anyhow::Result;
use crossterm::{
    event::{self, Event},
    execute,
    terminal::{EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode},
};
use ratatui::{
    Frame,
    backend::CrosstermBackend,
    layout::{Constraint, Direction, Layout, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Cell, Paragraph, Row as TableRow, Table},
};

use crate::app::{App, ConnectionStatus, RowType};

pub type Terminal = ratatui::Terminal<CrosstermBackend<Stdout>>;

/// Set up the terminal for TUI
pub fn setup_terminal() -> Result<Terminal> {
    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen)?;
    let backend = CrosstermBackend::new(stdout);
    let terminal = Terminal::new(backend)?;
    Ok(terminal)
}

/// Restore the terminal to normal mode
pub fn restore_terminal(terminal: &mut Terminal) -> Result<()> {
    disable_raw_mode()?;
    execute!(terminal.backend_mut(), LeaveAlternateScreen)?;
    terminal.show_cursor()?;
    Ok(())
}

/// Poll for terminal events with timeout
pub fn poll_event() -> Result<Option<Event>> {
    if event::poll(Duration::from_millis(100))? {
        Ok(Some(event::read()?))
    } else {
        Ok(None)
    }
}

/// Draw the entire UI
pub fn draw(frame: &mut Frame, app: &App) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(5), // Header (expanded for storage info)
            Constraint::Min(0),    // Data table
            Constraint::Length(3), // Status bar
        ])
        .split(frame.area());

    draw_header(frame, app, chunks[0]);
    draw_data_table(frame, app, chunks[1]);
    draw_status_bar(frame, app, chunks[2]);

    // Draw help panel on top if enabled
    if app.show_help {
        draw_help_panel(frame, app);
    }
}

/// Draw the header with title and connection info
fn draw_header(frame: &mut Frame, app: &App, area: Rect) {
    let title = vec![
        Span::styled(
            "Amp",
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        ),
        Span::raw(" "),
        Span::styled("TUI Streaming Client", Style::default().fg(Color::White)),
    ];

    let connection_info = format!(" Endpoint: {} ", app.args.endpoint);

    let storage_info = if app.args.lmdb.is_some() {
        format!(
            " Storage: LMDB ({})",
            app.args.lmdb.as_ref().unwrap().display()
        )
    } else {
        " Storage: In-Memory".to_string()
    };

    let header_text = vec![
        Line::from(title),
        Line::from(connection_info),
        Line::from(storage_info),
    ];

    let header = Paragraph::new(header_text)
        .block(Block::default().borders(Borders::ALL).title("Query Stream"));

    frame.render_widget(header, area);
}

/// Draw the main data table
fn draw_data_table(frame: &mut Frame, app: &App, area: Rect) {
    let all_columns = app.get_columns();

    if all_columns.is_empty() {
        let placeholder = Paragraph::new("No data available. Press 'q' to quit.")
            .block(Block::default().borders(Borders::ALL).title("Data"));
        frame.render_widget(placeholder, area);
        return;
    }

    // Calculate available dimensions
    let available_height = area.height.saturating_sub(3) as usize;
    let available_width = area.width.saturating_sub(2) as usize; // Account for borders
    let visible_rows = app.get_visible_rows(available_height);

    // Get visible columns based on horizontal scroll
    let visible_columns = app.get_visible_columns(available_width);

    if visible_columns.is_empty() {
        let placeholder = Paragraph::new("No columns to display.")
            .block(Block::default().borders(Borders::ALL).title("Data"));
        frame.render_widget(placeholder, area);
        return;
    }

    // Create header row with visible columns
    let header_cells: Vec<Cell> = visible_columns
        .iter()
        .map(|(_, col_name, _)| {
            Cell::from(col_name.as_str()).style(Style::default().fg(Color::Yellow))
        })
        .collect();
    let header = TableRow::new(header_cells)
        .style(Style::default().add_modifier(Modifier::BOLD))
        .height(1);

    // Create data rows with only visible columns
    let rows: Vec<TableRow> = visible_rows
        .iter()
        .map(|row| {
            let cells: Vec<Cell> = visible_columns
                .iter()
                .map(|(col_idx, _, _)| {
                    let value = row
                        .data
                        .get(*col_idx)
                        .map(|(_, v)| v.as_str())
                        .unwrap_or("");
                    Cell::from(value)
                })
                .collect();

            let style = match row.row_type {
                RowType::Insert => Style::default().fg(Color::Green),
                RowType::Delete => Style::default().fg(Color::Red),
                RowType::Normal => Style::default(),
            };

            TableRow::new(cells).style(style)
        })
        .collect();

    // Calculate column widths based on actual content
    let widths: Vec<Constraint> = visible_columns
        .iter()
        .map(|(_, _, width)| Constraint::Length(*width as u16))
        .collect();

    // Build title with scroll indicators
    let total_cols = all_columns.len();
    let showing_cols = visible_columns.len();
    let first_col_idx = visible_columns
        .first()
        .map(|(idx, _, _)| idx + 1)
        .unwrap_or(0);
    let last_col_idx = visible_columns
        .last()
        .map(|(idx, _, _)| idx + 1)
        .unwrap_or(0);

    let title = if total_cols > showing_cols {
        format!(
            "Data (Rows {}-{}/{}, Cols {}-{}/{})",
            app.scroll_offset + 1,
            (app.scroll_offset + visible_rows.len()).min(app.rows.len()),
            app.rows.len(),
            first_col_idx,
            last_col_idx,
            total_cols
        )
    } else {
        format!(
            "Data (Rows {}-{}/{})",
            app.scroll_offset + 1,
            (app.scroll_offset + visible_rows.len()).min(app.rows.len()),
            app.rows.len()
        )
    };

    let table = Table::new(rows, widths)
        .header(header)
        .block(Block::default().borders(Borders::ALL).title(title))
        .style(Style::default().fg(Color::White));

    frame.render_widget(table, area);
}

/// Draw the status bar with controls and status
fn draw_status_bar(frame: &mut Frame, app: &App, area: Rect) {
    let status_text = match &app.status {
        ConnectionStatus::Connected => Span::styled("Connected", Style::default().fg(Color::Green)),
        ConnectionStatus::Connecting => {
            Span::styled("Connecting...", Style::default().fg(Color::Yellow))
        }
        ConnectionStatus::Disconnected => {
            Span::styled("Disconnected", Style::default().fg(Color::Red))
        }
        ConnectionStatus::Error(err) => {
            Span::styled(format!("Error: {}", err), Style::default().fg(Color::Red))
        }
    };

    let paused_text = if app.paused {
        Span::styled(" [PAUSED]", Style::default().fg(Color::Yellow))
    } else {
        Span::raw("")
    };

    // Show buffer info
    let buffer_info = format!(
        " | Buffer: {}/{} rows",
        app.rows.len(),
        app.args.buffer_size
    );
    let buffer_text = Span::styled(buffer_info, Style::default().fg(Color::Cyan));

    let controls = if app.is_connected {
        " [?]Help [q]Quit [d]Disconnect [Space]Pause [↑↓/jk]Scroll [PgUp/PgDn]Page [Home/End]Jump "
    } else {
        " [?]Help [q]Quit [r]Reconnect [Space]Pause [↑↓/jk]Scroll [PgUp/PgDn]Page [Home/End]Jump "
    };

    let status_line = vec![
        Line::from(vec![status_text, paused_text, buffer_text]),
        Line::from(controls),
    ];

    let status =
        Paragraph::new(status_line).block(Block::default().borders(Borders::ALL).title("Status"));

    frame.render_widget(status, area);
}

/// Draw help panel overlay
fn draw_help_panel(frame: &mut Frame, _app: &App) {
    use ratatui::layout::Alignment;

    // Create centered popup area (80% width, 70% height)
    let area = frame.area();
    let popup_width = (area.width * 80) / 100;
    let popup_height = (area.height * 70) / 100;
    let popup_x = (area.width - popup_width) / 2;
    let popup_y = (area.height - popup_height) / 2;

    let popup_area = Rect {
        x: popup_x,
        y: popup_y,
        width: popup_width,
        height: popup_height,
    };

    // Help content
    let help_text = vec![
        Line::from(vec![Span::styled(
            "Keyboard Shortcuts",
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        )]),
        Line::from(""),
        Line::from(vec![Span::styled(
            "General",
            Style::default()
                .fg(Color::Yellow)
                .add_modifier(Modifier::BOLD),
        )]),
        Line::from(vec![
            Span::styled("  ?  or  F1   ", Style::default().fg(Color::Green)),
            Span::raw("Toggle this help panel"),
        ]),
        Line::from(vec![
            Span::styled("  q  or  Esc  ", Style::default().fg(Color::Green)),
            Span::raw("Quit the application"),
        ]),
        Line::from(vec![
            Span::styled("  Space       ", Style::default().fg(Color::Green)),
            Span::raw("Pause/Resume streaming (display freezes, data still received)"),
        ]),
        Line::from(""),
        Line::from(vec![Span::styled(
            "Connection Control",
            Style::default()
                .fg(Color::Yellow)
                .add_modifier(Modifier::BOLD),
        )]),
        Line::from(vec![
            Span::styled("  d           ", Style::default().fg(Color::Green)),
            Span::raw("Disconnect from stream (stops receiving data)"),
        ]),
        Line::from(vec![
            Span::styled("  r           ", Style::default().fg(Color::Green)),
            Span::raw("Reconnect to stream (resumes from last state if using LMDB)"),
        ]),
        Line::from(""),
        Line::from(vec![Span::styled(
            "Vertical Scrolling",
            Style::default()
                .fg(Color::Yellow)
                .add_modifier(Modifier::BOLD),
        )]),
        Line::from(vec![
            Span::styled("  ↑  or  k    ", Style::default().fg(Color::Green)),
            Span::raw("Scroll up one row"),
        ]),
        Line::from(vec![
            Span::styled("  ↓  or  j    ", Style::default().fg(Color::Green)),
            Span::raw("Scroll down one row"),
        ]),
        Line::from(vec![
            Span::styled("  PgUp        ", Style::default().fg(Color::Green)),
            Span::raw("Scroll up one page (10 rows)"),
        ]),
        Line::from(vec![
            Span::styled("  PgDn        ", Style::default().fg(Color::Green)),
            Span::raw("Scroll down one page (10 rows)"),
        ]),
        Line::from(""),
        Line::from(vec![Span::styled(
            "Horizontal Scrolling",
            Style::default()
                .fg(Color::Yellow)
                .add_modifier(Modifier::BOLD),
        )]),
        Line::from(vec![
            Span::styled("  ←  or  h    ", Style::default().fg(Color::Green)),
            Span::raw("Scroll left one column"),
        ]),
        Line::from(vec![
            Span::styled("  →  or  l    ", Style::default().fg(Color::Green)),
            Span::raw("Scroll right one column"),
        ]),
        Line::from(""),
        Line::from(vec![Span::styled(
            "Navigation",
            Style::default()
                .fg(Color::Yellow)
                .add_modifier(Modifier::BOLD),
        )]),
        Line::from(vec![
            Span::styled("  Home        ", Style::default().fg(Color::Green)),
            Span::raw("Jump to top-left (first row, first column)"),
        ]),
        Line::from(vec![
            Span::styled("  End         ", Style::default().fg(Color::Green)),
            Span::raw("Jump to bottom (last row)"),
        ]),
        Line::from(""),
        Line::from(vec![Span::styled(
            "Visual Indicators",
            Style::default()
                .fg(Color::Yellow)
                .add_modifier(Modifier::BOLD),
        )]),
        Line::from(vec![
            Span::styled("  Green rows  ", Style::default().fg(Color::Green)),
            Span::raw("Insert events (new data)"),
        ]),
        Line::from(vec![
            Span::styled("  Red rows    ", Style::default().fg(Color::Red)),
            Span::raw("Delete events (reorgs/rewinds)"),
        ]),
        Line::from(vec![
            Span::styled("  White rows  ", Style::default().fg(Color::White)),
            Span::raw("Normal/existing data"),
        ]),
        Line::from(""),
        Line::from(vec![
            Span::styled("Press ", Style::default().fg(Color::Gray)),
            Span::styled(
                "?",
                Style::default()
                    .fg(Color::Cyan)
                    .add_modifier(Modifier::BOLD),
            ),
            Span::styled(" or ", Style::default().fg(Color::Gray)),
            Span::styled(
                "F1",
                Style::default()
                    .fg(Color::Cyan)
                    .add_modifier(Modifier::BOLD),
            ),
            Span::styled(" to close this help", Style::default().fg(Color::Gray)),
        ]),
    ];

    let help_paragraph = Paragraph::new(help_text)
        .block(
            Block::default()
                .borders(Borders::ALL)
                .border_style(Style::default().fg(Color::Cyan))
                .title(" Help - Keyboard Shortcuts ")
                .title_alignment(Alignment::Center),
        )
        .alignment(Alignment::Left)
        .wrap(ratatui::widgets::Wrap { trim: false });

    // Clear the background
    frame.render_widget(ratatui::widgets::Clear, popup_area);
    frame.render_widget(help_paragraph, popup_area);
}
