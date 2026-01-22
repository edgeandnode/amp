//! Auth screen component for device flow authentication.

use std::borrow::Cow;

use ratatui::{
    Frame,
    layout::{Alignment, Constraint, Direction, Layout, Rect},
    style::{Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Paragraph},
};

use super::super::{AMP_LOGO, Theme};
use crate::app::{App, DeviceFlowStatus};

/// Render the full screen authentication screen.
pub fn render(f: &mut Frame, app: &App) {
    let Some(ref flow) = app.auth_device_flow else {
        return;
    };

    let area = f.area();

    let block = Block::default()
        .title(" Authentication ")
        .borders(Borders::ALL)
        .border_style(Theme::border_focused());

    let inner = block.inner(area);
    f.render_widget(block, area);

    // Layout: top padding, logo, gap, code label, gap, user code, gap, status, bottom padding
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Min(1),     // Top padding (flexible)
            Constraint::Length(20), // Logo
            Constraint::Length(2),  // Gap
            Constraint::Length(1),  // Code label
            Constraint::Length(1),  // Gap
            Constraint::Length(1),  // User code
            Constraint::Length(2),  // Gap
            Constraint::Length(1),  // Status
            Constraint::Min(1),     // Bottom padding (flexible)
        ])
        .split(inner);

    // Render logo
    render_logo(f, chunks[1]);

    // Render code label
    render_code_label(f, chunks[3], flow.copy_to_clipboard_failed);

    // Render user code
    render_user_code(f, chunks[5], &flow.user_code);

    // Render status
    render_status(f, chunks[7], &flow.status);
}

/// Render the ASCII logo.
fn render_logo(f: &mut Frame, area: Rect) {
    let logo_lines: Vec<&str> = AMP_LOGO.lines().skip(1).take(18).collect();
    let lines: Vec<Line> = logo_lines
        .iter()
        .map(|line| {
            Line::from(Span::styled(
                line.to_string(),
                Theme::accent().add_modifier(Modifier::BOLD),
            ))
        })
        .collect();

    let paragraph = Paragraph::new(lines).alignment(Alignment::Center);
    f.render_widget(paragraph, area);
}

/// Render the code label.
fn render_code_label(f: &mut Frame, area: Rect, copy_failed: bool) {
    let text = if copy_failed {
        "Enter this code in your browser:"
    } else {
        "Enter this code in your browser (copied to clipboard):"
    };
    let line = Line::from(Span::styled(text, Theme::text_secondary()));
    let paragraph = Paragraph::new(line).alignment(Alignment::Center);
    f.render_widget(paragraph, area);
}

/// Render the user code prominently.
fn render_user_code(f: &mut Frame, area: Rect, user_code: &str) {
    let text = Line::from(Span::styled(
        user_code,
        Style::default()
            .fg(Theme::GALACTIC_AQUA)
            .add_modifier(Modifier::BOLD),
    ));
    let paragraph = Paragraph::new(text).alignment(Alignment::Center);
    f.render_widget(paragraph, area);
}

/// Render the auth status message.
fn render_status(f: &mut Frame, area: Rect, status: &DeviceFlowStatus) {
    let (message, style): (Cow<'_, str>, Style) = match status {
        DeviceFlowStatus::AwaitingConfirmation => (
            "Press Enter to open browser, Esc to cancel".into(),
            Theme::version_tag(),
        ),
        DeviceFlowStatus::WaitingForBrowser => (
            "Opening browser... Complete authentication to continue.".into(),
            Theme::status_warning(),
        ),
        DeviceFlowStatus::Polling => {
            ("Waiting for authentication...".into(), Theme::status_warning())
        }
        DeviceFlowStatus::OpenBrowserFailure(url) => (
            format!("Failed to open browser. Go to: {url} to authenticate").into(),
            Theme::status_warning(),
        ),
        DeviceFlowStatus::Error(err) => (err.as_str().into(), Theme::status_error()),
    };

    let text = Line::from(Span::styled(message, style));
    let paragraph = Paragraph::new(text).alignment(Alignment::Center);
    f.render_widget(paragraph, area);
}
