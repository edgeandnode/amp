//! File watching infrastructure for hot-reloading dataset manifests.
//!
//! This module provides efficient file system monitoring with debouncing to enable
//! automatic reloading when the nozzle configuration file changes, improving developer
//! experience during iterative development.

use std::{path::PathBuf, sync::Arc, time::Duration};

use notify::{
    Config, Event, EventKind, PollWatcher, RecursiveMode, Watcher,
    event::{ModifyKind, RenameMode},
};
use tokio::sync::mpsc;
use tracing::{error, info, warn};

/// Debounce duration to prevent reload storms from rapid file saves.
/// Editors often write files in multiple chunks, so we wait for the dust to settle.
const DEBOUNCE_DURATION: Duration = Duration::from_millis(500);

/// Events emitted by the file watcher
#[derive(Debug, Clone)]
pub enum FileWatchEvent {
    /// The watched file has been modified and we should reload
    Changed,
}

/// Spawns a background task that watches a file for changes and sends events on modification.
///
/// This function sets up efficient file system monitoring with debouncing to handle
/// editors that write files in multiple operations. It only triggers on actual content
/// modifications, ignoring metadata changes.
///
/// # Architecture
/// - Uses polling-based file watching (compatible with Docker volumes)
/// - Polls every 2 seconds for changes (configurable via Config)
/// - Debounces rapid successive writes to prevent reload storms
/// - Watches parent directory (some editors use atomic rename patterns)
/// - Non-blocking: runs in background task with channel communication
///
/// # Arguments
/// * `path` - Path to the file to watch
/// * `tx` - Channel to send FileWatchEvent::Changed when file is modified
///
/// # Returns
/// A JoinHandle for the background watcher task
///
/// # Panics
/// Panics if the file path has no parent directory (e.g., root path)
pub fn spawn_file_watcher(
    path: PathBuf,
    tx: mpsc::Sender<FileWatchEvent>,
) -> tokio::task::JoinHandle<()> {
    tokio::task::spawn(async move {
        if let Err(e) = run_file_watcher(path, tx).await {
            error!("File watcher error: {}", e);
        }
    })
}

/// Internal implementation of file watching logic.
///
/// Sets up a notify watcher with debouncing to efficiently monitor file changes.
/// This function blocks until an error occurs or the receiver is dropped.
async fn run_file_watcher(
    path: PathBuf,
    event_tx: mpsc::Sender<FileWatchEvent>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let path = Arc::new(path);
    let file_name = path
        .file_name()
        .ok_or("Path has no filename")?
        .to_string_lossy()
        .to_string();

    // Watch the parent directory to catch atomic rename patterns (common in editors)
    let watch_dir = path
        .parent()
        .ok_or("Path has no parent directory")?
        .to_path_buf();

    info!(
        "Starting file watcher for '{}' in directory '{}'",
        file_name,
        watch_dir.display()
    );

    // Channel for raw file system events from notify
    // Use flume for cross-thread communication (sync thread -> async task)
    let (notify_tx, notify_rx) = flume::bounded::<Event>(32);

    // Spawn notify watcher in blocking thread (notify is sync-only)
    let watch_dir_clone = watch_dir.clone();
    std::thread::spawn(move || {
        // Use PollWatcher instead of RecommendedWatcher for Docker volume mount compatibility.
        // Docker volumes (especially on macOS) don't always propagate file system events properly,
        // so we use polling mode which checks the file system periodically (default: 30 seconds).
        // This is more reliable for containerized environments at the cost of some latency.
        //
        // with_compare_contents(true) ensures we detect changes even if mtime doesn't update
        // (which can happen with Docker volume mounts on macOS)
        let config = Config::default()
            .with_poll_interval(Duration::from_secs(2))
            .with_compare_contents(true);
        let mut watcher = match PollWatcher::new(
            move |res: Result<Event, notify::Error>| match res {
                Ok(event) => {
                    // Send event to async task (non-blocking send)
                    // If receiver dropped, watcher stops automatically
                    let _ = notify_tx.send(event);
                }
                Err(e) => error!("File watcher error: {}", e),
            },
            config,
        ) {
            Ok(w) => w,
            Err(e) => {
                error!("Failed to create file watcher: {}", e);
                return;
            }
        };

        // Watch the directory for changes
        if let Err(e) = watcher.watch(&watch_dir_clone, RecursiveMode::NonRecursive) {
            error!("Failed to watch directory: {}", e);
            return;
        }

        // Keep watcher alive - it stops watching when dropped
        loop {
            std::thread::park();
        }
    });

    // Debouncing state
    let mut debounce_timer: Option<tokio::time::Instant> = None;
    let mut pending_event = false;

    loop {
        tokio::select! {
            // Handle incoming file system events
            event_result = notify_rx.recv_async() => {
                match event_result {
                    Ok(event) => {
                        if is_relevant_event(&event, &file_name) {
                            pending_event = true;
                            debounce_timer = Some(tokio::time::Instant::now() + DEBOUNCE_DURATION);
                        }
                    }
                    Err(_) => {
                        warn!("File watcher notify channel closed");
                        break;
                    }
                }
            }

            // Check if debounce timer expired
            _ = async {
                match debounce_timer {
                    Some(deadline) => tokio::time::sleep_until(deadline).await,
                    None => std::future::pending().await,
                }
            } => {
                if pending_event {
                    info!("File '{}' changed, triggering reload", file_name);
                    if event_tx.send(FileWatchEvent::Changed).await.is_err() {
                        warn!("File watcher receiver dropped, shutting down");
                        break;
                    }
                    pending_event = false;
                    debounce_timer = None;
                }
            }
        }
    }

    Ok(())
}

/// Determines if a file system event is relevant for our watched file.
///
/// Filters events to only care about:
/// - Data modifications (content changes)
/// - Atomic renames (editor save patterns)
/// - Metadata changes (when using PollWatcher, these indicate file updates)
///
/// Ignores:
/// - Access events
/// - Events for other files in the directory
///
/// Note: PollWatcher emits Modify(Metadata(WriteTime)) events when file contents change
/// (when using with_compare_contents(true)), so we accept metadata events for polling mode.
fn is_relevant_event(event: &Event, target_file_name: &str) -> bool {
    // Check if this event is for our target file
    let is_target_file = event.paths.iter().any(|p| {
        p.file_name()
            .map(|name| name.to_string_lossy() == target_file_name)
            .unwrap_or(false)
    });

    if !is_target_file {
        return false;
    }

    // Filter for relevant event types
    // Note: We accept Modify(Metadata(WriteTime)) because PollWatcher uses this
    // to signal content changes when with_compare_contents(true) is set
    matches!(
        event.kind,
        EventKind::Modify(ModifyKind::Data(_))  // Content modified (native watchers)
        | EventKind::Modify(ModifyKind::Metadata(_))  // Metadata modified (polling watcher)
        | EventKind::Modify(ModifyKind::Name(RenameMode::To))  // Atomic rename (editor pattern)
        | EventKind::Create(_) // File created (handles some editor patterns)
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_relevant_event_content_modification() {
        use notify::event::{CreateKind, DataChange};

        // Test content modification events
        let event = Event {
            kind: EventKind::Modify(ModifyKind::Data(DataChange::Any)),
            paths: vec![std::path::PathBuf::from("/tmp/test.ts")],
            attrs: Default::default(),
        };
        assert!(is_relevant_event(&event, "test.ts"));

        // Test create events (some editors)
        let event = Event {
            kind: EventKind::Create(CreateKind::File),
            paths: vec![std::path::PathBuf::from("/tmp/test.ts")],
            attrs: Default::default(),
        };
        assert!(is_relevant_event(&event, "test.ts"));

        // Test rename events (atomic writes)
        let event = Event {
            kind: EventKind::Modify(ModifyKind::Name(RenameMode::To)),
            paths: vec![std::path::PathBuf::from("/tmp/test.ts")],
            attrs: Default::default(),
        };
        assert!(is_relevant_event(&event, "test.ts"));
    }

    #[test]
    fn test_is_relevant_event_wrong_file() {
        use notify::event::DataChange;

        let event = Event {
            kind: EventKind::Modify(ModifyKind::Data(DataChange::Any)),
            paths: vec![std::path::PathBuf::from("/tmp/other.ts")],
            attrs: Default::default(),
        };
        assert!(!is_relevant_event(&event, "test.ts"));
    }

    #[test]
    fn test_is_relevant_event_metadata_accepted() {
        use notify::event::MetadataKind;

        // Metadata changes are now accepted (for PollWatcher compatibility)
        let event = Event {
            kind: EventKind::Modify(ModifyKind::Metadata(MetadataKind::Any)),
            paths: vec![std::path::PathBuf::from("/tmp/test.ts")],
            attrs: Default::default(),
        };
        assert!(is_relevant_event(&event, "test.ts"));
    }
}
