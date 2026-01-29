use std::{future::Future, path::PathBuf};

use tokio::sync::mpsc;

use crate::postgres::{PostgresBuilder, PostgresError};

/// Creates a new persistent PostgreSQL database service with app defaults.
///
/// This is a convenience wrapper around [`PostgresBuilder`] that applies the
/// application's preferred defaults (locale `"C"`, encoding `"UTF8"`).
///
/// If the data directory already contains a PostgreSQL database (detected by
/// the presence of `PG_VERSION` file), the existing data is reused. Otherwise,
/// a new database cluster is initialized.
///
/// For full control over PostgreSQL configuration, use [`PostgresBuilder`] directly.
///
/// # Arguments
///
/// * `data_dir` - Path where PostgreSQL stores its data files and Unix socket.
///   This directory will be created if it doesn't exist.
///
/// # Returns
///
/// Returns a tuple of:
/// - `Handle` - Used to get the connection URL and control the service
/// - A future that runs the service and completes when the database shuts down
///
/// The returned future internally handles:
/// - **Stop requests**: via [`Handle::stop()`]
/// - **OS signals**: SIGINT (Ctrl+C) and SIGTERM trigger graceful shutdown
/// - **Unexpected exits**: detects if the postgres process crashes
///
/// # Errors
///
/// Returns `PostgresError` if:
/// - PostgreSQL binaries (`initdb`, `postgres`) are not found in PATH
/// - Data directory cannot be created or initialized
/// - PostgreSQL server fails to start or become ready
///
/// # Example
///
/// ```ignore
/// let data_dir = PathBuf::from(".amp/metadb");
/// let (handle, fut) = service::new(data_dir).await?;
///
/// // The future handles signals and process monitoring internally.
/// // Use it in a select! to compose with other services:
/// tokio::select! {
///     _ = fut => { /* postgres exited or received signal */ }
///     _ = other_service => { /* other service completed */ }
/// }
/// ```
pub async fn new(
    data_dir: PathBuf,
) -> Result<(Handle, impl Future<Output = Result<(), PostgresError>>), PostgresError> {
    PostgresBuilder::new(data_dir)
        .locale("C")
        .encoding("UTF8")
        .start()
        .await
}

/// Handle to interact with the PostgreSQL database service.
///
/// The database is automatically stopped when this handle is dropped (channel closure
/// signals the service to shut down) or when [`stop()`](Handle::stop) is explicitly called.
pub struct Handle {
    url: String,
    data_dir: PathBuf,
    tx_stop: mpsc::Sender<()>,
}

impl Handle {
    /// Creates a new handle (crate-internal).
    pub(crate) fn new(url: String, data_dir: PathBuf, tx_stop: mpsc::Sender<()>) -> Self {
        Self {
            url,
            data_dir,
            tx_stop,
        }
    }

    /// Gets the connection URL for connecting to this database.
    ///
    /// The URL uses Unix socket connections in the format:
    /// `postgresql:///postgres?host=/path/to/data_dir`
    pub fn url(&self) -> &str {
        &self.url
    }

    /// Gets the data directory where PostgreSQL stores its files.
    pub fn data_dir(&self) -> &std::path::Path {
        &self.data_dir
    }

    /// Stops the service and shuts down the PostgreSQL server.
    ///
    /// Waits for the service to shut down gracefully. The database is also stopped
    /// automatically when the handle is dropped (via channel closure), but this method
    /// allows for explicit control and awaiting completion.
    pub async fn stop(self) {
        if self.tx_stop.is_closed() {
            return;
        }

        let _ = self.tx_stop.send(()).await;

        // Wait for the channel to close
        self.tx_stop.closed().await;
    }
}

/// Returns a future that completes when a shutdown signal (SIGINT or SIGTERM) is received.
///
/// Follows the same pattern as `controller/src/service.rs`, `server/src/service.rs`,
/// and `worker/src/service.rs`.
pub(crate) async fn shutdown_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        let mut sigint = signal(SignalKind::interrupt()).expect("Failed to install SIGINT handler");
        let mut sigterm =
            signal(SignalKind::terminate()).expect("Failed to install SIGTERM handler");
        tokio::select! {
            _ = sigint.recv() => tracing::info!(signal = "SIGINT", "shutdown signal"),
            _ = sigterm.recv() => tracing::info!(signal = "SIGTERM", "shutdown signal"),
        }
    }

    #[cfg(not(unix))]
    {
        tokio::signal::ctrl_c()
            .await
            .expect("Failed to install Ctrl+C handler");
        tracing::info!("shutdown signal");
    }
}
