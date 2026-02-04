use std::{
    future::Future,
    path::{Path, PathBuf},
};

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
pub async fn new(
    data_dir: PathBuf,
) -> Result<(Handle, impl Future<Output = Result<(), PostgresError>>), PostgresError> {
    PostgresBuilder::new(data_dir)
        .locale("C")
        .encoding("UTF8")
        .start()
        .await
}

/// Signal sent from [`Handle`] to the background future to initiate shutdown.
///
/// Decoupled from `nix::sys::signal::Signal` so the public API does not
/// depend on the `nix` crate.
#[derive(Debug, Clone, Copy)]
pub(crate) enum ShutdownSignal {
    /// Graceful (smart) shutdown — postgres waits for sessions to disconnect.
    Graceful,
    /// Hard (fast) shutdown — postgres aborts transactions and disconnects clients.
    Hard,
}

/// Handle to interact with the PostgreSQL database service.
///
/// The caller **must** consume this handle by calling either
/// [`graceful_shutdown()`](Handle::graceful_shutdown) or
/// [`force_shutdown()`](Handle::force_shutdown). Both methods consume `self` by
/// value, returning an inert [`ShuttingDown`] token. The background future
/// then performs the actual shutdown sequence (signal → timeout → SIGKILL).
///
/// Dropping the handle without calling a shutdown method is a programming
/// error — the background future will hang on the oneshot receiver until it is
/// cancelled (e.g., by aborting its `JoinHandle`). When the future is
/// cancelled, `PostgresProcess::Drop` sends a best-effort SIGTERM via nix
/// as the sole safety net (fire-and-forget, no SIGKILL escalation, no reap).
#[derive(Debug)]
pub struct Handle {
    url: String,
    data_dir: PathBuf,
    shutdown_tx: tokio::sync::oneshot::Sender<ShutdownSignal>,
}

/// Inert token proving that shutdown was initiated.
///
/// Returned by [`Handle::graceful_shutdown`] and [`Handle::force_shutdown`].
/// Has no methods and no `Drop` side-effects — the background future handles
/// the actual shutdown sequence.
pub struct ShuttingDown(());

impl Handle {
    /// Creates a new handle (crate-internal).
    pub(crate) fn new(
        url: String,
        data_dir: PathBuf,
        shutdown_tx: tokio::sync::oneshot::Sender<ShutdownSignal>,
    ) -> Self {
        Self {
            url,
            data_dir,
            shutdown_tx,
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
    pub fn data_dir(&self) -> &Path {
        &self.data_dir
    }

    /// Initiates a graceful (smart) shutdown by sending SIGTERM to postgres.
    ///
    /// Postgres waits for all sessions to disconnect voluntarily, then exits.
    /// If the process does not exit within the timeout, the background future
    /// escalates to SIGKILL.
    ///
    /// Consumes the handle — the caller cannot interact with the service after
    /// initiating shutdown.
    pub fn graceful_shutdown(self) -> ShuttingDown {
        let _ignore_send_error = self.shutdown_tx.send(ShutdownSignal::Graceful);
        ShuttingDown(())
    }

    /// Initiates a hard (fast) shutdown by sending SIGINT to postgres.
    ///
    /// Postgres aborts active transactions, disconnects clients, then exits.
    /// If the process does not exit within the timeout, the background future
    /// escalates to SIGKILL.
    ///
    /// Consumes the handle — the caller cannot interact with the service after
    /// initiating shutdown.
    pub fn force_shutdown(self) -> ShuttingDown {
        let _ignore_send_error = self.shutdown_tx.send(ShutdownSignal::Hard);
        ShuttingDown(())
    }
}
