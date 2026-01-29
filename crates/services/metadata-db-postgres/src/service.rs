use std::{future::Future, path::PathBuf};

use tokio::sync::mpsc;

use crate::{error::PostgresError, postgres::PostgresProcess};

/// Creates a new persistent PostgreSQL database service
///
/// This function starts a PostgreSQL server using the specified data directory.
/// If the data directory already contains a PostgreSQL database (detected by
/// the presence of `PG_VERSION` file), the existing data is reused. Otherwise,
/// a new database cluster is initialized.
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
/// - A future that must be spawned to keep the database running
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
/// // Spawn the service future
/// tokio::spawn(fut);
///
/// // Use the database
/// let url = handle.url();
/// let db = MetadataDb::connect(url).await?;
///
/// // Shutdown when done
/// handle.stop().await;
/// ```
pub async fn new(data_dir: PathBuf) -> Result<(Handle, impl Future<Output = ()>), PostgresError> {
    let process = PostgresProcess::start(data_dir.clone()).await?;

    tracing::info!(
        data_dir = %data_dir.display(),
        url = %process.connection_url(),
        "PostgreSQL database ready"
    );

    let url = process.connection_url().to_string();
    let (tx_stop, mut rx_stop) = mpsc::channel(1);

    let handle = Handle {
        url,
        data_dir,
        tx_stop,
    };

    // Future that runs the service
    let fut = async move {
        // Wait for stop signal
        let _ = rx_stop.recv().await;

        // Gracefully shut down PostgreSQL
        if let Err(err) = process.shutdown().await {
            tracing::warn!(error = %err, "Error during PostgreSQL shutdown");
        }
    };

    Ok((handle, fut))
}

/// Handle to interact with the PostgreSQL database service
///
/// The database is automatically stopped when this handle is dropped (channel closure
/// signals the service to shut down) or when `stop()` is explicitly called.
pub struct Handle {
    url: String,
    data_dir: PathBuf,
    tx_stop: mpsc::Sender<()>,
}

impl Handle {
    /// Gets the connection URL for connecting to this database
    ///
    /// The URL uses Unix socket connections in the format:
    /// `postgresql:///postgres?host=/path/to/data_dir`
    pub fn url(&self) -> &str {
        &self.url
    }

    /// Gets the data directory where PostgreSQL stores its files
    pub fn data_dir(&self) -> &std::path::Path {
        &self.data_dir
    }

    /// Stops the service and shuts down the PostgreSQL server
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
