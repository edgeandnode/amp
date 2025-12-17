use std::{future::Future, path::PathBuf};

use pgtemp::PgTempDBBuilder;
use tokio::sync::mpsc;

/// Creates a new temporary PostgreSQL database service
///
/// The returned future must be spawned to keep the database alive. When the future
/// is dropped, the database is shut down and data is deleted (unless `keep` is true).
pub fn new(keep: bool) -> (Handle, impl Future<Output = ()>) {
    let pg_temp = PgTempDBBuilder::new()
        .with_initdb_arg("locale", "C")
        .persist_data(keep)
        .start();

    let data_dir = pg_temp.data_dir();
    tracing::info!("initializing temp database at: {}", data_dir.display());

    let url = pg_temp.connection_uri();
    tracing::info!("temp database connection URL: {}", url);

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

        // When we exit, pg_temp is dropped and cleanup happens
        // NOTE: Explicit drop ensures `pg_temp` is moved into this async block,
        // tying database cleanup to the future's lifetime.
        drop(pg_temp);
    };

    (handle, fut)
}

/// Handle to interact with the temporary database service
///
/// The database is automatically stopped when this handle is dropped (channel closure
/// signals the service to shut down) or when `stop()` is explicitly called.
pub struct Handle {
    url: String,
    data_dir: PathBuf,
    tx_stop: mpsc::Sender<()>,
}

impl Handle {
    /// Gets the connection URI for connecting to this temporary database
    pub fn url(&self) -> &str {
        &self.url
    }

    /// Gets the data directory where PostgreSQL stores its files
    pub fn data_dir(&self) -> &std::path::Path {
        &self.data_dir
    }

    /// Stops the service and triggers database cleanup
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
