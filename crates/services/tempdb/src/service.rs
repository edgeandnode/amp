use std::path::PathBuf;

use pgtemp::PgTempDBBuilder;
use tokio::sync::mpsc;

/// Create a new temporary PostgreSQL database service
///
/// Returns a handle to interact with the database and a future that represents
/// the service lifecycle. The future must be spawned to keep the service running.
pub fn new(keep: bool) -> (TempDbHandle, impl Future<Output = ()>) {
    // Set C locale. To remove this `unsafe` we need:
    // https://github.com/boustrophedon/pgtemp/pull/21
    unsafe {
        std::env::set_var("LANG", "C");
    }

    let pg_temp = PgTempDBBuilder::new().persist_data(keep).start();

    let data_dir = pg_temp.data_dir();
    tracing::info!("initializing temp database at: {}", data_dir.display());

    let url = pg_temp.connection_uri();
    tracing::info!("temp database connection URL: {}", url);

    let (tx_stop, mut rx_stop) = mpsc::channel(1);

    let handle = TempDbHandle {
        url,
        data_dir,
        tx_stop,
    };

    // Future that runs the service
    let fut = async move {
        // Wait for stop signal
        let _ = rx_stop.recv().await;

        // When we exit, pg_temp is dropped and cleanup happens
        drop(pg_temp);
    };

    (handle, fut)
}

/// Handle to interact with the temporary database service
pub struct TempDbHandle {
    url: String,
    data_dir: PathBuf,
    tx_stop: mpsc::Sender<()>,
}

impl TempDbHandle {
    /// Get the connection URI of the temporary database
    pub fn url(&self) -> &str {
        &self.url
    }

    /// Get the data directory of the temporary database
    pub fn data_dir(&self) -> &std::path::Path {
        &self.data_dir
    }

    /// Stop the service
    pub async fn stop(self) {
        if self.tx_stop.is_closed() {
            return;
        }

        let _ = self.tx_stop.send(()).await;

        // Wait for the channel to close
        self.tx_stop.closed().await;
    }
}
