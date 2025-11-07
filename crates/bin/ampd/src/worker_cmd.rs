use std::sync::Arc;

use common::config::Config;
use metadata_db::MetadataDb;
use worker::NodeId;

pub async fn run(
    config: Config,
    metadata_db: MetadataDb,
    node_id: NodeId,
    meter: Option<monitoring::telemetry::metrics::Meter>,
) -> Result<(), Error> {
    let config = Arc::new(config);

    // Initialize the worker (setup phase)
    let worker_fut = worker::service::new(node_id, config, metadata_db, meter)
        .await
        .map_err(Error::Init)?;

    // Run the worker (runtime phase)
    worker_fut.await.map_err(Error::Runtime)
}

/// Errors that can occur during worker execution.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Worker initialization failed.
    ///
    /// This occurs during the initialization phase (registration, heartbeat
    /// setup, notification listener setup, or bootstrap).
    #[error("Worker initialization failed: {0}")]
    Init(#[source] worker::InitError),

    /// Worker runtime error.
    ///
    /// This occurs during the worker's main event loop after successful initialization.
    #[error("Worker runtime error: {0}")]
    Runtime(#[source] worker::RuntimeError),
}
