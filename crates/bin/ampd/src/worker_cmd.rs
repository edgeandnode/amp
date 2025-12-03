use common::config::Config;
use metadata_db::MetadataDb;
use monitoring::telemetry::metrics::Meter;
use worker::node_id::NodeId;

pub async fn run(
    config: Config,
    metadata_db: MetadataDb,
    meter: Option<Meter>,
    node_id: NodeId,
) -> Result<(), Error> {
    // Convert common config to worker-specific config
    let worker_config = config_from_common(&config);

    // Initialize the worker (setup phase)
    let worker_fut = worker::service::new(worker_config, metadata_db, meter, node_id)
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
    #[error("Worker initialization failed")]
    Init(#[source] worker::service::InitError),

    /// Worker runtime error.
    ///
    /// This occurs during the worker's main event loop after successful initialization.
    #[error("Worker runtime error: {0}")]
    Runtime(#[source] worker::service::RuntimeError),
}

/// Convert common::config::Config to worker::config::Config
pub(crate) fn config_from_common(config: &Config) -> worker::config::Config {
    worker::config::Config {
        microbatch_max_interval: config.microbatch_max_interval,
        poll_interval: config.poll_interval,
        keep_alive_interval: config.keep_alive_interval,
        max_mem_mb: config.max_mem_mb,
        query_max_mem_mb: config.query_max_mem_mb,
        spill_location: config.spill_location.clone(),
        parquet: config.parquet.clone(),
        data_store: config.data_store.clone(),
        providers_store: config.providers_store.clone(),
        manifests_store: config.manifests_store.clone(),
        worker_info: worker::info::WorkerInfo {
            version: Some(config.build_info.version.clone()),
            commit_sha: Some(config.build_info.commit_sha.clone()),
            commit_timestamp: Some(config.build_info.commit_timestamp.clone()),
            build_date: Some(config.build_info.build_date.clone()),
        },
    }
}
