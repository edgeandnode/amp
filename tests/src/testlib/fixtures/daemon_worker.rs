//! Daemon worker fixture for isolated test environments.
//!
//! This fixture module provides the `DaemonWorkerFixture` type for managing Amp worker
//! instances in test environments. It handles worker lifecycle, task management, and provides
//! convenient configuration options for worker nodes.

use std::sync::Arc;

use common::{BoxError, store::Store};
use dataset_store::DatasetStore;
use metadata_db::MetadataDb;
use opentelemetry::metrics::Meter;
use tokio::task::JoinHandle;
use worker::{config::Config, node_id::NodeId, service::RuntimeError as WorkerRuntimeError};

/// Fixture for managing Amp daemon worker instances in tests.
///
/// This fixture wraps a running Amp worker instance and provides convenient configuration
/// and lifecycle management. The fixture automatically handles worker lifecycle and cleanup
/// by aborting the worker task when dropped.
pub struct DaemonWorker {
    config: Config,
    metadata_db: MetadataDb,
    data_store: Store,
    dataset_store: DatasetStore,
    node_id: NodeId,

    _task: JoinHandle<Result<(), WorkerRuntimeError>>,
}

impl DaemonWorker {
    /// Create and start a new Amp worker for testing.
    ///
    /// Starts a Amp worker with the provided configuration, metadata database, and worker ID.
    /// The worker will be automatically shut down when the fixture is dropped.
    pub async fn new(
        config: Arc<config::Config>,
        metadata_db: MetadataDb,
        data_store: Store,
        dataset_store: DatasetStore,
        meter: Option<Meter>,
        node_id: NodeId,
    ) -> Result<Self, BoxError> {
        // Two-phase worker initialization
        let worker_config = worker_config_from_common(&config);
        let worker_fut = worker::service::new(
            worker_config.clone(),
            metadata_db.clone(),
            data_store.clone(),
            dataset_store.clone(),
            meter,
            node_id.clone(),
        )
        .await
        .map_err(Box::new)?;
        let worker_task = tokio::spawn(worker_fut);

        Ok(Self {
            config: worker_config,
            metadata_db,
            data_store,
            dataset_store,
            node_id,
            _task: worker_task,
        })
    }

    /// Get the worker node ID.
    pub fn node_id(&self) -> &NodeId {
        &self.node_id
    }

    /// Get the worker-specific configuration.
    ///
    /// Returns the `worker::config::Config` that can be used directly with
    /// worker-related operations like `dump_internal` and `dump_dataset`.
    pub fn config(&self) -> &Config {
        &self.config
    }

    /// Get a reference to the metadata database.
    pub fn metadata_db(&self) -> &MetadataDb {
        &self.metadata_db
    }

    /// Get a reference to the data store.
    pub fn data_store(&self) -> &Store {
        &self.data_store
    }

    /// Get a reference to the dataset store.
    pub fn dataset_store(&self) -> &DatasetStore {
        &self.dataset_store
    }
}

impl Drop for DaemonWorker {
    fn drop(&mut self) {
        tracing::debug!(worker_id = %self.node_id, "Aborting daemon worker task");
        self._task.abort();
    }
}

/// Convert config::Config to worker::config::Config for tests
fn worker_config_from_common(config: &config::Config) -> Config {
    Config {
        microbatch_max_interval: config.microbatch_max_interval,
        poll_interval: config.poll_interval,
        keep_alive_interval: config.keep_alive_interval,
        max_mem_mb: config.max_mem_mb,
        query_max_mem_mb: config.query_max_mem_mb,
        spill_location: config.spill_location.clone(),
        parquet: config.parquet.clone(),
        worker_info: worker::info::WorkerInfo {
            version: Some(config.build_info.version.clone()),
            commit_sha: Some(config.build_info.commit_sha.clone()),
            commit_timestamp: Some(config.build_info.commit_timestamp.clone()),
            build_date: Some(config.build_info.build_date.clone()),
        },
    }
}
