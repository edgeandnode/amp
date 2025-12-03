//! Daemon worker fixture for isolated test environments.
//!
//! This fixture module provides the `DaemonWorkerFixture` type for managing Amp worker
//! instances in test environments. It handles worker lifecycle, task management, and provides
//! convenient configuration options for worker nodes.

use std::{str::FromStr as _, sync::Arc};

use common::BoxError;
use dataset_store::{
    DatasetStore, manifests::DatasetManifestsStore, providers::ProviderConfigsStore,
};
use metadata_db::MetadataDb;
use tokio::task::JoinHandle;
use worker::{config::Config, node_id::NodeId, service::RuntimeError as WorkerRuntimeError};

/// Fixture for managing Amp daemon worker instances in tests.
///
/// This fixture wraps a running Amp worker instance and provides convenient configuration
/// and lifecycle management. The fixture automatically handles worker lifecycle and cleanup
/// by aborting the worker task when dropped.
pub struct DaemonWorker {
    node_id: NodeId,
    worker_config: worker::config::Config,
    dataset_store: Arc<DatasetStore>,
    _worker_task: JoinHandle<Result<(), WorkerRuntimeError>>,
}

impl DaemonWorker {
    /// Create and start a new Amp worker for testing.
    ///
    /// Starts a Amp worker with the provided configuration, metadata database, and worker ID.
    /// The worker will be automatically shut down when the fixture is dropped.
    pub async fn new(
        node_id: NodeId,
        config: Arc<common::config::Config>,
        metadb: MetadataDb,
        meter: Option<monitoring::telemetry::metrics::Meter>,
    ) -> Result<Self, BoxError> {
        let dataset_store = {
            let provider_configs_store =
                ProviderConfigsStore::new(config.providers_store.prefixed_store());
            let dataset_manifests_store =
                DatasetManifestsStore::new(config.manifests_store.prefixed_store());
            DatasetStore::new(
                metadb.clone(),
                provider_configs_store,
                dataset_manifests_store,
            )
        };

        // Two-phase worker initialization
        let worker_config = worker_config_from_common(&config);
        let worker_fut =
            worker::service::new(worker_config.clone(), metadb, meter, node_id.clone())
                .await
                .map_err(Box::new)?;
        let worker_task = tokio::spawn(worker_fut);

        Ok(Self {
            node_id,
            worker_config,
            dataset_store,
            _worker_task: worker_task,
        })
    }

    /// Create and start a new Amp worker with a generated worker ID.
    ///
    /// Convenience method that creates a worker with a worker ID based on the provided name.
    /// This is useful for tests that don't need to specify a custom WorkerNodeId.
    pub async fn new_with_name(
        config: Arc<common::config::Config>,
        metadata_db: MetadataDb,
        worker_name: &str,
    ) -> Result<Self, BoxError> {
        let worker_node_id = NodeId::from_str(worker_name)
            .map_err(|err| format!("Invalid worker name '{}': {}", worker_name, err))?;

        Self::new(worker_node_id, config, metadata_db, None).await
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
        &self.worker_config
    }

    /// Get the dataset store used by the worker.
    pub fn dataset_store(&self) -> &Arc<DatasetStore> {
        &self.dataset_store
    }
}

impl Drop for DaemonWorker {
    fn drop(&mut self) {
        tracing::debug!(worker_id = %self.node_id, "Aborting daemon worker task");
        self._worker_task.abort();
    }
}

/// Convert common::config::Config to worker::config::Config for tests
fn worker_config_from_common(config: &common::config::Config) -> Config {
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
