//! Daemon worker fixture for isolated test environments.
//!
//! This fixture module provides the `DaemonWorkerFixture` type for managing Amp worker
//! instances in test environments. It handles worker lifecycle, task management, and provides
//! convenient configuration options for worker nodes.

use std::{str::FromStr as _, sync::Arc};

use common::{BoxError, config::Config};
use dataset_store::{
    DatasetStore, manifests::DatasetManifestsStore, providers::ProviderConfigsStore,
};
use metadata_db::MetadataDb;
use tokio::task::JoinHandle;
use worker::{Error as WorkerError, NodeId, Worker};

/// Fixture for managing Amp daemon worker instances in tests.
///
/// This fixture wraps a running Amp worker instance and provides convenient configuration
/// and lifecycle management. The fixture automatically handles worker lifecycle and cleanup
/// by aborting the worker task when dropped.
pub struct DaemonWorker {
    node_id: NodeId,
    config: Arc<Config>,
    dataset_store: Arc<DatasetStore>,
    _worker_task: JoinHandle<Result<(), WorkerError>>,
}

impl DaemonWorker {
    /// Create and start a new Amp worker for testing.
    ///
    /// Starts a Amp worker with the provided configuration, metadata database, and worker ID.
    /// The worker will be automatically shut down when the fixture is dropped.
    pub async fn new(
        node_id: NodeId,
        config: Arc<Config>,
        metadb: MetadataDb,
        meter: Option<monitoring::telemetry::metrics::Meter>,
    ) -> Result<Self, BoxError> {
        let dataset_store = {
            let provider_configs_store =
                ProviderConfigsStore::new(config.providers_store.prefixed_store());
            let dataset_manifests_store =
                DatasetManifestsStore::new(metadb.clone(), config.manifests_store.prefixed_store());
            DatasetStore::new(
                metadb.clone(),
                provider_configs_store,
                dataset_manifests_store,
            )
        };
        // Initialize the dataset store (scans the object store for manifests to preload)
        dataset_store.init().await;

        let worker = Worker::new(config.clone(), metadb, node_id.clone(), meter);

        let worker_task = tokio::spawn(worker.run());

        Ok(Self {
            node_id,
            config,
            dataset_store,
            _worker_task: worker_task,
        })
    }

    /// Create and start a new Amp worker with a generated worker ID.
    ///
    /// Convenience method that creates a worker with a worker ID based on the provided name.
    /// This is useful for tests that don't need to specify a custom WorkerNodeId.
    ///
    /// # Arguments
    ///
    /// * `config` - The configuration for the worker
    /// * `metadata_db` - The metadata database instance
    /// * `worker_name` - Name to use for generating the worker ID
    pub async fn new_with_name(
        config: Arc<Config>,
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

    /// Get the worker configuration.
    pub fn config(&self) -> &Arc<Config> {
        &self.config
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
