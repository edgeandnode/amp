//! Daemon controller fixture for isolated test environments.
//!
//! This fixture module provides the `DaemonController` type for managing Amp controller
//! instances in test environments. It handles controller lifecycle, task management, and provides
//! convenient access to the Admin API endpoint.

use std::{net::SocketAddr, sync::Arc};

use common::{BoxError, BoxResult, store::Store};
use controller::config::Config;
use opentelemetry::metrics::Meter;
use tokio::task::JoinHandle;

/// Fixture for managing Amp daemon controller instances in tests.
///
/// This fixture wraps a running Amp controller instance and provides convenient access
/// to the Admin API endpoint. The fixture automatically handles controller lifecycle
/// and cleanup by aborting the controller task when dropped.
pub struct DaemonController {
    config: Arc<Config>,
    metadata_db: metadata_db::MetadataDb,
    dataset_store: dataset_store::DatasetStore,
    admin_api_addr: SocketAddr,

    _task: JoinHandle<BoxResult<()>>,
}

impl DaemonController {
    /// Create and start a new Amp controller for testing.
    ///
    /// Starts a Amp controller with the provided configuration and metadata database.
    /// The controller will be automatically shut down when the fixture is dropped.
    pub async fn new(
        config: Arc<amp_config::Config>,
        metadata_db: metadata_db::MetadataDb,
        data_store: Store,
        dataset_store: dataset_store::DatasetStore,
        meter: Option<Meter>,
    ) -> Result<Self, BoxError> {
        // Convert common config to controller config
        let admin_api_addr = config.addrs.admin_api_addr;
        let config = Arc::new(controller_config_from_common(&config));

        let (admin_api_addr, controller_server) = controller::service::new(
            config.clone(),
            metadata_db.clone(),
            data_store,
            dataset_store.clone(),
            meter,
            admin_api_addr,
        )
        .await?;

        let controller_task = tokio::spawn(controller_server);

        Ok(Self {
            config,
            metadata_db,
            dataset_store,
            admin_api_addr,
            _task: controller_task,
        })
    }

    /// Get the controller-specific configuration.
    ///
    /// Returns the `controller::config::Config` that can be used directly with
    /// controller-related operations.
    pub fn config(&self) -> &Arc<Config> {
        &self.config
    }

    /// Get a reference to the metadata database.
    pub fn metadata_db(&self) -> &metadata_db::MetadataDb {
        &self.metadata_db
    }

    /// Get a reference to the dataset store.
    pub fn dataset_store(&self) -> &dataset_store::DatasetStore {
        &self.dataset_store
    }

    /// Get the Admin API server address.
    pub fn admin_api_addr(&self) -> SocketAddr {
        self.admin_api_addr
    }

    /// Get the Admin API server URL.
    pub fn admin_api_url(&self) -> String {
        format!("http://{}", self.admin_api_addr)
    }
}

impl Drop for DaemonController {
    fn drop(&mut self) {
        tracing::debug!("Aborting daemon controller task");
        self._task.abort();
    }
}

/// Convert common config to controller config
fn controller_config_from_common(config: &amp_config::Config) -> Config {
    Config {
        build_info: config.build_info.clone(),
    }
}
