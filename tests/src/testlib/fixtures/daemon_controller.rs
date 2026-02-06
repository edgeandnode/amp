//! Daemon controller fixture for isolated test environments.
//!
//! This fixture module provides the `DaemonController` type for managing Amp controller
//! instances in test environments. It handles controller lifecycle, task management, and provides
//! convenient access to the Admin API endpoint.

use std::{net::SocketAddr, sync::Arc};

use amp_data_store::DataStore;
use amp_dataset_store::DatasetStore;
use amp_datasets_registry::DatasetsRegistry;
use amp_providers_registry::ProvidersRegistry;
use anyhow::Result;
use metadata_db::MetadataDb;
use opentelemetry::metrics::Meter;
use tokio::task::JoinHandle;

use crate::testlib::build_info::BuildInfo;

/// Fixture for managing Amp daemon controller instances in tests.
///
/// This fixture wraps a running Amp controller instance and provides convenient access
/// to the Admin API endpoint. The fixture automatically handles controller lifecycle
/// and cleanup by aborting the controller task when dropped.
pub struct DaemonController {
    metadata_db: MetadataDb,
    dataset_store: DatasetStore,
    admin_api_addr: SocketAddr,

    _task: JoinHandle<Result<(), controller::service::ServerError>>,
}

impl DaemonController {
    /// Create and start a new Amp controller for testing.
    ///
    /// Starts a Amp controller with the provided configuration and metadata database.
    /// The controller will be automatically shut down when the fixture is dropped.
    #[allow(clippy::too_many_arguments)]
    pub async fn new(
        build_info: BuildInfo,
        config: Arc<amp_config::Config>,
        metadata_db: MetadataDb,
        datasets_registry: DatasetsRegistry,
        providers_registry: ProvidersRegistry,
        data_store: DataStore,
        dataset_store: DatasetStore,
        meter: Option<Meter>,
    ) -> Result<Self> {
        let admin_api_addr = config.controller_addrs.admin_api_addr;

        let (admin_api_addr, controller_server) = controller::service::new(
            build_info,
            metadata_db.clone(),
            datasets_registry,
            providers_registry,
            data_store,
            dataset_store.clone(),
            meter,
            admin_api_addr,
        )
        .await?;

        let controller_task = tokio::spawn(controller_server);

        Ok(Self {
            metadata_db,
            dataset_store,
            admin_api_addr,
            _task: controller_task,
        })
    }

    /// Get a reference to the metadata database.
    pub fn metadata_db(&self) -> &MetadataDb {
        &self.metadata_db
    }

    /// Get a reference to the dataset store.
    pub fn dataset_store(&self) -> &DatasetStore {
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

impl From<BuildInfo> for controller::build_info::BuildInfo {
    fn from(value: BuildInfo) -> Self {
        Self {
            version: value.version,
            commit_sha: value.commit_sha,
            commit_timestamp: value.commit_timestamp,
            build_date: value.build_date,
        }
    }
}
