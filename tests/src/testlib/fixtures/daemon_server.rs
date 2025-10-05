//! Daemon server fixture for isolated test environments.
//!
//! This fixture module provides the `DaemonServerFixture` type for managing Nozzle server
//! instances in test environments. It handles server lifecycle, task management, and provides
//! convenient access to server endpoints and addresses.

use std::{net::SocketAddr, sync::Arc};

use common::{BoxError, BoxResult, config::Config};
use dataset_store::{
    DatasetStore, manifests::DatasetManifestsStore, providers::ProviderConfigsStore,
};
use metadata_db::MetadataDb;
use nozzle::server::BoundAddrs;
use tokio::task::JoinHandle;

/// Fixture for managing Nozzle daemon server instances in tests.
///
/// This fixture wraps a running Nozzle server instance and provides convenient access
/// to server endpoints and addresses. The fixture automatically handles server lifecycle
/// and cleanup by aborting the server task when dropped.
pub struct DaemonServer {
    config: Arc<Config>,
    server_addrs: BoundAddrs,
    dataset_store: Arc<DatasetStore>,
    _server_task: JoinHandle<BoxResult<()>>,
}

impl DaemonServer {
    /// Create and start a new Nozzle server for testing.
    ///
    /// Starts a Nozzle server with the provided configuration and metadata database.
    /// The server will be automatically shut down when the fixture is dropped.
    pub async fn new(
        config: Arc<Config>,
        metadb: MetadataDb,
        enable_flight: bool,
        enable_jsonl: bool,
        enable_admin_api: bool,
    ) -> Result<Self, BoxError> {
        let dataset_store = {
            let provider_configs_store =
                ProviderConfigsStore::new(config.providers_store.prefixed_store());
            let dataset_manifests_store = DatasetManifestsStore::new(
                metadb.clone(),
                config.dataset_defs_store.prefixed_store(),
            );
            DatasetStore::new(
                metadb.clone(),
                provider_configs_store,
                dataset_manifests_store,
            )
        };

        let (server_addrs, server) = nozzle::server::run(
            config.clone(),
            metadb,
            enable_flight,
            enable_jsonl,
            enable_admin_api,
        )
        .await?;

        let server_task = tokio::spawn(server);

        Ok(Self {
            config,
            dataset_store,
            server_addrs,
            _server_task: server_task,
        })
    }

    /// Create and start a new Nozzle server with all services enabled.
    ///
    /// Convenience method that starts a server with all services (Flight, JSON Lines,
    /// and Admin API) enabled.
    pub async fn new_with_all_services(
        config: Arc<Config>,
        metadata_db: MetadataDb,
    ) -> Result<Self, BoxError> {
        Self::new(config, metadata_db, true, true, true).await
    }

    /// Get the server configuration.
    pub fn config(&self) -> &Arc<Config> {
        &self.config
    }

    /// Get the dataset store used by the server.
    pub fn dataset_store(&self) -> &Arc<DatasetStore> {
        &self.dataset_store
    }

    /// Get the Flight server address.
    pub fn flight_server_addr(&self) -> SocketAddr {
        self.server_addrs.flight_addr
    }

    /// Get the Flight server URL.
    pub fn flight_server_url(&self) -> String {
        format!("grpc://{}", self.server_addrs.flight_addr)
    }

    /// Get the JSON Lines server address.
    pub fn jsonl_server_addr(&self) -> SocketAddr {
        self.server_addrs.jsonl_addr
    }

    /// Get the JSON Lines server URL.
    pub fn jsonl_server_url(&self) -> String {
        format!("http://{}", self.server_addrs.jsonl_addr)
    }

    /// Get the admin API server address.
    pub fn admin_api_server_addr(&self) -> SocketAddr {
        self.server_addrs.admin_api_addr
    }

    /// Get the admin API server URL.
    pub fn admin_api_server_url(&self) -> String {
        format!("http://{}", self.server_addrs.admin_api_addr)
    }

    /// Get the bound addresses for all server endpoints.
    ///
    /// Returns the complete BoundAddrs structure containing all server socket addresses.
    /// This is useful for creating CLI fixtures and other components that need to connect
    /// to multiple server endpoints.
    pub fn bound_addrs(&self) -> BoundAddrs {
        self.server_addrs
    }
}

impl Drop for DaemonServer {
    fn drop(&mut self) {
        tracing::debug!("Aborting daemon server task");
        self._server_task.abort();
    }
}
