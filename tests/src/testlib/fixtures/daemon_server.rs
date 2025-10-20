//! Daemon server fixture for isolated test environments.
//!
//! This fixture module provides the `DaemonServer` type for managing Amp server
//! instances in test environments. It handles server lifecycle, task management, and provides
//! convenient access to query server endpoints (Flight and JSON Lines).

use std::{net::SocketAddr, sync::Arc};

use common::{BoxError, BoxResult, config::Config};
use dataset_store::{
    DatasetStore, manifests::DatasetManifestsStore, providers::ProviderConfigsStore,
};
use metadata_db::MetadataDb;
use server::BoundAddrs;
use tokio::task::JoinHandle;

/// Fixture for managing Amp daemon server instances in tests.
///
/// This fixture wraps a running Amp server instance and provides convenient access
/// to query server endpoints (Arrow Flight and JSON Lines). The fixture automatically
/// handles server lifecycle and cleanup by aborting the server task when dropped.
///
/// Note: For Admin API access, use the `DaemonController` fixture instead.
pub struct DaemonServer {
    config: Arc<Config>,
    server_addrs: BoundAddrs,
    dataset_store: Arc<DatasetStore>,
    _server_task: JoinHandle<BoxResult<()>>,
}

impl DaemonServer {
    /// Create and start a new Amp server for testing.
    ///
    /// Starts a Amp server with the provided configuration and metadata database.
    /// Only query servers (Flight and JSON Lines) are enabled. For Admin API,
    /// use the `DaemonController` fixture.
    /// The server will be automatically shut down when the fixture is dropped.
    pub async fn new(
        config: Arc<Config>,
        metadb: MetadataDb,
        enable_flight: bool,
        enable_jsonl: bool,
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

        // For tests, leak the meter to get a 'static reference
        // This is acceptable in tests since they're short-lived
        let meter_ref: Option<&'static monitoring::telemetry::metrics::Meter> =
            meter.map(|m| Box::leak(Box::new(m)) as &'static _);

        let (server_addrs, server) = server::serve(
            config.clone(),
            metadb,
            enable_flight,
            enable_jsonl,
            meter_ref,
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

    /// Create and start a new Amp server with all query services enabled.
    ///
    /// Convenience method that starts a server with both query services
    /// (Flight and JSON Lines) enabled. For Admin API, use `DaemonController`.
    pub async fn new_with_all_services(
        config: Arc<Config>,
        metadata_db: MetadataDb,
        meter: Option<monitoring::telemetry::metrics::Meter>,
    ) -> Result<Self, BoxError> {
        Self::new(config, metadata_db, true, true, meter).await
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

    /// Get the bound addresses for all query server endpoints.
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
