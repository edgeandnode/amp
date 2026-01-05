//! Daemon server fixture for isolated test environments.
//!
//! This fixture module provides the `DaemonServer` type for managing Amp server
//! instances in test environments. It handles server lifecycle, task management, and provides
//! convenient access to query server endpoints (Flight and JSON Lines).

use std::{net::SocketAddr, sync::Arc};

use amp_dataset_store::DatasetStore;
use common::{BoxError, store::Store};
use metadata_db::MetadataDb;
use opentelemetry::metrics::Meter;
use server::{config::Config, service::BoundAddrs};
use tokio::task::JoinHandle;

/// Fixture for managing Amp daemon server instances in tests.
///
/// This fixture wraps a running Amp server instance and provides convenient access
/// to query server endpoints (Arrow Flight and JSON Lines). The fixture automatically
/// handles server lifecycle and cleanup by aborting the server task when dropped.
pub struct DaemonServer {
    config: Arc<Config>,
    metadata_db: MetadataDb,
    data_store: Store,
    dataset_store: DatasetStore,
    server_addrs: BoundAddrs,

    _task: JoinHandle<Result<(), BoxError>>,
}

impl DaemonServer {
    /// Create and start a new Amp server for testing.
    ///
    /// Starts a Amp server with the provided configuration and metadata database.
    /// Only query servers (Flight and JSON Lines) are enabled. For Admin API,
    /// use the `DaemonController` fixture.
    /// The server will be automatically shut down when the fixture is dropped.
    pub async fn new(
        config: Arc<amp_config::Config>,
        metadb: MetadataDb,
        data_store: Store,
        dataset_store: DatasetStore,
        meter: Option<Meter>,
        enable_flight: bool,
        enable_jsonl: bool,
    ) -> Result<Self, BoxError> {
        let flight_at = if enable_flight {
            Some(config.addrs.flight_addr)
        } else {
            None
        };
        let jsonl_at = if enable_jsonl {
            Some(config.addrs.jsonl_addr)
        } else {
            None
        };

        let config = Arc::new(server_config_from_common(&config));
        let (server_addrs, server) = server::service::new(
            config.clone(),
            metadb.clone(),
            data_store.clone(),
            dataset_store.clone(),
            meter,
            flight_at,
            jsonl_at,
        )
        .await?;

        let server_task = tokio::spawn(server);

        Ok(Self {
            config,
            metadata_db: metadb,
            data_store,
            dataset_store,
            server_addrs,
            _task: server_task,
        })
    }

    /// Get the server-specific configuration.
    ///
    /// Returns the `server::config::Config` that can be used directly with
    /// server-related operations.
    pub fn config(&self) -> &Arc<Config> {
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

    /// Get the Flight server address.
    pub fn flight_server_addr(&self) -> SocketAddr {
        self.server_addrs
            .flight_addr
            .expect("Flight server was not started")
    }

    /// Get the Flight server URL.
    pub fn flight_server_url(&self) -> String {
        format!("grpc://{}", self.flight_server_addr())
    }

    /// Get the JSON Lines server address.
    pub fn jsonl_server_addr(&self) -> SocketAddr {
        self.server_addrs
            .jsonl_addr
            .expect("JSONL server was not started")
    }

    /// Get the JSON Lines server URL.
    pub fn jsonl_server_url(&self) -> String {
        format!("http://{}", self.jsonl_server_addr())
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
        self._task.abort();
    }
}

/// Convert config::Config to server::config::Config
fn server_config_from_common(config: &amp_config::Config) -> Config {
    Config {
        server_microbatch_max_interval: config.server_microbatch_max_interval,
        keep_alive_interval: config.keep_alive_interval,
        max_mem_mb: config.max_mem_mb,
        query_max_mem_mb: config.query_max_mem_mb,
        spill_location: config.spill_location.clone(),
        parquet_cache_size_mb: config.parquet.cache_size_mb,
    }
}
