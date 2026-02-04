use std::{future::Future, path::PathBuf, pin::Pin, sync::Arc};

use amp_config::{ConfigDefaultsOverride, DEFAULT_CONFIG_FILENAME, DEFAULT_METADB_DIRNAME};
use amp_data_store::DataStore;
use amp_dataset_store::DatasetStore;
use amp_datasets_registry::{DatasetsRegistry, manifests::DatasetManifestsStore};
use amp_object_store::ObjectStoreCreationError;
use amp_providers_registry::{ProviderConfigsStore, ProvidersRegistry};

use crate::{build_info, server_cmd, worker_cmd};

/// Type alias for the PostgreSQL service future to reduce type complexity
type PostgresFuture =
    Pin<Box<dyn Future<Output = Result<(), metadata_db_postgres::PostgresError>> + Send>>;

/// Type alias for the controller service future to reduce type complexity
type ControllerFuture =
    Pin<Box<dyn Future<Output = Result<(), controller::service::ServerError>> + Send>>;

/// Type alias for the server service future to reduce type complexity
type ServerFuture =
    Pin<Box<dyn Future<Output = Result<(), server::service::ServerRunError>> + Send>>;

pub async fn run(
    amp_dir: PathBuf,
    config_path: Option<PathBuf>,
    flight_server: bool,
    jsonl_server: bool,
    admin_server: bool,
) -> Result<(), Error> {
    // 1. Check if user provided metadata_db.url before starting managed PostgreSQL
    let effective_config_path = config_path
        .clone()
        .unwrap_or_else(|| amp_dir.join(DEFAULT_CONFIG_FILENAME));

    let user_provided_metadb_config = amp_config::metadb::load(&effective_config_path, None);

    // 2. Branch: external DB or managed pg
    let (metadata_db, pg_handle, mut pg_fut) = {
        let (config, pg_handle, pg_fut): (_, _, PostgresFuture) = match user_provided_metadb_config
        {
            Some(config) => {
                tracing::debug!("Using user-provided metadata_db.url, skipping managed PostgreSQL");
                (config, None, Box::pin(std::future::pending()))
            }
            None => {
                amp_config::ensure_amp_metadb_directory(&amp_dir).map_err(Error::AmpDirMetadb)?;
                let metadb_data_dir = amp_dir.join(DEFAULT_METADB_DIRNAME);
                let (handle, fut) = metadata_db_postgres::service::new(metadb_data_dir)
                    .await
                    .map_err(Error::PostgresStartup)?;

                let config = amp_config::metadb::load(&effective_config_path, Some(handle.url()))
                    .ok_or_else(|| Error::MetadataDbConfigLoad {
                    path: effective_config_path.clone(),
                })?;
                (config, Some(handle), Box::pin(fut))
            }
        };

        let metadata_db = metadata_db::connect_pool_with_config(
            config.url,
            config.pool_size,
            config.auto_migrate,
        )
        .await
        .map_err(Error::MetadataDbConnection)?;

        (metadata_db, pg_handle, pg_fut)
    };

    // 3. Load config
    let build_info = build_info::load();
    let config_path = config_path.unwrap_or_else(|| amp_dir.join(DEFAULT_CONFIG_FILENAME));
    let config = amp_config::load_config(
        config_path,
        [
            ConfigDefaultsOverride::data_dir(&amp_dir),
            ConfigDefaultsOverride::providers_dir(&amp_dir),
            ConfigDefaultsOverride::manifests_dir(&amp_dir),
        ],
    )
    .map_err(Error::ConfigLoad)?;

    // 4. Init monitoring
    let (_providers, meter) =
        monitoring::init(config.opentelemetry.as_ref()).map_err(Error::MonitoringInit)?;

    let worker_id = "worker".parse().expect("Invalid worker ID");

    let data_store = DataStore::new(
        metadata_db.clone(),
        config.data_store_url.clone(),
        config.parquet.cache_size_mb,
    )
    .map_err(Error::DataStoreCreation)?;

    let (datasets_registry, providers_registry) = {
        let provider_configs_store = ProviderConfigsStore::new(
            amp_object_store::new_with_prefix(
                &config.providers_store_url,
                config.providers_store_url.path(),
            )
            .map_err(Error::ProvidersStoreCreation)?,
        );
        let providers_registry = ProvidersRegistry::new(provider_configs_store);

        let dataset_manifests_store = DatasetManifestsStore::new(
            amp_object_store::new_with_prefix(
                &config.manifests_store_url,
                config.manifests_store_url.path(),
            )
            .map_err(Error::ManifestsStoreCreation)?,
        );
        let datasets_registry = DatasetsRegistry::new(metadata_db.clone(), dataset_manifests_store);
        (datasets_registry, providers_registry)
    };

    let dataset_store = DatasetStore::new(datasets_registry.clone(), providers_registry.clone());

    // Spawn controller (Admin API) if enabled
    let controller_fut: ControllerFuture = if admin_server {
        let (addr, fut) = controller::service::new(
            build_info.clone(),
            metadata_db.clone(),
            datasets_registry,
            providers_registry,
            data_store.clone(),
            dataset_store.clone(),
            meter.clone(),
            config.controller_addrs.admin_api_addr,
        )
        .await
        .map_err(Error::ServiceInit)?;

        tracing::info!("Controller Admin API running at {}", addr);
        Box::pin(fut)
    } else {
        Box::pin(std::future::pending())
    };

    // Spawn server only if at least one query server is enabled
    let server_fut: ServerFuture = if flight_server || jsonl_server {
        let flight_at = if flight_server {
            Some(config.server_addrs.flight_addr)
        } else {
            None
        };
        let jsonl_at = if jsonl_server {
            Some(config.server_addrs.jsonl_addr)
        } else {
            None
        };

        let server_config = server_cmd::config_from_common(&config);
        let (addrs, fut) = server::service::new(
            Arc::new(server_config),
            metadata_db.clone(),
            data_store.clone(),
            dataset_store.clone(),
            meter.clone(),
            flight_at,
            jsonl_at,
        )
        .await
        .map_err(Error::ServerRun)?;

        if let Some(addr) = addrs.flight_addr {
            tracing::info!("Arrow Flight RPC Server running at {}", addr);
        }
        if let Some(addr) = addrs.jsonl_addr {
            tracing::info!("JSON Lines Server running at {}", addr);
        }

        Box::pin(fut)
    } else {
        Box::pin(std::future::pending())
    };

    // Initialize worker
    let worker_config = worker_cmd::config_from_common(&config, &build_info);
    let worker_fut = worker::service::new(
        worker_config,
        metadata_db,
        data_store,
        dataset_store,
        meter,
        worker_id,
    )
    .await
    .map_err(Error::WorkerInit)?;

    // Phase 1: all services running — first signal or service exit wins
    tokio::select! {biased;
        _ = shutdown_signal() => {
            tracing::info!("Shutdown signal received");
        }
        res = &mut pg_fut => {
            // Only fires when managed; pending() never completes for external.
            res.map_err(Error::PostgresExited)?;
            return Ok(());
        }
        res = controller_fut => { res.map_err(Error::ControllerRuntime)?; return Ok(()); }
        res = worker_fut => { res.map_err(Error::WorkerRuntime)?; return Ok(()); }
        res = server_fut => { res.map_err(Error::ServerRuntime)?; return Ok(()); }
    }

    // Phase 2: graceful postgres shutdown (only when managed)
    if let Some(pg_handle) = pg_handle {
        tracing::info!("Initiating graceful postgres shutdown");
        let _shutting_down = pg_handle.graceful_shutdown();

        tokio::select! {
            res = &mut pg_fut => res.map_err(Error::PostgresExited)?,
            _ = shutdown_signal() => {
                tracing::warn!("Second shutdown signal received, forcing exit");
            }
        }
    }

    Ok(())
}

/// Returns a future that completes when a shutdown signal (SIGINT or SIGTERM)
/// is received.
///
/// On Unix, listens for both SIGINT (Ctrl+C) and SIGTERM. On other platforms,
/// listens for Ctrl+C only.
async fn shutdown_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};

        let mut sigint = signal(SignalKind::interrupt()).expect("Failed to install SIGINT handler");
        let mut sigterm =
            signal(SignalKind::terminate()).expect("Failed to install SIGTERM handler");

        tokio::select! {
            _ = sigint.recv() => tracing::info!(signal = "SIGINT", "shutdown signal"),
            _ = sigterm.recv() => tracing::info!(signal = "SIGTERM", "shutdown signal"),
        }
    }

    #[cfg(not(unix))]
    {
        tokio::signal::ctrl_c()
            .await
            .expect("Failed to install Ctrl+C handler");
        tracing::info!("shutdown signal");
    }
}

/// Errors that can occur during solo mode execution.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Failed to create the managed metadb directory inside the amp dir.
    #[error("Failed to create amp metadb directory: {0}")]
    AmpDirMetadb(#[source] amp_config::EnsureAmpMetadbDirectoryError),

    /// Failed to start managed PostgreSQL.
    #[error("Failed to start PostgreSQL: {0}")]
    PostgresStartup(#[source] metadata_db_postgres::PostgresError),

    /// Failed to load configuration from file.
    #[error("Failed to load config: {0}")]
    ConfigLoad(#[source] amp_config::LoadConfigError),

    /// Failed to load metadata database configuration.
    ///
    /// This occurs when the database configuration cannot be read. Check:
    /// - Configuration file syntax in the `[metadata_db]` section
    /// - Type correctness: `pool_size` should be a number, `auto_migrate` should be true/false
    /// - Environment variable values (if using `AMP_CONFIG_METADATA_DB__*`)
    #[error("Invalid database configuration (check config file or environment variables)")]
    MetadataDbConfigLoad {
        /// The config file path that was being loaded
        path: PathBuf,
    },

    /// Failed to initialize monitoring/telemetry.
    #[error("Failed to initialize monitoring: {0}")]
    MonitoringInit(#[source] monitoring::telemetry::ExporterBuildError),

    /// Failed to connect to metadata database
    ///
    /// This occurs when the solo command cannot establish a connection to the
    /// PostgreSQL metadata database.
    #[error("Failed to connect to metadata database")]
    MetadataDbConnection(#[source] metadata_db::Error),

    /// Failed to create data store
    ///
    /// This occurs when the data store cannot be created from the configured URL.
    #[error("Failed to create data store: {0}")]
    DataStoreCreation(#[source] ObjectStoreCreationError),

    /// Failed to create providers store
    ///
    /// This occurs when the providers store cannot be created from the configured URL.
    #[error("Failed to create providers store: {0}")]
    ProvidersStoreCreation(#[source] ObjectStoreCreationError),

    /// Failed to create manifests store
    ///
    /// This occurs when the manifests store cannot be created from the configured URL.
    #[error("Failed to create manifests store: {0}")]
    ManifestsStoreCreation(#[source] ObjectStoreCreationError),

    /// Failed to initialize the controller service (Admin API).
    ///
    /// This occurs during the initialization phase when attempting to bind and
    /// start the Admin API server.
    #[error("Failed to initialize controller service: {0}")]
    ServiceInit(#[source] controller::service::Error),

    /// Failed to start the query server (Arrow Flight RPC and/or JSON Lines).
    ///
    /// This occurs during the initialization phase when attempting to bind and
    /// start the query servers.
    #[error("Failed to start server: {0}")]
    ServerRun(#[source] server::service::InitError),

    /// Failed to initialize the worker service.
    ///
    /// This occurs during the worker initialization phase (registration, heartbeat
    /// setup, notification listener setup, or bootstrap).
    #[error("Failed to initialize worker: {0}")]
    WorkerInit(#[source] worker::service::InitError),

    /// Managed PostgreSQL exited unexpectedly.
    ///
    /// This occurs when the managed PostgreSQL process terminates while the solo
    /// command is still running. This is detected via the structured concurrency
    /// pattern — the postgres future is a `select!` branch in the solo command.
    #[error("PostgreSQL exited: {0}")]
    PostgresExited(#[source] metadata_db_postgres::PostgresError),

    /// Controller service (Admin API) encountered a runtime error.
    ///
    /// This occurs after the Admin API server has started successfully but
    /// encounters an error during operation.
    #[error("Controller runtime error: {0}")]
    ControllerRuntime(#[source] controller::service::ServerError),

    /// Worker encountered a runtime error.
    ///
    /// This occurs when the worker process encounters an error during operation.
    #[error("Worker runtime error: {0}")]
    WorkerRuntime(#[source] worker::service::RuntimeError),

    /// Query server encountered a runtime error.
    ///
    /// This occurs after the Arrow Flight RPC and/or JSON Lines servers have
    /// started successfully but encounter an error during operation.
    #[error("Server runtime error: {0}")]
    ServerRuntime(#[source] server::service::ServerRunError),
}
