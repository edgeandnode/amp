mod batch_utils;
mod conn;
mod dataset_definition;
mod manifest;
mod pgpq;
mod sql_validator;
mod sync_engine;

use std::{env, path::PathBuf, sync::Arc};

use common::BoxError;
use datasets_derived::{Manifest, manifest::TableInput};
use futures::StreamExt;
use nozzle_client::{ResponseBatchWithReorg, SqlClient, with_reorg};
use tracing::{debug, error, info, warn};

use crate::{
    batch_utils::convert_nanosecond_timestamps,
    conn::{DEFAULT_POOL_SIZE, DbConnPool},
    sync_engine::AmpsyncDbEngine,
};

#[cfg(feature = "snmalloc")]
#[global_allocator]
static ALLOC: snmalloc_rs::SnMalloc = snmalloc_rs::SnMalloc;

#[tokio::main]
async fn main() {
    match ampsync_runner().await {
        Ok(()) => {}
        Err(e) => {
            // Manually print the error so we can control the format.
            eprintln!("Exiting with error: {e}");
            std::process::exit(1);
        }
    }
}

/// Grab the configuration object of the db to connect to, as well as the amp dataset.
/// Listen to changes on the dataset and sync those changes to the db instance.
///
/// This will run in tandem with both amp (nozzle) and with a sync engine such as with-electricsql-sql
/// to provide subsets of the nozzle dataset to build a reactive query layer for application development.
async fn ampsync_runner() -> Result<(), BoxError> {
    // Initialize logging
    monitoring::logging::init();

    info!("Starting ampsync...");

    let config = AmpsyncConfig::from_env().await?;
    info!(
        "Loaded manifest: {} v{}",
        config.manifest.name, config.manifest.version
    );

    // Connect to target database
    let db_pool = DbConnPool::connect(&config.database_url, DEFAULT_POOL_SIZE).await?;

    // Connect to Nozzle server
    let mut sql_client = SqlClient::new(&config.amp_flight_addr).await?;
    info!("Connected to Nozzle server at {}", config.amp_flight_addr);

    info!("Preparing to sync dataset: {}", config.manifest.name);

    let ampsync_db_engine = AmpsyncDbEngine::new(&db_pool);

    // Create a semaphore to limit concurrent batch processing across all tables
    // This prevents OOM when many tables receive large batches simultaneously
    // Default: Allow 10 concurrent batch operations (configurable via env)
    let max_concurrent_batches = env::var("MAX_CONCURRENT_BATCHES")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(10);
    let batch_semaphore = Arc::new(tokio::sync::Semaphore::new(max_concurrent_batches));

    // Process each table: create table and set up streaming
    for (table_name, table) in &config.manifest.tables {
        debug!("Processing table: {}", table_name);

        // Create the table based on the Arrow schema (uses IF NOT EXISTS)
        ampsync_db_engine
            .create_table_from_schema(table_name, &table.schema.arrow)
            .await?;

        // Get the SQL query from the table definition
        let sql_query = match &table.input {
            TableInput::View(view) => &view.sql,
        };

        // Add streaming settings to the query
        let streaming_query = format!("{} SETTINGS stream = true", sql_query);
        debug!(
            "Executing streaming query for '{}': {}",
            table_name, streaming_query
        );

        // Execute the query to get ResultStream
        let result_stream = match sql_client.query(&streaming_query, None, None).await {
            Ok(stream) => stream,
            Err(e) => {
                error!("Failed to execute query for table '{}': {}", table_name, e);
                continue;
            }
        };

        // Wrap with with_reorg and spawn a task to handle the stream
        // Use Arc<str> instead of cloning String - more efficient
        let table_name_arc: Arc<str> = Arc::from(table_name.as_str());
        let ampsync_db_engine_clone = ampsync_db_engine.clone();
        let batch_semaphore_clone = batch_semaphore.clone();
        tokio::spawn(async move {
            let mut reorg_stream = with_reorg(result_stream);

            info!("Started reorg stream for table: {}", table_name_arc);

            while let Some(result) = reorg_stream.next().await {
                match result {
                    Ok(ResponseBatchWithReorg::Batch { data, metadata }) => {
                        info!(
                            "Received data batch for table '{}': {} rows, block ranges: {:?}",
                            table_name_arc,
                            data.num_rows(),
                            metadata.ranges
                        );

                        // Acquire semaphore permit before processing batch
                        // This provides backpressure to prevent OOM when many tables
                        // receive large batches simultaneously
                        let _permit = batch_semaphore_clone.acquire().await.unwrap();

                        // Convert nanosecond timestamps to microseconds for PostgreSQL compatibility
                        let converted_batch = match convert_nanosecond_timestamps(data) {
                            Ok(batch) => batch,
                            Err(e) => {
                                error!(
                                    "Failed to convert timestamps for table '{}': {}",
                                    table_name_arc, e
                                );
                                continue;
                            }
                        };

                        // High-performance bulk insert using pgpq
                        if let Err(e) = ampsync_db_engine_clone
                            .insert_record_batch(&table_name_arc, &converted_batch)
                            .await
                        {
                            error!(
                                "Failed to insert data for table '{}': {}",
                                table_name_arc, e
                            );
                        } else {
                            info!(
                                "Successfully bulk inserted {} rows into table '{}'",
                                converted_batch.num_rows(),
                                table_name_arc
                            );
                        }
                        // Permit is automatically released when _permit is dropped
                    }
                    Ok(ResponseBatchWithReorg::Reorg { invalidation }) => {
                        warn!(
                            "Reorg detected for table '{}', invalidating ranges: {:?}",
                            table_name_arc, invalidation
                        );

                        // Acquire semaphore permit for reorg handling too
                        let _permit = batch_semaphore_clone.acquire().await.unwrap();

                        // Handle reorg by deleting affected rows
                        if let Err(e) = ampsync_db_engine_clone
                            .handle_reorg(&table_name_arc, &invalidation)
                            .await
                        {
                            error!(
                                "Failed to handle reorg for table '{}': {}",
                                table_name_arc, e
                            );
                        } else {
                            info!("Successfully handled reorg for table '{}'", table_name_arc);
                        }
                    }
                    Err(e) => {
                        error!("Stream error for table '{}': {}", table_name_arc, e);
                        break;
                    }
                }
            }

            warn!("Stream ended for table: {}", table_name_arc);
        });
    }

    // Keep the main task alive and wait for shutdown signals
    // Handle both SIGTERM (Docker stop) and SIGINT (Ctrl+C)
    let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    let mut sigint = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())?;

    tokio::select! {
        _ = sigterm.recv() => {
            info!("Received SIGTERM, shutting down gracefully...");
        }
        _ = sigint.recv() => {
            info!("Received SIGINT, shutting down gracefully...");
        }
    }

    Ok(())
}

#[derive(Clone)]
pub struct AmpsyncConfig {
    /// Ampsync database url to connect.
    pub database_url: String,
    /// Amp ArrowFlight server endpoint to connect to.
    pub amp_flight_addr: String,
    /// Parsed dataset manifest.
    pub manifest: Arc<Manifest>,
}
impl AmpsyncConfig {
    pub async fn from_env() -> Result<Self, BoxError> {
        // Get dataset manifest path - required
        let dataset_manifest_path = env::var("DATASET_MANIFEST")
            .map_err(|_| "DATASET_MANIFEST environment variable is required")?;

        let dataset_manifest = PathBuf::from(dataset_manifest_path);

        // Verify the manifest file exists
        if !dataset_manifest.exists() {
            return Err(format!(
                "Dataset manifest file does not exist: {}",
                dataset_manifest.display()
            )
            .into());
        }

        // Verify it's a file, not a directory
        if !dataset_manifest.is_file() {
            return Err(format!(
                "Dataset manifest path is not a file: {}",
                dataset_manifest.display()
            )
            .into());
        }

        // Optionally validate the extension
        let valid_extensions = ["ts", "js", "mts", "mjs", "json"];
        if let Some(ext) = dataset_manifest.extension() {
            let ext_str = ext.to_string_lossy();
            if !valid_extensions.contains(&ext_str.as_ref()) {
                return Err(format!(
                    "Invalid dataset manifest extension '{}'. Expected one of: {}",
                    ext_str,
                    valid_extensions.join(", ")
                )
                .into());
            }
        } else {
            return Err(
                "Dataset manifest file must have an extension (ts, js, mts, mjs, or json)".into(),
            );
        }

        // Get Nozzle configuration (needed for schema inference)
        let amp_flight_addr =
            env::var("AMP_FLIGHT_ADDR").unwrap_or_else(|_| "http://localhost:1602".to_string());
        // Wire up the Amp admin api (used to fetch the manifest schema)
        let amp_admin_api_addr =
            env::var("AMP_ADMIN_API_ADDR").unwrap_or_else(|_| "http://localhost:1610".to_string());

        // Load manifest from registry (no Nozzle server query needed - solves empty table problem)
        let manifest =
            Arc::new(manifest::fetch_manifest(&amp_admin_api_addr, &dataset_manifest).await?);

        // First, try to get DATABASE_URL directly
        if let Ok(database_url) = env::var("DATABASE_URL") {
            return Ok(Self {
                database_url,
                amp_flight_addr,
                manifest,
            });
        }

        // Otherwise, try to construct from individual components
        let user = env::var("DATABASE_USER").ok();
        let password = env::var("DATABASE_PASSWORD").ok();
        let host = env::var("DATABASE_HOST").unwrap_or_else(|_| "localhost".to_string());
        let port = env::var("DATABASE_PORT")
            .unwrap_or_else(|_| "5432".to_string())
            .parse::<u16>()
            .map_err(|_| "Invalid DATABASE_PORT")?;
        let name = env::var("DATABASE_NAME").ok();

        // Check if we have the minimum required components
        if user.is_none() || name.is_none() {
            return Err(
                "Either DATABASE_URL or (DATABASE_USER and DATABASE_NAME) must be provided".into(),
            );
        }

        // Construct the PostgreSQL URL. format: postgresql://{user}:{password}@{host}:{port}/{database}
        let mut database_url = String::from("postgresql://");

        // Add user
        database_url.push_str(&user.unwrap());

        // Add password if provided
        if let Some(pass) = password {
            database_url.push(':');
            database_url.push_str(&pass);
        }

        // Add host and port
        database_url.push('@');
        database_url.push_str(&host);
        database_url.push(':');
        database_url.push_str(&port.to_string());

        // Add database name
        database_url.push('/');
        database_url.push_str(&name.unwrap());

        Ok(Self {
            database_url,
            amp_flight_addr,
            manifest,
        })
    }
}
