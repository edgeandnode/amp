mod conn;
mod manifest;
mod pgpq;
mod sync_engine;

use std::{env, path::PathBuf, sync::Arc};

use common::BoxError;
use datasets_derived::manifest::{Manifest, TableInput};
use futures::StreamExt;
use nozzle_client::{ResponseBatchWithReorg, SqlClient, with_reorg};
use tracing::{error, info, warn};

use crate::{
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
    info!("Configuration loaded from environment");
    info!(
        "Loaded manifest: {} v{} for network {}",
        config.manifest.name, config.manifest.version, config.manifest.network
    );

    // Connect to target database
    let db_pool = DbConnPool::connect(&config.database_url, DEFAULT_POOL_SIZE).await?;
    info!("Connected to target database");

    // Connect to Nozzle server
    let mut sql_client = SqlClient::new(&config.nozzle_endpoint).await?;
    info!("Connected to Nozzle server at {}", config.nozzle_endpoint);

    info!("Preparing to sync dataset: {}", config.manifest.name);

    let ampsync_db_engine = AmpsyncDbEngine::new(&db_pool);

    // Process each table: create table and set up streaming
    for (table_name, table) in &config.manifest.tables {
        info!("Processing table: {}", table_name);

        // Create the table based on the Arrow schema (uses IF NOT EXISTS)
        ampsync_db_engine
            .create_table_from_schema(table_name, &table.schema.arrow)
            .await?;
        info!("Ensured table '{}' exists", table_name);

        // Get the SQL query from the table definition
        let sql_query = match &table.input {
            TableInput::View(view) => &view.sql,
        };

        // Add streaming settings to the query
        let streaming_query = format!("{} SETTINGS stream = true", sql_query);
        info!(
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
        let table_name_clone = table_name.clone();
        let ampsync_db_engine_clone = ampsync_db_engine.clone();
        tokio::spawn(async move {
            let mut reorg_stream = with_reorg(result_stream);

            info!("Started reorg stream for table: {}", table_name_clone);

            while let Some(result) = reorg_stream.next().await {
                match result {
                    Ok(ResponseBatchWithReorg::Batch { data, metadata }) => {
                        info!(
                            "Received data batch for table '{}': {} rows, block ranges: {:?}",
                            table_name_clone,
                            data.num_rows(),
                            metadata.ranges
                        );

                        // High-performance bulk insert using pgpq
                        if let Err(e) = ampsync_db_engine_clone
                            .insert_record_batch(&table_name_clone, &data)
                            .await
                        {
                            error!(
                                "Failed to insert data for table '{}': {}",
                                table_name_clone, e
                            );
                        } else {
                            info!(
                                "Successfully bulk inserted {} rows into table '{}'",
                                data.num_rows(),
                                table_name_clone
                            );
                        }
                    }
                    Ok(ResponseBatchWithReorg::Reorg { invalidation }) => {
                        warn!(
                            "Reorg detected for table '{}', invalidating ranges: {:?}",
                            table_name_clone, invalidation
                        );

                        // Handle reorg by deleting affected rows
                        if let Err(e) = ampsync_db_engine_clone
                            .handle_reorg(&table_name_clone, &invalidation)
                            .await
                        {
                            error!(
                                "Failed to handle reorg for table '{}': {}",
                                table_name_clone, e
                            );
                        } else {
                            info!(
                                "Successfully handled reorg for table '{}'",
                                table_name_clone
                            );
                        }
                    }
                    Err(e) => {
                        error!("Stream error for table '{}': {}", table_name_clone, e);
                        break;
                    }
                }
            }

            warn!("Stream ended for table: {}", table_name_clone);
        });
    }

    info!("All streaming queries set up successfully");

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
    /// Nozzle server endpoint to connect to.
    pub nozzle_endpoint: String,
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

        // Load and parse the manifest
        let manifest = Arc::new(manifest::load_manifest(&dataset_manifest).await?);

        // Get Nozzle configuration
        let nozzle_endpoint =
            env::var("NOZZLE_ENDPOINT").unwrap_or_else(|_| "http://localhost:1602".to_string());

        // First, try to get DATABASE_URL directly
        if let Ok(database_url) = env::var("DATABASE_URL") {
            return Ok(Self {
                database_url,
                nozzle_endpoint,
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
            nozzle_endpoint,
            manifest,
        })
    }
}
