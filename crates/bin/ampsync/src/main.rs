mod batch_utils;
mod conn;
mod dataset_definition;
mod file_watcher;
mod manifest;
mod pgpq;
mod sql_validator;
mod sync_engine;

use std::{env, path::PathBuf, sync::Arc, time::Duration};

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

/// Default maximum number of concurrent batch operations across all tables
const DEFAULT_MAX_CONCURRENT_BATCHES: usize = 10;

/// Default number of retries when hot-reloading fails to fetch manifest
const DEFAULT_HOT_RELOAD_MAX_RETRIES: u32 = 3;

/// Maximum number of stream reconnection attempts before giving up
const MAX_STREAM_RETRIES: u32 = 5;

/// Maximum delay between reconnection attempts (in seconds)
const MAX_RETRY_DELAY_SECS: u64 = 60;

/// Graceful shutdown timeout - maximum time to wait for in-flight operations (in seconds)
const GRACEFUL_SHUTDOWN_TIMEOUT_SECS: u64 = 30;

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
/// This will run in tandem with both amp (nozzle) and with a sync engine such as electricsql-sql
/// to provide subsets of the nozzle dataset to build a reactive query layer for application development.
///
/// Supports hot-reloading: when the manifest file changes, streams are gracefully stopped,
/// schema migrations are applied, and streams are restarted with the new configuration.
async fn ampsync_runner() -> Result<(), BoxError> {
    // Initialize logging
    monitoring::logging::init();

    info!("Starting ampsync with hot-reload support");

    let mut config = AmpsyncConfig::from_env().await?;
    info!(
        dataset_name = %config.manifest.name,
        dataset_version = %config.manifest.version,
        "manifest_loaded"
    );

    // Connect to target database (reused across reloads)
    let db_pool = DbConnPool::connect(&config.database_url, DEFAULT_POOL_SIZE).await?;

    // Connect to Nozzle server (reused across reloads)
    let sql_client = SqlClient::new(&config.amp_flight_addr).await?;
    info!(
        flight_addr = %config.amp_flight_addr,
        "nozzle_server_connected"
    );

    let ampsync_db_engine = AmpsyncDbEngine::new(&db_pool);

    // Initialize checkpoint tracking table
    ampsync_db_engine.init_checkpoint_table().await?;
    info!("checkpoint_tracking_initialized");

    // Set up file watcher for hot-reload
    let (file_change_tx, mut file_change_rx) = tokio::sync::mpsc::channel(1);
    let manifest_path = config.manifest_path.clone();
    let _file_watcher_handle = file_watcher::spawn_file_watcher(manifest_path, file_change_tx);
    info!(
        manifest_path = %config.manifest_path.display(),
        "file_watcher_initialized"
    );

    // Setup signal handlers for graceful shutdown
    let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    let mut sigint = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())?;

    // Main reload loop
    loop {
        info!(
            dataset_name = %config.manifest.name,
            dataset_version = %config.manifest.version,
            table_count = config.manifest.tables.len(),
            "starting_dataset_sync"
        );

        // Spawn stream processing tasks for current configuration
        let shutdown_token = tokio_util::sync::CancellationToken::new();
        let task_handles = spawn_stream_tasks(
            &config,
            &sql_client,
            &ampsync_db_engine,
            shutdown_token.clone(),
        )
        .await?;

        // Wait for reload trigger or shutdown signal
        tokio::select! {
            // File changed - reload configuration
            Some(file_watcher::FileWatchEvent::Changed) = file_change_rx.recv() => {
                info!(
                    manifest_path = %config.manifest_path.display(),
                    "hot_reload_triggered"
                );

                // Gracefully stop all streams
                shutdown_token.cancel();
                shutdown_streams_gracefully(task_handles).await;

                // Reload manifest
                match config.reload_manifest().await {
                    Ok(new_config) => {
                        info!(
                            dataset_name = %new_config.manifest.name,
                            dataset_version = %new_config.manifest.version,
                            table_count = new_config.manifest.tables.len(),
                            "hot_reload_success"
                        );
                        config = new_config;
                        // Loop continues with new config
                    }
                    Err(e) => {
                        error!(
                            error = %e,
                            manifest_path = %config.manifest_path.display(),
                            "hot_reload_failed"
                        );
                        error!("Keeping previous configuration active");
                        // Loop continues with old config (safe fallback)
                    }
                }
            }

            // SIGTERM received - graceful shutdown
            _ = sigterm.recv() => {
                info!(signal = "SIGTERM", "shutdown_signal_received");
                shutdown_token.cancel();
                shutdown_streams_gracefully(task_handles).await;
                break;
            }

            // SIGINT received - graceful shutdown
            _ = sigint.recv() => {
                info!(signal = "SIGINT", "shutdown_signal_received");
                shutdown_token.cancel();
                shutdown_streams_gracefully(task_handles).await;
                break;
            }
        }
    }

    info!("shutdown_complete");
    Ok(())
}

/// Spawns stream processing tasks for all tables in the manifest.
///
/// Creates tables (with schema evolution if needed), retrieves checkpoints,
/// and spawns background tasks to process streaming data from Nozzle.
///
/// Returns task handles that can be used to await completion during shutdown.
async fn spawn_stream_tasks(
    config: &AmpsyncConfig,
    sql_client: &SqlClient,
    ampsync_db_engine: &AmpsyncDbEngine,
    shutdown_token: tokio_util::sync::CancellationToken,
) -> Result<Vec<tokio::task::JoinHandle<()>>, BoxError> {
    // Create a semaphore to limit concurrent batch processing across all tables
    // This provides backpressure to prevent OOM when many tables receive large batches simultaneously.
    // When all permits are taken, the stream processing will wait at semaphore.acquire(),
    // preventing the stream from pulling more data until processing capacity is available.
    // Default: Allow 10 concurrent batch operations (configurable via env)
    let max_concurrent_batches = env::var("MAX_CONCURRENT_BATCHES")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(DEFAULT_MAX_CONCURRENT_BATCHES);
    let batch_semaphore = Arc::new(tokio::sync::Semaphore::new(max_concurrent_batches));

    // Collect task handles for graceful shutdown
    let mut task_handles = Vec::new();

    // Process each table: create table and set up streaming
    for (table_name, table) in &config.manifest.tables {
        debug!(table = %table_name, "processing_table");

        // Get the SQL query from the table definition
        let sql_query = match &table.input {
            TableInput::View(view) => &view.sql,
        };

        // Validate that the query only uses incremental operations
        use crate::sql_validator::{
            SelectColumns, extract_select_columns, validate_incremental_query,
        };
        validate_incremental_query(sql_query)
            .map_err(|e| format!("Table '{}' has invalid query:\n{}", table_name, e))?;

        // Extract which columns are selected in the SQL query
        let selected_columns = extract_select_columns(sql_query)
            .map_err(|e| format!("Failed to parse SQL for table '{}': {}", table_name, e))?;

        // Filter the manifest schema based on what columns are actually selected
        let arrow_schema = match selected_columns {
            SelectColumns::All => {
                // SELECT * - use the full manifest schema
                info!(
                    table = %table_name,
                    column_count = table.schema.arrow.fields.len(),
                    select_mode = "all",
                    "creating_table_with_schema"
                );
                table.schema.arrow.clone()
            }
            SelectColumns::Specific(column_names) => {
                // SELECT col1, col2, ... - filter manifest schema to only include these columns
                info!(
                    table = %table_name,
                    column_count = column_names.len(),
                    columns = %column_names.join(", "),
                    select_mode = "specific",
                    "creating_table_with_schema"
                );

                // Build a HashMap for O(1) lookups of manifest fields
                // Key: unqualified column name (last segment after '.')
                // Value: reference to the field
                let mut field_map: std::collections::HashMap<
                    &str,
                    &datasets_derived::manifest::Field,
                > = std::collections::HashMap::new();

                for field in &table.schema.arrow.fields {
                    // Extract the unqualified column name (e.g., "block_num" from "anvil.blocks.block_num")
                    let unqualified_name = field.name.rsplit('.').next().unwrap_or(&field.name);
                    field_map.insert(unqualified_name, field);

                    // Also insert the fully qualified name for exact matches
                    field_map.insert(&field.name, field);
                }

                // Create a filtered schema with only the selected columns
                let mut filtered_fields = Vec::new();
                for col_name in &column_names {
                    // Extract unqualified name from the selected column
                    // (handles cases like "anvil.blocks.block_num" or just "block_num")
                    let unqualified_col = col_name.rsplit('.').next().unwrap_or(col_name);

                    // Try exact match first, then unqualified match
                    if let Some(field) = field_map
                        .get(col_name.as_str())
                        .or_else(|| field_map.get(unqualified_col))
                    {
                        filtered_fields.push((*field).clone());
                    } else {
                        warn!(
                            table = %table_name,
                            column = %col_name,
                            "column_not_found_in_schema"
                        );
                        // For expressions/computed columns, we'll need to handle them dynamically
                        // For now, skip them - they'll be handled when we get actual data
                    }
                }

                if filtered_fields.is_empty() {
                    return Err(format!(
                        "No matching columns found in manifest schema for table '{}'. \
                         Selected columns: {:?}, Available fields: {:?}",
                        table_name,
                        column_names,
                        table
                            .schema
                            .arrow
                            .fields
                            .iter()
                            .map(|f| &f.name)
                            .collect::<Vec<_>>()
                    )
                    .into());
                }

                datasets_derived::manifest::ArrowSchema {
                    fields: filtered_fields,
                }
            }
        };

        // Create the table based on the filtered schema
        ampsync_db_engine
            .create_table_from_schema(table_name, &arrow_schema)
            .await?;

        // Get checkpoint for this table to resume from last processed block
        let checkpoint = ampsync_db_engine.get_checkpoint(table_name).await?;
        let sql_query_with_checkpoint = if let Some(max_block) = checkpoint {
            info!(
                table = %table_name,
                checkpoint_block = max_block,
                "resuming_from_checkpoint"
            );
            // Add WHERE clause to resume from checkpoint
            // Note: This assumes the query has a block_num column accessible
            format!("{} WHERE block_num > {}", sql_query, max_block)
        } else {
            info!(
                table = %table_name,
                "starting_from_beginning"
            );
            sql_query.to_string()
        };

        // Add streaming settings to the query
        let streaming_query = format!("{} SETTINGS stream = true", sql_query_with_checkpoint);
        debug!(
            table = %table_name,
            query = %streaming_query,
            "executing_streaming_query"
        );

        // Wrap with with_reorg and spawn a task to handle the stream
        let table_name_owned = table_name.clone();
        let ampsync_db_engine_clone = ampsync_db_engine.clone();
        let batch_semaphore_clone = batch_semaphore.clone();
        let mut sql_client_clone = sql_client.clone();
        let streaming_query_clone = streaming_query.clone();
        let shutdown_token_clone = shutdown_token.clone();

        let task_handle = tokio::spawn(async move {
            let mut retry_count = 0u32;
            let max_retries = MAX_STREAM_RETRIES;

            loop {
                // Check if shutdown has been requested
                if shutdown_token_clone.is_cancelled() {
                    info!(
                        table = %table_name_owned,
                        "shutdown_requested"
                    );
                    return;
                }

                let result_stream = match sql_client_clone
                    .query(&streaming_query_clone, None, None)
                    .await
                {
                    Ok(stream) => stream,
                    Err(e) => {
                        error!(
                            table = %table_name_owned,
                            attempt = retry_count + 1,
                            max_retries = max_retries,
                            error = %e,
                            "stream_creation_failed"
                        );

                        if retry_count >= max_retries {
                            error!(
                                table = %table_name_owned,
                                max_retries = max_retries,
                                "max_retries_reached"
                            );
                            return;
                        }

                        // Exponential backoff: 2^retry_count seconds, capped at MAX_RETRY_DELAY_SECS
                        let delay_secs = std::cmp::min(2u64.pow(retry_count), MAX_RETRY_DELAY_SECS);
                        warn!(
                            table = %table_name_owned,
                            retry_delay_secs = delay_secs,
                            attempt = retry_count + 1,
                            "retrying_stream_creation"
                        );
                        tokio::time::sleep(Duration::from_secs(delay_secs)).await;
                        retry_count += 1;
                        continue;
                    }
                };

                let mut reorg_stream = with_reorg(result_stream);
                info!(
                    table = %table_name_owned,
                    "stream_started"
                );

                // Reset retry count on successful connection
                retry_count = 0;

                while let Some(result) = reorg_stream.next().await {
                    match result {
                        Ok(ResponseBatchWithReorg::Batch { data, metadata }) => {
                            info!(
                                table = %table_name_owned,
                                rows = data.num_rows(),
                                block_ranges = ?metadata.ranges,
                                "batch_received"
                            );

                            // Acquire semaphore permit before processing batch
                            // This provides backpressure to prevent OOM when many tables
                            // receive large batches simultaneously. The stream won't pull
                            // more data until we release this permit after processing.
                            let _permit = match batch_semaphore_clone.acquire().await {
                                Ok(permit) => permit,
                                Err(_) => {
                                    // Semaphore is closed - this means shutdown is in progress
                                    warn!(
                                        table = %table_name_owned,
                                        "semaphore_closed"
                                    );
                                    return;
                                }
                            };

                            // Convert nanosecond timestamps to microseconds for PostgreSQL compatibility
                            let converted_batch = match convert_nanosecond_timestamps(data) {
                                Ok(batch) => batch,
                                Err(e) => {
                                    error!(
                                        table = %table_name_owned,
                                        error = %e,
                                        "timestamp_conversion_failed"
                                    );
                                    // Break to trigger stream reconnection and retry from last good position
                                    break;
                                }
                            };

                            // High-performance bulk insert using pgpq
                            if let Err(e) = ampsync_db_engine_clone
                                .insert_record_batch(&table_name_owned, &converted_batch)
                                .await
                            {
                                error!(
                                    table = %table_name_owned,
                                    rows = converted_batch.num_rows(),
                                    error = %e,
                                    "batch_insert_failed"
                                );
                                // Break to trigger stream reconnection
                                // This ensures we don't silently drop data
                                break;
                            } else {
                                info!(
                                    table = %table_name_owned,
                                    rows = converted_batch.num_rows(),
                                    "batch_inserted"
                                );

                                // Update checkpoint with the max block_num from this batch
                                if let Some(max_block) =
                                    AmpsyncDbEngine::extract_max_block_num(&converted_batch)
                                {
                                    if let Err(e) = ampsync_db_engine_clone
                                        .update_checkpoint(&table_name_owned, max_block)
                                        .await
                                    {
                                        error!(
                                            table = %table_name_owned,
                                            block_num = max_block,
                                            error = %e,
                                            "checkpoint_update_failed"
                                        );
                                        // Don't break - checkpoint failure is not critical enough to halt stream
                                        // Worst case: we'll reprocess some data on restart
                                    } else {
                                        debug!(
                                            table = %table_name_owned,
                                            block_num = max_block,
                                            "checkpoint_updated"
                                        );
                                    }
                                }
                            }
                            // Permit is automatically released when _permit is dropped
                        }
                        Ok(ResponseBatchWithReorg::Reorg { invalidation }) => {
                            warn!(
                                table = %table_name_owned,
                                invalidation_ranges = ?invalidation,
                                "reorg_detected"
                            );

                            // Acquire semaphore permit for reorg handling too
                            let _permit = match batch_semaphore_clone.acquire().await {
                                Ok(permit) => permit,
                                Err(_) => {
                                    // Semaphore is closed - this means shutdown is in progress
                                    warn!(
                                        table = %table_name_owned,
                                        context = "reorg",
                                        "semaphore_closed"
                                    );
                                    return;
                                }
                            };

                            // Handle reorg by deleting affected rows
                            if let Err(e) = ampsync_db_engine_clone
                                .handle_reorg(&table_name_owned, &invalidation)
                                .await
                            {
                                error!(
                                    table = %table_name_owned,
                                    error = %e,
                                    "reorg_handling_failed"
                                );
                                // Break to trigger stream reconnection
                                // Reorgs must be handled correctly for data consistency
                                break;
                            } else {
                                info!(
                                    table = %table_name_owned,
                                    "reorg_handled"
                                );
                            }
                        }
                        Err(e) => {
                            error!(
                                table = %table_name_owned,
                                error = %e,
                                "stream_error"
                            );
                            break; // Break inner loop to trigger reconnection
                        }
                    }
                }

                // Stream ended - will retry with exponential backoff in outer loop
                warn!(
                    table = %table_name_owned,
                    "stream_ended"
                );
            }
        });

        task_handles.push(task_handle);
    }

    Ok(task_handles)
}

/// Gracefully shuts down all stream processing tasks.
///
/// Waits for tasks to complete with a timeout, logging any failures.
/// This ensures in-flight batches are processed before shutdown.
async fn shutdown_streams_gracefully(task_handles: Vec<tokio::task::JoinHandle<()>>) {
    info!(task_count = task_handles.len(), "shutting_down_streams");

    let shutdown_future = async {
        for (i, handle) in task_handles.into_iter().enumerate() {
            match handle.await {
                Ok(_) => debug!(task_index = i, "task_completed"),
                Err(e) => warn!(task_index = i, error = %e, "task_join_error"),
            }
        }
    };

    match tokio::time::timeout(
        Duration::from_secs(GRACEFUL_SHUTDOWN_TIMEOUT_SECS),
        shutdown_future,
    )
    .await
    {
        Ok(_) => info!("streams_shutdown_complete"),
        Err(_) => warn!(
            timeout_secs = GRACEFUL_SHUTDOWN_TIMEOUT_SECS,
            "graceful_shutdown_timeout"
        ),
    }
}

/// Sanitize a database URL for safe logging by redacting the password
///
/// Converts: postgresql://user:password@host:5432/db
/// To:       postgresql://user:***@host:5432/db
///
/// Handles passwords containing special characters including '@'
fn sanitize_database_url(url: &str) -> String {
    // Find password section (between : and last @ before /)
    if let Some(scheme_end) = url.find("://") {
        let after_scheme = &url[scheme_end + 3..];

        // Find the host/port section by looking for the last @ before any /
        // This handles passwords with @ in them
        let host_start = if let Some(slash_pos) = after_scheme.find('/') {
            // There's a database path - find last @ before it
            after_scheme[..slash_pos].rfind('@')
        } else {
            // No database path - find last @
            after_scheme.rfind('@')
        };

        if let Some(at_pos) = host_start {
            // Found @ - check if there's a password (look for : before @)
            if let Some(colon_pos) = after_scheme[..at_pos].find(':') {
                // Password exists - replace it with ***
                let before_password = &url[..scheme_end + 3 + colon_pos + 1];
                let after_password = &url[scheme_end + 3 + at_pos..];
                return format!("{}***{}", before_password, after_password);
            }
        }
    }
    // No password found or parsing failed - return as-is
    url.to_string()
}

#[derive(Clone)]
pub struct AmpsyncConfig {
    /// Ampsync database url to connect.
    pub database_url: String,
    /// Amp ArrowFlight server endpoint to connect to.
    pub amp_flight_addr: String,
    /// Amp Admin API endpoint for schema resolution.
    pub amp_admin_api_addr: String,
    /// Path to the dataset manifest file (for hot-reloading).
    pub manifest_path: PathBuf,
    /// Parsed dataset manifest.
    pub manifest: Arc<Manifest>,
    /// Maximum number of retries for hot-reload manifest fetch (configurable via HOT_RELOAD_MAX_RETRIES env var)
    pub hot_reload_max_retries: u32,
}
impl AmpsyncConfig {
    /// Get a sanitized version of the database URL safe for logging
    /// (redacts password if present)
    pub fn sanitized_database_url(&self) -> String {
        sanitize_database_url(&self.database_url)
    }

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

        // Get hot-reload retry configuration
        let hot_reload_max_retries = env::var("HOT_RELOAD_MAX_RETRIES")
            .ok()
            .and_then(|s| s.parse::<u32>().ok())
            .unwrap_or(DEFAULT_HOT_RELOAD_MAX_RETRIES);

        // Load manifest with startup polling (polls indefinitely until dataset is published)
        let manifest = Arc::new(
            manifest::fetch_manifest_with_startup_poll(&amp_admin_api_addr, &dataset_manifest)
                .await?,
        );

        // First, try to get DATABASE_URL directly
        if let Ok(database_url) = env::var("DATABASE_URL") {
            return Ok(Self {
                database_url,
                amp_flight_addr,
                amp_admin_api_addr,
                manifest_path: dataset_manifest,
                manifest,
                hot_reload_max_retries,
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
            amp_admin_api_addr,
            manifest_path: dataset_manifest,
            manifest,
            hot_reload_max_retries,
        })
    }

    /// Reload the manifest from disk (used for hot-reloading).
    ///
    /// This creates a new config with the updated manifest while preserving
    /// database connection details.
    ///
    /// Uses limited retries (configurable via hot_reload_max_retries) to give
    /// the dump command time to complete, then fails if dataset still not found.
    pub async fn reload_manifest(&self) -> Result<Self, BoxError> {
        info!("Reloading manifest from '{}'", self.manifest_path.display());

        let manifest = Arc::new(
            manifest::fetch_manifest_with_retry(
                &self.amp_admin_api_addr,
                &self.manifest_path,
                self.hot_reload_max_retries,
            )
            .await?,
        );

        Ok(Self {
            database_url: self.database_url.clone(),
            amp_flight_addr: self.amp_flight_addr.clone(),
            amp_admin_api_addr: self.amp_admin_api_addr.clone(),
            manifest_path: self.manifest_path.clone(),
            manifest,
            hot_reload_max_retries: self.hot_reload_max_retries,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sanitize_database_url_with_password() {
        let url = "postgresql://user:secret_pass@localhost:5432/mydb";
        let sanitized = sanitize_database_url(url);
        assert_eq!(sanitized, "postgresql://user:***@localhost:5432/mydb");
        assert!(!sanitized.contains("secret_pass"));
    }

    #[test]
    fn test_sanitize_database_url_without_password() {
        let url = "postgresql://user@localhost:5432/mydb";
        let sanitized = sanitize_database_url(url);
        assert_eq!(sanitized, "postgresql://user@localhost:5432/mydb");
    }

    #[test]
    fn test_sanitize_database_url_complex_password() {
        let url = "postgresql://admin:p@ssw0rd!@#$@db.example.com:5432/production";
        let sanitized = sanitize_database_url(url);
        assert_eq!(
            sanitized,
            "postgresql://admin:***@db.example.com:5432/production"
        );
        assert!(!sanitized.contains("p@ssw0rd!@#$"));
    }

    #[test]
    fn test_sanitize_database_url_invalid_format() {
        let url = "not-a-valid-url";
        let sanitized = sanitize_database_url(url);
        assert_eq!(sanitized, "not-a-valid-url");
    }
}
