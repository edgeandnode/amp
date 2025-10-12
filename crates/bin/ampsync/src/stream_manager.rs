//! Stream management for multiple tables.
//!
//! This module handles spawning and coordinating stream tasks for all tables
//! in a dataset, including schema setup, checkpoint recovery, and graceful shutdown.

use std::{sync::Arc, time::Duration};

use amp_client::SqlClient;
use common::BoxError;
use datasets_derived::manifest::TableInput;
use tracing::{debug, info, warn};

use crate::{
    config::AmpsyncConfig,
    sql_validator::{SelectColumns, extract_select_columns, validate_incremental_query},
    stream_task::StreamTask,
    sync_engine::AmpsyncDbEngine,
};

/// Graceful shutdown timeout - maximum time to wait for in-flight operations (in seconds)
const GRACEFUL_SHUTDOWN_TIMEOUT_SECS: u64 = 30;

/// Spawns stream processing tasks for all tables in the manifest.
///
/// Creates tables (with schema evolution if needed), retrieves checkpoints,
/// and spawns background tasks to process streaming data from Nozzle.
///
/// Returns task handles that can be used to await completion during shutdown.
pub async fn spawn_stream_tasks(
    config: &AmpsyncConfig,
    sql_client: &SqlClient,
    ampsync_db_engine: &AmpsyncDbEngine,
    shutdown_token: tokio_util::sync::CancellationToken,
) -> Result<Vec<tokio::task::JoinHandle<()>>, BoxError> {
    let batch_semaphore = Arc::new(tokio::sync::Semaphore::new(
        config.stream_max_concurrent_batches,
    ));

    let mut task_handles = Vec::new();

    // Process each table: create table and set up streaming
    for (table_name, table) in &config.manifest.tables {
        debug!(table = %table_name, "processing_table");

        // Get the SQL query from the table definition
        let sql_query_raw = match &table.input {
            TableInput::View(view) => &view.sql,
        };

        // Ensure the query has "SETTINGS stream = true" for streaming behavior
        let sql_query = if sql_query_raw.contains("SETTINGS stream = true") {
            sql_query_raw.to_string()
        } else {
            format!("{} SETTINGS stream = true", sql_query_raw.trim())
        };

        // Validate that the query only uses incremental operations
        validate_incremental_query(&sql_query)
            .map_err(|e| format!("Table '{}' has invalid query:\n{}", table_name, e))?;

        // Extract which columns are selected in the SQL query
        let selected_columns = extract_select_columns(&sql_query)
            .map_err(|e| format!("Failed to parse SQL for table '{}': {}", table_name, e))?;

        // Filter the manifest schema based on what columns are actually selected
        let arrow_schema = match selected_columns {
            SelectColumns::All => {
                info!(
                    table = %table_name,
                    column_count = table.schema.arrow.fields.len(),
                    select_mode = "all",
                    "creating_table_with_schema"
                );
                table.schema.arrow.clone()
            }
            SelectColumns::Specific(column_names) => {
                info!(
                    table = %table_name,
                    column_count = column_names.len(),
                    columns = %column_names.join(", "),
                    select_mode = "specific",
                    "creating_table_with_schema"
                );

                // Build a HashMap for O(1) lookups of manifest fields
                let mut field_map: std::collections::HashMap<
                    &str,
                    &datasets_derived::manifest::Field,
                > = std::collections::HashMap::new();

                for field in &table.schema.arrow.fields {
                    let unqualified_name = field.name.rsplit('.').next().unwrap_or(&field.name);
                    field_map.insert(unqualified_name, field);
                    field_map.insert(&field.name, field);
                }

                // Create a filtered schema with only the selected columns
                let mut filtered_fields = Vec::new();
                for col_name in &column_names {
                    let unqualified_col = col_name.rsplit('.').next().unwrap_or(col_name);

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

        // Load resume point for this table (watermark-first, fallback to incremental)
        let resume_point = ampsync_db_engine.load_resume_point(table_name).await?;

        let (resume_watermark, streaming_query) = match &resume_point {
            crate::sync_engine::ResumePoint::Watermark(watermark) => {
                info!(
                    table = %table_name,
                    watermark = ?watermark,
                    "resuming_from_watermark"
                );
                (Some(watermark.clone()), sql_query.clone())
            }
            crate::sync_engine::ResumePoint::Incremental {
                network,
                max_block_num,
            } => {
                info!(
                    table = %table_name,
                    network = %network,
                    max_block_num = max_block_num,
                    "resuming_from_incremental_checkpoint"
                );
                // sql_query already has "SETTINGS stream = true" (ensured above)
                // Remove it, add WHERE clause, then add SETTINGS back
                let base_query = sql_query.trim_end_matches(" SETTINGS stream = true");
                (
                    None,
                    format!(
                        "{} WHERE block_num > {} SETTINGS stream = true",
                        base_query, max_block_num
                    ),
                )
            }
            crate::sync_engine::ResumePoint::None => {
                info!(
                    table = %table_name,
                    "starting_from_beginning"
                );
                (None, sql_query)
            }
        };

        debug!(
            table = %table_name,
            query = %streaming_query,
            "executing_streaming_query"
        );

        // Create StreamTask and spawn it
        let stream_task = StreamTask::new(table_name.clone(), streaming_query, resume_watermark);

        let mut sql_client_clone = sql_client.clone();
        let ampsync_db_engine_clone = ampsync_db_engine.clone();
        let batch_semaphore_clone = batch_semaphore.clone();
        let shutdown_token_clone = shutdown_token.clone();

        let task_handle = tokio::spawn(async move {
            stream_task
                .run(
                    &mut sql_client_clone,
                    &ampsync_db_engine_clone,
                    batch_semaphore_clone,
                    shutdown_token_clone,
                )
                .await;
        });

        task_handles.push(task_handle);
    }

    Ok(task_handles)
}

/// Gracefully shuts down all stream processing tasks.
///
/// Waits for tasks to complete with a timeout, logging any failures.
/// This ensures in-flight batches are processed before shutdown.
pub async fn shutdown_streams_gracefully(task_handles: Vec<tokio::task::JoinHandle<()>>) {
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
