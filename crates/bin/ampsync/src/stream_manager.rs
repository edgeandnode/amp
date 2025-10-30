//! Stream management for multiple tables.
//!
//! This module handles spawning and coordinating stream tasks for all tables
//! in a dataset, including schema setup, checkpoint recovery, and graceful shutdown.

use std::{sync::Arc, time::Duration};

use amp_client::AmpClient;
use common::BoxError;
use tracing::{debug, info, warn};

use crate::{config::AmpsyncConfig, stream_task::StreamTask, sync_engine::AmpsyncDbEngine};

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
    sql_client: &AmpClient,
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

        // Construct simple query to stream from materialized dataset table
        // The dataset has already been materialized by ampd, so we just stream from it
        let base_query = format!(
            "SELECT * FROM \"{}\".\"{}\" SETTINGS stream = true",
            config.dataset_name, table_name
        );

        info!(
            table = %table_name,
            column_count = table.schema.arrow.fields.len(),
            query = %base_query,
            "creating_table_with_schema"
        );

        // Use full manifest schema (SELECT * includes all columns)
        let arrow_schema = table.schema.arrow.clone();

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
                (Some(watermark.clone()), base_query.clone())
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
                // Inject WHERE clause using _block_num (Nozzle system metadata column)
                // This filters out data we've already processed
                let query_with_where = format!(
                    "SELECT * FROM \"{}\".\"{}\" WHERE _block_num > {} SETTINGS stream = true",
                    config.dataset_name, table_name, max_block_num
                );
                (None, query_with_where)
            }
            crate::sync_engine::ResumePoint::None => {
                info!(
                    table = %table_name,
                    "starting_from_beginning"
                );
                (None, base_query)
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
