use std::{sync::Arc, time::Instant};

use amp_data_store::file_name::FileName;
use amp_worker_core::{
    Ctx, WriterProperties,
    compaction::{AmpCompactor, AmpCompactorTaskError},
    metrics,
    parquet_writer::{
        CommitMetadataError, ParquetFileWriter, ParquetFileWriterCloseError,
        ParquetFileWriterOutput, commit_metadata,
    },
    progress::{ProgressReporter, ProgressUpdate},
    retryable::RetryableErrorExt,
};
use common::{
    BlockNum,
    catalog::physical::Catalog,
    cursor::Cursor,
    detached_logical_plan::DetachedLogicalPlan,
    exec_env::ExecEnv,
    metadata::Generation,
    parquet::errors::ParquetError,
    physical_table::PhysicalTable,
    streaming_query::{
        QueryMessage, StreamingQuery, message_stream_with_block_complete::MessageStreamError,
    },
};
use futures::StreamExt as _;
use metadata_db::NotificationMultiplexerHandle;
use tracing::instrument;

#[instrument(skip_all, err)]
#[expect(clippy::too_many_arguments)]
pub async fn materialize_sql_query(
    ctx: &Ctx,
    env: &ExecEnv,
    catalog: &Catalog,
    query: DetachedLogicalPlan,
    start: BlockNum,
    end: Option<BlockNum>,
    cursor: Option<Cursor>,
    physical_table: Arc<PhysicalTable>,
    compactor: Arc<AmpCompactor>,
    opts: &Arc<WriterProperties>,
    progress_reporter: Option<Arc<dyn ProgressReporter>>,
    microbatch_max_interval: u64,
    notification_multiplexer: &Arc<NotificationMultiplexerHandle>,
    metrics: Option<Arc<metrics::MetricsRegistry>>,
) -> Result<(), MaterializeSqlQueryError> {
    tracing::info!(
        "materializing {} [{}-{}]",
        physical_table.table_ref_compact(),
        start,
        end.map(|e| e.to_string()).unwrap_or_default(),
    );
    let keep_alive_interval = ctx.config.keep_alive_interval;
    let mut stream = {
        StreamingQuery::spawn(
            env.clone(),
            catalog.clone(),
            query,
            start,
            end,
            cursor,
            notification_multiplexer,
            Some(physical_table.clone()),
            microbatch_max_interval,
            keep_alive_interval,
        )
        .await
        .map_err(MaterializeSqlQueryError::StreamingQuerySpawn)?
        .into_stream()
    };

    let mut microbatch_start = start;
    let mut filename = FileName::new_with_random_suffix(microbatch_start);
    let mut buf_writer = ctx
        .data_store
        .create_revision_file_writer(physical_table.revision(), &filename);
    let mut writer = ParquetFileWriter::new(
        ctx.data_store.clone(),
        buf_writer,
        filename,
        physical_table.clone(),
        opts.max_row_group_bytes,
        opts.parquet.clone(),
    )
    .map_err(MaterializeSqlQueryError::CreateParquetFileWriter)?;

    let table_name = physical_table.table_name();
    let location_id = *physical_table.location_id();

    // Track progress for event emission
    let mut files_count: u64 = 0;
    let mut total_size_bytes: u64 = 0;
    let mut last_emission_time = Instant::now() - ctx.config.progress_interval;
    let mut last_emitted_block: BlockNum = 0;
    let mut has_emitted = false;

    // Receive data from the query stream, commiting a file on every watermark update received. The
    // `microbatch_max_interval` parameter controls the frequency of these updates.
    while let Some(message) = stream.next().await {
        let message = message.map_err(MaterializeSqlQueryError::StreamingQuery)?;
        match message {
            QueryMessage::MicrobatchStart {
                range: _,
                is_reorg: _,
            } => (),
            QueryMessage::Data(batch) => {
                writer
                    .write(&batch)
                    .await
                    .map_err(MaterializeSqlQueryError::WriteBatch)?;

                if let Some(ref metrics) = metrics {
                    let num_rows: u64 = batch.num_rows().try_into().unwrap();
                    let num_bytes: u64 = batch.get_array_memory_size().try_into().unwrap();
                    metrics.record_ingestion_rows(num_rows, table_name.to_string(), location_id);
                    metrics.record_write_call(num_bytes, table_name.to_string(), location_id);
                }
            }
            QueryMessage::Watermark(_) => {
                // TODO: Check if file should be closed early
            }
            QueryMessage::MicrobatchEnd(range) => {
                let microbatch_end = range.end();
                let block_timestamp = range.timestamp();
                // Close current file and commit metadata
                let ParquetFileWriterOutput {
                    parquet_meta,
                    object_meta,
                    footer,
                    url,
                    ..
                } = writer
                    .close(range, vec![], Generation::default())
                    .await
                    .map_err(MaterializeSqlQueryError::CloseFile)?;

                // Track progress stats
                files_count += 1;
                total_size_bytes += object_meta.size;

                commit_metadata(
                    &ctx.metadata_db,
                    parquet_meta,
                    object_meta,
                    physical_table.location_id(),
                    &url,
                    footer,
                )
                .await
                .map_err(MaterializeSqlQueryError::CommitMetadata)?;

                compactor
                    .try_run()
                    .map_err(MaterializeSqlQueryError::Compactor)?;

                // Time-based progress event emission
                // Emit when interval has elapsed AND we have new progress
                if let Some(ref reporter) = progress_reporter {
                    let now = Instant::now();
                    let interval_elapsed =
                        now.duration_since(last_emission_time) >= ctx.config.progress_interval;
                    let progress_made = !has_emitted || microbatch_end > last_emitted_block;

                    if interval_elapsed && progress_made {
                        last_emission_time = now;
                        last_emitted_block = microbatch_end;
                        has_emitted = true;

                        reporter.report_progress(ProgressUpdate {
                            table_name: table_name.clone(),
                            start_block: start,
                            current_block: microbatch_end,
                            end_block: end, // None in continuous mode
                            files_count,
                            total_size_bytes,
                        });
                    }
                }

                // Open new file for next chunk
                microbatch_start = microbatch_end + 1;
                filename = FileName::new_with_random_suffix(microbatch_start);
                buf_writer = ctx
                    .data_store
                    .create_revision_file_writer(physical_table.revision(), &filename);
                writer = ParquetFileWriter::new(
                    ctx.data_store.clone(),
                    buf_writer,
                    filename,
                    physical_table.clone(),
                    opts.max_row_group_bytes,
                    opts.parquet.clone(),
                )
                .map_err(MaterializeSqlQueryError::CreateParquetFileWriter)?;

                if let Some(ref metrics) = metrics {
                    metrics.record_file_written(table_name.to_string(), location_id);
                    if let Some(ts) = block_timestamp {
                        metrics.record_table_freshness(table_name.to_string(), location_id, ts);
                    }
                }
            }
        }
    }

    Ok(())
}

/// Errors that occur when executing a SQL query materialization operation
///
/// This error type is used by `materialize_sql_query()`.
#[derive(Debug, thiserror::Error)]
pub enum MaterializeSqlQueryError {
    /// Failed to spawn the streaming query execution
    ///
    /// This occurs when initializing the streaming query executor fails.
    #[error("failed to spawn streaming query: {0}")]
    StreamingQuerySpawn(#[source] common::streaming_query::SpawnError),

    /// Failed to create the parquet file writer
    ///
    /// This occurs when initializing the parquet writer for output files fails.
    #[error("failed to create parquet file writer: {0}")]
    CreateParquetFileWriter(#[source] ParquetError),

    /// Failed to write a record batch to the parquet file
    ///
    /// This occurs when writing query result batches to the parquet file fails.
    #[error("failed to write batch: {0}")]
    WriteBatch(#[source] ParquetError),

    /// Failed to close and finalize the parquet file
    ///
    /// This occurs when closing the parquet writer and finalizing the file fails.
    #[error("failed to close file: {0}")]
    CloseFile(#[source] ParquetFileWriterCloseError),

    /// Failed to commit file metadata to the database
    ///
    /// This occurs when registering the written parquet file's metadata
    /// in the metadata database fails.
    #[error("failed to commit metadata: {0}")]
    CommitMetadata(#[source] CommitMetadataError),

    /// Failed to run the compactor after writing a file
    ///
    /// This occurs when the compactor task fails after a successful file write.
    #[error("failed to run compactor: {0}")]
    Compactor(#[source] AmpCompactorTaskError),

    /// Failed to receive the next message from the streaming query
    ///
    /// This occurs when the streaming query execution encounters an error
    /// while producing result batches.
    #[error("failed to get next message: {0}")]
    StreamingQuery(#[source] MessageStreamError),
}

impl RetryableErrorExt for MaterializeSqlQueryError {
    fn is_retryable(&self) -> bool {
        match self {
            // Query spawn failure — fatal (likely invalid SQL or schema mismatch)
            Self::StreamingQuerySpawn(_) => false,

            // Transient I/O failures — recoverable
            Self::CreateParquetFileWriter(_) => true,
            Self::WriteBatch(_) => true,

            // Delegate to inner error classification
            Self::CloseFile(err) => err.is_retryable(),
            Self::CommitMetadata(err) => err.is_retryable(),
            Self::Compactor(err) => err.is_retryable(),

            // Transient query execution failure — recoverable
            Self::StreamingQuery(_) => true,
        }
    }
}
