use std::{sync::Arc, time::Instant};

use amp_data_store::file_name::FileName;
use amp_parquet::{
    commit::{CommitMetadataError, commit_metadata},
    generation::Generation,
    retry::RetryableErrorExt as _,
    writer::{ParquetFileWriter, ParquetFileWriterCloseError, ParquetFileWriterOutput},
};
use amp_worker_core::{
    WriterProperties,
    compaction::{AmpCompactor, AmpCompactorTaskError},
    error_detail::ErrorDetailsProvider,
    progress::ProgressUpdate,
    retryable::RetryableErrorExt,
};
use common::{
    BlockNum,
    catalog::physical::Catalog,
    cursor::Cursor,
    detached_logical_plan::DetachedLogicalPlan,
    exec_env::ExecEnv,
    physical_table::PhysicalTable,
    retryable::RetryableErrorExt as _,
    streaming_query::{
        QueryMessage, StreamingQuery, StreamingQueryExecutionError,
        message_stream_with_block_complete::MessageStreamError,
    },
};
use datafusion::parquet::errors::ParquetError;
use futures::StreamExt as _;
use js_runtime::isolate_pool::IsolatePool;

use crate::job_ctx::Context;

#[tracing::instrument(skip_all, err)]
#[expect(clippy::too_many_arguments)]
pub async fn materialize_sql_query(
    ctx: &Context,
    env: &ExecEnv,
    isolate_pool: &IsolatePool,
    catalog: &Catalog,
    query: DetachedLogicalPlan,
    start: BlockNum,
    end: Option<BlockNum>,
    cursor: Option<Cursor>,
    physical_table: Arc<PhysicalTable>,
    compactor: Arc<AmpCompactor>,
    opts: &Arc<WriterProperties>,
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
            isolate_pool.clone(),
            catalog.clone(),
            query,
            start,
            end,
            cursor,
            &ctx.notification_multiplexer,
            Some(physical_table.clone()),
            ctx.config.microbatch_max_interval,
            keep_alive_interval,
            None,
        )
        .await
        .map_err(MaterializeSqlQueryError::streaming_query_spawn)?
        .into_stream()
    };

    let mut microbatch_start = start;
    let mut current_end = microbatch_start;
    let mut filename = FileName::new_with_random_suffix(microbatch_start);
    let mut buf_writer = ctx
        .data_store
        .create_revision_file_writer(physical_table.revision(), &filename);
    let mut writer = ParquetFileWriter::new(
        ctx.data_store.clone(),
        buf_writer,
        filename,
        physical_table.schema(),
        physical_table.table_ref_compact(),
        &*physical_table,
        opts.max_row_group_bytes,
        opts.parquet.clone(),
    )
    .map_err(MaterializeSqlQueryError::create_parquet_file_writer)?;

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
        let message = message
            .map_err(MaterializeSqlQueryError::streaming_query)
            .map_err(|err| err.with_block_range(microbatch_start, current_end))?;
        match message {
            QueryMessage::MicrobatchStart { range, is_reorg: _ } => {
                current_end = range.end();
            }
            QueryMessage::Data(batch) => {
                writer
                    .write(&batch)
                    .await
                    .map_err(MaterializeSqlQueryError::write_batch)
                    .map_err(|err| err.with_block_range(microbatch_start, current_end))?;

                if let Some(ref metrics) = ctx.metrics {
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
                    .map_err(MaterializeSqlQueryError::close_file)
                    .map_err(|err| err.with_block_range(microbatch_start, current_end))?;

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
                .map_err(MaterializeSqlQueryError::commit_metadata)
                .map_err(|err| err.with_block_range(microbatch_start, current_end))?;

                compactor
                    .try_run()
                    .map_err(MaterializeSqlQueryError::compactor)
                    .map_err(|err| err.with_block_range(microbatch_start, current_end))?;

                // Time-based progress event emission
                // Emit when interval has elapsed AND we have new progress
                if let Some(ref reporter) = ctx.progress_reporter {
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
                current_end = microbatch_start;
                filename = FileName::new_with_random_suffix(microbatch_start);
                buf_writer = ctx
                    .data_store
                    .create_revision_file_writer(physical_table.revision(), &filename);
                writer = ParquetFileWriter::new(
                    ctx.data_store.clone(),
                    buf_writer,
                    filename,
                    physical_table.schema(),
                    physical_table.table_ref_compact(),
                    &*physical_table,
                    opts.max_row_group_bytes,
                    opts.parquet.clone(),
                )
                .map_err(MaterializeSqlQueryError::create_parquet_file_writer)?;

                if let Some(ref metrics) = ctx.metrics {
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

/// Errors that occur when executing a SQL query materialization operation.
///
/// This is a wrapper struct around [`MaterializeSqlQueryErrorKind`] that optionally
/// carries the block range context of the failed microbatch. The block range is
/// attached via [`with_block_range`](Self::with_block_range) and surfaces as
/// structured JSON fields through [`ErrorDetailsProvider`].
#[derive(Debug)]
pub struct MaterializeSqlQueryError {
    kind: MaterializeSqlQueryErrorKind,
    block_range: Option<(BlockNum, BlockNum)>,
}

impl MaterializeSqlQueryError {
    /// Attach block range context to this error.
    fn with_block_range(mut self, start: BlockNum, end: BlockNum) -> Self {
        self.block_range = Some((start, end));
        self
    }

    fn streaming_query_spawn(err: common::streaming_query::SpawnError) -> Self {
        Self {
            kind: MaterializeSqlQueryErrorKind::StreamingQuerySpawn(err),
            block_range: None,
        }
    }

    fn create_parquet_file_writer(err: ParquetError) -> Self {
        Self {
            kind: MaterializeSqlQueryErrorKind::CreateParquetFileWriter(err),
            block_range: None,
        }
    }

    fn write_batch(err: ParquetError) -> Self {
        Self {
            kind: MaterializeSqlQueryErrorKind::WriteBatch(err),
            block_range: None,
        }
    }

    fn close_file(err: ParquetFileWriterCloseError) -> Self {
        Self {
            kind: MaterializeSqlQueryErrorKind::CloseFile(err),
            block_range: None,
        }
    }

    fn commit_metadata(err: CommitMetadataError) -> Self {
        Self {
            kind: MaterializeSqlQueryErrorKind::CommitMetadata(err),
            block_range: None,
        }
    }

    fn compactor(err: AmpCompactorTaskError) -> Self {
        Self {
            kind: MaterializeSqlQueryErrorKind::Compactor(err),
            block_range: None,
        }
    }

    fn streaming_query(err: MessageStreamError) -> Self {
        Self {
            kind: MaterializeSqlQueryErrorKind::StreamingQuery(err),
            block_range: None,
        }
    }
}

impl std::fmt::Display for MaterializeSqlQueryError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.kind.fmt(f)
    }
}

impl std::error::Error for MaterializeSqlQueryError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.kind.source()
    }
}

impl RetryableErrorExt for MaterializeSqlQueryError {
    fn is_retryable(&self) -> bool {
        self.kind.is_retryable()
    }
}

impl ErrorDetailsProvider for MaterializeSqlQueryError {
    fn error_details(&self) -> serde_json::Map<String, serde_json::Value> {
        let (start, end) = match self.block_range {
            Some((s, e)) => (Some(s), Some(e)),
            None => (None, None),
        };
        amp_worker_core::error_detail::block_range_details(start, end)
    }
}

/// The specific error variants for [`MaterializeSqlQueryError`].
#[derive(Debug, thiserror::Error)]
enum MaterializeSqlQueryErrorKind {
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

impl RetryableErrorExt for MaterializeSqlQueryErrorKind {
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

            // Delegate to the inner error when possible; default to retryable
            // for transient streaming failures.
            Self::StreamingQuery(err) => match err.downcast_ref::<StreamingQueryExecutionError>() {
                Some(inner) => inner.is_retryable(),
                None => true,
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use amp_worker_core::error_detail::ErrorDetailsProvider;
    use datafusion::parquet::errors::ParquetError;

    use super::MaterializeSqlQueryError;

    #[test]
    fn error_details_with_block_range_returns_start_and_end() {
        //* Given
        let err = MaterializeSqlQueryError::write_batch(ParquetError::General("test".into()))
            .with_block_range(50, 150);

        //* When
        let details = err.error_details();

        //* Then
        assert_eq!(details.len(), 2, "should contain exactly two entries");
        assert_eq!(
            details["block_range_start"],
            serde_json::json!(50),
            "block_range_start should match"
        );
        assert_eq!(
            details["block_range_end"],
            serde_json::json!(150),
            "block_range_end should match"
        );
    }

    #[test]
    fn error_details_without_block_range_returns_empty() {
        //* Given
        let err = MaterializeSqlQueryError::write_batch(ParquetError::General("test".into()));

        //* When
        let details = err.error_details();

        //* Then
        assert!(
            details.is_empty(),
            "details should be empty without block range"
        );
    }

    #[test]
    fn display_without_block_range_delegates_to_kind() {
        //* Given
        let err = MaterializeSqlQueryError::create_parquet_file_writer(ParquetError::General(
            "test".into(),
        ));

        //* When
        let msg = err.to_string();

        //* Then
        assert_eq!(
            msg, "failed to create parquet file writer: Parquet error: test",
            "should delegate to kind Display"
        );
    }

    #[test]
    fn display_with_block_range_delegates_to_kind() {
        //* Given
        let err = MaterializeSqlQueryError::create_parquet_file_writer(ParquetError::General(
            "test".into(),
        ))
        .with_block_range(10, 20);

        //* When
        let msg = err.to_string();

        //* Then
        assert_eq!(
            msg, "failed to create parquet file writer: Parquet error: test",
            "block range should not affect Display output"
        );
    }

    #[test]
    fn source_with_inner_error_returns_inner() {
        use std::error::Error;

        //* Given
        let err = MaterializeSqlQueryError::write_batch(ParquetError::General("inner".into()));

        //* When
        let source = err.source();

        //* Then
        let source = source.expect("should have a source error");
        assert_eq!(
            source.to_string(),
            "Parquet error: inner",
            "source should be the inner error"
        );
    }

    #[test]
    fn is_retryable_with_retryable_kind_returns_true() {
        use amp_worker_core::retryable::RetryableErrorExt;

        //* Given
        let err = MaterializeSqlQueryError::write_batch(ParquetError::General("test".into()));

        //* When
        let retryable = err.is_retryable();

        //* Then
        assert!(retryable, "WriteBatch errors should be retryable");
    }
}
