use std::{ops::RangeInclusive, pin::Pin, sync::Arc};

use datafusion::{
    error::DataFusionError, execution::SendableRecordBatchStream, logical_expr::LogicalPlan,
    physical_plan::stream::RecordBatchStreamAdapter,
};
use futures::{
    FutureExt, Stream, TryStreamExt as _,
    stream::{self, StreamExt},
};
use metadata_db::LocationId;
use tokio::sync::mpsc;
use tokio_stream::wrappers::{ReceiverStream, UnboundedReceiverStream, WatchStream};
use tokio_util::task::AbortOnDropHandle;
use tracing::instrument;

use crate::{
    BlockNum, BoxError, SPECIAL_BLOCK_NUM,
    arrow::{array::RecordBatch, datatypes::SchemaRef},
    catalog::physical::PhysicalTable,
    metadata::segments::{BlockRange, Chain},
    notification_multiplexer::NotificationMultiplexerHandle,
    plan_visitors::{order_by_block_num, propagate_block_num},
    query_context::QueryContext,
};

pub type BlockRangeStream = Pin<
    Box<
        dyn Stream<Item = Result<Option<RangeInclusive<BlockNum>>, BoxError>>
            + Send
            + Sync
            + 'static,
    >,
>;

/// Calculates ranges for the next batch of SQL execution. The result is based on the set of
/// physical tables the query executes over and the sequence of segments already processed.
struct ExecutionRanges {
    /// physical tables used for SQL execution
    tables: Vec<Arc<PhysicalTable>>,
    /// previously processed chain state
    stream_chain: Option<Chain>,
}

impl ExecutionRanges {
    fn new(tables: Vec<Arc<PhysicalTable>>) -> Self {
        Self {
            tables,
            stream_chain: None,
        }
    }

    /// Determines the block range for the next batch SQL query. This returns the range starting
    /// from the latest processed segments up to the latest common watermark across all relevant
    /// tables. The resulting range may overlap with previous ranges to re-execute over segments
    /// that have been reorganized out of the canonical chain.
    async fn next(&mut self) -> Result<Option<RangeInclusive<BlockNum>>, BoxError> {
        // This function assumes that we can expect segment watermarks to line up across tables
        // derived from the same raw table. It also facilitates this assumption by rewinding
        // execution back to a segment watermark when handling reorgs.

        fn contains_watermark(chain: &Chain, range: &BlockRange) -> bool {
            let w = range.watermark();
            chain.0.iter().rev().any(|s| s.range.watermark() == w)
        }

        // Create a single chain that represents the common state between the physical tables.
        let src_chain = {
            let mut src_chains: Vec<Chain> = Default::default();
            for table in &self.tables {
                match table.canonical_chain().await? {
                    Some(canonical) => src_chains.push(canonical),
                    None => return Ok(None),
                };
            }
            // sort by end block number, in ascending order.
            src_chains.sort_unstable_by_key(|c| c.end());
            if src_chains.is_empty() {
                return Ok(None);
            }
            let mut shortest_chain = src_chains.remove(0);

            // Remove the latest segments that don't have matching watermarks on all other chains.
            while !shortest_chain.0.is_empty() {
                let last_range = shortest_chain.last();
                if src_chains.iter().all(|c| contains_watermark(c, last_range)) {
                    break;
                }
                shortest_chain.0.pop();
            }
            if shortest_chain.0.is_empty() {
                return Ok(None);
            }
            shortest_chain
        };

        let range = match self.stream_chain.take() {
            None => src_chain.start()..=src_chain.end(),
            Some(mut stream_chain) => {
                // Remove latest segments that don't have matching watermarks on the source chain.
                while !stream_chain.0.is_empty() {
                    let last_range = stream_chain.last();
                    if contains_watermark(&src_chain, last_range) {
                        break;
                    }
                    stream_chain.0.pop();
                }
                if stream_chain.0.is_empty() {
                    src_chain.start()..=src_chain.end()
                } else {
                    (stream_chain.end() + 1)..=src_chain.end()
                }
            }
        };
        self.stream_chain = Some(src_chain);
        Ok(Some(range))
    }
}

/// Creates a stream of block range updates for the tables in the context.
///
/// `end_block` can be used to force the stream to end once a specific block number is reached.
#[instrument(skip_all, err)]
async fn block_range_updates(
    ctx: Arc<QueryContext>,
    multiplexer_handle: &NotificationMultiplexerHandle,
) -> Result<BlockRangeStream, BoxError> {
    let tables = ctx.catalog().tables().to_vec();

    // Set up change notifications
    let locations = ctx.catalog().tables().iter().map(|t| t.location_id());
    let mut notification_streams = Vec::new();
    for location in locations {
        let receiver = multiplexer_handle.subscribe(location).await;
        let stream = WatchStream::new(receiver).map(move |_| Ok::<LocationId, BoxError>(location));
        notification_streams.push(stream);
    }

    let mut notifications = futures::stream::select_all(notification_streams);

    // Create the stream channel. This is unbounded because we never want to put backpressure on the
    // PG notification queue.
    let (tx, rx) = mpsc::unbounded_channel();
    let mut execution_ranges = ExecutionRanges::new(tables);
    tx.send(execution_ranges.next().await).unwrap();

    // Spawn task to handle new ranges from notifications
    tokio::spawn(async move {
        while let Some(Ok(_)) = notifications.next().await {
            let result = execution_ranges.next().await;
            if tx.send(result).is_err() {
                break; // Receiver dropped
            }
        }
        tracing::warn!("notification stream ended");
    });

    Ok(Box::pin(UnboundedReceiverStream::new(rx)))
}

/// Represents a message from the streaming query, which can be either data or a completion signal.
/// Receiving `Completed(n)` indicates that the query has emitted all outputs up to block number `n`.
///
/// Completion points do not necessarily follow increments of 1, as the query progresses in batches.
pub enum QueryMessage {
    Data(RecordBatch),
    Completed(BlockNum),
}

impl QueryMessage {
    fn as_data(self) -> Option<RecordBatch> {
        match self {
            QueryMessage::Data(data) => Some(data),
            QueryMessage::Completed(_) => None,
        }
    }
}

/// A handle to a streaming query that can be used to retrieve results as a stream.
///
/// Aborts the query task when dropped.
pub struct StreamingQueryHandle {
    rx: mpsc::Receiver<QueryMessage>,
    join_handle: AbortOnDropHandle<Result<(), BoxError>>,
    schema: SchemaRef,
}

impl StreamingQueryHandle {
    pub fn as_stream(self) -> impl Stream<Item = Result<QueryMessage, BoxError>> + Unpin {
        let data_stream = ReceiverStream::new(self.rx);

        let join = self.join_handle;

        // If `tx` has been dropped then the query task has terminated. So we check if it has
        // terminated with errors, and if so send the error as the final item of the stream.
        let get_task_result = async move {
            // Unwrap: The task is known to have terminated.
            match join.now_or_never().unwrap() {
                Ok(Ok(())) => None,
                Ok(Err(e)) => Some(Err(e)),
                Err(join_err) => Some(Err(
                    format!("Streaming task failed to join: {}", join_err).into()
                )),
            }
        };

        data_stream
            .map(Ok)
            .chain(stream::once(get_task_result).filter_map(|x| async { x }))
            .boxed()
    }

    pub fn as_record_batch_stream(self) -> SendableRecordBatchStream {
        let schema = self.schema.clone();
        let stream = RecordBatchStreamAdapter::new(
            schema,
            self.as_stream()
                .try_filter_map(|m| async { Ok(m.as_data()) })
                .map_err(DataFusionError::External),
        );
        Box::pin(stream)
    }
}

/// A streaming query that continuously listens for new blocks and emits incremental results.
///
/// This follows a 'microbatch' model where it processes data in chunks based on a block range
/// stream.
pub struct StreamingQuery {
    ctx: Arc<QueryContext>,
    plan: LogicalPlan,
    start_block: BlockNum,
    end_block: Option<BlockNum>,
    block_range_stream: BlockRangeStream,
    tx: mpsc::Sender<QueryMessage>,
    microbatch_max_interval: u64,
    preserve_block_num: bool,
}

impl StreamingQuery {
    /// Creates a new streaming query. It is assumed that the `ctx` was built such that it contains
    /// only the tables relevant for the query.
    ///
    /// The query execution loop will run in its own task.
    pub async fn spawn(
        ctx: Arc<QueryContext>,
        plan: LogicalPlan,
        start_block: BlockNum,
        end_block: Option<BlockNum>,
        multiplexer_handle: &NotificationMultiplexerHandle,
        is_sql_dataset: bool,
        microbatch_max_interval: u64,
    ) -> Result<StreamingQueryHandle, BoxError> {
        let schema: SchemaRef = plan.schema().clone().as_ref().clone().into();
        let (tx, rx) = mpsc::channel(10);

        // Preserve `_block_num` for SQL materializaiton or if explicitly selected in the schema.
        let preserve_block_num = is_sql_dataset
            || plan
                .schema()
                .fields()
                .iter()
                .any(|f| f.name() == SPECIAL_BLOCK_NUM);

        // Plan transformations for streaming:
        // - Propagate the `_block_num` column.
        // - Enforce `order by _block_num`.
        // - Run logical optimizations ahead of execution.
        let plan = {
            let mut plan = propagate_block_num(plan)?;
            plan = order_by_block_num(plan);
            let plan = ctx.optimize_plan(&plan).await?;
            plan
        };

        let block_range_stream = block_range_updates(ctx.clone(), multiplexer_handle).await?;
        let streaming_query = Self {
            ctx,
            plan,
            tx,
            start_block,
            end_block,
            block_range_stream,
            microbatch_max_interval,
            preserve_block_num,
        };

        let join_handle = AbortOnDropHandle::new(tokio::spawn(streaming_query.execute()));

        Ok(StreamingQueryHandle {
            rx,
            join_handle,
            schema,
        })
    }

    /// The loop:
    /// 1. Get new input range
    /// 2. Start executing microbatch for the range
    /// 3. Stream out time-ordered results
    /// 4. Once execution of batch is exhausted, send completion trigger
    #[instrument(skip_all, err)]
    async fn execute(mut self) -> Result<(), BoxError> {
        loop {
            // Get the next execution range
            let range = {
                let Some(range) = self.block_range_stream.next().await else {
                    // Stream ended
                    return Ok(());
                };
                let Some(range) = range? else {
                    // Tables seem empty, lets wait for some data
                    continue;
                };

                let (start, end) = range.into_inner();
                let range =
                    start.max(self.start_block)..=self.end_block.map(|e| e.min(end)).unwrap_or(end);
                if range.start() > range.end() {
                    continue;
                }
                range
            };

            // Process in chunks based on microbatch_max_interval
            let mut microbatch_start = *range.start();
            while microbatch_start <= *range.end() {
                let microbatch_end = BlockNum::min(
                    microbatch_start + self.microbatch_max_interval - 1,
                    *range.end(),
                );

                // Start microbatch execution for this chunk
                let mut stream = self
                    .ctx
                    .execute_plan_for_range(
                        self.plan.clone(),
                        microbatch_start,
                        microbatch_end,
                        self.preserve_block_num,
                        false,
                    )
                    .await?;

                // Drain the microbatch completely
                while let Some(item) = stream.next().await {
                    let item = item?;

                    // If the receiver in `StreamingQueryHandle` is dropped, then this task has been
                    // aborted, so we don't bother checking for errors when sending a message.
                    let _ = self.tx.send(QueryMessage::Data(item)).await;
                }

                // Send completion message for this chunk
                let _ = self.tx.send(QueryMessage::Completed(microbatch_end)).await;

                microbatch_start = microbatch_end + 1;
            }

            if Some(*range.end()) == self.end_block {
                // If we reached the end block, we are done
                return Ok(());
            }
        }
    }
}
