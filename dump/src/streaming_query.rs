use std::{pin::Pin, sync::Arc};

use common::{
    BlockNum, BoxError, SPECIAL_BLOCK_NUM,
    arrow::{array::RecordBatch, datatypes::SchemaRef},
    catalog::physical::PhysicalTable,
    metadata::segments::{Chain, Watermark},
    notification_multiplexer::NotificationMultiplexerHandle,
    plan_visitors::{order_by_block_num, propagate_block_num},
    query_context::{QueryContext, QueryEnv, parse_sql},
};
use datafusion::{
    common::cast::{as_fixed_size_binary_array, as_uint64_array},
    error::DataFusionError,
    execution::SendableRecordBatchStream,
    logical_expr::LogicalPlan,
    physical_plan::stream::RecordBatchStreamAdapter,
};
use dataset_store::{DatasetStore, resolve_blocks_table};
use futures::{
    FutureExt, Stream, TryStreamExt as _,
    stream::{self, StreamExt},
};
use metadata_db::LocationId;
use tokio::sync::mpsc;
use tokio_stream::wrappers::{ReceiverStream, UnboundedReceiverStream, WatchStream};
use tokio_util::task::AbortOnDropHandle;
use tracing::instrument;

#[derive(Clone, Debug)]
struct Watermarks {
    start: Watermark,
    end: Watermark,
}

type WatermarksStream =
    Pin<Box<dyn Stream<Item = Result<Option<Watermarks>, BoxError>> + Send + Sync + 'static>>;

/// Calculates the watermarks for the next batch of SQL execution. The result is based on the set
/// of physical tables the query executes over and the sequence of segments already processed.
struct ExecutionWatermarks {
    /// Physical tables used for SQL execution.
    tables: Vec<Arc<PhysicalTable>>,
    /// Previously processed watermarks. These may be provided by the consumer to resume a stream.
    prev_watermarks: Option<Watermarks>,
    /// `blocks` table for the network associated with the physical tables.
    blocks_table: String,

    dataset_store: Arc<DatasetStore>,
    env: QueryEnv,
}

impl ExecutionWatermarks {
    /// Determines the watermarks for the next batch SQL query. This returns the range starting
    /// from the latest processed segments up to the latest common watermark across all relevant
    /// tables. The resulting range may overlap with previous ranges to re-execute over segments
    /// that have been reorganized out of the canonical chain.
    async fn next(&mut self) -> Result<Option<Watermarks>, BoxError> {
        let mut chains: Vec<Chain> = Default::default();
        for table in &self.tables {
            let Some(chain) = table.canonical_chain().await? else {
                return Ok(None);
            };
            chains.push(chain);
        }

        // For each chain, collect the latest segment
        let mut latest_src_watermarks: Vec<Watermark> = Default::default();
        'chain_loop: for chain in &chains {
            for segment in chain.0.iter().rev() {
                let watermark = segment.range.watermark();
                if self.canonical_chain_contains(&watermark).await? {
                    latest_src_watermarks.push(watermark);
                    continue 'chain_loop;
                }
            }
            return Ok(None);
        }
        // Select the minimum table watermark as the end.
        let Some(end) = latest_src_watermarks
            .iter()
            .min_by_key(|w| w.number)
            .cloned()
        else {
            return Ok(None);
        };

        let watermarks = match &self.prev_watermarks {
            None => {
                let Some(start) = self.canonical_chain_start().await? else {
                    return Ok(None);
                };
                Watermarks { start, end }
            }
            Some(previous) if self.canonical_chain_contains(&previous.end).await? => {
                let start_number = previous.end.number + 1;
                let Some(start) = self.canonical_chain_watermark(start_number).await? else {
                    return Ok(None);
                };
                Watermarks { start, end }
            }
            Some(previous) if self.canonical_chain_contains(&previous.start).await? => {
                // Reorg between previous start and end
                let start = previous.start.clone();
                Watermarks { start, end }
            }
            Some(previous) => {
                // Reorg before previous start
                let src_chain = chains.iter().min_by_key(|c| c.end()).unwrap();
                let mut start: Option<BlockNum> = None;
                for segment in src_chain.0.iter().rev() {
                    // Skip segments after the previous watermarks
                    if previous.end.number > segment.range.end() {
                        continue;
                    }
                    let segment_watermark = segment.range.watermark();
                    if self.canonical_chain_contains(&segment_watermark).await? {
                        start = Some(segment.range.start());
                        break;
                    }
                }
                match start {
                    None => return Ok(None),
                    Some(number) => match self.canonical_chain_watermark(number).await? {
                        None => return Ok(None),
                        Some(start) => Watermarks { start, end },
                    },
                }
            }
        };
        self.prev_watermarks = Some(watermarks.clone());
        Ok(Some(watermarks))
    }

    async fn canonical_chain_start(&self) -> Result<Option<Watermark>, BoxError> {
        let query = parse_sql(&format!(
            "SELECT block_num, hash FROM {} ORDER BY block_num ASC LIMIT 1",
            self.blocks_table,
        ))?;
        let ctx = self
            .dataset_store
            .ctx_for_sql(&query, self.env.clone())
            .await?;
        let plan = ctx.plan_sql(query).await?;
        let results = ctx.execute_and_concat(plan).await?;
        assert!(results.num_rows() <= 1);
        if results.num_rows() == 0 {
            return Ok(None);
        }
        let number = as_uint64_array(results.column_by_name("block_num").unwrap())
            .unwrap()
            .value(0)
            .try_into()
            .unwrap();
        let hash = as_fixed_size_binary_array(results.column_by_name("hash").unwrap())
            .unwrap()
            .value(0)
            .try_into()
            .unwrap();
        Ok(Some(Watermark { number, hash }))
    }

    async fn canonical_chain_contains(&self, watermark: &Watermark) -> Result<bool, BoxError> {
        let query = parse_sql(&format!(
            "SELECT 1 FROM {} WHERE block_num = {} AND hash = {}",
            self.blocks_table, watermark.number, watermark.hash,
        ))?;
        let ctx = self
            .dataset_store
            .ctx_for_sql(&query, self.env.clone())
            .await?;
        let plan = ctx.plan_sql(query).await?;
        let results = ctx.execute_and_concat(plan).await?;
        assert!(results.num_rows() <= 1);
        Ok(results.num_rows() == 1)
    }

    async fn canonical_chain_watermark(
        &self,
        number: BlockNum,
    ) -> Result<Option<Watermark>, BoxError> {
        let query = parse_sql(&format!(
            "SELECT hash FROM {} WHERE block_num = {}",
            self.blocks_table, number,
        ))?;
        let ctx = self
            .dataset_store
            .ctx_for_sql(&query, self.env.clone())
            .await?;
        let plan = ctx.plan_sql(query).await?;
        let results = ctx.execute_and_concat(plan).await?;
        assert!(results.num_rows() <= 1);
        if results.num_rows() == 0 {
            return Ok(None);
        }
        let hash = as_fixed_size_binary_array(results.column_by_name("hash").unwrap())
            .unwrap()
            .value(0)
            .try_into()
            .unwrap();
        Ok(Some(Watermark { number, hash }))
    }
}

/// Creates a stream of block range updates for the tables in the context.
///
/// `end_block` can be used to force the stream to end once a specific block number is reached.
#[instrument(skip_all, err)]
async fn watermarks_updates(
    ctx: Arc<QueryContext>,
    dataset_store: Arc<DatasetStore>,
    multiplexer_handle: &NotificationMultiplexerHandle,
) -> Result<WatermarksStream, BoxError> {
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

    let network = tables.iter().map(|t| t.network()).next().unwrap();
    let src_datasets = tables.iter().map(|t| t.dataset().name.as_str()).collect();
    let blocks_table = resolve_blocks_table(&dataset_store, &src_datasets, network).await?;
    let mut watermarks = ExecutionWatermarks {
        tables,
        // TODO: Set from client to resume a stream after dropping a connection.
        prev_watermarks: None,
        blocks_table,
        dataset_store,
        env: ctx.env.clone(),
    };

    tx.send(watermarks.next().await).unwrap();

    // Spawn task to handle new ranges from notifications
    tokio::spawn(async move {
        while let Some(Ok(_)) = notifications.next().await {
            let result = watermarks.next().await;
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
    watermarks_stream: WatermarksStream,
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
        dataset_store: Arc<DatasetStore>,
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

        let watermarks_stream =
            watermarks_updates(ctx.clone(), dataset_store, multiplexer_handle).await?;
        let streaming_query = Self {
            ctx,
            plan,
            tx,
            start_block,
            end_block,
            watermarks_stream,
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
            let (start, end) = {
                let Some(watermarks) = self.watermarks_stream.next().await else {
                    // Stream ended
                    return Ok(());
                };
                let Some(watermarks) = watermarks? else {
                    // Tables seem empty, lets wait for some data
                    continue;
                };

                let start = BlockNum::max(watermarks.start.number, self.start_block);
                let end = BlockNum::min(
                    watermarks.end.number,
                    self.end_block.unwrap_or(BlockNum::MAX),
                );
                (start, end)
            };

            // Process in chunks based on microbatch_max_interval
            let mut microbatch_start = start;
            while microbatch_start <= end {
                let microbatch_end =
                    BlockNum::min(microbatch_start + self.microbatch_max_interval - 1, end);

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

            if Some(end) == self.end_block {
                // If we reached the end block, we are done
                return Ok(());
            }
        }
    }
}
