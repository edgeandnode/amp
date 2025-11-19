pub mod message_stream_with_block_complete;

use std::{collections::BTreeMap, sync::Arc};

use alloy::{hex::ToHexExt as _, primitives::BlockHash};
use common::{
    BlockNum, BoxError, DetachedLogicalPlan, LogicalCatalog, PlanningContext, QueryContext,
    SPECIAL_BLOCK_NUM,
    arrow::{array::RecordBatch, datatypes::SchemaRef},
    catalog::physical::{Catalog, PhysicalTable},
    incrementalizer::incrementalize_plan,
    metadata::segments::{BlockRange, ResumeWatermark, Segment, Watermark},
    plan_visitors::{order_by_block_num, unproject_special_block_num_column},
    query_context::QueryEnv,
    sql_str::SqlStr,
};
use datafusion::{common::cast::as_fixed_size_binary_array, error::DataFusionError};
use dataset_store::{DatasetStore, resolve_blocks_table};
use futures::{
    FutureExt,
    stream::{self, BoxStream, StreamExt},
};
use message_stream_with_block_complete::MessageStreamWithBlockComplete;
use metadata_db::{LocationId, NotificationMultiplexerHandle};
use tokio::sync::{mpsc, watch};
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::task::AbortOnDropHandle;
use tracing::{Instrument, debug, instrument};

/// Awaits any update for tables in a query context catalog.
struct TableUpdates {
    subscriptions: BTreeMap<LocationId, watch::Receiver<()>>,
    ready: bool,
}

impl TableUpdates {
    async fn new(catalog: &Catalog, multiplexer_handle: &NotificationMultiplexerHandle) -> Self {
        let mut subscriptions: BTreeMap<LocationId, watch::Receiver<()>> = Default::default();
        for table in catalog.tables() {
            let location = table.location_id();
            subscriptions.insert(location, multiplexer_handle.subscribe(location).await);
        }
        Self {
            subscriptions,
            ready: true,
        }
    }

    fn set_ready(&mut self) {
        self.ready = true
    }

    async fn changed(&mut self) {
        if self.ready {
            self.ready = false;
            return;
        }

        // Never return if there are no remaining subscriptions.
        if self.subscriptions.is_empty() {
            std::future::pending::<()>().await;
        }

        let notifications = self
            .subscriptions
            .iter_mut()
            .map(|(location, rx)| {
                Box::pin(async move { rx.changed().await.map_err(|_| *location) })
            })
            .collect::<Vec<_>>();
        let result = futures::future::select_all(notifications).await.0;
        if let Err(location) = result {
            tracing::warn!("notifications ended for location {location}");
            self.subscriptions.remove(&location);
        }
    }
}

/// Represents a message from the streaming query, which can be either data or a completion signal.
///
/// Completion points do not necessarily follow increments of 1, as the query progresses in batches.
pub enum QueryMessage {
    MicrobatchStart { range: BlockRange, is_reorg: bool },
    Data(RecordBatch),
    MicrobatchEnd(BlockRange),

    //// Indicates that the query has emitted all outputs up to the given block number.
    BlockComplete(BlockNum),
}

struct MicrobatchRange {
    range: BlockRange,
    direction: StreamDirection,
}

/// Direction of the stream. Helpful to distinguish reorgs, with the payload being the first block
/// after the base of the fork.
enum StreamDirection {
    ForwardFrom(BlockRow),
    ReorgFrom(BlockRow),
}

impl StreamDirection {
    fn block(&self) -> &BlockRow {
        match self {
            StreamDirection::ForwardFrom(block) => block,
            StreamDirection::ReorgFrom(block) => block,
        }
    }

    fn is_reorg(&self) -> bool {
        matches!(self, StreamDirection::ReorgFrom(_))
    }
}
/// A handle to a streaming query that can be used to retrieve results as a stream.
///
/// Aborts the query task when dropped.
pub struct StreamingQueryHandle {
    rx: mpsc::Receiver<QueryMessage>,
    join_handle: AbortOnDropHandle<Result<(), BoxError>>,
}

impl StreamingQueryHandle {
    pub fn as_stream(self) -> BoxStream<'static, Result<QueryMessage, BoxError>> {
        let data_stream = MessageStreamWithBlockComplete::new(ReceiverStream::new(self.rx).map(Ok));

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
            .chain(stream::once(get_task_result).filter_map(|x| async { x }))
            .boxed()
    }
}

/// A streaming query that continuously listens for new blocks and emits incremental results.
///
/// This follows a 'microbatch' model where it processes data in chunks based on a block range
/// stream.
pub struct StreamingQuery {
    query_env: QueryEnv,
    catalog: Catalog,
    plan: DetachedLogicalPlan,
    start_block: BlockNum,
    end_block: Option<BlockNum>,
    table_updates: TableUpdates,
    tx: mpsc::Sender<QueryMessage>,
    microbatch_max_interval: u64,
    keep_alive_interval: Option<u64>,
    destination: Option<Arc<PhysicalTable>>,
    preserve_block_num: bool,
    network: String,
    /// `blocks` table for the network associated with the catalog.
    blocks_table: Arc<PhysicalTable>,
    /// The watermark associated with the previously processed range. This may be provided by the
    /// consumer to resume a stream.
    prev_watermark: Option<Watermark>,
}

impl StreamingQuery {
    /// Creates a new streaming query. It is assumed that the `ctx` was built such that it contains
    /// only the tables relevant for the query.
    ///
    /// The query execution loop will run in its own task.
    #[instrument(skip_all, err)]
    #[expect(clippy::too_many_arguments)]
    pub async fn spawn(
        query_env: QueryEnv,
        catalog: Catalog,
        dataset_store: Arc<DatasetStore>,
        plan: DetachedLogicalPlan,
        start_block: BlockNum,
        end_block: Option<BlockNum>,
        resume_watermark: Option<ResumeWatermark>,
        multiplexer_handle: &NotificationMultiplexerHandle,
        destination: Option<Arc<PhysicalTable>>,
        microbatch_max_interval: u64,
        keep_alive_interval: Option<u64>,
    ) -> Result<StreamingQueryHandle, BoxError> {
        let (tx, rx) = mpsc::channel(10);

        // Preserve `_block_num` for SQL materializaiton or if explicitly selected in the schema.
        let preserve_block_num = destination.is_some()
            || plan
                .schema()
                .fields()
                .iter()
                .any(|f| f.name() == SPECIAL_BLOCK_NUM);

        // This plan is the starting point of each microbatch execution. Transformations applied to it:œ
        // - Propagate the `_block_num` column.
        // - Run logical optimizations ahead of execution.
        let plan = {
            let plan = plan.propagate_block_num()?;

            let ctx = PlanningContext::new(catalog.logical().clone());
            ctx.optimize_plan(&plan).await?
        };

        let tables: Vec<Arc<PhysicalTable>> = catalog.tables().to_vec();
        let network = tables.iter().map(|t| t.network()).next().unwrap();
        let src_datasets = tables
            .iter()
            .map(|t| (t.dataset().manifest_hash().clone(), t.dataset().clone()))
            .collect();
        let blocks_table = resolve_blocks_table(&dataset_store, src_datasets, network).await?;
        let table_updates = TableUpdates::new(&catalog, multiplexer_handle).await;
        let prev_watermark = resume_watermark
            .map(|w| w.to_watermark(network))
            .transpose()?;
        let streaming_query = Self {
            query_env,
            catalog,
            plan,
            tx,
            start_block,
            end_block,
            prev_watermark,
            table_updates,
            microbatch_max_interval,
            keep_alive_interval,
            destination,
            preserve_block_num,
            network: network.to_string(),
            blocks_table: Arc::new(blocks_table),
        };

        let join_handle =
            AbortOnDropHandle::new(tokio::spawn(streaming_query.execute().in_current_span()));

        Ok(StreamingQueryHandle { rx, join_handle })
    }

    /// The loop:
    /// 1. Get new input range
    /// 2. Start executing microbatch for the range
    /// 3. Stream out time-ordered results
    /// 4. Once execution of batch is exhausted, send completion trigger
    #[instrument(skip_all, err)]
    async fn execute(mut self) -> Result<(), BoxError> {
        loop {
            self.table_updates.changed().await;

            // The table snapshots to execute the microbatch against.
            let ctx =
                QueryContext::for_catalog(self.catalog.clone(), self.query_env.clone(), false)
                    .await?;

            // Get the next execution range
            let Some(MicrobatchRange { range, direction }) =
                self.next_microbatch_range(&ctx).await?
            else {
                continue;
            };

            tracing::debug!("execute range [{}-{}]", range.start(), range.end());
            
            let plan = {
                // Incrementalize the plan
                let plan = self.plan.clone().attach_to(&ctx)?;
                let mut plan = incrementalize_plan(plan, range.start(), range.end())?;

                // Enforce `order by _block_num`.
                plan = order_by_block_num(plan);

                // Remove `_block_num` if not needed in the output.
                if !self.preserve_block_num {
                    plan = unproject_special_block_num_column(plan)?
                }
                plan
            };
            
            
            let mut stream = if let Some(keep_alive_interval) = self.keep_alive_interval {
                let schema = Arc::new(plan.schema().as_arrow().clone());
                keep_alive_stream(
                    ctx.execute_plan(plan, false).await?,
                    schema,
                    keep_alive_interval,
                )
            } else {
                ctx.execute_plan(plan, false).await?
            };

            // Send start message for this microbatch
            let _ = self
                .tx
                .send(QueryMessage::MicrobatchStart {
                    range: range.clone(),
                    is_reorg: direction.is_reorg(),
                })
                .await;

            // Drain the microbatch completely
            while let Some(item) = stream.next().await {
                let item = item?;

                // If the receiver in `StreamingQueryHandle` is dropped, then this task has been
                // aborted, so we don't bother checking for errors when sending a message.
                let _ = self.tx.send(QueryMessage::Data(item)).await;
            }

            // Send end message for this microbatch
            let _ = self
                .tx
                .send(QueryMessage::MicrobatchEnd(range.clone()))
                .await;

            if Some(range.end()) == self.end_block {
                // If we reached the end block, we are done
                return Ok(());
            }
            self.prev_watermark = Some(range.watermark());
        }
    }

    #[instrument(skip_all, err)]
    async fn next_microbatch_range(
        &mut self,
        ctx: &QueryContext,
    ) -> Result<Option<MicrobatchRange>, BoxError> {
        // Gather the chains for each source table.
        let chains = ctx
            .catalog()
            .table_snapshots()
            .iter()
            .map(|s| s.canonical_segments());

        // Use a single context for all queries against the blocks table. This is to keep a
        // consistent reference chain within the scope of this function.
        let blocks_ctx = {
            // Construct a catalog for the single `blocks_table`.
            let catalog = {
                let logical =
                    LogicalCatalog::from_tables(std::iter::once(self.blocks_table.table()));
                Catalog::new(vec![self.blocks_table.clone()], logical)
            };
            QueryContext::for_catalog(catalog, self.query_env.clone(), false).await?
        };

        // The latest common watermark across the source tables.
        let Some(common_watermark) = self.latest_src_watermark(&blocks_ctx, chains).await? else {
            // No common watermark across source tables.
            debug!("no common watermark found");
            return Ok(None);
        };

        if common_watermark.number < self.start_block {
            // Common watermark hasn't reached the requested start block yet.
            return Ok(None);
        }

        let Some(direction) = self.next_microbatch_start(&blocks_ctx).await? else {
            debug!("no next microbatch start found");
            return Ok(None);
        };
        let start = direction.block();
        let Some(end) = self
            .next_microbatch_end(&blocks_ctx, start, common_watermark)
            .await?
        else {
            debug!("no next microbatch end found");
            return Ok(None);
        };
        Ok(Some(MicrobatchRange {
            range: BlockRange {
                numbers: start.number..=end.number,
                network: self.network.clone(),
                hash: end.hash,
                prev_hash: start.prev_hash,
            },
            direction,
        }))
    }

    #[instrument(skip_all, err)]
    async fn next_microbatch_start(
        &self,
        ctx: &QueryContext,
    ) -> Result<Option<StreamDirection>, BoxError> {
        match &self.prev_watermark {
            // start stream
            None => {
                let block = self.blocks_table_fetch(ctx, self.start_block, None).await?;
                Ok(block.map(StreamDirection::ForwardFrom))
            }
            // continue stream
            Some(prev) if self.blocks_table_contains(ctx, prev).await? => {
                let block = self.blocks_table_fetch(ctx, prev.number + 1, None).await?;
                Ok(block.map(StreamDirection::ForwardFrom))
            }
            // rewind stream due to reorg
            Some(prev) => {
                let block = self.reorg_base(ctx, prev).await?;
                Ok(block.map(StreamDirection::ReorgFrom))
            }
        }
    }

    #[instrument(skip_all, err)]
    async fn next_microbatch_end(
        &mut self,
        ctx: &QueryContext,
        start: &BlockRow,
        common_watermark: Watermark,
    ) -> Result<Option<Watermark>, BoxError> {
        let number = {
            let end = self
                .end_block
                .map(|end_block| BlockNum::min(common_watermark.number, end_block))
                .unwrap_or(common_watermark.number);
            let limit = start.number + self.microbatch_max_interval - 1;
            if end > limit {
                // We're limiting this batch, so make sure we can immediately continue to the next
                // range regardless of source table updates.
                self.table_updates.set_ready();
                limit
            } else {
                end
            }
        };
        if number < start.number {
            // Invalid range: end block is before start block.
            return Ok(None);
        }
        if number == common_watermark.number {
            Ok(Some(common_watermark))
        } else {
            self.blocks_table_fetch(ctx, number, None)
                .await
                .map(|r| r.map(|r| r.watermark()))
        }
    }

    #[instrument(skip_all, err)]
    async fn latest_src_watermark(
        &self,
        ctx: &QueryContext,
        chains: impl Iterator<Item = &[Segment]>,
    ) -> Result<Option<Watermark>, BoxError> {
        // For each chain, collect the latest segment
        let mut latest_src_watermarks: Vec<Watermark> = Default::default();
        'chain_loop: for chain in chains {
            for segment in chain.iter().rev() {
                let watermark = segment.range.watermark();
                if self.blocks_table_contains(ctx, &watermark).await? {
                    latest_src_watermarks.push(watermark);
                    continue 'chain_loop;
                }
            }
            return Ok(None);
        }
        // Select the minimum table watermark as the end.
        Ok(latest_src_watermarks
            .iter()
            .min_by_key(|w| w.number)
            .cloned())
    }

    /// Find the block to resume streaming from after detecting a reorg.
    ///
    /// When a streaming query detects that the previous block range is no longer on the canonical
    /// chain (indicating a reorg), this method walks backwards from the end of the previous block
    /// range to find the latest adjacent block that exists on the canonical chain.
    #[instrument(skip_all, err)]
    async fn reorg_base(
        &self,
        ctx: &QueryContext,
        prev_watermark: &Watermark,
    ) -> Result<Option<BlockRow>, BoxError> {
        // context for querying forked blocks
        let fork_ctx = {
            let catalog = Catalog::new(
                ctx.catalog().physical_tables().cloned().collect(),
                ctx.catalog().logical().clone(),
            );
            QueryContext::for_catalog(catalog, ctx.env.clone(), true).await?
        };

        let mut min_fork_block_num = prev_watermark.number;
        let mut fork: Option<BlockRow> = self
            .blocks_table_fetch(&fork_ctx, prev_watermark.number, Some(&prev_watermark.hash))
            .await?;
        while let Some(block) = fork.take() {
            if self.blocks_table_contains(ctx, &block.watermark()).await? {
                break;
            }
            min_fork_block_num = block.number;
            fork = self
                .blocks_table_fetch(
                    &fork_ctx,
                    block.number.saturating_sub(1),
                    block.prev_hash.as_ref(),
                )
                .await?;
        }

        // If we're dumping a derived dataset, we must rewind to the start of the canonical segment
        // boudary. Otherwise, the new segments may not form a canonical chain.
        if let Some(destination) = self.destination.as_ref()
            && let Some(destination_chain) = destination.canonical_chain().await?
        {
            min_fork_block_num = *destination_chain
                .0
                .iter()
                .rev()
                .map(|s| &s.range.numbers)
                .find(|r| r.contains(&min_fork_block_num))
                .unwrap_or(&(0..=0))
                .start();
        }

        self.blocks_table_fetch(ctx, min_fork_block_num, None).await
    }

    #[instrument(skip_all, err)]
    async fn blocks_table_contains(
        &self,
        ctx: &QueryContext,
        watermark: &Watermark,
    ) -> Result<bool, BoxError> {
        self.blocks_table_fetch(ctx, watermark.number, Some(&watermark.hash))
            .await
            .map(|row| row.is_some())
    }

    #[instrument(skip(self, ctx), err)]
    async fn blocks_table_fetch(
        &self,
        ctx: &QueryContext,
        number: BlockNum,
        hash: Option<&BlockHash>,
    ) -> Result<Option<BlockRow>, BoxError> {
        let hash_constraint = hash
            .map(|h| format!("AND hash = x'{}'", h.encode_hex()))
            .unwrap_or_default();
        let sql = format!(
            "SELECT hash, parent_hash FROM {} WHERE block_num = {} {} LIMIT 1",
            self.blocks_table.table_ref(),
            number,
            hash_constraint,
        );

        // SAFETY: Validation is deferred to the SQL parser which will return appropriate errors
        // for empty or invalid SQL. The format! macro ensures non-empty output.
        let sql_str = SqlStr::new_unchecked(sql);
        let query = common::sql::parse(&sql_str)?;
        let plan = ctx.plan_sql(query).await?;
        let results = ctx.execute_and_concat(plan).await?;
        if results.num_rows() == 0 {
            debug!("blocks table missing block {} {:?}", number, hash);
            return Ok(None);
        }
        let get_hash_value = |column_name: &str| -> Option<BlockHash> {
            let column =
                as_fixed_size_binary_array(results.column_by_name(column_name).unwrap()).unwrap();
            let bytes = column.iter().flatten().next();
            bytes.map(|b| b.try_into().unwrap())
        };
        Ok(Some(BlockRow {
            number,
            hash: get_hash_value("hash").unwrap(),
            prev_hash: get_hash_value("parent_hash"),
        }))
    }
}

struct BlockRow {
    number: BlockNum,
    hash: BlockHash,
    prev_hash: Option<BlockHash>,
}

impl BlockRow {
    fn watermark(&self) -> Watermark {
        Watermark {
            number: self.number,
            hash: self.hash,
        }
    }
}

/// Wraps a record batch stream to periodically emit empty record batches as keep-alive signals.
/// These empty batches have the same schema as the original stream.
///
/// The keep-alive batches are emitted at the specified interval (in seconds) until the original
/// stream is exhausted.
pub fn keep_alive_stream<'a>(
    record_batch_stream: BoxStream<'a, Result<RecordBatch, DataFusionError>>,
    schema: SchemaRef,
    keep_alive_interval: u64,
) -> BoxStream<'a, Result<RecordBatch, DataFusionError>> {
    let period = tokio::time::Duration::from_secs(keep_alive_interval);
    let mut keep_alive_interval = tokio::time::interval(period);
    let mut record_batch_stream = record_batch_stream.fuse();

    Box::pin(async_stream::stream! {
        loop {
            tokio::select! {
                biased;

                maybe_batch = record_batch_stream.next() => {
                    match maybe_batch {
                        Some(batch) => {
                            yield batch;
                        }
                        None => {
                            break;
                        }
                    }
                }

                _ = keep_alive_interval.tick() => {
                    let empty_batch = RecordBatch::new_empty(schema.clone());
                    yield Ok(empty_batch);
                }
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use common::arrow::{
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    };
    use datafusion::error::DataFusionError;
    use futures::stream;

    use super::keep_alive_stream;

    #[tokio::test]
    async fn test_keep_alive_stream() {
        use tokio_stream::StreamExt;
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Utf8, false),
        ]));

        let record_batches = vec![
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(common::arrow::array::Int32Array::from(vec![1, 2, 3])),
                    Arc::new(common::arrow::array::StringArray::from(vec!["x", "y", "z"])),
                ],
            )
            .unwrap(),
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(common::arrow::array::Int32Array::from(vec![4, 5, 6])),
                    Arc::new(common::arrow::array::StringArray::from(vec!["u", "v", "w"])),
                ],
            )
            .unwrap(),
        ];
        let original_batches_count = record_batches.len();

        let record_batch_stream = Box::pin(
            stream::iter(
                record_batches
                    .into_iter()
                    .map(Ok)
                    .collect::<Vec<Result<RecordBatch, DataFusionError>>>(),
            )
            .throttle(Duration::from_secs(2)),
        );

        let keep_alive_interval = 1; // 1 second for testing
        let mut stream =
            keep_alive_stream(record_batch_stream, schema.clone(), keep_alive_interval);

        let mut received_batches = Vec::new();
        let mut ticks = 0;

        while let Some(Ok(batch)) = stream.next().await {
            if batch.num_rows() == 0 {
                ticks += 1;
            }
            received_batches.push(batch);
        }

        assert!(
            received_batches.len() == original_batches_count + ticks,
            "Expected total batches to be {}, got {}",
            original_batches_count + ticks,
            received_batches.len()
        );
    }
}
