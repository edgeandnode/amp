use std::{collections::BTreeMap, sync::Arc};

use alloy::{hex::ToHexExt as _, primitives::BlockHash};
use common::{
    BlockNum, BoxError, SPECIAL_BLOCK_NUM,
    arrow::{array::RecordBatch, datatypes::SchemaRef},
    catalog::physical::{Catalog, PhysicalTable},
    metadata::segments::{BlockRange, Chain, Watermark},
    notification_multiplexer::NotificationMultiplexerHandle,
    plan_visitors::{order_by_block_num, propagate_block_num},
    query_context::{NozzleSessionConfig, QueryContext, parse_sql},
};
use datafusion::{
    common::cast::as_fixed_size_binary_array, error::DataFusionError,
    execution::SendableRecordBatchStream, logical_expr::LogicalPlan,
    physical_plan::stream::RecordBatchStreamAdapter,
};
use dataset_store::{DatasetStore, resolve_blocks_table};
use futures::{
    FutureExt, Stream, TryStreamExt as _,
    stream::{self, StreamExt},
};
use metadata_db::LocationId;
use tokio::sync::{mpsc, watch};
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::task::AbortOnDropHandle;
use tracing::{Instrument, instrument};

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
/// Receiving `Completed(n)` indicates that the query has emitted all outputs up to block number `n`.
///
/// Completion points do not necessarily follow increments of 1, as the query progresses in batches.
pub enum QueryMessage {
    Data(RecordBatch),
    Completed(BlockRange),
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
    dataset_store: Arc<DatasetStore>,
    plan: LogicalPlan,
    start_block: BlockNum,
    end_block: Option<BlockNum>,
    table_updates: TableUpdates,
    tx: mpsc::Sender<QueryMessage>,
    microbatch_max_interval: u64,
    preserve_block_num: bool,
    /// Physical tables used for SQL execution.
    tables: Vec<Arc<PhysicalTable>>,
    network: String,
    /// `blocks` table for the network associated with the physical tables.
    blocks_table: String,
    /// Previously processed range. These may be provided by the consumer to resume a stream.
    prev_range: Option<BlockRange>,
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

        let tables: Vec<Arc<PhysicalTable>> = ctx.catalog().tables().iter().cloned().collect();
        let network = tables.iter().map(|t| t.network()).next().unwrap();
        let src_datasets = tables.iter().map(|t| t.dataset().name.as_str()).collect();
        let blocks_table = resolve_blocks_table(&dataset_store, &src_datasets, network).await?;
        let table_updates = TableUpdates::new(&ctx.catalog(), multiplexer_handle).await;
        let streaming_query = Self {
            ctx,
            dataset_store,
            plan,
            tx,
            start_block,
            end_block,
            table_updates,
            microbatch_max_interval,
            preserve_block_num,
            network: network.to_string(),
            tables,
            blocks_table,
            // TODO: Set from client to resume a stream after dropping a connection.
            prev_range: None,
        };

        let join_handle =
            AbortOnDropHandle::new(tokio::spawn(streaming_query.execute().in_current_span()));

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
            self.table_updates.changed().await;

            // Get the next execution range
            let Some(range) = self.next_microbatch_range().await? else {
                continue;
            };
            tracing::debug!("execute range [{}-{}]", range.start(), range.end());

            // Start microbatch execution for this chunk
            let mut stream = self
                .ctx
                .execute_plan_for_range(
                    self.plan.clone(),
                    range.start(),
                    range.end(),
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
            let _ = self.tx.send(QueryMessage::Completed(range.clone())).await;

            if Some(range.end()) == self.end_block {
                // If we reached the end block, we are done
                return Ok(());
            }
            self.prev_range = Some(range);
        }
    }

    async fn next_microbatch_range(&mut self) -> Result<Option<BlockRange>, BoxError> {
        // Load the canonical chains for each source table.
        let chains = {
            let mut chains: Vec<Chain> = Default::default();
            for table in &self.tables {
                let Some(chain) = table.canonical_chain().await? else {
                    // No canonical chain available for source table.
                    return Ok(None);
                };
                chains.push(chain);
            }
            chains
        };

        // Use a single context for all queries against the blocks table. This is to keep a
        // consistent reference chain within the scope of this function.
        let ctx = {
            let query = parse_sql(&format!(
                "SELECT block_num, hash FROM {}",
                self.blocks_table,
            ))?;
            self.dataset_store
                .ctx_for_sql(&query, self.ctx.env.clone())
                .await?
        };

        // The latest common watermark across the source tables.
        let Some(common_watermark) = self.latest_src_watermark(&ctx, &chains).await? else {
            // No common watermark across source tables.
            return Ok(None);
        };

        if common_watermark.number < self.start_block {
            // Common watermark hasn't reached the requested start block yet.
            return Ok(None);
        }

        let Some(start) = self.next_microbatch_start(&ctx).await? else {
            return Ok(None);
        };
        let Some(end) = self
            .next_microbatch_end(&ctx, &start, common_watermark)
            .await?
        else {
            return Ok(None);
        };
        Ok(Some(BlockRange {
            numbers: start.number..=end.number,
            network: self.network.clone(),
            hash: end.hash,
            prev_hash: start.prev_hash,
        }))
    }

    async fn next_microbatch_start(
        &self,
        ctx: &QueryContext,
    ) -> Result<Option<BlockRow>, BoxError> {
        match &self.prev_range {
            // start stream
            None => self.blocks_table_fetch(&ctx, self.start_block, None).await,
            // continue stream
            Some(prev) if self.blocks_table_contains(ctx, &prev.watermark()).await? => {
                self.blocks_table_fetch(&ctx, prev.end() + 1, None).await
            }
            // rewind stream due to reorg
            Some(prev) => self.reorg_base(ctx, prev).await,
        }
    }

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
            self.blocks_table_fetch(&ctx, number, None)
                .await
                .map(|r| r.map(|r| r.watermark()))
        }
    }

    async fn latest_src_watermark(
        &self,
        ctx: &QueryContext,
        chains: &[Chain],
    ) -> Result<Option<Watermark>, BoxError> {
        // For each chain, collect the latest segment
        let mut latest_src_watermarks: Vec<Watermark> = Default::default();
        'chain_loop: for chain in chains {
            for segment in chain.0.iter().rev() {
                let watermark = segment.range.watermark();
                if self.blocks_table_contains(&ctx, &watermark).await? {
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

    async fn reorg_base(
        &self,
        ctx: &QueryContext,
        prev_range: &BlockRange,
    ) -> Result<Option<BlockRow>, BoxError> {
        // context for querying forked blocks
        let fork_ctx = {
            let mut settings = NozzleSessionConfig::default();
            settings.ignore_canonical_segments = true;
            ctx.with_custom_settings(settings)
        };

        let mut min_fork_block_num = prev_range.end();
        let mut fork: Option<BlockRow> = self
            .blocks_table_fetch(&fork_ctx, prev_range.end(), Some(&prev_range.hash))
            .await?;
        while let Some(block) = fork.take() {
            assert!(block.number <= min_fork_block_num);
            min_fork_block_num = block.number;

            if self.blocks_table_contains(ctx, &block.watermark()).await? {
                break;
            }
            fork = self
                .blocks_table_fetch(
                    &fork_ctx,
                    block.number.saturating_sub(1),
                    block.prev_hash.as_ref(),
                )
                .await?;
        }

        self.blocks_table_fetch(ctx, min_fork_block_num, None).await
    }

    async fn blocks_table_contains(
        &self,
        ctx: &QueryContext,
        watermark: &Watermark,
    ) -> Result<bool, BoxError> {
        self.blocks_table_fetch(ctx, watermark.number, Some(&watermark.hash))
            .await
            .map(|row| row.is_some())
    }

    async fn blocks_table_fetch(
        &self,
        ctx: &QueryContext,
        number: BlockNum,
        hash: Option<&BlockHash>,
    ) -> Result<Option<BlockRow>, BoxError> {
        let hash_constraint = hash
            .map(|h| format!("AND hash = x'{}'", h.encode_hex()))
            .unwrap_or_default();
        let query = parse_sql(&format!(
            "SELECT hash, parent_hash FROM {} WHERE block_num = {} {} LIMIT 1",
            self.blocks_table, number, hash_constraint,
        ))?;
        let plan = ctx.plan_sql(query).await?;
        let results = ctx.execute_and_concat(plan).await?;
        if results.num_rows() == 0 {
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
