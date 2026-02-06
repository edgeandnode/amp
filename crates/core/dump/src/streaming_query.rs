pub mod message_stream_with_block_complete;

use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    sync::Arc,
    time::Duration,
};

use alloy::{hex::ToHexExt as _, primitives::BlockHash};
use amp_data_store::DataStore;
use amp_dataset_store::{DatasetStore, ResolveRevisionError};
use common::{
    BlockNum, BlockRange, DetachedLogicalPlan, LogicalCatalog, PlanningContext, QueryContext,
    ResumeWatermark, Watermark,
    arrow::{array::RecordBatch, datatypes::SchemaRef},
    catalog::{
        logical::LogicalTable,
        physical::{CanonicalChainError, Catalog, PhysicalTable},
    },
    incrementalizer::incrementalize_plan,
    metadata::segments::{Segment, WatermarkNotFoundError},
    plan_visitors::{order_by_block_num, unproject_special_block_num_column},
    query_context::QueryEnv,
    sql_str::SqlStr,
};
use datafusion::{common::cast::as_fixed_size_binary_array, error::DataFusionError};
use datasets_common::{
    block_num::RESERVED_BLOCK_NUM_COLUMN_NAME, dataset::Dataset, hash_reference::HashReference,
    network_id::NetworkId,
};
use datasets_derived::dataset::Dataset as DerivedDataset;
use futures::stream::{self, BoxStream, StreamExt};
use message_stream_with_block_complete::MessageStreamWithBlockComplete;
use metadata_db::{LocationId, NotificationMultiplexerHandle};
use tokio::{
    sync::{mpsc, watch},
    task::JoinError,
    time::MissedTickBehavior,
};
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::task::AbortOnDropHandle;
use tracing::{Instrument, instrument};

use crate::streaming_query::message_stream_with_block_complete::MessageStreamError;

/// Errors that occur when spawning a streaming query
///
/// Streaming queries execute SQL continuously over blockchain data, processing
/// new blocks as they arrive. This error type covers all initialization phases:
/// query planning, network validation, and blocks table resolution.
#[derive(thiserror::Error, Debug)]
pub enum SpawnError {
    /// Failed to propagate `_block_num` column through query plan
    ///
    /// The `_block_num` column is a special column added to all tables to enable
    /// incremental query processing. This error occurs when the logical plan
    /// transformation to propagate this column through the query fails.
    ///
    /// Common causes:
    /// - Invalid query structure that cannot support block number propagation
    /// - Incompatible aggregations or window functions
    /// - Schema conflicts when adding the special column
    /// - DataFusion internal errors during plan transformation
    ///
    /// This prevents the streaming query from being initialized as incremental
    /// processing requires the block number column.
    #[error("failed to propagate _block_num column in query plan")]
    PropagateBlockNum(#[source] DataFusionError),

    /// Failed to optimize query plan
    ///
    /// This occurs when DataFusion's logical optimizer fails to process the query
    /// plan during initialization. The optimizer applies transformations like
    /// predicate pushdown, projection pruning, and constant folding.
    ///
    /// Common causes:
    /// - Invalid logical plan structure
    /// - Optimizer rule failures
    /// - Type inference errors
    /// - Schema inconsistencies
    ///
    /// Optimization failures prevent the streaming query from starting with an
    /// efficient execution plan.
    #[error("failed to optimize query plan")]
    OptimizePlan(#[source] common::query_context::Error),

    /// Query references tables from multiple blockchain networks
    ///
    /// Streaming queries must operate on a single blockchain network. This error
    /// occurs when the catalog contains tables from different networks (e.g., both
    /// Ethereum mainnet and Polygon).
    ///
    /// Common causes:
    /// - User query joins tables across different networks
    /// - Catalog construction error including wrong network tables
    /// - Dataset dependency resolution pulling in multi-network data
    ///
    /// The networks field lists all detected networks in the query.
    ///
    /// Streaming queries require a single chain for consistent block ordering
    /// and watermark tracking.
    #[error("multi-network streaming queries are not supported: {networks:?}")]
    MultiNetwork { networks: Vec<NetworkId> },

    /// Failed to resolve blocks table for network
    ///
    /// Every streaming query requires access to a `blocks` table containing the
    /// canonical blockchain data for the network. This error occurs when finding
    /// or loading the blocks table fails.
    ///
    /// Common causes:
    /// - No raw dataset with blocks table exists for the network
    /// - Dataset dependency tree doesn't include a blocks table
    /// - Blocks table exists but hasn't been synced (no active revision)
    /// - Data store errors when loading table metadata
    /// - Dataset manifest not found or corrupted
    ///
    /// Without a blocks table, the streaming query cannot determine block ranges
    /// or detect chain reorganizations.
    #[error("failed to resolve blocks table for network")]
    ResolveBlocksTable(#[source] ResolveBlocksTableError),

    /// Failed to convert resume watermark to target network
    ///
    /// When resuming a streaming query from a previous watermark, the watermark
    /// must be converted to the target network's format. This error occurs when
    /// the resume watermark doesn't contain an entry for the expected network.
    ///
    /// Common causes:
    /// - Resume watermark from a different network than current query
    /// - Corrupted or invalid resume watermark state
    /// - Network name mismatch (e.g., "ethereum" vs "mainnet")
    /// - Resume state out of sync with current catalog
    ///
    /// This prevents the query from resuming at the correct position and may
    /// require starting from scratch or using a different resume point.
    #[error("failed to convert resume watermark")]
    ConvertResumeWatermark(#[source] WatermarkNotFoundError),
}

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

struct SegmentStart {
    number: BlockNum,
    prev_hash: BlockHash,
}

impl From<BlockRow> for SegmentStart {
    fn from(row: BlockRow) -> Self {
        Self {
            number: row.number,
            prev_hash: row.prev_hash,
        }
    }
}

/// Direction of the stream. Helpful to distinguish reorgs, with the payload being the first block
/// after the base of the fork.
enum StreamDirection {
    ForwardFrom(SegmentStart),
    ReorgFrom(SegmentStart),
}

impl StreamDirection {
    fn segment_start(&self) -> &SegmentStart {
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
    join_handle: AbortOnDropHandle<Result<(), StreamingQueryExecutionError>>,
}

impl StreamingQueryHandle {
    pub fn into_stream(self) -> BoxStream<'static, Result<QueryMessage, MessageStreamError>> {
        let data_stream = MessageStreamWithBlockComplete::new(ReceiverStream::new(self.rx).map(Ok));

        let join = self.join_handle;

        // If `tx` has been dropped then the query task has terminated. So we check if it has
        // terminated with errors, and if so send the error as the final item of the stream.
        let get_task_result = async move {
            match tokio::time::timeout(Duration::from_secs(1), join).await {
                Ok(Ok(Ok(()))) => None,
                Ok(Ok(Err(e))) => Some(Err(e.into())),
                Ok(Err(join_err)) => Some(Err(
                    StreamingQueryExecutionError::StreamingTaskFailedToJoin(join_err).into(),
                )),

                // This would only happen under extreme CPU or tokio scheduler contention.
                // Or blocking `Drop` implementations.
                Err(_) => Some(Err(StreamingQueryExecutionError::TaskTimeout.into())),
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
    data_store: DataStore,
    catalog: Catalog,
    plan: DetachedLogicalPlan,
    start_block: BlockNum,
    end_block: Option<BlockNum>,
    table_updates: TableUpdates,
    tx: mpsc::Sender<QueryMessage>,
    microbatch_max_interval: u64,
    keep_alive_interval: u64,
    destination: Option<Arc<PhysicalTable>>,
    preserve_block_num: bool,
    network: NetworkId,
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
        dataset_store: &DatasetStore,
        data_store: DataStore,
        plan: DetachedLogicalPlan,
        start_block: BlockNum,
        end_block: Option<BlockNum>,
        resume_watermark: Option<ResumeWatermark>,
        multiplexer_handle: &NotificationMultiplexerHandle,
        destination: Option<Arc<PhysicalTable>>,
        microbatch_max_interval: u64,
        keep_alive_interval: u64,
    ) -> Result<StreamingQueryHandle, SpawnError> {
        let (tx, rx) = mpsc::channel(10);

        // Preserve `_block_num` for SQL materializaiton or if explicitly selected in the schema.
        let preserve_block_num = destination.is_some()
            || plan
                .schema()
                .fields()
                .iter()
                .any(|f| f.name() == RESERVED_BLOCK_NUM_COLUMN_NAME);

        // This plan is the starting point of each microbatch execution. Transformations applied to it:œ
        // - Propagate the `_block_num` column.
        // - Run logical optimizations ahead of execution.
        let plan = {
            let plan = plan
                .propagate_block_num()
                .map_err(SpawnError::PropagateBlockNum)?;

            let ctx = PlanningContext::new(catalog.logical().clone());
            ctx.optimize_plan(&plan)
                .await
                .map_err(SpawnError::OptimizePlan)?
        };

        let tables: Vec<Arc<PhysicalTable>> = catalog.tables().to_vec();

        let networks: BTreeSet<&NetworkId> = tables.iter().map(|t| t.network()).collect();
        if networks.len() != 1 {
            let networks: Vec<NetworkId> = networks.into_iter().cloned().collect();
            return Err(SpawnError::MultiNetwork { networks });
        }
        let network = networks.into_iter().next().unwrap();

        let unique_refs: BTreeSet<&HashReference> =
            tables.iter().map(|t| t.dataset_reference()).collect();

        let blocks_table = resolve_blocks_table(
            dataset_store,
            data_store.clone(),
            unique_refs.into_iter(),
            network,
        )
        .await
        .map_err(SpawnError::ResolveBlocksTable)?;

        let table_updates = TableUpdates::new(&catalog, multiplexer_handle).await;
        let prev_watermark = resume_watermark
            .map(|w| w.to_watermark(network))
            .transpose()
            .map_err(SpawnError::ConvertResumeWatermark)?;
        let streaming_query = Self {
            query_env,
            data_store,
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
            network: network.clone(),
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
    async fn execute(mut self) -> Result<(), StreamingQueryExecutionError> {
        loop {
            self.table_updates.changed().await;

            // The table snapshots to execute the microbatch against.
            let ctx = QueryContext::for_catalog(
                self.catalog.clone(),
                self.query_env.clone(),
                self.data_store.clone(),
                false,
            )
            .await
            .map_err(StreamingQueryExecutionError::QueryContext)?;

            // Get the next execution range
            let Some(MicrobatchRange { range, direction }) = self
                .next_microbatch_range(&ctx)
                .await
                .map_err(StreamingQueryExecutionError::NextMicrobatchRange)?
            else {
                continue;
            };

            tracing::debug!("execute range [{}-{}]", range.start(), range.end());

            let plan = {
                // Incrementalize the plan
                let plan = self
                    .plan
                    .clone()
                    .attach_to(&ctx)
                    .map_err(StreamingQueryExecutionError::AttachToPlan)?;
                let mut plan = incrementalize_plan(plan, range.start(), range.end())
                    .map_err(StreamingQueryExecutionError::IncrementalizePlan)?;

                // Enforce `order by _block_num`.
                plan = order_by_block_num(plan);

                // Remove `_block_num` if not needed in the output.
                if !self.preserve_block_num {
                    plan = unproject_special_block_num_column(plan)
                        .map_err(StreamingQueryExecutionError::UnprojectSpecialBlockNumColumn)?
                }
                plan
            };

            let keep_alive_interval = self.keep_alive_interval.max(30);
            let schema = Arc::new(plan.schema().as_arrow().clone());
            let mut stream = keep_alive_stream(
                ctx.execute_plan(plan, false)
                    .await
                    .map_err(StreamingQueryExecutionError::ExecutePlan)?,
                schema,
                keep_alive_interval,
            );

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
                let item = item.map_err(StreamingQueryExecutionError::StreamItem)?;

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
            self.prev_watermark = Some((&range).into());
        }
    }

    #[instrument(skip_all, err)]
    async fn next_microbatch_range(
        &mut self,
        ctx: &QueryContext,
    ) -> Result<Option<MicrobatchRange>, NextMicrobatchRangeError> {
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
                let table = &self.blocks_table;
                let resolved_table = LogicalTable::new(
                    table.sql_table_ref_schema().to_string(),
                    table.dataset_reference().clone(),
                    table.table().clone(),
                );
                let logical = LogicalCatalog::from_tables(std::iter::once(&resolved_table));
                Catalog::new(vec![self.blocks_table.clone()], logical)
            };
            QueryContext::for_catalog(
                catalog,
                self.query_env.clone(),
                self.data_store.clone(),
                false,
            )
            .await
            .map_err(NextMicrobatchRangeError::QueryContext)?
        };

        // The latest common watermark across the source tables.
        let Some(common_watermark) = self
            .latest_src_watermark(&blocks_ctx, chains)
            .await
            .map_err(NextMicrobatchRangeError::LatestSrcWatermark)?
        else {
            // No common watermark across source tables.
            tracing::debug!("no common watermark found");
            return Ok(None);
        };

        if common_watermark.number < self.start_block {
            // Common watermark hasn't reached the requested start block yet.
            return Ok(None);
        }

        let Some(direction) = self
            .next_microbatch_start(&blocks_ctx)
            .await
            .map_err(NextMicrobatchRangeError::NextMicrobatchStart)?
        else {
            tracing::debug!("no next microbatch start found");
            return Ok(None);
        };
        let start = direction.segment_start();
        let Some(end) = self
            .next_microbatch_end(&blocks_ctx, start, common_watermark)
            .await
            .map_err(NextMicrobatchRangeError::NextMicrobatchEnd)?
        else {
            tracing::debug!("no next microbatch end found");
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
    ) -> Result<Option<StreamDirection>, NextMicrobatchStartError> {
        match &self.prev_watermark {
            // start stream
            None => {
                let block = self
                    .blocks_table_fetch(ctx, self.start_block, None)
                    .await
                    .map_err(NextMicrobatchStartError::BlocksTableFetch)?;
                Ok(block.map(|b| StreamDirection::ForwardFrom(b.into())))
            }
            // continue stream
            Some(prev)
                if self
                    .blocks_table_contains(ctx, prev)
                    .await
                    .map_err(NextMicrobatchStartError::BlocksTableContains)? =>
            {
                let segment_start = SegmentStart {
                    number: prev.number + 1,
                    prev_hash: prev.hash,
                };
                Ok(Some(StreamDirection::ForwardFrom(segment_start)))
            }
            // rewind stream due to reorg
            Some(prev) => {
                let block = self
                    .reorg_base(ctx, prev)
                    .await
                    .map_err(NextMicrobatchStartError::ReorgBase)?;
                Ok(block.map(|b| StreamDirection::ReorgFrom(b.into())))
            }
        }
    }

    #[instrument(skip_all, err)]
    async fn next_microbatch_end(
        &mut self,
        ctx: &QueryContext,
        start: &SegmentStart,
        common_watermark: Watermark,
    ) -> Result<Option<Watermark>, NextMicrobatchEndError> {
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
                .map_err(NextMicrobatchEndError)
        }
    }

    #[instrument(skip_all, err)]
    async fn latest_src_watermark(
        &self,
        ctx: &QueryContext,
        chains: impl Iterator<Item = &[Segment]>,
    ) -> Result<Option<Watermark>, LatestSrcWatermarkError> {
        // For each chain, collect the latest segment
        let mut latest_src_watermarks: Vec<Watermark> = Default::default();
        'chain_loop: for chain in chains {
            for segment in chain.iter().rev() {
                let watermark = (&segment.range).into();
                if self
                    .blocks_table_contains(ctx, &watermark)
                    .await
                    .map_err(LatestSrcWatermarkError)?
                {
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
    ) -> Result<Option<BlockRow>, ReorgBaseError> {
        // context for querying forked blocks
        let fork_ctx = {
            let catalog = Catalog::new(
                ctx.catalog().physical_tables().cloned().collect(),
                ctx.catalog().logical().clone(),
            );
            QueryContext::for_catalog(catalog, ctx.env.clone(), self.data_store.clone(), true)
                .await
                .map_err(ReorgBaseError::QueryContext)?
        };

        let mut min_fork_block_num = prev_watermark.number;
        let mut fork: Option<BlockRow> = self
            .blocks_table_fetch(&fork_ctx, prev_watermark.number, Some(&prev_watermark.hash))
            .await
            .map_err(ReorgBaseError::BlocksTableFetch)?;
        while let Some(block) = fork.take() {
            if self
                .blocks_table_contains(ctx, &block.watermark())
                .await
                .map_err(ReorgBaseError::BlocksTableContains)?
            {
                break;
            }
            min_fork_block_num = block.number;
            fork = self
                .blocks_table_fetch(
                    &fork_ctx,
                    block.number.saturating_sub(1),
                    Some(&block.prev_hash),
                )
                .await
                .map_err(ReorgBaseError::BlocksTableFetch)?;
        }

        // If we're dumping a derived dataset, we must rewind to the start of the canonical segment
        // boudary. Otherwise, the new segments may not form a canonical chain.
        if let Some(destination) = self.destination.as_ref()
            && let Some(destination_chain) = destination
                .canonical_chain()
                .await
                .map_err(ReorgBaseError::CanonicalChain)?
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

        self.blocks_table_fetch(ctx, min_fork_block_num, None)
            .await
            .map_err(ReorgBaseError::BlocksTableFetch)
    }

    #[instrument(skip_all, err)]
    async fn blocks_table_contains(
        &self,
        ctx: &QueryContext,
        watermark: &Watermark,
    ) -> Result<bool, BlocksTableContainsError> {
        // Panic safety: The `blocks_ctx` always has a single table.
        let blocks_segments = &ctx.catalog().table_snapshots()[0];

        // Optimization: Check segment metadata first to avoid expensive query,
        // Walk segments in reverse to find one that covers this block number.
        for segment in blocks_segments.canonical_segments().iter().rev() {
            if *segment.range.numbers.start() <= watermark.number {
                // Found segment that could contain this block
                if *segment.range.numbers.end() == watermark.number {
                    // Exact match on segment end - use segment hash directly
                    return Ok(segment.range.hash == watermark.hash);
                }
                // Block is inside segment but not at end.
                // So we will need to query the data file to find the hash.
                break;
            }
        }

        // Fallback to database query
        self.blocks_table_fetch(ctx, watermark.number, Some(&watermark.hash))
            .await
            .map(|row| row.is_some())
            .map_err(BlocksTableContainsError)
    }

    #[instrument(skip(self, ctx), err)]
    async fn blocks_table_fetch(
        &self,
        ctx: &QueryContext,
        number: BlockNum,
        hash: Option<&BlockHash>,
    ) -> Result<Option<BlockRow>, BlocksTableFetchError> {
        let hash_constraint = hash
            .map(|h| format!("AND hash = x'{}'", h.encode_hex()))
            .unwrap_or_default();
        let sql = format!(
            "SELECT hash, parent_hash FROM {} WHERE block_num = {} {} LIMIT 1",
            self.blocks_table.table_ref().to_quoted_string(),
            number,
            hash_constraint,
        );

        // SAFETY: Validation is deferred to the SQL parser which will return appropriate errors
        // for empty or invalid SQL. The format! macro ensures non-empty output.
        let sql_str = SqlStr::new_unchecked(sql);
        let query = common::sql::parse(&sql_str).map_err(BlocksTableFetchError::ParseSql)?;
        let plan = ctx
            .plan_sql(query)
            .await
            .map_err(BlocksTableFetchError::PlanSql)?;
        let results = ctx
            .execute_and_concat(plan)
            .await
            .map_err(BlocksTableFetchError::ExecuteSql)?;
        if results.num_rows() == 0 {
            tracing::debug!("blocks table missing block {} {:?}", number, hash);
            return Ok(None);
        }
        let get_hash_value = |column_name: &str| -> Result<BlockHash, GetHashValueError> {
            let column = results
                .column_by_name(column_name)
                .ok_or_else(|| GetHashValueError::MissingColumn(column_name.to_string()))?;
            let column = as_fixed_size_binary_array(column).map_err(GetHashValueError::Downcast)?;
            column
                .iter()
                .flatten()
                .next()
                .and_then(|b| BlockHash::try_from(b).ok())
                .ok_or_else(|| GetHashValueError::MissingBlockHashValue(column_name.to_string()))
        };
        Ok(Some(BlockRow {
            number,
            hash: get_hash_value("hash").map_err(BlocksTableFetchError::GetHashValue)?,
            prev_hash: get_hash_value("parent_hash")
                .map_err(BlocksTableFetchError::GetHashValue)?,
        }))
    }
}

/// Errors that occur during streaming query execution
///
/// This error type is used by `StreamingQuery::execute()`.
#[derive(Debug, thiserror::Error)]
pub enum StreamingQueryExecutionError {
    /// Streaming task failed to join
    ///
    /// This occurs when the streaming query task panics or is cancelled unexpectedly.
    /// The JoinError contains information about why the task failed to complete.
    #[error("streaming task failed to join: {0}")]
    StreamingTaskFailedToJoin(#[source] JoinError),

    /// Streaming task join timed out
    ///
    /// This occurs when the streaming query task does not complete within the expected
    /// timeout period (1 second) after being signaled to stop. This indicates extreme
    /// CPU or tokio scheduler contention, or blocking `Drop` implementations.
    #[error("streaming task join timed out")]
    TaskTimeout,

    /// Failed to create a query context
    ///
    /// This occurs when the query context cannot be created.
    #[error("failed to create query context: {0}")]
    QueryContext(#[source] common::query_context::Error),

    /// Failed to get the next microbatch range
    ///
    /// This occurs when the next microbatch range cannot be found.
    #[error("failed to get next microbatch range: {0}")]
    NextMicrobatchRange(#[source] NextMicrobatchRangeError),

    /// Failed to attach the plan to the query context
    ///
    /// This occurs when the plan cannot be attached to the query context.
    #[error("failed to attach the plan to the query context: {0}")]
    AttachToPlan(#[source] common::query_context::Error),

    /// Failed to incrementalize the plan
    ///
    /// This occurs when the plan cannot be incrementalized.
    #[error("failed to incrementalize the plan: {0}")]
    IncrementalizePlan(#[source] DataFusionError),

    /// Failed to unproject the special block num column
    ///
    /// This occurs when the special block num column cannot be unprojected.
    #[error("failed to unproject the special block num column: {0}")]
    UnprojectSpecialBlockNumColumn(#[source] DataFusionError),

    /// Failed to execute the plan
    ///
    /// This occurs when the plan cannot be executed.
    #[error("failed to execute the plan: {0}")]
    ExecutePlan(#[source] common::query_context::Error),

    /// Failed to stream item
    ///
    /// This occurs when the item cannot be streamed.
    #[error("failed to stream item: {0}")]
    StreamItem(#[source] DataFusionError),
}

/// Errors that occur when determining the next microbatch range
///
/// This error type is used by `StreamingQuery::next_microbatch_range()`.
#[derive(Debug, thiserror::Error)]
pub enum NextMicrobatchRangeError {
    /// Failed to create a query context
    ///
    /// This occurs when the query context cannot be created.
    #[error("failed to create query context: {0}")]
    QueryContext(#[source] common::query_context::Error),

    /// Failed to get the latest source watermark
    ///
    /// This occurs when the latest source watermark cannot be found.
    #[error("failed to get latest source watermark: {0}")]
    LatestSrcWatermark(#[source] LatestSrcWatermarkError),

    /// Failed to get the next microbatch start
    ///
    /// This occurs when the next microbatch start cannot be found.
    #[error("failed to get next microbatch start: {0}")]
    NextMicrobatchStart(#[source] NextMicrobatchStartError),

    /// Failed to get the next microbatch end
    ///
    /// This occurs when the next microbatch end cannot be found.
    #[error("failed to get next microbatch end: {0}")]
    NextMicrobatchEnd(#[source] NextMicrobatchEndError),
}

/// Errors that occur when determining the next microbatch start position
///
/// This error type is used by `StreamingQuery::next_microbatch_start()`.
#[derive(Debug, thiserror::Error)]
pub enum NextMicrobatchStartError {
    /// Failed to fetch the blocks table
    ///
    /// This occurs when the blocks table cannot be fetched.
    #[error("failed to fetch the blocks table: {0}")]
    BlocksTableFetch(#[source] BlocksTableFetchError),

    /// Failed to check if the blocks table contains the watermark
    ///
    /// This occurs when the blocks table cannot be checked if it contains the watermark.
    #[error("failed to check if the blocks table contains the watermark: {0}")]
    BlocksTableContains(#[source] BlocksTableContainsError),

    /// Failed to get the reorg base
    ///
    /// This occurs when the reorg base cannot be found.
    #[error("failed to get the reorg base: {0}")]
    ReorgBase(#[source] ReorgBaseError),
}

/// Failed to fetch the blocks table
///
/// This occurs when the blocks table cannot be fetched.
#[derive(Debug, thiserror::Error)]
#[error("failed to fetch the blocks table: {0}")]
pub struct NextMicrobatchEndError(#[source] BlocksTableFetchError);

/// Failed to get the latest source watermark
///
/// This error is returned by `latest_src_watermark()` when checking if blocks
/// are present in the blocks table fails.
#[derive(Debug, thiserror::Error)]
#[error("failed to get latest source watermark: {0}")]
pub struct LatestSrcWatermarkError(#[source] BlocksTableContainsError);

/// Errors that occur when finding the reorg base block
///
/// This error type is used by `StreamingQuery::reorg_base()`.
#[derive(Debug, thiserror::Error)]
pub enum ReorgBaseError {
    /// Failed to create a query context
    ///
    /// This occurs when the query context cannot be created.
    #[error("failed to create query context: {0}")]
    QueryContext(#[source] common::query_context::Error),

    /// Failed to fetch the blocks table
    ///
    /// This occurs when the blocks table cannot be fetched.
    #[error("failed to fetch the blocks table: {0}")]
    BlocksTableFetch(#[source] BlocksTableFetchError),

    /// Failed to check if the blocks table contains the watermark
    ///
    /// This occurs when the blocks table cannot be checked if it contains the watermark.
    #[error("failed to check if the blocks table contains the watermark: {0}")]
    BlocksTableContains(#[source] BlocksTableContainsError),

    /// Failed to get the canonical chain
    ///
    /// This occurs when the canonical chain cannot be found.
    #[error("failed to get the canonical chain: {0}")]
    CanonicalChain(#[source] CanonicalChainError),
}

/// Failed to check if the blocks table contains a watermark
///
/// This error type is used by `StreamingQuery::blocks_table_contains()`.
#[derive(Debug, thiserror::Error)]
#[error("failed to fetch the blocks table: {0}")]
pub struct BlocksTableContainsError(#[source] BlocksTableFetchError);

/// Errors that occur when fetching block data from the blocks table
///
/// This error type is used by `StreamingQuery::blocks_table_fetch()`.
#[derive(Debug, thiserror::Error)]
pub enum BlocksTableFetchError {
    /// Failed to parse the SQL
    ///
    /// This occurs when the SQL cannot be parsed.
    #[error("failed to parse the SQL: {0}")]
    ParseSql(#[source] common::sql::ParseSqlError),

    /// Failed to plan the SQL
    ///
    /// This occurs when the SQL cannot be planned.
    #[error("failed to plan the SQL: {0}")]
    PlanSql(#[source] common::query_context::Error),

    /// Failed to execute the SQL
    ///
    /// This occurs when the SQL cannot be executed.
    #[error("failed to execute the SQL: {0}")]
    ExecuteSql(#[source] common::query_context::Error),

    /// Failed to get the hash value
    ///
    /// This occurs when the hash value cannot be found.
    #[error("failed to get the hash value: {0}")]
    GetHashValue(#[source] GetHashValueError),
}

/// Errors that occur when extracting hash values from block query results
///
/// This error type is used internally by `blocks_table_fetch()` when extracting
/// block hash and parent hash values from query results.
#[derive(Debug, thiserror::Error)]
pub enum GetHashValueError {
    /// Blocks table missing expected column
    ///
    /// This occurs when the blocks table does not contain the expected column
    /// (e.g., 'hash' or 'parent_hash').
    #[error("blocks table missing column: {0}")]
    MissingColumn(String),

    /// Failed to downcast the column
    ///
    /// This occurs when the column cannot be downcast.
    #[error("failed to downcast the column: {0}")]
    Downcast(#[source] DataFusionError),

    /// Blocks table missing block hash value
    ///
    /// This occurs when the blocks table column exists but does not contain
    /// a valid block hash value (either null or not convertible to BlockHash).
    #[error("blocks table missing block hash value for column {0}")]
    MissingBlockHashValue(String),
}

struct BlockRow {
    number: BlockNum,
    hash: BlockHash,
    prev_hash: BlockHash,
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
    let period = Duration::from_secs(keep_alive_interval);
    let mut keep_alive_interval = tokio::time::interval(period);

    let missed_tick_behavior = MissedTickBehavior::Delay;
    keep_alive_interval.set_missed_tick_behavior(missed_tick_behavior);

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

/// Return a table identifier, in the form `{dataset}.blocks`, for the given network.
async fn resolve_blocks_table(
    dataset_store: &DatasetStore,
    data_store: DataStore,
    root_dataset_refs: impl Iterator<Item = &HashReference>,
    network: &NetworkId,
) -> Result<PhysicalTable, ResolveBlocksTableError> {
    let dataset = search_dependencies_for_raw_dataset(dataset_store, root_dataset_refs, network)
        .await
        .map_err(ResolveBlocksTableError::SearchDependencies)?;

    let table = dataset
        .tables()
        .iter()
        .find(|t| t.name() == "blocks")
        .ok_or_else(|| {
            ResolveBlocksTableError::BlocksTableNotFound(dataset.reference().to_string())
        })?;

    let revision = data_store
        .get_table_active_revision(dataset.reference(), table.name())
        .await
        .map_err(ResolveBlocksTableError::GetActivePhysicalTable)?
        .ok_or_else(|| {
            ResolveBlocksTableError::TableNotSynced(
                dataset.reference().to_string(),
                table.name().to_string(),
            )
        })?;

    let sql_table_ref_schema = dataset.reference().to_reference().to_string();
    Ok(PhysicalTable::from_revision(
        data_store,
        dataset.reference().clone(),
        dataset.start_block(),
        table.clone(),
        revision,
        sql_table_ref_schema,
    ))
}

/// Errors that occur when resolving the blocks table for a network
///
/// This error type is used by `resolve_blocks_table()`.
#[derive(Debug, thiserror::Error)]
pub enum ResolveBlocksTableError {
    /// Failed to search dependencies for raw dataset
    ///
    /// This occurs when searching for a raw dataset matching the target network fails.
    #[error("Failed to search dependencies for raw dataset")]
    SearchDependencies(#[source] SearchDependenciesForRawDatasetError),

    /// Blocks table not found
    ///
    /// This occurs when the blocks table is not found in the dataset.
    #[error("'blocks' table not found in dataset '{0}'")]
    BlocksTableNotFound(String),

    /// Failed to get active physical table revision
    ///
    /// This occurs when querying for an active physical table revision fails.
    #[error("Failed to get active physical table revision")]
    GetActivePhysicalTable(#[source] amp_data_store::GetTableActiveRevisionError),

    /// Table not synced
    ///
    /// This occurs when the table has not been synced.
    #[error("table '{0}.{1}' has not been synced")]
    TableNotSynced(String, String),
}

// Breadth-first search over dataset dependencies to find a raw dataset matching the target network.
async fn search_dependencies_for_raw_dataset(
    dataset_store: &DatasetStore,
    root_dataset_refs: impl Iterator<Item = &HashReference>,
    network: &NetworkId,
) -> Result<Arc<dyn Dataset>, SearchDependenciesForRawDatasetError> {
    let mut queue: VecDeque<Arc<dyn datasets_common::dataset::Dataset>> = VecDeque::new();
    for hash_ref in root_dataset_refs {
        let dataset = dataset_store
            .get_dataset(hash_ref)
            .await
            .map_err(SearchDependenciesForRawDatasetError::GetDataset)?;
        queue.push_back(dataset);
    }

    let mut visited = BTreeSet::new();
    while let Some(dataset) = queue.pop_front() {
        let dataset_ref = dataset.reference().clone();

        // Skip duplicates
        if !visited.insert(dataset_ref) {
            continue;
        }

        // Check if this is a raw dataset matching the target network
        if !dataset.is::<DerivedDataset>()
            && let Some(table) = dataset.tables().first()
            && table.network() == network
        {
            // Found matching dataset
            return Ok(dataset);
        }

        // Enqueue dependencies for exploration (only derived datasets have dependencies)
        if let Some(derived) = dataset.downcast_ref::<DerivedDataset>() {
            for dep in derived.dependencies().values() {
                // Resolve the reference to a hash reference first
                let hash_ref = dataset_store
                    .resolve_revision(dep.to_reference())
                    .await
                    .map_err(SearchDependenciesForRawDatasetError::ResolveRevision)?
                    .ok_or_else(|| {
                        SearchDependenciesForRawDatasetError::NotFound(
                            dep.to_reference().to_string(),
                        )
                    })?;
                let dataset = dataset_store
                    .get_dataset(&hash_ref)
                    .await
                    .map_err(SearchDependenciesForRawDatasetError::GetDataset)?;

                queue.push_back(dataset);
            }
        }
    }

    Err(SearchDependenciesForRawDatasetError::NoRawDatasetFound {
        network: network.clone(),
    })
}

/// Errors that occur when searching dataset dependencies for a raw dataset
///
/// This error type is used by `search_dependencies_for_raw_dataset()`.
#[derive(Debug, thiserror::Error)]
pub enum SearchDependenciesForRawDatasetError {
    /// Failed to get dataset from dataset store
    ///
    /// This occurs when retrieving the dataset instance from the dataset store fails.
    /// The dataset store loads dataset manifests and parses them into Dataset instances.
    #[error("Failed to get dataset")]
    GetDataset(#[source] amp_dataset_store::GetDatasetError),

    /// Failed to resolve revision
    ///
    /// This occurs when resolving a dataset revision fails.
    #[error("Failed to resolve revision")]
    ResolveRevision(#[source] ResolveRevisionError),

    /// Failed to find dependency
    ///
    /// This occurs when a dependency is not found.
    #[error("dependency '{0}' not found")]
    NotFound(String),

    /// No raw dataset found for network
    ///
    /// This occurs when no raw dataset is found for the target network.
    #[error("no raw dataset found for network '{network}'")]
    NoRawDatasetFound { network: NetworkId },
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
