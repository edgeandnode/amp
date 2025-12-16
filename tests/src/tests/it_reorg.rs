use std::{collections::BTreeMap, ops::RangeInclusive, time::Duration};

use alloy::primitives::BlockHash;
use arrow_flight::FlightData;
use common::{
    BlockNum, catalog::sql::catalog_for_sql, metadata::segments::BlockRange, sql, sql_str::SqlStr,
};
use datasets_common::reference::Reference;
use monitoring::logging;
use rand::{Rng, RngCore, SeedableRng as _, rngs::StdRng};
use serde::Deserialize;
use tokio::sync::mpsc;

use crate::testlib::{
    ctx::{TestCtx, TestCtxBuilder},
    fixtures::{BlockInfo, DatasetPackage, FlightClient},
    helpers as test_helpers,
};

#[tokio::test]
async fn rpc_reorg_prop() {
    let test = ReorgTestCtx::setup("rpc_reorg_prop", "anvil_rpc").await;

    let seed = rand::rng().next_u64();
    println!("seed: {seed}");
    let mut rng = StdRng::seed_from_u64(seed);

    for _ in 0..3 {
        test.mine(rng.random_range(1..=3)).await;
        let latest_block_num = test.latest_block().await.block_num;
        test.dump("anvil_rpc", latest_block_num).await;

        let blocks0 = test.query_blocks("anvil_rpc", None).await;
        eprintln!("blocks0 = {:#?}", blocks0);
        check_blocks(&blocks0);
        assert_eq!(
            blocks0.len(),
            test.latest_block().await.block_num as usize + 1
        );

        let reorg_depth = u64::min(rng.random_range(1..=3), test.latest_block().await.block_num);
        test.reorg(reorg_depth).await;
        let latest_after_reorg = test.latest_block().await.block_num;
        test.dump("anvil_rpc", latest_after_reorg).await;

        // no reorg detected, since dumped block height has not increased
        assert_eq!(blocks0, test.query_blocks("anvil_rpc", None).await);

        // mine at least one block to detect reorg
        test.mine(rng.random_range(1..=3)).await;
        let latest_after_mine = test.latest_block().await.block_num;
        test.dump("anvil_rpc", latest_after_mine).await;

        // the canonical chain must be resolved in at most reorg_depth dumps
        for _ in 0..reorg_depth {
            test.mine(rng.random_range(0..=3)).await;
            let latest_in_loop = test.latest_block().await.block_num;
            test.dump("anvil_rpc", latest_in_loop).await;
        }

        let latest = test.latest_block().await;
        eprintln!("latest = {:#?}", latest);
        let blocks1 = test.query_blocks("anvil_rpc", None).await;
        eprintln!("blocks1 = {:#?}", blocks1);
        check_blocks(&blocks1);
        let mut ranges = test.metadata_ranges("anvil_rpc").await;
        ranges.sort_by_key(|r| *r.numbers.start());
        eprintln!("ranges = {:#?}", ranges);
        assert_eq!(blocks1.len(), latest.block_num as usize + 1);
        assert_eq!(
            blocks1.last().expect("blocks1 should not be empty").hash,
            latest.hash
        );
    }
}

#[tokio::test]
async fn anvil_rpc_reorg_with_depth_one_maintains_canonical_chain() {
    let test = ReorgTestCtx::setup(
        "anvil_rpc_reorg_with_depth_one_maintains_canonical_chain",
        "anvil_rpc",
    )
    .await;

    test.mine(2).await;
    test.dump("anvil_rpc", 2).await;
    let blocks0 = test.query_blocks("anvil_rpc", None).await;

    test.reorg(1).await;
    test.mine(1).await;
    test.dump("anvil_rpc", 3).await;
    let blocks1 = test.query_blocks("anvil_rpc", None).await;

    // Check that reorgs are fully handled in the same block in which they are detected
    assert_eq!(blocks0.len(), 3);
    assert_eq!(blocks1.len(), 4);

    let ranges = test.metadata_ranges("anvil_rpc").await;
    assert_eq!(ranges.len(), 3);
    assert_eq!(
        &ranges[0],
        &BlockRange {
            numbers: 0..=2,
            network: "anvil".to_string(),
            hash: blocks0[2].hash,
            prev_hash: Some(blocks0[0].parent_hash),
        }
    );
    assert_eq!(ranges[1].numbers, 3..=3);
    assert_eq!(&ranges[1].network, "anvil");
    assert_ne!(&ranges[1].prev_hash, &Some(ranges[0].hash));
}

#[tokio::test]
async fn dump_finalized() {
    let test = ReorgTestCtx::setup("dump_finalized", "anvil_rpc_finalized").await;

    let last_block = 70;
    test.mine(last_block).await;

    {
        let worker_config = test.ctx.daemon_worker().config().clone();
        let metadata_db = test.ctx.daemon_worker().metadata_db().clone();
        let data_store = test.ctx.daemon_worker().data_store().clone();
        let dataset_store = test.ctx.daemon_worker().dataset_store().clone();
        tokio::spawn(async move {
            let dataset_ref: Reference = "_/anvil_rpc_finalized@0.0.0".parse().unwrap();
            test_helpers::dump_internal(
                worker_config,
                metadata_db,
                data_store,
                dataset_store,
                dataset_ref,
                dump::EndBlock::None,
                1,
                None, // microbatch_max_interval_override
            )
            .await
            .expect("Failed to start continuous dump task");
        });
    }

    loop {
        tokio::time::sleep(Duration::from_secs(1)).await;
        if let Some(block) = test.max_dump_block("anvil_rpc_finalized").await
            && block >= (last_block - 64)
        {
            break;
        }
    }

    // Ethereum PoS finalizes after 2 epochs (32 slots/blocks each) totalling 64 blocks.
    assert_eq!(
        test.max_dump_block("anvil_rpc_finalized").await,
        Some(last_block - 64)
    );
}

#[tokio::test]
async fn flight_data_app_metadata() {
    let test = ReorgTestCtx::setup("flight_data_app_metadata", "anvil_rpc").await;
    let query = "SELECT block_num, hash FROM anvil_rpc.blocks SETTINGS stream = true";

    // Test initial block (block 0)
    test.dump("anvil_rpc", 0).await;
    let mut flight_data = test.flight_metadata_stream(query).await;

    // This sleep avoids a race between the first schema message, the flight data message,
    // and receiving both.
    tokio::time::sleep(Duration::from_millis(100)).await;
    let metadata = ReorgTestCtx::pull_flight_metadata(&mut flight_data).await;
    assert_eq!(metadata.len(), 1);
    assert_eq!(
        metadata[0],
        test.expected_block_range("anvil_rpc", 0..=0).await
    );

    // Test mining and dumping more blocks
    test.mine(2).await;
    test.dump("anvil_rpc", 2).await;

    let metadata = ReorgTestCtx::pull_flight_metadata(&mut flight_data).await;
    assert_eq!(metadata.len(), 1);
    assert_eq!(
        metadata[0],
        test.expected_block_range("anvil_rpc", 1..=2).await
    );

    // Test reorg scenario
    test.reorg(1).await;
    test.mine(1).await;
    test.dump("anvil_rpc", 3).await;
    test.dump("anvil_rpc", 3).await; // Dump twice to ensure reorg is detected

    let metadata = ReorgTestCtx::pull_flight_metadata(&mut flight_data).await;
    assert_eq!(metadata.len(), 1);
    assert_eq!(
        metadata[0],
        test.expected_block_range("anvil_rpc", 2..=3).await
    );
}

#[tokio::test]
async fn streaming_reorg_desync() {
    let test = ReorgTestCtx::setup_multi_dataset(
        "streaming_reorg_desync",
        &["anvil_rpc"],
        &["sql_over_anvil_1", "sql_over_anvil_2"],
    )
    .await;

    // Initial dump of all datasets to block 0
    test.dump("anvil_rpc", 0).await;
    test.dump("sql_over_anvil_1", 0).await;
    test.dump("sql_over_anvil_2", 0).await;

    // Set up streaming query that unions both SQL datasets
    let streaming_query = r#"
        SELECT block_num, hash, parent_hash FROM sql_over_anvil_1.blocks
        UNION ALL
        SELECT block_num, hash, parent_hash FROM sql_over_anvil_2.blocks
        SETTINGS stream = true
    "#;

    let mut flight_client = test.new_flight_client().await;
    flight_client
        .register_stream("stream", streaming_query)
        .await
        .expect("Failed to register stream");

    // Check initial stream (should have 2 blocks: one from each dataset for block 0)
    check_batch(&mut flight_client, 2).await;

    // Mine more blocks and cause desync between datasets
    test.mine(2).await;
    test.dump("anvil_rpc", 2).await;
    test.dump("sql_over_anvil_1", 2).await; // Only dump one dataset
    test.reorg(1).await;
    test.mine(2).await;
    test.dump("anvil_rpc", 4).await;
    test.dump("anvil_rpc", 4).await; // Dump twice to ensure reorg detection
    test.dump("sql_over_anvil_2", 4).await; // Only dump the other dataset

    // At this point, datasets should be out of sync
    assert_ne!(
        test.query_blocks("sql_over_anvil_1", None).await,
        test.query_blocks("sql_over_anvil_2", None).await,
        "Datasets should be out of sync"
    );

    // Resync the first dataset
    test.dump("sql_over_anvil_1", 4).await;

    // Check final stream (should have 8 blocks total: 4 from each dataset after resync)
    check_batch(&mut flight_client, 8).await;
}

#[tokio::test]
async fn streaming_reorg_rewind_shallow() {
    let test = ReorgTestCtx::setup_multi_dataset(
        "streaming_reorg_rewind_shallow",
        &["anvil_rpc"],
        &["sql_over_anvil_1"],
    )
    .await;

    // Initial dump to block 0
    test.dump("anvil_rpc", 0).await;
    test.dump("sql_over_anvil_1", 0).await;

    // Set up streaming query
    let streaming_query = r#"
        SELECT block_num, hash, parent_hash
        FROM sql_over_anvil_1.blocks
        SETTINGS stream = true
    "#;

    let mut flight_client = test.new_flight_client().await;
    flight_client
        .register_stream("stream", streaming_query)
        .await
        .expect("Failed to register stream");

    // Check initial stream matches queried data
    assert_eq!(
        take_blocks_from_stream(&mut flight_client, 1).await,
        test.query_blocks("anvil_rpc", None).await,
    );

    // Mine more blocks and dump
    test.mine(2).await;
    test.dump("anvil_rpc", 2).await;
    test.dump("sql_over_anvil_1", 2).await;

    // Check stream includes new blocks
    assert_eq!(
        &take_blocks_from_stream(&mut flight_client, 2).await,
        &test.query_blocks("anvil_rpc", None).await[1..=2],
    );

    // Trigger shallow reorg (depth 1)
    test.reorg(1).await;
    test.mine(2).await;
    test.dump("anvil_rpc", 4).await;
    test.dump("anvil_rpc", 4).await; // Dump twice to ensure reorg detection
    test.dump("sql_over_anvil_1", 4).await;

    // Check stream shows rewind from shallow reorg
    assert_eq!(
        &take_blocks_from_stream(&mut flight_client, 3).await,
        &test.query_blocks("anvil_rpc", None).await[2..=4],
    );
}

#[tokio::test]
async fn streaming_reorg_rewind_deep() {
    let test = ReorgTestCtx::setup_multi_dataset(
        "streaming_reorg_rewind_deep",
        &["anvil_rpc"],
        &["sql_over_anvil_1"],
    )
    .await;

    // Initial dump to block 0
    test.dump("anvil_rpc", 0).await;
    test.dump("sql_over_anvil_1", 0).await;

    // Set up streaming query
    let streaming_query = r#"
        SELECT block_num, hash, parent_hash
        FROM sql_over_anvil_1.blocks
        SETTINGS stream = true
    "#;

    let mut flight_client = test.new_flight_client().await;
    flight_client
        .register_stream("stream", streaming_query)
        .await
        .expect("Failed to register stream");

    // Check initial stream matches queried data
    assert_eq!(
        take_blocks_from_stream(&mut flight_client, 1).await,
        test.query_blocks("anvil_rpc", None).await,
    );

    // Mine many blocks and dump incrementally
    test.mine(6).await;
    test.dump("anvil_rpc", 6).await;
    test.dump("sql_over_anvil_1", 2).await;
    test.dump("sql_over_anvil_1", 4).await;
    test.dump("sql_over_anvil_1", 6).await;

    // Check stream includes all new blocks
    assert_eq!(
        &take_blocks_from_stream(&mut flight_client, 6).await,
        &test.query_blocks("anvil_rpc", None).await[1..=6],
    );

    // Trigger deep reorg (depth 5)
    test.reorg(5).await;
    test.mine(2).await;
    test.dump("anvil_rpc", 8).await;
    test.dump("anvil_rpc", 8).await; // Dump twice to ensure reorg detection
    test.dump("sql_over_anvil_1", 8).await;

    // Check stream shows rewind from deep reorg
    assert_eq!(
        &take_blocks_from_stream(&mut flight_client, 7).await,
        &test.query_blocks("anvil_rpc", None).await[2..=8],
    );
}

/// Custom test context wrapper that provides convenience methods for reorg testing.
///
/// This wraps the testlib TestCtx with additional methods that match the old AnvilTestContext
/// interface, making it easier to migrate existing tests with minimal changes.
struct ReorgTestCtx {
    ctx: TestCtx,
}

impl ReorgTestCtx {
    /// Create a new ReorgTestContext with the given test name and dataset.
    async fn setup(test_name: &str, dataset_name: &str) -> Self {
        logging::init();

        let ctx = TestCtxBuilder::new(test_name)
            .with_dataset_manifest(dataset_name)
            .with_anvil_ipc()
            .build()
            .await
            .expect("Failed to create test context");

        Self { ctx }
    }

    /// Create a new ReorgTestContext with multiple datasets.
    ///
    /// Takes separate parameters for explicit control:
    /// - `datasets`: Regular dataset manifests (e.g., "anvil_rpc")
    /// - `derived_datasets`: Derived dataset names to register as TypeScript datasets (e.g., "sql_over_anvil_1")
    async fn setup_multi_dataset(
        test_name: &str,
        datasets: &[&str],
        derived_datasets: &[&str],
    ) -> Self {
        logging::init();

        let builder = TestCtxBuilder::new(test_name)
            .with_anvil_ipc()
            .with_dataset_manifests(datasets.iter().map(|&s| s.to_string()));

        let ctx = builder
            .build()
            .await
            .expect("Failed to create test context");

        // Register derived (TypeScript) datasets
        let cli = ctx.new_amp_cli();
        for dataset_name in derived_datasets {
            let dataset = DatasetPackage::new(dataset_name, Some("amp.config.ts"));
            dataset
                .register(&cli, "0.0.0")
                .await
                .unwrap_or_else(|e| panic!("Failed to register {} dataset: {}", dataset_name, e));
        }

        Self { ctx }
    }

    /// Mine the specified number of blocks using the Anvil fixture.
    async fn mine(&self, blocks: u64) {
        self.ctx
            .anvil()
            .mine(blocks)
            .await
            .expect("Failed to mine blocks");
    }

    /// Trigger a blockchain reorganization with the specified depth.
    async fn reorg(&self, depth: u64) {
        self.ctx
            .anvil()
            .reorg(depth)
            .await
            .expect("Failed to trigger reorg");
    }

    /// Dump dataset up to the specified end block.
    async fn dump(&self, dataset: &str, end: BlockNum) {
        let dataset_ref: Reference = format!("_/{}@0.0.0", dataset).parse().unwrap();
        test_helpers::dump_dataset(
            self.ctx.daemon_worker().config().clone(),
            self.ctx.daemon_worker().metadata_db().clone(),
            self.ctx.daemon_worker().data_store().clone(),
            self.ctx.daemon_worker().dataset_store().clone(),
            dataset_ref,
            end,
        )
        .await
        .expect("Failed to dump dataset");
    }

    /// Query blocks from the specified dataset.
    async fn query_blocks(&self, dataset: &str, take: Option<usize>) -> Vec<BlockRow> {
        let test_env = &self.ctx;
        let jsonl_client = test_env.new_jsonl_client();
        let limit_clause = take.map(|n| format!(" LIMIT {}", n)).unwrap_or_default();
        let sql = format!(
            "SELECT block_num, hash, parent_hash FROM {}.blocks ORDER BY block_num ASC{}",
            dataset, limit_clause
        );

        jsonl_client
            .query(&sql)
            .await
            .map(|mut rows: Vec<BlockRow>| {
                rows.sort_by_key(|r| r.block_num);
                rows
            })
            .expect("Failed to query blocks")
    }

    /// Get metadata ranges for the specified dataset.
    async fn metadata_ranges(&self, dataset: &str) -> Vec<BlockRange> {
        let test_env = &self.ctx;
        let sql_query = SqlStr::new_unchecked(format!("select * from {}.blocks", dataset));
        let sql = sql::parse(&sql_query).expect("Failed to parse SQL for dataset.blocks");
        let env = test_env
            .daemon_server()
            .config()
            .make_query_env()
            .expect("Failed to create query environment");
        let catalog = catalog_for_sql(
            test_env.daemon_server().dataset_store(),
            test_env.daemon_server().metadata_db(),
            test_env.daemon_server().data_store(),
            &sql,
            env,
        )
        .await
        .expect("Failed to create catalog for SQL query");
        let table = catalog
            .tables()
            .iter()
            .find(|t| t.table_name() == "blocks")
            .expect("Failed to find blocks table in catalog");
        test_helpers::get_table_block_ranges(table).await
    }

    /// Get information about the latest block.
    async fn latest_block(&self) -> BlockRow {
        self.ctx
            .anvil()
            .latest_block()
            .await
            .expect("Failed to get latest block")
            .into()
    }

    /// Get the maximum dumped block number for a dataset.
    async fn max_dump_block(&self, dataset: &str) -> Option<BlockNum> {
        let ranges = self.metadata_ranges(dataset).await;
        ranges.iter().map(|r| r.end()).max()
    }

    /// Create a new Flight client for this test context.
    async fn new_flight_client(&self) -> FlightClient {
        self.ctx
            .new_flight_client()
            .await
            .expect("Failed to create flight client")
    }

    /// Create a Flight data stream for metadata extraction.
    ///
    /// Returns a receiver that will receive raw FlightData messages containing
    /// app_metadata fields with block range information.
    async fn flight_metadata_stream(&self, query: &str) -> mpsc::UnboundedReceiver<FlightData> {
        let mut client = self
            .ctx
            .new_flight_client()
            .await
            .expect("Failed to create flight client");
        client
            .execute_with_metadata_stream(query)
            .await
            .expect("Failed to start metadata stream")
    }

    /// Extract BlockRange metadata from FlightData app_metadata field on microbatch end.
    ///
    /// Returns None if the FlightData contains no metadata, parsing fails, or the microbatch is
    /// incomplete.
    fn extract_block_range(data: &FlightData) -> Option<BlockRange> {
        if data.app_metadata.is_empty() || data.data_body.is_empty() {
            return None;
        }

        #[derive(Deserialize)]
        struct Metadata {
            ranges: Vec<BlockRange>,
        }

        let metadata: Metadata = serde_json::from_slice(&data.app_metadata)
            .expect("Failed to parse app_metadata as JSON");

        assert_eq!(
            metadata.ranges.len(),
            1,
            "Expected exactly one range in metadata"
        );
        metadata.ranges.into_iter().next()
    }

    /// Pull and extract all metadata from a flight stream.
    ///
    /// Drains the receiver and returns all BlockRange metadata found.
    async fn pull_flight_metadata(rx: &mut mpsc::UnboundedReceiver<FlightData>) -> Vec<BlockRange> {
        let mut buffer = Vec::new();
        rx.recv_many(&mut buffer, usize::MAX).await;
        buffer
            .into_iter()
            .filter_map(|data| Self::extract_block_range(&data))
            .collect()
    }

    /// Create expected BlockRange for testing metadata.
    ///
    /// Builds the expected BlockRange based on queried blocks and the given range.
    async fn expected_block_range(
        &self,
        dataset: &str,
        numbers: RangeInclusive<BlockNum>,
    ) -> BlockRange {
        let blocks = self.query_blocks(dataset, None).await;
        BlockRange {
            numbers: numbers.clone(),
            network: "anvil".to_string(),
            hash: blocks[*numbers.end() as usize].hash,
            prev_hash: Some(blocks[*numbers.start() as usize].parent_hash),
        }
    }
}

#[derive(Debug, PartialEq, Eq, serde::Deserialize)]
struct BlockRow {
    block_num: BlockNum,
    hash: BlockHash,
    parent_hash: BlockHash,
}

impl From<BlockInfo> for BlockRow {
    fn from(block_info: BlockInfo) -> Self {
        Self {
            block_num: block_info.block_num,
            hash: block_info.hash,
            parent_hash: block_info.parent_hash,
        }
    }
}

// Helper functions

/// Check that blocks form a valid chain (consecutive block numbers and hash links).
#[track_caller]
fn check_blocks(blocks: &[BlockRow]) {
    for b in blocks.windows(2) {
        assert_eq!(b[0].block_num, b[1].block_num - 1);
        assert_eq!(b[0].hash, b[1].parent_hash);
    }
}

/// Check that streaming results contain duplicates from both datasets.
async fn check_batch(flight_client: &mut FlightClient, take: usize) {
    let (blocks, _batch_count) = flight_client
        .take_from_stream("stream", take)
        .await
        .expect("Failed to take from stream");
    let blocks: Vec<BlockRow> =
        serde_json::from_value(blocks).expect("Failed to deserialize blocks");

    let mut by_number: BTreeMap<BlockNum, Vec<BlockRow>> = BTreeMap::new();
    for block in blocks {
        by_number.entry(block.block_num).or_default().push(block);
    }

    eprintln!("stream blocks = {:#?}", by_number);
    for blocks in by_number.values() {
        assert_eq!(
            blocks.len(),
            2,
            "Should have duplicate blocks from both datasets"
        );
        assert_eq!(blocks[0], blocks[1], "Blocks should be identical");
    }
}

/// Take and sort blocks from a stream.
async fn take_blocks_from_stream(flight_client: &mut FlightClient, take: usize) -> Vec<BlockRow> {
    let (blocks, _batch_count) = flight_client
        .take_from_stream("stream", take)
        .await
        .expect("Failed to take from stream");
    let mut blocks: Vec<BlockRow> =
        serde_json::from_value(blocks).expect("Failed to deserialize blocks");
    blocks.sort_by_key(|b| b.block_num);
    blocks
}
