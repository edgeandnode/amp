use std::{collections::BTreeMap, ops::RangeInclusive, sync::Arc};

use alloy::{
    node_bindings::{Anvil, AnvilInstance},
    primitives::BlockHash,
    providers::{Provider as _, ext::AnvilApi as _},
    rpc::types::anvil::ReorgOptions,
};
use common::{BlockNum, metadata::segments::BlockRange, query_context::parse_sql};
use dataset_store::DatasetStore;
use monitoring::logging;
use rand::{Rng, RngCore, SeedableRng as _, rngs::StdRng};

use crate::{
    test_client::TestClient,
    test_support::{SnapshotContext, TestEnv, table_ranges},
};

struct AnvilTestContext {
    env: TestEnv,
    client: TestClient,
    provider: alloy::providers::DynProvider,
    _anvil: AnvilInstance,
}

impl AnvilTestContext {
    async fn setup(test_name: &str) -> Self {
        logging::init();
        let anvil = Anvil::new().port(0_u16).spawn();
        let url = anvil.endpoint_url();
        let env = TestEnv::new(test_name, true, Some(url.as_str()))
            .await
            .unwrap();
        let client = TestClient::connect(&env).await.unwrap();
        let provider = alloy::providers::ProviderBuilder::new().connect_http(url);
        Self {
            env,
            client,
            provider: provider.erased(),
            _anvil: anvil,
        }
    }

    async fn dataset_store(&self) -> Arc<DatasetStore> {
        DatasetStore::new(self.env.config.clone(), self.env.metadata_db.clone())
    }

    async fn mine(&self, blocks: u64) {
        tracing::info!(blocks, "mine");
        self.provider.anvil_mine(Some(blocks), None).await.unwrap()
    }

    async fn reorg(&self, depth: u64) {
        tracing::info!(depth, "reorg");
        assert_ne!(depth, 0);
        let original_head = self.latest_block().await;
        self.provider
            .anvil_reorg(ReorgOptions {
                depth,
                tx_block_pairs: vec![],
            })
            .await
            .unwrap();
        let new_head = self.latest_block().await;
        assert_eq!(original_head.block_num, new_head.block_num);
        assert_ne!(original_head.hash, new_head.hash);
    }

    async fn dump(&self, dataset: &str, range: RangeInclusive<BlockNum>) -> SnapshotContext {
        SnapshotContext::temp_dump(&self.env, dataset, *range.start(), *range.end(), 1)
            .await
            .unwrap()
    }

    async fn metadata_ranges(&self, dataset: &str) -> Vec<BlockRange> {
        let sql = parse_sql(&format!("select * from {}.blocks", dataset)).unwrap();
        let env = self.env.config.make_query_env().unwrap();
        let dataset_store = self.dataset_store().await;
        let ctx = dataset_store.ctx_for_sql(&sql, env).await.unwrap();
        let tables = ctx.catalog().tables();
        let table = tables.iter().find(|t| t.table_name() == "blocks").unwrap();
        table_ranges(&table).await.unwrap()
    }

    async fn latest_block(&self) -> BlockRow {
        let block = self
            .provider
            .get_block(alloy::eips::BlockId::latest())
            .await
            .unwrap()
            .unwrap();
        BlockRow {
            block_num: block.header.number,
            hash: block.header.hash,
            parent_hash: block.header.parent_hash,
        }
    }

    async fn query_blocks(&mut self, dataset: &str, take: Option<usize>) -> Vec<BlockRow> {
        let query = format!(
            r#"
            SELECT block_num, hash, parent_hash
            FROM {dataset}.blocks
            ORDER BY block_num ASC
            "#,
        );
        let response = self.client.run_query(&query, take).await.unwrap();
        let rows: Vec<BlockRow> = serde_json::from_value(response).unwrap();
        rows
    }
}

#[derive(Debug, PartialEq, Eq, serde::Deserialize)]
struct BlockRow {
    block_num: BlockNum,
    hash: BlockHash,
    parent_hash: BlockHash,
}

#[tokio::test]
async fn rpc_reorg_simple() {
    let mut test = AnvilTestContext::setup("rpc_reorg_simple").await;

    test.dump("anvil_rpc", 0..=0).await;
    test.mine(2).await;
    test.dump("anvil_rpc", 1..=2).await;
    let blocks0 = test.query_blocks("anvil_rpc", None).await;
    test.reorg(1).await;
    test.mine(1).await;
    test.dump("anvil_rpc", 0..=3).await;
    let blocks1 = test.query_blocks("anvil_rpc", None).await;
    test.dump("anvil_rpc", 0..=3).await;
    let blocks2 = test.query_blocks("anvil_rpc", None).await;

    // At this point, the chain looks like this:
    //   0, 1, 2
    //       , 2', 3
    // blocks0 should contain ranges [0,0], [1,2]
    // blocks1 should contain ranges [0,0], [1,2] (missing block 2', and therefore treating range [3,3] as a fork)
    assert_eq!(blocks0.len(), 3);
    assert_eq!(&blocks0, &blocks1);
    // blocks2 should contain ranges [0,0], [1,2'], [3,3] (retaining range [1,2] as a fork)
    assert_eq!(blocks2.len(), 4);
    assert_ne!(&blocks1[2].hash, &blocks2[3].parent_hash);
    for window in blocks2.windows(2) {
        assert_eq!(window[0].hash, window[1].parent_hash);
    }
    let mut ranges = test.metadata_ranges("anvil_rpc").await;
    ranges.sort_by_key(|r| *r.numbers.start());
    assert_eq!(
        ranges.iter().map(|r| r.numbers.clone()).collect::<Vec<_>>(),
        vec![0..=0, 1..=2, 1..=2, 3..=3],
    );
    assert!(ranges.contains(&BlockRange {
        numbers: blocks1[1].block_num..=blocks1[2].block_num,
        network: "anvil".to_string(),
        hash: blocks1[2].hash,
        prev_hash: Some(blocks1[1].parent_hash),
    }));
}

#[tokio::test]
async fn rpc_reorg_prop() {
    let mut test = AnvilTestContext::setup("rpc_reorg_prop").await;

    let seed = rand::rng().next_u64();
    println!("seed: {seed}");
    let mut rng = StdRng::seed_from_u64(seed);

    #[track_caller]
    fn check_blocks(blocks: &[BlockRow]) {
        for b in blocks.windows(2) {
            assert_eq!(b[0].block_num, b[1].block_num - 1);
            assert_eq!(b[0].hash, b[1].parent_hash);
        }
    }

    for _ in 0..3 {
        test.mine(rng.random_range(1..=3)).await;
        test.dump("anvil_rpc", 0..=test.latest_block().await.block_num)
            .await;
        let blocks0 = test.query_blocks("anvil_rpc", None).await;
        eprintln!("blocks0 = {:#?}", blocks0);
        check_blocks(&blocks0);
        assert_eq!(
            blocks0.len(),
            test.latest_block().await.block_num as usize + 1
        );

        let reorg_depth = u64::min(rng.random_range(1..=3), test.latest_block().await.block_num);
        test.reorg(reorg_depth).await;
        test.dump("anvil_rpc", 0..=test.latest_block().await.block_num)
            .await;
        // no reorg detected, since dumped block height has not increased
        assert_eq!(blocks0, test.query_blocks("anvil_rpc", None).await);

        // mine at least one block to detect reorg
        test.mine(rng.random_range(1..=3)).await;
        test.dump("anvil_rpc", 0..=test.latest_block().await.block_num)
            .await;

        // the canonical chain must be resolved in at most reorg_depth dumps
        for _ in 0..reorg_depth {
            test.mine(rng.random_range(0..=3)).await;
            test.dump("anvil_rpc", 0..=test.latest_block().await.block_num)
                .await;
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
        assert_eq!(blocks1.last().unwrap().hash, latest.hash);
    }
}

#[tokio::test]
async fn streaming_reorg_desync() {
    let mut test = AnvilTestContext::setup("streaming_reorg_desync").await;

    test.dump("anvil_rpc", 0..=0).await;
    test.dump("sql_over_anvil_1", 0..=0).await;
    test.dump("sql_over_anvil_2", 0..=0).await;

    let streaming_query = r#"
        SELECT block_num, hash, parent_hash FROM sql_over_anvil_1.blocks
        UNION ALL
        SELECT block_num, hash, parent_hash FROM sql_over_anvil_2.blocks
        SETTINGS stream = true
    "#;
    test.client
        .register_stream("stream", streaming_query)
        .await
        .unwrap();

    async fn check_batch(client: &mut TestClient, take: usize) {
        let blocks = client.take_from_stream("stream", take).await.unwrap();
        let blocks: Vec<BlockRow> = serde_json::from_value(blocks).unwrap();
        let mut by_number: BTreeMap<BlockNum, Vec<BlockRow>> = Default::default();
        for block in blocks {
            by_number.entry(block.block_num).or_default().push(block);
        }
        eprintln!("stream blocks = {:#?}", by_number);
        for blocks in by_number.values() {
            assert_eq!(blocks.len(), 2);
            assert_eq!(blocks[0], blocks[1]);
        }
    }

    check_batch(&mut test.client, 2).await;

    test.mine(2).await;
    test.dump("anvil_rpc", 1..=2).await;
    test.dump("sql_over_anvil_1", 1..=2).await;
    test.reorg(1).await;
    test.mine(2).await;
    test.dump("anvil_rpc", 1..=4).await;
    test.dump("anvil_rpc", 1..=4).await;
    test.dump("sql_over_anvil_2", 1..=4).await;

    assert_ne!(
        test.query_blocks("sql_over_anvil_1", None).await,
        test.query_blocks("sql_over_anvil_2", None).await,
    );

    test.dump("sql_over_anvil_1", 1..=4).await;
    test.dump("sql_over_anvil_1", 1..=4).await;

    check_batch(&mut test.client, 8).await;
}

#[tokio::test]
async fn streaming_reorg_rewind_shallow() {
    let mut test = AnvilTestContext::setup("streaming_reorg_rewind").await;

    test.dump("anvil_rpc", 0..=0).await;
    test.dump("sql_over_anvil_1", 0..=0).await;

    let streaming_query = r#"
        SELECT block_num, hash, parent_hash
        FROM sql_over_anvil_1.blocks
        SETTINGS stream = true
    "#;
    test.client
        .register_stream("stream", streaming_query)
        .await
        .unwrap();

    async fn take_blocks(client: &mut TestClient, take: usize) -> Vec<BlockRow> {
        let blocks = client.take_from_stream("stream", take).await.unwrap();
        let mut blocks: Vec<BlockRow> = serde_json::from_value(blocks).unwrap();
        blocks.sort_by_key(|b| b.block_num);
        blocks
    }

    assert_eq!(
        take_blocks(&mut test.client, 1).await,
        test.query_blocks("anvil_rpc", None).await,
    );

    test.mine(2).await;
    test.dump("anvil_rpc", 1..=2).await;
    test.dump("sql_over_anvil_1", 1..=2).await;

    assert_eq!(
        &take_blocks(&mut test.client, 2).await,
        &test.query_blocks("anvil_rpc", None).await[1..=2],
    );

    test.reorg(1).await;
    test.mine(2).await;
    test.dump("anvil_rpc", 1..=4).await;
    test.dump("anvil_rpc", 1..=4).await;
    test.dump("sql_over_anvil_1", 1..=4).await;
    test.dump("sql_over_anvil_1", 1..=4).await;

    assert_eq!(
        &take_blocks(&mut test.client, 4).await,
        &test.query_blocks("anvil_rpc", None).await[1..=4],
    );
}

#[tokio::test]
async fn streaming_reorg_rewind_deep() {
    let mut test = AnvilTestContext::setup("streaming_reorg_rewind").await;

    test.dump("anvil_rpc", 0..=0).await;
    test.dump("sql_over_anvil_1", 0..=0).await;

    let streaming_query = r#"
        SELECT block_num, hash, parent_hash
        FROM sql_over_anvil_1.blocks
        SETTINGS stream = true
    "#;
    test.client
        .register_stream("stream", streaming_query)
        .await
        .unwrap();

    async fn take_blocks(client: &mut TestClient, take: usize) -> Vec<BlockRow> {
        let blocks = client.take_from_stream("stream", take).await.unwrap();
        let mut blocks: Vec<BlockRow> = serde_json::from_value(blocks).unwrap();
        blocks.sort_by_key(|b| b.block_num);
        blocks
    }

    assert_eq!(
        take_blocks(&mut test.client, 1).await,
        test.query_blocks("anvil_rpc", None).await,
    );

    test.mine(6).await;
    test.dump("anvil_rpc", 1..=6).await;
    test.dump("sql_over_anvil_1", 1..=2).await;
    test.dump("sql_over_anvil_1", 3..=4).await;
    test.dump("sql_over_anvil_1", 5..=6).await;

    assert_eq!(
        &take_blocks(&mut test.client, 6).await,
        &test.query_blocks("anvil_rpc", None).await[1..=6],
    );

    test.reorg(5).await;
    test.mine(2).await;
    test.dump("anvil_rpc", 1..=8).await;
    test.dump("anvil_rpc", 1..=8).await;
    test.dump("sql_over_anvil_1", 1..=8).await;
    test.dump("sql_over_anvil_1", 1..=8).await;
    test.dump("sql_over_anvil_1", 1..=8).await;
    test.dump("sql_over_anvil_1", 1..=8).await;

    assert_eq!(
        &take_blocks(&mut test.client, 8).await,
        &test.query_blocks("anvil_rpc", None).await[1..=8],
    );
}
