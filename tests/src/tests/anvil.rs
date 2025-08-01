use std::{ops::RangeInclusive, sync::Arc};

use alloy::{
    node_bindings::{Anvil, AnvilInstance},
    primitives::BlockHash,
    providers::{Provider as _, ext::AnvilApi as _},
    transports::http::reqwest,
};
use common::{BlockNum, metadata::segments::BlockRange, query_context::parse_sql, tracing_helpers};
use dataset_store::DatasetStore;
use rand::{Rng, RngCore, SeedableRng as _, rngs::StdRng};

use crate::test_support::{SnapshotContext, TestEnv, table_ranges};

const DATASET_NAME: &str = "anvil_rpc";

pub(crate) struct AnvilTestContext {
    pub(crate) env: TestEnv,
    http: reqwest::Client,
    provider: alloy::providers::DynProvider,
    _anvil: AnvilInstance,
}

impl AnvilTestContext {
    pub(crate) async fn setup(test_name: &str) -> Self {
        tracing_helpers::register_logger();
        let http = reqwest::Client::new();
        let anvil = Anvil::new().port(0_u16).spawn();
        let url = anvil.endpoint_url();
        let env = TestEnv::new(test_name, true, Some(url.as_str()))
            .await
            .unwrap();
        let provider = alloy::providers::ProviderBuilder::new().connect_reqwest(http.clone(), url);
        Self {
            env,
            http,
            provider: provider.erased(),
            _anvil: anvil,
        }
    }

    async fn dataset_store(&self) -> Arc<DatasetStore> {
        DatasetStore::new(self.env.config.clone(), self.env.metadata_db.clone())
    }

    pub(crate) async fn mine(&self, blocks: u64) {
        tracing::info!(blocks, "mine");
        self.provider.anvil_mine(Some(blocks), None).await.unwrap()
    }

    async fn reorg(&self, depth: u64) {
        tracing::info!(depth, "reorg");
        assert_ne!(depth, 0);
        let original_head = self.latest_block().await;
        self.provider
            .anvil_reorg(alloy_rpc_types_anvil::ReorgOptions {
                depth,
                tx_block_pairs: vec![],
            })
            .await
            .unwrap();
        let new_head = self.latest_block().await;
        assert_eq!(original_head.block_num, new_head.block_num);
        assert_ne!(original_head.hash, new_head.hash);
    }

    async fn dump(&self, range: RangeInclusive<BlockNum>) -> SnapshotContext {
        SnapshotContext::temp_dump(&self.env, DATASET_NAME, *range.start(), *range.end(), 1)
            .await
            .unwrap()
    }

    async fn metadata_ranges(&self) -> Vec<BlockRange> {
        let sql = parse_sql(&format!("select * from {}.blocks", DATASET_NAME)).unwrap();
        let env = self.env.config.make_query_env().unwrap();
        let dataset_store = self.dataset_store().await;
        let ctx = dataset_store.ctx_for_sql(&sql, env).await.unwrap();
        let tables = ctx.catalog().tables();
        let table = tables.iter().find(|t| t.table_name() == "blocks").unwrap();
        table_ranges(&table).await.unwrap()
    }

    pub(crate) async fn latest_block(&self) -> BlockRow {
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

    async fn query_blocks(&self, range: RangeInclusive<BlockNum>) -> Vec<BlockRow> {
        let url = format!("http://{}/", self.env.server_addrs.jsonl_addr);
        let sql = format!(
            r#"
            select block_num, hash, parent_hash
            from anvil_rpc.blocks
            where block_num >= {} and block_num <= {}
            order by block_num asc
            "#,
            range.start(),
            range.end(),
        );
        let response = self.http.post(url).body(sql).send().await.unwrap();
        let buffer = response.text().await.unwrap();
        let mut rows: Vec<BlockRow> = Default::default();
        for line in buffer.lines() {
            rows.push(serde_json::from_str(line).unwrap());
        }
        rows.sort_by_key(|r| r.block_num);
        rows
    }
}

#[derive(Debug, PartialEq, Eq, serde::Deserialize)]
pub(crate) struct BlockRow {
    pub(crate) block_num: BlockNum,
    hash: BlockHash,
    parent_hash: BlockHash,
}

#[tokio::test]
async fn rpc_reorg_simple() {
    let test = AnvilTestContext::setup("rpc_reorg_simple").await;

    test.dump(0..=0).await;
    test.mine(2).await;
    test.dump(0..=2).await;
    let blocks0 = test.query_blocks(0..=2).await;
    test.reorg(1).await;
    test.mine(1).await;
    test.dump(0..=3).await;
    let blocks1 = test.query_blocks(0..=3).await;
    test.dump(0..=3).await;
    let blocks2 = test.query_blocks(0..=3).await;

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
    let mut ranges = test.metadata_ranges().await;
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
    let test = AnvilTestContext::setup("rpc_reorg_prop").await;

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
        test.dump(0..=test.latest_block().await.block_num).await;
        let blocks0 = test.query_blocks(0..=1_000).await;
        eprintln!("blocks0 = {:#?}", blocks0);
        check_blocks(&blocks0);
        assert_eq!(
            blocks0.len(),
            test.latest_block().await.block_num as usize + 1
        );

        let reorg_depth = u64::min(rng.random_range(1..=3), test.latest_block().await.block_num);
        test.reorg(reorg_depth).await;
        test.dump(0..=test.latest_block().await.block_num).await;
        // no reorg detected, since dumped block height has not increased
        assert_eq!(blocks0, test.query_blocks(0..=1_000).await);

        // mine at least one block to detect reorg
        test.mine(rng.random_range(1..=3)).await;
        test.dump(0..=test.latest_block().await.block_num).await;

        // the canonical chain must be resolved in at most reorg_depth dumps
        for _ in 0..reorg_depth {
            test.mine(rng.random_range(0..=3)).await;
            test.dump(0..=test.latest_block().await.block_num).await;
        }

        let latest = test.latest_block().await;
        eprintln!("latest = {:#?}", latest);
        let blocks1 = test.query_blocks(0..=1_000).await;
        eprintln!("blocks1 = {:#?}", blocks1);
        check_blocks(&blocks1);
        let mut ranges = test.metadata_ranges().await;
        ranges.sort_by_key(|r| *r.numbers.start());
        eprintln!("ranges = {:#?}", ranges);
        assert_eq!(blocks1.len(), latest.block_num as usize + 1);
        assert_eq!(blocks1.last().unwrap().hash, latest.hash);
    }
}
