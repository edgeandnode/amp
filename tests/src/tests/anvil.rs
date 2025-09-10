use std::{collections::BTreeMap, ops::RangeInclusive, sync::Arc, usize};

use alloy::{
    node_bindings::Anvil,
    primitives::BlockHash,
    providers::{Provider as _, ext::AnvilApi as _},
    rpc::types::anvil::ReorgOptions,
};
use arrow_flight::{
    FlightData, flight_service_client::FlightServiceClient, sql::client::FlightSqlServiceClient,
};
use common::{
    BlockNum,
    arrow::array::{FixedSizeBinaryArray, UInt64Array},
    metadata::segments::{BlockRange, ResumeWatermark},
    query_context::parse_sql,
};
use dataset_store::DatasetStore;
use futures::StreamExt as _;
use monitoring::logging;
use nozzle::dump_cmd::dump;
use rand::{Rng, RngCore, SeedableRng as _, rngs::StdRng};
use serde::Deserialize;
use tokio::{sync::mpsc, task::JoinHandle};

use crate::{
    test_client::TestClient,
    test_support::{SnapshotContext, TestEnv, table_ranges},
};

pub(crate) struct AnvilTestContext {
    pub(crate) env: TestEnv,
    client: TestClient,
    provider: alloy::providers::DynProvider,
    _anvil: alloy::node_bindings::AnvilInstance,
    _ipc: tempfile::NamedTempFile,
}

impl AnvilTestContext {
    pub(crate) async fn setup(test_name: &str) -> Self {
        logging::init();

        // Generate unique IPC path for this test using tempfile
        let temp = tempfile::Builder::new().prefix("anvil").tempfile().unwrap();
        let anvil = Anvil::new().ipc_path(temp.path().to_string_lossy()).spawn();
        let path = anvil.ipc_path();
        let env = TestEnv::new_with_ipc(test_name, true, path).await.unwrap();

        let client = TestClient::connect(&env).await.unwrap();
        let provider = alloy::providers::ProviderBuilder::new()
            .connect_ipc(path.to_string().into())
            .await
            .unwrap();

        Self {
            env,
            client,
            provider: provider.erased(),
            _anvil: anvil,
            _ipc: temp,
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

    async fn dump(&self, dataset: &str, end: BlockNum) -> SnapshotContext {
        SnapshotContext::temp_dump(&self.env, dataset, end, 1)
            .await
            .unwrap()
    }

    async fn metadata_ranges(&self, dataset: &str) -> Vec<BlockRange> {
        let sql = parse_sql(&format!("select * from {}.blocks", dataset)).unwrap();
        let env = self.env.config.make_query_env().unwrap();
        let dataset_store = self.dataset_store().await;
        let catalog = dataset_store.catalog_for_sql(&sql, env).await.unwrap();
        let tables = catalog.tables();
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
pub(crate) struct BlockRow {
    pub(crate) block_num: BlockNum,
    hash: BlockHash,
    parent_hash: BlockHash,
}

#[tokio::test]
async fn rpc_reorg_simple() {
    let mut test = AnvilTestContext::setup("rpc_reorg_simple").await;

    test.dump("anvil_rpc", 0).await;
    test.mine(2).await;
    test.dump("anvil_rpc", 2).await;
    let blocks0 = test.query_blocks("anvil_rpc", None).await;
    test.reorg(1).await;
    test.mine(1).await;
    test.dump("anvil_rpc", 3).await;
    let blocks1 = test.query_blocks("anvil_rpc", None).await;
    test.dump("anvil_rpc", 3).await;
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
        test.dump("anvil_rpc", test.latest_block().await.block_num)
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
        test.dump("anvil_rpc", test.latest_block().await.block_num)
            .await;
        // no reorg detected, since dumped block height has not increased
        assert_eq!(blocks0, test.query_blocks("anvil_rpc", None).await);

        // mine at least one block to detect reorg
        test.mine(rng.random_range(1..=3)).await;
        test.dump("anvil_rpc", test.latest_block().await.block_num)
            .await;

        // the canonical chain must be resolved in at most reorg_depth dumps
        for _ in 0..reorg_depth {
            test.mine(rng.random_range(0..=3)).await;
            test.dump("anvil_rpc", test.latest_block().await.block_num)
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

    test.dump("anvil_rpc", 0).await;
    test.dump("sql_over_anvil_1", 0).await;
    test.dump("sql_over_anvil_2", 0).await;

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
    test.dump("anvil_rpc", 2).await;
    test.dump("sql_over_anvil_1", 2).await;
    test.reorg(1).await;
    test.mine(2).await;
    test.dump("anvil_rpc", 4).await;
    test.dump("anvil_rpc", 4).await;
    test.dump("sql_over_anvil_2", 4).await;

    assert_ne!(
        test.query_blocks("sql_over_anvil_1", None).await,
        test.query_blocks("sql_over_anvil_2", None).await,
    );

    test.dump("sql_over_anvil_1", 4).await;

    check_batch(&mut test.client, 8).await;
}

#[tokio::test]
async fn streaming_reorg_rewind_shallow() {
    let mut test = AnvilTestContext::setup("streaming_reorg_rewind").await;

    test.dump("anvil_rpc", 0).await;
    test.dump("sql_over_anvil_1", 0).await;

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
    test.dump("anvil_rpc", 2).await;
    test.dump("sql_over_anvil_1", 2).await;

    assert_eq!(
        &take_blocks(&mut test.client, 2).await,
        &test.query_blocks("anvil_rpc", None).await[1..=2],
    );

    test.reorg(1).await;
    test.mine(2).await;
    test.dump("anvil_rpc", 4).await;
    test.dump("anvil_rpc", 4).await;
    test.dump("sql_over_anvil_1", 4).await;

    assert_eq!(
        &take_blocks(&mut test.client, 3).await,
        &test.query_blocks("anvil_rpc", None).await[2..=4],
    );
}

#[tokio::test]
async fn streaming_reorg_rewind_deep() {
    let mut test = AnvilTestContext::setup("streaming_reorg_rewind").await;

    test.dump("anvil_rpc", 0).await;
    test.dump("sql_over_anvil_1", 0).await;

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
    test.dump("anvil_rpc", 6).await;
    test.dump("sql_over_anvil_1", 2).await;
    test.dump("sql_over_anvil_1", 4).await;
    test.dump("sql_over_anvil_1", 6).await;

    assert_eq!(
        &take_blocks(&mut test.client, 6).await,
        &test.query_blocks("anvil_rpc", None).await[1..=6],
    );

    test.reorg(5).await;
    test.mine(2).await;
    test.dump("anvil_rpc", 8).await;
    test.dump("anvil_rpc", 8).await;
    test.dump("sql_over_anvil_1", 8).await;

    assert_eq!(
        &take_blocks(&mut test.client, 7).await,
        &test.query_blocks("anvil_rpc", None).await[2..=8],
    );
}

#[tokio::test]
async fn flight_data_app_metadata() {
    async fn pull_flight_metadata(rx: &mut mpsc::UnboundedReceiver<FlightData>) -> Vec<BlockRange> {
        let mut buffer: Vec<FlightData> = Default::default();
        rx.recv_many(&mut buffer, usize::MAX).await;
        buffer.into_iter().filter_map(extract_metadata).collect()
    }
    fn extract_metadata(data: FlightData) -> Option<BlockRange> {
        if data.app_metadata.is_empty() {
            return None;
        }
        #[derive(Deserialize)]
        struct Metadata {
            ranges: Vec<BlockRange>,
        }
        let mut metadata: Metadata = serde_json::from_slice(&data.app_metadata.to_vec()).unwrap();
        assert_eq!(metadata.ranges.len(), 1);
        let range = metadata.ranges.remove(0);
        Some(range)
    }
    async fn expected_range(
        test: &mut AnvilTestContext,
        numbers: RangeInclusive<BlockNum>,
    ) -> BlockRange {
        let blocks = test.query_blocks("anvil_rpc", None).await;
        BlockRange {
            numbers: numbers.clone(),
            network: "anvil".to_string(),
            hash: blocks[*numbers.end() as usize].hash,
            prev_hash: Some(blocks[*numbers.start() as usize].parent_hash),
        }
    }

    let mut test = AnvilTestContext::setup("flight_data_app_metadata").await;
    let query = "SELECT block_num, hash FROM anvil_rpc.blocks SETTINGS stream = true";

    test.dump("anvil_rpc", 0).await;
    let mut flight_data = flight_data_stream(&test, query).await;
    assert_eq!(
        pull_flight_metadata(&mut flight_data).await[0],
        expected_range(&mut test, 0..=0).await,
    );

    test.mine(2).await;
    test.dump("anvil_rpc", 2).await;
    assert_eq!(
        pull_flight_metadata(&mut flight_data).await[0],
        expected_range(&mut test, 1..=2).await,
    );

    test.reorg(1).await;
    test.mine(1).await;
    test.dump("anvil_rpc", 3).await;
    test.dump("anvil_rpc", 3).await;
    assert_eq!(
        pull_flight_metadata(&mut flight_data).await[0],
        expected_range(&mut test, 2..=3).await,
    );
}

async fn flight_data_stream(
    test: &AnvilTestContext,
    query: &str,
) -> mpsc::UnboundedReceiver<FlightData> {
    let addr = format!("grpc://{}", test.env.server_addrs.flight_addr);
    let flight_client = FlightServiceClient::connect(addr).await.unwrap();
    let mut client = FlightSqlServiceClient::new_from_inner(flight_client);
    let info = client.execute(query.to_string(), None).await.unwrap();
    let ticket = info.endpoint[0].ticket.clone().unwrap();
    let response = client.inner_mut().do_get(ticket).await.unwrap();
    let mut flight_data_stream = response.into_inner();
    let (tx, rx) = mpsc::unbounded_channel();
    tokio::spawn(async move {
        while let Some(result) = flight_data_stream.next().await {
            let flight_data = result.unwrap();
            if let Err(_) = tx.send(flight_data) {
                return;
            }
        }
    });
    rx
}

#[tokio::test]
async fn nozzle_client() {
    let test = AnvilTestContext::setup("nozzle_client").await;
    let query = "SELECT block_num, hash FROM anvil_rpc.blocks SETTINGS stream = true";
    let last_block = 3;

    let endpoint = format!("grpc://{}", test.env.server_addrs.flight_addr);
    let mut client = nozzle_client::SqlClient::new(&endpoint).await.unwrap();
    test.dump("anvil_rpc", 0).await;
    let stream = client.query(query, None, None).await.unwrap();
    #[derive(Debug, PartialEq, Eq)]
    enum ControlMessage {
        Batch(Vec<nozzle_client::InvalidationRange>),
        Reorg(Vec<nozzle_client::InvalidationRange>),
    }
    let handle: JoinHandle<Vec<ControlMessage>> = tokio::spawn(async move {
        let mut control_messages: Vec<ControlMessage> = Default::default();
        let mut stream = nozzle_client::with_reorg(stream);
        while let Some(result) = stream.next().await {
            let response_batch = result.unwrap();
            control_messages.push(match response_batch {
                nozzle_client::ResponseBatchWithReorg::Batch { metadata, .. } => {
                    ControlMessage::Batch(metadata.ranges.into_iter().map(Into::into).collect())
                }
                nozzle_client::ResponseBatchWithReorg::Reorg { invalidation } => {
                    ControlMessage::Reorg(invalidation)
                }
            });
            if let Some(ControlMessage::Batch(ranges)) = control_messages.last()
                && *ranges[0].numbers.end() == last_block
            {
                break;
            }
        }
        control_messages
    });

    test.mine(1).await;
    test.dump("anvil_rpc", 1).await;
    test.mine(1).await;
    test.dump("anvil_rpc", 2).await;
    test.reorg(1).await;
    test.mine(1).await;
    test.dump("anvil_rpc", 3).await;
    test.dump("anvil_rpc", 3).await;

    assert_eq!(
        handle.await.unwrap(),
        vec![
            ControlMessage::Batch(vec![nozzle_client::InvalidationRange {
                network: "anvil".to_string(),
                numbers: 0..=0,
            }]),
            ControlMessage::Batch(vec![nozzle_client::InvalidationRange {
                network: "anvil".to_string(),
                numbers: 1..=1,
            }]),
            ControlMessage::Batch(vec![nozzle_client::InvalidationRange {
                network: "anvil".to_string(),
                numbers: 2..=2,
            }]),
            ControlMessage::Reorg(vec![nozzle_client::InvalidationRange {
                network: "anvil".to_string(),
                numbers: 2..=3,
            }]),
            ControlMessage::Batch(vec![nozzle_client::InvalidationRange {
                network: "anvil".to_string(),
                numbers: 2..=3,
            }]),
        ],
    );
}

#[tokio::test]
async fn dump_finalized() {
    let test = AnvilTestContext::setup("dump_finalized").await;
    async fn max_dump_block(test: &AnvilTestContext) -> BlockNum {
        let ranges = test.metadata_ranges("anvil_rpc").await;
        ranges.iter().map(|r| r.end()).max().unwrap()
    }

    let last_block = 70;
    test.mine(last_block).await;

    dump(
        test.env.config.clone(),
        test.env.metadata_db.clone(),
        vec!["anvil_rpc".to_string()],
        true,
        None,
        1,
        100,
        None,
        None,
        None,
        false,
        None,
        true,
    )
    .await
    .unwrap();
    // Ethereum PoS finalizes after 2 epochs (32 slots/blocks each) totalling 64 blocks.
    assert_eq!(max_dump_block(&test).await, last_block - 64);
    test.dump("anvil_rpc", last_block).await;
    assert_eq!(max_dump_block(&test).await, last_block);
}

#[tokio::test]
async fn client_stream_resume() {
    let mut test = AnvilTestContext::setup("client_stream_resume").await;

    let endpoint = format!("grpc://{}", test.env.server_addrs.flight_addr);
    let query = "SELECT block_num, hash, parent_hash FROM anvil_rpc.blocks SETTINGS stream = true";
    let mut client = nozzle_client::SqlClient::new(&endpoint).await.unwrap();

    #[derive(Debug)]
    struct Records {
        records: Vec<BlockRow>,
        watermark: ResumeWatermark,
    }

    async fn stream_blocks(
        client: &mut nozzle_client::SqlClient,
        query: &str,
        latest_block: BlockNum,
        resume_watermark: Option<&ResumeWatermark>,
    ) -> Records {
        println!("{:?}", resume_watermark);
        let mut stream = client.query(query, None, resume_watermark).await.unwrap();
        let mut records: Vec<BlockRow> = Default::default();
        while let Some(result) = stream.next().await {
            let batch = result.unwrap();
            let block_num_array = batch
                .data
                .column(0)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap();
            let hash_array = batch
                .data
                .column(1)
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .unwrap();
            let parent_hash_array = batch
                .data
                .column(2)
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .unwrap();
            for i in 0..batch.data.num_rows() {
                records.push(BlockRow {
                    block_num: block_num_array.value(i),
                    hash: BlockHash::from_slice(hash_array.value(i)),
                    parent_hash: BlockHash::from_slice(parent_hash_array.value(i)),
                });
            }
            let end_block = batch.metadata.ranges[0].end();
            let watermark = ResumeWatermark::from_ranges(batch.metadata.ranges);
            if end_block == latest_block {
                return Records { records, watermark };
            }
        }
        panic!("ran out of response batches")
    }

    test.mine(1).await;
    test.dump("anvil_rpc", 1).await;
    let stream1 = stream_blocks(&mut client, query, 1, None).await;
    // stream blocks [0, 1]
    assert_eq!(stream1.records, test.query_blocks("anvil_rpc", None).await);

    test.mine(1).await;
    test.dump("anvil_rpc", 2).await;
    let stream2 = stream_blocks(&mut client, query, 2, Some(&stream1.watermark)).await;
    // stream blocks [2]
    assert_eq!(
        stream2.records,
        test.query_blocks("anvil_rpc", None).await[2..=2],
    );

    test.reorg(2).await;
    test.mine(1).await;
    test.dump("anvil_rpc", 3).await;
    test.dump("anvil_rpc", 3).await;
    test.dump("anvil_rpc", 3).await;
    let stream3 = stream_blocks(&mut client, query, 3, Some(&stream2.watermark)).await;
    // stream blocks [1', 2', 3]
    assert_eq!(
        stream3.records,
        test.query_blocks("anvil_rpc", None).await[1..=3],
    );
}
