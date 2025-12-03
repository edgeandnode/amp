use std::ops::RangeInclusive;

use alloy::primitives::BlockHash;
use amp_client::AmpClient;
use common::{
    BlockNum,
    arrow::array::{FixedSizeBinaryArray, UInt64Array},
    metadata::segments::ResumeWatermark,
};
use futures::StreamExt;
use monitoring::logging;
use tokio::task::JoinHandle;

use crate::testlib::{self, fixtures::BlockInfo, helpers as test_helpers};

#[tokio::test]
async fn query_with_reorg_stream_returns_correct_control_messages() {
    let test = TestCtx::setup("amp_client", "anvil_rpc").await;
    let query = "SELECT block_num, hash FROM anvil_rpc.blocks SETTINGS stream = true";
    let last_block = 3;
    let client = test.new_amp_client().await;
    test.dump("_/anvil_rpc@0.0.0", 0).await;

    #[derive(Debug, PartialEq, Eq)]
    enum ControlMessage {
        Batch(RangeInclusive<BlockNum>),
        Reorg(RangeInclusive<BlockNum>),
    }

    let handle: JoinHandle<Vec<ControlMessage>> = tokio::spawn(async move {
        let mut control_messages: Vec<ControlMessage> = Default::default();
        let mut stream = client.stream(query).await.expect("Failed to create stream");
        while let Some(result) = stream.next().await {
            let event = result.expect("Failed to get event from stream");
            match event {
                amp_client::ProtocolMessage::Data { .. } => {}
                amp_client::ProtocolMessage::Watermark { ranges, .. } => {
                    control_messages.push(ControlMessage::Batch(ranges[0].numbers.clone()));
                }
                amp_client::ProtocolMessage::Reorg { invalidation, .. } => {
                    control_messages.push(ControlMessage::Reorg(invalidation[0].numbers.clone()));
                }
            };
            if let Some(ControlMessage::Batch(numbers)) = control_messages.last()
                && *numbers.end() == last_block
            {
                break;
            }
        }
        control_messages
    });

    test.mine(1).await;
    test.dump("_/anvil_rpc@0.0.0", 1).await;
    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    test.mine(1).await;
    test.dump("_/anvil_rpc@0.0.0", 2).await;
    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    test.reorg(1).await;
    test.mine(1).await;
    test.dump("_/anvil_rpc@0.0.0", 3).await;
    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    test.dump("_/anvil_rpc@0.0.0", 3).await;
    tokio::time::sleep(std::time::Duration::from_millis(10)).await;

    assert_eq!(
        handle.await.expect("Failed to await control messages task"),
        vec![
            ControlMessage::Batch(0..=0),
            ControlMessage::Batch(1..=1),
            ControlMessage::Batch(2..=2),
            ControlMessage::Reorg(2..=3),
            ControlMessage::Batch(2..=3),
        ],
    );
}

#[tokio::test]
async fn stream_with_resume_watermark_returns_incremental_blocks() {
    let test = TestCtx::setup("client_stream_resume", "anvil_rpc").await;
    let query = "SELECT block_num, hash, parent_hash FROM anvil_rpc.blocks SETTINGS stream = true";
    let mut client = test.new_amp_client().await;

    test.mine(1).await;
    test.dump("_/anvil_rpc@0.0.0", 1).await;
    let stream1 = stream_blocks(&mut client, query, 1, None).await;
    // stream blocks [0, 1]
    assert_eq!(stream1.records, test.query_blocks("anvil_rpc", None).await);

    test.mine(1).await;
    test.dump("_/anvil_rpc@0.0.0", 2).await;
    let stream2 = stream_blocks(&mut client, query, 2, Some(&stream1.watermark)).await;
    // stream blocks [2]
    assert_eq!(
        stream2.records,
        test.query_blocks("anvil_rpc", None).await[2..=2],
    );

    test.reorg(2).await;
    test.mine(1).await;
    test.dump("_/anvil_rpc@0.0.0", 3).await;
    test.dump("_/anvil_rpc@0.0.0", 3).await;
    test.dump("_/anvil_rpc@0.0.0", 3).await;
    let stream3 = stream_blocks(&mut client, query, 3, Some(&stream2.watermark)).await;
    // stream blocks [1', 2', 3]
    assert_eq!(
        stream3.records,
        test.query_blocks("anvil_rpc", None).await[1..=3],
    );
}

/// Test context wrapper for client-related tests.
///
/// This provides convenience methods for testing amp client functionality
/// with blockchain data and reorg scenarios.
struct TestCtx {
    ctx: testlib::ctx::TestCtx,
}

impl TestCtx {
    /// Set up a new test context for client testing.
    async fn setup(test_name: &str, dataset: &str) -> Self {
        logging::init();

        let ctx = testlib::ctx::TestCtxBuilder::new(test_name)
            .with_dataset_manifest(dataset)
            .with_anvil_ipc()
            .build()
            .await
            .expect("Failed to create test context");

        Self { ctx }
    }

    /// Mine blocks using the anvil fixture.
    async fn mine(&self, count: u64) {
        self.ctx
            .anvil()
            .mine(count)
            .await
            .expect("Failed to mine blocks");
    }

    /// Reorg blocks using the anvil fixture.
    async fn reorg(&self, depth: u64) {
        self.ctx
            .anvil()
            .reorg(depth)
            .await
            .expect("Failed to reorg blocks");
    }

    /// Dump a dataset using amp dump command.
    async fn dump(&self, dataset: &str, end: BlockNum) {
        test_helpers::dump_dataset(
            self.ctx.daemon_worker().config().clone(),
            self.ctx.daemon_worker().metadata_db().clone(),
            self.ctx.daemon_worker().dataset_store().clone(),
            dataset.parse().expect("failed to parse dataset reference"),
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

    /// Create a new amp_client::AmpClient for this test context.
    async fn new_amp_client(&self) -> AmpClient {
        let endpoint = self.ctx.daemon_server().flight_server_url();
        AmpClient::from_endpoint(&endpoint)
            .await
            .expect("Failed to create amp client")
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

// Helper types and functions

/// Stream records with resumption watermark.
#[derive(Debug)]
struct Records {
    records: Vec<BlockRow>,
    watermark: ResumeWatermark,
}

/// Stream blocks from client with optional resumption watermark.
async fn stream_blocks(
    client: &mut AmpClient,
    query: &str,
    latest_block: BlockNum,
    resume_watermark: Option<&ResumeWatermark>,
) -> Records {
    tracing::debug!(
        "Stream blocks with resume watermark: {:?}",
        resume_watermark
    );

    let mut stream = client
        .request(query, resume_watermark, true)
        .await
        .expect("Failed to create client query stream");
    let mut records: Vec<BlockRow> = Default::default();

    while let Some(result) = stream.next().await {
        let batch = result.expect("Failed to get batch result from stream");
        let block_num_array = batch
            .data
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .expect("Failed to downcast to UInt64Array");
        let hash_array = batch
            .data
            .column(1)
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .expect("Failed to downcast hash column to FixedSizeBinaryArray");
        let parent_hash_array = batch
            .data
            .column(2)
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .expect("Failed to downcast parent_hash column to FixedSizeBinaryArray");

        for i in 0..batch.data.num_rows() {
            records.push(BlockRow {
                block_num: block_num_array.value(i),
                hash: BlockHash::from_slice(hash_array.value(i)),
                parent_hash: BlockHash::from_slice(parent_hash_array.value(i)),
            });
        }

        let end_block = batch.metadata.ranges[0].end();
        let watermark = ResumeWatermark::from_ranges(&batch.metadata.ranges);
        if end_block == latest_block {
            return Records { records, watermark };
        }
    }

    unreachable!("ran out of response batches")
}
