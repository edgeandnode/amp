use alloy::primitives::BlockHash;
use amp_client::{InMemoryResumeStore, ResumeStore, SqlClient};
use common::{
    BlockNum,
    arrow::array::{FixedSizeBinaryArray, UInt64Array},
};
use futures::StreamExt;
use monitoring::logging;

use crate::testlib::{self, helpers as test_helpers};

#[tokio::test]
async fn resume_store_simple_streaming_resumption() {
    let test = TestCtx::setup("resume_store_streaming", "anvil_rpc").await;
    let query = "SELECT block_num, hash FROM anvil_rpc.blocks SETTINGS stream = true";
    let mut client = test.client().await;
    let store = InMemoryResumeStore::new();
    let id = "streaming_query";

    // Start streaming query and mine 5 blocks
    test.dump("anvil_rpc", 0).await; // Dump genesis block

    let stream = client
        .query(query, None, None)
        .await
        .expect("Failed to create query stream");
    let reorg_stream = amp_client::with_reorg(stream);
    let mut resumable_stream = amp_client::with_resume_store(id, store.clone(), reorg_stream);

    // Mine 5 blocks while stream is active
    for i in 1..=5 {
        test.mine(1).await;
        test.dump("anvil_rpc", i).await;
    }

    // Consume blocks from stream
    let mut blocks_phase1 = vec![];
    let mut saw_watermark = false;
    let mut batch_count = 0;

    while let Some(result) = resumable_stream.next().await {
        match result.expect("Failed to get response batch") {
            amp_client::ResponseBatchWithReorg::Batch { data, .. } => {
                blocks_phase1.extend(extract_blocks(&data));
                batch_count += 1;
            }
            amp_client::ResponseBatchWithReorg::Watermark(_) => {
                saw_watermark = true;
                // After receiving watermark for block 5, we have all blocks
                if blocks_phase1.len() >= 6 {
                    // genesis + 5 mined blocks
                    break;
                }
            }
            amp_client::ResponseBatchWithReorg::Reorg { .. } => {}
        }

        // Safety: break after reasonable number of batches
        if batch_count > 20 {
            break;
        }
    }

    // Verify we received blocks [0, 1, 2, 3, 4, 5]
    assert!(saw_watermark, "Should have received watermark");
    assert_eq!(
        blocks_phase1.len(),
        6,
        "Should have received 6 blocks (genesis + 5 mined)"
    );
    assert_eq!(blocks_phase1[0].block_num, 0);
    assert_eq!(blocks_phase1[5].block_num, 5);

    // Verify watermark is stored and points to block 5
    let stored_watermark = store
        .get_watermark(id)
        .await
        .expect("Failed to get watermark")
        .expect("Watermark should be stored");
    assert_eq!(
        stored_watermark.0["anvil"].number, 5,
        "Watermark should be at block 5"
    );

    // Drop the first stream (simulating disconnect)
    drop(resumable_stream);

    // Start second stream with stored watermark
    let stream = client
        .query(query, None, Some(&stored_watermark))
        .await
        .expect("Failed to create resumed query stream");
    let reorg_stream = amp_client::with_reorg(stream);
    let mut resumable_stream = amp_client::with_resume_store(id, store.clone(), reorg_stream);

    // Mine 5 more blocks
    for i in 6..=10 {
        test.mine(1).await;
        test.dump("anvil_rpc", i).await;
    }

    // Consume blocks from resumed stream
    let mut blocks_phase2 = vec![];
    let mut saw_watermark_phase2 = false;
    let mut batch_count = 0;

    while let Some(result) = resumable_stream.next().await {
        match result.expect("Failed to get response batch") {
            amp_client::ResponseBatchWithReorg::Batch { data, .. } => {
                blocks_phase2.extend(extract_blocks(&data));
                batch_count += 1;
            }
            amp_client::ResponseBatchWithReorg::Watermark(_) => {
                saw_watermark_phase2 = true;
                // After receiving watermark for block 10, we have all new blocks
                if blocks_phase2.len() >= 5 {
                    break;
                }
            }
            amp_client::ResponseBatchWithReorg::Reorg { .. } => {}
        }

        // Safety: break after reasonable number of batches
        if batch_count > 20 {
            break;
        }
    }

    // Verify we only received NEW blocks [6, 7, 8, 9, 10]
    assert!(saw_watermark_phase2, "Should have received watermark");
    assert_eq!(
        blocks_phase2.len(),
        5,
        "Should have received only 5 new blocks"
    );
    assert_eq!(blocks_phase2[0].block_num, 6, "First new block should be 6");
    assert_eq!(
        blocks_phase2[4].block_num, 10,
        "Last new block should be 10"
    );

    // Verify watermark was updated to block 10
    let final_watermark = store
        .get_watermark(id)
        .await
        .expect("Failed to get final watermark")
        .expect("Final watermark should be stored");
    assert_eq!(
        final_watermark.0["anvil"].number, 10,
        "Watermark should be updated to block 10"
    );
}

#[tokio::test]
async fn resume_store_captures_watermarks_and_enables_resumption() {
    let test = TestCtx::setup("resume_store_basic", "anvil_rpc").await;
    let query = "SELECT block_num, hash, parent_hash FROM anvil_rpc.blocks SETTINGS stream = true";
    let mut client = test.client().await;
    let store = InMemoryResumeStore::new();
    let id = "test_query";

    // Stream initial blocks and capture watermark
    test.mine(2).await;
    test.dump("anvil_rpc", 2).await;

    {
        let stream = client
            .query(query, None, None)
            .await
            .expect("Failed to create query stream");
        let reorg_stream = amp_client::with_reorg(stream);
        let mut resumable_stream = amp_client::with_resume_store(id, store.clone(), reorg_stream);

        let mut blocks_phase1 = vec![];
        let mut saw_watermark = false;
        while let Some(result) = resumable_stream.next().await {
            match result.expect("Failed to get response batch") {
                amp_client::ResponseBatchWithReorg::Batch { data, .. } => {
                    blocks_phase1.extend(extract_blocks(&data));
                }
                amp_client::ResponseBatchWithReorg::Watermark(_) => {
                    // Watermark is automatically stored by with_resume_store
                    saw_watermark = true;
                    // Break after receiving watermark for block 2
                    if blocks_phase1.last().map(|b| b.block_num) == Some(2) {
                        break;
                    }
                }
                amp_client::ResponseBatchWithReorg::Reorg { .. } => {
                    panic!("Unexpected reorg");
                }
            }
        }
        assert!(saw_watermark, "Should have received at least one watermark");

        // Verify we received blocks [0, 1, 2]
        assert_eq!(blocks_phase1.len(), 3);
        assert_eq!(blocks_phase1[0].block_num, 0);
        assert_eq!(blocks_phase1[1].block_num, 1);
        assert_eq!(blocks_phase1[2].block_num, 2);
    }
    // Stream dropped here (simulating disconnect)

    // Verify watermark was automatically captured
    let stored_watermark = store
        .get_watermark(id)
        .await
        .expect("Failed to get watermark")
        .expect("Watermark should be stored");
    // Network name is "anvil", not dataset name "anvil_rpc"
    assert!(stored_watermark.0.contains_key("anvil"));

    // Mine more blocks and resume from stored watermark
    test.mine(2).await;
    test.dump("anvil_rpc", 4).await;

    {
        let stream = client
            .query(query, None, Some(&stored_watermark))
            .await
            .expect("Failed to create resumed query stream");
        let reorg_stream = amp_client::with_reorg(stream);
        let mut resumable_stream = amp_client::with_resume_store(id, store.clone(), reorg_stream);

        let mut blocks_phase2 = vec![];
        while let Some(result) = resumable_stream.next().await {
            match result.expect("Failed to get response batch") {
                amp_client::ResponseBatchWithReorg::Batch { data, .. } => {
                    blocks_phase2.extend(extract_blocks(&data));
                }
                amp_client::ResponseBatchWithReorg::Watermark(_) => {
                    // Watermark updated automatically
                    // Break after receiving watermark for block 4
                    if blocks_phase2.last().map(|b| b.block_num) == Some(4) {
                        break;
                    }
                }
                amp_client::ResponseBatchWithReorg::Reorg { .. } => {
                    panic!("Unexpected reorg");
                }
            }
        }

        // Verify we only received NEW blocks [3, 4], not [0, 1, 2] again
        assert_eq!(blocks_phase2.len(), 2);
        assert_eq!(blocks_phase2[0].block_num, 3);
        assert_eq!(blocks_phase2[1].block_num, 4);
    }
}

#[tokio::test]
async fn resume_store_handles_multiple_queries() {
    let test = TestCtx::setup("resume_store_multi", "anvil_rpc").await;
    let query = "SELECT block_num, hash FROM anvil_rpc.blocks SETTINGS stream = true";
    let mut client = test.client().await;
    let store = InMemoryResumeStore::new();

    // Mine and dump initial blocks
    test.mine(2).await;
    test.dump("anvil_rpc", 2).await;

    // Capture watermark for first query
    {
        let stream = client
            .query(query, None, None)
            .await
            .expect("Failed to create query stream");
        let reorg_stream = amp_client::with_reorg(stream);
        let mut resumable_stream =
            amp_client::with_resume_store("query_1", store.clone(), reorg_stream);

        let mut saw_watermark = false;
        let mut batch_count = 0;
        while let Some(result) = resumable_stream.next().await {
            match result.expect("Failed to get response batch") {
                amp_client::ResponseBatchWithReorg::Batch { .. } => {
                    batch_count += 1;
                }
                amp_client::ResponseBatchWithReorg::Watermark(_) => {
                    saw_watermark = true;
                    break;
                }
                amp_client::ResponseBatchWithReorg::Reorg { .. } => {}
            }
            if batch_count > 10 {
                break;
            }
        }
        assert!(saw_watermark, "should receive watermark");
    }

    // Capture watermark for second query (same data)
    {
        let stream = client
            .query(query, None, None)
            .await
            .expect("Failed to create query stream");
        let reorg_stream = amp_client::with_reorg(stream);
        let mut resumable_stream =
            amp_client::with_resume_store("query_2", store.clone(), reorg_stream);

        let mut saw_watermark = false;
        let mut batch_count = 0;
        while let Some(result) = resumable_stream.next().await {
            match result.expect("Failed to get response batch") {
                amp_client::ResponseBatchWithReorg::Batch { .. } => {
                    batch_count += 1;
                }
                amp_client::ResponseBatchWithReorg::Watermark(_) => {
                    saw_watermark = true;
                    break;
                }
                amp_client::ResponseBatchWithReorg::Reorg { .. } => {}
            }
            if batch_count > 10 {
                break;
            }
        }
        assert!(saw_watermark, "should receive watermark");
    }

    // Verify both watermarks are stored independently
    let watermark1 = store
        .get_watermark("query_1")
        .await
        .expect("Failed to get watermark 1")
        .expect("Watermark 1 should be stored");

    let watermark2 = store
        .get_watermark("query_2")
        .await
        .expect("Failed to get watermark 2")
        .expect("Watermark 2 should be stored");

    // Both should have anvil network watermarks
    assert!(
        watermark1.0.contains_key("anvil"),
        "Watermark 1 should have anvil"
    );
    assert!(
        watermark2.0.contains_key("anvil"),
        "Watermark 2 should have anvil"
    );

    // Mine more blocks and update first query's watermark
    test.mine(1).await;
    test.dump("anvil_rpc", 3).await;

    {
        let watermark1_before = watermark1.clone();
        let stream = client
            .query(query, None, Some(&watermark1_before))
            .await
            .expect("Failed to create query stream");
        let reorg_stream = amp_client::with_reorg(stream);
        let mut resumable_stream =
            amp_client::with_resume_store("query_1", store.clone(), reorg_stream);

        let mut saw_watermark = false;
        let mut batch_count = 0;
        while let Some(result) = resumable_stream.next().await {
            match result.expect("Failed to get response batch") {
                amp_client::ResponseBatchWithReorg::Batch { .. } => {
                    batch_count += 1;
                }
                amp_client::ResponseBatchWithReorg::Watermark(_) => {
                    saw_watermark = true;
                    break;
                }
                amp_client::ResponseBatchWithReorg::Reorg { .. } => {}
            }
            if batch_count > 10 {
                break;
            }
        }
        assert!(saw_watermark, "should receive updated watermark");
    }

    // Verify first query's watermark was updated but second query's wasn't
    let watermark1_after = store
        .get_watermark("query_1")
        .await
        .expect("Failed to get watermark 1 after")
        .expect("Watermark 1 should still be stored");

    let watermark2_unchanged = store
        .get_watermark("query_2")
        .await
        .expect("Failed to get watermark 2 unchanged")
        .expect("Watermark 2 should still be stored");

    // s block number should have increased
    assert!(
        watermark1_after.0["anvil"].number > watermark1.0["anvil"].number,
        "watermark should have advanced"
    );

    // s watermark should be unchanged
    assert_eq!(
        watermark2_unchanged.0["anvil"].number, watermark2.0["anvil"].number,
        "watermark should be unchanged"
    );
}

/// Test context wrapper for resume store tests.
struct TestCtx {
    ctx: testlib::ctx::TestCtx,
}

impl TestCtx {
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

    async fn mine(&self, count: u64) {
        self.ctx
            .anvil()
            .mine(count)
            .await
            .expect("Failed to mine blocks");
    }

    async fn dump(&self, dataset: &str, end: BlockNum) {
        test_helpers::dump_dataset(
            self.ctx.daemon_server().config(),
            self.ctx.metadata_db(),
            dataset,
            end,
            1,
            None,
        )
        .await
        .expect("Failed to dump dataset");
    }

    async fn client(&self) -> SqlClient {
        let endpoint = self.ctx.daemon_server().flight_server_url();
        SqlClient::new(&endpoint)
            .await
            .expect("Failed to create amp client")
    }
}

#[derive(Debug, PartialEq, Eq)]
struct BlockRow {
    block_num: BlockNum,
    hash: BlockHash,
}

/// Extract block rows from a RecordBatch.
fn extract_blocks(batch: &common::arrow::array::RecordBatch) -> Vec<BlockRow> {
    let block_num_array = batch
        .column(0)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .expect("Failed to downcast to UInt64Array");
    let hash_array = batch
        .column(1)
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .expect("Failed to downcast to FixedSizeBinaryArray");

    (0..batch.num_rows())
        .map(|i| BlockRow {
            block_num: block_num_array.value(i),
            hash: BlockHash::from_slice(hash_array.value(i)),
        })
        .collect()
}
