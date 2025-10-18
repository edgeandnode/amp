use std::collections::HashMap;

use amp_debezium_client::{DebeziumClient, DebeziumOp};
use common::BlockNum;
use futures::StreamExt;
use monitoring::logging;

use crate::testlib::{self, helpers as test_helpers};

#[tokio::test]
async fn debezium_stream_emits_create_events() {
    //* Given
    let test = TestCtx::setup("debezium_basic", "anvil_rpc").await;
    let last_block = 2;

    // Dump genesis block
    test.dump("anvil_rpc", 0).await;

    // Create Debezium client with streaming query
    let client = test.new_debezium_client().await;
    let query = "SELECT block_num, hash, parent_hash FROM anvil_rpc.blocks SETTINGS stream = true";
    let mut stream = client.stream(query).await.expect("Failed to create stream");

    //* When - Spawn task to consume stream, then mine/dump blocks
    let handle = tokio::spawn(async move {
        let mut records = Vec::new();
        while let Some(result) = stream.next().await {
            let batch = result.expect("Failed to get record batch");

            for record in batch {
                // Check block number to know when to stop
                let block_num = record
                    .after
                    .as_ref()
                    .and_then(|v| v.get("block_num"))
                    .and_then(|v| v.as_u64())
                    .unwrap();

                records.push(record);

                // Stop after receiving last expected block
                if block_num == last_block {
                    return records;
                }
            }
        }
        records
    });

    // Mine and dump blocks after stream is created
    test.mine(2).await;
    test.dump("anvil_rpc", 2).await;

    let records = handle.await.expect("Failed to await records task");

    //* Then
    assert_eq!(records.len(), 3, "Should receive 3 records");

    // All should be create operations
    for record in &records {
        assert_eq!(
            record.op,
            DebeziumOp::Create,
            "All records should be creates"
        );
        assert!(record.before.is_none(), "Creates should have no 'before'");
        assert!(record.after.is_some(), "Creates should have 'after'");
    }

    // Verify JSON structure
    let first_record = &records[0];
    let after = first_record.after.as_ref().unwrap();
    assert!(
        after.get("block_num").is_some(),
        "Record should have block_num"
    );
    assert!(after.get("hash").is_some(), "Record should have hash");
    assert!(
        after.get("parent_hash").is_some(),
        "Record should have parent_hash"
    );
}

#[tokio::test]
async fn debezium_stream_emits_delete_events_on_reorg() {
    //* Given
    let test = TestCtx::setup("debezium_reorg", "anvil_rpc").await;
    let last_block = 3;

    // Dump genesis
    test.dump("anvil_rpc", 0).await;

    // Create Debezium client with streaming query
    let client = test.new_debezium_client().await;
    let query = "SELECT block_num, hash, parent_hash FROM anvil_rpc.blocks SETTINGS stream = true";
    let mut stream = client.stream(query).await.expect("Failed to create stream");

    //* When - Spawn task to collect events, then mine/reorg
    let handle = tokio::spawn(async move {
        let mut all_records = Vec::new();

        while let Some(result) = stream.next().await {
            let batch = result.expect("Failed to get record batch");

            for record in batch {
                // Get block number (handle both create and delete)
                let block_num = if record.op == DebeziumOp::Delete {
                    record.before.as_ref()
                } else {
                    record.after.as_ref()
                }
                .and_then(|v| v.get("block_num"))
                .and_then(|v| v.as_u64())
                .unwrap();

                all_records.push(record);

                // Stop after we've seen the final block (after reorg)
                // We're looking for block 3 as a create (after reorg completes)
                if block_num >= last_block && all_records.last().unwrap().op == DebeziumOp::Create {
                    return all_records;
                }
            }
        }

        all_records
    });

    // Mine initial blocks
    test.mine(2).await;
    test.dump("anvil_rpc", 2).await;

    // Trigger reorg
    test.reorg(1).await; // Reorg block 2
    test.mine(1).await; // Mine new block 2'
    test.dump("anvil_rpc", 3).await;
    test.dump("anvil_rpc", 3).await; // Dump twice to ensure reorg is detected

    let all_records = handle.await.expect("Failed to await task");

    // Find the original block 2 hash from initial create events
    let original_block_2 = all_records
        .iter()
        .find(|r| {
            r.op == DebeziumOp::Create
                && r.after
                    .as_ref()
                    .and_then(|v| v.get("block_num"))
                    .and_then(|v| v.as_u64())
                    == Some(2)
        })
        .expect("Should find original block 2");

    let original_block_2_hash = original_block_2
        .after
        .as_ref()
        .unwrap()
        .get("hash")
        .unwrap()
        .as_str()
        .unwrap()
        .to_string();

    // Find delete event for block 2 specifically
    // (Note: batch-level granularity means blocks 1-2 dumped together are both retracted)
    let delete_event = all_records
        .iter()
        .find(|r| {
            r.op == DebeziumOp::Delete
                && r.before
                    .as_ref()
                    .and_then(|v| v.get("block_num"))
                    .and_then(|v| v.as_u64())
                    == Some(2)
        })
        .expect("Should have delete event for block 2");

    //* Then - Verify delete event
    assert_eq!(
        delete_event.op,
        DebeziumOp::Delete,
        "Should have delete operation"
    );
    assert!(delete_event.before.is_some(), "Delete should have 'before'");
    assert!(
        delete_event.after.is_none(),
        "Delete should have no 'after'"
    );

    // Verify deleted record matches original block 2
    let deleted_hash = delete_event
        .before
        .as_ref()
        .unwrap()
        .get("hash")
        .unwrap()
        .as_str()
        .unwrap();
    assert_eq!(
        deleted_hash, original_block_2_hash,
        "Deleted record should match original block 2"
    );

    // Find new create event for block 2' (after delete)
    let new_block_2 = all_records
        .iter()
        .rev() // Search from end
        .find(|r| {
            r.op == DebeziumOp::Create
                && r.after
                    .as_ref()
                    .and_then(|v| v.get("block_num"))
                    .and_then(|v| v.as_u64())
                    == Some(2)
        })
        .expect("Should find new block 2'");

    // Verify new block 2' has different hash
    let new_hash = new_block_2
        .after
        .as_ref()
        .unwrap()
        .get("hash")
        .unwrap()
        .as_str()
        .unwrap();
    assert_ne!(
        new_hash, original_block_2_hash,
        "New block should have different hash"
    );
}

#[tokio::test]
async fn debezium_stream_basic_functionality() {
    //* Given
    let test = TestCtx::setup("debezium_basic_func", "anvil_rpc").await;
    let last_block = 3;

    // Dump genesis
    test.dump("anvil_rpc", 0).await;

    // Create client
    let endpoint = test.ctx.daemon_server().flight_server_url();
    let amp_client = amp_client::AmpClient::from_endpoint(&endpoint)
        .await
        .expect("Failed to create amp client");
    let client = DebeziumClient::new(amp_client, None);

    let query = "SELECT block_num, hash, parent_hash FROM anvil_rpc.blocks SETTINGS stream = true";
    let mut stream = client.stream(query).await.expect("Failed to create stream");

    //* When - Spawn task, then mine blocks
    let handle = tokio::spawn(async move {
        let mut records = Vec::new();
        let mut seen_keys = HashMap::new();

        while let Some(Ok(batch)) = stream.next().await {
            for record in batch {
                // Track combinations of block_num + hash to verify data structure
                if let Some(after) = &record.after {
                    let block_num = after.get("block_num").unwrap().as_u64().unwrap();
                    let hash = after.get("hash").unwrap().as_str().unwrap().to_string();
                    let key = (block_num, hash);

                    if seen_keys.contains_key(&key) {
                        panic!("Duplicate key detected: {:?}", key);
                    }
                    seen_keys.insert(key, ());

                    records.push(record);

                    // Stop after we reach last block
                    if block_num >= last_block {
                        return (records, seen_keys);
                    }
                }
            }
        }
        (records, seen_keys)
    });

    // Mine blocks
    test.mine(3).await;
    test.dump("anvil_rpc", 3).await;

    let (records, seen_keys) = handle.await.expect("Failed to await task");

    //* Then
    assert!(!records.is_empty(), "Should receive some records");

    // In this test, blocks should be unique (each block emitted once)
    assert_eq!(
        records.len(),
        seen_keys.len(),
        "All records should be distinct"
    );
}

/// Test context wrapper for debezium client tests.
struct TestCtx {
    ctx: testlib::ctx::TestCtx,
}

impl TestCtx {
    /// Set up a new test context.
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
            self.ctx.daemon_server().config(),
            self.ctx.metadata_db(),
            format!("default/{}@dev", dataset).parse().unwrap(),
            end,
            1,
            None,
        )
        .await
        .expect("Failed to dump dataset");
    }

    /// Create a new DebeziumClient for this test context.
    async fn new_debezium_client(&self) -> DebeziumClient {
        let endpoint = self.ctx.daemon_server().flight_server_url();
        let amp_client = amp_client::AmpClient::from_endpoint(&endpoint)
            .await
            .expect("Failed to create amp client");

        DebeziumClient::new(amp_client, None)
    }
}
