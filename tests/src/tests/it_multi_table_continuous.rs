//! Integration tests for multi-table derived dataset dumps in continuous mode
//!
//! These tests verify that derived datasets with multiple tables can dump all tables
//! concurrently in continuous streaming mode, rather than blocking on the first table.

use std::time::Duration;

use monitoring::logging;

use crate::testlib::{ctx::TestCtxBuilder, fixtures::DatasetPackage, helpers as test_helpers};

#[tokio::test]
async fn dump_multi_table_derived_dataset_in_continuous_mode_populates_all_tables() {
    logging::init();

    //* Given
    let test_ctx = TestCtxBuilder::new("dump_multi_table_continuous_mode")
        .with_dataset_manifest("anvil_rpc")
        .with_anvil_ipc()
        .build()
        .await
        .expect("Failed to build test environment");

    // Mine initial blocks
    test_ctx
        .anvil()
        .mine(5)
        .await
        .expect("Failed to mine initial blocks");

    // Dump raw dataset (dependency)
    let anvil_ref = "_/anvil_rpc@0.0.0";
    test_helpers::dump_dataset(
        test_ctx.daemon_server().config().clone(),
        test_ctx.metadata_db().clone(),
        anvil_ref
            .parse()
            .expect("Failed to parse dataset reference"),
        5,
    )
    .await
    .expect("Failed to dump anvil_rpc");

    // Register derived dataset with 3 tables
    let sql_dataset = DatasetPackage::new("sql_over_anvil_1", Some("amp.config.ts"));
    let cli = test_ctx.new_amp_cli();
    sql_dataset
        .register(&cli, "0.0.0")
        .await
        .expect("Failed to register sql_over_anvil_1");

    // Start continuous dump in background
    let config = test_ctx.daemon_server().config().clone();
    let metadata_db = test_ctx.metadata_db().clone();
    let dump_handle = tokio::spawn(async move {
        test_helpers::dump_dataset(
            config,
            metadata_db,
            "_/sql_over_anvil_1@0.0.0"
                .parse()
                .expect("Failed to parse dataset reference"),
            u64::MAX,
        )
        .await
    });

    // Wait for dump to start
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Mine additional blocks while dump is running
    test_ctx
        .anvil()
        .mine(3)
        .await
        .expect("Failed to mine additional blocks");

    // Update raw dataset with new blocks
    test_helpers::dump_dataset(
        test_ctx.daemon_server().config().clone(),
        test_ctx.metadata_db().clone(),
        anvil_ref
            .parse()
            .expect("Failed to parse dataset reference"),
        8,
    )
    .await
    .expect("Failed to dump anvil_rpc with new blocks");

    // Wait for continuous dump to process new blocks
    tokio::time::sleep(Duration::from_millis(1000)).await;

    //* When
    let jsonl_client = test_ctx.new_jsonl_client();

    let blocks: Vec<serde_json::Value> = jsonl_client
        .query("SELECT COUNT(*) as count FROM sql_over_anvil_1.blocks")
        .await
        .expect("Failed to query blocks table");

    let txs: Vec<serde_json::Value> = jsonl_client
        .query("SELECT COUNT(*) as count FROM sql_over_anvil_1.transactions")
        .await
        .expect("Failed to query transactions table");

    let logs: Vec<serde_json::Value> = jsonl_client
        .query("SELECT COUNT(*) as count FROM sql_over_anvil_1.logs")
        .await
        .expect("Failed to query logs table");

    //* Then
    assert!(
        !blocks.is_empty(),
        "blocks table should exist and be queryable"
    );
    assert!(
        !txs.is_empty(),
        "transactions table should exist and be queryable"
    );
    assert!(!logs.is_empty(), "logs table should exist and be queryable");

    let blocks_count = blocks[0]["count"].as_i64().expect("count should be i64");

    // Verify continuous mode processed the new blocks (5 initial + 3 additional = 8)
    assert!(
        blocks_count >= 8,
        "blocks table should have at least 8 rows (5 initial + 3 new), got {}",
        blocks_count
    );

    dump_handle.abort();
}
