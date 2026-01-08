//! Integration tests for streaming joins with Anvil.
//!
//! These tests validate that streaming queries with JOINs correctly deliver
//! incremental results as new blocks are mined on Anvil.

use monitoring::logging;

use crate::{steps::run_spec, testlib::ctx::TestCtxBuilder};

#[tokio::test(flavor = "multi_thread")]
async fn streaming_join_self() {
    logging::init();

    let test_ctx = TestCtxBuilder::new("streaming_join_self")
        .with_anvil_ipc()
        .with_dataset_manifest("anvil_rpc")
        .build()
        .await
        .expect("Failed to create test environment");

    let mut client = test_ctx
        .new_flight_client()
        .await
        .expect("Failed to connect FlightClient");

    run_spec("streaming-join-anvil", &test_ctx, &mut client, None)
        .await
        .expect("Failed to run streaming join spec");
}

#[tokio::test(flavor = "multi_thread")]
async fn streaming_join_cross_table() {
    logging::init();

    let test_ctx = TestCtxBuilder::new("streaming_join_cross_table")
        .with_anvil_ipc()
        .with_dataset_manifest("anvil_rpc")
        .build()
        .await
        .expect("Failed to create test environment");

    let mut client = test_ctx
        .new_flight_client()
        .await
        .expect("Failed to connect FlightClient");

    run_spec("streaming-join-cross-table", &test_ctx, &mut client, None)
        .await
        .expect("Failed to run streaming cross-table join spec");
}

#[tokio::test(flavor = "multi_thread")]
async fn streaming_join_with_reorg() {
    logging::init();

    let test_ctx = TestCtxBuilder::new("streaming_join_with_reorg")
        .with_anvil_ipc()
        .with_dataset_manifest("anvil_rpc")
        .build()
        .await
        .expect("Failed to create test environment");

    let mut client = test_ctx
        .new_flight_client()
        .await
        .expect("Failed to connect FlightClient");

    run_spec("streaming-join-with-reorg", &test_ctx, &mut client, None)
        .await
        .expect("Failed to run streaming join with reorg spec");
}
