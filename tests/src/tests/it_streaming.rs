use monitoring::logging;

use crate::{steps::load_test_steps, testlib::ctx::TestCtxBuilder};

#[tokio::test(flavor = "multi_thread")]
async fn streaming_tests_basic() {
    logging::init();
    let test_ctx = TestCtxBuilder::new("sql_streaming_tests_basic")
        .with_dataset_manifests(["eth_firehose", "eth_firehose_stream"])
        .with_dataset_snapshots(["eth_firehose"])
        .with_provider_configs(["firehose_eth_mainnet"])
        .build()
        .await
        .expect("Failed to create test environment");
    let mut client = test_ctx
        .new_flight_client()
        .await
        .expect("Failed to connect FlightClient");

    for step in load_test_steps("sql-streaming-tests-basic.yaml").unwrap() {
        step.run(&test_ctx, &mut client).await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn streaming_tests_with_sql_datasets() {
    logging::init();
    let test_ctx = TestCtxBuilder::new("sql_streaming_tests_with_sql_datasets")
        .with_provider_config("firehose_eth_mainnet")
        .with_dataset_manifests(["eth_firehose", "sql_stream_ds"])
        .with_sql_dataset_files([
            "sql_stream_ds/even_blocks",
            "sql_stream_ds/even_blocks_hashes_only",
        ])
        .with_dataset_snapshots(["eth_firehose"])
        .build()
        .await
        .expect("Failed to create test environment");
    let mut client = test_ctx
        .new_flight_client()
        .await
        .expect("Failed to connect FlightClient");

    for step in load_test_steps("sql-streaming-tests-with-sql-datasets.yaml").unwrap() {
        step.run(&test_ctx, &mut client).await.unwrap();
    }
}
