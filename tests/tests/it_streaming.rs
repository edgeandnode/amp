use monitoring::logging;
use tests::{run_spec, testlib::ctx::TestCtxBuilder};

#[tokio::test(flavor = "multi_thread")]
async fn streaming_tests_basic() {
    logging::init();
    let test_ctx = TestCtxBuilder::new("sql_streaming_tests_basic")
        .with_dataset_manifests(["eth_rpc"])
        .with_dataset_snapshots(["eth_rpc"])
        .with_provider_configs(["rpc_eth_mainnet"])
        .build()
        .await
        .expect("Failed to create test environment");
    let mut client = test_ctx
        .new_flight_client()
        .await
        .expect("Failed to connect FlightClient");

    run_spec!("sql-streaming-tests-basic", (&test_ctx, &mut client));
}

#[tokio::test(flavor = "multi_thread")]
async fn streaming_tests_with_sql_datasets() {
    logging::init();
    let test_ctx = TestCtxBuilder::new("sql_streaming_tests_with_sql_datasets")
        .with_provider_config("rpc_eth_mainnet")
        .with_dataset_manifests(["eth_rpc"])
        .with_dataset_snapshots(["eth_rpc"])
        .build()
        .await
        .expect("Failed to create test environment");
    let mut client = test_ctx
        .new_flight_client()
        .await
        .expect("Failed to connect FlightClient");

    run_spec!(
        "sql-streaming-tests-with-sql-datasets",
        (&test_ctx, &mut client)
    );
}
