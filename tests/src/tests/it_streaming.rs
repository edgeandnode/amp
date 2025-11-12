use monitoring::logging;

use crate::{
    steps::run_spec,
    testlib::{ctx::TestCtxBuilder, helpers as test_helpers},
};

#[tokio::test(flavor = "multi_thread")]
async fn streaming_tests_basic() {
    logging::init();
    let metrics = test_helpers::create_test_metrics_context();
    let test_ctx = TestCtxBuilder::new("sql_streaming_tests_basic")
        .with_dataset_manifests(["eth_rpc"])
        .with_dataset_snapshots(["eth_rpc"])
        .with_provider_configs(["rpc_eth_mainnet"])
        .with_meter(metrics.create_meter())
        .build()
        .await
        .expect("Failed to create test environment");
    let mut client = test_ctx
        .new_flight_client()
        .await
        .expect("Failed to connect FlightClient");

    run_spec("sql-streaming-tests-basic", &test_ctx, &mut client, None)
        .await
        .expect("Failed to run spec");
}

#[tokio::test(flavor = "multi_thread")]
async fn streaming_tests_with_sql_datasets() {
    logging::init();
    let metrics = test_helpers::create_test_metrics_context();
    let test_ctx = TestCtxBuilder::new("sql_streaming_tests_with_sql_datasets")
        .with_provider_config("rpc_eth_mainnet")
        .with_dataset_manifests(["eth_rpc"])
        .with_dataset_snapshots(["eth_rpc"])
        .with_meter(metrics.create_meter())
        .build()
        .await
        .expect("Failed to create test environment");
    let mut client = test_ctx
        .new_flight_client()
        .await
        .expect("Failed to connect FlightClient");

    run_spec(
        "sql-streaming-tests-with-sql-datasets",
        &test_ctx,
        &mut client,
        None,
    )
    .await
    .expect("Failed to run spec");
}
