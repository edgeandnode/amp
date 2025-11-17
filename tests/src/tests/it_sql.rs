use monitoring::logging;

use crate::{steps::run_spec, testlib::ctx::TestCtxBuilder};

#[tokio::test]
async fn sql_tests() {
    logging::init();

    let test_ctx = TestCtxBuilder::new("sql_tests")
        .with_dataset_manifests(["eth_rpc", "eth_firehose"])
        .with_dataset_snapshots(["eth_rpc", "eth_firehose"])
        .with_provider_configs(["rpc_eth_mainnet", "firehose_eth_mainnet"])
        .build()
        .await
        .expect("Failed to create test environment");
    let mut client = test_ctx
        .new_flight_client()
        .await
        .expect("Failed to connect FlightClient");

    run_spec("sql-tests", &test_ctx, &mut client, None)
        .await
        .expect("Failed to run spec");
}

#[tokio::test]
async fn sql_advanced_tests() {
    logging::init();

    let test_ctx = TestCtxBuilder::new("sql_advanced_tests")
        .with_dataset_manifests(["eth_rpc", "eth_firehose"])
        .with_dataset_snapshots(["eth_rpc", "eth_firehose"])
        .with_provider_configs(["rpc_eth_mainnet", "firehose_eth_mainnet"])
        .build()
        .await
        .expect("Failed to create test environment");
    let mut client = test_ctx
        .new_flight_client()
        .await
        .expect("Failed to connect FlightClient");

    run_spec("sql-advanced-tests", &test_ctx, &mut client, None)
        .await
        .expect("Failed to run spec");
}

#[tokio::test]
async fn sql_regression_tests() {
    logging::init();

    let test_ctx = TestCtxBuilder::new("sql_regression_tests")
        .with_dataset_manifests(["eth_firehose_evm_decode_empty"])
        .with_dataset_snapshots(["eth_firehose_evm_decode_empty"])
        .with_provider_configs(["firehose_eth_mainnet"])
        .build()
        .await
        .expect("Failed to create test environment");
    let mut client = test_ctx
        .new_flight_client()
        .await
        .expect("Failed to connect FlightClient");

    run_spec("sql-regression-tests", &test_ctx, &mut client, None)
        .await
        .expect("Failed to run spec");
}
