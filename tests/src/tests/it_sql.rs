use monitoring::logging;

use crate::{steps::load_test_steps, testlib::ctx::TestCtxBuilder};

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

    for step in load_test_steps("sql-tests.yaml").unwrap() {
        step.run(&test_ctx, &mut client).await.unwrap()
    }
}
