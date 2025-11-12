use monitoring::logging;

use crate::{steps::run_spec, testlib::ctx::TestCtxBuilder};

#[tokio::test]
async fn joins_tests() {
    logging::init();

    let test_ctx = TestCtxBuilder::new("joins_tests")
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

    run_spec("joins-tests", &test_ctx, &mut client, None).await;
}
