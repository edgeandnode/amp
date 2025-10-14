use monitoring::logging;
use crate::{run_spec, testlib::ctx::TestCtxBuilder};

#[tokio::test(flavor = "multi_thread")]
async fn non_incremental_tests() {
    logging::init();
    let test_ctx = TestCtxBuilder::new("non_incremental_tests")
        .with_dataset_manifest("eth_rpc")
        .with_dataset_snapshot("eth_rpc")
        .with_provider_config("rpc_eth_mainnet")
        .build()
        .await
        .expect("Failed to create test environment");
    let mut client = test_ctx
        .new_flight_client()
        .await
        .expect("Failed to connect FlightClient");

    run_spec!("non-incremental-tests", (&test_ctx, &mut client));
}
