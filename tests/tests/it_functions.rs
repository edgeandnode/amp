use monitoring::logging;
use tests::{run_spec, testlib::ctx::TestCtxBuilder};

#[tokio::test]
async fn basic_function() {
    logging::init();

    let test_ctx = TestCtxBuilder::new("basic_function")
        .with_dataset_manifest("eth_firehose")
        .with_dataset_snapshot("eth_firehose")
        .with_provider_config("firehose_eth_mainnet")
        .build()
        .await
        .expect("Failed to create test environment");
    let mut client = test_ctx
        .new_flight_client()
        .await
        .expect("Failed to connect FlightClient");

    run_spec!("basic-function", (&test_ctx, &mut client));
}
