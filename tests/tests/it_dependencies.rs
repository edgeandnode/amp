use std::time::Duration;

use monitoring::logging;
use tests::{run_spec, testlib::ctx::TestCtxBuilder};

#[tokio::test]
#[ignore = "The intra-deps resolution functionality is broken. Enable this test once fixed"]
async fn intra_deps_test() {
    logging::init();

    let test_ctx = TestCtxBuilder::new("intra_deps_test")
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

    run_spec!(
        "intra-deps",
        (&test_ctx, &mut client),
        delay = Duration::from_secs(1)
    );
}

#[tokio::test]
async fn multi_version_test() {
    logging::init();

    let test_ctx = TestCtxBuilder::new("multi_version_test")
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

    run_spec!(
        "multi-version",
        (&test_ctx, &mut client),
        delay = Duration::from_secs(1)
    );
}
