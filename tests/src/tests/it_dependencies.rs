use std::time::Duration;

use common::BoxError;
use monitoring::logging;

use crate::{run_spec, testlib::ctx::TestCtxBuilder};

#[tokio::test]
async fn intra_deps_test() -> Result<(), BoxError> {
    logging::init();

    let test_ctx = TestCtxBuilder::new("intra_deps_test")
        .with_dataset_manifests(["intra_deps", "intra_deps__0_1_0", "eth_firehose"])
        .with_dataset_snapshots(["eth_firehose"])
        .with_provider_configs(["firehose_eth_mainnet"])
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
    Ok(())
}

#[tokio::test]
async fn multi_version_test() -> Result<(), BoxError> {
    logging::init();

    let test_ctx = TestCtxBuilder::new("multi_version_test")
        .with_dataset_manifests([
            "multi_version__0_0_1",
            "multi_version__0_0_2",
            "eth_firehose",
        ])
        .with_dataset_snapshots(["eth_firehose"])
        .with_provider_configs(["firehose_eth_mainnet"])
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
    Ok(())
}
