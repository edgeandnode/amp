use common::BoxError;
use monitoring::logging;

use crate::{run_spec, testlib::ctx::TestCtxBuilder};

#[tokio::test]
async fn basic_function() -> Result<(), BoxError> {
    logging::init();

    let test_ctx = TestCtxBuilder::new("basic_function")
        .with_dataset_manifests(["basic_function", "basic_function__0_1_0", "eth_firehose"])
        .with_dataset_snapshots(["eth_firehose"])
        .with_provider_configs(["firehose_eth_mainnet"])
        .build()
        .await
        .expect("Failed to create test environment");
    let mut client = test_ctx
        .new_flight_client()
        .await
        .expect("Failed to connect FlightClient");

    run_spec!("basic-function", (&test_ctx, &mut client));

    Ok(())
}
