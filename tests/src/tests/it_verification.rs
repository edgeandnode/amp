use datasets_common::reference::Reference;
use monitoring::logging;

use crate::testlib::{ctx::TestCtxBuilder, helpers as test_helpers};

#[tokio::test]
async fn verify_eth_rpc_snapshot_block_15000000() {
    logging::init();

    let test_ctx = TestCtxBuilder::new("it_verification_eth_rpc")
        .with_dataset_manifest("eth_rpc")
        .with_dataset_snapshot("eth_rpc")
        .build()
        .await
        .expect("Failed to build test environment");

    let dataset_ref: Reference = "_/eth_rpc@0.0.0".parse().unwrap();
    let ampctl = test_ctx.new_ampctl();
    test_helpers::restore_dataset_snapshot(
        &ampctl,
        test_ctx.daemon_controller().dataset_store(),
        test_ctx.daemon_server().data_store(),
        &dataset_ref,
    )
    .await
    .expect("Failed to restore snapshot");

    let flight_url = test_ctx.daemon_server().flight_server_url();

    verification::run(verification::Args {
        amp_url: flight_url,
        amp_auth: String::new(),
        dataset: "_/eth_rpc".to_string(),
        start_block: 15_000_000,
        end_block: 15_000_000,
        parallelism: 1,
        rpc_url: None,
    })
    .await
    .expect("Verification failed");
}
