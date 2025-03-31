use crate::test_support::{check_blocks, check_provider_file, SnapshotContext};
use common::tracing;

#[tokio::test]
async fn evm_rpc_single() {
    let dataset_name = "eth_rpc";
    check_provider_file("rpc_eth_mainnet.toml").await;
    tracing::register_logger();

    let blessed = SnapshotContext::blessed(&dataset_name, None).await.unwrap();

    // Check the dataset directly against the RPC provider with `check_blocks`.
    check_blocks(dataset_name, 15_000_000, 15_000_000)
        .await
        .expect("blessed data differed from provider");

    // Now dump the dataset to a temporary directory and check it again against the blessed files.
    let temp_dump = SnapshotContext::temp_dump(&dataset_name, 15_000_000, 15_000_000, None)
        .await
        .expect("temp dump failed");
    temp_dump.assert_eq(&blessed).await.unwrap();
}

#[tokio::test]
async fn eth_firehose_single() {
    let dataset_name = "eth_firehose";
    check_provider_file("firehose_eth_mainnet.toml").await;
    tracing::register_logger();

    let blessed = SnapshotContext::blessed(&dataset_name, None).await.unwrap();

    // Check the dataset directly against the Firehose provider with `check_blocks`.
    check_blocks(dataset_name, 15_000_000, 15_000_000)
        .await
        .expect("blessed data differed from provider");

    // Now dump the dataset to a temporary directory and check it again against the blessed files.
    let temp_dump = SnapshotContext::temp_dump(&dataset_name, 15_000_000, 15_000_000, None)
        .await
        .expect("temp dump failed");
    temp_dump.assert_eq(&blessed).await.unwrap();
}
