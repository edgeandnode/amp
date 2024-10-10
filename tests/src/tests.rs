use crate::test_support::{
    assert_temp_eq_blessed, bless, check_blocks, check_provider_file, temp_dump,
};
use common::tracing;
use log::warn;

#[tokio::test]
async fn evm_rpc_single() {
    let dataset_name = "eth_rpc";
    check_provider_file("rpc_eth_mainnet.toml").await;
    tracing::register_logger();

    if std::env::var("NOZZLE_TESTS_BLESS").is_ok() {
        bless(dataset_name, 15_000_000, 15_000_000).await.unwrap();

        warn!("wrote new blessed dataset for {dataset_name}");
    }

    // Check the dataset directly against the RPC provider with `check_blocks`.
    check_blocks(dataset_name, 15_000_000, 15_000_000)
        .await
        .expect("blessed data differed from provider");

    // Now dump the dataset to a temporary directory and check it again against the blessed files.
    let temp_dataset_dump = temp_dump(&dataset_name, 15_000_000, 15_000_000)
        .await
        .expect("temp dump failed");
    assert_temp_eq_blessed(&temp_dataset_dump).await.unwrap();
}

#[tokio::test]
async fn eth_firehose_single() {
    let dataset_name = "eth_firehose";
    check_provider_file("firehose_eth_mainnet.toml").await;
    tracing::register_logger();

    if std::env::var("NOZZLE_TESTS_BLESS").is_ok() {
        bless(&dataset_name, 15_000_000, 15_000_000).await.unwrap();

        warn!("wrote new blessed dataset for {dataset_name}");
    }

    // Check the dataset directly against the Firehose provider with `check_blocks`.
    check_blocks(dataset_name, 15_000_000, 15_000_000)
        .await
        .expect("blessed data differed from provider");

    // Now dump the dataset to a temporary directory and check it again against the blessed files.
    let temp_dataset_dump = temp_dump(&dataset_name, 15_000_000, 15_000_000)
        .await
        .expect("temp dump failed");
    assert_temp_eq_blessed(&temp_dataset_dump).await.unwrap();
}
