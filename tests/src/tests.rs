use crate::test_support::{bless, check_blocks, check_provider_file};
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

    check_blocks(dataset_name, 15_000_000, 15_000_000)
        .await
        .unwrap();
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

    check_blocks(dataset_name, 15_000_000, 15_000_000)
        .await
        .unwrap();
}
