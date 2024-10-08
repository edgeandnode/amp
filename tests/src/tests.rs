use crate::test_support::{bless, check_blocks};
use common::tracing;
use log::warn;

#[tokio::test]
async fn evm_rpc_single() {
    let dataset_name = "eth_rpc";
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
    tracing::register_logger();

    if std::env::var("NOZZLE_TESTS_BLESS").is_ok() {
        bless(&dataset_name, 15_000_000, 15_000_000).await.unwrap();

        warn!("wrote new blessed dataset for {dataset_name}");
    }

    check_blocks(dataset_name, 15_000_000, 15_000_000)
        .await
        .unwrap();
}
