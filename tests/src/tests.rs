use std::sync::LazyLock;

use crate::{
    temp_metadata_db::test_metadata_db,
    test_support::{check_blocks, check_provider_file, SnapshotContext},
};
use common::tracing;

static KEEP_TEMP_DIRS: LazyLock<bool> = LazyLock::new(|| std::env::var("KEEP_TEMP_DIRS").is_ok());

#[tokio::test]
async fn evm_rpc_single() {
    let dataset_name = "eth_rpc";
    check_provider_file("rpc_eth_mainnet.toml").await;
    tracing::register_logger();

    let metadata_db = test_metadata_db(*KEEP_TEMP_DIRS).await;
    let blessed = SnapshotContext::blessed(&dataset_name).await.unwrap();

    // Check the dataset directly against the RPC provider with `check_blocks`.
    check_blocks(dataset_name, 15_000_000, 15_000_000)
        .await
        .expect("blessed data differed from provider");

    // Now dump the dataset to a temporary directory and check it again against the blessed files.
    let temp_dump = SnapshotContext::temp_dump(
        &dataset_name,
        vec![],
        15_000_000,
        15_000_000,
        1,
        Some(metadata_db),
        *KEEP_TEMP_DIRS,
    )
    .await
    .expect("temp dump failed");
    temp_dump
        .assert_eq(&blessed, Some(&*metadata_db))
        .await
        .unwrap();
}

#[tokio::test]
async fn eth_firehose_single() {
    let dataset_name = "eth_firehose";
    check_provider_file("firehose_eth_mainnet.toml").await;
    tracing::register_logger();

    let blessed = SnapshotContext::blessed(&dataset_name).await.unwrap();

    // Check the dataset directly against the Firehose provider with `check_blocks`.
    check_blocks(dataset_name, 15_000_000, 15_000_000)
        .await
        .expect("blessed data differed from provider");

    // Now dump the dataset to a temporary directory and check it again against the blessed files.
    let temp_dump = SnapshotContext::temp_dump(
        &dataset_name,
        vec![],
        15_000_000,
        15_000_000,
        1,
        None,
        *KEEP_TEMP_DIRS,
    )
    .await
    .expect("temp dump failed");
    temp_dump.assert_eq(&blessed, None).await.unwrap();
}

#[tokio::test]
async fn sql_over_eth_firehose_dump() {
    let dataset_name = "sql_over_eth_firehose";
    tracing::register_logger();

    let blessed = SnapshotContext::blessed(&dataset_name).await.unwrap();

    // Now dump the dataset to a temporary directory and check blessed files against it.
    let temp_dump = SnapshotContext::temp_dump(
        &dataset_name,
        vec!["eth_firehose"],
        15_000_000,
        15_000_000,
        2,
        None,
        *KEEP_TEMP_DIRS,
    )
    .await
    .expect("temp dump failed");
    blessed.assert_eq(&temp_dump, None).await.unwrap();
}
