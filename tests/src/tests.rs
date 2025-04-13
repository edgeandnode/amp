use std::sync::{Arc, LazyLock};

use crate::{
    temp_metadata_db::test_metadata_db,
    test_support::{check_blocks, check_provider_file, SnapshotContext},
};
use common::{
    meta_tables::scanned_ranges::ScannedRange,
    parquet::arrow::async_reader::{AsyncFileReader, ParquetObjectReader},
    tracing,
};
use futures::StreamExt;
use metadata_db::{FileMetadata, TableId};
use object_store::local::LocalFileSystem;

static KEEP_TEMP_DIRS: LazyLock<bool> = LazyLock::new(|| std::env::var("KEEP_TEMP_DIRS").is_ok());

#[tokio::test]
async fn evm_rpc_single() {
    let dataset_name = "eth_rpc";
    check_provider_file("rpc_eth_mainnet.toml").await;
    tracing::register_logger();

    let metadata_db = test_metadata_db(*KEEP_TEMP_DIRS).await;
    let blessed = SnapshotContext::blessed(&dataset_name).await.unwrap();
    let store = Arc::new(LocalFileSystem::new());
    // Check the dataset directly against the RPC provider with `check_blocks`.
    check_blocks(dataset_name, 15_000_000, 15_000_000)
        .await
        .expect("blessed data differed from provider");

    // Now dump the dataset to a temporary directory and check it again against the blessed files.
    let temp_dump = SnapshotContext::temp_dump(
        &dataset_name,
        15_000_000,
        15_000_000,
        Some(metadata_db),
        *KEEP_TEMP_DIRS,
    )
    .await
    .expect("temp dump failed");

    temp_dump
        .assert_eq(&blessed, Some(&*metadata_db))
        .await
        .unwrap();

    let tbl = TableId {
        dataset: &dataset_name,
        dataset_version: None,
        table: "blocks",
    };
    let mut object_meta_stream = metadata_db.stream_nozzle_metadata(tbl);

    while let Some(Ok(FileMetadata {
        object_meta,
        range: (range_start, range_end),
        size_hint,
        ..
    })) = object_meta_stream.next().await
    {
        let mut reader = ParquetObjectReader::new(store.clone(), object_meta)
            .with_footer_size_hint(size_hint as usize);
        let metadata = reader.get_metadata().await.unwrap();
        let kv = metadata.file_metadata().key_value_metadata().unwrap();
        let nozzle_metadata: ScannedRange = serde_json::from_str(
            kv.into_iter()
                .find(|kv| kv.key == "nozzle_metadata")
                .expect("nozzle_metadata not found")
                .value
                .clone()
                .expect("nozzle_metadata value not found")
                .as_str(),
        )
        .expect("failed to deserialize nozzle_metadata");

        assert_eq!(nozzle_metadata.range_start, range_start as u64);
        assert_eq!(nozzle_metadata.range_end, range_end as u64);
    }
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
    let temp_dump =
        SnapshotContext::temp_dump(&dataset_name, 15_000_000, 15_000_000, None, *KEEP_TEMP_DIRS)
            .await
            .expect("temp dump failed");
    temp_dump.assert_eq(&blessed, None).await.unwrap();
}
