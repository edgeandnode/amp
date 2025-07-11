use std::ops::RangeInclusive;

use alloy::{
    primitives::BlockHash,
    providers::{DynProvider, Provider, ext::AnvilApi as _},
    transports::http::reqwest,
};
use common::{
    BlockNum, BoxError, metadata::segments::BlockRange, query_context::parse_sql, tracing_helpers,
};
use dataset_store::{DatasetDefsCommon, DatasetStore, SerializableSchema};
use generate_manifest;

use crate::{
    steps::load_test_steps,
    test_client::TestClient,
    test_support::{
        SnapshotContext, TestEnv, check_blocks, check_provider_file, restore_blessed_dataset,
        table_ranges,
    },
};

#[tokio::test]
async fn evm_rpc_single_dump() {
    tracing_helpers::register_logger();

    let dataset_name = "eth_rpc";
    check_provider_file("rpc_eth_mainnet.toml").await;

    let test_env = TestEnv::temp("evm_rpc_single_dump").await.unwrap();

    let blessed = SnapshotContext::blessed(&test_env, &dataset_name)
        .await
        .unwrap();

    // Check the dataset directly against the RPC provider with `check_blocks`.
    check_blocks(&test_env, dataset_name, 15_000_000, 15_000_000)
        .await
        .expect("blessed data differed from provider");

    // Now dump the dataset to a temporary directory and check it again against the blessed files.
    let temp_dump = SnapshotContext::temp_dump(&test_env, &dataset_name, 15_000_000, 15_000_000, 1)
        .await
        .expect("temp dump failed");
    temp_dump.assert_eq(&blessed).await.unwrap();
}

#[tokio::test]
async fn eth_firehose_single_dump() {
    tracing_helpers::register_logger();

    let dataset_name = "eth_firehose";
    check_provider_file("firehose_eth_mainnet.toml").await;

    let test_env = TestEnv::temp("eth_firehose_single_dump").await.unwrap();
    let blessed = SnapshotContext::blessed(&test_env, &dataset_name)
        .await
        .unwrap();

    // Check the dataset directly against the Firehose provider with `check_blocks`.
    check_blocks(&test_env, dataset_name, 15_000_000, 15_000_000)
        .await
        .expect("blessed data differed from provider");
    // Now dump the dataset to a temporary directory and check it again against the blessed files.
    let temp_dump = SnapshotContext::temp_dump(&test_env, &dataset_name, 15_000_000, 15_000_000, 1)
        .await
        .expect("temp dump failed");
    temp_dump.assert_eq(&blessed).await.unwrap();
}

#[tokio::test]
async fn sql_over_eth_firehose_dump() {
    tracing_helpers::register_logger();
    let dataset_name = "sql_over_eth_firehose";

    let test_env = TestEnv::temp("sql_over_eth_firehose").await.unwrap();
    let blessed = SnapshotContext::blessed(&test_env, &dataset_name)
        .await
        .unwrap();

    // Restore dependency
    restore_blessed_dataset("eth_firehose", &test_env.metadata_db)
        .await
        .unwrap();

    // Now dump the dataset to a temporary directory and check blessed files against it.
    let temp_dump = SnapshotContext::temp_dump(&test_env, &dataset_name, 15_000_000, 15_000_000, 2)
        .await
        .expect("temp dump failed");
    temp_dump.assert_eq(&blessed).await.unwrap();
}

#[tokio::test]
async fn sql_tests() {
    tracing_helpers::register_logger();
    let test_env = TestEnv::temp("sql_tests").await.unwrap();
    let mut client = TestClient::connect(&test_env).await.unwrap();

    for step in load_test_steps("sql-tests.yaml").unwrap() {
        step.run(&test_env, &mut client).await.unwrap()
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn streaming_tests() {
    tracing_helpers::register_logger();
    let test_env = TestEnv::temp("sql_streaming_tests").await.unwrap();
    let mut client = TestClient::connect(&test_env).await.unwrap();

    for step in load_test_steps("sql-streaming-tests.yaml").unwrap() {
        step.run(&test_env, &mut client).await.unwrap();
    }
}

#[tokio::test]
async fn basic_function() -> Result<(), BoxError> {
    tracing_helpers::register_logger();

    let test_env = TestEnv::temp("basic_function").await.unwrap();
    let mut client = TestClient::connect(&test_env).await.unwrap();

    for step in load_test_steps("basic-function.yaml").unwrap() {
        step.run(&test_env, &mut client).await.unwrap();
    }

    Ok(())
}

#[tokio::test]
async fn anvil_rpc_reorg() {
    tracing_helpers::register_logger();

    let dataset_name = "anvil_rpc";
    check_provider_file("rpc_anvil.toml").await;

    let http = reqwest::Client::new();
    let test_env = TestEnv::temp("anvil_rpc_reorg").await.unwrap();
    let dataset_store = DatasetStore::new(test_env.config.clone(), test_env.metadata_db.clone());
    let provider = alloy::providers::ProviderBuilder::new()
        .connect_anvil_with_config(|anvil| anvil.port(8545 as u16));
    let provider = DynProvider::new(provider);

    #[derive(Debug, PartialEq, Eq, serde::Deserialize)]
    struct BlockRow {
        block_num: BlockNum,
        hash: BlockHash,
        parent_hash: BlockHash,
    }

    let mine = async |blocks: u64| provider.anvil_mine(Some(blocks), None).await.unwrap();
    let latest_block = async || {
        let block = provider
            .get_block(alloy::eips::BlockId::latest())
            .await
            .unwrap()
            .unwrap();
        BlockRow {
            block_num: block.header.number,
            hash: block.header.hash,
            parent_hash: block.header.parent_hash,
        }
    };
    let reorg = async |depth: u64| {
        assert_ne!(depth, 0);
        let original_head = latest_block().await;
        tracing::info!(depth, "reorg");
        provider
            .anvil_reorg(alloy_rpc_types_anvil::ReorgOptions {
                depth,
                tx_block_pairs: vec![],
            })
            .await
            .unwrap();
        let new_head = latest_block().await;
        assert_eq!(original_head.block_num, new_head.block_num);
        assert_ne!(original_head.hash, new_head.hash);
    };
    let query_blocks = async |range: RangeInclusive<BlockNum>| -> Vec<BlockRow> {
        let url = format!("http://{}/", test_env.server_addrs.jsonl_addr);
        let sql = format!(
            r#"
            select block_num, hash, parent_hash
            from anvil_rpc.blocks
            where block_num >= {} and block_num <= {}
            "#,
            range.start(),
            range.end(),
        );
        let response = http.post(url).body(sql).send().await.unwrap();
        let buffer = response.text().await.unwrap();
        let mut rows: Vec<BlockRow> = Default::default();
        for line in buffer.lines() {
            rows.push(serde_json::from_str(line).unwrap());
        }
        rows.sort_by_key(|r| r.block_num);
        rows
    };
    let dump = async |range: RangeInclusive<BlockNum>| {
        SnapshotContext::temp_dump(&test_env, &dataset_name, *range.start(), *range.end(), 1)
            .await
            .unwrap()
    };
    let metadata_ranges = async || -> Vec<BlockRange> {
        let sql = parse_sql("select * from anvil_rpc.blocks").unwrap();
        let env = test_env.config.make_query_env().unwrap();
        let ctx = dataset_store.ctx_for_sql(&sql, env).await.unwrap();
        let tables = ctx.catalog().tables();
        let table = tables.iter().find(|t| t.table_name() == "blocks").unwrap();
        table_ranges(&table).await.unwrap()
    };

    mine(2).await;
    dump(0..=2).await;
    let blocks0 = query_blocks(0..=2).await;
    reorg(1).await;
    mine(1).await;
    dump(0..=3).await;
    let blocks1 = query_blocks(0..=3).await;

    // For now, we don't fully handle reorgs. But we're checking that metadata reflects the current
    // expected behavior. We only expect to query the "canonical chain", which will not resolve the
    // reorg unless the uncled block range is re-dumped.
    assert_eq!(&blocks0, &blocks1);
    let ranges = metadata_ranges().await;
    assert_eq!(ranges.len(), 2);
    assert_eq!(
        &ranges[0],
        &BlockRange {
            numbers: 0..=2,
            network: "anvil".to_string(),
            hash: blocks1[2].hash,
            prev_hash: Some(blocks1[0].parent_hash),
        }
    );
    assert_eq!(ranges[1].numbers, 3..=3);
    assert_eq!(&ranges[1].network, "anvil");
    assert_ne!(&ranges[1].prev_hash, &Some(ranges[0].hash));
}

#[tokio::test]
async fn generate_manifest_evm_rpc_builtin() {
    tracing_helpers::register_logger();

    let network = "mainnet".to_string();
    let kind = "evm-rpc".to_string();
    let name = "eth_rpc".to_string();

    let mut out = Vec::new();

    let _ = generate_manifest::run(
        network.clone(),
        kind.clone(),
        name.clone(),
        None,
        None,
        &mut out,
    )
    .await
    .unwrap();

    let out: DatasetDefsCommon = serde_json::from_slice(&out).unwrap();
    let builtin_schema: SerializableSchema = evm_rpc_datasets::tables::all(&network).into();

    assert_eq!(out.network, network);
    assert_eq!(out.kind, kind);
    assert_eq!(out.name, name);
    assert_eq!(out.schema.unwrap(), builtin_schema)
}

#[tokio::test]
async fn generate_manifest_firehose_builtin() {
    tracing_helpers::register_logger();

    let network = "mainnet".to_string();
    let kind = "firehose".to_string();
    let name = "firehose".to_string();

    let mut out = Vec::new();

    let _ = generate_manifest::run(
        network.clone(),
        kind.clone(),
        name.clone(),
        None,
        None,
        &mut out,
    )
    .await
    .unwrap();

    let out: DatasetDefsCommon = serde_json::from_slice(&out).unwrap();
    let builtin_schema: SerializableSchema = firehose_datasets::evm::tables::all(&network).into();

    assert_eq!(out.network, network);
    assert_eq!(out.kind, kind);
    assert_eq!(out.name, name);
    assert_eq!(out.schema.unwrap(), builtin_schema)
}

#[tokio::test]
async fn generate_manifest_substreams() {
    tracing_helpers::register_logger();

    let network = "mainnet".to_string();
    let kind = "substreams".to_string();
    let name = "substreams".to_string();
    let manifest = "https://spkg.io/pinax-network/weth-v0.1.0.spkg".to_string();
    let module = "map_events".to_string();

    let mut out = Vec::new();

    let _ = generate_manifest::run(
        network.clone(),
        kind.clone(),
        name.clone(),
        Some(manifest.clone()),
        Some(module.clone()),
        &mut out,
    )
    .await
    .unwrap();

    let out: DatasetDefsCommon = serde_json::from_slice(&out).unwrap();
    let dataset_def = substreams_datasets::dataset::DatasetDef {
        kind: kind.clone(),
        network: network.clone(),
        name: name.clone(),
        manifest,
        module,
    };

    let schema = substreams_datasets::tables(dataset_def)
        .await
        .map(Into::into)
        .unwrap();

    assert_eq!(out.network, network);
    assert_eq!(out.kind, kind);
    assert_eq!(out.name, name);
    assert_eq!(out.schema.unwrap(), schema);
}

#[tokio::test]
async fn generate_manifest_sql() {
    tracing_helpers::register_logger();

    let network = "mainnet".to_string();
    let kind = "sql".to_string();
    let name = "sql_over_eth_firehose".to_string();

    let mut out = Vec::new();

    let err = generate_manifest::run(
        network.clone(),
        kind.clone(),
        name.clone(),
        None,
        None,
        &mut out,
    )
    .await
    .unwrap_err()
    .to_string();
    assert!(err.contains("doesn't support dataset generation"));
}

#[tokio::test]
async fn generate_manifest_manifest_builtin() {
    tracing_helpers::register_logger();

    let network = "mainnet".to_string();
    let kind = "manifest".to_string();
    let name = "basic_function".to_string();

    let mut out = Vec::new();

    let err = generate_manifest::run(
        network.clone(),
        kind.clone(),
        name.clone(),
        None,
        None,
        &mut out,
    )
    .await
    .unwrap_err()
    .to_string();
    assert!(err.contains("doesn't support dataset generation"));
}

#[tokio::test]
async fn generate_manifest_bad_dataset_kind() {
    tracing_helpers::register_logger();

    let network = "mainnet".to_string();
    let bad_kind = "bad_kind".to_string();
    let name = "eth_rpc".to_string();
    let manifest = Some("https://spkg.io/pinax-network/weth-v0.1.0.spkg".to_string());
    let module = Some("map_events".to_string());

    let mut out = Vec::new();

    let err = generate_manifest::run(
        network.clone(),
        bad_kind.clone(),
        name.clone(),
        manifest,
        module,
        &mut out,
    )
    .await
    .unwrap_err();

    assert_eq!(
        err.to_string(),
        format!("unsupported dataset kind '{}'", bad_kind)
    );
}
