mod anvil;
mod deploy;
mod registry;

use alloy::{
    node_bindings::Anvil,
    primitives::BlockHash,
    providers::{DynProvider, Provider, ProviderBuilder, ext::AnvilApi as _},
    transports::http::reqwest,
};
use common::{BlockNum, BoxError, metadata::segments::BlockRange, query_context::parse_sql};
use dataset_store::{DatasetDefsCommon, DatasetStore, SerializableSchema};
use generate_manifest;
use monitoring::logging;
use schemars::schema_for;

use crate::{
    steps::load_test_steps,
    test_client::TestClient,
    test_support::{
        self, SnapshotContext, TestEnv, check_blocks, restore_blessed_dataset,
        spawn_compaction_and_await_completion,
    },
};

#[tokio::test]
async fn evm_rpc_single_dump() {
    logging::init();

    let dataset_name = "eth_rpc";
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
async fn evm_rpc_single_dump_fetch_receipts_per_tx() {
    logging::init();

    let dataset_name = "eth_rpc";
    let test_env = TestEnv::temp_with_config(
        "evm_rpc_single_dump_fetch_receipts_per_tx",
        "per_tx_receipt_config.toml",
    )
    .await
    .unwrap();

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
async fn evm_rpc_base_single_dump() {
    logging::init();

    let dataset_name = "base";
    let test_env = TestEnv::temp("evm_rpc_base_single_dump").await.unwrap();

    let blessed = SnapshotContext::blessed(&test_env, &dataset_name)
        .await
        .unwrap();

    // Check the dataset directly against the RPC provider with `check_blocks`.
    check_blocks(&test_env, dataset_name, 33_411_770, 33_411_770)
        .await
        .expect("blessed data differed from provider");

    // Now dump the dataset to a temporary directory and check it again against the blessed files.
    let temp_dump = SnapshotContext::temp_dump(&test_env, &dataset_name, 33_411_770, 33_411_770, 1)
        .await
        .expect("temp dump failed");
    temp_dump.assert_eq(&blessed).await.unwrap();
}

#[tokio::test]
async fn evm_rpc_base_single_dump_fetch_receipts_per_tx() {
    logging::init();

    let dataset_name = "base";
    let test_env = TestEnv::temp_with_config(
        "evm_rpc_base_single_dump_fetch_receipts_per_tx",
        "per_tx_receipt_config.toml",
    )
    .await
    .unwrap();

    let blessed = SnapshotContext::blessed(&test_env, &dataset_name)
        .await
        .unwrap();

    // Check the dataset directly against the RPC provider with `check_blocks`.
    check_blocks(&test_env, dataset_name, 33_411_770, 33_411_770)
        .await
        .expect("blessed data differed from provider");

    // Now dump the dataset to a temporary directory and check it again against the blessed files.
    let temp_dump = SnapshotContext::temp_dump(&test_env, &dataset_name, 33_411_770, 33_411_770, 1)
        .await
        .expect("temp dump failed");
    temp_dump.assert_eq(&blessed).await.unwrap();
}

#[tokio::test]
async fn eth_firehose_single_dump() {
    logging::init();

    let dataset_name = "eth_firehose";
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
async fn eth_beacon_single_dump() {
    logging::init();

    let dataset_name = "eth_beacon";
    let test_env = TestEnv::temp("eth_beacon_single_dump").await.unwrap();

    let blessed = SnapshotContext::blessed(&test_env, &dataset_name)
        .await
        .unwrap();
    check_blocks(&test_env, dataset_name, 12_000_000, 12_000_000)
        .await
        .expect("blessed data differed from provider");
    let temp_dump = SnapshotContext::temp_dump(&test_env, &dataset_name, 12_000_000, 12_000_000, 1)
        .await
        .expect("temp dump failed");
    temp_dump.assert_eq(&blessed).await.unwrap();
}

#[tokio::test]
async fn sql_over_eth_firehose_dump() {
    logging::init();
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
    logging::init();
    let test_env = TestEnv::temp("sql_tests").await.unwrap();
    let mut client = TestClient::connect(&test_env).await.unwrap();

    for step in load_test_steps("sql-tests.yaml").unwrap() {
        step.run(&test_env, &mut client).await.unwrap()
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn streaming_tests_basic() {
    logging::init();
    let test_env = TestEnv::temp("sql_streaming_tests_basic").await.unwrap();
    let mut client = TestClient::connect(&test_env).await.unwrap();

    for step in load_test_steps("sql-streaming-tests-basic.yaml").unwrap() {
        step.run(&test_env, &mut client).await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn streaming_tests_with_sql_datasets() {
    logging::init();
    let test_env = TestEnv::temp("sql_streaming_tests_with_sql_datasets")
        .await
        .unwrap();
    let mut client = TestClient::connect(&test_env).await.unwrap();

    for step in load_test_steps("sql-streaming-tests-with-sql-datasets.yaml").unwrap() {
        step.run(&test_env, &mut client).await.unwrap();
    }
}

#[tokio::test]
async fn basic_function() -> Result<(), BoxError> {
    logging::init();

    let test_env = TestEnv::temp("basic_function").await.unwrap();
    let mut client = TestClient::connect(&test_env).await.unwrap();

    for step in load_test_steps("basic-function.yaml").unwrap() {
        step.run(&test_env, &mut client).await.unwrap();
    }

    Ok(())
}

#[tokio::test]
async fn intra_deps_test() -> Result<(), BoxError> {
    logging::init();

    let test_env = TestEnv::temp("intra_deps_test").await.unwrap();
    let mut client = TestClient::connect(&test_env).await.unwrap();

    for step in load_test_steps("intra-deps.yaml").unwrap() {
        step.run(&test_env, &mut client).await.unwrap();
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    }
    Ok(())
}

#[tokio::test]
async fn multi_version_test() -> Result<(), BoxError> {
    logging::init();

    let test_env = TestEnv::temp("multi_version_test").await.unwrap();
    let mut client = TestClient::connect(&test_env).await.unwrap();

    for step in load_test_steps("multi-version.yaml").unwrap() {
        step.run(&test_env, &mut client).await.unwrap();
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    }
    Ok(())
}

#[tokio::test]
async fn persist_start_block_test() -> Result<(), BoxError> {
    logging::init();

    // Spawn Anvil on a random port
    let anvil = Anvil::new().port(0_u16).spawn();
    let anvil_url = anvil.endpoint_url();

    // Set up the test environment with the Anvil provider
    let test_env = TestEnv::new("persist_start_block_test", true, Some(anvil_url.as_str())).await?;
    let mut client = TestClient::connect(&test_env).await?;

    // Create a provider and mine 10 blocks
    let provider = ProviderBuilder::new().connect_http(anvil_url);
    provider.anvil_mine(Some(10), None).await?;

    // Verify that blocks were mined (current block should be 10)
    let block_number = provider.get_block_number().await?;
    assert_eq!(block_number, 10, "Expected block number 10 after mining");

    // Run test steps from YAML, which should set and verify the start block
    for step in load_test_steps("persist-start-block-test.yaml")? {
        step.run(&test_env, &mut client).await?;
    }

    Ok(())
}

#[tokio::test]
async fn persist_start_block_set_on_creation() -> Result<(), BoxError> {
    logging::init();

    // Set up the Anvil test environment from the `anvil.rs` test helpers
    let test = anvil::AnvilTestContext::setup("persist_start_block_set_on_creation").await;

    // Mine 10 blocks so the dump has something to process
    test.mine(10).await;
    assert_eq!(test.latest_block().await.block_num, 10);

    // Perform the initial dump from block 0. This should succeed and persist `start_block = 0`.
    test_support::dump_dataset(&test.env.config, "anvil_rpc", 0, 9, 1, None).await?;

    // Query the database to verify that the start_block was persisted correctly as 0.
    let location = test
        .env
        .metadata_db
        .get_active_location(metadata_db::TableId {
            dataset: "anvil_rpc",
            dataset_version: None,
            table: "blocks",
        })
        .await?
        .ok_or("No active location found for anvil_rpc.blocks")?;
    let location_id = location.1;

    test.env
        .metadata_db
        .check_start_block(location_id, 0)
        .await?;

    // Now, attempt to dump again with a *different* start_block. This must fail.
    let result = test_support::dump_dataset(&test.env.config, "anvil_rpc", 1, 9, 1, None).await;

    assert!(result.is_err(), "Expected dump to fail but it succeeded");
    let err_msg = result.unwrap_err().to_string();
    let expected_err =
        "Cannot start dump: location has existing start_block=0, but requested start_block=1";
    assert!(
        err_msg.contains(expected_err),
        "Error message did not match.\nGot: '{}'\nExpected to contain: '{}'",
        err_msg,
        expected_err
    );

    Ok(())
}

#[tokio::test]
async fn anvil_rpc_reorg() {
    logging::init();

    let dataset_name = "anvil_rpc";

    let http = reqwest::Client::new();
    let test_env = TestEnv::temp("anvil_rpc_reorg").await.unwrap();
    let dataset_store = DatasetStore::new(test_env.config.clone(), test_env.metadata_db.clone());
    // Start Anvil on port 8545 to match the rpc_anvil.toml configuration
    let provider = alloy::providers::ProviderBuilder::new()
        .connect_anvil_with_config(|anvil| anvil.port(8545u16));
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
            .anvil_reorg(alloy::rpc::types::anvil::ReorgOptions {
                depth,
                tx_block_pairs: vec![],
            })
            .await
            .unwrap();
        let new_head = latest_block().await;
        assert_eq!(original_head.block_num, new_head.block_num);
        assert_ne!(original_head.hash, new_head.hash);
    };
    let query_blocks = async |range: std::ops::RangeInclusive<BlockNum>| -> Vec<BlockRow> {
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
    let dump = async |range: std::ops::RangeInclusive<BlockNum>| {
        SnapshotContext::temp_dump(&test_env, &dataset_name, *range.start(), *range.end(), 1)
            .await
            .unwrap()
    };
    let metadata_ranges = async || -> Vec<BlockRange> {
        let sql = parse_sql("select * from anvil_rpc.blocks").unwrap();
        let env = test_env.config.make_query_env().unwrap();
        let catalog = dataset_store.catalog_for_sql(&sql, env).await.unwrap();
        let tables = catalog.tables();
        let table = tables.iter().find(|t| t.table_name() == "blocks").unwrap();
        test_support::table_ranges(&table).await.unwrap()
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
    logging::init();

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
    logging::init();

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
    logging::init();

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
    logging::init();

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
    logging::init();

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
    logging::init();

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

#[tokio::test]
async fn sql_dataset_input_batch_size() {
    logging::init();

    // 1. Setup
    let test_env = TestEnv::temp("sql_dataset_input_batch_size").await.unwrap();

    // 2. First dump eth_firehose dependency on the spot
    let start = 15_000_000;
    let end = 15_000_003;

    test_support::dump_dataset(&test_env.config, "eth_firehose", start, end, 1, None)
        .await
        .unwrap();

    // 3. Execute dump of sql_stream_ds with microbatch_max_interval=1
    let dataset_name = "sql_stream_ds";

    test_support::dump_dataset(&test_env.config, dataset_name, start, end, 1, Some(1))
        .await
        .unwrap();

    // 4. Get catalog and count files
    let catalog = test_support::catalog_for_dataset(
        dataset_name,
        &test_env.dataset_store,
        test_env.metadata_db.clone(),
    )
    .await
    .unwrap();

    // Find the even_blocks table
    let table = catalog
        .tables()
        .iter()
        .find(|t| t.table_name() == "even_blocks")
        .unwrap();

    let file_count = table.files().await.unwrap().len();

    // 5. With batch size 1 and 4 blocks, we expect 4 files to be dumped (even if some are empty)
    // since microbatch_max_interval=1 should create one file per block even_blocks only includes
    // even block numbers, so we expect 2 files with data for blocks 15000000 and 15000002, plus
    // empty files for odd blocks.
    assert_eq!(file_count, 4);

    spawn_compaction_and_await_completion(table, &test_env.config).await;

    // 6. After compaction, we expect an additional file to be created, with all data in it.
    let file_count_after = table.files().await.unwrap().len();
    assert_eq!(file_count_after, 5);

    let mut test_client = TestClient::connect(&test_env).await.unwrap();
    let res = test_client
        .run_query("select count(*) from sql_stream_ds.even_blocks", None)
        .await
        .unwrap();
    assert_eq!(res, serde_json::json!([{"count(*)": 2}]));
}

#[test]
fn generate_json_schemas() {
    let json_schemas = [
        ("EvmRpc", schema_for!(evm_rpc_datasets::DatasetDef)),
        (
            "Substreams",
            schema_for!(substreams_datasets::dataset::DatasetDef),
        ),
        (
            "Firehose",
            schema_for!(firehose_datasets::dataset::DatasetDef),
        ),
        ("Common", schema_for!(dataset_store::DatasetDefsCommon)),
        ("Sql", schema_for!(dataset_store::sql_datasets::DatasetDef)),
        ("Manifest", schema_for!(common::manifest::Manifest)),
    ];

    for (name, schema) in json_schemas {
        let schema = serde_json::to_string_pretty(&schema).unwrap();
        let dir = env!("CARGO_MANIFEST_DIR");
        let dir = format!("{dir}/../docs/dataset-def-schemas");
        std::fs::create_dir_all(&dir).expect("Failed to create JSON schema output directory");
        let path = format!("{}/{}.json", dir, name);
        std::fs::write(path, schema).unwrap();
    }
}
