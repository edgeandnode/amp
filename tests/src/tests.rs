use std::{ops::RangeInclusive, str::FromStr as _};

use alloy::{
    primitives::BlockHash,
    providers::{ext::AnvilApi as _, DynProvider, Provider},
    transports::http::reqwest,
};
use common::{
    metadata::range::BlockRange, query_context::parse_sql, tracing_helpers, BlockNum, BoxError,
};
use dataset_store::DatasetStore;
use dump::worker::Worker;
use futures::StreamExt;
use metadata_db::workers::WorkerNodeId;

use crate::{
    steps::load_test_steps,
    test_client::TestClient,
    test_support::{
        check_blocks, check_provider_file, record_batch_to_json, restore_blessed_dataset,
        DatasetPackage, SnapshotContext, TestEnv,
    },
};

#[tokio::test]
async fn evm_rpc_single_dump() {
    tracing_helpers::register_logger();

    let dataset_name = "eth_rpc";
    check_provider_file("rpc_eth_mainnet.toml").await;

    let test_env = TestEnv::temp().await.unwrap();

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

    let test_env = TestEnv::temp().await.unwrap();
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

    let test_env = TestEnv::temp().await.unwrap();
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
    let test_env = TestEnv::temp().await.unwrap();
    let mut client = TestClient::connect(&test_env).await.unwrap();

    for step in load_test_steps("sql-tests.yaml").unwrap() {
        step.run(&test_env, &mut client).await.unwrap()
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn streaming_tests() {
    tracing_helpers::register_logger();
    let test_env = TestEnv::temp().await.unwrap();
    let mut client = TestClient::connect(&test_env).await.unwrap();

    for step in load_test_steps("sql-streaming-tests.yaml").unwrap() {
        step.run(&test_env, &mut client).await.unwrap();
    }
}

#[tokio::test]
async fn basic_function() -> Result<(), BoxError> {
    tracing_helpers::register_logger();

    let test_env = TestEnv::temp().await.unwrap();
    let (bound_addrs, server) = nozzle::server::run(
        test_env.config.clone(),
        test_env.metadata_db.clone(),
        false,
        false,
    )
    .await?;
    tokio::spawn(server);

    let worker = Worker::new(
        test_env.config.clone(),
        test_env.metadata_db.clone(),
        WorkerNodeId::from_str("basic_function").unwrap(),
    );
    tokio::spawn(worker.run());

    // Run `pnpm build` on the dataset.
    let dataset = DatasetPackage::new("basic_function");
    dataset.pnpm_install().await?;
    dataset.deploy(bound_addrs).await?;

    let dataset_store = DatasetStore::new(test_env.config.clone(), test_env.metadata_db.clone());
    let env = test_env.config.make_query_env()?;
    let ctx = dataset_store
        .ctx_for_sql(&parse_sql("SELECT basic_function.testString()")?, env)
        .await?;
    let result = ctx
        .execute_sql("SELECT basic_function.testString()")
        .await?
        .next()
        .await
        .unwrap()?;
    assert_eq!(
        record_batch_to_json(result),
        "[{\"basic_function.testString()\":\"I'm a function\"}]"
    );

    // TOOD: Fix function calls on flight server.
    // for test in load_sql_tests("basic-function.yaml").unwrap() {
    //     let results = run_query_on_fresh_server(&test.query, vec![], vec![], None)
    //         .await
    //         .map_err(|e| format!("{e:?}"));
    //     test.assert_result_eq(results);
    // }

    Ok(())
}

#[tokio::test]
async fn anvil_rpc_reorg() {
    tracing_helpers::register_logger();

    let dataset_name = "anvil_rpc";
    check_provider_file("rpc_anvil.toml").await;

    let http = reqwest::Client::new();
    let test_env = TestEnv::temp().await.unwrap();
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
    let reorg = async |depth: u8| {
        assert_ne!(depth, 0);
        let original_head = latest_block().await;
        tracing::info!(depth, "reorg");
        provider
            .anvil_reorg(alloy_rpc_types_anvil::ReorgOptions {
                depth: 1,
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
        table.ranges().await.unwrap()
    };

    mine(2).await;
    dump(0..=2).await;
    let blocks0 = query_blocks(0..=2).await;
    reorg(1).await;
    mine(1).await;
    dump(0..=3).await;
    let blocks1 = query_blocks(0..=3).await;

    // For now, we don't handle reorgs. But we're checking that metadata reflects the current
    // expected behavior.
    assert_eq!(&blocks0, &blocks1[0..=2]);
    assert_ne!(&blocks1[2].hash, &blocks1[3].parent_hash);
    let ranges = metadata_ranges().await;
    let expected_ranges = vec![
        BlockRange {
            numbers: 0..=2,
            network: "anvil".to_string(),
            hash: blocks1[2].hash,
            prev_hash: Some(blocks1[0].parent_hash),
        },
        BlockRange {
            numbers: 3..=3,
            network: "anvil".to_string(),
            hash: blocks1[3].hash,
            prev_hash: Some(blocks1[3].parent_hash),
        },
    ];
    assert_eq!(ranges, expected_ranges);
}
