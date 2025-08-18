mod anvil;
mod deploy;
mod registry;

use common::BoxError;
use dataset_store::{DatasetDefsCommon, SerializableSchema};
use generate_manifest;
use monitoring::logging;
use schemars::schema_for;

use crate::{
    steps::load_test_steps,
    test_client::TestClient,
    test_support::{
        self, SnapshotContext, TestEnv, check_blocks, check_provider_file, restore_blessed_dataset,
    },
};

#[tokio::test]
async fn evm_rpc_single_dump() {
    logging::init();

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
async fn evm_rpc_base_single_dump() {
    logging::init();

    let dataset_name = "base";
    check_provider_file("rpc_eth_base.toml").await;

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
async fn eth_firehose_single_dump() {
    logging::init();

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

    // 5. With batch size 1 and 4 blocks, we expect 4 files (even if some are empty) since
    // microbatch_max_interval=1 should create one file per block even_blocks only includes even
    // block numbers, so we expect 2 files with data for blocks 15000000 and 15000002, plus empty
    // files for odd blocks
    assert_eq!(file_count, 4);

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
