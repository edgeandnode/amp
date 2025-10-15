use ampd::gen_manifest_cmd;
use common::manifest::common::schema_from_tables;
use datasets_common::{manifest::Manifest as CommonManifest, name::Name};
use datasets_derived::DerivedDatasetKind;
use evm_rpc_datasets::EvmRpcDatasetKind;
use firehose_datasets::FirehoseDatasetKind;
use monitoring::logging;
use substreams_datasets::SubstreamsDatasetKind;

#[tokio::test]
async fn gen_manifest_cmd_run_with_evm_rpc_kind_generates_valid_manifest() {
    //* Given
    logging::init();
    let name = "eth_rpc"
        .parse::<Name>()
        .expect("should parse valid dataset name");
    let kind = EvmRpcDatasetKind;
    let network = "mainnet".to_string();
    let expected_schema = schema_from_tables(&evm_rpc_datasets::tables::all(&network));

    //* When
    let mut out = Vec::new();
    let result =
        gen_manifest_cmd::run(name.clone(), kind, network.clone(), None, None, &mut out).await;

    //* Then
    assert!(
        result.is_ok(),
        "manifest generation should succeed with valid EVM RPC parameters"
    );
    let manifest: CommonManifest =
        serde_json::from_slice(&out).expect("generated manifest should be valid JSON");

    assert_eq!(
        manifest.network,
        Some(network),
        "network should match input"
    );
    assert_eq!(manifest.kind, kind, "kind should match input");
    assert_eq!(manifest.name, name, "name should match input");
    assert_eq!(
        manifest.schema.expect("schema should be present"),
        expected_schema,
        "schema should match expected builtin schema"
    );
}

#[tokio::test]
async fn gen_manifest_cmd_run_with_firehose_kind_generates_valid_manifest() {
    //* Given
    logging::init();
    let name = "firehose"
        .parse::<Name>()
        .expect("should parse valid dataset name");
    let kind = FirehoseDatasetKind;
    let network = "mainnet".to_string();
    let expected_schema = schema_from_tables(&firehose_datasets::evm::tables::all(&network));

    //* When
    let mut out = Vec::new();
    let result =
        gen_manifest_cmd::run(name.clone(), kind, network.clone(), None, None, &mut out).await;

    //* Then
    assert!(
        result.is_ok(),
        "manifest generation should succeed with valid Firehose parameters"
    );
    let manifest: CommonManifest =
        serde_json::from_slice(&out).expect("generated manifest should be valid JSON");

    assert_eq!(
        manifest.network,
        Some(network),
        "network should match input"
    );
    assert_eq!(manifest.kind, kind, "kind should match input");
    assert_eq!(manifest.name, name, "name should match input");
    assert_eq!(
        manifest.schema.expect("schema should be present"),
        expected_schema,
        "schema should match expected builtin schema"
    );
}

#[tokio::test]
async fn gen_manifest_cmd_run_with_substreams_kind_generates_valid_manifest() {
    //* Given
    logging::init();
    let name = "substreams"
        .parse::<Name>()
        .expect("should parse valid dataset name");
    let kind = SubstreamsDatasetKind;
    let network = "mainnet".to_string();
    let manifest_url = "https://spkg.io/pinax-network/weth-v0.1.0.spkg".to_string();
    let module_name = "map_events".to_string();

    // Prepare expected schema
    let dataset_def = substreams_datasets::dataset::Manifest {
        name: name.clone(),
        version: Default::default(),
        kind,
        network: network.clone(),
        manifest: manifest_url.clone(),
        module: module_name.clone(),
    };
    let expected_schema = schema_from_tables(
        &substreams_datasets::tables(dataset_def)
            .await
            .expect("should fetch substreams tables"),
    );

    //* When
    let mut out = Vec::new();
    let result = gen_manifest_cmd::run(
        name.clone(),
        kind,
        network.clone(),
        Some(manifest_url),
        Some(module_name),
        &mut out,
    )
    .await;

    //* Then
    assert!(
        result.is_ok(),
        "manifest generation should succeed with valid Substreams parameters"
    );
    let manifest: CommonManifest =
        serde_json::from_slice(&out).expect("generated manifest should be valid JSON");

    assert_eq!(
        manifest.network,
        Some(network),
        "network should match input"
    );
    assert_eq!(manifest.kind, kind, "kind should match input");
    assert_eq!(manifest.name, name, "name should match input");
    assert_eq!(
        manifest.schema.expect("schema should be present"),
        expected_schema,
        "schema should match expected substreams schema"
    );
}

#[tokio::test]
async fn gen_manifest_cmd_run_with_derived_kind_fails_with_unsupported_error() {
    //* Given
    logging::init();
    let name = "basic_function"
        .parse()
        .expect("should parse valid dataset name");
    let kind = DerivedDatasetKind;
    let network = "mainnet".to_string();

    //* When
    let mut out = Vec::new();
    let result = gen_manifest_cmd::run(name, kind, network, None, None, &mut out).await;

    //* Then
    assert!(
        result.is_err(),
        "manifest generation should fail for derived dataset kind"
    );
    let error = result.expect_err("should return error for derived kind");
    let error_message = error.to_string();
    assert!(
        error_message.contains("doesn't support dataset generation"),
        "error should indicate derived datasets are not supported, got: {}",
        error_message
    );
}
