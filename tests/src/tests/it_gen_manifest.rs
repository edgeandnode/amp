use ampctl::cmd::gen_manifest;
use datasets_common::name::Name;
use datasets_derived::DerivedDatasetKind;
use evm_rpc_datasets::EvmRpcDatasetKind;
use firehose_datasets::FirehoseDatasetKind;
use monitoring::logging;

#[tokio::test]
async fn gen_manifest_produces_expected_eth_rpc_json() {
    //* Given
    logging::init();
    let name = "eth_rpc"
        .parse::<Name>()
        .expect("should parse valid dataset name");
    let kind = EvmRpcDatasetKind;
    let network = "mainnet".to_string();
    let start_block = Some(15000000u64);

    //* When
    let mut out = Vec::new();
    let result = gen_manifest::generate_manifest(
        name.clone(),
        kind,
        network.clone(),
        start_block,
        false,
        &mut out,
    )
    .await;

    //* Then
    assert!(result.is_ok(), "manifest generation should succeed");

    // Read the expected manifest file
    let expected_manifest_path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .join("tests/config/manifests/eth_rpc.json");
    let expected_json = std::fs::read_to_string(&expected_manifest_path)
        .expect("should read expected manifest file");

    // Parse both as JSON values for comparison
    let generated: serde_json::Value =
        serde_json::from_slice(&out).expect("generated manifest should be valid JSON");
    let expected: serde_json::Value =
        serde_json::from_str(&expected_json).expect("expected manifest should be valid JSON");

    assert_eq!(
        generated, expected,
        "generated manifest should match expected manifest exactly"
    );
}

#[tokio::test]
async fn gen_manifest_cmd_run_with_evm_rpc_kind_generates_valid_manifest() {
    //* Given
    logging::init();
    let name = "eth_rpc"
        .parse::<Name>()
        .expect("should parse valid dataset name");
    let kind = EvmRpcDatasetKind;
    let network = "mainnet".to_string();

    //* When
    let mut out = Vec::new();
    let result =
        gen_manifest::generate_manifest(name.clone(), kind, network.clone(), None, false, &mut out)
            .await;

    //* Then
    assert!(
        result.is_ok(),
        "manifest generation should succeed with valid EVM RPC parameters"
    );
    let manifest: evm_rpc_datasets::Manifest =
        serde_json::from_slice(&out).expect("generated manifest should be valid JSON");

    assert_eq!(manifest.network, network, "network should match input");
    assert_eq!(manifest.kind, kind, "kind should match input");
    assert_eq!(manifest.name, name, "name should match input");
    assert_eq!(manifest.start_block, 0, "start_block should default to 0");
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

    //* When
    let mut out = Vec::new();
    let result =
        gen_manifest::generate_manifest(name.clone(), kind, network.clone(), None, false, &mut out)
            .await;

    //* Then
    assert!(
        result.is_ok(),
        "manifest generation should succeed with valid Firehose parameters"
    );
    let manifest: firehose_datasets::dataset::Manifest =
        serde_json::from_slice(&out).expect("generated manifest should be valid JSON");

    assert_eq!(manifest.network, network, "network should match input");
    assert_eq!(manifest.kind, kind, "kind should match input");
    assert_eq!(manifest.name, name, "name should match input");
    assert_eq!(manifest.start_block, 0, "start_block should default to 0");
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
    let result = gen_manifest::generate_manifest(name, kind, network, None, false, &mut out).await;

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

#[tokio::test]
async fn gen_manifest_cmd_run_with_start_block_includes_it_in_manifest() {
    //* Given
    logging::init();
    let name = "eth_rpc"
        .parse::<Name>()
        .expect("should parse valid dataset name");
    let kind = EvmRpcDatasetKind;
    let network = "mainnet".to_string();
    let start_block = 1000000u64;

    //* When
    let mut out = Vec::new();
    let result = gen_manifest::generate_manifest(
        name.clone(),
        kind,
        network.clone(),
        Some(start_block),
        false,
        &mut out,
    )
    .await;

    //* Then
    assert!(
        result.is_ok(),
        "manifest generation should succeed with start_block parameter"
    );
    let manifest: evm_rpc_datasets::Manifest =
        serde_json::from_slice(&out).expect("generated manifest should be valid JSON");

    assert_eq!(
        manifest.start_block, start_block,
        "start_block should match input"
    );
    assert_eq!(manifest.name, name, "name should match input");
    assert_eq!(manifest.network, network, "network should match input");
}

#[tokio::test]
async fn gen_manifest_cmd_run_without_start_block_defaults_to_zero() {
    //* Given
    logging::init();
    let name = "firehose"
        .parse::<Name>()
        .expect("should parse valid dataset name");
    let kind = FirehoseDatasetKind;
    let network = "mainnet".to_string();

    //* When
    let mut out = Vec::new();
    let result =
        gen_manifest::generate_manifest(name.clone(), kind, network.clone(), None, false, &mut out)
            .await;

    //* Then
    assert!(
        result.is_ok(),
        "manifest generation should succeed without start_block parameter"
    );
    let manifest: firehose_datasets::dataset::Manifest =
        serde_json::from_slice(&out).expect("generated manifest should be valid JSON");

    assert_eq!(manifest.start_block, 0, "start_block should default to 0");
    assert_eq!(manifest.name, name, "name should match input");
    assert_eq!(manifest.network, network, "network should match input");
}
