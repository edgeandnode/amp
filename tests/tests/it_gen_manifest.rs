use common::manifest::common::schema_from_tables;
use datasets_common::manifest::Manifest as CommonManifest;
use generate_manifest;
use monitoring::logging;

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

    let out: CommonManifest = serde_json::from_slice(&out).unwrap();
    let builtin_schema = schema_from_tables(evm_rpc_datasets::tables::all(&network));

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

    let out: CommonManifest = serde_json::from_slice(&out).unwrap();
    let builtin_schema = schema_from_tables(firehose_datasets::evm::tables::all(&network));

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

    let out: CommonManifest = serde_json::from_slice(&out).unwrap();
    let dataset_def = substreams_datasets::dataset::DatasetDef {
        kind: kind.clone(),
        network: network.clone(),
        name: name.clone(),
        manifest,
        module,
    };

    let schema = schema_from_tables(substreams_datasets::tables(dataset_def).await.unwrap());

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
        format!("unsupported dataset kind '{bad_kind}'")
    );
}
