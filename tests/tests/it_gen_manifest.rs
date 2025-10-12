use ampd::gen_manifest_cmd;
use common::manifest::common::schema_from_tables;
use datasets_common::{manifest::Manifest as CommonManifest, name::Name};
use monitoring::logging;
use substreams_datasets::SubstreamsDatasetKind;

#[tokio::test]
async fn generate_manifest_evm_rpc_builtin() {
    logging::init();

    let network = "mainnet".to_string();
    let kind = "evm-rpc".to_string();
    let name = "eth_rpc".to_string();

    let mut out = Vec::new();

    let _ = gen_manifest_cmd::run(
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
    let builtin_schema = schema_from_tables(&evm_rpc_datasets::tables::all(&network));

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

    let _ = gen_manifest_cmd::run(
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
    let builtin_schema = schema_from_tables(&firehose_datasets::evm::tables::all(&network));

    assert_eq!(out.network, network);
    assert_eq!(out.kind, kind);
    assert_eq!(out.name, name);
    assert_eq!(out.schema.unwrap(), builtin_schema)
}

#[tokio::test]
async fn generate_manifest_substreams() {
    logging::init();

    let name = "substreams".parse::<Name>().expect("invalid name");
    let network = "mainnet".to_string();
    let kind = SubstreamsDatasetKind;
    let manifest = "https://spkg.io/pinax-network/weth-v0.1.0.spkg".to_string();
    let module = "map_events".to_string();

    let mut out = Vec::new();

    let _ = gen_manifest_cmd::run(
        network.clone(),
        kind.to_string(),
        name.to_string(),
        Some(manifest.clone()),
        Some(module.clone()),
        &mut out,
    )
    .await
    .unwrap();

    let out: CommonManifest = serde_json::from_slice(&out).unwrap();
    let dataset_def = substreams_datasets::dataset::Manifest {
        name: name.clone(),
        version: Default::default(),
        kind: kind.clone(),
        network: network.clone(),
        manifest,
        module,
    };

    let schema = schema_from_tables(&substreams_datasets::tables(dataset_def).await.unwrap());

    assert_eq!(out.network, network);
    assert_eq!(out.kind, kind.to_string());
    assert_eq!(out.name, name);
    assert_eq!(out.schema.unwrap(), schema);
}

#[tokio::test]
async fn generate_manifest_manifest_builtin() {
    logging::init();

    let network = "mainnet".to_string();
    let kind = "manifest".to_string();
    let name = "basic_function".to_string();

    let mut out = Vec::new();

    let err = gen_manifest_cmd::run(
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

    let err = gen_manifest_cmd::run(
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
        format!("Unsupported dataset kind '{bad_kind}'")
    );
}
