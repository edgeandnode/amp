use std::str::FromStr;

use dataset_store::DatasetKind;
use datasets_common::{name::Name, namespace::Namespace, version::Version};

use crate::testlib::{self, fixtures::DatasetPackage};

#[tokio::test]
async fn load_sql_dataset_returns_sql_dataset_with_correct_kind() {
    monitoring::logging::init();

    //* Given
    let ctx = testlib::ctx::TestCtxBuilder::new("dataset_store_sql_dataset")
        .with_dataset_manifest("anvil_rpc")
        .with_anvil_ipc()
        .build()
        .await
        .expect("should create test context");

    // Register the TypeScript dataset
    let sql_over_anvil_1 = DatasetPackage::new("sql_over_anvil_1", Some("amp.config.ts"));
    let cli = ctx.new_amp_cli();
    sql_over_anvil_1
        .register(&cli)
        .await
        .expect("Failed to register sql_over_anvil_1 dataset");

    let metadata_db = ctx.metadata_db();
    let dataset_store = ctx.daemon_server().dataset_store();

    //* When
    let result = dataset_store
        .get_derived_manifest("sql_over_anvil_1", None)
        .await
        .expect("load derived manifest should succeed")
        .expect("derived manifest should exist");

    //* Then
    assert_eq!(
        result.kind.to_string(),
        "manifest",
        "dataset kind should be 'manifest' for TypeScript datasets"
    );

    // Assert that the dataset is registered in the metadata DB
    // TODO: Pass the actual namespace instead of using a placeholder
    let namespace = "_"
        .parse::<Namespace>()
        .expect("'_' should be a valid namespace");
    let db_dataset = metadata_db
        .get_dataset_latest_version_with_details(&namespace, &result.name)
        .await
        .expect("should query metadata DB");
    assert!(
        db_dataset.is_some(),
        "dataset {} should be registered in metadata DB",
        result.name
    );
}

#[tokio::test]
async fn load_manifest_dataset_returns_manifest_with_correct_kind() {
    monitoring::logging::init();

    //* Given
    let ctx = testlib::ctx::TestCtxBuilder::new("dataset_store_manifest_dataset")
        .with_dataset_manifest("register_test_dataset__1_0_0")
        .with_dataset_manifest("eth_firehose")
        .with_dataset_snapshot("eth_firehose")
        .build()
        .await
        .expect("should create test context");
    let metadata_db = ctx.metadata_db();
    let dataset_store = ctx.daemon_server().dataset_store();

    let name = "register_test_dataset"
        .parse::<Name>()
        .expect("name should be a valid identifier");
    let version = "1.0.0".parse::<Version>().expect("should parse version");

    //* When
    let result = dataset_store
        .get_derived_manifest(&name, &version)
        .await
        .expect("load derived manifest should succeed")
        .expect("derived manifest should exist");

    //* Then
    assert_eq!(
        result.kind.to_string(),
        "manifest",
        "dataset kind should be 'manifest'"
    );

    // Assert that the dataset is registered in the metadata DB
    // TODO: Pass the actual namespace instead of using a placeholder
    let namespace = "_"
        .parse::<Namespace>()
        .expect("'_' should be a valid namespace");
    let db_dataset = metadata_db
        .get_dataset_with_details(&namespace, &name, &version)
        .await
        .expect("should query metadata DB");
    assert!(
        db_dataset.is_some(),
        "dataset {} with version {} should be registered in metadata DB",
        name,
        version
    );
}

#[tokio::test]
async fn all_datasets_returns_available_datasets_without_error() {
    monitoring::logging::init();

    //* Given
    let ctx = testlib::ctx::TestCtxBuilder::new("dataset_store_all_datasets")
        .with_dataset_manifest("anvil_rpc")
        .with_dataset_manifest("eth_rpc")
        .with_dataset_manifest("register_test_dataset__1_0_0")
        .with_anvil_ipc()
        .build()
        .await
        .expect("should create test context");

    // Register the TypeScript dataset
    let sql_over_anvil_1 = DatasetPackage::new("sql_over_anvil_1", Some("amp.config.ts"));
    let cli = ctx.new_amp_cli();
    sql_over_anvil_1
        .register(&cli)
        .await
        .expect("Failed to register sql_over_anvil_1 dataset");

    let metadata_db = ctx.metadata_db();
    let dataset_store = ctx.daemon_server().dataset_store();

    //* When
    let result = dataset_store
        .get_all_datasets()
        .await
        .expect("load_all_datasets should succeed");

    //* Then
    assert!(!result.is_empty(), "should return at least one dataset");
    assert!(
        result.iter().all(|dataset| !dataset.kind.is_empty()),
        "all datasets should have non-empty kind"
    );
    assert!(
        result
            .iter()
            .all(|dataset| DatasetKind::from_str(&dataset.kind).is_ok()),
        "all dataset kinds should be valid"
    );

    // Assert that the dataset is registered in the metadata DB
    // TODO: Pass the actual namespace instead of using a placeholder
    let namespace = "_"
        .parse::<Namespace>()
        .expect("'_' should be a valid namespace");
    for dataset in result {
        let db_dataset = metadata_db
            .get_dataset_latest_version_with_details(&namespace, &dataset.name)
            .await
            .expect("should query metadata DB");
        assert!(
            db_dataset.is_some(),
            "dataset {} should be registered in metadata DB",
            dataset.name
        );
    }
}
