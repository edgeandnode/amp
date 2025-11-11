use ampctl::client::{self, datasets::RegisterError};
use datasets_common::{
    fqn::FullyQualifiedName, name::Name, namespace::Namespace, version::Version,
};
use datasets_derived::Manifest as DerivedDatasetManifest;
use serde_json::value::RawValue;

use crate::testlib::ctx::TestCtxBuilder;

#[tokio::test]
async fn register_new_dataset_with_manifest_succeeds() {
    //* Given
    let ctx = TestCtx::setup("test_register_with_new_manifest").await;
    let namespace = "_".parse::<Namespace>().expect("valid namespace");
    let name = "register_test_new"
        .parse::<Name>()
        .expect("valid dataset name");
    let version = "1.0.0".parse::<Version>().expect("valid version");

    let manifest = create_test_manifest();
    let manifest_str =
        serde_json::to_string(&manifest).expect("failed to serialize manifest to JSON");

    //* When
    let result = ctx
        .register_dataset(&namespace, &name, &version, &manifest_str)
        .await;

    //* Then
    assert!(
        result.is_ok(),
        "registration should succeed with valid manifest: {:?}",
        result.err()
    );
    assert!(
        ctx.verify_dataset_tag_exists(&namespace, &name, &version)
            .await,
        "dataset should exist after successful registration"
    );
}

#[tokio::test]
async fn register_with_missing_dependency_fails() {
    //* Given
    let ctx = TestCtx::setup("test_register_invalid_dependency").await;
    let namespace = "_".parse::<Namespace>().expect("valid namespace");
    let name = "missing_dep".parse::<Name>().expect("valid dataset name");
    let version = "1.0.0".parse::<Version>().expect("valid version");

    let mut manifest = create_test_manifest();
    manifest.dependencies.clear();
    let manifest_str =
        serde_json::to_string(&manifest).expect("failed to serialize manifest to JSON");

    //* When
    let result = ctx
        .register_dataset(&namespace, &name, &version, &manifest_str)
        .await;

    //* Then
    assert!(
        result.is_err(),
        "registration should fail with missing dependencies"
    );
    let err = result.unwrap_err();
    match err {
        RegisterError::ManifestValidationError(api_err) => {
            assert_eq!(api_err.error_code, "MANIFEST_VALIDATION_ERROR");
            assert_eq!(
                api_err.error_message,
                r#"Manifest validation error: Dependency alias not found: In table 'test_table': Dependency alias 'eth_firehose' referenced in table but not provided in dependencies"#,
            );
        }
        _ => panic!("Expected ManifestValidationError, got: {:?}", err),
    }
}

#[tokio::test]
async fn register_existing_dataset_is_idempotent() {
    //* Given
    let ctx = TestCtx::setup("test_register_dataset_idempotent").await;
    let namespace = "_".parse::<Namespace>().expect("valid namespace");
    let name = "register_test_existing_dataset"
        .parse::<Name>()
        .expect("valid dataset name");
    let version = "1.0.0".parse::<Version>().expect("valid version");
    let manifest = create_test_manifest();

    let manifest_str =
        serde_json::to_string(&manifest).expect("failed to serialize manifest to JSON");

    // Register dataset first to create existing state
    let register_result = ctx
        .register_dataset(&namespace, &name, &version, &manifest_str)
        .await;
    assert!(
        register_result.is_ok(),
        "initial registration should succeed: {:?}",
        register_result.err()
    );

    //* When - Register same dataset again
    let register_again = ctx
        .register_dataset(&namespace, &name, &version, &manifest_str)
        .await;

    //* Then
    assert!(
        register_again.is_ok(),
        "registration should be idempotent when same dataset is registered again: {:?}",
        register_again.err()
    );

    // Verify dataset still exists and is accessible
    assert!(
        ctx.verify_dataset_tag_exists(&namespace, &name, &version)
            .await,
        "dataset should still exist after duplicate registration"
    );
}

#[tokio::test]
async fn register_multiple_versions_of_same_dataset_succeeds() {
    //* Given
    let ctx = TestCtx::setup("test_register_multiple_versions").await;

    let namespace = "_".parse::<Namespace>().expect("valid namespace");
    let name = "register_test_multi_version"
        .parse::<Name>()
        .expect("valid dataset name");
    let versions = vec!["1.0.0", "1.1.0", "2.0.0"]
        .into_iter()
        .map(|s| s.parse().expect("valid version"))
        .collect::<Vec<Version>>();

    //* When
    // Register multiple versions of the same dataset
    for version in &versions {
        let manifest = create_test_manifest();
        let manifest_str =
            serde_json::to_string(&manifest).expect("failed to serialize manifest to JSON");

        let result = ctx
            .register_dataset(&namespace, &name, version, &manifest_str)
            .await;

        assert!(
            result.is_ok(),
            "each version registration should succeed for version {}: {:?}",
            version,
            result.err()
        );
    }

    //* Then
    // Verify all versions exist
    for version in &versions {
        assert!(
            ctx.verify_dataset_tag_exists(&namespace, &name, version)
                .await,
            "version {} should exist after registration",
            version
        );
    }
}

struct TestCtx {
    ctx: crate::testlib::ctx::TestCtx,
    ampctl_client: client::Client,
}

impl TestCtx {
    async fn setup(test_name: &str) -> Self {
        let ctx = TestCtxBuilder::new(test_name)
            .with_dataset_manifest(("eth_firehose", "_/eth_firehose@0.0.1"))
            .with_provider_config("firehose_eth_mainnet")
            .build()
            .await
            .expect("failed to build test context");

        let admin_api_url = ctx.daemon_controller().admin_api_url();
        let base_url = admin_api_url
            .parse()
            .expect("failed to parse admin API URL");

        let ampctl_client = client::Client::new(base_url);

        Self { ctx, ampctl_client }
    }

    /// Register a dataset using the ampctl client
    async fn register_dataset(
        &self,
        namespace: &Namespace,
        name: &Name,
        version: impl Into<Option<&Version>>,
        manifest: &str,
    ) -> Result<(), RegisterError> {
        let fqn = FullyQualifiedName::new(namespace.clone(), name.clone());
        let manifest_json: Box<RawValue> =
            serde_json::from_str(manifest).expect("failed to parse manifest JSON");
        self.ampctl_client
            .datasets()
            .register(&fqn, version.into(), manifest_json)
            .await
    }

    async fn verify_dataset_tag_exists(
        &self,
        namespace: &Namespace,
        name: &Name,
        version: &Version,
    ) -> bool {
        metadata_db::datasets::get_version_tag(self.ctx.metadata_db(), namespace, name, version)
            .await
            .expect("failed to check if dataset exists")
            .is_some()
    }
}

fn create_test_manifest() -> DerivedDatasetManifest {
    let manifest_json = indoc::indoc! {r#"
        {
            "kind": "manifest",
            "dependencies": {
                "eth_firehose": "_/eth_firehose@0.0.1"
            },
            "tables": {
                "test_table": {
                    "input": {
                        "sql": "SELECT block_num, miner, hash, parent_hash FROM eth_firehose.blocks"
                    },
                    "schema": {
                        "arrow": {
                            "fields": [
                                {
                                    "name": "_block_num",
                                    "type": "UInt64",
                                    "nullable": false
                                },
                                {
                                    "name": "block_num",
                                    "type": "UInt64",
                                    "nullable": false
                                },
                                {
                                    "name": "miner",
                                    "type": {
                                        "FixedSizeBinary": 20
                                    },
                                    "nullable": false
                                },
                                {
                                    "name": "hash",
                                    "type": {
                                        "FixedSizeBinary": 32
                                    },
                                    "nullable": false
                                },
                                {
                                    "name": "parent_hash",
                                    "type": {
                                        "FixedSizeBinary": 32
                                    },
                                    "nullable": false
                                }
                            ]
                        }
                    },
                    "network": "mainnet"
                }
            },
            "functions": {}
        }
    "#};

    serde_json::from_str(manifest_json).expect("failed to parse manifest JSON")
}
