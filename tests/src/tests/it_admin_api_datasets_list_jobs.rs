use ampctl::client::{self, datasets::ListJobsError};
use datasets_common::{
    fqn::FullyQualifiedName, name::Name, namespace::Namespace, reference::Reference,
    revision::Revision, version::Version,
};
use datasets_derived::Manifest as DerivedDatasetManifest;
use serde_json::value::RawValue;

use crate::testlib::ctx::TestCtxBuilder;

#[tokio::test]
async fn list_jobs_for_dataset_without_jobs_returns_empty() {
    //* Given
    let ctx = TestCtx::setup("test_list_jobs_no_jobs").await;
    let namespace = "_".parse::<Namespace>().expect("valid namespace");
    let name = "no_jobs_test".parse::<Name>().expect("valid dataset name");
    let version = "1.0.0".parse::<Version>().expect("valid version");

    // Register dataset but don't deploy (no jobs scheduled)
    let manifest = create_test_manifest();
    let manifest_str =
        serde_json::to_string(&manifest).expect("failed to serialize manifest to JSON");
    ctx.register_dataset(&namespace, &name, &version, &manifest_str)
        .await
        .expect("dataset registration should succeed");

    //* When
    let reference = Reference::new(
        namespace.clone(),
        name.clone(),
        Revision::Version(version.clone()),
    );
    let result = ctx.list_jobs(&reference).await;

    //* Then
    assert!(
        result.is_ok(),
        "list_jobs should succeed even with no jobs: {:?}",
        result.err()
    );
    let jobs = result.expect("should return empty jobs array");
    assert!(
        jobs.is_empty(),
        "should return empty jobs array when no jobs exist"
    );
}

#[tokio::test]
async fn list_jobs_for_nonexistent_dataset_returns_404() {
    //* Given
    let ctx = TestCtx::setup("test_list_jobs_not_found").await;
    let namespace = "_".parse::<Namespace>().expect("valid namespace");
    let name = "nonexistent".parse::<Name>().expect("valid dataset name");
    let version = "1.0.0".parse::<Version>().expect("valid version");

    //* When
    let reference = Reference::new(
        namespace.clone(),
        name.clone(),
        Revision::Version(version.clone()),
    );
    let result = ctx.list_jobs(&reference).await;

    //* Then
    assert!(
        result.is_err(),
        "list_jobs should fail for nonexistent dataset"
    );
    let err = result.unwrap_err();
    match err {
        ListJobsError::DatasetNotFound(api_err) => {
            assert_eq!(
                api_err.error_code, "DATASET_NOT_FOUND",
                "Expected DATASET_NOT_FOUND error code, got: {}",
                api_err.error_code
            );
        }
        _ => panic!("Expected DatasetNotFound variant, got: {:?}", err),
    }
}

#[tokio::test]
async fn list_jobs_for_deployed_dataset_returns_jobs() {
    //* Given
    let ctx = TestCtx::setup("test_list_jobs_deployed").await;
    let namespace = "_".parse::<Namespace>().expect("valid namespace");
    let name = "list_jobs_test"
        .parse::<Name>()
        .expect("valid dataset name");
    let version = "1.0.0".parse::<Version>().expect("valid version");

    // Register dataset
    let manifest = create_test_manifest();
    let manifest_str =
        serde_json::to_string(&manifest).expect("failed to serialize manifest to JSON");
    ctx.register_dataset(&namespace, &name, &version, &manifest_str)
        .await
        .expect("dataset registration should succeed");

    // Wait for worker to be registered and ready before deploying
    // The worker needs time to start up, register, and send its first heartbeat
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // Deploy dataset (schedules a job)
    ctx.deploy_dataset(&namespace, &name, &version)
        .await
        .expect("dataset deployment should succeed");

    // Give job scheduler time to process
    // Note: Job scheduling involves worker selection, database writes, location creation,
    // and async notification processing. Allow sufficient time for all operations.
    tokio::time::sleep(tokio::time::Duration::from_millis(250)).await;

    //* When
    let reference = Reference::new(
        namespace.clone(),
        name.clone(),
        Revision::Version(version.clone()),
    );
    let result = ctx.list_jobs(&reference).await;

    //* Then
    assert!(
        result.is_ok(),
        "list_jobs should succeed: {:?}",
        result.err()
    );
    let jobs = result.expect("should return jobs");
    assert!(
        !jobs.is_empty(),
        "should return at least one job after deployment"
    );

    // Verify job properties
    let job = &jobs[0];
    assert!(!job.id.to_string().is_empty(), "job should have a valid ID");
    assert!(!job.status.is_empty(), "job should have a status");
    assert!(
        !job.created_at.is_empty(),
        "job should have a created_at timestamp"
    );
    assert!(
        !job.updated_at.is_empty(),
        "job should have an updated_at timestamp"
    );

    // Verify job descriptor references the dataset
    let descriptor_str = job.descriptor.to_string();
    assert!(
        descriptor_str.contains(&name.to_string()),
        "job descriptor should reference the dataset name, got: {}",
        descriptor_str
    );
}

struct TestCtx {
    _ctx: crate::testlib::ctx::TestCtx,
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

        Self {
            _ctx: ctx,
            ampctl_client,
        }
    }

    /// Register a dataset using the ampctl client
    async fn register_dataset(
        &self,
        namespace: &Namespace,
        name: &Name,
        version: &Version,
        manifest: &str,
    ) -> Result<(), client::datasets::RegisterError> {
        let fqn = FullyQualifiedName::new(namespace.clone(), name.clone());
        let manifest_json: Box<RawValue> =
            serde_json::from_str(manifest).expect("failed to parse manifest JSON");
        self.ampctl_client
            .datasets()
            .register(&fqn, Some(version), manifest_json)
            .await
    }

    async fn deploy_dataset(
        &self,
        namespace: &Namespace,
        name: &Name,
        version: &Version,
    ) -> Result<worker::job::JobId, client::datasets::DeployError> {
        let reference = Reference::new(
            namespace.clone(),
            name.clone(),
            Revision::Version(version.clone()),
        );
        self.ampctl_client
            .datasets()
            .deploy(&reference, None, 1, None)
            .await
    }

    async fn list_jobs(
        &self,
        reference: &Reference,
    ) -> Result<Vec<client::jobs::JobInfo>, client::datasets::ListJobsError> {
        self.ampctl_client.datasets().list_jobs(reference).await
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
