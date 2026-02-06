//! Integration tests for the PUT /jobs/{id}/stop endpoint.
//!
//! These tests verify the job stop functionality including:
//! - Stopping a running/scheduled job
//! - Idempotent behavior when stopping already stopped jobs
//! - Idempotent behavior for jobs in terminal states (completed, failed)
//! - Error handling for nonexistent jobs

use ampctl::client::{
    self,
    jobs::{JobInfo, StopError},
};
use datasets_common::{
    end_block::EndBlock, fqn::FullyQualifiedName, name::Name, namespace::Namespace,
    reference::Reference, revision::Revision, version::Version,
};
use datasets_derived::Manifest as DerivedDatasetManifest;
use serde_json::value::RawValue;
use worker::job::JobId;

use crate::testlib::ctx::TestCtxBuilder;

#[tokio::test]
async fn stop_nonexistent_job_returns_404() {
    //* Given
    let ctx = TestCtx::setup("test_stop_nonexistent").await;
    let fake_job_id = JobId::try_from(999999i64).expect("valid job ID");

    //* When
    let result = ctx.stop_job(&fake_job_id).await;

    //* Then
    assert!(result.is_err(), "stop should fail for nonexistent job");
    let err = result.unwrap_err();
    match err {
        StopError::NotFound(api_err) => {
            assert_eq!(
                api_err.error_code, "JOB_NOT_FOUND",
                "Expected JOB_NOT_FOUND error code, got: {}",
                api_err.error_code
            );
        }
        _ => panic!("Expected NotFound error, got: {:?}", err),
    }
}

#[tokio::test]
async fn stop_running_job_succeeds() {
    //* Given
    let ctx = TestCtx::setup("test_stop_running").await;

    // Deploy dataset (schedules a job)
    let job_id = ctx
        .deploy_dataset(None)
        .await
        .expect("dataset deployment should succeed");

    // Give job scheduler time to process
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    //* When
    let job_info_before = ctx
        .inspect_job(&job_id)
        .await
        .expect("failed to inspect job");
    let result = ctx.stop_job(&job_id).await;

    // Wait for job to transition to stopped state
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    let job_info_after = ctx
        .inspect_job(&job_id)
        .await
        .expect("failed to inspect job");

    //* Then
    assert!(
        job_info_before.status == "SCHEDULED" || job_info_before.status == "RUNNING",
        "job should be in SCHEDULED or RUNNING state before stop, got: {}",
        job_info_before.status
    );
    assert!(
        result.is_ok(),
        "stopping running job should succeed: {:?}",
        result.err()
    );
    assert_eq!(
        job_info_after.status, "STOPPED",
        "job should be in STOPPED state after stop, got: {}",
        job_info_after.status
    );
}

#[tokio::test]
async fn stop_already_stopped_job_is_idempotent() {
    //* Given
    let ctx = TestCtx::setup("test_stop_already_stopped").await;

    // Deploy dataset (schedules a job)
    let job_id = ctx
        .deploy_dataset(None)
        .await
        .expect("dataset deployment should succeed");

    // Give job scheduler time to process
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // Stop the job first time
    ctx.stop_job(&job_id)
        .await
        .expect("first stop should succeed");

    // Wait for job to transition to stopped state
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    //* When
    let job_info_before = ctx
        .inspect_job(&job_id)
        .await
        .expect("failed to inspect job");
    let result = ctx.stop_job(&job_id).await;
    let job_info_after = ctx
        .inspect_job(&job_id)
        .await
        .expect("failed to inspect job");

    //* Then
    assert_eq!(
        job_info_before.status, "STOPPED",
        "job should be in STOPPED state before second stop"
    );
    assert!(
        result.is_ok(),
        "stopping already stopped job should succeed (idempotent): {:?}",
        result.err()
    );
    assert_eq!(
        job_info_after.status, "STOPPED",
        "job should remain in STOPPED state after second stop"
    );
}

#[tokio::test]
async fn stop_completed_job_is_idempotent() {
    //* Given
    let ctx = TestCtx::setup("test_stop_completed").await;

    // Deploy dataset with end block to allow completion
    let job_id = ctx
        .deploy_dataset(Some(EndBlock::Absolute(15000001)))
        .await
        .expect("dataset deployment should succeed");

    // Wait for job to complete
    let timeout = tokio::time::Duration::from_secs(30);
    let poll_interval = tokio::time::Duration::from_millis(200);
    let start = tokio::time::Instant::now();
    loop {
        let job_info = ctx
            .inspect_job(&job_id)
            .await
            .expect("failed to inspect job");
        if job_info.status == "COMPLETED" {
            break;
        }
        if start.elapsed() > timeout {
            panic!(
                "Timeout waiting for job to complete, current status: {}",
                job_info.status
            );
        }
        tokio::time::sleep(poll_interval).await;
    }

    //* When
    let result = ctx.stop_job(&job_id).await;
    let job_info_after = ctx
        .inspect_job(&job_id)
        .await
        .expect("failed to inspect job");

    //* Then
    assert!(
        result.is_ok(),
        "stopping completed job should succeed (idempotent): {:?}",
        result.err()
    );
    assert_eq!(
        job_info_after.status, "COMPLETED",
        "job should remain in COMPLETED state after stop attempt"
    );
}

#[tokio::test]
async fn stop_failed_job_is_idempotent() {
    //* Given
    let ctx = TestCtx::setup_with_derived("test_stop_failed").await;

    // Deploy derived dataset (will fail due to base dataset has not been deployed)
    let job_id = ctx
        .deploy_derived_dataset()
        .await
        .expect("dataset deployment should succeed");

    // Wait for job to fail
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    //* When
    let job_info_before = ctx
        .inspect_job(&job_id)
        .await
        .expect("failed to inspect job");
    let result = ctx.stop_job(&job_id).await;
    let job_info_after = ctx
        .inspect_job(&job_id)
        .await
        .expect("failed to inspect job");

    //* Then
    assert_eq!(
        job_info_before.status, "FAILED",
        "job should be in FAILED state, got: {}",
        job_info_before.status
    );
    assert!(
        result.is_ok(),
        "stopping failed job should succeed (idempotent): {:?}",
        result.err()
    );
    assert_eq!(
        job_info_after.status, "FAILED",
        "job should remain in FAILED state after stop attempt"
    );
}

struct TestCtx {
    _ctx: crate::testlib::ctx::TestCtx,
    dataset_ref: Reference,
    ampctl_client: client::Client,
}

impl TestCtx {
    async fn setup(test_name: &str) -> Self {
        // Use a raw dataset (like eth_rpc) instead of a derived manifest
        let dataset_ref: Reference = "_/eth_rpc@0.0.0"
            .parse()
            .expect("Failed to parse dataset reference");

        let ctx = TestCtxBuilder::new(test_name)
            .with_dataset_manifest(dataset_ref.name().to_string())
            .with_provider_config("rpc_eth_mainnet")
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
            dataset_ref,
            ampctl_client,
        }
    }

    async fn setup_with_derived(test_name: &str) -> Self {
        // Use eth_firehose as the base dataset for derived manifests
        let dataset_ref: Reference = "_/eth_firehose@0.0.1"
            .parse()
            .expect("Failed to parse dataset reference");

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
            dataset_ref,
            ampctl_client,
        }
    }

    async fn deploy_dataset(
        &self,
        end_block: Option<EndBlock>,
    ) -> Result<JobId, client::datasets::DeployError> {
        self.ampctl_client
            .datasets()
            .deploy(&self.dataset_ref, end_block, 1, None)
            .await
    }

    async fn deploy_derived_dataset(&self) -> Result<JobId, client::datasets::DeployError> {
        let namespace = "_".parse::<Namespace>().expect("valid namespace");
        let name = "failing_derived_test"
            .parse::<Name>()
            .expect("valid dataset name");
        let version = "1.0.0".parse::<Version>().expect("valid version");

        let manifest = create_manifest();
        let manifest_str =
            serde_json::to_string(&manifest).expect("failed to serialize manifest to JSON");

        self.register_dataset(&namespace, &name, &version, &manifest_str)
            .await
            .expect("dataset registration should succeed");

        // Wait for worker to be ready
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

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

    async fn stop_job(&self, job_id: &JobId) -> Result<(), StopError> {
        self.ampctl_client.jobs().stop(job_id).await
    }

    async fn inspect_job(&self, job_id: &JobId) -> Result<JobInfo, client::jobs::GetError> {
        self.ampctl_client
            .jobs()
            .get(job_id)
            .await
            .map(|job| job.unwrap())
    }
}

fn create_manifest() -> DerivedDatasetManifest {
    let manifest_json = indoc::indoc! {r#"
        {
            "kind": "manifest",
            "dependencies": {
                "eth_firehose": "_/eth_firehose@0.0.1"
            },
            "tables": {
                "failing_table": {
                    "input": {
                        "sql": "SELECT block_num FROM eth_firehose.blocks"
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
