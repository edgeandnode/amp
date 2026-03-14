//! Integration tests for the DELETE /jobs/{id} and DELETE /jobs?status={filter} endpoints.
//!
//! These tests verify job deletion functionality including:
//! - Deleting completed, stopped, and fatal jobs removes all data (jobs, jobs_status, job_events)
//! - Conflict when deleting non-terminal jobs
//! - Idempotent behavior for nonexistent and already-deleted jobs
//! - Bulk deletion by status filter
//!
//! After deletion, each test queries the `jobs`, `jobs_status`, and `job_events` tables
//! directly via the metadata database to confirm cascade deletes.

use std::time::Duration;

use amp_client_admin::{
    self as client,
    jobs::{DeleteByIdError, JobInfo, JobStatusFilter},
};
use amp_worker_core::jobs::job_id::JobId;
use datasets_common::{
    fqn::FullyQualifiedName, name::Name, namespace::Namespace, reference::Reference,
    revision::Revision, version::Version,
};
use datasets_derived::Manifest as DerivedDatasetManifest;
use serde_json::value::RawValue;

use crate::testlib::{ctx::TestCtxBuilder, helpers::wait_for_job_completion};

// ---------------------------------------------------------------------------
// DELETE /jobs/{id} tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn delete_completed_job_removes_all_data() {
    //* Given
    let ctx = TestCtx::setup_with_anvil("delete_completed_job").await;

    ctx.anvil().mine(5).await.expect("failed to mine blocks");

    let job_id = ctx.deploy_dataset(Some(5)).await;

    wait_for_job_completion(
        &ctx.ctx.new_ampctl(),
        job_id,
        false,
        Duration::from_secs(30),
        Duration::from_millis(100),
    )
    .await
    .expect("job should complete");

    // Verify the job exists before deletion
    let job_before = ctx.inspect_job(&job_id).await;
    assert!(job_before.is_some(), "job should exist before deletion");
    assert_eq!(job_before.unwrap().status, "COMPLETED");

    ctx.assert_job_data_exists(&job_id).await;

    //* When
    ctx.delete_job_by_id(&job_id)
        .await
        .expect("deleting completed job should succeed");

    //* Then
    ctx.assert_job_fully_removed(&job_id).await;
}

#[tokio::test]
async fn delete_stopped_job_removes_all_data() {
    //* Given
    let ctx = TestCtx::setup_with_anvil("delete_stopped_job").await;

    let job_id = ctx.deploy_dataset(None).await;

    tokio::time::sleep(Duration::from_millis(100)).await;

    ctx.stop_job(&job_id)
        .await
        .expect("stopping job should succeed");

    ctx.wait_for_status(&job_id, "STOPPED", Duration::from_secs(30))
        .await;

    ctx.assert_job_data_exists(&job_id).await;

    //* When
    ctx.delete_job_by_id(&job_id)
        .await
        .expect("deleting stopped job should succeed");

    //* Then
    ctx.assert_job_fully_removed(&job_id).await;
}

#[tokio::test]
async fn delete_fatal_job_removes_all_data() {
    //* Given
    let ctx = TestCtx::setup_with_derived("delete_fatal_job").await;

    let job_id = ctx
        .deploy_derived_dataset()
        .await
        .expect("dataset deployment should succeed");

    ctx.wait_for_status(&job_id, "FATAL", Duration::from_secs(30))
        .await;

    ctx.assert_job_data_exists(&job_id).await;

    //* When
    ctx.delete_job_by_id(&job_id)
        .await
        .expect("deleting fatal job should succeed");

    //* Then
    ctx.assert_job_fully_removed(&job_id).await;
}

#[tokio::test]
async fn delete_scheduled_job_returns_conflict() {
    //* Given
    let ctx = TestCtx::setup_with_anvil("delete_scheduled_conflict").await;

    let job_id = ctx.deploy_dataset(None).await;

    // Give scheduler time to create the job but not enough to complete
    tokio::time::sleep(Duration::from_millis(100)).await;

    let job_info = ctx.inspect_job(&job_id).await.expect("job should exist");

    // Only assert conflict if the job is still non-terminal
    if job_info.status == "SCHEDULED" || job_info.status == "RUNNING" {
        //* When
        let result = ctx.delete_job_by_id(&job_id).await;

        //* Then
        assert!(result.is_err(), "deleting non-terminal job should fail");
        match result.unwrap_err() {
            DeleteByIdError::Conflict(_) => {} // expected
            err => panic!("expected Conflict error, got: {:?}", err),
        }
    }
}

#[tokio::test]
async fn delete_nonexistent_job_succeeds() {
    //* Given
    let ctx = TestCtx::setup_with_anvil("delete_nonexistent_job").await;
    let fake_job_id = JobId::try_from(999999i64).expect("valid job ID");

    //* When
    let result = ctx.delete_job_by_id(&fake_job_id).await;

    //* Then
    assert!(
        result.is_ok(),
        "deleting nonexistent job should succeed (idempotent): {:?}",
        result.err()
    );
}

#[tokio::test]
async fn delete_already_deleted_job_is_idempotent() {
    //* Given
    let ctx = TestCtx::setup_with_anvil("delete_already_deleted").await;

    ctx.anvil().mine(3).await.expect("failed to mine blocks");

    let job_id = ctx.deploy_dataset(Some(3)).await;

    wait_for_job_completion(
        &ctx.ctx.new_ampctl(),
        job_id,
        false,
        Duration::from_secs(30),
        Duration::from_millis(100),
    )
    .await
    .expect("job should complete");

    ctx.delete_job_by_id(&job_id)
        .await
        .expect("first delete should succeed");

    //* When
    let result = ctx.delete_job_by_id(&job_id).await;

    //* Then
    assert!(
        result.is_ok(),
        "second delete should succeed (idempotent): {:?}",
        result.err()
    );
}

// ---------------------------------------------------------------------------
// DELETE /jobs?status={filter} tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn delete_by_status_terminal_removes_all_terminal_jobs() {
    //* Given
    let ctx = TestCtx::setup_with_anvil("delete_terminal_jobs").await;

    ctx.anvil().mine(5).await.expect("failed to mine blocks");

    let job_id = ctx.deploy_dataset(Some(5)).await;

    wait_for_job_completion(
        &ctx.ctx.new_ampctl(),
        job_id,
        false,
        Duration::from_secs(30),
        Duration::from_millis(100),
    )
    .await
    .expect("job should complete");

    //* When
    ctx.delete_jobs_by_status(Some(&JobStatusFilter::Terminal))
        .await
        .expect("deleting terminal jobs should succeed");

    //* Then
    ctx.assert_job_fully_removed(&job_id).await;
}

#[tokio::test]
async fn delete_by_status_completed_only_removes_completed_jobs() {
    //* Given
    let ctx = TestCtx::setup_with_anvil_multi("delete_completed_only").await;

    ctx.anvil().mine(5).await.expect("failed to mine blocks");

    // Deploy and complete a job using anvil_rpc
    let completed_job_id = ctx.deploy_dataset_by_name("anvil_rpc", Some(5)).await;

    wait_for_job_completion(
        &ctx.ctx.new_ampctl(),
        completed_job_id,
        false,
        Duration::from_secs(30),
        Duration::from_millis(100),
    )
    .await
    .expect("completed job should complete");

    // Deploy and stop a second job using anvil_rpc_finalized
    let stopped_job_id = ctx
        .deploy_dataset_by_name("anvil_rpc_finalized", None)
        .await;

    tokio::time::sleep(Duration::from_millis(100)).await;

    ctx.stop_job(&stopped_job_id)
        .await
        .expect("stopping job should succeed");

    ctx.wait_for_status(&stopped_job_id, "STOPPED", Duration::from_secs(30))
        .await;

    //* When
    ctx.delete_jobs_by_status(Some(&JobStatusFilter::Completed))
        .await
        .expect("deleting completed jobs should succeed");

    //* Then — completed job removed, stopped job still exists
    ctx.assert_job_fully_removed(&completed_job_id).await;

    let stopped_job = ctx.inspect_job(&stopped_job_id).await;
    assert!(
        stopped_job.is_some(),
        "stopped job should still exist after deleting only completed jobs"
    );
    assert_eq!(stopped_job.unwrap().status, "STOPPED");
}

// ---------------------------------------------------------------------------
// Test helper
// ---------------------------------------------------------------------------

struct TestCtx {
    ctx: crate::testlib::ctx::TestCtx,
    dataset_ref: Reference,
    ampctl_client: client::Client,
}

impl TestCtx {
    async fn setup_with_anvil(test_name: &str) -> Self {
        let ctx = TestCtxBuilder::new(test_name)
            .with_anvil_http()
            .with_dataset_manifest("anvil_rpc")
            .build()
            .await
            .expect("failed to build test context");

        let admin_api_url = ctx.daemon_controller().admin_api_url();
        let base_url = admin_api_url
            .parse()
            .expect("failed to parse admin API URL");

        let ampctl_client = client::Client::new(base_url);

        let dataset_ref: Reference = "_/anvil_rpc@0.0.0"
            .parse()
            .expect("failed to parse dataset reference");

        Self {
            ctx,
            dataset_ref,
            ampctl_client,
        }
    }

    async fn setup_with_anvil_multi(test_name: &str) -> Self {
        let ctx = TestCtxBuilder::new(test_name)
            .with_anvil_http()
            .with_dataset_manifests(["anvil_rpc", "anvil_rpc_finalized"])
            .build()
            .await
            .expect("failed to build test context");

        let admin_api_url = ctx.daemon_controller().admin_api_url();
        let base_url = admin_api_url
            .parse()
            .expect("failed to parse admin API URL");

        let ampctl_client = client::Client::new(base_url);

        let dataset_ref: Reference = "_/anvil_rpc@0.0.0"
            .parse()
            .expect("failed to parse dataset reference");

        Self {
            ctx,
            dataset_ref,
            ampctl_client,
        }
    }

    async fn setup_with_derived(test_name: &str) -> Self {
        let dataset_ref: Reference = "_/eth_firehose@0.0.1"
            .parse()
            .expect("failed to parse dataset reference");

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
            ctx,
            dataset_ref,
            ampctl_client,
        }
    }

    fn anvil(&self) -> &crate::testlib::fixtures::Anvil {
        self.ctx.anvil()
    }

    async fn deploy_dataset(&self, end_block: Option<u64>) -> JobId {
        let name = self.dataset_ref.name();
        self.deploy_dataset_by_name(name, end_block).await
    }

    async fn deploy_dataset_by_name(&self, name: &str, end_block: Option<u64>) -> JobId {
        let ampctl = self.ctx.new_ampctl();
        let reference = format!("_/{}@0.0.0", name);

        ampctl
            .dataset_deploy(&reference, end_block, None, None, false)
            .await
            .expect("failed to deploy dataset")
    }

    async fn deploy_derived_dataset(&self) -> Result<JobId, client::datasets::DeployError> {
        let namespace = "_".parse::<Namespace>().expect("valid namespace");
        let name = "failing_derived_delete_test"
            .parse::<Name>()
            .expect("valid dataset name");
        let version = "1.0.0".parse::<Version>().expect("valid version");

        let manifest = create_derived_manifest();
        let manifest_str =
            serde_json::to_string(&manifest).expect("failed to serialize manifest to JSON");

        self.register_dataset(&namespace, &name, &version, &manifest_str)
            .await
            .expect("dataset registration should succeed");

        tokio::time::sleep(Duration::from_millis(100)).await;

        let reference = Reference::new(
            namespace.clone(),
            name.clone(),
            Revision::Version(version.clone()),
        );
        self.ampctl_client
            .datasets()
            .deploy(&reference, None, 1, None, false)
            .await
    }

    async fn register_dataset(
        &self,
        namespace: &Namespace,
        name: &Name,
        version: &Version,
        manifest: &str,
    ) -> Result<client::datasets::RegisterResponse, client::datasets::RegisterError> {
        let fqn = FullyQualifiedName::new(namespace.clone(), name.clone());
        let manifest_json: Box<RawValue> =
            serde_json::from_str(manifest).expect("failed to parse manifest JSON");
        self.ampctl_client
            .datasets()
            .register(&fqn, Some(version), manifest_json)
            .await
    }

    async fn delete_job_by_id(&self, job_id: &JobId) -> Result<(), DeleteByIdError> {
        self.ampctl_client.jobs().delete_by_id(job_id).await
    }

    async fn delete_jobs_by_status(
        &self,
        status: Option<&JobStatusFilter>,
    ) -> Result<(), client::jobs::DeleteByStatusError> {
        self.ampctl_client.jobs().delete(status).await
    }

    async fn inspect_job(&self, job_id: &JobId) -> Option<JobInfo> {
        self.ampctl_client
            .jobs()
            .get(job_id)
            .await
            .expect("failed to get job")
    }

    async fn stop_job(&self, job_id: &JobId) -> Result<(), client::jobs::StopError> {
        self.ampctl_client.jobs().stop(job_id).await
    }

    async fn wait_for_status(&self, job_id: &JobId, expected: &str, timeout: Duration) {
        let start = tokio::time::Instant::now();
        let poll_interval = Duration::from_millis(200);
        loop {
            let job = self.inspect_job(job_id).await;
            if let Some(ref info) = job
                && info.status == expected
            {
                return;
            }
            if start.elapsed() > timeout {
                panic!(
                    "timeout waiting for job {} to reach status {}, current: {:?}",
                    job_id,
                    expected,
                    job.map(|j| j.status)
                );
            }
            tokio::time::sleep(poll_interval).await;
        }
    }

    /// Verify that data exists for this job in all three tables before deletion.
    async fn assert_job_data_exists(&self, job_id: &JobId) {
        let db = self.ctx.metadata_db();
        let id = **job_id;

        let jobs_row: Option<(i64,)> = sqlx::query_as("SELECT id FROM jobs WHERE id = $1")
            .bind(id)
            .fetch_optional(db)
            .await
            .expect("failed to query jobs table");
        assert!(
            jobs_row.is_some(),
            "job {} should exist in jobs table before deletion",
            job_id
        );

        let status_row: Option<(i64,)> =
            sqlx::query_as("SELECT job_id FROM jobs_status WHERE job_id = $1")
                .bind(id)
                .fetch_optional(db)
                .await
                .expect("failed to query jobs_status table");
        assert!(
            status_row.is_some(),
            "job {} should exist in jobs_status table before deletion",
            job_id
        );

        let events_row: Option<(i64,)> =
            sqlx::query_as("SELECT job_id FROM job_events WHERE job_id = $1 LIMIT 1")
                .bind(id)
                .fetch_optional(db)
                .await
                .expect("failed to query job_events table");
        assert!(
            events_row.is_some(),
            "job {} should have events in job_events table before deletion",
            job_id
        );
    }

    /// Verify that no data remains for this job in any of the three tables.
    async fn assert_job_fully_removed(&self, job_id: &JobId) {
        let db = self.ctx.metadata_db();
        let id = **job_id;

        // jobs table
        let jobs_row: Option<(i64,)> = sqlx::query_as("SELECT id FROM jobs WHERE id = $1")
            .bind(id)
            .fetch_optional(db)
            .await
            .expect("failed to query jobs table");
        assert!(
            jobs_row.is_none(),
            "job {} should not exist in jobs table after deletion",
            job_id
        );

        // jobs_status table
        let status_row: Option<(i64,)> =
            sqlx::query_as("SELECT job_id FROM jobs_status WHERE job_id = $1")
                .bind(id)
                .fetch_optional(db)
                .await
                .expect("failed to query jobs_status table");
        assert!(
            status_row.is_none(),
            "job {} should not exist in jobs_status table after deletion",
            job_id
        );

        // job_events table
        let events_row: Option<(i64,)> =
            sqlx::query_as("SELECT job_id FROM job_events WHERE job_id = $1 LIMIT 1")
                .bind(id)
                .fetch_optional(db)
                .await
                .expect("failed to query job_events table");
        assert!(
            events_row.is_none(),
            "job {} should have no events in job_events table after deletion",
            job_id
        );
    }
}

fn create_derived_manifest() -> DerivedDatasetManifest {
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

    serde_json::from_str(manifest_json).expect("failed to parse derived manifest JSON")
}
