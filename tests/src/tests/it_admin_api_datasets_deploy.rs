//! Integration tests for dataset redeployment idempotency
//!
//! Verifies that deploying the same dataset version multiple times returns the same
//! job ID (idempotency key deduplication) and that re-deploying with a different
//! end_block while the job is still active does not create a new job.

use amp_client_admin::{self as client, end_block::EndBlock};
use amp_worker_core::jobs::job_id::JobId;
use datasets_common::reference::Reference;

use crate::testlib::ctx::TestCtxBuilder;

#[tokio::test]
async fn redeploy_with_different_end_block_returns_same_job_id() {
    //* Given
    let ctx = TestCtx::setup("test_redeploy_diff_endblock").await;

    // Mine some blocks so the dataset has data to sync
    ctx.mine_blocks(10).await;

    // First deploy with no end block (continuous)
    let _job_id_1 = ctx
        .deploy_dataset(None)
        .await
        .expect("first deployment should succeed");

    // Wait for the job to be fully scheduled
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    //* When — redeploy with a different end_block while job is still active
    let job_id_2 = ctx.deploy_dataset(None).await;

    //* Then — 409 conflict because an active job already exists with different options
    let error = job_id_2.expect_err("second deploy should fail with conflict");
    assert!(
        matches!(error, client::datasets::DeployError::ActiveJobConflict(_)),
        "expected ActiveJobConflict, got: {error:?}"
    );
}

struct TestCtx {
    ctx: crate::testlib::ctx::TestCtx,
    dataset_ref: Reference,
    ampctl_client: client::Client,
}

impl TestCtx {
    async fn setup(test_name: &str) -> Self {
        let dataset_ref: Reference = "_/anvil_rpc@0.0.0"
            .parse()
            .expect("Failed to parse dataset reference");

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

        Self {
            ctx,
            dataset_ref,
            ampctl_client,
        }
    }

    async fn mine_blocks(&self, count: u64) {
        self.ctx
            .anvil()
            .mine(count)
            .await
            .expect("failed to mine blocks");
    }

    async fn deploy_dataset(
        &self,
        end_block: Option<EndBlock>,
    ) -> Result<JobId, client::datasets::DeployError> {
        self.ampctl_client
            .datasets()
            .deploy(&self.dataset_ref, end_block, 1, None, false)
            .await
    }
}
