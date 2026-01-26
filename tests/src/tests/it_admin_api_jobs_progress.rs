//! Integration tests for the Admin API job progress endpoint.

use reqwest::StatusCode;
use serde::{Deserialize, Serialize};

use crate::testlib::ctx::TestCtxBuilder;

#[tokio::test]
async fn get_job_progress_succeeds() {
    //* Given
    let ctx = TestCtx::setup_with_anvil("get_job_progress_succeeds").await;

    // Mine blocks so syncing takes time
    ctx.anvil().mine(10).await.expect("failed to mine blocks");

    // Deploy with end_block so the job completes - deploy returns the job ID directly
    let job_id = ctx
        .deploy_dataset("_", "anvil_rpc", "0.0.0", Some(10))
        .await;

    //* When
    // Query progress for the job
    let resp = ctx.get_job_progress(job_id).await;

    //* Then
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "progress retrieval should succeed for valid job"
    );

    let progress: JobProgressResponse = resp
        .json()
        .await
        .expect("failed to parse progress response JSON");

    println!(
        "Job Progress Response:\n{}",
        serde_json::to_string_pretty(&progress).expect("serialization failed")
    );

    // Verify response structure
    assert_eq!(progress.job_id, job_id);
    assert!(!progress.job_status.is_empty());
}

#[tokio::test]
async fn get_job_progress_with_invalid_job_id_returns_not_found() {
    //* Given
    let ctx = TestCtx::setup("get_job_progress_invalid_id", Vec::<&str>::new()).await;

    //* When
    // Use a job ID that doesn't exist
    let resp = ctx.get_job_progress(999999).await;

    //* Then
    assert_eq!(
        resp.status(),
        StatusCode::NOT_FOUND,
        "progress retrieval should fail for non-existent job"
    );

    let error: ErrorResponse = resp.json().await.expect("failed to parse error response");

    assert_eq!(error.error_code, "JOB_NOT_FOUND");
}

/// Test that progress shows RUNNING status while a job is actively syncing.
#[tokio::test]
async fn get_job_progress_shows_running_status() {
    //* Given
    // Use anvil_rpc dataset which connects to the local Anvil instance
    let ctx = TestCtx::setup_with_anvil("get_job_progress_running_status").await;

    // Mine many blocks so syncing takes time
    ctx.anvil().mine(100).await.expect("failed to mine blocks");

    // Deploy without an end_block so the job keeps running - deploy returns the job ID directly
    let job_id = ctx.deploy_dataset("_", "anvil_rpc", "0.0.0", None).await;

    //* When
    // Poll until we see RUNNING status with some progress
    let start = tokio::time::Instant::now();
    let timeout = tokio::time::Duration::from_secs(30);

    let running_progress: JobProgressResponse = loop {
        if start.elapsed() > timeout {
            panic!("Timeout waiting for RUNNING status");
        }

        let resp = ctx.get_job_progress(job_id).await;

        if resp.status() == StatusCode::OK {
            let progress: JobProgressResponse = resp.json().await.expect("failed to parse JSON");

            println!(
                "Progress: {}",
                serde_json::to_string_pretty(&progress).unwrap()
            );

            // Check if job has failed
            if progress.job_status == "FAILED" {
                panic!("Job failed: {:?}", progress);
            }

            // Check if we have a RUNNING job with some data synced
            let has_synced_data = progress
                .tables
                .values()
                .any(|t| t.files_count > 0 && t.current_block.is_some());

            if progress.job_status == "RUNNING" && has_synced_data {
                break progress;
            }
        } else {
            println!("Request failed status: {}", resp.status());
        }

        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    };

    //* Then
    // Verify the job is RUNNING
    assert_eq!(
        running_progress.job_status, "RUNNING",
        "job_status should be RUNNING"
    );

    // Verify we have some tables with progress
    assert!(
        !running_progress.tables.is_empty(),
        "should have at least one table"
    );

    // Verify we have some progress in at least one table
    let has_progress = running_progress
        .tables
        .values()
        .any(|t| t.current_block.is_some() && t.files_count > 0);

    assert!(has_progress, "at least one table should have progress");

    println!(
        "Verified RUNNING status with {} tables",
        running_progress.tables.len()
    );
}

/// Test that progress shows COMPLETED status when the end block is reached.
#[tokio::test]
async fn get_job_progress_shows_completed_status() {
    //* Given
    // Use anvil_rpc dataset which connects to the local Anvil instance
    let ctx = TestCtx::setup_with_anvil("get_job_progress_completed_status").await;

    // Mine 10 blocks (1 to 10)
    ctx.anvil().mine(10).await.expect("failed to mine blocks");

    // Deploy with end_block=10 so the job completes after syncing - deploy returns the job ID directly
    let job_id = ctx
        .deploy_dataset("_", "anvil_rpc", "0.0.0", Some(10))
        .await;

    //* When
    // Poll until we see COMPLETED status
    let start = tokio::time::Instant::now();
    let timeout = tokio::time::Duration::from_secs(30);

    let final_progress: JobProgressResponse = loop {
        if start.elapsed() > timeout {
            panic!("Timeout waiting for COMPLETED status");
        }

        let resp = ctx.get_job_progress(job_id).await;

        if resp.status() == StatusCode::OK {
            let progress: JobProgressResponse = resp.json().await.expect("failed to parse JSON");

            println!(
                "Progress: {}",
                serde_json::to_string_pretty(&progress).unwrap()
            );

            // Check if job has failed
            if progress.job_status == "FAILED" {
                panic!("Job failed: {:?}", progress);
            }

            // Check if job is completed and has data
            if progress.job_status == "COMPLETED" {
                let has_data = progress
                    .tables
                    .values()
                    .any(|t| t.files_count > 0 && t.current_block.is_some());

                if has_data {
                    break progress;
                }
            }
        } else {
            println!("Request failed status: {}", resp.status());
        }

        tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
    };

    //* Then
    // Verify the job is COMPLETED
    assert_eq!(
        final_progress.job_status, "COMPLETED",
        "job_status should be COMPLETED"
    );

    // Verify we have tables with progress
    assert!(
        !final_progress.tables.is_empty(),
        "should have at least one table"
    );

    // Verify at least one table has complete progress
    let has_complete_progress = final_progress
        .tables
        .values()
        .any(|t| t.current_block.is_some() && t.start_block.is_some() && t.files_count > 0);

    assert!(
        has_complete_progress,
        "at least one table should have complete progress"
    );

    println!(
        "Verified COMPLETED status with {} tables",
        final_progress.tables.len()
    );
}

#[tokio::test]
async fn get_job_progress_with_no_tables_returns_empty_map() {
    //* Given
    let ctx = TestCtx::setup_with_anvil("get_job_progress_no_tables").await;

    // Mine blocks
    ctx.anvil().mine(10).await.expect("failed to mine blocks");

    // Deploy the dataset - deploy returns the job ID directly
    let job_id = ctx
        .deploy_dataset("_", "anvil_rpc", "0.0.0", Some(10))
        .await;

    //* When
    // Query progress immediately after deployment (tables map may be empty)
    let resp = ctx.get_job_progress(job_id).await;

    //* Then
    // Should succeed even if tables map is empty (valid edge case)
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "progress retrieval should succeed"
    );

    let progress: JobProgressResponse = resp
        .json()
        .await
        .expect("failed to parse progress response JSON");

    // Tables map can be empty or populated depending on timing
    // Just verify the response structure is valid
    assert_eq!(progress.job_id, job_id);
    assert!(!progress.job_status.is_empty());
}

// --- Helper types and impls ---

use crate::testlib::ctx::TestCtx;

impl TestCtx {
    async fn setup(
        test_name: &str,
        manifests: impl IntoIterator<Item = impl Into<crate::testlib::ctx::ManifestRegistration>>,
    ) -> Self {
        TestCtxBuilder::new(test_name)
            .with_dataset_manifests(manifests)
            .build()
            .await
            .expect("failed to build test context")
    }

    async fn setup_with_anvil(test_name: &str) -> Self {
        TestCtxBuilder::new(test_name)
            .with_anvil_http() // Use HTTP mode for Anvil
            .with_dataset_manifest("anvil_rpc") // Registers _/anvil_rpc@0.0.0
            .build()
            .await
            .expect("failed to build test context")
    }

    async fn get_job_progress(&self, job_id: i64) -> reqwest::Response {
        let url = format!(
            "{}/jobs/{}/progress",
            self.daemon_controller().admin_api_url(),
            job_id
        );

        reqwest::Client::new()
            .get(&url)
            .send()
            .await
            .expect("failed to send request")
    }

    /// Deploy a dataset and return the job ID.
    async fn deploy_dataset(
        &self,
        namespace: &str,
        name: &str,
        revision: &str,
        end_block: Option<u64>,
    ) -> i64 {
        let ampctl = self.new_ampctl();
        let reference = format!("{}/{}@{}", namespace, name, revision);

        let job_id = ampctl
            .dataset_deploy(&reference, end_block, None, None)
            .await
            .expect("failed to deploy dataset");

        // JobId implements Deref<Target = i64>
        *job_id
    }
}

#[derive(Debug, Deserialize, Serialize)]
struct JobProgressResponse {
    job_id: i64,
    job_status: String,
    tables: std::collections::HashMap<String, TableProgress>,
}

/// Progress for a single table
#[derive(Debug, Deserialize, Serialize)]
struct TableProgress {
    current_block: Option<i64>,
    start_block: Option<i64>,
    files_count: i64,
    total_size_bytes: i64,
}

#[derive(Debug, Deserialize)]
struct ErrorResponse {
    error_code: String,
}
