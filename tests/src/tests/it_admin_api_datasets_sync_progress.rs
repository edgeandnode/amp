//! Integration tests for the Admin API dataset sync progress endpoint.

use reqwest::StatusCode;
use serde::{Deserialize, Serialize};

use crate::testlib::ctx::TestCtxBuilder;

#[tokio::test]
async fn get_sync_progress_with_valid_dataset_succeeds() {
    //* Given
    let ctx = TestCtx::setup(
        "get_sync_progress_with_valid_dataset",
        [("eth_rpc", "_/eth_rpc@0.0.0")],
    )
    .await;

    //* When
    // Using default namespace '_' and version '0.0.0'
    let resp = ctx.get_sync_progress("_", "eth_rpc", "0.0.0").await;

    //* Then
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "sync progress retrieval should succeed for valid dataset"
    );

    let progress: SyncProgressResponse = resp
        .json()
        .await
        .expect("failed to parse sync progress response JSON");

    println!(
        "Sync Progress Response:\n{}",
        serde_json::to_string_pretty(&progress).expect("serialization failed")
    );

    // Verify response structure
    assert_eq!(progress.dataset_namespace, "_");
    assert_eq!(progress.dataset_name, "eth_rpc");
    assert_eq!(progress.revision, "0.0.0");
    assert!(!progress.manifest_hash.is_empty());

    // Check tables if present
    for table in progress.tables {
        assert!(!table.table_name.is_empty());
        // Initial state will have zero files (no job deployed yet)
        assert!(table.files_count >= 0);
    }
}

#[tokio::test]
async fn get_sync_progress_with_non_existent_dataset_returns_not_found() {
    //* Given
    let ctx = TestCtx::setup("get_sync_progress_non_existent", Vec::<&str>::new()).await;

    //* When
    let resp = ctx
        .get_sync_progress("non_existent", "dataset", "latest")
        .await;

    //* Then
    assert_eq!(
        resp.status(),
        StatusCode::NOT_FOUND,
        "sync progress retrieval should fail for non-existent dataset"
    );

    let error: ErrorResponse = resp.json().await.expect("failed to parse error response");

    assert_eq!(error.error_code, "DATASET_NOT_FOUND");
}

#[tokio::test]
async fn get_sync_progress_returns_progress_for_running_job() {
    //* Given
    // Use anvil_rpc dataset which connects to the local Anvil instance
    let ctx = TestCtx::setup_with_anvil("get_sync_progress_running").await;

    // Mine some blocks so there is data to sync
    ctx.anvil().mine(10).await.expect("failed to mine blocks");

    // Deploy the dataset to start the sync job
    ctx.deploy_dataset("_", "anvil_rpc", "0.0.0").await;

    // Expected tables from anvil_rpc manifest
    let expected_tables: std::collections::HashSet<&str> =
        ["blocks", "logs", "transactions"].into_iter().collect();

    // Wait for the job to start and sync some data
    // We poll the sync progress until we see progress or timeout
    let start = tokio::time::Instant::now();
    let timeout = tokio::time::Duration::from_secs(30);

    let final_progress: SyncProgressResponse = loop {
        if start.elapsed() > timeout {
            panic!("Timeout waiting for sync progress");
        }

        let resp = ctx.get_sync_progress("_", "anvil_rpc", "0.0.0").await;
        if resp.status() == StatusCode::OK {
            let progress: SyncProgressResponse = resp.json().await.expect("failed to parse JSON");

            println!(
                "Sync Progress: {}",
                serde_json::to_string_pretty(&progress).unwrap()
            );

            // Check if any job has failed
            for table in &progress.tables {
                if table.job_status.as_deref() == Some("FAILED") {
                    panic!(
                        "Job failed for table '{}': {:?}",
                        table.table_name, progress
                    );
                }
            }

            // Check if the blocks table has files with block range metadata populated
            // We specifically wait for current_block to be populated, not just files_count > 0
            if let Some(blocks_table) = progress
                .tables
                .iter()
                .find(|t| t.table_name == "blocks")
                .filter(|t| t.files_count > 0 && t.current_block.is_some())
            {
                let _ = blocks_table; // used to confirm we found a matching table
                break progress;
            }
        } else {
            println!("Request failed status: {}", resp.status());
        }

        tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
    };

    //* Then
    // Verify all expected tables are present
    let actual_tables: std::collections::HashSet<&str> = final_progress
        .tables
        .iter()
        .map(|t| t.table_name.as_str())
        .collect();

    assert_eq!(
        actual_tables, expected_tables,
        "all expected tables should be present in sync progress"
    );

    // Verify each table has the expected progress data
    for table in &final_progress.tables {
        // Every table should have a job assigned
        assert!(
            table.job_id.is_some(),
            "table '{}' should have a job_id",
            table.table_name
        );

        // Every table should have a job status
        let status = table
            .job_status
            .as_ref()
            .unwrap_or_else(|| panic!("table '{}' should have a job_status", table.table_name));

        // Job should be running or completed (not failed, not pending)
        assert!(
            status == "RUNNING" || status == "COMPLETED",
            "table '{}' job_status should be RUNNING or COMPLETED, got '{}'",
            table.table_name,
            status
        );
    }

    // Specifically verify the blocks table has complete progress tracking
    let blocks_table = final_progress
        .tables
        .iter()
        .find(|t| t.table_name == "blocks")
        .expect("blocks table should exist");

    // Verify block range tracking
    let current_block = blocks_table
        .current_block
        .expect("blocks table should have current_block");
    let start_block = blocks_table
        .start_block
        .expect("blocks table should have start_block");

    assert!(
        start_block >= 0,
        "start_block should be non-negative, got {}",
        start_block
    );
    assert!(
        current_block >= start_block,
        "current_block ({}) should be >= start_block ({})",
        current_block,
        start_block
    );

    // Verify file statistics
    assert!(
        blocks_table.files_count > 0,
        "blocks table should have at least one file"
    );
    assert!(
        blocks_table.total_size_bytes > 0,
        "blocks table should have non-zero total_size_bytes"
    );

    println!(
        "Sync progress verified: blocks synced from {} to {}, {} files, {} bytes",
        start_block, current_block, blocks_table.files_count, blocks_table.total_size_bytes
    );
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

    async fn get_sync_progress(
        &self,
        namespace: &str,
        name: &str,
        revision: &str,
    ) -> reqwest::Response {
        let url = format!(
            "{}/datasets/{}/{}/versions/{}/sync-progress",
            self.daemon_controller().admin_api_url(),
            namespace,
            name,
            revision
        );

        reqwest::Client::new()
            .get(&url)
            .send()
            .await
            .expect("failed to send request")
    }

    async fn deploy_dataset(&self, namespace: &str, name: &str, revision: &str) {
        let ampctl = self.new_ampctl();
        let reference = format!("{}/{}@{}", namespace, name, revision);

        ampctl
            .dataset_deploy(&reference, None, None, None)
            .await
            .expect("failed to deploy dataset");
    }
}

#[derive(Debug, Deserialize, Serialize)]
struct SyncProgressResponse {
    dataset_namespace: String,
    dataset_name: String,
    revision: String,
    manifest_hash: String,
    tables: Vec<TableSyncProgress>,
}

#[derive(Debug, Deserialize, Serialize)]
struct TableSyncProgress {
    table_name: String,
    current_block: Option<i64>,
    start_block: Option<i64>,
    job_id: Option<i64>,
    job_status: Option<String>,
    files_count: i64,
    total_size_bytes: i64,
}

#[derive(Debug, Deserialize)]
struct ErrorResponse {
    error_code: String,
}
