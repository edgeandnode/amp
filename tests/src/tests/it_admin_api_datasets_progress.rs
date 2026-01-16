//! Integration tests for the Admin API dataset progress endpoint.

use reqwest::StatusCode;
use serde::{Deserialize, Serialize};

use crate::testlib::ctx::TestCtxBuilder;

#[tokio::test]
async fn get_progress_with_valid_dataset_succeeds() {
    //* Given
    let ctx = TestCtx::setup(
        "get_progress_with_valid_dataset",
        [("eth_rpc", "_/eth_rpc@0.0.0")],
    )
    .await;

    //* When
    // Using default namespace '_' and version '0.0.0'
    let resp = ctx.get_progress("_", "eth_rpc", "0.0.0").await;

    //* Then
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "progress retrieval should succeed for valid dataset"
    );

    let progress: SyncProgressResponse = resp
        .json()
        .await
        .expect("failed to parse progress response JSON");

    println!(
        "Progress Response:\n{}",
        serde_json::to_string_pretty(&progress).expect("serialization failed")
    );

    // Verify response structure
    assert_eq!(progress.dataset_namespace, "_");
    assert_eq!(progress.dataset_name, "eth_rpc");
    assert_eq!(progress.revision, "0.0.0");
    assert!(!progress.manifest_hash.is_empty());

    // Check tables if present
    for (table_name, table) in &progress.tables {
        assert!(!table_name.is_empty());
        // Initial state will have zero files (no job deployed yet)
        assert!(table.files_count >= 0);
    }
}

#[tokio::test]
async fn get_progress_with_non_existent_dataset_returns_not_found() {
    //* Given
    let ctx = TestCtx::setup("get_progress_non_existent", Vec::<&str>::new()).await;

    //* When
    let resp = ctx.get_progress("non_existent", "dataset", "latest").await;

    //* Then
    assert_eq!(
        resp.status(),
        StatusCode::NOT_FOUND,
        "progress retrieval should fail for non-existent dataset"
    );

    let error: ErrorResponse = resp.json().await.expect("failed to parse error response");

    assert_eq!(error.error_code, "DATASET_NOT_FOUND");
}

/// Test that progress shows RUNNING status while a job is actively syncing.
#[tokio::test]
async fn get_progress_shows_running_status() {
    //* Given
    // Use anvil_rpc dataset which connects to the local Anvil instance
    let ctx = TestCtx::setup_with_anvil("get_progress_running_status").await;

    // Mine many blocks so syncing takes time
    ctx.anvil().mine(100).await.expect("failed to mine blocks");

    // Deploy without an end_block so the job keeps running
    ctx.deploy_dataset("_", "anvil_rpc", "0.0.0", None).await;

    // Expected tables from anvil_rpc manifest
    let expected_tables: std::collections::HashSet<&str> =
        ["blocks", "logs", "transactions"].into_iter().collect();

    //* When
    // Poll until we see RUNNING status with some progress
    let start = tokio::time::Instant::now();
    let timeout = tokio::time::Duration::from_secs(30);

    let running_progress: SyncProgressResponse = loop {
        if start.elapsed() > timeout {
            panic!("Timeout waiting for RUNNING status");
        }

        let resp = ctx.get_progress("_", "anvil_rpc", "0.0.0").await;
        if resp.status() == StatusCode::OK {
            let progress: SyncProgressResponse = resp.json().await.expect("failed to parse JSON");

            println!(
                "Progress: {}",
                serde_json::to_string_pretty(&progress).unwrap()
            );

            // Check if any job has failed
            for (table_name, table) in &progress.tables {
                if table.job_status.as_deref() == Some("FAILED") {
                    panic!("Job failed for table '{}': {:?}", table_name, progress);
                }
            }

            // Check if we have a RUNNING job with some data synced
            let has_running_job = progress
                .tables
                .values()
                .any(|t| t.job_status.as_deref() == Some("RUNNING"));

            let has_synced_data = progress
                .tables
                .values()
                .any(|t| t.files_count > 0 && t.current_block.is_some());

            if has_running_job && has_synced_data {
                break progress;
            }
        } else {
            println!("Request failed status: {}", resp.status());
        }

        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    };

    //* Then
    // Verify all expected tables are present
    let actual_tables: std::collections::HashSet<&str> =
        running_progress.tables.keys().map(|s| s.as_str()).collect();

    assert_eq!(
        actual_tables, expected_tables,
        "all expected tables should be present in progress"
    );

    // Verify the job is RUNNING
    for (table_name, table) in &running_progress.tables {
        assert!(
            table.job_id.is_some(),
            "table '{}' should have a job_id",
            table_name
        );

        let status = table
            .job_status
            .as_ref()
            .unwrap_or_else(|| panic!("table '{}' should have a job_status", table_name));

        assert_eq!(
            status, "RUNNING",
            "table '{}' job_status should be RUNNING, got '{}'",
            table_name, status
        );
    }

    // Verify we have some progress
    let blocks_table = running_progress
        .tables
        .get("blocks")
        .expect("blocks table should exist");

    assert!(
        blocks_table.current_block.is_some(),
        "blocks table should have current_block while RUNNING"
    );
    assert!(
        blocks_table.files_count > 0,
        "blocks table should have at least one file while RUNNING"
    );

    println!(
        "Verified RUNNING status with current_block={:?}, files_count={}",
        blocks_table.current_block, blocks_table.files_count
    );
}

/// Test that progress shows COMPLETED status when the end block is reached.
#[tokio::test]
async fn get_progress_shows_completed_status_when_end_block_reached() {
    //* Given
    // Use anvil_rpc dataset which connects to the local Anvil instance
    let ctx = TestCtx::setup_with_anvil("get_progress_completed_status").await;

    // Mine 10 blocks (1 to 10)
    ctx.anvil().mine(10).await.expect("failed to mine blocks");

    // Deploy with end_block=10 so the job completes after syncing
    ctx.deploy_dataset("_", "anvil_rpc", "0.0.0", Some(10))
        .await;

    // Expected tables from anvil_rpc manifest
    let expected_tables: std::collections::HashSet<&str> =
        ["blocks", "logs", "transactions"].into_iter().collect();

    //* When
    // Poll until we see COMPLETED status
    let start = tokio::time::Instant::now();
    let timeout = tokio::time::Duration::from_secs(30);

    let final_progress: SyncProgressResponse = loop {
        if start.elapsed() > timeout {
            panic!("Timeout waiting for COMPLETED status");
        }

        let resp = ctx.get_progress("_", "anvil_rpc", "0.0.0").await;
        if resp.status() == StatusCode::OK {
            let progress: SyncProgressResponse = resp.json().await.expect("failed to parse JSON");

            println!(
                "Progress: {}",
                serde_json::to_string_pretty(&progress).unwrap()
            );

            // Check if any job has failed
            for (table_name, table) in &progress.tables {
                if table.job_status.as_deref() == Some("FAILED") {
                    panic!("Job failed for table '{}': {:?}", table_name, progress);
                }
            }

            // Check if all tables have completed jobs
            let all_completed = progress
                .tables
                .values()
                .all(|t| t.job_status.as_deref() == Some("COMPLETED"));

            if all_completed {
                // Verify blocks table has data
                if progress
                    .tables
                    .get("blocks")
                    .filter(|t| t.files_count > 0 && t.current_block.is_some())
                    .is_some()
                {
                    break progress;
                }
            }
        } else {
            println!("Request failed status: {}", resp.status());
        }

        tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
    };

    //* Then
    // Verify all expected tables are present
    let actual_tables: std::collections::HashSet<&str> =
        final_progress.tables.keys().map(|s| s.as_str()).collect();

    assert_eq!(
        actual_tables, expected_tables,
        "all expected tables should be present in progress"
    );

    // Verify the job is COMPLETED
    for (table_name, table) in &final_progress.tables {
        assert!(
            table.job_id.is_some(),
            "table '{}' should have a job_id",
            table_name
        );

        let status = table
            .job_status
            .as_ref()
            .unwrap_or_else(|| panic!("table '{}' should have a job_status", table_name));

        assert_eq!(
            status, "COMPLETED",
            "table '{}' job_status should be COMPLETED, got '{}'",
            table_name, status
        );
    }

    // Verify the blocks table has complete progress tracking
    let blocks_table = final_progress
        .tables
        .get("blocks")
        .expect("blocks table should exist");

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
    // Since we set end_block to 10, current_block should be 10
    assert_eq!(current_block, 10, "current_block should be 10");

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
        "Verified COMPLETED status: blocks synced from {} to {}, {} files, {} bytes",
        start_block, current_block, blocks_table.files_count, blocks_table.total_size_bytes
    );
}

// --- Table-level progress endpoint tests ---

#[tokio::test]
async fn get_table_progress_with_valid_table_succeeds() {
    //* Given
    let ctx = TestCtx::setup("get_table_progress_valid", [("eth_rpc", "_/eth_rpc@0.0.0")]).await;

    //* When
    // Request progress for the "blocks" table specifically
    let resp = ctx
        .get_table_progress("_", "eth_rpc", "0.0.0", "blocks")
        .await;

    //* Then
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "table progress retrieval should succeed for valid table"
    );

    let progress: TableSyncProgress = resp
        .json()
        .await
        .expect("failed to parse table progress response JSON");

    println!(
        "Table Progress Response:\n{}",
        serde_json::to_string_pretty(&progress).expect("serialization failed")
    );

    // Verify response structure - table_name is no longer in the response
    // (it was the path parameter, so it's implicit)
    // Initial state will have zero files (no job deployed yet)
    assert!(progress.files_count >= 0);
}

#[tokio::test]
async fn get_table_progress_with_non_existent_table_returns_not_found() {
    //* Given
    let ctx = TestCtx::setup(
        "get_table_progress_non_existent_table",
        [("eth_rpc", "_/eth_rpc@0.0.0")],
    )
    .await;

    //* When
    // Request progress for a table that doesn't exist in the dataset
    let resp = ctx
        .get_table_progress("_", "eth_rpc", "0.0.0", "non_existent_table")
        .await;

    //* Then
    assert_eq!(
        resp.status(),
        StatusCode::NOT_FOUND,
        "table progress retrieval should fail for non-existent table"
    );

    let error: ErrorResponse = resp.json().await.expect("failed to parse error response");

    assert_eq!(error.error_code, "TABLE_NOT_FOUND");
}

#[tokio::test]
async fn get_table_progress_with_non_existent_dataset_returns_not_found() {
    //* Given
    let ctx = TestCtx::setup(
        "get_table_progress_non_existent_dataset",
        Vec::<&str>::new(),
    )
    .await;

    //* When
    let resp = ctx
        .get_table_progress("non_existent", "dataset", "latest", "blocks")
        .await;

    //* Then
    assert_eq!(
        resp.status(),
        StatusCode::NOT_FOUND,
        "table progress retrieval should fail for non-existent dataset"
    );

    let error: ErrorResponse = resp.json().await.expect("failed to parse error response");

    assert_eq!(error.error_code, "DATASET_NOT_FOUND");
}

/// Test that table progress shows correct data after deployment
#[tokio::test]
async fn get_table_progress_shows_running_status_for_specific_table() {
    //* Given
    let ctx = TestCtx::setup_with_anvil("get_table_progress_running").await;

    // Mine blocks so syncing takes time
    ctx.anvil().mine(50).await.expect("failed to mine blocks");

    // Deploy without an end_block so the job keeps running
    ctx.deploy_dataset("_", "anvil_rpc", "0.0.0", None).await;

    //* When
    // Poll until we see RUNNING status with some progress for the blocks table
    let start = tokio::time::Instant::now();
    let timeout = tokio::time::Duration::from_secs(30);

    let table_progress: TableSyncProgress = loop {
        if start.elapsed() > timeout {
            panic!("Timeout waiting for RUNNING status on blocks table");
        }

        let resp = ctx
            .get_table_progress("_", "anvil_rpc", "0.0.0", "blocks")
            .await;
        if resp.status() == StatusCode::OK {
            let progress: TableSyncProgress = resp.json().await.expect("failed to parse JSON");

            println!(
                "Table Progress: {}",
                serde_json::to_string_pretty(&progress).unwrap()
            );

            // Check if job has failed
            if progress.job_status.as_deref() == Some("FAILED") {
                panic!("Job failed for blocks table: {:?}", progress);
            }

            // Check if we have a RUNNING job with some data synced
            if progress.job_status.as_deref() == Some("RUNNING")
                && progress.files_count > 0
                && progress.current_block.is_some()
            {
                break progress;
            }
        }

        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    };

    //* Then
    // table_name is no longer in the response (it was the path parameter)
    assert!(
        table_progress.job_id.is_some(),
        "blocks table should have a job_id"
    );
    assert_eq!(
        table_progress.job_status.as_deref(),
        Some("RUNNING"),
        "blocks table should be RUNNING"
    );
    assert!(
        table_progress.current_block.is_some(),
        "blocks table should have current_block"
    );
    assert!(
        table_progress.files_count > 0,
        "blocks table should have at least one file"
    );

    println!(
        "Verified table RUNNING status: current_block={:?}, files_count={}",
        table_progress.current_block, table_progress.files_count
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

    async fn get_progress(&self, namespace: &str, name: &str, revision: &str) -> reqwest::Response {
        let url = format!(
            "{}/datasets/{}/{}/versions/{}/progress",
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

    async fn get_table_progress(
        &self,
        namespace: &str,
        name: &str,
        revision: &str,
        table: &str,
    ) -> reqwest::Response {
        let url = format!(
            "{}/datasets/{}/{}/versions/{}/tables/{}/progress",
            self.daemon_controller().admin_api_url(),
            namespace,
            name,
            revision,
            table
        );

        reqwest::Client::new()
            .get(&url)
            .send()
            .await
            .expect("failed to send request")
    }

    async fn deploy_dataset(
        &self,
        namespace: &str,
        name: &str,
        revision: &str,
        end_block: Option<u64>,
    ) {
        let ampctl = self.new_ampctl();
        let reference = format!("{}/{}@{}", namespace, name, revision);

        ampctl
            .dataset_deploy(&reference, end_block, None, None)
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
    tables: std::collections::HashMap<String, TableSyncProgress>,
}

/// Progress for a single table (used in both dataset-level and table-level responses)
#[derive(Debug, Deserialize, Serialize)]
struct TableSyncProgress {
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
