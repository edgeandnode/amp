//! Integration tests for worker event streaming.
//!
//! These tests verify that the worker event streaming functionality correctly:
//! - Emits progress events at percentage increments
//! - Emits lifecycle events (started, completed, failed)
//! - Handles continuous ingestion with microbatch events

use std::sync::Arc;

use dump::{ProgressCallback, ProgressUpdate};
use kafka_client::proto;
use monitoring::logging;
use worker::{
    events::{EventEmitter, WorkerProgressCallback},
    job::JobId,
};

/// Test hash constant (64 hex characters).
const TEST_HASH: &str = "0000000000000000000000000000000000000000000000000000000000000001";
const TEST_HASH_2: &str = "0000000000000000000000000000000000000000000000000000000000000002";

/// Helper to create a test DatasetInfo.
fn test_dataset_info(namespace: &str, name: &str, hash: &str) -> proto::DatasetInfo {
    proto::DatasetInfo {
        namespace: namespace.to_string(),
        name: name.to_string(),
        manifest_hash: hash.to_string(),
    }
}

use crate::testlib::fixtures::MockEventEmitter;

/// Test that WorkerProgressCallback correctly forwards progress updates to the event emitter.
#[tokio::test]
async fn test_progress_callback_forwards_to_emitter() {
    logging::init();

    // Given
    let emitter = Arc::new(MockEventEmitter::new());
    let dataset_info = test_dataset_info("test", "dataset", TEST_HASH);

    let job_id = JobId::try_from(1i64).unwrap();
    let callback = WorkerProgressCallback::new(job_id, dataset_info, emitter.clone());

    // When - emit multiple progress updates
    for block in [10u64, 25, 50, 75, 100] {
        callback.on_progress(ProgressUpdate {
            table_name: "blocks".parse().unwrap(),
            start_block: 0,
            current_block: block,
            end_block: Some(100),
            files_count: 1,
            total_size_bytes: 1000,
        });
    }

    // Give async tasks time to complete
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // Then
    let progress_events = emitter.progress_events();
    assert_eq!(progress_events.len(), 5, "Should have 5 progress events");

    // Verify event contents
    assert_eq!(
        progress_events[0].progress.as_ref().unwrap().current_block,
        10
    );
    assert_eq!(
        progress_events[1].progress.as_ref().unwrap().current_block,
        25
    );
    assert_eq!(
        progress_events[2].progress.as_ref().unwrap().current_block,
        50
    );
    assert_eq!(
        progress_events[3].progress.as_ref().unwrap().current_block,
        75
    );
    assert_eq!(
        progress_events[4].progress.as_ref().unwrap().current_block,
        100
    );

    // Verify all events have correct metadata
    for event in &progress_events {
        assert_eq!(event.job_id, 1);
        let dataset = event.dataset.as_ref().unwrap();
        assert_eq!(dataset.namespace, "test");
        assert_eq!(dataset.name, "dataset");
        assert_eq!(event.table_name, "blocks");
    }
}

/// Test that percentage calculation from progress events is accurate.
#[tokio::test]
async fn test_progress_percentages_calculation() {
    logging::init();

    // Given
    let emitter = Arc::new(MockEventEmitter::new());
    let total_blocks = 100u64;

    // Emit progress at 0%, 25%, 50%, 75%, 100%
    for pct in [0u64, 25, 50, 75, 100] {
        emitter
            .emit_sync_progress(proto::SyncProgress {
                job_id: 1,
                dataset: Some(test_dataset_info("test", "dataset", TEST_HASH)),
                table_name: "blocks".to_string(),
                progress: Some(proto::ProgressInfo {
                    start_block: 0,
                    current_block: pct,
                    end_block: Some(100),
                    percentage: Some(pct as u32),
                    files_count: 0,
                    total_size_bytes: 0,
                }),
            })
            .await;
    }

    // When
    let percentages = emitter.progress_percentages(total_blocks);

    // Then
    assert_eq!(percentages, vec![0, 25, 50, 75, 100]);
}

/// Test that started events are recorded correctly.
#[tokio::test]
async fn test_started_events_recorded() {
    // Given
    let emitter = Arc::new(MockEventEmitter::new());

    // When
    emitter
        .emit_sync_started(proto::SyncStarted {
            job_id: 42,
            dataset: Some(test_dataset_info("ethereum", "mainnet", TEST_HASH)),
            table_name: "blocks".to_string(),
            start_block: Some(1000),
            end_block: Some(2000),
        })
        .await;

    // Then
    let started_events = emitter.started_events();
    assert_eq!(started_events.len(), 1);

    let event = &started_events[0];
    assert_eq!(event.job_id, 42);
    let dataset = event.dataset.as_ref().unwrap();
    assert_eq!(dataset.namespace, "ethereum");
    assert_eq!(dataset.name, "mainnet");
    assert_eq!(event.table_name, "blocks");
    assert_eq!(event.start_block, Some(1000));
    assert_eq!(event.end_block, Some(2000));
}

/// Test that completed events are recorded correctly.
#[tokio::test]
async fn test_completed_events_recorded() {
    // Given
    let emitter = Arc::new(MockEventEmitter::new());

    // When
    emitter
        .emit_sync_completed(proto::SyncCompleted {
            job_id: 42,
            dataset: Some(test_dataset_info("ethereum", "mainnet", TEST_HASH)),
            table_name: "blocks".to_string(),
            final_block: 2000,
            duration_millis: 5000,
        })
        .await;

    // Then
    let completed_events = emitter.completed_events();
    assert_eq!(completed_events.len(), 1);

    let event = &completed_events[0];
    assert_eq!(event.job_id, 42);
    assert_eq!(event.final_block, 2000);
    assert_eq!(event.duration_millis, 5000);
}

/// Test that failed events are recorded correctly.
#[tokio::test]
async fn test_failed_events_recorded() {
    // Given
    let emitter = Arc::new(MockEventEmitter::new());

    // When
    emitter
        .emit_sync_failed(proto::SyncFailed {
            job_id: 42,
            dataset: Some(test_dataset_info("ethereum", "mainnet", TEST_HASH)),
            table_name: "blocks".to_string(),
            error_message: "Connection timeout".to_string(),
            error_type: Some("NetworkError".to_string()),
        })
        .await;

    // Then
    let failed_events = emitter.failed_events();
    assert_eq!(failed_events.len(), 1);

    let event = &failed_events[0];
    assert_eq!(event.job_id, 42);
    assert_eq!(event.error_message, "Connection timeout");
    assert_eq!(event.error_type, Some("NetworkError".to_string()));
}

/// Test that the full job lifecycle emits events in the correct order.
#[tokio::test]
async fn test_full_job_lifecycle_events() {
    logging::init();

    // Given
    let emitter = Arc::new(MockEventEmitter::new());
    let dataset = test_dataset_info("test", "dataset", TEST_HASH);

    // When - simulate a complete job lifecycle
    emitter
        .emit_sync_started(proto::SyncStarted {
            job_id: 1,
            dataset: Some(dataset.clone()),
            table_name: "blocks".to_string(),
            start_block: Some(0),
            end_block: Some(100),
        })
        .await;

    // Progress at EVERY 1% from 1 to 99 (0% is start, 100% is completed)
    for pct in 1u64..100 {
        emitter
            .emit_sync_progress(proto::SyncProgress {
                job_id: 1,
                dataset: Some(dataset.clone()),
                table_name: "blocks".to_string(),
                progress: Some(proto::ProgressInfo {
                    start_block: 0,
                    current_block: pct,
                    end_block: Some(100),
                    percentage: Some(pct as u32),
                    files_count: 1,
                    total_size_bytes: pct * 100,
                }),
            })
            .await;
    }

    emitter
        .emit_sync_completed(proto::SyncCompleted {
            job_id: 1,
            dataset: Some(dataset.clone()),
            table_name: "blocks".to_string(),
            final_block: 100,
            duration_millis: 10000,
        })
        .await;

    // Then
    assert_eq!(emitter.started_events().len(), 1);
    assert_eq!(emitter.progress_events().len(), 99); // 1% to 99%
    assert_eq!(emitter.completed_events().len(), 1);
    assert_eq!(emitter.failed_events().len(), 0);
    assert_eq!(emitter.event_count(), 101); // 1 started + 99 progress + 1 completed
}

/// Test that events can be cleared from the mock emitter.
#[tokio::test]
async fn test_mock_emitter_clear() {
    // Given
    let emitter = Arc::new(MockEventEmitter::new());
    emitter
        .emit_sync_started(proto::SyncStarted {
            job_id: 1,
            dataset: Some(test_dataset_info("test", "dataset", TEST_HASH)),
            table_name: "blocks".to_string(),
            start_block: Some(0),
            end_block: Some(100),
        })
        .await;
    assert_eq!(emitter.event_count(), 1);

    // When
    emitter.clear();

    // Then
    assert_eq!(emitter.event_count(), 0);
}

/// Test continuous ingestion scenario (no end block).
///
/// In continuous ingestion mode, events should be emitted on each microbatch
/// completion rather than at percentage intervals.
#[tokio::test]
async fn test_continuous_ingestion_events() {
    logging::init();

    // Given
    let emitter = Arc::new(MockEventEmitter::new());
    let dataset = test_dataset_info("test", "stream", TEST_HASH_2);

    // When - simulate continuous ingestion (no end_block)
    emitter
        .emit_sync_started(proto::SyncStarted {
            job_id: 1,
            dataset: Some(dataset.clone()),
            table_name: "events".to_string(),
            start_block: Some(1000),
            end_block: None, // No end block = continuous mode
        })
        .await;

    // Emit progress for each microbatch (simulating continuous ingestion)
    // In continuous mode, end_block and percentage are None (no fixed target)
    for batch_end in [1010u64, 1020, 1030, 1040, 1050] {
        emitter
            .emit_sync_progress(proto::SyncProgress {
                job_id: 1,
                dataset: Some(dataset.clone()),
                table_name: "events".to_string(),
                progress: Some(proto::ProgressInfo {
                    start_block: 1000,
                    current_block: batch_end,
                    end_block: None, // No fixed end in continuous mode
                    percentage: None,
                    files_count: 1,
                    total_size_bytes: 500,
                }),
            })
            .await;
    }

    // Then - all progress events should be recorded (no throttling in continuous mode)
    let progress_events = emitter.progress_events();
    assert_eq!(progress_events.len(), 5);

    // Verify blocks are sequential microbatches
    let blocks: Vec<_> = progress_events
        .iter()
        .filter_map(|p| p.progress.as_ref().map(|prog| prog.current_block))
        .collect();
    assert_eq!(blocks, vec![1010, 1020, 1030, 1040, 1050]);

    // Started event has no end_block
    let started = &emitter.started_events()[0];
    assert_eq!(started.end_block, None);
}

/// Test that multiple tables emit separate events.
#[tokio::test]
async fn test_multiple_tables_emit_separate_events() {
    logging::init();

    // Given
    let emitter = Arc::new(MockEventEmitter::new());
    let job_id = JobId::try_from(1i64).unwrap();
    let callback = WorkerProgressCallback::new(
        job_id,
        test_dataset_info("test", "dataset", TEST_HASH),
        emitter.clone(),
    );

    // When - emit progress for different tables
    callback.on_progress(ProgressUpdate {
        table_name: "blocks".parse().unwrap(),
        start_block: 0,
        current_block: 50,
        end_block: Some(100),
        files_count: 1,
        total_size_bytes: 1000,
    });
    callback.on_progress(ProgressUpdate {
        table_name: "transactions".parse().unwrap(),
        start_block: 0,
        current_block: 50,
        end_block: Some(100),
        files_count: 2,
        total_size_bytes: 2000,
    });
    callback.on_progress(ProgressUpdate {
        table_name: "logs".parse().unwrap(),
        start_block: 0,
        current_block: 50,
        end_block: Some(100),
        files_count: 3,
        total_size_bytes: 3000,
    });

    // Give async tasks time to complete
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // Then
    let progress_events = emitter.progress_events();
    assert_eq!(progress_events.len(), 3);

    let table_names: Vec<_> = progress_events
        .iter()
        .map(|p| p.table_name.as_str())
        .collect();
    assert!(table_names.contains(&"blocks"));
    assert!(table_names.contains(&"transactions"));
    assert!(table_names.contains(&"logs"));
}

// NOTE: Unit tests for retry logic are in the kafka_client crate:
// - crates/clients/kafka/src/client.rs (FixedDelayBackoff tests)
// - crates/clients/kafka/src/error.rs (is_retryable tests)

// ============================================================================
// End-to-End Integration Tests with Anvil
// ============================================================================
//
// These tests use a real Anvil node and deploy actual datasets to verify
// that the event streaming works correctly in a real environment.

use crate::testlib::ctx::TestCtxBuilder;

/// End-to-end test that deploys a dataset to Anvil and verifies events are captured.
///
/// This test:
/// 1. Starts Anvil with mined blocks
/// 2. Injects a MockEventEmitter into the worker
/// 3. Deploys the anvil_rpc dataset
/// 4. Waits for the sync to complete
/// 5. Verifies that started, progress, and completed events were captured
#[tokio::test]
async fn test_e2e_anvil_sync_emits_lifecycle_events() {
    logging::init();

    // Given: Create a MockEventEmitter to capture events
    let mock_emitter = Arc::new(MockEventEmitter::new());

    // Build test context with Anvil and inject the mock emitter
    let ctx = TestCtxBuilder::new("e2e_anvil_sync_events")
        .with_anvil_http()
        .with_dataset_manifest("anvil_rpc")
        .with_event_emitter(mock_emitter.clone())
        .build()
        .await
        .expect("failed to build test context");

    // Mine some blocks so we have data to sync
    ctx.anvil().mine(2000).await.expect("failed to mine blocks");

    // When: Deploy the dataset with an end block so the job completes
    let ampctl = ctx.new_ampctl();
    let job_id = ampctl
        .dataset_deploy("_/anvil_rpc@0.0.0", Some(2000), None, None)
        .await
        .expect("failed to deploy dataset");

    tracing::info!(%job_id, "Dataset deployed, waiting for sync to complete");

    // Poll until job completes or fails (timeout after 120 seconds for larger block counts)
    let start = tokio::time::Instant::now();
    let timeout = tokio::time::Duration::from_secs(120);

    loop {
        if start.elapsed() > timeout {
            panic!(
                "Timeout waiting for job to complete. Events captured: started={}, progress={}, completed={}, failed={}",
                mock_emitter.started_events().len(),
                mock_emitter.progress_events().len(),
                mock_emitter.completed_events().len(),
                mock_emitter.failed_events().len()
            );
        }

        // Check if we have a completed or failed event
        let completed = mock_emitter.completed_events();
        let failed = mock_emitter.failed_events();

        if !completed.is_empty() {
            tracing::info!("Job completed successfully");
            break;
        }

        if !failed.is_empty() {
            panic!("Job failed with error: {}", failed[0].error_message);
        }

        tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
    }

    // Then: Verify events were captured
    let started_events = mock_emitter.started_events();
    let progress_events = mock_emitter.progress_events();
    let completed_events = mock_emitter.completed_events();

    // anvil_rpc has 3 tables (blocks, transactions, logs), so we expect 3 events of each type
    // Events are emitted per-table to ensure partition affinity for consumers
    let expected_tables = 3;

    // Should have one started event per table
    assert_eq!(
        started_events.len(),
        expected_tables,
        "Should have {} started events (one per table), got {}",
        expected_tables,
        started_events.len()
    );

    // Should have one completed event per table
    assert_eq!(
        completed_events.len(),
        expected_tables,
        "Should have {} completed events (one per table), got {}",
        expected_tables,
        completed_events.len()
    );

    // Should have some progress events (the exact number depends on block count and tables)
    // With 2000 blocks and 1% throttling, we expect ~100 events per table = ~300 total
    tracing::info!(
        "Captured {} progress events from real sync",
        progress_events.len()
    );

    // Verify all tables are represented in started events
    let started_tables: std::collections::HashSet<_> = started_events
        .iter()
        .map(|e| e.table_name.as_str())
        .collect();
    assert!(
        started_tables.contains("blocks"),
        "Missing started event for blocks table"
    );
    assert!(
        started_tables.contains("transactions"),
        "Missing started event for transactions table"
    );
    assert!(
        started_tables.contains("logs"),
        "Missing started event for logs table"
    );

    // Verify started events have correct metadata
    for started in &started_events {
        assert_eq!(started.job_id, *job_id);
        let dataset = started.dataset.as_ref().unwrap();
        assert_eq!(dataset.namespace, "_");
        assert_eq!(dataset.name, "anvil_rpc");
        assert_eq!(started.end_block, Some(2000));
    }

    // Verify completed events have correct metadata
    for completed in &completed_events {
        assert_eq!(completed.job_id, *job_id);
        assert!(completed.duration_millis > 0, "Duration should be positive");
    }

    // Log summary
    tracing::info!(
        "E2E test passed: started={}, progress={}, completed={}",
        started_events.len(),
        progress_events.len(),
        completed_events.len()
    );
}
