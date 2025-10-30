//! DB integration tests for the workers activity tracker

use std::time::Duration;

use pgtemp::PgTempDB;

use crate::{MetadataDb, WorkerInfo, WorkerNodeId, workers};

#[tokio::test]
async fn register_worker() {
    //* Given
    let temp_db = PgTempDB::new();

    let metadata_db =
        MetadataDb::connect_with_retry(&temp_db.connection_uri(), MetadataDb::default_pool_size())
            .await
            .expect("Failed to connect to metadata db");

    let worker_id = WorkerNodeId::from_ref_unchecked("test-worker-id");
    let worker_info = WorkerInfo::default(); // {}

    //* When
    workers::register(&metadata_db, &worker_id, worker_info)
        .await
        .expect("Failed to register the worker");

    let active_workers = workers::list_active(&metadata_db, Duration::from_secs(5))
        .await
        .expect("Failed to get active workers");

    //* Then
    assert!(
        active_workers.contains(&worker_id),
        "Worker not found in active workers"
    );
}

#[tokio::test]
async fn detect_inactive_worker() {
    //* Given
    const ACTIVE_INTERVAL: Duration = Duration::from_secs(1);

    let temp_db = PgTempDB::new();

    let metadata_db =
        MetadataDb::connect(&temp_db.connection_uri(), MetadataDb::default_pool_size())
            .await
            .expect("Failed to connect to metadata db");

    // Pre-register a worker
    let worker_id = WorkerNodeId::from_ref_unchecked("test-worker-id");
    let worker_info = WorkerInfo::default(); // {}
    workers::register(&metadata_db, &worker_id, worker_info)
        .await
        .expect("Failed to pre-register the worker");

    //* When
    // Sleep for 2 ACTIVE_INTERVAL to ensure the worker is considered inactive
    tokio::time::sleep(2 * ACTIVE_INTERVAL).await;

    let active_workers = workers::list_active(&metadata_db, ACTIVE_INTERVAL)
        .await
        .expect("Failed to get active workers");

    //* Then
    assert!(
        !active_workers.contains(&worker_id),
        "The worker should be inactive"
    );
}
