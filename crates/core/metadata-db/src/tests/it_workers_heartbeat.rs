//! DB integration tests for the workers activity tracker

use std::time::Duration;

use crate::{
    tests::helpers::{TEST_WORKER_ID, setup_test_db},
    workers::{self, WorkerNodeId},
};

#[tokio::test]
async fn register_worker() {
    //* Given
    let (_db, conn) = setup_test_db().await;
    let worker_id = WorkerNodeId::from_ref_unchecked(TEST_WORKER_ID);

    //* When
    let active_workers = workers::list_active(&conn, Duration::from_secs(5))
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

    let (_db, conn) = setup_test_db().await;
    let worker_id = WorkerNodeId::from_ref_unchecked(TEST_WORKER_ID);

    //* When
    // Sleep for 2 ACTIVE_INTERVAL to ensure the worker is considered inactive
    tokio::time::sleep(2 * ACTIVE_INTERVAL).await;

    let active_workers = workers::list_active(&conn, ACTIVE_INTERVAL)
        .await
        .expect("Failed to get active workers");

    //* Then
    assert!(
        !active_workers.contains(&worker_id),
        "The worker should be inactive"
    );
}
