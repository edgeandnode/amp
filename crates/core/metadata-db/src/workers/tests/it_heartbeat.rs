//! In-tree DB integration tests for the workers activity tracker

use std::time::Duration;

use pgtemp::PgTempDB;

use crate::{
    WorkerInfo,
    db::Connection,
    workers::{self, NodeId},
};

/// The interval used for testing the active workers list
const TEST_ACTIVE_INTERVAL: Duration = Duration::from_secs(1);

#[tokio::test]
async fn new_worker_is_active_on_registration() {
    //* Given
    let temp_db = PgTempDB::new();
    let mut conn = Connection::connect_with_retry(&temp_db.connection_uri())
        .await
        .expect("Failed to connect to metadata db");
    conn.run_migrations()
        .await
        .expect("Failed to run migrations");

    let worker_id = NodeId::from_ref_unchecked("test-worker-new");
    let worker_info = WorkerInfo::default(); // {}

    //* When
    workers::register(&mut *conn, worker_id.clone(), worker_info)
        .await
        .expect("Failed to register worker");

    //* Then
    let active_workers = workers::list_active(&mut *conn, TEST_ACTIVE_INTERVAL)
        .await
        .expect("Failed to get active workers");
    assert!(
        active_workers.contains(&worker_id),
        "Newly registered worker should be active"
    );
}

#[tokio::test]
async fn reregistration_updates_heartbeat() {
    //* Given
    let temp_db = PgTempDB::new();
    let mut conn = Connection::connect_with_retry(&temp_db.connection_uri())
        .await
        .expect("Failed to connect to metadata db");
    conn.run_migrations()
        .await
        .expect("Failed to run migrations");

    let worker_id = NodeId::from_ref_unchecked("test-worker-reregister");
    let worker_info = WorkerInfo::default(); // {}

    // Initial registration
    workers::register(&mut *conn, worker_id.clone(), worker_info)
        .await
        .expect("Failed to initially register worker");

    // Wait for a period, but less than TEST_ACTIVE_INTERVAL to ensure it would become inactive if not for re-registration
    tokio::time::sleep(TEST_ACTIVE_INTERVAL / 2).await;

    //* When
    // Re-register the worker (this should update the heartbeat)
    let worker_info = WorkerInfo::default(); // {}
    workers::register(&mut *conn, worker_id.clone(), worker_info)
        .await
        .expect("Failed to re-register worker");

    //* Then
    // Wait for a period that would make the initial registration inactive
    tokio::time::sleep(TEST_ACTIVE_INTERVAL / 2 + Duration::from_millis(100)).await;

    let active_workers = workers::list_active(&mut *conn, TEST_ACTIVE_INTERVAL)
        .await
        .expect("Failed to get active workers");
    assert!(
        active_workers.contains(&worker_id),
        "Re-registered worker should remain active"
    );
}

#[tokio::test]
async fn heartbeat_update_maintains_activity() {
    //* Given
    let temp_db = PgTempDB::new();
    let mut conn = Connection::connect_with_retry(&temp_db.connection_uri())
        .await
        .expect("Failed to connect to metadata db");
    conn.run_migrations()
        .await
        .expect("Failed to run migrations");

    let worker_id = NodeId::from_ref_unchecked("test-worker-heartbeat");
    let worker_info = WorkerInfo::default(); // {}

    workers::register(&mut *conn, worker_id.clone(), worker_info)
        .await
        .expect("Failed to register worker");

    // Wait for a period, but less than TEST_ACTIVE_INTERVAL
    tokio::time::sleep(TEST_ACTIVE_INTERVAL / 2).await;

    //* When
    workers::update_heartbeat(&mut *conn, worker_id.clone())
        .await
        .expect("Failed to update heartbeat");

    // Wait for a period that would make the initial registration inactive if heartbeat wasn't updated.
    tokio::time::sleep(TEST_ACTIVE_INTERVAL / 2 + Duration::from_millis(100)).await;

    //* Then
    let active_workers = workers::list_active(&mut *conn, TEST_ACTIVE_INTERVAL)
        .await
        .expect("Failed to get active workers");
    assert!(
        active_workers.contains(&worker_id),
        "Worker should remain active after heartbeat update"
    );
}

#[tokio::test]
async fn worker_is_inactive_after_interval() {
    //* Given
    let temp_db = PgTempDB::new();
    let mut conn = Connection::connect_with_retry(&temp_db.connection_uri())
        .await
        .expect("Failed to connect to metadata db");
    conn.run_migrations()
        .await
        .expect("Failed to run migrations");

    let worker_id = NodeId::from_ref_unchecked("test-worker-inactive");
    let worker_info = WorkerInfo::default(); // {}

    workers::register(&mut *conn, worker_id.clone(), worker_info)
        .await
        .expect("Failed to register worker");

    // Check it's active initially
    let active_workers_initial = workers::list_active(&mut *conn, TEST_ACTIVE_INTERVAL)
        .await
        .expect("Failed to get active workers");
    assert!(
        active_workers_initial.contains(&worker_id),
        "Worker should be active immediately after registration"
    );

    //* When
    // Wait for longer than the active interval
    tokio::time::sleep(2 * TEST_ACTIVE_INTERVAL).await;

    //* Then
    let active_workers_after_wait = workers::list_active(&mut *conn, TEST_ACTIVE_INTERVAL)
        .await
        .expect("Failed to get active workers after wait");
    assert!(
        !active_workers_after_wait.contains(&worker_id),
        "Worker should be inactive after the interval has passed without a heartbeat"
    );
}

/// The current implementation of `update_heartbeat` will not error if the worker doesn't exist,
/// as UPDATE commands without a WHERE match are silent. This is acceptable.
/// This test verifies that it doesn't panic and no worker is magically created/activated.
#[tokio::test]
async fn heartbeat_on_unknown_worker_is_noop() {
    //* Given
    let temp_db = PgTempDB::new();
    let mut conn = Connection::connect_with_retry(&temp_db.connection_uri())
        .await
        .expect("Failed to connect to metadata db");
    conn.run_migrations()
        .await
        .expect("Failed to run migrations");

    let non_existent_worker_id = NodeId::from_ref_unchecked("worker-does-not-exist");

    //* When
    let res = workers::update_heartbeat(&mut *conn, non_existent_worker_id.clone()).await;

    //* Then
    assert!(
        res.is_ok(),
        "Updating heartbeat for a non-existent worker should not return an error"
    );

    let active_workers = workers::list_active(&mut *conn, TEST_ACTIVE_INTERVAL)
        .await
        .expect("Failed to get active workers");
    assert!(
        !active_workers.contains(&non_existent_worker_id),
        "The non-existent worker should not appear in the active workers list"
    );
}

#[tokio::test]
async fn active_workers_empty_when_none_registered() {
    //* Given
    let temp_db = PgTempDB::new();
    let mut conn = Connection::connect_with_retry(&temp_db.connection_uri())
        .await
        .expect("Failed to connect to metadata db");
    conn.run_migrations()
        .await
        .expect("Failed to run migrations");

    //* When
    let active_workers = workers::list_active(&mut *conn, TEST_ACTIVE_INTERVAL)
        .await
        .expect("Failed to get active workers");

    //* Then
    assert!(
        active_workers.is_empty(),
        "Should return an empty list if no workers are registered"
    );
}

/// Test to ensure that the ON CONFLICT clause in `register_worker` correctly updates the timestamp
#[tokio::test]
async fn registration_conflict_updates_timestamp() {
    //* Given
    let temp_db = PgTempDB::new();
    let mut conn = Connection::connect_with_retry(&temp_db.connection_uri())
        .await
        .expect("Failed to connect to metadata db");
    conn.run_migrations()
        .await
        .expect("Failed to run migrations");

    let worker_id = NodeId::from_ref_unchecked("conflict-worker");
    let worker_info = WorkerInfo::default(); // {}

    // First registration
    workers::register(&mut *conn, worker_id.clone(), worker_info)
        .await
        .expect("Failed to register worker initially");

    // Ensure some time passes so that a subsequent registration can demonstrate an update
    // Make the worker "old" enough that if it wasn't updated by the second registration, it would be inactive.
    tokio::time::sleep(2 * TEST_ACTIVE_INTERVAL).await;

    //* When
    // Second registration of the same worker ID (triggers ON CONFLICT DO UPDATE)
    let worker_info = WorkerInfo::default(); // {}
    workers::register(&mut *conn, worker_id.clone(), worker_info)
        .await
        .expect("Failed to register worker on conflict");

    //* Then
    // If the timestamp was updated, the worker should now be active again.
    let active_workers = workers::list_active(&mut *conn, TEST_ACTIVE_INTERVAL)
        .await
        .expect("Failed to get active workers");
    assert!(
        active_workers.contains(&worker_id),
        "Worker should be active after re-registration via ON CONFLICT clause, indicating timestamp update."
    );
    assert_eq!(active_workers.len(), 1, "Only one worker should be active.");
}
