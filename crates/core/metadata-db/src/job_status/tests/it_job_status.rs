//! Integration tests for the job_status module

use crate::{
    job_status,
    jobs::{self, JobStatus},
    tests::common::{TEST_WORKER_ID, raw_descriptor, register_job, setup_test_db},
    workers::{self, WorkerInfo, WorkerNodeId},
};

#[tokio::test]
async fn register_inserts_status_row() {
    //* Given
    let (_db, conn) = setup_test_db().await;
    let worker_id = WorkerNodeId::from_ref_unchecked(TEST_WORKER_ID);
    let job_desc = raw_descriptor(&serde_json::json!({"test": "register"}));

    //* When
    let job_id = register_job(&conn, &job_desc, &worker_id, None).await;

    //* Then
    let job = jobs::get_by_id(&conn, job_id)
        .await
        .expect("Failed to get job")
        .expect("Job should exist");
    assert_eq!(job.status, JobStatus::Scheduled);
    assert_eq!(job.node_id.as_str(), TEST_WORKER_ID);
}

#[tokio::test]
async fn mark_running_from_scheduled_succeeds() {
    //* Given
    let (_db, conn) = setup_test_db().await;
    let worker_id = WorkerNodeId::from_ref_unchecked(TEST_WORKER_ID);
    let job_desc = raw_descriptor(&serde_json::json!({"test": "mark_running"}));
    let job_id = register_job(&conn, &job_desc, &worker_id, None).await;

    //* When
    job_status::mark_running(&conn, job_id)
        .await
        .expect("Failed to mark job running");

    //* Then
    let job = jobs::get_by_id(&conn, job_id)
        .await
        .expect("Failed to get job")
        .expect("Job should exist");
    assert_eq!(job.status, JobStatus::Running);
}

#[tokio::test]
async fn mark_running_from_running_returns_state_conflict() {
    //* Given
    let (_db, conn) = setup_test_db().await;
    let worker_id = WorkerNodeId::from_ref_unchecked(TEST_WORKER_ID);
    let job_desc = raw_descriptor(&serde_json::json!({"test": "conflict"}));
    let job_id = register_job(&conn, &job_desc, &worker_id, None).await;
    job_status::mark_running(&conn, job_id)
        .await
        .expect("Failed to mark job running");

    //* When
    let result = job_status::mark_running(&conn, job_id).await;

    //* Then
    let err = result.expect_err("Should return error for already running job");
    assert!(
        matches!(
            err,
            crate::error::Error::JobStatusUpdate(
                job_status::JobStatusUpdateError::StateConflict { .. }
            )
        ),
        "Expected StateConflict, got: {err:?}"
    );
}

#[tokio::test]
async fn mark_running_nonexistent_job_returns_not_found() {
    //* Given
    let (_db, conn) = setup_test_db().await;

    //* When
    let result = job_status::mark_running(&conn, jobs::JobId::from_i64_unchecked(999999)).await;

    //* Then
    let err = result.expect_err("Should return error for nonexistent job");
    assert!(
        matches!(
            err,
            crate::error::Error::JobStatusUpdate(job_status::JobStatusUpdateError::NotFound)
        ),
        "Expected NotFound, got: {err:?}"
    );
}

#[tokio::test]
async fn mark_completed_from_running_succeeds() {
    //* Given
    let (_db, conn) = setup_test_db().await;
    let worker_id = WorkerNodeId::from_ref_unchecked(TEST_WORKER_ID);
    let job_desc = raw_descriptor(&serde_json::json!({"test": "completed"}));
    let job_id = register_job(&conn, &job_desc, &worker_id, None).await;
    job_status::mark_running(&conn, job_id)
        .await
        .expect("Failed to mark running");

    //* When
    job_status::mark_completed(&conn, job_id)
        .await
        .expect("Failed to mark completed");

    //* Then
    let job = jobs::get_by_id(&conn, job_id)
        .await
        .expect("Failed to get job")
        .expect("Job should exist");
    assert_eq!(job.status, JobStatus::Completed);
}

#[tokio::test]
async fn mark_completed_from_scheduled_returns_state_conflict() {
    //* Given
    let (_db, conn) = setup_test_db().await;
    let worker_id = WorkerNodeId::from_ref_unchecked(TEST_WORKER_ID);
    let job_desc = raw_descriptor(&serde_json::json!({"test": "completed_conflict"}));
    let job_id = register_job(&conn, &job_desc, &worker_id, None).await;

    //* When
    let result = job_status::mark_completed(&conn, job_id).await;

    //* Then
    let err = result.expect_err("Should return error");
    assert!(
        matches!(
            err,
            crate::error::Error::JobStatusUpdate(
                job_status::JobStatusUpdateError::StateConflict { .. }
            )
        ),
        "Expected StateConflict, got: {err:?}"
    );
}

#[tokio::test]
async fn request_stop_from_scheduled_succeeds() {
    //* Given
    let (_db, conn) = setup_test_db().await;
    let worker_id = WorkerNodeId::from_ref_unchecked(TEST_WORKER_ID);
    let job_desc = raw_descriptor(&serde_json::json!({"test": "stop_scheduled"}));
    let job_id = register_job(&conn, &job_desc, &worker_id, None).await;

    //* When
    let changed = job_status::request_stop(&conn, job_id)
        .await
        .expect("Failed to request stop");

    //* Then
    assert!(changed, "Status should have changed");
    let job = jobs::get_by_id(&conn, job_id)
        .await
        .expect("Failed to get job")
        .expect("Job should exist");
    assert_eq!(job.status, JobStatus::StopRequested);
}

#[tokio::test]
async fn request_stop_from_running_succeeds() {
    //* Given
    let (_db, conn) = setup_test_db().await;
    let worker_id = WorkerNodeId::from_ref_unchecked(TEST_WORKER_ID);
    let job_desc = raw_descriptor(&serde_json::json!({"test": "stop_running"}));
    let job_id = register_job(&conn, &job_desc, &worker_id, None).await;
    job_status::mark_running(&conn, job_id)
        .await
        .expect("Failed to mark running");

    //* When
    let changed = job_status::request_stop(&conn, job_id)
        .await
        .expect("Failed to request stop");

    //* Then
    assert!(changed, "Status should have changed");
    let job = jobs::get_by_id(&conn, job_id)
        .await
        .expect("Failed to get job")
        .expect("Job should exist");
    assert_eq!(job.status, JobStatus::StopRequested);
}

#[tokio::test]
async fn request_stop_is_idempotent_when_already_stop_requested() {
    //* Given
    let (_db, conn) = setup_test_db().await;
    let worker_id = WorkerNodeId::from_ref_unchecked(TEST_WORKER_ID);
    let job_desc = raw_descriptor(&serde_json::json!({"test": "stop_idempotent"}));
    let job_id = register_job(&conn, &job_desc, &worker_id, None).await;
    let first = job_status::request_stop(&conn, job_id)
        .await
        .expect("Failed to request stop");
    assert!(first, "First call should indicate status changed");

    //* When — second stop request should succeed (idempotent)
    let second = job_status::request_stop(&conn, job_id)
        .await
        .expect("Repeated stop request should be idempotent");

    //* Then
    assert!(!second, "Second call should indicate no status change");
}

#[tokio::test]
async fn request_stop_from_stopping_returns_false() {
    //* Given
    let (_db, conn) = setup_test_db().await;
    let worker_id = WorkerNodeId::from_ref_unchecked(TEST_WORKER_ID);
    let job_desc = raw_descriptor(&serde_json::json!({"test": "stop_from_stopping"}));
    let job_id = register_job(&conn, &job_desc, &worker_id, None).await;
    job_status::request_stop(&conn, job_id)
        .await
        .expect("Failed to request stop");
    job_status::mark_stopping(&conn, job_id)
        .await
        .expect("Failed to mark stopping");

    //* When
    let changed = job_status::request_stop(&conn, job_id)
        .await
        .expect("request_stop from Stopping should not error");

    //* Then
    assert!(!changed, "Should indicate no status change from Stopping");
}

#[tokio::test]
async fn request_stop_from_completed_returns_error() {
    //* Given
    let (_db, conn) = setup_test_db().await;
    let worker_id = WorkerNodeId::from_ref_unchecked(TEST_WORKER_ID);
    let job_desc = raw_descriptor(&serde_json::json!({"test": "stop_completed"}));
    let job_id = register_job(&conn, &job_desc, &worker_id, None).await;
    job_status::mark_running(&conn, job_id)
        .await
        .expect("Failed to mark running");
    job_status::mark_completed(&conn, job_id)
        .await
        .expect("Failed to mark completed");

    //* When
    let result = job_status::request_stop(&conn, job_id).await;

    //* Then
    assert!(result.is_err(), "Should not stop a completed job");
}

#[tokio::test]
async fn full_stop_lifecycle() {
    //* Given
    let (_db, conn) = setup_test_db().await;
    let worker_id = WorkerNodeId::from_ref_unchecked(TEST_WORKER_ID);
    let job_desc = raw_descriptor(&serde_json::json!({"test": "lifecycle"}));
    let job_id = register_job(&conn, &job_desc, &worker_id, None).await;

    //* When — walk through the full stop lifecycle
    job_status::mark_running(&conn, job_id)
        .await
        .expect("Scheduled → Running");

    job_status::request_stop(&conn, job_id)
        .await
        .expect("Running → StopRequested");

    job_status::mark_stopping(&conn, job_id)
        .await
        .expect("StopRequested → Stopping");

    job_status::mark_stopped(&conn, job_id)
        .await
        .expect("Stopping → Stopped");

    //* Then
    let job = jobs::get_by_id(&conn, job_id)
        .await
        .expect("Failed to get job")
        .expect("Job should exist");
    assert_eq!(job.status, JobStatus::Stopped);
}

#[tokio::test]
async fn mark_stopping_from_running_returns_state_conflict() {
    //* Given
    let (_db, conn) = setup_test_db().await;
    let worker_id = WorkerNodeId::from_ref_unchecked(TEST_WORKER_ID);
    let job_desc = raw_descriptor(&serde_json::json!({"test": "stopping_conflict"}));
    let job_id = register_job(&conn, &job_desc, &worker_id, None).await;
    job_status::mark_running(&conn, job_id)
        .await
        .expect("Failed to mark running");

    //* When — mark_stopping requires StopRequested, not Running
    let result = job_status::mark_stopping(&conn, job_id).await;

    //* Then
    assert!(result.is_err(), "Should not transition Running → Stopping");
}

#[tokio::test]
async fn mark_stopped_from_scheduled_returns_state_conflict() {
    //* Given
    let (_db, conn) = setup_test_db().await;
    let worker_id = WorkerNodeId::from_ref_unchecked(TEST_WORKER_ID);
    let job_desc = raw_descriptor(&serde_json::json!({"test": "stopped_conflict"}));
    let job_id = register_job(&conn, &job_desc, &worker_id, None).await;

    //* When — mark_stopped requires Stopping, not Scheduled
    let result = job_status::mark_stopped(&conn, job_id).await;

    //* Then
    assert!(result.is_err(), "Should not transition Scheduled → Stopped");
}

#[tokio::test]
async fn mark_failed_recoverable_from_running_succeeds() {
    //* Given
    let (_db, conn) = setup_test_db().await;
    let worker_id = WorkerNodeId::from_ref_unchecked(TEST_WORKER_ID);
    let job_desc = raw_descriptor(&serde_json::json!({"test": "error_running"}));
    let job_id = register_job(&conn, &job_desc, &worker_id, None).await;
    job_status::mark_running(&conn, job_id)
        .await
        .expect("Failed to mark running");

    //* When
    job_status::mark_failed_recoverable(&conn, job_id)
        .await
        .expect("Failed to mark error");

    //* Then
    let job = jobs::get_by_id(&conn, job_id)
        .await
        .expect("Failed to get job")
        .expect("Job should exist");
    assert_eq!(job.status, JobStatus::Error);
}

#[tokio::test]
async fn mark_failed_recoverable_from_scheduled_succeeds() {
    //* Given
    let (_db, conn) = setup_test_db().await;
    let worker_id = WorkerNodeId::from_ref_unchecked(TEST_WORKER_ID);
    let job_desc = raw_descriptor(&serde_json::json!({"test": "error_scheduled"}));
    let job_id = register_job(&conn, &job_desc, &worker_id, None).await;

    //* When
    job_status::mark_failed_recoverable(&conn, job_id)
        .await
        .expect("Failed to mark error");

    //* Then
    let job = jobs::get_by_id(&conn, job_id)
        .await
        .expect("Failed to get job")
        .expect("Job should exist");
    assert_eq!(job.status, JobStatus::Error);
}

#[tokio::test]
async fn mark_failed_fatal_from_running_succeeds() {
    //* Given
    let (_db, conn) = setup_test_db().await;
    let worker_id = WorkerNodeId::from_ref_unchecked(TEST_WORKER_ID);
    let job_desc = raw_descriptor(&serde_json::json!({"test": "fatal_running"}));
    let job_id = register_job(&conn, &job_desc, &worker_id, None).await;
    job_status::mark_running(&conn, job_id)
        .await
        .expect("Failed to mark running");

    //* When
    job_status::mark_failed_fatal(&conn, job_id)
        .await
        .expect("Failed to mark fatal");

    //* Then
    let job = jobs::get_by_id(&conn, job_id)
        .await
        .expect("Failed to get job")
        .expect("Job should exist");
    assert_eq!(job.status, JobStatus::Fatal);
}

#[tokio::test]
async fn mark_failed_fatal_from_completed_returns_state_conflict() {
    //* Given
    let (_db, conn) = setup_test_db().await;
    let worker_id = WorkerNodeId::from_ref_unchecked(TEST_WORKER_ID);
    let job_desc = raw_descriptor(&serde_json::json!({"test": "fatal_completed"}));
    let job_id = register_job(&conn, &job_desc, &worker_id, None).await;
    job_status::mark_running(&conn, job_id)
        .await
        .expect("Failed to mark running");
    job_status::mark_completed(&conn, job_id)
        .await
        .expect("Failed to mark completed");

    //* When
    let result = job_status::mark_failed_fatal(&conn, job_id).await;

    //* Then
    assert!(result.is_err(), "Should not mark completed job as fatal");
}

#[tokio::test]
async fn reschedule_updates_status_and_worker() {
    //* Given
    let (_db, conn) = setup_test_db().await;
    let worker_id = WorkerNodeId::from_ref_unchecked(TEST_WORKER_ID);

    let new_worker_id = WorkerNodeId::from_ref_unchecked("worker-2");
    workers::register(&conn, new_worker_id.clone(), WorkerInfo::default())
        .await
        .expect("Failed to register worker-2");

    let job_desc = raw_descriptor(&serde_json::json!({"test": "reschedule"}));
    let job_id = register_job(&conn, &job_desc, &worker_id, None).await;

    // Move to Error state so reschedule makes sense
    job_status::mark_failed_recoverable(&conn, job_id)
        .await
        .expect("Failed to mark error");

    //* When
    job_status::reschedule(&conn, job_id, &new_worker_id)
        .await
        .expect("Failed to reschedule");

    //* Then
    let job = jobs::get_by_id(&conn, job_id)
        .await
        .expect("Failed to get job")
        .expect("Job should exist");
    assert_eq!(job.status, JobStatus::Scheduled);
    assert_eq!(job.node_id.as_str(), "worker-2");
}
