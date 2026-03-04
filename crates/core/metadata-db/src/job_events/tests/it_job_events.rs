//! Integration tests for the job_events module

use crate::{
    job_events,
    jobs::JobStatus,
    tests::helpers::{TEST_WORKER_ID, raw_descriptor, register_job, setup_test_db},
    workers::WorkerNodeId,
};

#[tokio::test]
async fn register_inserts_event() {
    //* Given
    let (_db, conn) = setup_test_db().await;
    let worker_id = WorkerNodeId::from_ref_unchecked(TEST_WORKER_ID);
    let job_desc = raw_descriptor(&serde_json::json!({"test": "event"}));

    //* When
    let job_id = register_job(&conn, &job_desc, &worker_id, None).await;

    //* Then
    let attempts = job_events::get_attempts_for_job(&conn, job_id)
        .await
        .expect("Failed to get attempts");
    assert_eq!(attempts.len(), 1);
    assert_eq!(attempts[0].job_id, job_id);
    assert_eq!(attempts[0].retry_index, 0);
}

#[tokio::test]
async fn get_attempts_returns_only_scheduled_events() {
    //* Given
    let (_db, conn) = setup_test_db().await;
    let worker_id = WorkerNodeId::from_ref_unchecked(TEST_WORKER_ID);
    let job_desc = raw_descriptor(&serde_json::json!({"test": "filter"}));

    // Insert a mix of event types
    let job_id = register_job(&conn, &job_desc, &worker_id, None).await;
    job_events::register(&conn, job_id, &worker_id, JobStatus::Running)
        .await
        .expect("Failed to register RUNNING");
    job_events::register(&conn, job_id, &worker_id, JobStatus::Error)
        .await
        .expect("Failed to register ERROR");
    job_events::register(&conn, job_id, &worker_id, JobStatus::Scheduled)
        .await
        .expect("Failed to register second SCHEDULED");

    //* When
    let attempts = job_events::get_attempts_for_job(&conn, job_id)
        .await
        .expect("Failed to get attempts");

    //* Then — only SCHEDULED events are returned
    assert_eq!(attempts.len(), 2);
    assert_eq!(attempts[0].retry_index, 0);
    assert_eq!(attempts[1].retry_index, 1);
}

#[tokio::test]
async fn get_attempts_scoped_to_job() {
    //* Given
    let (_db, conn) = setup_test_db().await;
    let worker_id = WorkerNodeId::from_ref_unchecked(TEST_WORKER_ID);
    let job_desc = raw_descriptor(&serde_json::json!({"test": "scope"}));

    // Insert events for both jobs
    let job_id_1 = register_job(&conn, &job_desc, &worker_id, None).await;
    job_events::register(&conn, job_id_1, &worker_id, JobStatus::Scheduled)
        .await
        .expect("Failed to register second event for job 1");

    let job_id_2 = register_job(&conn, &job_desc, &worker_id, None).await;

    //* When
    let attempts_1 = job_events::get_attempts_for_job(&conn, job_id_1)
        .await
        .expect("Failed to get attempts for job 1");
    let attempts_2 = job_events::get_attempts_for_job(&conn, job_id_2)
        .await
        .expect("Failed to get attempts for job 2");

    //* Then — events are scoped per job
    assert_eq!(attempts_1.len(), 2);
    assert_eq!(attempts_2.len(), 1);
    // retry_index is per-job, starting from 0
    assert_eq!(attempts_1[0].retry_index, 0);
    assert_eq!(attempts_1[1].retry_index, 1);
    assert_eq!(attempts_2[0].retry_index, 0);
}
