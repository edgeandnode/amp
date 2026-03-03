//! Integration tests for the job_events module

use pgtemp::PgTempDB;

use crate::{
    config::DEFAULT_POOL_MAX_CONNECTIONS,
    job_events,
    jobs::{self, JobDescriptorRaw, JobStatus},
    workers::{self, WorkerInfo, WorkerNodeId},
};

fn raw_descriptor(value: &serde_json::Value) -> JobDescriptorRaw<'static> {
    let raw = serde_json::value::to_raw_value(value).expect("Failed to serialize to raw value");
    JobDescriptorRaw::from_owned_unchecked(raw)
}

#[tokio::test]
async fn register_inserts_event() {
    //* Given
    let temp_db = PgTempDB::new();
    let conn =
        crate::connect_pool_with_retry(&temp_db.connection_uri(), DEFAULT_POOL_MAX_CONNECTIONS)
            .await
            .expect("Failed to connect to metadata db");

    let worker_id = WorkerNodeId::from_ref_unchecked("test-worker");
    workers::register(&conn, worker_id.clone(), WorkerInfo::default())
        .await
        .expect("Failed to register worker");

    let job_desc = raw_descriptor(&serde_json::json!({"test": "event"}));
    let job_id = jobs::sql::insert_with_default_status(&conn, worker_id.clone(), &job_desc)
        .await
        .expect("Failed to insert job");

    //* When
    job_events::register(&conn, job_id, &worker_id, JobStatus::Scheduled)
        .await
        .expect("Failed to register event");

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
    let temp_db = PgTempDB::new();
    let conn =
        crate::connect_pool_with_retry(&temp_db.connection_uri(), DEFAULT_POOL_MAX_CONNECTIONS)
            .await
            .expect("Failed to connect to metadata db");

    let worker_id = WorkerNodeId::from_ref_unchecked("test-worker");
    workers::register(&conn, worker_id.clone(), WorkerInfo::default())
        .await
        .expect("Failed to register worker");

    let job_desc = raw_descriptor(&serde_json::json!({"test": "filter"}));
    let job_id = jobs::sql::insert_with_default_status(&conn, worker_id.clone(), &job_desc)
        .await
        .expect("Failed to insert job");

    // Insert a mix of event types
    job_events::register(&conn, job_id, &worker_id, JobStatus::Scheduled)
        .await
        .expect("Failed to register SCHEDULED");
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
async fn get_attempts_returns_empty_for_no_events() {
    //* Given
    let temp_db = PgTempDB::new();
    let conn =
        crate::connect_pool_with_retry(&temp_db.connection_uri(), DEFAULT_POOL_MAX_CONNECTIONS)
            .await
            .expect("Failed to connect to metadata db");

    let worker_id = WorkerNodeId::from_ref_unchecked("test-worker");
    workers::register(&conn, worker_id.clone(), WorkerInfo::default())
        .await
        .expect("Failed to register worker");

    let job_desc = raw_descriptor(&serde_json::json!({"test": "empty"}));
    let job_id = jobs::sql::insert_with_default_status(&conn, worker_id.clone(), &job_desc)
        .await
        .expect("Failed to insert job");

    //* When
    let attempts = job_events::get_attempts_for_job(&conn, job_id)
        .await
        .expect("Failed to get attempts");

    //* Then
    assert!(attempts.is_empty());
}

#[tokio::test]
async fn get_attempts_scoped_to_job() {
    //* Given
    let temp_db = PgTempDB::new();
    let conn =
        crate::connect_pool_with_retry(&temp_db.connection_uri(), DEFAULT_POOL_MAX_CONNECTIONS)
            .await
            .expect("Failed to connect to metadata db");

    let worker_id = WorkerNodeId::from_ref_unchecked("test-worker");
    workers::register(&conn, worker_id.clone(), WorkerInfo::default())
        .await
        .expect("Failed to register worker");

    let job_desc = raw_descriptor(&serde_json::json!({"test": "scope"}));

    let job_id_1 = jobs::sql::insert_with_default_status(&conn, worker_id.clone(), &job_desc)
        .await
        .expect("Failed to insert job 1");
    let job_id_2 = jobs::sql::insert_with_default_status(&conn, worker_id.clone(), &job_desc)
        .await
        .expect("Failed to insert job 2");

    // Insert events for both jobs
    job_events::register(&conn, job_id_1, &worker_id, JobStatus::Scheduled)
        .await
        .expect("Failed to register event for job 1");
    job_events::register(&conn, job_id_1, &worker_id, JobStatus::Scheduled)
        .await
        .expect("Failed to register second event for job 1");
    job_events::register(&conn, job_id_2, &worker_id, JobStatus::Scheduled)
        .await
        .expect("Failed to register event for job 2");

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
