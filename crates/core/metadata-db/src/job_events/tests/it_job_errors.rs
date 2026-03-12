//! Integration tests for the event detail functionality

use crate::{
    job_events::{self, EventDetail},
    job_status,
    jobs::JobStatus,
    tests::helpers::{TEST_WORKER_ID, raw_descriptor, register_job, setup_test_db},
    workers::WorkerNodeId,
};

#[tokio::test]
async fn register_event_with_detail_persists_jsonb_succeeds() {
    //* Given
    let (_db, conn) = setup_test_db().await;
    let worker_id = WorkerNodeId::from_ref_unchecked(TEST_WORKER_ID);
    let job_desc = raw_descriptor(&serde_json::json!({"test": "detail_persist"}));
    let job_id = register_job(&conn, &job_desc, &worker_id, None, None).await;

    let detail_value = serde_json::json!({
        "error_code": "MATERIALIZE_RAW_FAILED",
        "error_message": "Failed to materialize raw dataset",
        "error_context": {
            "error_chain": ["source error 1", "source error 2"]
        }
    });
    let detail = EventDetail::from_value(&detail_value);

    //* When
    job_events::register(&conn, job_id, &worker_id, JobStatus::Error, detail)
        .await
        .expect("Failed to register event with detail");

    //* Then — verify detail was persisted by querying the raw table
    let row: (serde_json::Value,) =
        sqlx::query_as("SELECT detail FROM job_events WHERE job_id = $1 AND event_type = $2")
            .bind(job_id)
            .bind(JobStatus::Error)
            .fetch_one(&conn)
            .await
            .expect("Failed to query event detail");

    assert_eq!(
        row.0["error_code"], "MATERIALIZE_RAW_FAILED",
        "error_code should match"
    );
    assert_eq!(
        row.0["error_message"], "Failed to materialize raw dataset",
        "error_message should match"
    );
    assert_eq!(
        row.0["error_context"]["error_chain"][0], "source error 1",
        "first error chain entry should match"
    );
    assert_eq!(
        row.0["error_context"]["error_chain"][1], "source error 2",
        "second error chain entry should match"
    );
}

#[tokio::test]
async fn register_event_without_detail_stores_null_succeeds() {
    //* Given
    let (_db, conn) = setup_test_db().await;
    let worker_id = WorkerNodeId::from_ref_unchecked(TEST_WORKER_ID);
    let job_desc = raw_descriptor(&serde_json::json!({"test": "detail_null"}));
    let job_id = register_job(&conn, &job_desc, &worker_id, None, None).await;

    //* When
    job_events::register(&conn, job_id, &worker_id, JobStatus::Running, None)
        .await
        .expect("Failed to register event without detail");

    //* Then
    let row: (Option<serde_json::Value>,) =
        sqlx::query_as("SELECT detail FROM job_events WHERE job_id = $1 AND event_type = $2")
            .bind(job_id)
            .bind(JobStatus::Running)
            .fetch_one(&conn)
            .await
            .expect("Failed to query event detail");

    assert!(row.0.is_none(), "detail should be NULL when not provided");
}

#[tokio::test]
async fn mark_error_persists_detail_to_jobs_status_succeeds() {
    //* Given
    let (_db, conn) = setup_test_db().await;
    let worker_id = WorkerNodeId::from_ref_unchecked(TEST_WORKER_ID);
    let job_desc = raw_descriptor(&serde_json::json!({"test": "status_detail"}));
    let job_id = register_job(&conn, &job_desc, &worker_id, None, None).await;

    let detail_value = serde_json::json!({
        "error_code": "MATERIALIZE_DERIVED_FAILED",
        "error_message": "Failed to materialize derived dataset",
    });
    let detail = EventDetail::from_value(&detail_value);

    //* When
    job_status::mark_error(&conn, job_id, &detail)
        .await
        .expect("Failed to mark recoverable with detail");

    //* Then
    let row: (serde_json::Value,) =
        sqlx::query_as("SELECT detail FROM jobs_status WHERE job_id = $1")
            .bind(job_id)
            .fetch_one(&conn)
            .await
            .expect("Failed to query status detail");

    assert_eq!(
        row.0["error_code"], "MATERIALIZE_DERIVED_FAILED",
        "error_code should match"
    );
    assert_eq!(
        row.0["error_message"], "Failed to materialize derived dataset",
        "error_message should match"
    );
}

#[tokio::test]
async fn mark_fatal_persists_detail_to_jobs_status_succeeds() {
    //* Given
    let (_db, conn) = setup_test_db().await;
    let worker_id = WorkerNodeId::from_ref_unchecked(TEST_WORKER_ID);
    let job_desc = raw_descriptor(&serde_json::json!({"test": "fatal_detail"}));
    let job_id = register_job(&conn, &job_desc, &worker_id, None, None).await;
    job_status::mark_running(&conn, job_id)
        .await
        .expect("Failed to mark running");

    let detail_value = serde_json::json!({
        "error_code": "PANIC",
        "error_message": "job panicked",
    });
    let detail = EventDetail::from_value(&detail_value);

    //* When
    job_status::mark_fatal(&conn, job_id, &detail)
        .await
        .expect("Failed to mark fatal with detail");

    //* Then
    let row: (serde_json::Value,) =
        sqlx::query_as("SELECT detail FROM jobs_status WHERE job_id = $1")
            .bind(job_id)
            .fetch_one(&conn)
            .await
            .expect("Failed to query status detail");

    assert_eq!(row.0["error_code"], "PANIC", "error_code should match");
    assert_eq!(
        row.0["error_message"], "job panicked",
        "error_message should match"
    );
}
