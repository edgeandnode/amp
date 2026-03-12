//! Integration tests for idempotency key behavior in job registration

use crate::{
    job_events, job_status,
    jobs::{self, IdempotencyKey, JobStatus},
    tests::helpers::{TEST_WORKER_ID, raw_descriptor, register_job, setup_test_db},
    workers::WorkerNodeId,
};

#[tokio::test]
async fn register_with_same_key_returns_same_job_id() {
    //* Given
    let (_db, conn) = setup_test_db().await;
    let worker_id = WorkerNodeId::from_ref_unchecked(TEST_WORKER_ID);
    let job_desc = raw_descriptor(&serde_json::json!({ "dataset": "test" }));
    let key = IdempotencyKey::from_ref_unchecked("dedup-key-1");
    let job_id_1 = register_job(&conn, &job_desc, &worker_id, Some(key.clone()), None).await;

    //* When
    let job_id_2 = register_job(&conn, &job_desc, &worker_id, Some(key), None).await;

    //* Then
    assert_eq!(job_id_1, job_id_2, "duplicate key must return same job ID");
}

#[tokio::test]
async fn different_keys_produce_different_jobs() {
    //* Given
    let (_db, conn) = setup_test_db().await;
    let worker_id = WorkerNodeId::from_ref_unchecked(TEST_WORKER_ID);
    let job_id_a = jobs::register(
        &conn,
        IdempotencyKey::from_ref_unchecked("key-a"),
        &worker_id,
    )
    .await
    .expect("register key-a failed");

    //* When
    let job_id_b = jobs::register(
        &conn,
        IdempotencyKey::from_ref_unchecked("key-b"),
        &worker_id,
    )
    .await
    .expect("register key-b failed");

    //* Then
    assert_ne!(
        job_id_a, job_id_b,
        "different keys must produce different job IDs"
    );
}

#[tokio::test]
async fn get_by_idempotency_key_returns_job() {
    //* Given
    let (_db, conn) = setup_test_db().await;
    let worker_id = WorkerNodeId::from_ref_unchecked(TEST_WORKER_ID);
    let job_desc = raw_descriptor(&serde_json::json!({ "dataset": "lookup" }));
    let key = IdempotencyKey::from_ref_unchecked("lookup-key");

    let mut tx = conn.begin_txn().await.expect("begin txn failed");
    let job_id = jobs::register(&mut tx, key.clone(), &worker_id)
        .await
        .expect("register failed");
    job_events::register(
        &mut tx,
        job_id,
        &worker_id,
        JobStatus::Scheduled,
        Some(job_desc.clone().into()),
    )
    .await
    .expect("register event failed");
    job_status::register(
        &mut tx,
        job_id,
        &worker_id,
        JobStatus::Scheduled,
        Some(job_desc.into()),
    )
    .await
    .expect("register status failed");
    tx.commit().await.expect("commit failed");

    //* When
    let result = jobs::get_by_idempotency_key(&conn, key)
        .await
        .expect("get_by_idempotency_key failed");

    //* Then
    let job = result.expect("job should exist");
    assert_eq!(job.id, job_id);
    assert_eq!(job.status, JobStatus::Scheduled);
}

#[tokio::test]
async fn get_by_idempotency_key_returns_none_for_unknown_key() {
    //* Given
    let (_db, conn) = setup_test_db().await;

    //* When
    let result =
        jobs::get_by_idempotency_key(&conn, IdempotencyKey::from_ref_unchecked("nonexistent-key"))
            .await
            .expect("get_by_idempotency_key failed");

    //* Then
    assert!(result.is_none(), "unknown key must return None");
}
