use pgtemp::PgTempDB;

use crate::{
    config::DEFAULT_POOL_MAX_CONNECTIONS,
    job_events, job_status,
    jobs::{self, JobDescriptorRaw, JobStatus},
    workers::{self, WorkerInfo, WorkerNodeId},
};

/// Default worker ID used by [`setup_test_db`].
pub const TEST_WORKER_ID: &str = "test-worker";

/// Helper to create a [`JobDescriptorRaw`] from a [`serde_json::Value`].
pub fn raw_descriptor(value: &serde_json::Value) -> JobDescriptorRaw<'static> {
    let raw = serde_json::value::to_raw_value(value).expect("Failed to serialize to raw value");
    JobDescriptorRaw::from_owned_unchecked(raw)
}

/// Set up a test database with a single registered worker ([`TEST_WORKER_ID`]).
///
/// Returns the temp DB handle (must be kept alive) and the connection pool.
/// Use [`TEST_WORKER_ID`] to reference the pre-registered worker.
pub async fn setup_test_db() -> (PgTempDB, crate::MetadataDb) {
    let temp_db = PgTempDB::new();
    let conn =
        crate::connect_pool_with_retry(&temp_db.connection_uri(), DEFAULT_POOL_MAX_CONNECTIONS)
            .await
            .expect("Failed to connect to metadata db");

    let worker_id = WorkerNodeId::from_ref_unchecked(TEST_WORKER_ID);
    workers::register(&conn, worker_id, WorkerInfo::default())
        .await
        .expect("Failed to register worker");

    (temp_db, conn)
}

/// Helper to register a job with its event and status in a single transaction.
///
/// Performs the 3-step atomic registration: insert job → register event → register status.
pub async fn register_job(
    conn: &crate::MetadataDb,
    job_desc: &JobDescriptorRaw<'_>,
    worker_id: &WorkerNodeId<'_>,
    status: Option<JobStatus>,
) -> jobs::JobId {
    let status = status.unwrap_or(JobStatus::Scheduled);
    let mut tx = conn.begin_txn().await.expect("Failed to begin transaction");
    let job_id = jobs::register(&mut tx, worker_id, job_desc)
        .await
        .expect("Failed to register job");
    job_events::register(&mut tx, job_id, worker_id, status)
        .await
        .expect("Failed to register job event");
    job_status::register(&mut tx, job_id, worker_id, status)
        .await
        .expect("Failed to register job status");
    tx.commit().await.expect("Failed to commit transaction");
    job_id
}
