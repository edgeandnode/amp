//! In-tree DB integration tests for the workers queue

use pgtemp::PgTempDB;

use crate::{
    conn::DbConn,
    workers::{
        heartbeat,
        jobs::{self, JobStatus},
    },
};

#[tokio::test]
async fn schedule_and_retrieve_job() {
    //* Given
    let temp_db = PgTempDB::new();
    let mut conn = DbConn::connect(&temp_db.connection_uri())
        .await
        .expect("Failed to connect to metadata db");
    conn.run_migrations()
        .await
        .expect("Failed to run migrations");

    // Pre-register the worker
    let worker_id = "test-worker-id".parse().expect("Invalid worker ID");
    heartbeat::register_worker(&mut *conn, &worker_id)
        .await
        .expect("Failed to pre-register the worker");

    // Specify the job descriptor
    let job_desc = serde_json::json!({
        "dataset": "test-dataset",
        "dataset_version": "test-dataset-version",
        "table": "test-table",
        "locations": ["test-location"],
    });
    let job_desc_str = serde_json::to_string(&job_desc).expect("Failed to serialize job desc");

    //* When
    // Register the job
    let job_id = jobs::register_job(&mut *conn, &worker_id, &job_desc_str)
        .await
        .expect("Failed to schedule job");

    // Get the job
    let job = jobs::get_job(&mut *conn, &job_id)
        .await
        .expect("Failed to get job")
        .expect("Job not found");

    //* Then
    assert_eq!(job.id, job_id);
    assert_eq!(job.status, JobStatus::Scheduled);
    assert_eq!(job.node_id, worker_id);
    assert_eq!(job.desc, job_desc);
}

#[tokio::test]
async fn update_job_status_modifies_status() {
    //* Given
    let temp_db = PgTempDB::new();
    let mut conn = DbConn::connect(&temp_db.connection_uri())
        .await
        .expect("Failed to connect to metadata db");
    conn.run_migrations()
        .await
        .expect("Failed to run migrations");

    let worker_id = "test-worker-update".parse().expect("Invalid worker ID");
    heartbeat::register_worker(&mut *conn, &worker_id)
        .await
        .expect("Failed to pre-register the worker");

    let job_desc = serde_json::json!({ "key": "value" });
    let job_desc_str = serde_json::to_string(&job_desc).expect("Failed to serialize");

    let job_id = jobs::register_job(&mut *conn, &worker_id, &job_desc_str)
        .await
        .expect("Failed to schedule job");

    //* When
    jobs::update_job_status(&mut *conn, &job_id, JobStatus::Running)
        .await
        .expect("Failed to update job status");

    //* Then
    let job = jobs::get_job(&mut *conn, &job_id)
        .await
        .expect("Failed to get job")
        .expect("Job not found");

    assert_eq!(job.id, job_id);
    assert_eq!(job.status, JobStatus::Running);
    assert_eq!(job.node_id, worker_id);
}

#[tokio::test]
async fn get_job_descriptor_retrieves_correct_descriptor() {
    //* Given
    let temp_db = PgTempDB::new();
    let mut conn = DbConn::connect(&temp_db.connection_uri())
        .await
        .expect("Failed to connect to metadata db");
    conn.run_migrations()
        .await
        .expect("Failed to run migrations");

    let worker_id = "test-worker-descriptor".parse().expect("Invalid worker ID");
    heartbeat::register_worker(&mut *conn, &worker_id)
        .await
        .expect("Failed to pre-register the worker");

    let job_desc_json = serde_json::json!({
        "type": "descriptor-test",
        "payload": "important-data"
    });
    let job_desc_str = serde_json::to_string(&job_desc_json).expect("Failed to serialize");

    let job_id = jobs::register_job(&mut *conn, &worker_id, &job_desc_str)
        .await
        .expect("Failed to schedule job");

    //* When
    let retrieved_desc_str = jobs::get_job_descriptor(&mut *conn, &job_id)
        .await
        .expect("Failed to get job descriptor")
        .expect("Job descriptor not found");

    //* Then
    let retrieved_desc_json: serde_json::Value =
        serde_json::from_str(&retrieved_desc_str).expect("Failed to deserialize descriptor");
    assert_eq!(retrieved_desc_json, job_desc_json);
}

#[tokio::test]
async fn get_job_ids_for_node_retrieves_all_jobs() {
    //* Given
    let temp_db = PgTempDB::new();
    let mut conn = DbConn::connect(&temp_db.connection_uri())
        .await
        .expect("Failed to connect to metadata db");
    conn.run_migrations()
        .await
        .expect("Failed to run migrations");

    let worker_id_main = "test-worker-main".parse().expect("Invalid worker ID");
    heartbeat::register_worker(&mut *conn, &worker_id_main)
        .await
        .expect("Failed to pre-register the worker 1");
    let worker_id_other = "test-worker-other".parse().expect("Invalid worker ID");
    heartbeat::register_worker(&mut *conn, &worker_id_other)
        .await
        .expect("Failed to pre-register the worker 2");

    //
    let job_desc1 = serde_json::json!({ "job": 1 });
    let job_desc_str1 = serde_json::to_string(&job_desc1).expect("Failed to serialize");
    let job_id1 = jobs::register_job(&mut *conn, &worker_id_main, &job_desc_str1)
        .await
        .expect("Failed to register job 1");

    let job_desc2 = serde_json::json!({ "job": 2 });
    let job_desc_str2 = serde_json::to_string(&job_desc2).expect("Failed to serialize");
    let job_id2 = jobs::register_job(&mut *conn, &worker_id_main, &job_desc_str2)
        .await
        .expect("Failed to register job 2");

    // Register a job for a different worker to ensure it's not retrieved
    let job_desc_other = serde_json::json!({ "job": "other" });
    let job_desc_str_other = serde_json::to_string(&job_desc_other).expect("Failed to serialize");
    jobs::register_job(&mut *conn, &worker_id_other, &job_desc_str_other)
        .await
        .expect("Failed to register job for other worker");

    //* When
    let job_ids = jobs::get_job_ids_for_node(&mut *conn, &worker_id_main)
        .await
        .expect("Failed to get job IDs for node");

    //* Then
    assert_eq!(job_ids, [job_id1, job_id2]);
}

#[tokio::test]
async fn get_active_job_ids_for_node_retrieves_only_active_jobs() {
    //* Given
    let temp_db = PgTempDB::new();

    // Connect to the DB
    let mut db = DbConn::connect(&temp_db.connection_uri())
        .await
        .expect("Failed to connect to metadata db");
    db.run_migrations().await.expect("Failed to run migrations");

    let worker_id = "test-worker-active-ids".parse().expect("Invalid worker ID");
    heartbeat::register_worker(&mut *db, &worker_id)
        .await
        .expect("Failed to pre-register the worker");

    let job_desc = serde_json::json!({ "key": "value" });
    let job_desc_str = serde_json::to_string(&job_desc).expect("Failed to serialize");

    // Active jobs
    let job_id_scheduled = jobs::register_job(&mut *db, &worker_id, &job_desc_str)
        .await
        .expect("Failed to register job_id_scheduled");

    let job_id_running = jobs::register_job(&mut *db, &worker_id, &job_desc_str)
        .await
        .expect("Failed to register job_id_running");
    jobs::update_job_status(&mut *db, &job_id_running, JobStatus::Running)
        .await
        .expect("Failed to update job_id_running to Running");

    // Terminal state jobs (should not be retrieved)
    let job_id_completed = jobs::register_job(&mut *db, &worker_id, &job_desc_str)
        .await
        .expect("Failed to register job_id_completed");
    jobs::update_job_status(&mut *db, &job_id_completed, JobStatus::Completed)
        .await
        .expect("Failed to update job_id_completed to Completed");

    let job_id_failed = jobs::register_job(&mut *db, &worker_id, &job_desc_str)
        .await
        .expect("Failed to schedule job_id_failed");
    jobs::update_job_status(&mut *db, &job_id_failed, JobStatus::Failed)
        .await
        .expect("Failed to update job_id_failed to Failed");

    let job_id_stopped = jobs::register_job(&mut *db, &worker_id, &job_desc_str)
        .await
        .expect("Failed to register job_id_stopped");
    jobs::update_job_status(&mut *db, &job_id_stopped, JobStatus::Stopped)
        .await
        .expect("Failed to update job_id_stopped to Stopped");

    //* When
    let active_job_ids = jobs::get_active_job_ids_for_node(&mut *db, &worker_id)
        .await
        .expect("Failed to get active job IDs for node");

    //* Then
    assert_eq!(active_job_ids, [job_id_scheduled, job_id_running]);
}
