//! DB integration tests for the workers queue

use metadata_db::{JobStatus, MetadataDb};
use pgtemp::PgTempDB;

#[tokio::test]
async fn schedule_and_retrieve_job() {
    //* Given
    let temp_db = PgTempDB::new();

    let metadata_db = MetadataDb::connect(&temp_db.connection_uri())
        .await
        .expect("Failed to connect to metadata db");

    // Pre-register the worker
    let worker_id = "test-worker-id".parse().expect("Invalid worker ID");
    metadata_db
        .register_worker(&worker_id)
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
    // Schedule the job
    let job_id = metadata_db
        .schedule_job(&worker_id, &job_desc_str, &[])
        .await
        .expect("Failed to schedule job");

    // Get the job
    let job = metadata_db
        .get_job(&job_id)
        .await
        .expect("Failed to get job")
        .expect("Job not found");

    //* Then
    assert_eq!(job.id, job_id);
    assert_eq!(job.status, JobStatus::Scheduled);
    assert_eq!(job.node_id, worker_id);
    assert_eq!(job.desc, job_desc);
}
