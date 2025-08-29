//! DB integration tests for the workers queue

use metadata_db::{JobStatus, MetadataDb};
use pgtemp::PgTempDB;

#[tokio::test]
async fn schedule_and_retrieve_job() {
    //* Given
    let temp_db = PgTempDB::new();

    let metadata_db =
        MetadataDb::connect_with_retry(&temp_db.connection_uri(), MetadataDb::default_pool_size())
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

#[tokio::test]
async fn pagination_traverses_all_jobs_ordered() {
    //* Given
    let temp_db = PgTempDB::new();
    let metadata_db =
        MetadataDb::connect_with_retry(&temp_db.connection_uri(), MetadataDb::default_pool_size())
            .await
            .expect("Failed to connect to metadata db");

    let total_jobs = 7;
    let worker_id = "test-worker-traverse".parse().expect("Invalid worker ID");
    metadata_db
        .register_worker(&worker_id)
        .await
        .expect("Failed to register worker");

    let mut created_job_ids = Vec::new();
    for i in 0..total_jobs {
        let job_desc = serde_json::json!({
            "dataset": format!("dataset-{}", i),
            "table": "test-table",
        });
        let job_desc_str = serde_json::to_string(&job_desc).expect("Failed to serialize");

        let job_id = metadata_db
            .schedule_job(&worker_id, &job_desc_str, &[])
            .await
            .expect("Failed to schedule job");
        created_job_ids.push(job_id);
    }

    //* When
    let mut all_jobs = Vec::new();
    let page_size = 3;
    let mut cursor = None;

    loop {
        let page = metadata_db
            .list_jobs_with_details(page_size, cursor)
            .await
            .expect("Failed to list jobs");

        if let Some(job) = page.last() {
            cursor = Some(job.id);
            all_jobs.extend(page);
        } else {
            break;
        }
    }

    //* Then
    assert_eq!(all_jobs.len(), total_jobs);
    let retrieved_ids: Vec<_> = all_jobs.iter().map(|j| j.id).collect();
    for created_id in &created_job_ids {
        assert!(retrieved_ids.contains(created_id));
    }
    let unique_ids: std::collections::HashSet<_> = retrieved_ids.iter().collect();
    assert_eq!(unique_ids.len(), retrieved_ids.len());
    for i in 0..all_jobs.len() - 1 {
        assert!(all_jobs[i].id > all_jobs[i + 1].id);
    }
}
