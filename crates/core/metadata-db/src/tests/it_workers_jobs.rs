//! DB integration tests for the workers queue

use crate::{
    job_events,
    jobs::{self, JobStatus},
    tests::helpers::{TEST_WORKER_ID, raw_descriptor, register_job, setup_test_db},
    workers::WorkerNodeId,
};

#[tokio::test]
async fn schedule_and_retrieve_job() {
    //* Given
    let (_db, conn) = setup_test_db().await;
    let worker_id = WorkerNodeId::from_ref_unchecked(TEST_WORKER_ID);

    // Specify the job descriptor
    let job_desc_json = serde_json::json!({
        "dataset": "test-dataset",
        "dataset_version": "test-dataset-version",
        "table": "test-table",
        "locations": ["test-location"],
    });
    let job_desc = raw_descriptor(&job_desc_json);

    //* When
    // Register the job
    let job_id = register_job(&conn, &job_desc, &worker_id, None, None).await;

    // Get the job
    let job = jobs::get_by_id(&conn, job_id)
        .await
        .expect("Failed to get job")
        .expect("Job not found");

    //* Then
    assert_eq!(job.id, job_id);
    assert_eq!(job.status, JobStatus::Scheduled);
    assert_eq!(job.node_id, worker_id);
    let desc = job_events::get_latest_descriptor(&conn, job_id)
        .await
        .expect("Failed to get job descriptor")
        .expect("Job descriptor not found");
    let roundtripped: serde_json::Value =
        serde_json::from_str(desc.as_str()).expect("Failed to parse descriptor");
    assert_eq!(roundtripped, job_desc_json);
}

#[tokio::test]
async fn pagination_traverses_all_jobs_ordered() {
    //* Given
    let (_db, conn) = setup_test_db().await;

    let total_jobs = 7;
    let worker_id = WorkerNodeId::from_ref_unchecked(TEST_WORKER_ID);

    let mut created_job_ids = Vec::new();
    for i in 0..total_jobs {
        let job_desc = raw_descriptor(&serde_json::json!({
            "dataset": format!("dataset-{}", i),
            "table": "test-table",
        }));

        let job_id = register_job(&conn, &job_desc, &worker_id, None, None).await;
        created_job_ids.push(job_id);
    }

    //* When
    let mut all_jobs = Vec::new();
    let page_size = 3;
    let mut cursor = None;

    loop {
        let page = jobs::list(&conn, page_size, cursor, None)
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
