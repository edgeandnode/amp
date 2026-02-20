//! DB integration tests for the workers queue

use pgtemp::PgTempDB;

use crate::{DEFAULT_POOL_MAX_CONNECTIONS, JobStatus, WorkerInfo, WorkerNodeId, jobs, workers};

#[tokio::test]
async fn schedule_and_retrieve_job() {
    //* Given
    let temp_db = PgTempDB::new();

    let conn =
        crate::connect_pool_with_retry(&temp_db.connection_uri(), DEFAULT_POOL_MAX_CONNECTIONS)
            .await
            .expect("Failed to connect to metadata db");

    // Pre-register the worker
    let worker_id = WorkerNodeId::from_ref_unchecked("test-worker-id");
    let worker_info = WorkerInfo::default(); // {}
    workers::register(&conn, &worker_id, worker_info)
        .await
        .expect("Failed to pre-register the worker");

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
    let job_id = jobs::register(&conn, &worker_id, &job_desc)
        .await
        .expect("Failed to register job");

    // Get the job
    let job = jobs::get_by_id(&conn, job_id)
        .await
        .expect("Failed to get job")
        .expect("Job not found");

    //* Then
    assert_eq!(job.id, job_id);
    assert_eq!(job.status, JobStatus::Scheduled);
    assert_eq!(job.node_id, worker_id);
    let roundtripped: serde_json::Value =
        serde_json::from_str(job.desc.as_str()).expect("Failed to parse descriptor");
    assert_eq!(roundtripped, job_desc_json);
}

#[tokio::test]
async fn pagination_traverses_all_jobs_ordered() {
    //* Given
    let temp_db = PgTempDB::new();
    let conn =
        crate::connect_pool_with_retry(&temp_db.connection_uri(), DEFAULT_POOL_MAX_CONNECTIONS)
            .await
            .expect("Failed to connect to metadata db");

    let total_jobs = 7;
    let worker_id = WorkerNodeId::from_ref_unchecked("test-worker-traverse");
    let worker_info = WorkerInfo::default(); // {}
    workers::register(&conn, &worker_id, worker_info)
        .await
        .expect("Failed to register worker");

    let mut created_job_ids = Vec::new();
    for i in 0..total_jobs {
        let job_desc = raw_descriptor(&serde_json::json!({
            "dataset": format!("dataset-{}", i),
            "table": "test-table",
        }));

        let job_id = jobs::register(&conn, &worker_id, &job_desc)
            .await
            .expect("Failed to register job");
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

/// Helper to create a [`jobs::JobDescriptorRaw`] from a [`serde_json::Value`].
fn raw_descriptor(value: &serde_json::Value) -> jobs::JobDescriptorRaw<'static> {
    let raw = serde_json::value::to_raw_value(value).expect("Failed to serialize to raw value");
    jobs::JobDescriptorRaw::from_owned_unchecked(raw)
}
