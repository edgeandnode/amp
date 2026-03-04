//! In-tree DB integration tests for the workers queue

use crate::{
    job_events, job_status,
    jobs::{self, JobStatus},
    tests::helpers::{TEST_WORKER_ID, raw_descriptor, register_job, setup_test_db},
    workers::{self, WorkerInfo, WorkerNodeId},
};

#[tokio::test]
async fn register_job_creates_with_scheduled_status() {
    //* Given
    let (_db, conn) = setup_test_db().await;
    let worker_id = WorkerNodeId::from_ref_unchecked(TEST_WORKER_ID);

    let job_desc_json = serde_json::json!({
        "dataset": "test-dataset",
        "dataset_version": "test-dataset-version",
        "table": "test-table",
        "locations": ["test-location"],
    });
    let job_desc = raw_descriptor(&job_desc_json);

    //* When
    let job_id = register_job(&conn, &job_desc, &worker_id, None).await;

    //* Then
    let job = jobs::sql::get_by_id(&conn, job_id)
        .await
        .expect("Failed to get job")
        .expect("Job not found");
    assert_eq!(job.id, job_id);
    assert_eq!(job.status, JobStatus::Scheduled);
    assert_eq!(job.node_id, worker_id);
    let roundtripped: serde_json::Value =
        serde_json::from_str(job.desc.as_str()).expect("Failed to parse descriptor");
    assert_eq!(roundtripped, job_desc_json);
}

#[tokio::test]
async fn get_jobs_for_node_filters_by_node_id() {
    //* Given
    let (_db, conn) = setup_test_db().await;

    let worker_id_main = WorkerNodeId::from_ref_unchecked(TEST_WORKER_ID);
    let worker_id_other = WorkerNodeId::from_ref_unchecked("test-worker-other");
    workers::register(&conn, worker_id_other.clone(), WorkerInfo::default())
        .await
        .expect("Failed to pre-register the worker 2");

    // Register jobs
    let job_desc1 = raw_descriptor(&serde_json::json!({ "job": 1 }));
    let job_id1 = register_job(&conn, &job_desc1, &worker_id_main, None).await;

    let job_desc2 = raw_descriptor(&serde_json::json!({ "job": 2 }));
    let job_id2 = register_job(&conn, &job_desc2, &worker_id_main, None).await;

    // Register a job for a different worker to ensure it's not retrieved
    let job_desc_other = raw_descriptor(&serde_json::json!({ "job": "other" }));
    let job_id_other = register_job(&conn, &job_desc_other, &worker_id_other, None).await;

    //* When
    let jobs_list = jobs::sql::get_by_node_id_and_statuses(
        &conn,
        worker_id_main.clone(),
        [JobStatus::Scheduled],
    )
    .await
    .expect("Failed to get jobs for node");

    //* Then
    assert!(
        jobs_list.iter().any(|job| job.id == job_id1),
        "job 1 not found in jobs list: {:?}",
        jobs_list
    );
    assert!(
        jobs_list.iter().any(|job| job.id == job_id2),
        "job 2 not found in jobs list: {:?}",
        jobs_list
    );
    assert!(
        !jobs_list.iter().any(|job| job.id == job_id_other),
        "job for other worker found in jobs list: {:?}",
        jobs_list
    );
}

#[tokio::test]
async fn get_jobs_for_node_filters_by_status() {
    //* Given
    let (_db, conn) = setup_test_db().await;
    let worker_id = WorkerNodeId::from_ref_unchecked(TEST_WORKER_ID);

    let job_desc = raw_descriptor(&serde_json::json!({ "key": "value" }));

    // Active jobs
    let job_id_scheduled =
        register_job(&conn, &job_desc, &worker_id, Some(JobStatus::Scheduled)).await;

    let job_id_running = register_job(&conn, &job_desc, &worker_id, Some(JobStatus::Running)).await;

    // Terminal state jobs (should not be retrieved)
    register_job(&conn, &job_desc, &worker_id, Some(JobStatus::Completed)).await;

    register_job(&conn, &job_desc, &worker_id, Some(JobStatus::Error)).await;

    let job_id_stop_requested =
        register_job(&conn, &job_desc, &worker_id, Some(JobStatus::StopRequested)).await;

    register_job(&conn, &job_desc, &worker_id, Some(JobStatus::Stopped)).await;

    //* When
    let active_jobs = jobs::sql::get_by_node_id_and_statuses(
        &conn,
        worker_id.clone(),
        [
            JobStatus::Scheduled,
            JobStatus::Running,
            JobStatus::StopRequested,
        ],
    )
    .await
    .expect("Failed to get active job IDs for node");

    //* Then
    assert!(
        active_jobs.iter().any(|job| job.id == job_id_scheduled),
        "scheduled job not found in active jobs: {:?}",
        active_jobs
    );
    assert!(
        active_jobs.iter().any(|job| job.id == job_id_running),
        "running job not found in active jobs: {:?}",
        active_jobs
    );
    assert!(
        active_jobs
            .iter()
            .any(|job| job.id == job_id_stop_requested),
        "stop requested job not found in active jobs: {:?}",
        active_jobs
    );
}

#[tokio::test]
async fn get_job_by_id_returns_job() {
    //* Given
    let (_db, conn) = setup_test_db().await;
    let worker_id = WorkerNodeId::from_ref_unchecked(TEST_WORKER_ID);

    let job_desc_json = serde_json::json!({
        "dataset": "test-dataset",
        "table": "test-table",
        "operation": "dump"
    });
    let job_desc = raw_descriptor(&job_desc_json);

    let job_id = register_job(&conn, &job_desc, &worker_id, None).await;

    //* When
    let job = jobs::sql::get_by_id(&conn, job_id)
        .await
        .expect("Failed to get job")
        .expect("Job not found");

    //* Then
    assert_eq!(job.id, job_id);
    assert_eq!(job.node_id, worker_id);
    assert_eq!(job.status, JobStatus::Scheduled);
    let roundtripped: serde_json::Value =
        serde_json::from_str(job.desc.as_str()).expect("Failed to parse descriptor");
    assert_eq!(roundtripped, job_desc_json);
}

#[tokio::test]
async fn get_job_includes_timestamps() {
    //* Given
    let (_db, conn) = setup_test_db().await;
    let worker_id = WorkerNodeId::from_ref_unchecked(TEST_WORKER_ID);

    let job_desc_json = serde_json::json!({
        "dataset": "detailed-dataset",
        "table": "detailed-table",
    });
    let job_desc = raw_descriptor(&job_desc_json);

    let job_id = register_job(&conn, &job_desc, &worker_id, None).await;

    //* When
    let job = jobs::sql::get_by_id(&conn, job_id)
        .await
        .expect("Failed to get job")
        .expect("Job not found");

    //* Then
    assert_eq!(job.id, job_id);
    assert_eq!(job.node_id, worker_id);
    assert_eq!(job.status, JobStatus::Scheduled);
    let roundtripped: serde_json::Value =
        serde_json::from_str(job.desc.as_str()).expect("Failed to parse descriptor");
    assert_eq!(roundtripped, job_desc_json);
    assert!(job.created_at <= job.updated_at);
}

#[tokio::test]
async fn list_jobs_first_page_when_empty() {
    //* Given
    let (_db, conn) = setup_test_db().await;

    //* When
    let jobs = jobs::sql::list_first_page(&conn, 10, None)
        .await
        .expect("Failed to list jobs");

    //* Then
    assert!(jobs.is_empty());
}

#[tokio::test]
async fn list_jobs_first_page_respects_limit() {
    //* Given
    let (_db, conn) = setup_test_db().await;

    // Create workers and jobs
    let mut job_ids = Vec::new();
    for i in 0..5 {
        let worker_id = WorkerNodeId::from_owned_unchecked(format!("test-worker-{}", i));
        workers::register(&conn, worker_id.clone(), WorkerInfo::default())
            .await
            .expect("Failed to register worker");

        let job_desc = raw_descriptor(&serde_json::json!({
            "dataset": format!("dataset-{}", i),
            "table": "test-table",
        }));

        let job_id = register_job(&conn, &job_desc, &worker_id, None).await;
        job_ids.push(job_id);

        // Small delay to ensure different timestamps
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
    }

    //* When
    let jobs = jobs::sql::list_first_page(&conn, 3, None)
        .await
        .expect("Failed to list jobs");

    //* Then
    assert_eq!(jobs.len(), 3);
    assert!(jobs[0].id > jobs[1].id);
    assert!(jobs[1].id > jobs[2].id);
    for job in &jobs {
        assert_eq!(job.status, JobStatus::Scheduled);
        assert!(job.created_at <= job.updated_at);
    }
}

#[tokio::test]
async fn list_jobs_next_page_uses_cursor() {
    //* Given
    let (_db, conn) = setup_test_db().await;

    // Create 10 jobs
    let mut all_job_ids = Vec::new();
    for i in 0..10 {
        let worker_id = WorkerNodeId::from_owned_unchecked(format!("test-worker-page-{}", i));
        workers::register(&conn, worker_id.clone(), WorkerInfo::default())
            .await
            .expect("Failed to register worker");

        let job_desc = raw_descriptor(&serde_json::json!({
            "dataset": format!("dataset-{}", i),
            "table": "test-table",
        }));

        let job_id = register_job(&conn, &job_desc, &worker_id, None).await;
        all_job_ids.push(job_id);

        // Small delay to ensure different timestamps
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
    }

    // Get the first page to establish cursor
    let first_page = jobs::sql::list_first_page(&conn, 3, None)
        .await
        .expect("Failed to list first page");
    let cursor = first_page
        .last()
        .expect("First page should not be empty")
        .id;

    //* When
    let second_page = jobs::sql::list_next_page(&conn, 3, cursor, None)
        .await
        .expect("Failed to list second page");

    //* Then
    assert_eq!(second_page.len(), 3);
    // Verify no overlap with first page
    let first_page_ids: Vec<_> = first_page.iter().map(|j| j.id).collect();
    for job in &second_page {
        assert!(!first_page_ids.contains(&job.id));
    }
    // Verify ordering
    assert!(second_page[0].id > second_page[1].id);
    assert!(second_page[1].id > second_page[2].id);
    // Verify cursor worked correctly
    assert!(cursor > second_page[0].id);
}

#[tokio::test]
async fn delete_by_id_and_statuses_deletes_matching_job() {
    //* Given
    let (_db, conn) = setup_test_db().await;
    let worker_id = WorkerNodeId::from_ref_unchecked(TEST_WORKER_ID);

    let job_desc = raw_descriptor(&serde_json::json!({"test": "job"}));

    let job_id = register_job(&conn, &job_desc, &worker_id, None).await;

    //* When
    let deleted = jobs::sql::delete_by_id_and_statuses(&conn, job_id, [JobStatus::Scheduled])
        .await
        .expect("Failed to delete job");

    //* Then
    assert!(deleted);

    let job = jobs::sql::get_by_id(&conn, job_id)
        .await
        .expect("Failed to query job");
    assert!(job.is_none());
}

#[tokio::test]
async fn delete_by_id_and_statuses_does_not_delete_wrong_status() {
    //* Given
    let (_db, conn) = setup_test_db().await;
    let worker_id = WorkerNodeId::from_ref_unchecked(TEST_WORKER_ID);

    let job_desc = raw_descriptor(&serde_json::json!({"test": "job"}));

    let job_id = register_job(&conn, &job_desc, &worker_id, Some(JobStatus::Running)).await;

    //* When
    let deleted = jobs::sql::delete_by_id_and_statuses(&conn, job_id, [JobStatus::Scheduled])
        .await
        .expect("Failed to delete job");

    //* Then
    assert!(!deleted);

    let job = jobs::sql::get_by_id(&conn, job_id)
        .await
        .expect("Failed to query job")
        .expect("Job should still exist");
    assert_eq!(job.status, JobStatus::Running);
}

#[tokio::test]
async fn delete_by_status_deletes_all_matching_jobs() {
    //* Given
    let (_db, conn) = setup_test_db().await;
    let worker_id = WorkerNodeId::from_ref_unchecked(TEST_WORKER_ID);

    let job_desc = raw_descriptor(&serde_json::json!({"test": "job"}));

    // Create 3 jobs, 2 will be Completed, 1 will be Running
    let job_id1 = register_job(&conn, &job_desc, &worker_id, Some(JobStatus::Completed)).await;
    let job_id2 = register_job(&conn, &job_desc, &worker_id, Some(JobStatus::Completed)).await;
    let job_id3 = register_job(&conn, &job_desc, &worker_id, Some(JobStatus::Running)).await;

    //* When
    let deleted_count = jobs::sql::delete_by_status(&conn, [JobStatus::Completed])
        .await
        .expect("Failed to delete jobs");

    //* Then
    assert_eq!(deleted_count, 2);

    // Verify the Completed jobs are gone
    assert!(
        jobs::sql::get_by_id(&conn, job_id1)
            .await
            .expect("Failed to query job 1")
            .is_none()
    );
    assert!(
        jobs::sql::get_by_id(&conn, job_id2)
            .await
            .expect("Failed to query job 2")
            .is_none()
    );

    // Verify the Running job still exists
    let running_job = jobs::sql::get_by_id(&conn, job_id3)
        .await
        .expect("Failed to query job 3")
        .expect("Running job should still exist");
    assert_eq!(running_job.status, JobStatus::Running);
}

#[tokio::test]
async fn delete_by_statuses_deletes_jobs_with_any_matching_status() {
    //* Given
    let (_db, conn) = setup_test_db().await;
    let worker_id = WorkerNodeId::from_ref_unchecked(TEST_WORKER_ID);

    let job_desc = raw_descriptor(&serde_json::json!({"test": "job"}));

    // Create 4 jobs with different statuses
    let job_id1 = register_job(&conn, &job_desc, &worker_id, Some(JobStatus::Completed)).await;
    let job_id2 = register_job(&conn, &job_desc, &worker_id, Some(JobStatus::Error)).await;
    let job_id3 = register_job(&conn, &job_desc, &worker_id, Some(JobStatus::Stopped)).await;
    let job_id4 = register_job(&conn, &job_desc, &worker_id, Some(JobStatus::Running)).await;

    //* When
    let deleted_count = jobs::sql::delete_by_status(
        &conn,
        [JobStatus::Completed, JobStatus::Error, JobStatus::Stopped],
    )
    .await
    .expect("Failed to delete jobs");

    //* Then
    assert_eq!(deleted_count, 3);

    // Verify terminal jobs are gone
    assert!(
        jobs::sql::get_by_id(&conn, job_id1)
            .await
            .expect("Failed to query job 1")
            .is_none()
    );
    assert!(
        jobs::sql::get_by_id(&conn, job_id2)
            .await
            .expect("Failed to query job 2")
            .is_none()
    );
    assert!(
        jobs::sql::get_by_id(&conn, job_id3)
            .await
            .expect("Failed to query job 3")
            .is_none()
    );

    // Verify the Running job still exists
    let running_job = jobs::sql::get_by_id(&conn, job_id4)
        .await
        .expect("Failed to query job 4")
        .expect("Running job should still exist");
    assert_eq!(running_job.status, JobStatus::Running);
}

#[tokio::test]
async fn get_failed_jobs_ready_for_retry_returns_eligible_jobs() {
    //* Given
    let (_db, conn) = setup_test_db().await;
    let worker_id = WorkerNodeId::from_ref_unchecked(TEST_WORKER_ID);

    let job_desc = raw_descriptor(&serde_json::json!({"test": "job"}));

    // Create a failed (recoverable) job
    let job_id = register_job(&conn, &job_desc, &worker_id, Some(JobStatus::Error)).await;

    // Wait longer than initial backoff (1 second)
    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;

    //* When
    let ready_jobs = jobs::sql::get_failed_jobs_ready_for_retry(&conn)
        .await
        .expect("Failed to get ready jobs");

    //* Then
    assert_eq!(ready_jobs.len(), 1);
    assert_eq!(ready_jobs[0].job.id, job_id);
    assert_eq!(ready_jobs[0].next_retry_index, 0);
}

#[tokio::test]
async fn get_failed_jobs_ready_for_retry_excludes_not_ready() {
    //* Given
    let (_db, conn) = setup_test_db().await;
    let worker_id = WorkerNodeId::from_ref_unchecked(TEST_WORKER_ID);

    let job_desc = raw_descriptor(&serde_json::json!({"test": "job"}));

    // Create a failed (recoverable) job
    register_job(&conn, &job_desc, &worker_id, Some(JobStatus::Error)).await;

    //* When (immediately check, before backoff expires)
    let ready_jobs = jobs::sql::get_failed_jobs_ready_for_retry(&conn)
        .await
        .expect("Failed to get ready jobs");

    //* Then
    assert_eq!(ready_jobs.len(), 0);
}

#[tokio::test]
async fn get_failed_jobs_calculates_retry_index_from_attempts() {
    //* Given
    let (_db, conn) = setup_test_db().await;
    let worker_id = WorkerNodeId::from_ref_unchecked(TEST_WORKER_ID);

    let job_desc = raw_descriptor(&serde_json::json!({"test": "job"}));

    // Create a failed (recoverable) job
    let job_id = register_job(&conn, &job_desc, &worker_id, Some(JobStatus::Error)).await;

    // Insert 3 SCHEDULED events (simulating initial schedule + 2 retries)
    for _ in 0..3 {
        job_events::sql::insert(&conn, job_id, &worker_id, JobStatus::Scheduled)
            .await
            .expect("Failed to insert SCHEDULED event");
    }

    // Wait longer than backoff for 3 SCHEDULED events (2^3 = 8 seconds)
    tokio::time::sleep(tokio::time::Duration::from_secs(9)).await;

    //* When
    let ready_jobs = jobs::sql::get_failed_jobs_ready_for_retry(&conn)
        .await
        .expect("Failed to get ready jobs");

    //* Then
    assert_eq!(ready_jobs.len(), 1);
    assert_eq!(ready_jobs[0].job.id, job_id);
    assert_eq!(ready_jobs[0].next_retry_index, 3); // COUNT of SCHEDULED events
}

#[tokio::test]
async fn get_failed_jobs_handles_missing_attempts() {
    //* Given
    let (_db, conn) = setup_test_db().await;
    let worker_id = WorkerNodeId::from_ref_unchecked(TEST_WORKER_ID);

    let job_desc = raw_descriptor(&serde_json::json!({"test": "job"}));

    // Create a failed (recoverable) job WITHOUT any SCHEDULED events (edge case)
    let job_id = register_job(&conn, &job_desc, &worker_id, Some(JobStatus::Error)).await;

    // Wait longer than initial backoff (1 second for retry_index 0)
    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;

    //* When
    let ready_jobs = jobs::sql::get_failed_jobs_ready_for_retry(&conn)
        .await
        .expect("Failed to get ready jobs");

    //* Then
    assert_eq!(ready_jobs.len(), 1);
    assert_eq!(ready_jobs[0].job.id, job_id);
    assert_eq!(ready_jobs[0].next_retry_index, 0); // COUNT returns 0 when no SCHEDULED events
}

#[tokio::test]
async fn reschedule_updates_status_and_worker() {
    //* Given
    let (_db, conn) = setup_test_db().await;

    let worker_id1 = WorkerNodeId::from_ref_unchecked(TEST_WORKER_ID);
    let worker_id2 = WorkerNodeId::from_ref_unchecked("test-worker-new");
    workers::register(&conn, worker_id2.clone(), WorkerInfo::default())
        .await
        .expect("Failed to register worker 2");

    let job_desc = raw_descriptor(&serde_json::json!({"test": "job"}));

    let job_id = register_job(&conn, &job_desc, &worker_id1, Some(JobStatus::Error)).await;

    //* When
    job_status::sql::reschedule(&conn, job_id, &worker_id2)
        .await
        .expect("Failed to reschedule job");

    //* Then
    let job = jobs::sql::get_by_id(&conn, job_id)
        .await
        .expect("Failed to get job")
        .expect("Job not found");
    assert_eq!(job.status, JobStatus::Scheduled);
    assert_eq!(job.node_id, worker_id2);
    assert!(job.updated_at > job.created_at);
}
