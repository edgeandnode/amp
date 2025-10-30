//! In-tree DB integration tests for the workers queue

use pgtemp::PgTempDB;

use crate::{
    MetadataDb, WorkerInfo, WorkerNodeId,
    jobs::{self, JobStatus},
    workers,
};

#[tokio::test]
async fn register_job_creates_with_scheduled_status() {
    //* Given
    let temp_db = PgTempDB::new();
    let metadata_db =
        MetadataDb::connect_with_retry(&temp_db.connection_uri(), MetadataDb::default_pool_size())
            .await
            .expect("Failed to connect to metadata db");

    let worker_id = WorkerNodeId::from_ref_unchecked("test-worker-id");
    let worker_info = WorkerInfo::default(); // {}
    workers::register(&metadata_db, worker_id.clone(), worker_info)
        .await
        .expect("Failed to pre-register the worker");

    let job_desc = serde_json::json!({
        "dataset": "test-dataset",
        "dataset_version": "test-dataset-version",
        "table": "test-table",
        "locations": ["test-location"],
    });
    let job_desc_str = serde_json::to_string(&job_desc).expect("Failed to serialize job desc");

    //* When
    let job_id =
        jobs::sql::insert_with_default_status(&metadata_db, worker_id.clone(), &job_desc_str)
            .await
            .expect("Failed to schedule job");

    //* Then
    let job = jobs::sql::get_by_id(&metadata_db, job_id)
        .await
        .expect("Failed to get job")
        .expect("Job not found");
    assert_eq!(job.id, job_id);
    assert_eq!(job.status, JobStatus::Scheduled);
    assert_eq!(job.node_id, worker_id);
    assert_eq!(job.desc, job_desc);
}

#[tokio::test]
async fn get_jobs_for_node_filters_by_node_id() {
    //* Given
    let temp_db = PgTempDB::new();
    let metadata_db =
        MetadataDb::connect_with_retry(&temp_db.connection_uri(), MetadataDb::default_pool_size())
            .await
            .expect("Failed to connect to metadata db");

    let worker_id_main = WorkerNodeId::from_ref_unchecked("test-worker-main");
    let worker_info = WorkerInfo::default(); // {}
    workers::register(&metadata_db, worker_id_main.clone(), worker_info)
        .await
        .expect("Failed to pre-register the worker 1");
    let worker_id_other = WorkerNodeId::from_ref_unchecked("test-worker-other");
    let worker_info = WorkerInfo::default(); // {}
    workers::register(&metadata_db, worker_id_other.clone(), worker_info)
        .await
        .expect("Failed to pre-register the worker 2");

    // Register jobs
    let job_desc1 = serde_json::json!({ "job": 1 });
    let job_desc_str1 = serde_json::to_string(&job_desc1).expect("Failed to serialize");
    let job_id1 = jobs::sql::insert(
        &metadata_db,
        worker_id_main.clone(),
        &job_desc_str1,
        JobStatus::default(),
    )
    .await
    .expect("Failed to register job 1");

    let job_desc2 = serde_json::json!({ "job": 2 });
    let job_desc_str2 = serde_json::to_string(&job_desc2).expect("Failed to serialize");
    let job_id2 = jobs::sql::insert(
        &metadata_db,
        worker_id_main.clone(),
        &job_desc_str2,
        JobStatus::default(),
    )
    .await
    .expect("Failed to register job 2");

    // Register a job for a different worker to ensure it's not retrieved
    let job_desc_other = serde_json::json!({ "job": "other" });
    let job_desc_str_other = serde_json::to_string(&job_desc_other).expect("Failed to serialize");
    let job_id_other = jobs::sql::insert(
        &metadata_db,
        worker_id_other.clone(),
        &job_desc_str_other,
        JobStatus::default(),
    )
    .await
    .expect("Failed to register job for other worker");

    //* When
    let jobs_list = jobs::sql::get_by_node_id_and_statuses(
        &metadata_db,
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
    let temp_db = PgTempDB::new();

    // Connect to the DB
    let metadata_db =
        MetadataDb::connect_with_retry(&temp_db.connection_uri(), MetadataDb::default_pool_size())
            .await
            .expect("Failed to connect to metadata db");

    let worker_id = WorkerNodeId::from_ref_unchecked("test-worker-active-ids");
    let worker_info = WorkerInfo::default(); // {}
    workers::register(&metadata_db, worker_id.clone(), worker_info)
        .await
        .expect("Failed to pre-register the worker");

    let job_desc = serde_json::json!({ "key": "value" });
    let job_desc_str = serde_json::to_string(&job_desc).expect("Failed to serialize");

    // Active jobs
    let job_id_scheduled = jobs::sql::insert(
        &metadata_db,
        worker_id.clone(),
        &job_desc_str,
        JobStatus::Scheduled,
    )
    .await
    .expect("Failed to register job_id_scheduled");

    let job_id_running = jobs::sql::insert(
        &metadata_db,
        worker_id.clone(),
        &job_desc_str,
        JobStatus::Running,
    )
    .await
    .expect("Failed to register job_id_running");

    // Terminal state jobs (should not be retrieved)
    jobs::sql::insert(
        &metadata_db,
        worker_id.clone(),
        &job_desc_str,
        JobStatus::Completed,
    )
    .await
    .expect("Failed to register completed job");

    jobs::sql::insert(
        &metadata_db,
        worker_id.clone(),
        &job_desc_str,
        JobStatus::Failed,
    )
    .await
    .expect("Failed to register failed job");

    let job_id_stop_requested = jobs::sql::insert(
        &metadata_db,
        worker_id.clone(),
        &job_desc_str,
        JobStatus::StopRequested,
    )
    .await
    .expect("Failed to register job_id_stop_requested");

    jobs::sql::insert(
        &metadata_db,
        worker_id.clone(),
        &job_desc_str,
        JobStatus::Stopped,
    )
    .await
    .expect("Failed to register stopped job");

    //* When
    let active_jobs = jobs::sql::get_by_node_id_and_statuses(
        &metadata_db,
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
    let temp_db = PgTempDB::new();
    let metadata_db =
        MetadataDb::connect_with_retry(&temp_db.connection_uri(), MetadataDb::default_pool_size())
            .await
            .expect("Failed to connect to metadata db");

    let worker_id = WorkerNodeId::from_ref_unchecked("test-worker-get");
    let worker_info = WorkerInfo::default(); // {}
    workers::register(&metadata_db, worker_id.clone(), worker_info)
        .await
        .expect("Failed to register worker");

    let job_desc = serde_json::json!({
        "dataset": "test-dataset",
        "table": "test-table",
        "operation": "dump"
    });
    let job_desc_str = serde_json::to_string(&job_desc).expect("Failed to serialize");

    let job_id =
        jobs::sql::insert_with_default_status(&metadata_db, worker_id.clone(), &job_desc_str)
            .await
            .expect("Failed to register job");

    //* When
    let job = jobs::sql::get_by_id(&metadata_db, job_id)
        .await
        .expect("Failed to get job")
        .expect("Job not found");

    //* Then
    assert_eq!(job.id, job_id);
    assert_eq!(job.node_id, worker_id);
    assert_eq!(job.status, JobStatus::Scheduled);
    assert_eq!(job.desc, job_desc);
}

#[tokio::test]
async fn get_job_includes_timestamps() {
    //* Given
    let temp_db = PgTempDB::new();
    let metadata_db =
        MetadataDb::connect_with_retry(&temp_db.connection_uri(), MetadataDb::default_pool_size())
            .await
            .expect("Failed to connect to metadata db");

    let worker_id = WorkerNodeId::from_ref_unchecked("test-worker-details");
    let worker_info = WorkerInfo::default(); // {}
    workers::register(&metadata_db, worker_id.clone(), worker_info)
        .await
        .expect("Failed to register worker");

    let job_desc = serde_json::json!({
        "dataset": "detailed-dataset",
        "table": "detailed-table",
    });
    let job_desc_str = serde_json::to_string(&job_desc).expect("Failed to serialize");

    let job_id =
        jobs::sql::insert_with_default_status(&metadata_db, worker_id.clone(), &job_desc_str)
            .await
            .expect("Failed to register job");

    //* When
    let job = jobs::sql::get_by_id(&metadata_db, job_id)
        .await
        .expect("Failed to get job")
        .expect("Job not found");

    //* Then
    assert_eq!(job.id, job_id);
    assert_eq!(job.node_id, worker_id);
    assert_eq!(job.status, JobStatus::Scheduled);
    assert_eq!(job.desc, job_desc);
    assert!(job.created_at <= job.updated_at);
}

#[tokio::test]
async fn list_jobs_first_page_when_empty() {
    //* Given
    let temp_db = PgTempDB::new();
    let metadata_db =
        MetadataDb::connect_with_retry(&temp_db.connection_uri(), MetadataDb::default_pool_size())
            .await
            .expect("Failed to connect to metadata db");

    //* When
    let jobs = jobs::sql::list_first_page(&metadata_db, 10)
        .await
        .expect("Failed to list jobs");

    //* Then
    assert!(jobs.is_empty());
}

#[tokio::test]
async fn list_jobs_first_page_respects_limit() {
    //* Given
    let temp_db = PgTempDB::new();
    let metadata_db =
        MetadataDb::connect_with_retry(&temp_db.connection_uri(), MetadataDb::default_pool_size())
            .await
            .expect("Failed to connect to metadata db");

    // Create workers and jobs
    let mut job_ids = Vec::new();
    for i in 0..5 {
        let worker_id = WorkerNodeId::from_owned_unchecked(format!("test-worker-{}", i));
        let worker_info = WorkerInfo::default(); // {}
        workers::register(&metadata_db, worker_id.clone(), worker_info)
            .await
            .expect("Failed to register worker");

        let job_desc = serde_json::json!({
            "dataset": format!("dataset-{}", i),
            "table": "test-table",
        });
        let job_desc_str = serde_json::to_string(&job_desc).expect("Failed to serialize");

        let job_id =
            jobs::sql::insert_with_default_status(&metadata_db, worker_id.clone(), &job_desc_str)
                .await
                .expect("Failed to register job");
        job_ids.push(job_id);

        // Small delay to ensure different timestamps
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
    }

    //* When
    let jobs = jobs::sql::list_first_page(&metadata_db, 3)
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
    let temp_db = PgTempDB::new();
    let metadata_db =
        MetadataDb::connect_with_retry(&temp_db.connection_uri(), MetadataDb::default_pool_size())
            .await
            .expect("Failed to connect to metadata db");

    // Create 10 jobs
    let mut all_job_ids = Vec::new();
    for i in 0..10 {
        let worker_id = WorkerNodeId::from_owned_unchecked(format!("test-worker-page-{}", i));
        let worker_info = WorkerInfo::default(); // {}
        workers::register(&metadata_db, worker_id.clone(), worker_info)
            .await
            .expect("Failed to register worker");

        let job_desc = serde_json::json!({
            "dataset": format!("dataset-{}", i),
            "table": "test-table",
        });
        let job_desc_str = serde_json::to_string(&job_desc).expect("Failed to serialize");

        let job_id =
            jobs::sql::insert_with_default_status(&metadata_db, worker_id.clone(), &job_desc_str)
                .await
                .expect("Failed to register job");
        all_job_ids.push(job_id);

        // Small delay to ensure different timestamps
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
    }

    // Get the first page to establish cursor
    let first_page = jobs::sql::list_first_page(&metadata_db, 3)
        .await
        .expect("Failed to list first page");
    let cursor = first_page
        .last()
        .expect("First page should not be empty")
        .id;

    //* When
    let second_page = jobs::sql::list_next_page(&metadata_db, 3, cursor)
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
    let temp_db = PgTempDB::new();
    let metadata_db =
        MetadataDb::connect_with_retry(&temp_db.connection_uri(), MetadataDb::default_pool_size())
            .await
            .expect("Failed to connect to metadata db");

    let worker_id = WorkerNodeId::from_ref_unchecked("test-worker-delete");
    let worker_info = WorkerInfo::default(); // {}
    workers::register(&metadata_db, worker_id.clone(), worker_info)
        .await
        .expect("Failed to register worker");

    let job_desc = serde_json::json!({"test": "job"});
    let job_desc_str = serde_json::to_string(&job_desc).expect("Failed to serialize");

    let job_id =
        jobs::sql::insert_with_default_status(&metadata_db, worker_id.clone(), &job_desc_str)
            .await
            .expect("Failed to insert job");

    //* When
    let deleted =
        jobs::sql::delete_by_id_and_statuses(&metadata_db, job_id, [JobStatus::Scheduled])
            .await
            .expect("Failed to delete job");

    //* Then
    assert!(deleted);

    let job = jobs::sql::get_by_id(&metadata_db, job_id)
        .await
        .expect("Failed to query job");
    assert!(job.is_none());
}

#[tokio::test]
async fn delete_by_id_and_statuses_does_not_delete_wrong_status() {
    //* Given
    let temp_db = PgTempDB::new();
    let metadata_db =
        MetadataDb::connect_with_retry(&temp_db.connection_uri(), MetadataDb::default_pool_size())
            .await
            .expect("Failed to connect to metadata db");

    let worker_id = WorkerNodeId::from_ref_unchecked("test-worker-no-delete");
    let worker_info = WorkerInfo::default(); // {}
    workers::register(&metadata_db, worker_id.clone(), worker_info)
        .await
        .expect("Failed to register worker");

    let job_desc = serde_json::json!({"test": "job"});
    let job_desc_str = serde_json::to_string(&job_desc).expect("Failed to serialize");

    let job_id = jobs::sql::insert(
        &metadata_db,
        worker_id.clone(),
        &job_desc_str,
        JobStatus::Running,
    )
    .await
    .expect("Failed to insert job");

    //* When
    let deleted =
        jobs::sql::delete_by_id_and_statuses(&metadata_db, job_id, [JobStatus::Scheduled])
            .await
            .expect("Failed to delete job");

    //* Then
    assert!(!deleted);

    let job = jobs::sql::get_by_id(&metadata_db, job_id)
        .await
        .expect("Failed to query job")
        .expect("Job should still exist");
    assert_eq!(job.status, JobStatus::Running);
}

#[tokio::test]
async fn delete_by_status_deletes_all_matching_jobs() {
    //* Given
    let temp_db = PgTempDB::new();
    let metadata_db =
        MetadataDb::connect_with_retry(&temp_db.connection_uri(), MetadataDb::default_pool_size())
            .await
            .expect("Failed to connect to metadata db");

    let worker_id = WorkerNodeId::from_ref_unchecked("test-worker-bulk-delete");
    let worker_info = WorkerInfo::default(); // {}
    workers::register(&metadata_db, worker_id.clone(), worker_info)
        .await
        .expect("Failed to register worker");

    let job_desc = serde_json::json!({"test": "job"});
    let job_desc_str = serde_json::to_string(&job_desc).expect("Failed to serialize");

    // Create 3 jobs, 2 will be Completed, 1 will be Running
    let job_id1 = jobs::sql::insert(
        &metadata_db,
        worker_id.clone(),
        &job_desc_str,
        JobStatus::Completed,
    )
    .await
    .expect("Failed to insert job 1");
    let job_id2 = jobs::sql::insert(
        &metadata_db,
        worker_id.clone(),
        &job_desc_str,
        JobStatus::Completed,
    )
    .await
    .expect("Failed to insert job 2");
    let job_id3 = jobs::sql::insert(
        &metadata_db,
        worker_id.clone(),
        &job_desc_str,
        JobStatus::Running,
    )
    .await
    .expect("Failed to insert job 3");

    //* When
    let deleted_count = jobs::sql::delete_by_status(&metadata_db, [JobStatus::Completed])
        .await
        .expect("Failed to delete jobs");

    //* Then
    assert_eq!(deleted_count, 2);

    // Verify the Completed jobs are gone
    assert!(
        jobs::sql::get_by_id(&metadata_db, job_id1)
            .await
            .expect("Failed to query job 1")
            .is_none()
    );
    assert!(
        jobs::sql::get_by_id(&metadata_db, job_id2)
            .await
            .expect("Failed to query job 2")
            .is_none()
    );

    // Verify the Running job still exists
    let running_job = jobs::sql::get_by_id(&metadata_db, job_id3)
        .await
        .expect("Failed to query job 3")
        .expect("Running job should still exist");
    assert_eq!(running_job.status, JobStatus::Running);
}

#[tokio::test]
async fn delete_by_statuses_deletes_jobs_with_any_matching_status() {
    //* Given
    let temp_db = PgTempDB::new();
    let metadata_db =
        MetadataDb::connect_with_retry(&temp_db.connection_uri(), MetadataDb::default_pool_size())
            .await
            .expect("Failed to connect to metadata db");

    let worker_id = WorkerNodeId::from_ref_unchecked("test-worker-multi-delete");
    let worker_info = WorkerInfo::default(); // {}
    workers::register(&metadata_db, worker_id.clone(), worker_info)
        .await
        .expect("Failed to register worker");

    let job_desc = serde_json::json!({"test": "job"});
    let job_desc_str = serde_json::to_string(&job_desc).expect("Failed to serialize");

    // Create 4 jobs with different statuses
    let job_id1 = jobs::sql::insert(
        &metadata_db,
        worker_id.clone(),
        &job_desc_str,
        JobStatus::Completed,
    )
    .await
    .expect("Failed to insert job 1");
    let job_id2 = jobs::sql::insert(
        &metadata_db,
        worker_id.clone(),
        &job_desc_str,
        JobStatus::Failed,
    )
    .await
    .expect("Failed to insert job 2");
    let job_id3 = jobs::sql::insert(
        &metadata_db,
        worker_id.clone(),
        &job_desc_str,
        JobStatus::Stopped,
    )
    .await
    .expect("Failed to insert job 3");
    let job_id4 = jobs::sql::insert(
        &metadata_db,
        worker_id.clone(),
        &job_desc_str,
        JobStatus::Running,
    )
    .await
    .expect("Failed to insert job 4");

    //* When
    let deleted_count = jobs::sql::delete_by_status(
        &metadata_db,
        [JobStatus::Completed, JobStatus::Failed, JobStatus::Stopped],
    )
    .await
    .expect("Failed to delete jobs");

    //* Then
    assert_eq!(deleted_count, 3);

    // Verify terminal jobs are gone
    assert!(
        jobs::sql::get_by_id(&metadata_db, job_id1)
            .await
            .expect("Failed to query job 1")
            .is_none()
    );
    assert!(
        jobs::sql::get_by_id(&metadata_db, job_id2)
            .await
            .expect("Failed to query job 2")
            .is_none()
    );
    assert!(
        jobs::sql::get_by_id(&metadata_db, job_id3)
            .await
            .expect("Failed to query job 3")
            .is_none()
    );

    // Verify the Running job still exists
    let running_job = jobs::sql::get_by_id(&metadata_db, job_id4)
        .await
        .expect("Failed to query job 4")
        .expect("Running job should still exist");
    assert_eq!(running_job.status, JobStatus::Running);
}
