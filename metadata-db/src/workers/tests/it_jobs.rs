//! In-tree DB integration tests for the workers queue

use pgtemp::PgTempDB;

use crate::{
    conn::DbConn,
    workers::{
        heartbeat,
        job_id::JobId,
        jobs::{self, JobStatus},
    },
};

/// Private helper function for tests - simplified job status update
async fn update_job_status<'c, E>(exe: E, id: &JobId, status: JobStatus) -> Result<(), sqlx::Error>
where
    E: sqlx::Executor<'c, Database = sqlx::Postgres>,
{
    let query = indoc::indoc! {r#"
        UPDATE jobs
        SET status = $1, updated_at = (timezone('UTC', now()))
        WHERE id = $2
    "#};
    sqlx::query(query)
        .bind(status)
        .bind(id)
        .execute(exe)
        .await?;
    Ok(())
}

#[tokio::test]
async fn register_job_creates_with_scheduled_status() {
    //* Given
    let temp_db = PgTempDB::new();
    let mut conn = DbConn::connect(&temp_db.connection_uri())
        .await
        .expect("Failed to connect to metadata db");
    conn.run_migrations()
        .await
        .expect("Failed to run migrations");

    let worker_id = "test-worker-id".parse().expect("Invalid worker ID");
    heartbeat::register_worker(&mut *conn, &worker_id)
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
    let job_id = jobs::register(&mut *conn, &worker_id, &job_desc_str)
        .await
        .expect("Failed to schedule job");

    //* Then
    let job = jobs::get_by_id(&mut *conn, &job_id)
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

    // Register jobs
    let job_desc1 = serde_json::json!({ "job": 1 });
    let job_desc_str1 = serde_json::to_string(&job_desc1).expect("Failed to serialize");
    let job_id1 = jobs::register(&mut *conn, &worker_id_main, &job_desc_str1)
        .await
        .expect("Failed to register job 1");

    let job_desc2 = serde_json::json!({ "job": 2 });
    let job_desc_str2 = serde_json::to_string(&job_desc2).expect("Failed to serialize");
    let job_id2 = jobs::register(&mut *conn, &worker_id_main, &job_desc_str2)
        .await
        .expect("Failed to register job 2");

    // Register a job for a different worker to ensure it's not retrieved
    let job_desc_other = serde_json::json!({ "job": "other" });
    let job_desc_str_other = serde_json::to_string(&job_desc_other).expect("Failed to serialize");
    let job_id_other = jobs::register(&mut *conn, &worker_id_other, &job_desc_str_other)
        .await
        .expect("Failed to register job for other worker");

    //* When
    let jobs_list =
        jobs::get_by_node_id_and_statuses(&mut *conn, &worker_id_main, [JobStatus::Scheduled])
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
    let job_id_scheduled = jobs::register(&mut *db, &worker_id, &job_desc_str)
        .await
        .expect("Failed to register job_id_scheduled");

    let job_id_running = jobs::register(&mut *db, &worker_id, &job_desc_str)
        .await
        .expect("Failed to register job_id_running");
    update_job_status(&mut *db, &job_id_running, JobStatus::Running)
        .await
        .expect("Failed to update job_id_running to Running");

    // Terminal state jobs (should not be retrieved)
    let job_id_completed = jobs::register(&mut *db, &worker_id, &job_desc_str)
        .await
        .expect("Failed to register job_id_completed");
    update_job_status(&mut *db, &job_id_completed, JobStatus::Completed)
        .await
        .expect("Failed to update job_id_completed to Completed");

    let job_id_failed = jobs::register(&mut *db, &worker_id, &job_desc_str)
        .await
        .expect("Failed to schedule job_id_failed");
    update_job_status(&mut *db, &job_id_failed, JobStatus::Failed)
        .await
        .expect("Failed to update job_id_failed to Failed");

    let job_id_stop_requested = jobs::register(&mut *db, &worker_id, &job_desc_str)
        .await
        .expect("Failed to register job_id_stop_requested");
    update_job_status(&mut *db, &job_id_stop_requested, JobStatus::StopRequested)
        .await
        .expect("Failed to update job_id_stop_requested to StopRequested");

    let job_id_stopped = jobs::register(&mut *db, &worker_id, &job_desc_str)
        .await
        .expect("Failed to register job_id_stopped");
    update_job_status(&mut *db, &job_id_stopped, JobStatus::Stopped)
        .await
        .expect("Failed to update job_id_stopped to Stopped");

    //* When
    let active_jobs = jobs::get_by_node_id_and_statuses(
        &mut *db,
        &worker_id,
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
    let mut conn = DbConn::connect(&temp_db.connection_uri())
        .await
        .expect("Failed to connect to metadata db");
    conn.run_migrations()
        .await
        .expect("Failed to run migrations");

    let worker_id = "test-worker-get".parse().expect("Invalid worker ID");
    heartbeat::register_worker(&mut *conn, &worker_id)
        .await
        .expect("Failed to register worker");

    let job_desc = serde_json::json!({
        "dataset": "test-dataset",
        "table": "test-table",
        "operation": "dump"
    });
    let job_desc_str = serde_json::to_string(&job_desc).expect("Failed to serialize");

    let job_id = jobs::register(&mut *conn, &worker_id, &job_desc_str)
        .await
        .expect("Failed to register job");

    //* When
    let job = jobs::get_by_id(&mut *conn, &job_id)
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
async fn get_job_with_details_includes_timestamps() {
    //* Given
    let temp_db = PgTempDB::new();
    let mut conn = DbConn::connect(&temp_db.connection_uri())
        .await
        .expect("Failed to connect to metadata db");
    conn.run_migrations()
        .await
        .expect("Failed to run migrations");

    let worker_id = "test-worker-details".parse().expect("Invalid worker ID");
    heartbeat::register_worker(&mut *conn, &worker_id)
        .await
        .expect("Failed to register worker");

    let job_desc = serde_json::json!({
        "dataset": "detailed-dataset",
        "table": "detailed-table",
    });
    let job_desc_str = serde_json::to_string(&job_desc).expect("Failed to serialize");

    let job_id = jobs::register(&mut *conn, &worker_id, &job_desc_str)
        .await
        .expect("Failed to register job");

    //* When
    let job_details = jobs::get_by_id_with_details(&mut *conn, &job_id)
        .await
        .expect("Failed to get job with details")
        .expect("Job not found");

    //* Then
    assert_eq!(job_details.id, job_id);
    assert_eq!(job_details.node_id, worker_id);
    assert_eq!(job_details.status, JobStatus::Scheduled);
    assert_eq!(job_details.desc, job_desc);
    assert!(job_details.created_at <= job_details.updated_at);
}

#[tokio::test]
async fn list_jobs_first_page_when_empty() {
    //* Given
    let temp_db = PgTempDB::new();
    let mut conn = DbConn::connect(&temp_db.connection_uri())
        .await
        .expect("Failed to connect to metadata db");
    conn.run_migrations()
        .await
        .expect("Failed to run migrations");

    //* When
    let jobs = jobs::list_first_page(&mut *conn, 10)
        .await
        .expect("Failed to list jobs");

    //* Then
    assert!(jobs.is_empty());
}

#[tokio::test]
async fn list_jobs_first_page_respects_limit() {
    //* Given
    let temp_db = PgTempDB::new();
    let mut conn = DbConn::connect(&temp_db.connection_uri())
        .await
        .expect("Failed to connect to metadata db");
    conn.run_migrations()
        .await
        .expect("Failed to run migrations");

    // Create workers and jobs
    let mut job_ids = Vec::new();
    for i in 0..5 {
        let worker_id = format!("test-worker-{}", i)
            .parse()
            .expect("Invalid worker ID");
        heartbeat::register_worker(&mut *conn, &worker_id)
            .await
            .expect("Failed to register worker");

        let job_desc = serde_json::json!({
            "dataset": format!("dataset-{}", i),
            "table": "test-table",
        });
        let job_desc_str = serde_json::to_string(&job_desc).expect("Failed to serialize");

        let job_id = jobs::register(&mut *conn, &worker_id, &job_desc_str)
            .await
            .expect("Failed to register job");
        job_ids.push(job_id);

        // Small delay to ensure different timestamps
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
    }

    //* When
    let jobs = jobs::list_first_page(&mut *conn, 3)
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
    let mut conn = DbConn::connect(&temp_db.connection_uri())
        .await
        .expect("Failed to connect to metadata db");
    conn.run_migrations()
        .await
        .expect("Failed to run migrations");

    // Create 10 jobs
    let mut all_job_ids = Vec::new();
    for i in 0..10 {
        let worker_id = format!("test-worker-page-{}", i)
            .parse()
            .expect("Invalid worker ID");
        heartbeat::register_worker(&mut *conn, &worker_id)
            .await
            .expect("Failed to register worker");

        let job_desc = serde_json::json!({
            "dataset": format!("dataset-{}", i),
            "table": "test-table",
        });
        let job_desc_str = serde_json::to_string(&job_desc).expect("Failed to serialize");

        let job_id = jobs::register(&mut *conn, &worker_id, &job_desc_str)
            .await
            .expect("Failed to register job");
        all_job_ids.push(job_id);

        // Small delay to ensure different timestamps
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
    }

    // Get the first page to establish cursor
    let first_page = jobs::list_first_page(&mut *conn, 3)
        .await
        .expect("Failed to list first page");
    let cursor = first_page
        .last()
        .expect("First page should not be empty")
        .id;

    //* When
    let second_page = jobs::list_next_page(&mut *conn, 3, cursor)
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
