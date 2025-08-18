//! DB integration tests for the workers events notifications

use futures::StreamExt;
use metadata_db::{JobNotifAction, JobStatus, MetadataDb};
use pgtemp::PgTempDB;

#[tokio::test]
async fn schedule_job_and_receive_notification() {
    //* Given
    let temp_db = PgTempDB::new();

    let metadata_db =
        MetadataDb::connect(&temp_db.connection_uri(), MetadataDb::default_pool_size())
            .await
            .expect("Failed to connect to metadata db");

    // Pre-register the worker
    let worker_id = "test-worker-events".parse().expect("Invalid worker ID");
    metadata_db
        .register_worker(&worker_id)
        .await
        .expect("Failed to pre-register the worker");

    // Specify the job descriptor
    let job_desc = serde_json::json!({
        "dataset": "test-dataset-events",
        "dataset_version": "test-dataset-version",
        "table": "test-table",
        "locations": ["test-location"],
    });
    let job_desc_str = serde_json::to_string(&job_desc).expect("Failed to serialize job desc");

    // Start listening for notifications before scheduling the job
    let listener = metadata_db
        .listen_for_job_notifications()
        .await
        .expect("Failed to create job notification listener");

    let mut notification_stream = std::pin::pin!(listener.into_stream());

    //* When
    // Schedule the job (this will automatically send a START notification)
    let job_id = metadata_db
        .schedule_job(&worker_id, &job_desc_str, &[])
        .await
        .expect("Failed to schedule job");

    // Receive the notification
    let received_notification = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        notification_stream.next(),
    )
    .await
    .expect("Timeout waiting for notification")
    .expect("Stream ended unexpectedly")
    .expect("Failed to receive notification");

    //* Then
    // Verify the notification
    assert_eq!(received_notification.node_id, worker_id);
    assert_eq!(received_notification.job_id, job_id);
    assert!(matches!(
        received_notification.action,
        JobNotifAction::Start
    ));

    // Verify the job was actually registered
    let job = metadata_db
        .get_job(&job_id)
        .await
        .expect("Failed to get job")
        .expect("Job not found");

    assert_eq!(job.id, job_id);
    assert_eq!(job.status, JobStatus::Scheduled);
    assert_eq!(job.node_id, worker_id);
    assert_eq!(job.desc, job_desc);
}
