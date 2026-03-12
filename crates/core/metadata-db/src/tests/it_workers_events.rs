//! DB integration tests for the workers events notifications

use futures::StreamExt;

use crate::{
    job_events,
    jobs::{JobId, JobStatus},
    tests::helpers::{TEST_WORKER_ID, raw_descriptor, register_job, setup_test_db},
    workers::{self, WorkerNodeId},
};

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct JobNotification {
    job_id: JobId,
    action: String,
}

#[tokio::test]
async fn schedule_job_and_receive_notification() {
    //* Given
    let (_db, conn) = setup_test_db().await;
    let worker_id = WorkerNodeId::from_ref_unchecked(TEST_WORKER_ID);

    // Specify the job descriptor
    let job_desc_json = serde_json::json!({
        "dataset": "test-dataset-events",
        "dataset_version": "test-dataset-version",
        "table": "test-table",
        "locations": ["test-location"],
    });
    let job_desc = raw_descriptor(&job_desc_json);

    // Start listening for notifications before scheduling the job
    let listener = workers::listen_for_job_notif(&conn, worker_id.clone())
        .await
        .expect("Failed to create job notification listener");

    let mut notification_stream = std::pin::pin!(listener.into_stream::<JobNotification>());

    //* When
    // Register the job
    let job_id = register_job(&conn, &job_desc, &worker_id, None, None).await;

    // Send notification to the worker
    workers::send_job_notif(
        &conn,
        worker_id.to_owned(),
        &JobNotification {
            job_id,
            action: "START".to_string(),
        },
    )
    .await
    .expect("Failed to send job notification");

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
    // Verify the notification payload
    assert_eq!(received_notification.job_id, job_id);
    assert_eq!(received_notification.action, "START");

    // Verify the job was actually registered
    let job = crate::jobs::get_by_id(&conn, job_id)
        .await
        .expect("Failed to get job")
        .expect("Job not found");

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
