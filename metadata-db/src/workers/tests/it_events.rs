//! In-tree DB integration tests for the workers events system

use std::pin::pin;

use futures::stream::StreamExt;
use pgtemp::PgTempDB;
use tokio::time::{Duration, timeout};

use crate::{
    conn::DbConn,
    jobs::JobId,
    workers::{
        events::{self, JobNotifAction, JobNotification},
        node_id::WorkerNodeId,
    },
};

#[tokio::test]
async fn send_and_receive_start_notification() {
    //* Given
    let temp_db = PgTempDB::new();
    let mut conn = DbConn::connect_with_retry(&temp_db.connection_uri())
        .await
        .expect("Failed to connect to metadata db");
    conn.run_migrations()
        .await
        .expect("Failed to run migrations");

    let worker_id: WorkerNodeId = "test-worker-notify".parse().expect("Invalid worker ID");
    let job_id = JobId::new_unchecked(1i64);

    // Create listener
    let listener = events::listen_url(&temp_db.connection_uri())
        .await
        .expect("Failed to create listener");
    let mut stream = std::pin::pin!(listener.into_stream());

    //* When
    let notification = JobNotification::start(worker_id.clone(), job_id);
    events::notify(&mut *conn, notification.clone())
        .await
        .expect("Failed to send notification");

    //* Then
    let received = timeout(Duration::from_secs(5), stream.next())
        .await
        .expect("Timeout waiting for notification")
        .expect("Stream ended unexpectedly")
        .expect("Failed to receive notification");

    assert_eq!(received.node_id, worker_id);
    assert_eq!(received.job_id, job_id);
    assert!(matches!(received.action, JobNotifAction::Start));
}

#[tokio::test]
async fn send_and_receive_stop_notification() {
    //* Given
    let temp_db = PgTempDB::new();
    let mut conn = DbConn::connect_with_retry(&temp_db.connection_uri())
        .await
        .expect("Failed to connect to metadata db");
    conn.run_migrations()
        .await
        .expect("Failed to run migrations");

    let worker_id: WorkerNodeId = "test-worker-stop".parse().expect("Invalid worker ID");
    let job_id = JobId::new_unchecked(2i64);

    let listener = events::listen_url(&temp_db.connection_uri())
        .await
        .expect("Failed to create listener");
    let mut stream = pin!(listener.into_stream());

    //* When
    let notification = JobNotification::stop(worker_id.clone(), job_id);
    events::notify(&mut *conn, notification.clone())
        .await
        .expect("Failed to send notification");

    //* Then
    let received = timeout(Duration::from_secs(5), stream.next())
        .await
        .expect("Timeout waiting for notification")
        .expect("Stream ended unexpectedly")
        .expect("Failed to receive notification");

    assert_eq!(received.node_id, worker_id);
    assert_eq!(received.job_id, job_id);
    assert!(matches!(received.action, JobNotifAction::Stop));
}

#[tokio::test]
async fn notification_serialization_roundtrip() {
    //* Given
    let temp_db = PgTempDB::new();
    let mut conn = DbConn::connect_with_retry(&temp_db.connection_uri())
        .await
        .expect("Failed to connect to metadata db");
    conn.run_migrations()
        .await
        .expect("Failed to run migrations");

    let worker_id: WorkerNodeId = "test-worker-serialize".parse().expect("Invalid worker ID");
    let job_id = JobId::new_unchecked(3i64);

    //* When & Then - Test START action
    let start_notification = JobNotification::start(worker_id.clone(), job_id);
    let serialized = serde_json::to_string(&start_notification).expect("Failed to serialize");
    let deserialized: JobNotification =
        serde_json::from_str(&serialized).expect("Failed to deserialize");

    assert_eq!(deserialized.node_id, worker_id);
    assert_eq!(deserialized.job_id, job_id);
    assert!(matches!(deserialized.action, JobNotifAction::Start));

    //* When & Then - Test STOP action
    let stop_notification = JobNotification::stop(worker_id.clone(), job_id);
    let serialized = serde_json::to_string(&stop_notification).expect("Failed to serialize");
    let deserialized: JobNotification =
        serde_json::from_str(&serialized).expect("Failed to deserialize");

    assert_eq!(deserialized.node_id, worker_id);
    assert_eq!(deserialized.job_id, job_id);
    assert!(matches!(deserialized.action, JobNotifAction::Stop));
}

#[tokio::test]
async fn multiple_listeners_receive_same_notification() {
    //* Given
    let temp_db = PgTempDB::new();
    let mut conn = DbConn::connect_with_retry(&temp_db.connection_uri())
        .await
        .expect("Failed to connect to metadata db");
    conn.run_migrations()
        .await
        .expect("Failed to run migrations");

    let worker_id: WorkerNodeId = "test-worker-multi".parse().expect("Invalid worker ID");
    let job_id = JobId::new_unchecked(4i64);

    // Create multiple listeners
    let listener1 = events::listen_url(&temp_db.connection_uri())
        .await
        .expect("Failed to create listener 1");
    let mut stream1 = std::pin::pin!(listener1.into_stream());

    let listener2 = events::listen_url(&temp_db.connection_uri())
        .await
        .expect("Failed to create listener 2");
    let mut stream2 = std::pin::pin!(listener2.into_stream());

    //* When
    let notification = JobNotification::start(worker_id.clone(), job_id);
    events::notify(&mut *conn, notification.clone())
        .await
        .expect("Failed to send notification");

    //* Then
    let received1 = timeout(Duration::from_secs(5), stream1.next())
        .await
        .expect("Timeout waiting for notification on listener 1")
        .expect("Stream 1 ended unexpectedly")
        .expect("Failed to receive notification on listener 1");

    let received2 = timeout(Duration::from_secs(5), stream2.next())
        .await
        .expect("Timeout waiting for notification on listener 2")
        .expect("Stream 2 ended unexpectedly")
        .expect("Failed to receive notification on listener 2");

    // Both listeners should receive the same notification
    assert_eq!(received1.node_id, worker_id);
    assert_eq!(received1.job_id, job_id);
    assert_eq!(received2.node_id, worker_id);
    assert_eq!(received2.job_id, job_id);
    assert!(matches!(received1.action, JobNotifAction::Start));
    assert!(matches!(received2.action, JobNotifAction::Start));
}

#[tokio::test]
async fn listener_stream_yields_notifications() {
    //* Given
    let temp_db = PgTempDB::new();
    let mut conn = DbConn::connect_with_retry(&temp_db.connection_uri())
        .await
        .expect("Failed to connect to metadata db");
    conn.run_migrations()
        .await
        .expect("Failed to run migrations");

    let worker_id: WorkerNodeId = "test-worker-stream".parse().expect("Invalid worker ID");
    let job_id1 = JobId::new_unchecked(5i64);
    let job_id2 = JobId::new_unchecked(6i64);

    let listener = events::listen_url(&temp_db.connection_uri())
        .await
        .expect("Failed to create listener");
    let mut stream = std::pin::pin!(listener.into_stream());

    //* When
    let notification1 = JobNotification::start(worker_id.clone(), job_id1);
    let notification2 = JobNotification::stop(worker_id.clone(), job_id2);

    events::notify(&mut *conn, notification1)
        .await
        .expect("Failed to send notification 1");
    events::notify(&mut *conn, notification2)
        .await
        .expect("Failed to send notification 2");

    //* Then
    let received1 = timeout(Duration::from_secs(5), stream.next())
        .await
        .expect("Timeout waiting for notification 1")
        .expect("Stream ended unexpectedly")
        .expect("Failed to receive notification 1");

    let received2 = timeout(Duration::from_secs(5), stream.next())
        .await
        .expect("Timeout waiting for notification 2")
        .expect("Stream ended unexpectedly")
        .expect("Failed to receive notification 2");

    assert_eq!(received1.node_id, worker_id);
    assert_eq!(received1.job_id, job_id1);
    assert!(matches!(received1.action, JobNotifAction::Start));

    assert_eq!(received2.node_id, worker_id);
    assert_eq!(received2.job_id, job_id2);
    assert!(matches!(received2.action, JobNotifAction::Stop));
}

#[tokio::test]
async fn invalid_payload_deserialization_error() {
    //* Given
    let temp_db = PgTempDB::new();
    let mut conn = DbConn::connect_with_retry(&temp_db.connection_uri())
        .await
        .expect("Failed to connect to metadata db");
    conn.run_migrations()
        .await
        .expect("Failed to run migrations");

    let listener = events::listen_url(&temp_db.connection_uri())
        .await
        .expect("Failed to create listener");
    let mut stream = std::pin::pin!(listener.into_stream());

    //* When - Send invalid JSON payload directly via PostgreSQL
    sqlx::query("SELECT pg_notify('worker_actions', $1)")
        .bind("{invalid json}")
        .execute(&mut *conn)
        .await
        .expect("Failed to send invalid notification");

    //* Then
    let result = timeout(Duration::from_secs(5), stream.next())
        .await
        .expect("Timeout waiting for notification")
        .expect("Stream ended unexpectedly");

    assert!(
        result.is_err(),
        "Expected deserialization error, got: {:?}",
        result
    );
}

#[tokio::test]
async fn notification_not_received_before_listen() {
    //* Given
    let temp_db = PgTempDB::new();
    let mut conn = DbConn::connect_with_retry(&temp_db.connection_uri())
        .await
        .expect("Failed to connect to metadata db");
    conn.run_migrations()
        .await
        .expect("Failed to run migrations");

    let worker_id: WorkerNodeId = "test-worker-timing".parse().expect("Invalid worker ID");
    let job_id = JobId::new_unchecked(7i64);

    //* When - Send notification BEFORE creating listener
    let notification = JobNotification::start(worker_id.clone(), job_id);
    events::notify(&mut *conn, notification)
        .await
        .expect("Failed to send notification");

    // Now create the listener (after the notification was sent)
    let listener = events::listen_url(&temp_db.connection_uri())
        .await
        .expect("Failed to create listener");
    let mut stream = std::pin::pin!(listener.into_stream());

    //* Then - Should not receive the notification that was sent before LISTEN
    let result = timeout(Duration::from_millis(500), stream.next()).await;
    assert!(
        result.is_err(),
        "Should not have received notification sent before LISTEN"
    );
}
