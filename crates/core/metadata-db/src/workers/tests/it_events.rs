//! In-tree DB integration tests for the job events system

use std::pin::pin;

use futures::stream::StreamExt;
use pgtemp::PgTempDB;
use tokio::time::{Duration, timeout};

use crate::{
    conn::DbConn,
    jobs::JobId,
    workers::{NodeId, events},
};

/// Test payload for job notifications (without node_id as it's in the wrapper)
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
struct TestJobPayload {
    job_id: JobId,
    action: String,
}

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

    let worker_id = NodeId::from_ref_unchecked("test-worker-notify");
    let job_id = JobId::from_i64_unchecked(1i64);

    // Create listener for this specific worker
    let listener = events::listen_url(&temp_db.connection_uri(), worker_id.to_owned())
        .await
        .expect("Failed to create listener");
    let mut stream = std::pin::pin!(listener.into_stream::<TestJobPayload>());

    //* When
    let payload = TestJobPayload {
        job_id,
        action: "START".to_string(),
    };
    events::notify(&mut *conn, worker_id.to_owned(), &payload)
        .await
        .expect("Failed to send notification");

    //* Then
    let received = timeout(Duration::from_secs(5), stream.next())
        .await
        .expect("Timeout waiting for notification")
        .expect("Stream ended unexpectedly")
        .expect("Failed to receive notification");

    assert_eq!(received.job_id, job_id);
    assert_eq!(received.action, "START");
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

    let worker_id = NodeId::from_ref_unchecked("test-worker-stop");
    let job_id = JobId::from_i64_unchecked(2i64);

    let listener = events::listen_url(&temp_db.connection_uri(), worker_id.to_owned())
        .await
        .expect("Failed to create listener");
    let mut stream = pin!(listener.into_stream::<TestJobPayload>());

    //* When
    let payload = TestJobPayload {
        job_id,
        action: "STOP".to_string(),
    };
    events::notify(&mut *conn, worker_id.to_owned(), &payload)
        .await
        .expect("Failed to send notification");

    //* Then
    let received = timeout(Duration::from_secs(5), stream.next())
        .await
        .expect("Timeout waiting for notification")
        .expect("Stream ended unexpectedly")
        .expect("Failed to receive notification");

    assert_eq!(received.job_id, job_id);
    assert_eq!(received.action, "STOP");
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

    let worker_id = NodeId::from_ref_unchecked("test-worker-multi");
    let job_id = JobId::from_i64_unchecked(4i64);

    // Create multiple listeners both listening for the same worker
    let listener1 = events::listen_url(&temp_db.connection_uri(), worker_id.to_owned())
        .await
        .expect("Failed to create listener 1");
    let mut stream1 = std::pin::pin!(listener1.into_stream::<TestJobPayload>());

    let listener2 = events::listen_url(&temp_db.connection_uri(), worker_id.to_owned())
        .await
        .expect("Failed to create listener 2");
    let mut stream2 = std::pin::pin!(listener2.into_stream::<TestJobPayload>());

    //* When
    let payload = TestJobPayload {
        job_id,
        action: "START".to_string(),
    };
    events::notify(&mut *conn, worker_id.to_owned(), &payload)
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

    // Both listeners should receive the same notification payload
    assert_eq!(received1.job_id, job_id);
    assert_eq!(received2.job_id, job_id);
    assert_eq!(received1.action, "START");
    assert_eq!(received2.action, "START");
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

    let worker_id = NodeId::from_ref_unchecked("test-worker-stream");
    let job_id1 = JobId::from_i64_unchecked(5i64);
    let job_id2 = JobId::from_i64_unchecked(6i64);

    let listener = events::listen_url(&temp_db.connection_uri(), worker_id.to_owned())
        .await
        .expect("Failed to create listener");
    let mut stream = std::pin::pin!(listener.into_stream::<TestJobPayload>());

    //* When
    let payload1 = TestJobPayload {
        job_id: job_id1,
        action: "START".to_string(),
    };
    let payload2 = TestJobPayload {
        job_id: job_id2,
        action: "STOP".to_string(),
    };

    events::notify(&mut *conn, worker_id.to_owned(), &payload1)
        .await
        .expect("Failed to send notification 1");
    events::notify(&mut *conn, worker_id.to_owned(), &payload2)
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

    assert_eq!(received1.job_id, job_id1);
    assert_eq!(received1.action, "START");

    assert_eq!(received2.job_id, job_id2);
    assert_eq!(received2.action, "STOP");
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

    let worker_id = NodeId::from_ref_unchecked("test-worker-timing");
    let job_id = JobId::from_i64_unchecked(7i64);

    //* When - Send notification BEFORE creating listener
    let payload = TestJobPayload {
        job_id,
        action: "START".to_string(),
    };
    events::notify(&mut *conn, worker_id.to_owned(), &payload)
        .await
        .expect("Failed to send notification");

    // Now create the listener (after the notification was sent)
    let listener = events::listen_url(&temp_db.connection_uri(), worker_id.to_owned())
        .await
        .expect("Failed to create listener");
    let mut stream = std::pin::pin!(listener.into_stream::<TestJobPayload>());

    //* Then - Should not receive the notification that was sent before LISTEN
    let result = timeout(Duration::from_millis(500), stream.next()).await;
    assert!(
        result.is_err(),
        "Should not have received notification sent before LISTEN"
    );
}
