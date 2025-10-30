//! Integration tests for version polling functionality.
//!
//! Tests verify that:
//! - Version polling only fetches version numbers (not manifests)
//! - Version changes trigger manifest reload
//! - Polling uses efficient endpoints

use datasets_common::{name::Name, namespace::Namespace, version::Version};
use mockito::Server;
use tokio::sync::watch;

/// Test that fetch_latest_version only calls the versions endpoint.
///
/// This is critical for efficiency - we should NOT be fetching the full manifest
/// on every poll interval (default: 5 seconds).
#[tokio::test]
async fn test_fetch_latest_version_uses_versions_endpoint_only() {
    let mut server = Server::new_async().await;

    let dataset_namespace: Namespace = "_".parse().unwrap();
    let dataset_name: Name = "test_dataset".parse().unwrap();

    // Mock the versions/latest endpoint - this is the ONLY endpoint that should be called
    let versions_mock = server
        .mock("GET", "/datasets/_/test_dataset/versions/latest")
        .with_status(200)
        .with_header("content-type", "application/json")
        .with_body(
            r#"{
                "namespace": "_",
                "name": "test_dataset",
                "revision": "0.2.0-LTcyNjgzMjc1NA",
                "manifest_hash": "a7ffc6f8bf1ed76651c14756a061d662f580ff4de43b49fa82d80a4b80f8434a",
                "kind": "evm-rpc"
            }"#,
        )
        .expect(1) // Should be called exactly once
        .create_async()
        .await;

    // Mock the manifest endpoint - this should NOT be called
    let manifest_mock = server
        .mock(
            "GET",
            "/datasets/_/test_dataset/versions/0.2.0-LTcyNjgzMjc1NA/manifest",
        )
        .with_status(200)
        .expect(0) // Should NOT be called
        .create_async()
        .await;

    // Call fetch_latest_version
    let result =
        crate::manifest::fetch_latest_version(&server.url(), &dataset_namespace, &dataset_name)
            .await;

    // Verify success
    assert!(result.is_ok(), "fetch_latest_version failed: {:?}", result);
    let version = result.unwrap();
    assert_eq!(version.to_string(), "0.2.0-LTcyNjgzMjc1NA");

    // Verify mocks were called as expected
    versions_mock.assert_async().await;
    manifest_mock.assert_async().await; // Asserts it was NOT called (expect(0))
}

/// Test that version polling detects version changes.
///
/// Verifies that when a new version appears, the polling task sends a notification
/// through the channel without fetching the manifest.
#[tokio::test]
async fn test_version_polling_detects_changes() {
    let mut server = Server::new_async().await;

    let dataset_namespace: Namespace = "_".parse().unwrap();
    let dataset_name: Name = "test_dataset".parse().unwrap();
    let initial_version: Version = "0.1.0-LTcyNjgzMjc1NA".parse().unwrap();

    // Create watch channel for version notifications
    let (tx, mut rx) = watch::channel::<Version>(initial_version.clone());

    // Mock versions/latest endpoint - returns old version first, then new version
    // Use expect_at_least since the polling task will continue running
    let _versions_mock = server
        .mock("GET", "/datasets/_/test_dataset/versions/latest")
        .with_status(200)
        .with_header("content-type", "application/json")
        .with_body(
            r#"{
                "namespace": "_",
                "name": "test_dataset",
                "revision": "0.1.0-LTcyNjgzMjc1NA",
                "manifest_hash": "a7ffc6f8bf1ed76651c14756a061d662f580ff4de43b49fa82d80a4b80f8434a",
                "kind": "evm-rpc"
            }"#,
        )
        .expect_at_least(1)
        .create_async()
        .await;

    // Spawn version polling task with very short interval
    let admin_api_addr = server.url();
    let poll_interval_secs = 1; // 1 second for faster test

    let poll_handle = tokio::spawn(async move {
        crate::version_polling::version_poll_task(
            admin_api_addr.clone(),
            dataset_namespace.clone(),
            dataset_name.clone(),
            initial_version,
            poll_interval_secs,
            tx,
        )
        .await;
    });

    // Wait for first poll (should not send notification - version unchanged)
    tokio::time::sleep(tokio::time::Duration::from_millis(1500)).await;

    // Now update the mock to return new version
    let _new_version_mock = server
        .mock("GET", "/datasets/_/test_dataset/versions/latest")
        .with_status(200)
        .with_header("content-type", "application/json")
        .with_body(
            r#"{
                "namespace": "_",
                "name": "test_dataset",
                "revision": "0.2.0-LTcyNjgzMjc1NA",
                "manifest_hash": "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9",
                "kind": "evm-rpc"
            }"#,
        )
        .expect_at_least(1)
        .create_async()
        .await;

    // Verify we received the new version notification
    tokio::time::timeout(tokio::time::Duration::from_secs(3), rx.changed())
        .await
        .expect("Timeout waiting for version notification")
        .expect("Channel closed unexpectedly");

    let new_version = rx.borrow_and_update().clone();
    assert_eq!(new_version.to_string(), "0.2.0-LTcyNjgzMjc1NA");

    // Cleanup - abort task immediately after verification
    poll_handle.abort();
}

/// Test that version polling handles API errors gracefully.
///
/// Polling should continue even when the API returns errors (transient failures).
#[tokio::test]
async fn test_version_polling_handles_api_errors() {
    let mut server = Server::new_async().await;

    let dataset_namespace: Namespace = "_".parse().unwrap();
    let dataset_name: Name = "test_dataset".parse().unwrap();
    let initial_version: Version = "0.1.0-LTcyNjgzMjc1NA".parse().unwrap();

    let (tx, mut rx) = watch::channel::<Version>(initial_version.clone());

    // First poll: API error (500) - should be retried at least once
    let _error_mock = server
        .mock("GET", "/datasets/_/test_dataset/versions/latest")
        .with_status(500)
        .with_body("Internal Server Error")
        .expect_at_least(1)
        .create_async()
        .await;

    let admin_api_addr = server.url();
    let poll_interval_secs = 1;

    let poll_handle = tokio::spawn(async move {
        crate::version_polling::version_poll_task(
            admin_api_addr.clone(),
            dataset_namespace.clone(),
            dataset_name.clone(),
            initial_version,
            poll_interval_secs,
            tx,
        )
        .await;
    });

    // Wait for first poll (error - should log but continue)
    tokio::time::sleep(tokio::time::Duration::from_millis(1500)).await;

    // Now create success mock that will return new version
    let _success_mock = server
        .mock("GET", "/datasets/_/test_dataset/versions/latest")
        .with_status(200)
        .with_header("content-type", "application/json")
        .with_body(
            r#"{
                "namespace": "_",
                "name": "test_dataset",
                "revision": "0.2.0-LTcyNjgzMjc1NA",
                "manifest_hash": "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9",
                "kind": "evm-rpc"
            }"#,
        )
        .expect_at_least(1)
        .create_async()
        .await;

    // Verify we still received the new version notification after error
    tokio::time::timeout(tokio::time::Duration::from_secs(3), rx.changed())
        .await
        .expect("Timeout waiting for version notification")
        .expect("Channel closed unexpectedly");

    let new_version = rx.borrow_and_update().clone();
    assert_eq!(new_version.to_string(), "0.2.0-LTcyNjgzMjc1NA");

    // Cleanup - abort task immediately after verification
    poll_handle.abort();
}

/// Test that fetch_latest_version returns the latest version from the endpoint.
///
/// The versions/latest endpoint returns the latest version directly.
#[tokio::test]
async fn test_fetch_latest_version_returns_first_version() {
    let mut server = Server::new_async().await;

    let dataset_namespace: Namespace = "_".parse().unwrap();
    let dataset_name: Name = "test_dataset".parse().unwrap();

    // Mock the versions/latest endpoint
    let versions_mock = server
        .mock("GET", "/datasets/_/test_dataset/versions/latest")
        .with_status(200)
        .with_header("content-type", "application/json")
        .with_body(
            r#"{
                "namespace": "_",
                "name": "test_dataset",
                "revision": "0.3.0-LTcyNjgzMjc1NA",
                "manifest_hash": "c3ab8ff13720e8ad9047dd39466b3c8974e592c2fa383d4a3960714caef0c4f2",
                "kind": "evm-rpc"
            }"#,
        )
        .expect(1)
        .create_async()
        .await;

    let result =
        crate::manifest::fetch_latest_version(&server.url(), &dataset_namespace, &dataset_name)
            .await;

    assert!(result.is_ok());
    let version = result.unwrap();

    // Should return the latest version
    assert_eq!(version.to_string(), "0.3.0-LTcyNjgzMjc1NA");

    versions_mock.assert_async().await;
}

/// Test that fetch_latest_version handles 404 when no versions exist.
#[tokio::test]
async fn test_fetch_latest_version_handles_empty_list() {
    let mut server = Server::new_async().await;

    let dataset_namespace: Namespace = "_".parse().unwrap();
    let dataset_name: Name = "test_dataset".parse().unwrap();

    // Mock 404 response when no versions exist
    let versions_mock = server
        .mock("GET", "/datasets/_/test_dataset/versions/latest")
        .with_status(404)
        .expect(1)
        .create_async()
        .await;

    let result =
        crate::manifest::fetch_latest_version(&server.url(), &dataset_namespace, &dataset_name)
            .await;

    // Should return error for 404 (no versions available)
    assert!(result.is_err());
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("No versions available")
    );

    versions_mock.assert_async().await;
}
