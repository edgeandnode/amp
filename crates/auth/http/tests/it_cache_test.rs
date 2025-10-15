//! Integration tests for caching behavior

use auth_http::AuthService;
use chrono::Duration;
use jsonwebtoken::Validation;
use mockito::Server;

#[tokio::test]
async fn user_cache_hit() {
    //* Given
    let mut server = Server::new_async().await;

    // Mock should only be called once despite multiple requests
    let user_mock = server
        .mock("GET", "/v1/users/cached_user")
        .with_status(200)
        .with_header("content-type", "application/json")
        .with_body(
            r#"{
                "id": "cached_user",
                "created_at": 1731974895,
                "has_accepted_terms": true,
                "is_guest": false,
                "linked_accounts": [],
                "mfa_methods": []
            }"#,
        )
        .expect(1) // Should only be called once
        .create_async()
        .await;

    let auth_service = AuthService::with_config(
        format!("{}/jwks.json", server.url()),
        format!("{}/v1/users", server.url()),
        "test-app-id".to_string(),
        "test-secret".to_string(),
        Duration::hours(1),
        Validation::default(),
    );

    //* When - Make two requests for the same user
    let user1 = auth_service
        .maybe_fetch_auth_user("cached_user")
        .await
        .expect("First fetch failed");

    let user2 = auth_service
        .maybe_fetch_auth_user("cached_user")
        .await
        .expect("Second fetch failed");

    //* Then
    user_mock.assert_async().await; // Verifies only 1 call
    assert!(user1.is_some());
    assert!(user2.is_some());
    assert_eq!(user1.unwrap().id, user2.unwrap().id);
}

#[tokio::test]
async fn user_cache_404() {
    //* Given
    let mut server = Server::new_async().await;

    // 404 should also be cached to prevent repeated lookups
    let user_mock = server
        .mock("GET", "/v1/users/nonexistent")
        .with_status(404)
        .expect(1) // Should only be called once
        .create_async()
        .await;

    let auth_service = AuthService::with_config(
        format!("{}/jwks.json", server.url()),
        format!("{}/v1/users", server.url()),
        "test-app-id".to_string(),
        "test-secret".to_string(),
        Duration::hours(1),
        Validation::default(),
    );

    //* When - Request same non-existent user twice
    let user1 = auth_service
        .maybe_fetch_auth_user("nonexistent")
        .await
        .expect("First fetch should not error");

    let user2 = auth_service
        .maybe_fetch_auth_user("nonexistent")
        .await
        .expect("Second fetch should not error");

    //* Then
    user_mock.assert_async().await; // Only called once
    assert!(user1.is_none());
    assert!(user2.is_none());
}

#[tokio::test]
async fn user_cache_expiration() {
    //* Given
    let mut server = Server::new_async().await;

    // Mock should be called twice after cache expires
    let user_mock = server
        .mock("GET", "/v1/users/expiring_user")
        .with_status(200)
        .with_header("content-type", "application/json")
        .with_body(
            r#"{
                "id": "expiring_user",
                "created_at": 1731974895,
                "has_accepted_terms": true,
                "is_guest": false,
                "linked_accounts": [],
                "mfa_methods": []
            }"#,
        )
        .expect(2) // Should be called twice
        .create_async()
        .await;

    // Use very short cache duration for testing
    let auth_service = AuthService::with_config(
        format!("{}/jwks.json", server.url()),
        format!("{}/v1/users", server.url()),
        "test-app-id".to_string(),
        "test-secret".to_string(),
        Duration::milliseconds(100), // 100ms cache
        Validation::default(),
    );

    //* When
    let user1 = auth_service
        .maybe_fetch_auth_user("expiring_user")
        .await
        .expect("First fetch failed");

    // Wait for cache to expire
    tokio::time::sleep(tokio::time::Duration::from_millis(150)).await;

    let user2 = auth_service
        .maybe_fetch_auth_user("expiring_user")
        .await
        .expect("Second fetch failed");

    //* Then
    user_mock.assert_async().await; // Called twice
    assert!(user1.is_some());
    assert!(user2.is_some());
}

#[tokio::test]
async fn cache_stats() {
    //* Given
    let mut server = Server::new_async().await;

    server
        .mock("GET", "/v1/users/user1")
        .with_status(200)
        .with_header("content-type", "application/json")
        .with_body(
            r#"{
                "id": "user1",
                "created_at": 1731974895,
                "has_accepted_terms": true,
                "is_guest": false,
                "linked_accounts": [],
                "mfa_methods": []
            }"#,
        )
        .create_async()
        .await;

    server
        .mock("GET", "/v1/users/user2")
        .with_status(200)
        .with_header("content-type", "application/json")
        .with_body(
            r#"{
                "id": "user2",
                "created_at": 1731974895,
                "has_accepted_terms": true,
                "is_guest": false,
                "linked_accounts": [],
                "mfa_methods": []
            }"#,
        )
        .create_async()
        .await;

    let auth_service = AuthService::with_config(
        format!("{}/jwks.json", server.url()),
        format!("{}/v1/users", server.url()),
        "test-app-id".to_string(),
        "test-secret".to_string(),
        Duration::hours(1),
        Validation::default(),
    )
    .with_user_cache_size(100);

    //* When
    let (initial_size, capacity) = auth_service.cache_stats().await;
    assert_eq!(initial_size, 0);
    assert_eq!(capacity, 100);

    // Add two users to cache
    auth_service
        .maybe_fetch_auth_user("user1")
        .await
        .expect("Fetch failed");
    auth_service
        .maybe_fetch_auth_user("user2")
        .await
        .expect("Fetch failed");

    //* Then
    let (size, capacity) = auth_service.cache_stats().await;
    assert_eq!(size, 2);
    assert_eq!(capacity, 100);
}

#[tokio::test]
async fn clean_expired_cache_entries() {
    //* Given
    let mut server = Server::new_async().await;

    server
        .mock("GET", "/v1/users/user_to_expire")
        .with_status(200)
        .with_header("content-type", "application/json")
        .with_body(
            r#"{
                "id": "user_to_expire",
                "created_at": 1731974895,
                "has_accepted_terms": true,
                "is_guest": false,
                "linked_accounts": [],
                "mfa_methods": []
            }"#,
        )
        .create_async()
        .await;

    // Very short cache duration
    let auth_service = AuthService::with_config(
        format!("{}/jwks.json", server.url()),
        format!("{}/v1/users", server.url()),
        "test-app-id".to_string(),
        "test-secret".to_string(),
        Duration::milliseconds(50),
        Validation::default(),
    );

    //* When
    auth_service
        .maybe_fetch_auth_user("user_to_expire")
        .await
        .expect("Fetch failed");

    let (size_before, _) = auth_service.cache_stats().await;
    assert_eq!(size_before, 1);

    // Wait for expiration
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // Clean expired entries
    auth_service.clean_expired_cache_entries().await;

    //* Then
    let (size_after, _) = auth_service.cache_stats().await;
    assert_eq!(size_after, 0, "Expired entries should be cleaned");
}
