//! Integration tests for user fetching from IDaaS API

use auth_http::AuthService;
use chrono::Duration;
use jsonwebtoken::Validation;
use mockito::Server;

#[tokio::test]
async fn fetch_user_success() {
    //* Given
    let mut server = Server::new_async().await;
    let app_id = "test-app-id";
    let app_secret = "test-secret";

    // Mock successful user fetch
    let user_mock = server
        .mock("GET", "/v1/users/test_user_123")
        .match_header("privy-app-id", app_id)
        .match_header("authorization", mockito::Matcher::Any)
        .with_status(200)
        .with_header("content-type", "application/json")
        .with_body(
            r#"{
                "id": "test_user_123",
                "created_at": 1731974895,
                "has_accepted_terms": true,
                "is_guest": false,
                "linked_accounts": [
                    {
                        "type": "smart_wallet",
                        "address": "0x1234567890123456789012345678901234567890",
                        "smart_wallet_type": "safe",
                        "verified_at": 1731974895
                    }
                ],
                "mfa_methods": []
            }"#,
        )
        .expect(1)
        .create_async()
        .await;

    //* When
    let auth_service = AuthService::with_config(
        format!("{}/jwks.json", server.url()),
        format!("{}/v1/users", server.url()),
        app_id.to_string(),
        app_secret.to_string(),
        Duration::hours(1),
        Validation::default(),
    );

    let user = auth_service
        .maybe_fetch_auth_user("test_user_123")
        .await
        .expect("Failed to fetch user");

    //* Then
    user_mock.assert_async().await;
    assert!(user.is_some());
    let user_data = user.unwrap();
    assert_eq!(user_data.id, "test_user_123");
    assert_eq!(
        user_data.ethereum_address(),
        Some("0x1234567890123456789012345678901234567890".to_string())
    );
}

#[tokio::test]
async fn fetch_user_not_found() {
    //* Given
    let mut server = Server::new_async().await;

    let user_mock = server
        .mock("GET", "/v1/users/nonexistent_user")
        .with_status(404)
        .expect(1)
        .create_async()
        .await;

    //* When
    let auth_service = AuthService::with_config(
        format!("{}/jwks.json", server.url()),
        format!("{}/v1/users", server.url()),
        "test-app-id".to_string(),
        "test-secret".to_string(),
        Duration::hours(1),
        Validation::default(),
    );

    let user = auth_service
        .maybe_fetch_auth_user("nonexistent_user")
        .await
        .expect("Should not error on 404");

    //* Then
    user_mock.assert_async().await;
    assert!(user.is_none(), "User should be None for 404");
}

#[tokio::test]
async fn fetch_user_server_error() {
    //* Given
    let mut server = Server::new_async().await;

    let user_mock = server
        .mock("GET", "/v1/users/test_user")
        .with_status(500)
        .with_body("Internal Server Error")
        .expect(1)
        .create_async()
        .await;

    //* When
    let auth_service = AuthService::with_config(
        format!("{}/jwks.json", server.url()),
        format!("{}/v1/users", server.url()),
        "test-app-id".to_string(),
        "test-secret".to_string(),
        Duration::hours(1),
        Validation::default(),
    );

    let result = auth_service.maybe_fetch_auth_user("test_user").await;

    //* Then
    user_mock.assert_async().await;
    assert!(result.is_err(), "Should error on 5xx");
}

#[tokio::test]
async fn fetch_user_with_multiple_wallets() {
    //* Given
    let mut server = Server::new_async().await;

    let user_mock = server
        .mock("GET", "/v1/users/test_user")
        .with_status(200)
        .with_header("content-type", "application/json")
        .with_body(
            r#"{
                "id": "test_user",
                "created_at": 1731974895,
                "has_accepted_terms": true,
                "is_guest": false,
                "linked_accounts": [
                    {
                        "type": "wallet",
                        "address": "0x2222222222222222222222222222222222222222",
                        "chain_type": "ethereum",
                        "wallet_client": "metamask",
                        "verified_at": 1731974895
                    },
                    {
                        "type": "smart_wallet",
                        "address": "0x1111111111111111111111111111111111111111",
                        "smart_wallet_type": "safe",
                        "verified_at": 1731974895
                    },
                    {
                        "type": "wallet",
                        "address": "0x3333333333333333333333333333333333333333",
                        "chain_type": "ethereum",
                        "wallet_client": "privy",
                        "connector_type": "embedded",
                        "verified_at": 1731974895
                    }
                ],
                "mfa_methods": []
            }"#,
        )
        .expect(1)
        .create_async()
        .await;

    //* When
    let auth_service = AuthService::with_config(
        format!("{}/jwks.json", server.url()),
        format!("{}/v1/users", server.url()),
        "test-app-id".to_string(),
        "test-secret".to_string(),
        Duration::hours(1),
        Validation::default(),
    );

    let user = auth_service
        .maybe_fetch_auth_user("test_user")
        .await
        .expect("Failed to fetch user")
        .expect("User should exist");

    //* Then
    user_mock.assert_async().await;

    // Should return smart wallet (highest priority)
    assert_eq!(
        user.ethereum_address(),
        Some("0x1111111111111111111111111111111111111111".to_string())
    );

    // Should return all ethereum addresses
    let all_addresses = user.all_ethereum_addresses();
    assert_eq!(all_addresses.len(), 3);
    assert!(all_addresses.contains(&"0x1111111111111111111111111111111111111111".to_string()));
    assert!(all_addresses.contains(&"0x2222222222222222222222222222222222222222".to_string()));
    assert!(all_addresses.contains(&"0x3333333333333333333333333333333333333333".to_string()));
}
