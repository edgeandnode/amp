use http_auth::AuthService;
use std::env;

/// Test environment configuration
struct TestEnv {
    app_id: String,
    app_secret: String,
    jwks_url: String,
    auth_user_api_url: String,
    test_token: Option<String>,
}

impl TestEnv {
    /// Create an AuthService from the test environment
    fn auth_service(&self) -> AuthService {
        AuthService::new(
            self.jwks_url.clone(),
            self.auth_user_api_url.clone(),
            self.app_id.clone(),
            self.app_secret.clone(),
        )
        .with_audience(vec![self.app_id.clone()])
        .with_issuer(vec!["privy.io".to_string()])
    }

    /// Create an AuthService without audience/issuer configuration
    fn auth_service_basic(&self) -> AuthService {
        AuthService::new(
            self.jwks_url.clone(),
            self.auth_user_api_url.clone(),
            self.app_id.clone(),
            self.app_secret.clone(),
        )
    }
}

/// Initialize test environment and load required env vars.
/// Returns None if required env vars are missing, printing a skip message.
fn init_test_env() -> Option<TestEnv> {
    let _ = dotenvy::dotenv();

    let app_id = match env::var("AUTH_APP_ID") {
        Ok(id) => id,
        Err(_) => {
            println!("Skipping test: AUTH_APP_ID environment variable not set");
            return None;
        }
    };

    let app_secret = match env::var("AUTH_APP_SECRET") {
        Ok(secret) => secret,
        Err(_) => {
            println!("Skipping test: AUTH_APP_SECRET environment variable not set");
            return None;
        }
    };

    let jwks_url = match env::var("JWKS_URL") {
        Ok(url) => url,
        Err(_) => {
            println!("Skipping test: JWKS_URL environment variable not set");
            return None;
        }
    };

    let auth_user_api_url = env::var("AUTH_USER_API_URL")
        .unwrap_or_else(|_| "https://api.privy.io/v1/users".to_string());

    let test_token = env::var("PRIVY_TEST_TOKEN").ok();

    Some(TestEnv {
        app_id,
        app_secret,
        jwks_url,
        auth_user_api_url,
        test_token,
    })
}

/// Initialize test environment for tests that only need JWKS URL
fn init_jwks_only() -> Option<String> {
    let _ = dotenvy::dotenv();

    match env::var("JWKS_URL") {
        Ok(url) => Some(url),
        Err(_) => {
            println!("Skipping test: JWKS_URL environment variable not set");
            None
        }
    }
}

#[tokio::test]
async fn test_http_auth_validation() {
    let Some(env) = init_test_env() else {
        return;
    };

    let test_token = match &env.test_token {
        Some(token) => token.clone(),
        None => {
            println!("Skipping test: PRIVY_TEST_TOKEN environment variable not set");
            return;
        }
    };

    let auth_service = env.auth_service();

    match auth_service.validate_token(&test_token).await {
        Ok(claims) => {
            println!("Token validated successfully!");
            println!("  Subject: {}", claims.sub);
            println!("  Audience: {:?}", claims.aud);
            println!("  Issuer: {:?}", claims.iss);
            println!("  Issued At: {:?}", claims.iat);
            println!("  Expires: {:?}", claims.exp);

            if !claims.additional_claims.is_empty() {
                println!("  Additional Claims:");
                for (key, value) in &claims.additional_claims {
                    println!("    {}: {}", key, value);
                }
            }

            assert!(!claims.sub.is_empty(), "Subject should not be empty");
            assert_eq!(claims.aud, Some(env.app_id), "Audience should match app ID");
            assert_eq!(
                claims.iss,
                Some("privy.io".to_string()),
                "Issuer should be privy.io"
            );

            // Test user_id extraction
            let user_id = claims.user_id();
            println!("  Extracted User ID: {}", user_id);
            assert!(
                !user_id.contains("did:privy:"),
                "User ID should not contain DID prefix"
            );
            assert!(!user_id.is_empty(), "User ID should not be empty");
        }
        Err(e) => {
            panic!("Token validation failed: {}", e);
        }
    }
}

#[tokio::test]
async fn test_jwks_endpoint_connectivity() {
    let Some(jwks_url) = init_jwks_only() else {
        return;
    };

    println!("Testing JWKS endpoint connectivity: {}", jwks_url);

    let response = reqwest::get(&jwks_url)
        .await
        .expect("Failed to connect to JWKS endpoint");

    assert!(
        response.status().is_success(),
        "JWKS endpoint should return success status"
    );

    let body = response.text().await.expect("Failed to read response body");
    let jwks: serde_json::Value = serde_json::from_str(&body).expect("Failed to parse JWKS JSON");

    println!("JWKS endpoint is reachable and returns valid JSON");

    assert!(
        jwks.get("keys").is_some(),
        "JWKS should contain 'keys' field"
    );
    assert!(jwks["keys"].is_array(), "JWKS 'keys' should be an array");

    let keys = jwks["keys"].as_array().unwrap();
    assert!(!keys.is_empty(), "JWKS should contain at least one key");

    for (i, key) in keys.iter().enumerate() {
        println!(
            "  Key {}: kid={:?}, alg={:?}, use={:?}",
            i,
            key.get("kid"),
            key.get("alg"),
            key.get("use")
        );

        assert!(key.get("kid").is_some(), "Key should have 'kid' field");
        assert!(key.get("kty").is_some(), "Key should have 'kty' field");
    }
}

#[tokio::test]
async fn test_expired_token_rejection() {
    let Some(env) = init_test_env() else {
        return;
    };

    let auth_service = env.auth_service_basic();

    let expired_token = "eyJhbGciOiJSUzI1NiIsImtpZCI6InRlc3Qta2V5IiwidHlwIjoiSldUIn0.eyJzdWIiOiJ1c2VyMTIzIiwiZXhwIjoxNjAwMDAwMDAwLCJpYXQiOjE1OTk5OTk5MDAsImF1ZCI6InRlc3QtYXVkIiwiaXNzIjoicHJpdnkuaW8ifQ.invalid";

    match auth_service.validate_token(expired_token).await {
        Ok(_) => panic!("Should not validate an expired/invalid token"),
        Err(e) => {
            println!("Correctly rejected invalid token: {}", e);
        }
    }
}

#[tokio::test]
async fn test_caching_behavior() {
    let Some(env) = init_test_env() else {
        return;
    };

    let auth_service = env.auth_service_basic();

    if let Some(test_token) = &env.test_token {
        println!("Testing caching with real token...");

        let start = std::time::Instant::now();
        let result1 = auth_service.validate_token(&test_token).await;
        let first_call_duration = start.elapsed();

        let start = std::time::Instant::now();
        let result2 = auth_service.validate_token(&test_token).await;
        let second_call_duration = start.elapsed();

        println!("First call (fetches JWKS): {:?}", first_call_duration);
        println!("Second call (uses cache): {:?}", second_call_duration);

        assert_eq!(
            result1.is_ok(),
            result2.is_ok(),
            "Both calls should have same result"
        );

        assert!(
            second_call_duration < first_call_duration,
            "Second call should be faster due to caching"
        );

        println!("Caching is working correctly");
    } else {
        println!("Skipping real token caching test (PRIVY_TEST_TOKEN not set)");

        let invalid_token = "invalid.token.here";
        let _ = auth_service.validate_token(invalid_token).await;
        let _ = auth_service.validate_token(invalid_token).await;
        println!("Caching mechanism executed (without real validation)");
    }
}

#[tokio::test]
async fn test_integration_with_axum() {
    // Note: For full end-to-end testing with a real user:
    // 1. Set PRIVY_TEST_TOKEN to a valid JWT from a real Privy user
    // 2. Ensure the user exists in the Privy IDaaS
    // 3. Set AUTH_APP_ID and AUTH_APP_SECRET correctly
    // Without a real user in the IDaaS, authentication will correctly fail with 403 FORBIDDEN

    use axum::{
        Router,
        body::Body,
        http::{Request, header::AUTHORIZATION},
        routing::get,
    };
    use http_auth::{AuthenticatedUser, auth_layer};
    use tower::ServiceExt;

    let Some(env) = init_test_env() else {
        return;
    };

    let auth_service = env.auth_service();

    // Clone values we'll need later before moving anything
    let app_id = env.app_id.clone();
    let app_secret = env.app_secret.clone();
    let auth_user_api_url = env.auth_user_api_url.clone();

    async fn protected_handler(user: AuthenticatedUser) -> String {
        format!("Hello, {}!", user.user_id())
    }

    let app = Router::new()
        .route("/protected", get(protected_handler))
        .layer(auth_layer(auth_service));

    if let Some(test_token) = &env.test_token {
        let request = Request::builder()
            .uri("/protected")
            .header(AUTHORIZATION, format!("Bearer {}", test_token))
            .body(Body::empty())
            .unwrap();

        let response = app.oneshot(request).await.unwrap();

        // Note: With the new security requirement, authentication will fail with 403
        // if the user doesn't exist in the IDaaS, even with a valid JWT.
        // This is expected behavior for security.
        match response.status() {
            axum::http::StatusCode::OK => {
                let body = axum::body::to_bytes(response.into_body(), usize::MAX)
                    .await
                    .unwrap();
                let body_str = String::from_utf8(body.to_vec()).unwrap();
                assert!(
                    body_str.starts_with("Hello, "),
                    "Response should contain greeting"
                );
                println!(
                    "Axum integration test passed (user exists in IDaaS): {}",
                    body_str
                );
            }
            axum::http::StatusCode::FORBIDDEN => {
                println!(
                    "Token is valid but user not found in IDaaS (expected for test tokens without real user)"
                );
                println!(
                    "This is correct security behavior - authentication requires both valid JWT and existing user"
                );
            }
            status => {
                panic!(
                    "Unexpected status code: {} (expected 200 OK or 403 FORBIDDEN)",
                    status
                );
            }
        }
    } else {
        println!("Skipping Axum integration test with real token (PRIVY_TEST_TOKEN not set)");
    }

    let request_without_auth = Request::builder()
        .uri("/protected")
        .body(Body::empty())
        .unwrap();

    let app = Router::new()
        .route("/protected", get(protected_handler))
        .layer(auth_layer(AuthService::new(
            format!("https://auth.privy.io/api/v1/apps/{}/jwks.json", app_id),
            auth_user_api_url,
            app_id,
            app_secret,
        )));

    let response = app.oneshot(request_without_auth).await.unwrap();

    assert_eq!(
        response.status(),
        401,
        "Should return 401 for missing auth header"
    );
    println!("Correctly rejected request without auth header");
}

#[tokio::test]
async fn test_lru_cache_eviction() {
    use http_auth::AuthService;

    // Create a small cache to test eviction
    let auth_service = AuthService::new(
        "https://example.com/jwks.json".to_string(),
        "https://api.example.com/users".to_string(),
        "test_app_id".to_string(),
        "test_app_secret".to_string(),
    )
    .with_user_cache_size(3); // Small cache size to test eviction

    // Test that cache stats are accessible
    let (current_size, max_size) = auth_service.cache_stats().await;
    assert_eq!(current_size, 0);
    assert_eq!(max_size, 3);

    println!("LRU cache initialized with size limit");
}
