//! Integration tests for the Admin API dataset manifest endpoint.

use datasets_common::{name::Name, version_tag::VersionTag};
use reqwest::StatusCode;
use serde_json::Value as JsonValue;

use crate::testlib::ctx::TestCtxBuilder;

#[tokio::test]
async fn get_dataset_version_manifest_with_valid_dataset_succeeds() {
    //* Given
    let ctx = TestCtx::setup("get_dataset_version_manifest_with_valid_dataset").await;

    //* When
    let resp = ctx.get_manifest("eth_rpc", "0.0.0").await;

    //* Then
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "manifest retrieval should succeed for valid dataset"
    );

    let manifest: JsonValue = resp
        .json()
        .await
        .expect("failed to parse manifest response JSON");

    // Verify manifest structure
    assert!(
        manifest.is_object(),
        "manifest response should be a JSON object"
    );

    // Verify key fields exist
    assert_eq!(
        manifest.get("name").and_then(|v| v.as_str()),
        Some("eth_rpc"),
        "manifest should contain correct dataset name"
    );
    assert_eq!(
        manifest.get("kind").and_then(|v| v.as_str()),
        Some("evm-rpc"),
        "manifest should contain correct kind"
    );
    assert_eq!(
        manifest.get("network").and_then(|v| v.as_str()),
        Some("mainnet"),
        "manifest should contain correct network"
    );
    assert!(
        manifest.get("schema").is_some(),
        "manifest should contain schema field"
    );

    // Verify schema contains expected tables
    let schema = manifest
        .get("schema")
        .expect("schema field should exist")
        .as_object()
        .expect("schema should be an object");

    assert!(
        schema.contains_key("blocks"),
        "schema should contain blocks table"
    );
    assert!(
        schema.contains_key("logs"),
        "schema should contain logs table"
    );
    assert!(
        schema.contains_key("transactions"),
        "schema should contain transactions table"
    );
}

#[tokio::test]
async fn get_dataset_version_manifest_with_non_existent_dataset_returns_not_found() {
    //* Given
    let ctx = TestCtx::setup("get_dataset_version_manifest_with_non_existent_dataset").await;

    //* When
    let resp = ctx.get_manifest("non_existent_dataset", "1.0.0").await;

    //* Then
    assert_eq!(
        resp.status(),
        StatusCode::NOT_FOUND,
        "manifest retrieval should fail with 404 for non-existent dataset"
    );

    let error_response: ErrorResponse = resp
        .json()
        .await
        .expect("failed to parse error response JSON");

    assert_eq!(
        error_response.error_code, "MANIFEST_NOT_FOUND",
        "should return MANIFEST_NOT_FOUND error code"
    );
    assert!(
        error_response
            .error_message
            .contains("non_existent_dataset"),
        "error message should include dataset name"
    );
    assert!(
        error_response.error_message.contains("1.0.0"),
        "error message should include version"
    );
}

#[tokio::test]
async fn get_dataset_version_manifest_with_invalid_name_returns_bad_request() {
    //* Given
    let ctx = TestCtx::setup("get_dataset_version_manifest_with_invalid_name").await;

    //* When
    let resp = ctx.get_manifest_raw("invalid dataset name", "1.0.0").await;

    //* Then
    assert_eq!(
        resp.status(),
        StatusCode::BAD_REQUEST,
        "manifest retrieval should fail with 400 for invalid dataset name"
    );

    let error_response: ErrorResponse = resp
        .json()
        .await
        .expect("failed to parse error response JSON");

    assert_eq!(
        error_response.error_code, "INVALID_SELECTOR",
        "should return INVALID_SELECTOR error code"
    );
    assert!(
        error_response
            .error_message
            .contains("invalid dataset selector"),
        "error message should indicate invalid selector"
    );
}

#[tokio::test]
async fn get_dataset_version_manifest_with_invalid_version_returns_bad_request() {
    //* Given
    let ctx = TestCtx::setup("get_dataset_version_manifest_with_invalid_version").await;

    //* When
    let resp = ctx.get_manifest_raw("eth_rpc", "not_a_version").await;

    //* Then
    assert_eq!(
        resp.status(),
        StatusCode::BAD_REQUEST,
        "manifest retrieval should fail with 400 for invalid version"
    );

    let error_response: ErrorResponse = resp
        .json()
        .await
        .expect("failed to parse error response JSON");

    assert_eq!(
        error_response.error_code, "INVALID_SELECTOR",
        "should return INVALID_SELECTOR error code"
    );
    assert!(
        error_response
            .error_message
            .contains("invalid dataset selector"),
        "error message should indicate invalid selector"
    );
}

#[tokio::test]
async fn get_dataset_version_manifest_with_invalid_name_and_version_returns_bad_request() {
    //* Given
    let ctx = TestCtx::setup("get_dataset_version_manifest_with_invalid_name_and_version").await;

    //* When
    let resp = ctx
        .get_manifest_raw("invalid dataset name", "not_a_version")
        .await;

    //* Then
    assert_eq!(
        resp.status(),
        StatusCode::BAD_REQUEST,
        "manifest retrieval should fail with 400 for invalid name and version"
    );

    let error_response: ErrorResponse = resp
        .json()
        .await
        .expect("failed to parse error response JSON");

    assert_eq!(
        error_response.error_code, "INVALID_SELECTOR",
        "should return INVALID_SELECTOR error code"
    );
    assert!(
        error_response
            .error_message
            .contains("invalid dataset selector"),
        "error message should indicate invalid selector"
    );
}

struct TestCtx {
    _ctx: crate::testlib::ctx::TestCtx,
    client: reqwest::Client,
    admin_api_url: String,
}

impl TestCtx {
    async fn setup(test_name: &str) -> Self {
        let ctx = TestCtxBuilder::new(test_name)
            .with_provider_config("rpc_eth_mainnet")
            .with_dataset_manifest("eth_rpc")
            .with_dataset_snapshot("eth_rpc")
            .build()
            .await
            .expect("failed to build test context");

        let client = reqwest::Client::new();
        let admin_api_url = ctx.daemon_controller().admin_api_url();

        Self {
            _ctx: ctx,
            client,
            admin_api_url,
        }
    }

    async fn get_manifest(&self, name: &str, version: &str) -> reqwest::Response {
        let name = name.parse::<Name>().expect("valid dataset name");
        let version = version.parse::<VersionTag>().expect("valid version");

        self.client
            .get(&format!(
                "{}/datasets/{}/versions/{}/manifest",
                &self.admin_api_url, name, version
            ))
            .send()
            .await
            .expect("failed to send manifest request")
    }

    async fn get_manifest_raw(&self, name: &str, version: &str) -> reqwest::Response {
        self.client
            .get(&format!(
                "{}/datasets/{}/versions/{}/manifest",
                &self.admin_api_url, name, version
            ))
            .send()
            .await
            .expect("failed to send manifest request")
    }
}

#[derive(Debug, serde::Deserialize)]
struct ErrorResponse {
    error_code: String,
    error_message: String,
}
