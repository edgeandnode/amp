//! Integration tests for the Admin API dataset schema endpoint.

use datasets_common::{name::Name, version::Version};
use reqwest::StatusCode;
use serde_json::Value as JsonValue;

use crate::testlib::ctx::TestCtxBuilder;

#[tokio::test]
async fn get_dataset_version_schema_with_valid_dataset_succeeds() {
    //* Given
    let ctx = TestCtx::setup("get_dataset_version_schema_with_valid_dataset").await;

    //* When
    let resp = ctx.get_schema("eth_rpc", "0.0.0").await;

    //* Then
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "schema retrieval should succeed for valid dataset"
    );

    let response: DatasetSchemaResponse = resp
        .json()
        .await
        .expect("failed to parse schema response JSON");

    assert_eq!(
        response.name, "eth_rpc",
        "response should contain correct dataset name"
    );
    assert_eq!(
        response.version, "0.0.0",
        "response should contain correct dataset version"
    );
    assert_eq!(
        response.tables.len(),
        3,
        "eth_rpc dataset should have 3 tables"
    );

    // Verify blocks table
    let blocks_table = response
        .tables
        .iter()
        .find(|t| t.name == "blocks")
        .expect("schema should include blocks table");
    assert_eq!(
        blocks_table.network, "mainnet",
        "blocks table should have mainnet network"
    );
    assert!(
        !blocks_table.schema.arrow.fields.is_empty(),
        "blocks table should have at least one field"
    );

    // Verify logs table
    let logs_table = response
        .tables
        .iter()
        .find(|t| t.name == "logs")
        .expect("schema should include logs table");
    assert_eq!(
        logs_table.network, "mainnet",
        "logs table should have mainnet network"
    );
    assert!(
        !logs_table.schema.arrow.fields.is_empty(),
        "logs table should have at least one field"
    );

    // Verify transactions table
    let transactions_table = response
        .tables
        .iter()
        .find(|t| t.name == "transactions")
        .expect("schema should include transactions table");
    assert_eq!(
        transactions_table.network, "mainnet",
        "transactions table should have mainnet network"
    );
    assert!(
        !transactions_table.schema.arrow.fields.is_empty(),
        "transactions table should have at least one field"
    );
}

#[tokio::test]
async fn get_dataset_version_schema_with_non_existent_dataset_returns_not_found() {
    //* Given
    let ctx = TestCtx::setup("get_dataset_version_schema_with_non_existent_dataset").await;

    //* When
    let resp = ctx.get_schema("non_existent_dataset", "1.0.0").await;

    //* Then
    assert_eq!(
        resp.status(),
        StatusCode::NOT_FOUND,
        "schema retrieval should fail with 404 for non-existent dataset"
    );

    let error_response: ErrorResponse = resp
        .json()
        .await
        .expect("failed to parse error response JSON");

    assert_eq!(
        error_response.error_code, "DATASET_NOT_FOUND",
        "should return DATASET_NOT_FOUND error code"
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
async fn get_dataset_version_schema_with_invalid_name_returns_bad_request() {
    //* Given
    let ctx = TestCtx::setup("get_dataset_version_schema_with_invalid_name").await;

    //* When
    let resp = ctx.get_schema_raw("invalid dataset name", "1.0.0").await;

    //* Then
    assert_eq!(
        resp.status(),
        StatusCode::BAD_REQUEST,
        "schema retrieval should fail with 400 for invalid dataset name"
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
async fn get_dataset_version_schema_with_invalid_version_returns_bad_request() {
    //* Given
    let ctx = TestCtx::setup("get_dataset_version_schema_with_invalid_version").await;

    //* When
    let resp = ctx.get_schema_raw("eth_rpc", "not_a_version").await;

    //* Then
    assert_eq!(
        resp.status(),
        StatusCode::BAD_REQUEST,
        "schema retrieval should fail with 400 for invalid version"
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
async fn get_dataset_version_schema_with_invalid_name_and_version_returns_bad_request() {
    //* Given
    let ctx = TestCtx::setup("get_dataset_version_schema_with_invalid_name_and_version").await;

    //* When
    let resp = ctx
        .get_schema_raw("invalid dataset name", "not_a_version")
        .await;

    //* Then
    assert_eq!(
        resp.status(),
        StatusCode::BAD_REQUEST,
        "schema retrieval should fail with 400 for invalid name and version"
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

    async fn get_schema(&self, name: &str, version: &str) -> reqwest::Response {
        let name = name.parse::<Name>().expect("valid dataset name");
        let version = version.parse::<Version>().expect("valid version");

        self.client
            .get(format!(
                "{}/datasets/{}/versions/{}/schema",
                &self.admin_api_url, name, version
            ))
            .send()
            .await
            .expect("failed to send schema request")
    }

    async fn get_schema_raw(&self, name: &str, version: &str) -> reqwest::Response {
        self.client
            .get(format!(
                "{}/datasets/{}/versions/{}/schema",
                &self.admin_api_url, name, version
            ))
            .send()
            .await
            .expect("failed to send schema request")
    }
}

#[derive(Debug, serde::Deserialize)]
struct DatasetSchemaResponse {
    name: String,
    version: String,
    tables: Vec<TableSchemaInfo>,
}

#[derive(Debug, serde::Deserialize)]
struct TableSchemaInfo {
    name: String,
    network: String,
    schema: TableSchema,
}

#[derive(Debug, serde::Deserialize)]
struct TableSchema {
    arrow: ArrowSchema,
}

#[derive(Debug, serde::Deserialize)]
struct ArrowSchema {
    fields: Vec<JsonValue>,
}

#[derive(Debug, serde::Deserialize)]
struct ErrorResponse {
    error_code: String,
    error_message: String,
}
