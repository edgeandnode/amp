//! Integration tests for the Admin API schema resolution endpoint.

use reqwest::StatusCode;
use serde_json::Value as JsonValue;
use tests::testlib::ctx::TestCtxBuilder;

#[tokio::test]
async fn resolve_schema_with_valid_table_succeeds() {
    //* Given
    let ctx = TestCtx::setup("resolve_schema_with_valid_table").await;
    let sql_query = indoc::indoc! {"
        SELECT
            block_num,
            gas_limit,
            gas_used,
            nonce,
            miner,
            hash,
            parent_hash
        FROM eth_firehose.blocks
    "};

    //* When
    let resp = ctx.send_shema_request(sql_query, false).await;

    //* Then
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "schema resolution should succeed for valid table reference"
    );
    let response: SchemaResponse = resp
        .json()
        .await
        .expect("failed to parse schema response JSON");

    assert!(
        !response.schema.arrow.fields.is_empty(),
        "resolved schema should contain at least one field"
    );
    assert!(
        response.networks.contains(&"mainnet".to_string()),
        "resolved schema should include mainnet in referenced networks"
    );
}

#[tokio::test]
async fn resolve_schema_with_filtered_query_succeeds() {
    //* Given
    let ctx = TestCtx::setup("resolve_schema_with_filtered_query").await;
    let sql_query = indoc::indoc! {"
        SELECT
            block_num,
            gas_limit,
            hash,
            parent_hash
        FROM eth_firehose.blocks
        WHERE block_num > 1000000
    "};

    //* When
    let resp = ctx.send_shema_request(sql_query, false).await;

    //* Then
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "schema resolution should succeed for filtered query"
    );
    let response: SchemaResponse = resp
        .json()
        .await
        .expect("failed to parse schema response JSON");

    assert!(
        !response.schema.arrow.fields.is_empty(),
        "resolved schema should contain at least one field"
    );
    assert!(
        response.networks.contains(&"mainnet".to_string()),
        "resolved schema should include mainnet in referenced networks"
    );
}

#[tokio::test]
async fn resolve_schema_with_non_existent_dataset_fails() {
    //* Given
    let ctx = TestCtx::setup("resolve_schema_with_non_existent_dataset").await;
    let sql_query = indoc::indoc! {"
        SELECT
            block_num,
            gas_limit
        FROM non_existent_dataset.blocks
    "};

    //* When
    let resp = ctx.send_shema_request(sql_query, false).await;

    //* Then
    assert_eq!(
        resp.status(),
        StatusCode::INTERNAL_SERVER_ERROR,
        "schema resolution should fail for non-existent dataset"
    );

    let response: ErrorResponse = resp
        .json()
        .await
        .expect("failed to parse error response JSON");

    assert_eq!(
        response.error_code, "DATASET_STORE_ERROR",
        "should return DATASET_STORE_ERROR for non-existent dataset"
    );
}

#[tokio::test]
async fn resolve_schema_with_non_existent_table_fails() {
    //* Given
    let ctx = TestCtx::setup("resolve_schema_with_non_existent_table").await;
    let sql_query = indoc::indoc! {"
        SELECT
            block_num,
            gas_limit
        FROM eth_firehose.non_existent_table
    "};

    //* When
    let resp = ctx.send_shema_request(sql_query, false).await;

    //* Then
    assert_eq!(
        resp.status(),
        StatusCode::INTERNAL_SERVER_ERROR,
        "schema resolution should fail for non-existent table"
    );

    let response: ErrorResponse = resp
        .json()
        .await
        .expect("failed to parse error response JSON");

    assert_eq!(
        response.error_code, "PLANNING_ERROR",
        "should return PLANNING_ERROR for non-existent table in valid dataset"
    );
}

struct TestCtx {
    _ctx: tests::testlib::ctx::TestCtx,
    client: reqwest::Client,
    schema_api_url: String,
}

impl TestCtx {
    async fn setup(test_name: &str) -> Self {
        let ctx = TestCtxBuilder::new(test_name)
            .with_dataset_manifest("eth_firehose")
            .with_dataset_snapshot("eth_firehose")
            .with_provider_config("firehose_eth_mainnet")
            .build()
            .await
            .expect("failed to build test context");

        let client = reqwest::Client::new();
        let admin_api_url = ctx.daemon_controller().admin_api_url();

        let test_ctx = Self {
            _ctx: ctx,
            client,
            schema_api_url: format!("{}/schema", admin_api_url),
        };

        test_ctx
    }

    async fn send_shema_request(&self, sql_query: &str, is_sql_dataset: bool) -> reqwest::Response {
        let request_payload = serde_json::json!({
            "sql_query": sql_query,
            "is_sql_dataset": is_sql_dataset
        });

        self.client
            .post(&self.schema_api_url)
            .json(&request_payload)
            .send()
            .await
            .expect("failed to send schema validation request")
    }
}

#[derive(Debug, serde::Deserialize)]
struct SchemaResponse {
    schema: TableSchema,
    networks: Vec<String>,
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
}
