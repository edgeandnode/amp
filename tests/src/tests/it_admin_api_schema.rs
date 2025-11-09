//! Integration tests for the Admin API schema resolution endpoint.
//!
//! This test suite provides comprehensive coverage of the schema resolution API,
//! testing various dataset reference formats, error conditions, and SQL query scenarios.
//!
//! ## Test Coverage
//!
//! ### Dataset Reference Formats
//! - **Full references**: `namespace/name@revision` format
//!   - Global namespace: `_/eth_firehose@0.0.1`
//!   - Non-global namespace: `edgeandnode/eth_firehose@0.0.1`
//! - **Partial references** with defaults:
//!   - Name only: `eth_firehose` (defaults: namespace=`_`, revision=`latest`)
//!   - Namespace + Name: `edgeandnode/eth_firehose` (defaults: revision=`latest`)
//!   - Name + Version: `eth_firehose@0.0.1` (defaults: namespace=`_`)
//!   - Namespace + Name (no version): `_/eth_firehose` (defaults: revision=`latest`)
//! - **Revision types**:
//!   - Explicit version: `@0.0.1`
//!   - Latest tag: `@latest`
//!   - Dev tag: `@dev`
//!   - Hash reference: `@<hash>`
//!
//! ### Table Qualification Levels
//! - Schema-qualified: `eth_firehose.blocks` ✓
//! - Full reference qualified: `"_/eth_firehose@0.0.1".blocks` ✓
//! - Unqualified: `blocks` → ERROR (BAD_REQUEST)
//! - Catalog-qualified: `catalog.eth_firehose.blocks` → ERROR (BAD_REQUEST)
//!
//! ### Error Conditions
//! - Non-existent table in valid dataset → PLANNING_ERROR
//! - Non-existent dataset → PLANNING_ERROR
//! - Non-existent version → PLANNING_ERROR
//! - Invalid SQL syntax → PLANNING_ERROR
//! - Empty query string → PLANNING_ERROR
//! - Unqualified/catalog-qualified tables → BAD_REQUEST
//!
//! ### Query Complexity
//! - Simple SELECT with specific columns
//! - SELECT with WHERE clause
//! - JOIN within same dataset
//! - JOIN across different datasets
//! - Aggregations (COUNT, AVG, MAX) with GROUP BY

use datasets_common::{hash::Hash, reference::Reference};
use reqwest::StatusCode;
use serde_json::Value as JsonValue;

use crate::testlib::{ctx::TestCtxBuilder, fixtures::Ampctl};

#[tokio::test]
async fn resolve_schema_with_valid_table_succeeds() {
    //* Given
    let ctx = TestCtx::setup(
        "resolve_schema_with_valid_table",
        [("eth_firehose", "_/eth_firehose@0.0.1")], // latest
    )
    .await;
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
    let resp = ctx.send_shema_request(sql_query).await;

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
    let ctx = TestCtx::setup(
        "resolve_schema_with_filtered_query",
        [("eth_firehose", "_/eth_firehose@0.0.1")], // latest
    )
    .await;
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
    let resp = ctx.send_shema_request(sql_query).await;

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
async fn resolve_schema_with_non_existent_table_fails() {
    //* Given
    let ctx = TestCtx::setup(
        "resolve_schema_with_non_existent_table",
        [("eth_firehose", "_/eth_firehose@0.0.1")], // latest
    )
    .await;
    let sql_query = indoc::indoc! {"
        SELECT
            block_num,
            gas_limit
        FROM eth_firehose.non_existent_table
    "};

    //* When
    let resp = ctx.send_shema_request(sql_query).await;

    //* Then
    assert_eq!(
        resp.status(),
        StatusCode::NOT_FOUND,
        "schema resolution should fail for non-existent table"
    );

    let response: ErrorResponse = resp
        .json()
        .await
        .expect("failed to parse error response JSON");

    assert_eq!(
        response.error_code, "TABLE_NOT_FOUND_IN_DATASET",
        "should return TABLE_NOT_FOUND_IN_DATASET for non-existent table in valid dataset\n {}: {}",
        response.error_code, response.error_message,
    );
}

#[tokio::test]
async fn resolve_schema_with_unqualified_table_fails() {
    //* Given
    let ctx = TestCtx::setup(
        "resolve_schema_with_unqualified_table",
        [("eth_firehose", "_/eth_firehose@0.0.1")], // latest
    )
    .await;
    let sql_query = r#"SELECT block_num, hash FROM blocks"#;

    //* When
    let resp = ctx.send_shema_request(sql_query).await;

    //* Then
    assert_eq!(
        resp.status(),
        StatusCode::BAD_REQUEST,
        "schema resolution should fail for unqualified table reference"
    );

    let response: ErrorResponse = resp
        .json()
        .await
        .expect("failed to parse error response JSON");

    assert_eq!(
        response.error_code, "UNQUALIFIED_TABLE",
        "should return UNQUALIFIED_TABLE for unqualified table"
    );
    assert!(
        response.error_message.contains("Unqualified table"),
        "error message should mention unqualified table, got: {}",
        response.error_message
    );
}

#[tokio::test]
async fn resolve_schema_with_catalog_qualified_table_fails() {
    //* Given
    let ctx = TestCtx::setup(
        "resolve_schema_with_catalog_qualified_table",
        [("eth_firehose", "_/eth_firehose@0.0.1")], // latest
    )
    .await;
    let sql_query = r#"SELECT block_num, hash FROM catalog.eth_firehose.blocks"#;

    //* When
    let resp = ctx.send_shema_request(sql_query).await;

    //* Then
    assert_eq!(
        resp.status(),
        StatusCode::BAD_REQUEST,
        "schema resolution should fail for catalog-qualified table reference"
    );

    let response: ErrorResponse = resp
        .json()
        .await
        .expect("failed to parse error response JSON");

    assert_eq!(
        response.error_code, "CATALOG_QUALIFIED_TABLE",
        "should return CATALOG_QUALIFIED_TABLE for catalog-qualified table"
    );
    assert!(
        response.error_message.contains("Catalog-qualified table"),
        "error message should mention catalog-qualified table, got: {}",
        response.error_message
    );
}

#[tokio::test]
async fn resolve_schema_with_full_reference_succeeds() {
    //* Given
    let ctx = TestCtx::setup(
        "resolve_schema_with_full_reference",
        [("eth_firehose", "_/eth_firehose@0.0.1")], // latest
    )
    .await;
    let sql_query = r#"SELECT block_num, gas_limit, hash FROM "_/eth_firehose@0.0.1".blocks"#;

    //* When
    let resp = ctx.send_shema_request(sql_query).await;

    //* Then
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "schema resolution should succeed with full reference (namespace/name@version)"
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
async fn resolve_schema_with_namespace_and_name_succeeds() {
    //* Given
    let ctx = TestCtx::setup(
        "resolve_schema_with_namespace_and_name",
        [("eth_firehose", "_/eth_firehose@0.0.1")], // latest
    )
    .await;
    let sql_query = r#"SELECT block_num, gas_limit, hash FROM "_/eth_firehose".blocks"#;

    //* When
    let resp = ctx.send_shema_request(sql_query).await;

    //* Then
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "schema resolution should succeed with namespace and name (defaults to latest)"
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
async fn resolve_schema_with_name_and_version_succeeds() {
    //* Given
    let ctx = TestCtx::setup(
        "resolve_schema_with_name_and_version",
        [("eth_firehose", "_/eth_firehose@0.0.1")], // latest
    )
    .await;
    let sql_query = r#"SELECT block_num, gas_limit, hash FROM "eth_firehose@0.0.1".blocks"#;

    //* When
    let resp = ctx.send_shema_request(sql_query).await;

    //* Then
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "schema resolution should succeed with name and version (namespace defaults to _)"
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
async fn resolve_schema_with_hash_reference_succeeds() {
    //* Given
    let ctx = TestCtx::setup(
        "resolve_schema_with_hash_reference",
        [("eth_firehose", "_/eth_firehose@0.0.1")], // latest
    )
    .await;

    // Retrieve the manifest hash for the registered dataset
    let hash = ctx.get_dataset_hash("_/eth_firehose@0.0.1").await;

    let sql_query = format!(
        r#"SELECT block_num, gas_limit, hash FROM "_/eth_firehose@{}".blocks"#,
        hash
    );

    //* When
    let resp = ctx.send_shema_request(&sql_query).await;

    //* Then
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "schema resolution should succeed with hash reference"
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
async fn resolve_schema_with_latest_tag_succeeds() {
    //* Given
    let ctx = TestCtx::setup(
        "resolve_schema_with_latest_tag",
        [("eth_firehose", "_/eth_firehose@0.0.1")], // latest
    )
    .await;
    let sql_query = r#"SELECT block_num, gas_limit, hash FROM "_/eth_firehose@latest".blocks"#;

    //* When
    let resp = ctx.send_shema_request(sql_query).await;

    //* Then
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "schema resolution should succeed with explicit latest tag"
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
async fn resolve_schema_with_non_global_namespace_succeeds() {
    //* Given
    let ctx = TestCtx::setup(
        "resolve_schema_with_non_global_namespace",
        [("eth_firehose", "edgeandnode/eth_firehose@0.0.1")],
    )
    .await;
    let sql_query =
        r#"SELECT block_num, gas_limit, hash FROM "edgeandnode/eth_firehose@0.0.1".blocks"#;

    //* When
    let resp = ctx.send_shema_request(sql_query).await;

    //* Then
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "schema resolution should succeed with non-global namespace"
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
async fn resolve_schema_with_name_only_succeeds() {
    //* Given
    let ctx = TestCtx::setup(
        "resolve_schema_with_name_only",
        [("eth_firehose", "_/eth_firehose@0.0.1")], // latest
    )
    .await;
    let sql_query = r#"SELECT block_num, gas_limit, hash FROM "eth_firehose".blocks"#;

    //* When
    let resp = ctx.send_shema_request(sql_query).await;

    //* Then
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "schema resolution should succeed with name only (defaults: namespace=_, revision=latest)"
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
async fn resolve_schema_with_namespace_name_only_succeeds() {
    //* Given
    let ctx = TestCtx::setup(
        "resolve_schema_with_namespace_name_only",
        [("eth_firehose", "edgeandnode/eth_firehose@0.0.1")], // latest
    )
    .await;
    let sql_query = r#"SELECT block_num, gas_limit, hash FROM "edgeandnode/eth_firehose".blocks"#;

    //* When
    let resp = ctx.send_shema_request(sql_query).await;

    //* Then
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "schema resolution should succeed with namespace/name (defaults: revision=latest)"
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
    let ctx = TestCtx::setup(
        "resolve_schema_with_non_existent_dataset",
        [("eth_firehose", "edgeandnode/eth_firehose@0.0.1")], // latest
    )
    .await;
    let sql_query = r#"SELECT block_num, gas_limit, hash FROM "foo/non_existent@1.0.0".blocks"#;

    //* When
    let resp = ctx.send_shema_request(sql_query).await;

    //* Then
    assert_eq!(
        resp.status(),
        StatusCode::NOT_FOUND,
        "schema resolution should fail for non-existent dataset"
    );

    let response: ErrorResponse = resp
        .json()
        .await
        .expect("failed to parse error response JSON");

    assert_eq!(
        response.error_code, "DATASET_NOT_FOUND",
        "should return DATASET_NOT_FOUND for non-existent dataset\n {}: {}",
        response.error_code, response.error_message,
    );
}

#[tokio::test]
async fn resolve_schema_with_non_existent_version_fails() {
    //* Given
    let ctx = TestCtx::setup(
        "resolve_schema_with_non_existent_version",
        [("eth_firehose", "_/eth_firehose@0.0.1")],
    )
    .await;
    let sql_query = r#"SELECT block_num, gas_limit, hash FROM "_/eth_firehose@99.99.99".blocks"#;

    //* When
    let resp = ctx.send_shema_request(sql_query).await;

    //* Then
    assert_eq!(
        resp.status(),
        StatusCode::NOT_FOUND,
        "schema resolution should fail for non-existent version"
    );

    let response: ErrorResponse = resp
        .json()
        .await
        .expect("failed to parse error response JSON");

    assert_eq!(
        response.error_code, "DATASET_NOT_FOUND",
        "should return DATASET_NOT_FOUND for non-existent version\n {}: {}",
        response.error_code, response.error_message,
    );
}

#[tokio::test]
async fn resolve_schema_with_invalid_sql_fails() {
    //* Given
    let ctx = TestCtx::setup(
        "resolve_schema_with_invalid_sql",
        [("eth_firehose", "_/eth_firehose@0.0.1")],
    )
    .await;
    let sql_query = r#"SELECT FROM WHERE INVALID SQL"#;

    //* When
    let resp = ctx.send_shema_request(sql_query).await;

    //* Then
    assert_eq!(
        resp.status(),
        StatusCode::BAD_REQUEST,
        "schema resolution should fail for invalid SQL syntax"
    );

    let response: ErrorResponse = resp
        .json()
        .await
        .expect("failed to parse error response JSON");

    assert_eq!(
        response.error_code, "SQL_PARSE_ERROR",
        "should return SQL_PARSE_ERROR for invalid SQL syntax\n {}: {}",
        response.error_code, response.error_message,
    );
}

#[tokio::test]
async fn resolve_schema_with_dev_tag_succeeds() {
    //* Given
    let ctx = TestCtx::setup(
        "resolve_schema_with_dev_tag",
        [("eth_firehose", "_/eth_firehose@dev")],
    )
    .await;
    let sql_query = r#"SELECT block_num, gas_limit, hash FROM "_/eth_firehose@dev".blocks"#;

    //* When
    let resp = ctx.send_shema_request(sql_query).await;

    //* Then
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "schema resolution should succeed with @dev tag"
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
async fn resolve_schema_with_join_same_dataset_succeeds() {
    //* Given
    let ctx = TestCtx::setup(
        "resolve_schema_with_join_same_dataset",
        [("eth_firehose", "_/eth_firehose@0.0.1")],
    )
    .await;
    let sql_query = indoc::indoc! {r#"
        SELECT
            b.block_num,
            b.hash,
            t.tx_hash
        FROM eth_firehose.blocks b
        JOIN eth_firehose.transactions t ON b.hash = t.block_hash
    "#};

    //* When
    let resp = ctx.send_shema_request(sql_query).await;

    //* Then
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "schema resolution should succeed with JOIN within same dataset"
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
async fn resolve_schema_with_join_different_datasets_succeeds() {
    //* Given
    let ctx = TestCtx::setup(
        "resolve_schema_with_join_different_datasets",
        [
            ("eth_firehose", "_/eth_firehose@0.0.1"),
            ("base_firehose", "_/base_firehose@0.0.1"),
        ],
    )
    .await;
    let sql_query = indoc::indoc! {r#"
        SELECT
            e.block_num,
            e.hash,
            b.gas_used
        FROM eth_firehose.blocks e
        JOIN base_firehose.blocks b ON e.block_num = b.block_num
    "#};

    //* When
    let resp = ctx.send_shema_request(sql_query).await;

    //* Then
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "schema resolution should succeed with JOIN across different datasets"
    );

    let response: SchemaResponse = resp
        .json()
        .await
        .expect("failed to parse schema response JSON");

    assert!(
        !response.schema.arrow.fields.is_empty(),
        "resolved schema should contain at least one field"
    );
}

#[tokio::test]
async fn resolve_schema_with_empty_query_fails() {
    //* Given
    let ctx = TestCtx::setup(
        "resolve_schema_with_empty_query",
        [("eth_firehose", "_/eth_firehose@0.0.1")],
    )
    .await;
    let sql_query = "";

    //* When
    let resp = ctx.send_shema_request(sql_query).await;

    //* Then
    assert_eq!(
        resp.status(),
        StatusCode::BAD_REQUEST,
        "schema resolution should fail for empty query"
    );

    let response: ErrorResponse = resp
        .json()
        .await
        .expect("failed to parse error response JSON");

    assert_eq!(
        response.error_code, "INVALID_PAYLOAD_FORMAT",
        "should return INVALID_PAYLOAD_FORMAT for empty query\n {}: {}",
        response.error_code, response.error_message,
    );
}

#[tokio::test]
async fn resolve_schema_with_invalid_payload_fails() {
    //* Given
    let ctx = TestCtx::setup(
        "resolve_schema_with_invalid_payload",
        [("eth_firehose", "_/eth_firehose@0.0.1")],
    )
    .await;

    // Send invalid JSON (missing required field)
    let invalid_payload = serde_json::json!({
        "not_sql_query": "SELECT * FROM eth_firehose.blocks"
    });

    //* When
    let resp = ctx
        .client
        .post(&ctx.schema_api_url)
        .json(&invalid_payload)
        .send()
        .await
        .expect("failed to send schema validation request");

    //* Then
    assert_eq!(
        resp.status(),
        StatusCode::BAD_REQUEST,
        "schema resolution should fail for invalid payload format"
    );

    let response: ErrorResponse = resp
        .json()
        .await
        .expect("failed to parse error response JSON");

    assert_eq!(
        response.error_code, "INVALID_PAYLOAD_FORMAT",
        "should return INVALID_PAYLOAD_FORMAT for invalid request payload\n {}: {}",
        response.error_code, response.error_message,
    );
}

#[tokio::test]
async fn resolve_schema_with_aggregations_succeeds() {
    //* Given
    let ctx = TestCtx::setup(
        "resolve_schema_with_aggregations",
        [("eth_firehose", "_/eth_firehose@0.0.1")],
    )
    .await;
    let sql_query = indoc::indoc! {r#"
        SELECT
            miner,
            COUNT(*) as block_count,
            AVG(gas_used) as avg_gas_used,
            MAX(block_num) as max_block_num
        FROM eth_firehose.blocks
        GROUP BY miner
    "#};

    //* When
    let resp = ctx.send_shema_request(sql_query).await;

    //* Then
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "schema resolution should succeed with aggregations and GROUP BY"
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

struct TestCtx {
    _ctx: crate::testlib::ctx::TestCtx,
    client: reqwest::Client,
    schema_api_url: String,
    ampctl: Ampctl,
}

impl TestCtx {
    async fn setup(
        test_name: &str,
        manifests: impl IntoIterator<Item = impl Into<crate::testlib::ctx::ManifestRegistration>>,
    ) -> Self {
        let ctx = TestCtxBuilder::new(test_name)
            .with_dataset_manifests(manifests)
            .with_provider_config("firehose_eth_mainnet")
            .build()
            .await
            .expect("failed to build test context");

        let client = reqwest::Client::new();
        let admin_api_url = ctx.daemon_controller().admin_api_url();
        let ampctl = Ampctl::new(admin_api_url.clone());

        Self {
            _ctx: ctx,
            client,
            schema_api_url: format!("{}/schema", admin_api_url),
            ampctl,
        }
    }

    async fn send_shema_request(&self, sql_query: &str) -> reqwest::Response {
        let request_payload = serde_json::json!({
            "sql_query": sql_query
        });

        self.client
            .post(&self.schema_api_url)
            .json(&request_payload)
            .send()
            .await
            .expect("failed to send schema validation request")
    }

    async fn get_dataset_hash(&self, reference: &str) -> Hash {
        let dataset_ref: Reference = reference
            .parse()
            .expect("failed to parse dataset reference");

        self.ampctl
            .get_latest_manifest_hash(&dataset_ref)
            .await
            .expect("failed to get manifest hash")
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
    error_message: String,
}
