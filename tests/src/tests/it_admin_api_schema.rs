//! Integration tests for the Admin API schema resolution endpoint.
//!
//! This test suite provides coverage of the schema resolution API error conditions.
//!
//! ## Test Coverage
//!
//! ### Table Qualification Levels
//! - Unqualified: `blocks` → ERROR (BAD_REQUEST)
//! - Catalog-qualified: `catalog.eth_firehose.blocks` → ERROR (BAD_REQUEST)
//!
//! ### Error Conditions
//! - Empty query string → INVALID_PAYLOAD_FORMAT
//! - Unqualified/catalog-qualified tables → BAD_REQUEST

use reqwest::StatusCode;

use crate::testlib::ctx::TestCtxBuilder;

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
    let resp = ctx
        .send_schema_request_with_tables_and_deps([("query", sql_query)], [])
        .await;

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
    let resp = ctx
        .send_schema_request_with_tables_and_deps([("query", sql_query)], [])
        .await;

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
async fn resolve_schema_with_empty_query_fails() {
    //* Given
    let ctx = TestCtx::setup(
        "resolve_schema_with_empty_query",
        [("eth_firehose", "_/eth_firehose@0.0.1")],
    )
    .await;
    let sql_query = "";

    //* When
    let resp = ctx
        .send_schema_request_with_tables_and_deps([("query", sql_query)], [])
        .await;

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
async fn empty_tables_and_functions_fails() {
    //* Given
    let ctx = TestCtx::setup(
        "empty_tables_and_functions",
        [("eth_firehose", "_/eth_firehose@0.0.1")],
    )
    .await;

    //* When
    let resp = ctx.send_schema_request_with_tables_and_deps([], []).await;

    //* Then
    assert_eq!(
        resp.status(),
        StatusCode::BAD_REQUEST,
        "schema resolution should fail with empty tables and functions"
    );

    let response: ErrorResponse = resp
        .json()
        .await
        .expect("failed to parse error response JSON");

    assert_eq!(
        response.error_code, "EMPTY_TABLES_AND_FUNCTIONS",
        "should return EMPTY_TABLES_AND_FUNCTIONS when both are empty"
    );
}

#[tokio::test]
async fn invalid_table_name_fails() {
    //* Given
    let ctx = TestCtx::setup(
        "invalid_table_name",
        [("eth_firehose", "_/eth_firehose@0.0.1")],
    )
    .await;

    //* When
    let resp = ctx
        .send_schema_request_with_tables_and_deps(
            [("123invalid", "SELECT block_num FROM eth_firehose.blocks")],
            [],
        )
        .await;

    //* Then
    assert_eq!(
        resp.status(),
        StatusCode::BAD_REQUEST,
        "schema resolution should fail with invalid table name"
    );

    let response: ErrorResponse = resp
        .json()
        .await
        .expect("failed to parse error response JSON");

    assert_eq!(
        response.error_code, "INVALID_PAYLOAD_FORMAT",
        "should return INVALID_PAYLOAD_FORMAT for invalid table name"
    );
}

#[tokio::test]
async fn whitespace_only_sql_fails() {
    //* Given
    let ctx = TestCtx::setup(
        "whitespace_only_sql",
        [("eth_firehose", "_/eth_firehose@0.0.1")],
    )
    .await;

    //* When
    let resp = ctx
        .send_schema_request_with_tables_and_deps([("table1", "   ")], [])
        .await;

    //* Then
    assert_eq!(
        resp.status(),
        StatusCode::BAD_REQUEST,
        "schema resolution should fail with whitespace-only SQL"
    );

    let response: ErrorResponse = resp
        .json()
        .await
        .expect("failed to parse error response JSON");

    assert_eq!(
        response.error_code, "INVALID_PAYLOAD_FORMAT",
        "should return INVALID_PAYLOAD_FORMAT for whitespace-only SQL"
    );
}

#[tokio::test]
async fn multiple_tables_version_deps_succeeds() {
    //* Given
    let ctx = TestCtx::setup(
        "multiple_tables_version_deps",
        [
            ("eth_firehose", "_/eth_firehose@0.0.1"),
            ("base_firehose", "_/base_firehose@0.0.1"),
        ],
    )
    .await;

    //* When
    let resp = ctx
        .send_schema_request_with_tables_and_deps(
            [
                ("table1", "SELECT block_num FROM eth.blocks"),
                ("table2", "SELECT block_num FROM base.blocks"),
            ],
            [
                ("eth", "_/eth_firehose@0.0.1"),
                ("base", "_/base_firehose@0.0.1"),
            ],
        )
        .await;

    //* Then
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "schema resolution should succeed with version dependencies"
    );

    let _response: SchemaResponse = resp
        .json()
        .await
        .expect("failed to parse schema response JSON");
}

#[tokio::test]
async fn multiple_tables_nonexistent_version_fails() {
    //* Given
    let ctx = TestCtx::setup(
        "multiple_tables_nonexistent_version",
        [("eth_firehose", "_/eth_firehose@0.0.1")],
    )
    .await;

    //* When
    let resp = ctx
        .send_schema_request_with_tables_and_deps(
            [("table1", "SELECT block_num FROM eth.blocks")],
            [("eth", "_/eth_firehose@99.99.99")],
        )
        .await;

    //* Then
    assert_eq!(
        resp.status(),
        StatusCode::NOT_FOUND,
        "schema resolution should fail with nonexistent version"
    );

    let response: ErrorResponse = resp
        .json()
        .await
        .expect("failed to parse error response JSON");

    assert_eq!(
        response.error_code, "DEPENDENCY_NOT_FOUND",
        "should return DEPENDENCY_NOT_FOUND for nonexistent version"
    );
}

#[tokio::test]
async fn multiple_tables_latest_reference_fails() {
    //* Given
    let ctx = TestCtx::setup(
        "multiple_tables_latest_reference",
        [("eth_firehose", "_/eth_firehose@0.0.1")],
    )
    .await;

    //* When
    let resp = ctx
        .send_schema_request_with_tables_and_deps(
            [("table1", "SELECT block_num FROM eth.blocks")],
            [("eth", "_/eth_firehose@latest")],
        )
        .await;

    //* Then
    assert_eq!(
        resp.status(),
        StatusCode::BAD_REQUEST,
        "schema resolution should fail with latest reference"
    );

    let response: ErrorResponse = resp
        .json()
        .await
        .expect("failed to parse error response JSON");

    assert_eq!(
        response.error_code, "INVALID_PAYLOAD_FORMAT",
        "should return INVALID_PAYLOAD_FORMAT for latest reference"
    );
}

#[tokio::test]
async fn multiple_tables_dev_reference_fails() {
    //* Given
    let ctx = TestCtx::setup(
        "multiple_tables_dev_reference",
        [("eth_firehose", "_/eth_firehose@0.0.1")],
    )
    .await;

    //* When
    let resp = ctx
        .send_schema_request_with_tables_and_deps(
            [("table1", "SELECT block_num FROM eth.blocks")],
            [("eth", "_/eth_firehose@dev")],
        )
        .await;

    //* Then
    assert_eq!(
        resp.status(),
        StatusCode::BAD_REQUEST,
        "schema resolution should fail with dev reference"
    );

    let response: ErrorResponse = resp
        .json()
        .await
        .expect("failed to parse error response JSON");

    assert_eq!(
        response.error_code, "INVALID_PAYLOAD_FORMAT",
        "should return INVALID_PAYLOAD_FORMAT for dev reference"
    );
}

#[tokio::test]
async fn multiple_tables_malformed_dep_reference_fails() {
    //* Given
    let ctx = TestCtx::setup(
        "multiple_tables_malformed_dep",
        [("eth_firehose", "_/eth_firehose@0.0.1")],
    )
    .await;

    //* When
    let resp = ctx
        .send_schema_request_with_tables_and_deps(
            [("table1", "SELECT block_num FROM eth.blocks")],
            [("eth", "invalid-reference-format")],
        )
        .await;

    //* Then
    assert_eq!(
        resp.status(),
        StatusCode::BAD_REQUEST,
        "schema resolution should fail with malformed dependency reference"
    );

    let response: ErrorResponse = resp
        .json()
        .await
        .expect("failed to parse error response JSON");

    assert_eq!(
        response.error_code, "INVALID_PAYLOAD_FORMAT",
        "should return INVALID_PAYLOAD_FORMAT for malformed reference"
    );
}

#[tokio::test]
async fn multiple_tables_dep_missing_at_fails() {
    //* Given
    let ctx = TestCtx::setup(
        "multiple_tables_dep_missing_at",
        [("eth_firehose", "_/eth_firehose@0.0.1")],
    )
    .await;

    //* When
    let resp = ctx
        .send_schema_request_with_tables_and_deps(
            [("table1", "SELECT block_num FROM eth.blocks")],
            [("eth", "_/eth_firehose")],
        )
        .await;

    //* Then
    assert_eq!(
        resp.status(),
        StatusCode::BAD_REQUEST,
        "schema resolution should fail with missing @ separator"
    );

    let response: ErrorResponse = resp
        .json()
        .await
        .expect("failed to parse error response JSON");

    assert_eq!(
        response.error_code, "INVALID_PAYLOAD_FORMAT",
        "should return INVALID_PAYLOAD_FORMAT for missing @"
    );
}

#[tokio::test]
async fn multiple_tables_shared_dep_succeeds() {
    //* Given
    let ctx = TestCtx::setup(
        "multiple_tables_shared_dep",
        [("eth_firehose", "_/eth_firehose@0.0.1")],
    )
    .await;

    //* When
    let resp = ctx
        .send_schema_request_with_tables_and_deps(
            [
                ("table1", "SELECT block_num FROM eth.blocks"),
                ("table2", "SELECT hash FROM eth.blocks"),
                ("table3", "SELECT gas_limit FROM eth.blocks"),
            ],
            [("eth", "_/eth_firehose@0.0.1")],
        )
        .await;

    //* Then
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "schema resolution should succeed with shared dependency"
    );

    let _response: SchemaResponse = resp
        .json()
        .await
        .expect("failed to parse schema response JSON");
}

#[tokio::test]
async fn multiple_tables_different_deps_succeeds() {
    //* Given
    let ctx = TestCtx::setup(
        "multiple_tables_different_deps",
        [
            ("eth_firehose", "_/eth_firehose@0.0.1"),
            ("base_firehose", "_/base_firehose@0.0.1"),
        ],
    )
    .await;

    //* When
    let resp = ctx
        .send_schema_request_with_tables_and_deps(
            [
                ("table1", "SELECT block_num FROM eth.blocks"),
                ("table2", "SELECT block_num FROM base.blocks"),
            ],
            [
                ("eth", "_/eth_firehose@0.0.1"),
                ("base", "_/base_firehose@0.0.1"),
            ],
        )
        .await;

    //* Then
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "schema resolution should succeed with different dependencies"
    );

    let _response: SchemaResponse = resp
        .json()
        .await
        .expect("failed to parse schema response JSON");
}

#[tokio::test]
async fn multiple_tables_invalid_sql_fails() {
    //* Given
    let ctx = TestCtx::setup(
        "multiple_tables_invalid_sql",
        [("eth_firehose", "_/eth_firehose@0.0.1")],
    )
    .await;

    //* When
    let resp = ctx
        .send_schema_request_with_tables_and_deps(
            [
                ("table1", "SELECT block_num FROM eth.blocks"),
                ("table2", "THIS IS NOT VALID SQL AT ALL"),
            ],
            [("eth", "_/eth_firehose@0.0.1")],
        )
        .await;

    //* Then
    assert_eq!(
        resp.status(),
        StatusCode::BAD_REQUEST,
        "schema resolution should fail with invalid SQL"
    );

    let response: ErrorResponse = resp
        .json()
        .await
        .expect("failed to parse error response JSON");

    assert_eq!(
        response.error_code, "INVALID_TABLE_SQL",
        "should return INVALID_TABLE_SQL for invalid SQL syntax"
    );
}

#[tokio::test]
async fn multiple_tables_select_statements_succeeds() {
    //* Given
    let ctx = TestCtx::setup(
        "multiple_tables_select_statements",
        [("eth_firehose", "_/eth_firehose@0.0.1")],
    )
    .await;

    //* When
    let resp = ctx
        .send_schema_request_with_tables_and_deps(
            [
                ("table1", "SELECT block_num, hash FROM eth.blocks"),
                (
                    "table2",
                    "SELECT block_num, gas_limit, gas_used FROM eth.blocks",
                ),
            ],
            [("eth", "_/eth_firehose@0.0.1")],
        )
        .await;

    //* Then
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "schema resolution should succeed with multiple SELECT statements"
    );

    let _response: SchemaResponse = resp
        .json()
        .await
        .expect("failed to parse schema response JSON");
}

#[tokio::test]
async fn multiple_tables_unqualified_table_fails() {
    //* Given
    let ctx = TestCtx::setup(
        "multiple_tables_unqualified_table",
        [("eth_firehose", "_/eth_firehose@0.0.1")],
    )
    .await;

    //* When
    let resp = ctx
        .send_schema_request_with_tables_and_deps(
            [
                ("table1", "SELECT block_num FROM eth.blocks"),
                ("table2", "SELECT block_num FROM blocks"),
            ],
            [("eth", "_/eth_firehose@0.0.1")],
        )
        .await;

    //* Then
    assert_eq!(
        resp.status(),
        StatusCode::BAD_REQUEST,
        "schema resolution should fail with unqualified table"
    );

    let response: ErrorResponse = resp
        .json()
        .await
        .expect("failed to parse error response JSON");

    assert_eq!(
        response.error_code, "UNQUALIFIED_TABLE",
        "should return UNQUALIFIED_TABLE"
    );
}

#[tokio::test]
async fn multiple_tables_catalog_qualified_fails() {
    //* Given
    let ctx = TestCtx::setup(
        "multiple_tables_catalog_qualified",
        [("eth_firehose", "_/eth_firehose@0.0.1")],
    )
    .await;

    //* When
    let resp = ctx
        .send_schema_request_with_tables_and_deps(
            [
                ("table1", "SELECT block_num FROM eth.blocks"),
                ("table2", "SELECT block_num FROM catalog.eth.blocks"),
            ],
            [("eth", "_/eth_firehose@0.0.1")],
        )
        .await;

    //* Then
    assert_eq!(
        resp.status(),
        StatusCode::BAD_REQUEST,
        "schema resolution should fail with catalog-qualified table"
    );

    let response: ErrorResponse = resp
        .json()
        .await
        .expect("failed to parse error response JSON");

    assert_eq!(
        response.error_code, "CATALOG_QUALIFIED_TABLE",
        "should return CATALOG_QUALIFIED_TABLE"
    );
}

#[tokio::test]
async fn multiple_tables_schema_qualified_succeeds() {
    //* Given
    let ctx = TestCtx::setup(
        "multiple_tables_schema_qualified",
        [("eth_firehose", "_/eth_firehose@0.0.1")],
    )
    .await;

    //* When
    let resp = ctx
        .send_schema_request_with_tables_and_deps(
            [
                ("table1", "SELECT block_num FROM eth.blocks"),
                ("table2", "SELECT block_num FROM eth.transactions"),
            ],
            [("eth", "_/eth_firehose@0.0.1")],
        )
        .await;

    //* Then
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "schema resolution should succeed with schema-qualified tables"
    );

    let _response: SchemaResponse = resp
        .json()
        .await
        .expect("failed to parse schema response JSON");
}

#[tokio::test]
async fn multiple_tables_undefined_alias_fails() {
    //* Given
    let ctx = TestCtx::setup(
        "multiple_tables_undefined_alias",
        [("eth_firehose", "_/eth_firehose@0.0.1")],
    )
    .await;

    //* When
    let resp = ctx
        .send_schema_request_with_tables_and_deps(
            [
                ("table1", "SELECT block_num FROM eth.blocks"),
                ("table2", "SELECT block_num FROM undefined.blocks"),
            ],
            [("eth", "_/eth_firehose@0.0.1")],
        )
        .await;

    //* Then
    assert_eq!(
        resp.status(),
        StatusCode::BAD_REQUEST,
        "schema resolution should fail with undefined alias"
    );

    let response: ErrorResponse = resp
        .json()
        .await
        .expect("failed to parse error response JSON");

    assert_eq!(
        response.error_code, "DEPENDENCY_ALIAS_NOT_FOUND",
        "should return DEPENDENCY_ALIAS_NOT_FOUND"
    );
}

#[tokio::test]
async fn multiple_tables_nonexistent_dataset_fails() {
    //* Given
    let ctx = TestCtx::setup(
        "multiple_tables_nonexistent_dataset",
        [] as [(&str, &str); 0],
    )
    .await;

    //* When
    let resp = ctx
        .send_schema_request_with_tables_and_deps(
            [("table1", "SELECT block_num FROM foo.blocks")],
            [("foo", "_/nonexistent@0.0.1")],
        )
        .await;

    //* Then
    assert_eq!(
        resp.status(),
        StatusCode::NOT_FOUND,
        "schema resolution should fail with nonexistent dataset"
    );

    let response: ErrorResponse = resp
        .json()
        .await
        .expect("failed to parse error response JSON");

    assert_eq!(
        response.error_code, "DEPENDENCY_NOT_FOUND",
        "should return DEPENDENCY_NOT_FOUND"
    );
}

#[tokio::test]
async fn two_tables_same_dep_succeeds() {
    //* Given
    let ctx = TestCtx::setup(
        "two_tables_same_dep",
        [("eth_firehose", "_/eth_firehose@0.0.1")],
    )
    .await;

    //* When
    let resp = ctx
        .send_schema_request_with_tables_and_deps(
            [
                ("blocks_summary", "SELECT block_num, hash FROM eth.blocks"),
                (
                    "transactions_summary",
                    "SELECT tx_hash, block_hash FROM eth.transactions",
                ),
            ],
            [("eth", "_/eth_firehose@0.0.1")],
        )
        .await;

    //* Then
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "schema resolution should succeed with two tables using same dependency"
    );

    let _response: SchemaResponse = resp
        .json()
        .await
        .expect("failed to parse schema response JSON");
}

#[tokio::test]
async fn three_tables_different_deps_succeeds() {
    //* Given
    let ctx = TestCtx::setup(
        "three_tables_different_deps",
        [
            ("eth_firehose", "_/eth_firehose@0.0.1"),
            ("base_firehose", "_/base_firehose@0.0.1"),
        ],
    )
    .await;

    //* When
    let resp = ctx
        .send_schema_request_with_tables_and_deps(
            [
                ("eth_blocks", "SELECT block_num FROM eth.blocks"),
                ("base_blocks", "SELECT block_num FROM base.blocks"),
                ("eth_txs", "SELECT tx_hash FROM eth.transactions"),
            ],
            [
                ("eth", "_/eth_firehose@0.0.1"),
                ("base", "_/base_firehose@0.0.1"),
            ],
        )
        .await;

    //* Then
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "schema resolution should succeed with three tables and different dependencies"
    );

    let _response: SchemaResponse = resp
        .json()
        .await
        .expect("failed to parse schema response JSON");
}

#[tokio::test]
async fn multiple_tables_join_statements_succeeds() {
    //* Given
    let ctx = TestCtx::setup(
        "multiple_tables_join_statements",
        [("eth_firehose", "_/eth_firehose@0.0.1")],
    )
    .await;

    //* When
    let resp = ctx
        .send_schema_request_with_tables_and_deps(
            [
                (
                    "table1",
                    "SELECT b.block_num, t.tx_hash FROM eth.blocks b JOIN eth.transactions t ON b.hash = t.block_hash",
                ),
                (
                    "table2",
                    "SELECT b.block_num, l.address FROM eth.blocks b JOIN eth.logs l ON b.hash = l.block_hash",
                ),
            ],
            [("eth", "_/eth_firehose@0.0.1")],
        )
        .await;

    //* Then
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "schema resolution should succeed with JOIN statements"
    );

    let _response: SchemaResponse = resp
        .json()
        .await
        .expect("failed to parse schema response JSON");
}

#[tokio::test]
async fn multiple_tables_aggregations_succeeds() {
    //* Given
    let ctx = TestCtx::setup(
        "multiple_tables_aggregations",
        [("eth_firehose", "_/eth_firehose@0.0.1")],
    )
    .await;

    //* When
    let resp = ctx
        .send_schema_request_with_tables_and_deps(
            [
                ("table1", "SELECT COUNT(*) as total FROM eth.blocks"),
                (
                    "table2",
                    "SELECT miner, COUNT(*) as block_count FROM eth.blocks GROUP BY miner",
                ),
            ],
            [("eth", "_/eth_firehose@0.0.1")],
        )
        .await;

    //* Then
    assert_eq!(
        resp.status(),
        StatusCode::BAD_REQUEST,
        "schema resolution should fail with aggregations since it's non-incremental"
    );

    let _response: ErrorResponse = resp
        .json()
        .await
        .expect("failed to parse error response JSON");
}

#[tokio::test]
async fn multiple_tables_subqueries_succeeds() {
    //* Given
    let ctx = TestCtx::setup(
        "multiple_tables_subqueries",
        [("eth_firehose", "_/eth_firehose@0.0.1")],
    )
    .await;

    //* When
    let resp = ctx
        .send_schema_request_with_tables_and_deps(
            [
                (
                    "table1",
                    "SELECT block_num FROM eth.blocks WHERE block_num > (SELECT MIN(block_num) FROM eth.blocks)",
                ),
            ],
            [("eth", "_/eth_firehose@0.0.1")],
        )
        .await;

    //* Then
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "schema resolution should succeed with subqueries"
    );

    let _response: SchemaResponse = resp
        .json()
        .await
        .expect("failed to parse schema response JSON");
}

#[tokio::test]
async fn multiple_tables_ctes_succeeds() {
    //* Given
    let ctx = TestCtx::setup(
        "multiple_tables_ctes",
        [("eth_firehose", "_/eth_firehose@0.0.1")],
    )
    .await;

    //* When
    let resp = ctx
        .send_schema_request_with_tables_and_deps(
            [
                (
                    "table1",
                    "WITH block_summary AS (SELECT block_num, hash FROM eth.blocks) SELECT * FROM block_summary",
                ),
            ],
            [("eth", "_/eth_firehose@0.0.1")],
        )
        .await;

    //* Then
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "schema resolution should succeed with CTEs"
    );

    let _response: SchemaResponse = resp
        .json()
        .await
        .expect("failed to parse schema response JSON");
}

#[tokio::test]
async fn multiple_tables_mixed_queries_succeeds() {
    //* Given
    let ctx = TestCtx::setup(
        "multiple_tables_mixed_queries",
        [("eth_firehose", "_/eth_firehose@0.0.1")],
    )
    .await;

    //* When
    let resp = ctx
        .send_schema_request_with_tables_and_deps(
            [
                ("simple", "SELECT block_num FROM eth.blocks"),
                (
                    "aggregate",
                    "SELECT COUNT(*) as total FROM eth.transactions",
                ),
            ],
            [("eth", "_/eth_firehose@0.0.1")],
        )
        .await;

    //* Then
    assert_eq!(
        resp.status(),
        StatusCode::BAD_REQUEST,
        "schema resolution should fail with mixed query types since aggregate is non-incremental"
    );

    let _response: ErrorResponse = resp
        .json()
        .await
        .expect("failed to parse error response JSON");
}

#[tokio::test]
async fn multiple_tables_alias_qualified_succeeds() {
    //* Given
    let ctx = TestCtx::setup(
        "multiple_tables_alias_qualified",
        [("eth_firehose", "_/eth_firehose@0.0.1")],
    )
    .await;

    //* When
    let resp = ctx
        .send_schema_request_with_tables_and_deps(
            [
                ("table1", "SELECT block_num FROM eth_alias.blocks"),
                ("table2", "SELECT tx_hash FROM eth_alias.transactions"),
            ],
            [("eth_alias", "_/eth_firehose@0.0.1")],
        )
        .await;

    //* Then
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "schema resolution should succeed with alias-qualified tables"
    );

    let _response: SchemaResponse = resp
        .json()
        .await
        .expect("failed to parse schema response JSON");
}

#[tokio::test]
async fn multiple_tables_cross_referencing_succeeds() {
    //* Given
    let ctx = TestCtx::setup(
        "multiple_tables_cross_referencing",
        [
            ("eth_firehose", "_/eth_firehose@0.0.1"),
            ("base_firehose", "_/base_firehose@0.0.1"),
        ],
    )
    .await;

    //* When
    let resp = ctx
        .send_schema_request_with_tables_and_deps(
            [
                ("eth_data", "SELECT block_num FROM eth.blocks"),
                ("base_data", "SELECT block_num FROM base.blocks"),
            ],
            [
                ("eth", "_/eth_firehose@0.0.1"),
                ("base", "_/base_firehose@0.0.1"),
            ],
        )
        .await;

    //* Then
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "schema resolution should succeed with cross-referencing tables"
    );

    let _response: SchemaResponse = resp
        .json()
        .await
        .expect("failed to parse schema response JSON");
}

#[tokio::test]
async fn multiple_tables_same_external_table_succeeds() {
    //* Given
    let ctx = TestCtx::setup(
        "multiple_tables_same_external_table",
        [("eth_firehose", "_/eth_firehose@0.0.1")],
    )
    .await;

    //* When
    let resp = ctx
        .send_schema_request_with_tables_and_deps(
            [
                ("blocks_view1", "SELECT block_num FROM eth.blocks"),
                ("blocks_view2", "SELECT hash FROM eth.blocks"),
            ],
            [("eth", "_/eth_firehose@0.0.1")],
        )
        .await;

    //* Then
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "schema resolution should succeed when same external table used in multiple queries"
    );

    let _response: SchemaResponse = resp
        .json()
        .await
        .expect("failed to parse schema response JSON");
}

#[tokio::test]
async fn multiple_tables_existing_datasets_succeeds() {
    //* Given
    let ctx = TestCtx::setup(
        "multiple_tables_existing_datasets",
        [
            ("eth_firehose", "_/eth_firehose@0.0.1"),
            ("base_firehose", "_/base_firehose@0.0.1"),
        ],
    )
    .await;

    //* When
    let resp = ctx
        .send_schema_request_with_tables_and_deps(
            [
                ("eth_query", "SELECT block_num FROM eth.blocks"),
                ("base_query", "SELECT block_num FROM base.blocks"),
            ],
            [
                ("eth", "_/eth_firehose@0.0.1"),
                ("base", "_/base_firehose@0.0.1"),
            ],
        )
        .await;

    //* Then
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "schema resolution should succeed with existing datasets"
    );

    let _response: SchemaResponse = resp
        .json()
        .await
        .expect("failed to parse schema response JSON");
}

#[tokio::test]
async fn multiple_tables_table_not_in_dataset_fails() {
    //* Given
    let ctx = TestCtx::setup(
        "multiple_tables_table_not_in_dataset",
        [("eth_firehose", "_/eth_firehose@0.0.1")],
    )
    .await;

    //* When
    let resp = ctx
        .send_schema_request_with_tables_and_deps(
            [
                ("table1", "SELECT block_num FROM eth.blocks"),
                ("table2", "SELECT foo FROM eth.nonexistent_table"),
            ],
            [("eth", "_/eth_firehose@0.0.1")],
        )
        .await;

    //* Then
    assert_eq!(
        resp.status(),
        StatusCode::NOT_FOUND,
        "schema resolution should fail with table not in dataset"
    );

    let response: ErrorResponse = resp
        .json()
        .await
        .expect("failed to parse error response JSON");

    assert_eq!(
        response.error_code, "TABLE_NOT_FOUND_IN_DATASET",
        "should return TABLE_NOT_FOUND_IN_DATASET"
    );
}

#[tokio::test]
async fn multiple_tables_different_versions_succeeds() {
    //* Given
    let ctx = TestCtx::setup(
        "multiple_tables_different_versions",
        [("eth_firehose", "_/eth_firehose@0.0.1")],
    )
    .await;

    //* When
    let resp = ctx
        .send_schema_request_with_tables_and_deps(
            [("table1", "SELECT block_num FROM eth_v1.blocks")],
            [("eth_v1", "_/eth_firehose@0.0.1")],
        )
        .await;

    //* Then
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "schema resolution should succeed with different versions"
    );

    let _response: SchemaResponse = resp
        .json()
        .await
        .expect("failed to parse schema response JSON");
}

#[tokio::test]
async fn five_tables_mixed_deps_succeeds() {
    //* Given
    let ctx = TestCtx::setup(
        "five_tables_mixed_deps",
        [
            ("eth_firehose", "_/eth_firehose@0.0.1"),
            ("base_firehose", "_/base_firehose@0.0.1"),
        ],
    )
    .await;

    //* When
    let resp = ctx
        .send_schema_request_with_tables_and_deps(
            [
                ("eth_blocks", "SELECT block_num FROM eth.blocks"),
                ("eth_txs", "SELECT tx_hash FROM eth.transactions"),
                ("base_blocks", "SELECT block_num FROM base.blocks"),
                ("base_txs", "SELECT tx_hash FROM base.transactions"),
                ("eth_logs", "SELECT address FROM eth.logs"),
            ],
            [
                ("eth", "_/eth_firehose@0.0.1"),
                ("base", "_/base_firehose@0.0.1"),
            ],
        )
        .await;

    //* Then
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "schema resolution should succeed with five tables and mixed dependencies"
    );

    let _response: SchemaResponse = resp
        .json()
        .await
        .expect("failed to parse schema response JSON");
}

#[tokio::test]
async fn multiple_tables_parse_error_fails() {
    //* Given
    let ctx = TestCtx::setup(
        "multiple_tables_parse_error",
        [("eth_firehose", "_/eth_firehose@0.0.1")],
    )
    .await;

    //* When
    let resp = ctx
        .send_schema_request_with_tables_and_deps(
            [
                ("table1", "SELECT block_num FROM eth.blocks"),
                ("table2", "NOT VALID SQL SYNTAX!!!"),
            ],
            [("eth", "_/eth_firehose@0.0.1")],
        )
        .await;

    //* Then
    assert_eq!(
        resp.status(),
        StatusCode::BAD_REQUEST,
        "schema resolution should fail with parse error"
    );

    let response: ErrorResponse = resp
        .json()
        .await
        .expect("failed to parse error response JSON");

    assert_eq!(
        response.error_code, "INVALID_TABLE_SQL",
        "should return INVALID_TABLE_SQL"
    );
}

#[tokio::test]
async fn multiple_tables_long_name_fails() {
    //* Given
    let ctx = TestCtx::setup(
        "multiple_tables_long_name",
        [("eth_firehose", "_/eth_firehose@0.0.1")],
    )
    .await;

    //* When
    let very_long_name = "a".repeat(64);
    let resp = ctx
        .send_schema_request_with_tables_and_deps(
            [(very_long_name.as_str(), "SELECT block_num FROM eth.blocks")],
            [("eth", "_/eth_firehose@0.0.1")],
        )
        .await;

    //* Then
    assert_eq!(
        resp.status(),
        StatusCode::BAD_REQUEST,
        "schema resolution should fail with overly long table name"
    );

    let response: ErrorResponse = resp
        .json()
        .await
        .expect("failed to parse error response JSON");

    assert_eq!(
        response.error_code, "INVALID_PAYLOAD_FORMAT",
        "should return INVALID_PAYLOAD_FORMAT for long table name"
    );
}

#[tokio::test]
async fn multiple_tables_special_chars_fails() {
    //* Given
    let ctx = TestCtx::setup(
        "multiple_tables_special_chars",
        [("eth_firehose", "_/eth_firehose@0.0.1")],
    )
    .await;

    //* When
    let resp = ctx
        .send_schema_request_with_tables_and_deps(
            [("table@name!", "SELECT block_num FROM eth.blocks")],
            [("eth", "_/eth_firehose@0.0.1")],
        )
        .await;

    //* Then
    assert_eq!(
        resp.status(),
        StatusCode::BAD_REQUEST,
        "schema resolution should fail with special characters in table name"
    );

    let response: ErrorResponse = resp
        .json()
        .await
        .expect("failed to parse error response JSON");

    assert_eq!(
        response.error_code, "INVALID_PAYLOAD_FORMAT",
        "should return INVALID_PAYLOAD_FORMAT for special characters"
    );
}

#[tokio::test]
async fn function_not_in_dataset_fails_at_catalog_construction() {
    //* Given
    let ctx = TestCtx::setup(
        "function_not_in_dataset_fails_at_catalog_construction",
        [("eth_firehose", "_/eth_firehose@0.0.1")],
    )
    .await;

    // eth_firehose has no custom UDFs defined - only raw tables
    // This function reference will not be found in the dataset
    let sql_query = r#"SELECT block_num, eth.nonexistent_function(hash) FROM eth.blocks"#;

    //* When
    let resp = ctx
        .send_schema_request_with_tables_and_deps(
            [("query", sql_query)],
            [("eth", "_/eth_firehose@0.0.1")],
        )
        .await;

    //* Then
    assert_eq!(
        resp.status(),
        StatusCode::NOT_FOUND,
        "schema resolution should fail at catalog construction with 404"
    );

    let response: ErrorResponse = resp
        .json()
        .await
        .expect("failed to parse error response JSON");

    assert_eq!(
        response.error_code, "FUNCTION_NOT_FOUND_IN_DATASET",
        "should return FUNCTION_NOT_FOUND_IN_DATASET error"
    );
    assert!(
        response.error_message.contains("eth.nonexistent_function")
            && response.error_message.contains("_/eth_firehose"),
        "error message should indicate function and dataset, got: {}",
        response.error_message
    );
}

#[tokio::test]
async fn multiple_tables_with_missing_function_fails_on_first() {
    //* Given
    let ctx = TestCtx::setup(
        "multiple_tables_with_missing_function",
        [("eth_firehose", "_/eth_firehose@0.0.1")],
    )
    .await;

    //* When
    let resp = ctx
        .send_schema_request_with_tables_and_deps(
            [
                ("table1", "SELECT block_num FROM eth.blocks"),
                ("table2", "SELECT eth.fake_decode(hash) FROM eth.logs"),
                (
                    "table3",
                    "SELECT eth.another_missing_fn(data) FROM eth.transactions",
                ),
            ],
            [("eth", "_/eth_firehose@0.0.1")],
        )
        .await;

    //* Then
    assert_eq!(
        resp.status(),
        StatusCode::NOT_FOUND,
        "schema resolution should fail when a table references missing function"
    );

    let response: ErrorResponse = resp
        .json()
        .await
        .expect("failed to parse error response JSON");

    assert_eq!(
        response.error_code, "FUNCTION_NOT_FOUND_IN_DATASET",
        "should return FUNCTION_NOT_FOUND_IN_DATASET error"
    );
    // Should mention the missing function and the dataset
    assert!(
        response
            .error_message
            .contains("function 'eth.fake_decode' not found in dataset _/eth_firehose"),
        "error message should reference the missing function and dataset, got: {}",
        response.error_message
    );
}

#[tokio::test]
async fn bare_builtin_function_succeeds() {
    //* Given
    let ctx = TestCtx::setup(
        "bare_builtin_function",
        [("eth_firehose", "_/eth_firehose@0.0.1")],
    )
    .await;

    // Using CAST, which is a built-in function that doesn't need qualification
    let sql_query = r#"SELECT CAST(block_num AS VARCHAR) as block_str FROM eth.blocks"#;

    //* When
    let resp = ctx
        .send_schema_request_with_tables_and_deps(
            [("query", sql_query)],
            [("eth", "_/eth_firehose@0.0.1")],
        )
        .await;

    //* Then
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "schema resolution should succeed with bare built-in functions"
    );

    let response: SchemaResponse = resp
        .json()
        .await
        .expect("failed to parse schema response JSON");

    let schemas = response
        .schemas
        .as_object()
        .expect("schemas should be an object");
    assert!(schemas.contains_key("query"), "query schema should exist");
}

#[tokio::test]
async fn function_with_catalog_qualification_fails() {
    //* Given
    let ctx = TestCtx::setup(
        "function_with_catalog_qualification",
        [("eth_firehose", "_/eth_firehose@0.0.1")],
    )
    .await;

    // Catalog-qualified function (3 parts) - not supported
    let sql_query = r#"SELECT catalog.schema.function(data) FROM eth.blocks"#;

    //* When
    let resp = ctx
        .send_schema_request_with_tables_and_deps(
            [("query", sql_query)],
            [("eth", "_/eth_firehose@0.0.1")],
        )
        .await;

    //* Then
    assert_eq!(
        resp.status(),
        StatusCode::BAD_REQUEST,
        "schema resolution should fail with catalog-qualified function"
    );

    let response: ErrorResponse = resp
        .json()
        .await
        .expect("failed to parse error response JSON");

    // The error should indicate catalog-qualified function (not supported)
    assert_eq!(
        response.error_code, "CATALOG_QUALIFIED_FUNCTION",
        "should return CATALOG_QUALIFIED_FUNCTION for catalog-qualified function"
    );
    assert!(
        response
            .error_message
            .contains("Catalog-qualified function references are not supported"),
        "error message should mention catalog-qualified functions are not supported, got: {}",
        response.error_message
    );
}

#[tokio::test]
async fn function_with_invalid_format_fails() {
    //* Given
    let ctx = TestCtx::setup(
        "function_with_invalid_format",
        [("eth_firehose", "_/eth_firehose@0.0.1")],
    )
    .await;

    // Invalid function format (4+ parts) - invalid format
    let sql_query = r#"SELECT a.b.c.d(data) FROM eth.blocks"#;

    //* When
    let resp = ctx
        .send_schema_request_with_tables_and_deps(
            [("query", sql_query)],
            [("eth", "_/eth_firehose@0.0.1")],
        )
        .await;

    //* Then
    assert_eq!(
        resp.status(),
        StatusCode::BAD_REQUEST,
        "schema resolution should fail with invalid function format"
    );

    let response: ErrorResponse = resp
        .json()
        .await
        .expect("failed to parse error response JSON");

    // The error should indicate function reference resolution failure
    assert_eq!(
        response.error_code, "FUNCTION_REFERENCE_RESOLUTION",
        "should return FUNCTION_REFERENCE_RESOLUTION for invalid function format"
    );
    assert!(
        response.error_message.contains("Invalid function format"),
        "error message should mention invalid function format, got: {}",
        response.error_message
    );
}

#[tokio::test]
async fn function_with_undefined_alias_fails() {
    //* Given
    let ctx = TestCtx::setup(
        "function_with_undefined_alias",
        [("eth_firehose", "_/eth_firehose@0.0.1")],
    )
    .await;

    // Reference function with undefined alias
    let sql_query = r#"SELECT block_num, undefined_alias.my_function(hash) FROM eth.blocks"#;

    //* When
    let resp = ctx
        .send_schema_request_with_tables_and_deps(
            [("query", sql_query)],
            [("eth", "_/eth_firehose@0.0.1")], // Only 'eth' alias defined
        )
        .await;

    //* Then
    assert_eq!(
        resp.status(),
        StatusCode::BAD_REQUEST,
        "schema resolution should fail with undefined function alias"
    );

    let response: ErrorResponse = resp
        .json()
        .await
        .expect("failed to parse error response JSON");

    assert_eq!(
        response.error_code, "DEPENDENCY_ALIAS_NOT_FOUND",
        "should return DEPENDENCY_ALIAS_NOT_FOUND for undefined function alias"
    );
    assert!(
        response.error_message.contains("undefined_alias"),
        "error message should mention the undefined alias, got: {}",
        response.error_message
    );
}

#[tokio::test]
async fn function_on_nonexistent_dataset_fails() {
    //* Given
    let ctx = TestCtx::setup("function_on_nonexistent_dataset", [] as [(&str, &str); 0]).await;

    // Reference function from non-existent dataset
    let sql_query = r#"SELECT fake_dataset.fake_fn(data) FROM eth.blocks"#;

    //* When
    let resp = ctx
        .send_schema_request_with_tables_and_deps(
            [("query", sql_query)],
            [
                ("eth", "_/eth_firehose@0.0.1"),
                ("fake_dataset", "_/nonexistent@0.0.1"),
            ],
        )
        .await;

    //* Then
    assert_eq!(
        resp.status(),
        StatusCode::NOT_FOUND,
        "schema resolution should fail with non-existent dataset for function"
    );

    let response: ErrorResponse = resp
        .json()
        .await
        .expect("failed to parse error response JSON");

    assert_eq!(
        response.error_code, "DEPENDENCY_NOT_FOUND",
        "should return DEPENDENCY_NOT_FOUND for non-existent dataset"
    );
}

#[tokio::test]
async fn multiple_functions_mixed_validity_fails() {
    //* Given
    let ctx = TestCtx::setup(
        "multiple_functions_mixed_validity",
        [("eth_firehose", "_/eth_firehose@0.0.1")],
    )
    .await;

    // Mix of built-in functions (COUNT, SUM) and non-existent custom function
    let sql_query = r#"SELECT COUNT(*) as total, eth.nonexistent_fn(hash), SUM(gas_used) as gas FROM eth.blocks"#;

    //* When
    let resp = ctx
        .send_schema_request_with_tables_and_deps(
            [("query", sql_query)],
            [("eth", "_/eth_firehose@0.0.1")],
        )
        .await;

    //* Then
    assert_eq!(
        resp.status(),
        StatusCode::NOT_FOUND,
        "schema resolution should fail due to invalid function despite valid built-ins"
    );

    let response: ErrorResponse = resp
        .json()
        .await
        .expect("failed to parse error response JSON");

    assert_eq!(
        response.error_code, "FUNCTION_NOT_FOUND_IN_DATASET",
        "should return FUNCTION_NOT_FOUND_IN_DATASET error"
    );
    assert!(
        response.error_message.contains("eth.nonexistent_fn"),
        "error message should indicate the invalid function, got: {}",
        response.error_message
    );
}

#[tokio::test]
async fn non_incremental_query_with_limit_fails() {
    //* Given
    let ctx = TestCtx::setup(
        "non_incremental_query_with_limit",
        [("eth_firehose", "_/eth_firehose@0.0.1")],
    )
    .await;

    //* When
    let resp = ctx
        .send_schema_request_with_tables_and_deps(
            [("table1", "SELECT block_num FROM eth.blocks LIMIT 10")],
            [("eth", "_/eth_firehose@0.0.1")],
        )
        .await;

    //* Then
    assert_eq!(
        resp.status(),
        StatusCode::BAD_REQUEST,
        "schema resolution should fail with LIMIT clause"
    );

    let response: ErrorResponse = resp
        .json()
        .await
        .expect("failed to parse error response JSON");

    assert_eq!(
        response.error_code, "NON_INCREMENTAL_QUERY",
        "should return NON_INCREMENTAL_QUERY for LIMIT"
    );
}

struct TestCtx {
    _ctx: crate::testlib::ctx::TestCtx,
    client: reqwest::Client,
    schema_api_url: String,
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

        Self {
            _ctx: ctx,
            client,
            schema_api_url: format!("{}/schema", admin_api_url),
        }
    }

    async fn send_schema_request_with_tables_and_deps(
        &self,
        tables: impl IntoIterator<Item = (&str, &str)>,
        dependencies: impl IntoIterator<Item = (&str, &str)>,
    ) -> reqwest::Response {
        let tables_map: serde_json::Map<String, serde_json::Value> = tables
            .into_iter()
            .map(|(name, sql)| (name.to_string(), serde_json::Value::String(sql.to_string())))
            .collect();

        let deps_map: serde_json::Map<String, serde_json::Value> = dependencies
            .into_iter()
            .map(|(alias, reference)| {
                (
                    alias.to_string(),
                    serde_json::Value::String(reference.to_string()),
                )
            })
            .collect();

        let request_payload = serde_json::json!({
            "tables": tables_map,
            "dependencies": deps_map
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
struct ErrorResponse {
    error_code: String,
    error_message: String,
}

#[derive(Debug, serde::Deserialize)]
#[allow(dead_code)]
struct SchemaResponse {
    schemas: serde_json::Value,
}
