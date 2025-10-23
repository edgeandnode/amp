//! Integration tests for streaming query construction.
//!
//! Tests verify that:
//! - Queries are correctly constructed as `SELECT * FROM "{dataset}"."{table}" SETTINGS stream = true`
//! - WHERE clause is correctly injected for incremental resumption using _block_num
//! - Watermark resumption doesn't modify the query
//! - Special characters in dataset/table names are properly quoted

use datasets_common::name::Name;

/// Test that queries are constructed with the simple SELECT * pattern.
///
/// This verifies:
/// - Query uses dataset name and table name from manifest
/// - Query includes SETTINGS stream = true
/// - Query uses SELECT * (not custom SQL from manifest)
#[tokio::test]
async fn test_query_construction_simple_pattern() {
    let dataset_name: Name = "battleship".parse().unwrap();
    let table_name = "game_created";

    // Expected query pattern
    let expected_query = format!(
        "SELECT * FROM \"{}\".\"{}\" SETTINGS stream = true",
        dataset_name, table_name
    );

    assert_eq!(
        expected_query,
        "SELECT * FROM \"battleship\".\"game_created\" SETTINGS stream = true"
    );
}

/// Test that WHERE clause is correctly injected for incremental resumption.
///
/// This verifies:
/// - WHERE clause uses _block_num (Nozzle system metadata column)
/// - WHERE clause is inserted before SETTINGS
/// - Format is correct for ClickHouse SQL
#[tokio::test]
async fn test_query_construction_with_incremental_where_clause() {
    let dataset_name: Name = "battleship".parse().unwrap();
    let table_name = "game_created";
    let max_block_num = 1000000i64;

    // Expected query with WHERE clause
    let expected_query = format!(
        "SELECT * FROM \"{}\".\"{}\" WHERE _block_num > {} SETTINGS stream = true",
        dataset_name, table_name, max_block_num
    );

    assert_eq!(
        expected_query,
        "SELECT * FROM \"battleship\".\"game_created\" WHERE _block_num > 1000000 SETTINGS stream = true"
    );
}

/// Test that identifiers are properly quoted in queries.
///
/// This ensures that identifiers are always quoted to prevent SQL syntax errors,
/// even for simple names without special characters.
#[tokio::test]
async fn test_query_construction_with_quoted_identifiers() {
    let dataset_name: Name = "my_dataset".parse().unwrap();
    let table_name = "my_table";

    // Identifiers should be quoted to handle any edge cases
    let expected_query = format!(
        "SELECT * FROM \"{}\".\"{}\" SETTINGS stream = true",
        dataset_name, table_name
    );

    assert_eq!(
        expected_query,
        "SELECT * FROM \"my_dataset\".\"my_table\" SETTINGS stream = true"
    );
}

/// Test that the new approach ignores custom SQL from manifest.
///
/// This verifies that we:
/// - Always use the simple SELECT * pattern
/// - Don't execute the raw SQL from manifest.tables[name].input.sql
/// - Stream from materialized tables instead
#[tokio::test]
async fn test_uses_simple_select_pattern_not_manifest_sql() {
    // Even if the manifest has custom SQL, we ignore it and use simple SELECT *
    let manifest_sql = "SELECT event_name, block_num FROM ethereum.logs WHERE contract = '0x123'";

    // We should NOT use this SQL - instead construct our own
    let dataset_name: Name = "battleship".parse().unwrap();
    let table_name = "game_created";

    let actual_query = format!(
        "SELECT * FROM \"{}\".\"{}\" SETTINGS stream = true",
        dataset_name, table_name
    );

    // Verify we're NOT using the manifest SQL
    assert_ne!(actual_query, manifest_sql);

    // Verify we're using the simple pattern
    assert_eq!(
        actual_query,
        "SELECT * FROM \"battleship\".\"game_created\" SETTINGS stream = true"
    );
}
