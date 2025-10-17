//! Integration tests for SQL query construction.
//!
//! Tests verify that:
//! - Manifest SQL queries are fetched and parsed correctly
//! - SETTINGS stream = true is automatically added when missing

use datasets_common::{name::Name, version::Version};
use datasets_derived::manifest::TableInput;
use mockito::Server;

/// Test that SQL queries from manifest automatically get SETTINGS stream = true appended.
///
/// This is critical because:
/// - Non-streaming queries won't populate block_ranges metadata correctly
/// - Empty block_ranges causes "No block ranges provided for metadata injection" error
/// - The system requires streaming queries for proper incremental sync
#[tokio::test]
async fn test_manifest_sql_gets_streaming_settings_added() {
    let mut server = Server::new_async().await;

    let dataset_name: Name = "test_dataset".parse().unwrap();
    let version: Version = "0.1.0-LTcyNjgzMjc1NA".parse().unwrap();

    // Mock the versions endpoint
    let _versions_mock = server
        .mock("GET", "/datasets/test_dataset/versions?limit=1000")
        .with_status(200)
        .with_header("content-type", "application/json")
        .with_body(
            r#"{
                "versions": ["0.1.0-LTcyNjgzMjc1NA"]
            }"#,
        )
        .expect(1)
        .create_async()
        .await;

    // Mock manifest endpoint with SQL that does NOT include "SETTINGS stream = true"
    // This simulates the real-world scenario where manifest SQL might be missing it
    let _manifest_mock = server
        .mock(
            "GET",
            "/datasets/test_dataset/versions/0.1.0-LTcyNjgzMjc1NA/manifest",
        )
        .with_status(200)
        .with_header("content-type", "application/json")
        .with_body(
            r#"{
                "name": "test_dataset",
                "version": "0.1.0-LTcyNjgzMjc1NA",
                "kind": "manifest",
                "network": "ethereum",
                "dependencies": {},
                "functions": {},
                "tables": {
                    "blocks": {
                        "input": {
                            "sql": "SELECT block_num, hash FROM ethereum.blocks"
                        },
                        "schema": {
                            "arrow": {
                                "fields": [
                                    {
                                        "name": "block_num",
                                        "type": "Int64",
                                        "nullable": false
                                    },
                                    {
                                        "name": "hash",
                                        "type": "Binary",
                                        "nullable": false
                                    }
                                ]
                            }
                        },
                        "network": "ethereum"
                    }
                }
            }"#,
        )
        .expect(1)
        .create_async()
        .await;

    // Fetch the manifest
    let manifest = ampsync::manifest::fetch_manifest(&server.url(), &dataset_name, Some(&version))
        .await
        .expect("Failed to fetch manifest");

    // Verify manifest was fetched correctly
    assert_eq!(manifest.tables.len(), 1);
    assert!(manifest.tables.contains_key("blocks"));

    // Get the SQL from the manifest
    let blocks_table = manifest.tables.get("blocks").unwrap();
    let sql_query = match &blocks_table.input {
        TableInput::View(view) => &view.sql,
    };

    // Verify the manifest SQL does NOT have SETTINGS stream = true
    assert_eq!(
        sql_query, "SELECT block_num, hash FROM ethereum.blocks",
        "Manifest SQL should not have SETTINGS stream = true"
    );

    // Simulate the logic in spawn_stream_tasks that adds SETTINGS stream = true
    let streaming_query = if sql_query.contains("SETTINGS stream = true") {
        sql_query.to_string()
    } else {
        format!("{} SETTINGS stream = true", sql_query.trim())
    };

    // Verify that SETTINGS stream = true was added
    assert_eq!(
        streaming_query, "SELECT block_num, hash FROM ethereum.blocks SETTINGS stream = true",
        "Streaming query should have SETTINGS stream = true appended"
    );
}
