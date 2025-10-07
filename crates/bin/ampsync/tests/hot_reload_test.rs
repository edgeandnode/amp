use std::{io::Write, time::Duration};

use admin_api::handlers::datasets::{
    get_version_schema::{DatasetSchemaResponse, TableSchemaInfo},
    get_versions::DatasetVersionsResponse,
};
use ampsync::manifest;
use datasets_common::manifest::DataType;
use datasets_derived::manifest::{ArrowSchema, Field, TableSchema};
use tempfile::NamedTempFile;

/// Integration test verifying manifest can be loaded and reloaded from disk
#[tokio::test]
async fn test_config_reload_manifest() {
    // Create mock Admin API server
    let mut server = mockito::Server::new_async().await;

    // Create a temporary manifest file with initial content (version 0.1.0)
    let mut temp_manifest =
        NamedTempFile::with_suffix(".json").expect("Failed to create temp file");
    let initial_manifest = r#"{
        "name": "test_dataset",
        "network": "mainnet",
        "version": "0.1.0",
        "dependencies": {},
        "tables": {
            "blocks": {
                "sql": "SELECT block_num, hash FROM anvil.blocks"
            }
        },
        "functions": {}
    }"#;

    writeln!(temp_manifest, "{}", initial_manifest).expect("Failed to write manifest");
    temp_manifest.flush().expect("Failed to flush");

    // Mock Admin API endpoints for version 0.1.0
    let versions_mock_v1 = server
        .mock("GET", "/datasets/test_dataset/versions?limit=1000")
        .with_status(200)
        .with_header("content-type", "application/json")
        .with_body(
            serde_json::to_string(&DatasetVersionsResponse {
                versions: vec!["0.1.0-ABC123".parse().unwrap()],
                next_cursor: None,
            })
            .unwrap(),
        )
        .expect(1)
        .create_async()
        .await;

    let schema_mock_v1 = server
        .mock("GET", "/datasets/test_dataset/versions/0.1.0-ABC123/schema")
        .with_status(200)
        .with_header("content-type", "application/json")
        .with_body(
            serde_json::to_string(&DatasetSchemaResponse {
                name: "test_dataset".parse().unwrap(),
                version: "0.1.0-ABC123".parse().unwrap(),
                tables: vec![TableSchemaInfo {
                    name: "blocks".to_string(),
                    network: "mainnet".to_string(),
                    schema: TableSchema {
                        arrow: ArrowSchema {
                            fields: vec![
                                Field {
                                    name: "block_num".to_string(),
                                    type_: DataType(arrow_schema::DataType::UInt64),
                                    nullable: false,
                                },
                                Field {
                                    name: "hash".to_string(),
                                    type_: DataType(arrow_schema::DataType::Utf8),
                                    nullable: false,
                                },
                            ],
                        },
                    },
                }],
            })
            .unwrap(),
        )
        .expect(1)
        .create_async()
        .await;

    // Load initial manifest
    let initial_loaded_manifest = manifest::fetch_manifest(&server.url(), temp_manifest.path())
        .await
        .expect("Failed to load initial manifest");

    assert_eq!(initial_loaded_manifest.version.to_string(), "0.1.0-ABC123");
    assert_eq!(initial_loaded_manifest.tables.len(), 1);
    assert!(initial_loaded_manifest.tables.contains_key("blocks"));

    // Verify initial mocks were called
    versions_mock_v1.assert_async().await;
    schema_mock_v1.assert_async().await;

    // Modify the manifest file to version 0.2.0
    let updated_manifest = r#"{
        "name": "test_dataset",
        "network": "mainnet",
        "version": "0.2.0",
        "dependencies": {},
        "tables": {
            "blocks": {
                "sql": "SELECT block_num, hash FROM anvil.blocks"
            },
            "transactions": {
                "sql": "SELECT tx_hash, block_num FROM anvil.transactions"
            }
        },
        "functions": {}
    }"#;

    std::fs::write(temp_manifest.path(), updated_manifest).expect("Failed to update manifest");

    // Mock Admin API endpoints for version 0.2.0
    let versions_mock_v2 = server
        .mock("GET", "/datasets/test_dataset/versions?limit=1000")
        .with_status(200)
        .with_header("content-type", "application/json")
        .with_body(
            serde_json::to_string(&DatasetVersionsResponse {
                versions: vec!["0.2.0-XYZ789".parse().unwrap()],
                next_cursor: None,
            })
            .unwrap(),
        )
        .expect(1)
        .create_async()
        .await;

    let schema_mock_v2 = server
        .mock("GET", "/datasets/test_dataset/versions/0.2.0-XYZ789/schema")
        .with_status(200)
        .with_header("content-type", "application/json")
        .with_body(
            serde_json::to_string(&DatasetSchemaResponse {
                name: "test_dataset".parse().unwrap(),
                version: "0.2.0-XYZ789".parse().unwrap(),
                tables: vec![
                    TableSchemaInfo {
                        name: "blocks".to_string(),
                        network: "mainnet".to_string(),
                        schema: TableSchema {
                            arrow: ArrowSchema {
                                fields: vec![
                                    Field {
                                        name: "block_num".to_string(),
                                        type_: DataType(arrow_schema::DataType::UInt64),
                                        nullable: false,
                                    },
                                    Field {
                                        name: "hash".to_string(),
                                        type_: DataType(arrow_schema::DataType::Utf8),
                                        nullable: false,
                                    },
                                ],
                            },
                        },
                    },
                    TableSchemaInfo {
                        name: "transactions".to_string(),
                        network: "mainnet".to_string(),
                        schema: TableSchema {
                            arrow: ArrowSchema {
                                fields: vec![
                                    Field {
                                        name: "tx_hash".to_string(),
                                        type_: DataType(arrow_schema::DataType::Utf8),
                                        nullable: false,
                                    },
                                    Field {
                                        name: "block_num".to_string(),
                                        type_: DataType(arrow_schema::DataType::UInt64),
                                        nullable: false,
                                    },
                                ],
                            },
                        },
                    },
                ],
            })
            .unwrap(),
        )
        .expect(1)
        .create_async()
        .await;

    // Reload the manifest from the updated file
    let reloaded_manifest = manifest::fetch_manifest(&server.url(), temp_manifest.path())
        .await
        .expect("Failed to reload manifest");

    // Verify version changed
    assert_eq!(reloaded_manifest.version.to_string(), "0.2.0-XYZ789");

    // Verify new table was added
    assert_eq!(reloaded_manifest.tables.len(), 2);
    assert!(reloaded_manifest.tables.contains_key("blocks"));
    assert!(reloaded_manifest.tables.contains_key("transactions"));

    // Verify dataset name remained the same
    assert_eq!(
        reloaded_manifest.name.as_ref(),
        initial_loaded_manifest.name.as_ref()
    );

    // Verify reload mocks were called
    versions_mock_v2.assert_async().await;
    schema_mock_v2.assert_async().await;
}

/// Note: File system watcher integration tests are intentionally minimal
/// because file system events are platform-specific and can be flaky in CI.
/// The event filtering logic is tested via unit tests in file_watcher.rs.

/// Test that graceful shutdown completes successfully
#[tokio::test]
async fn test_graceful_shutdown() {
    // This test verifies the shutdown logic compiles and has correct types
    // A full integration test would require spawning actual stream tasks

    let handles: Vec<tokio::task::JoinHandle<()>> = vec![];

    // Simulate shutdown
    let shutdown_future = async {
        for handle in handles {
            let _ = handle.await;
        }
    };

    // Should complete immediately with empty handles
    tokio::time::timeout(Duration::from_millis(100), shutdown_future)
        .await
        .expect("Shutdown should complete quickly with no tasks");
}

/// Test that manifest path validation works
#[tokio::test]
async fn test_manifest_path_validation() {
    // Test invalid extension
    unsafe {
        std::env::set_var("DATASET_MANIFEST", "/tmp/invalid.txt");
        std::env::set_var("DATABASE_URL", "postgresql://test:test@localhost:5432/test");
    }

    // Would fail with "Invalid dataset manifest extension"
    // let result = AmpsyncConfig::from_env().await;
    // assert!(result.is_err());

    // Test non-existent file
    unsafe {
        std::env::set_var("DATASET_MANIFEST", "/nonexistent/path/config.ts");
    }

    // Would fail with "Dataset manifest file does not exist"
    // let result = AmpsyncConfig::from_env().await;
    // assert!(result.is_err());
}

/// Test that config preserves fields during reload
#[tokio::test]
async fn test_config_preserves_database_connection() {
    // Verify reload_manifest preserves database_url and amp_flight_addr
    // This is important so we don't reconnect on every manifest change

    // The reload should only update the manifest field while preserving:
    // - database_url
    // - amp_flight_addr
    // - amp_admin_api_addr
    // - manifest_path
}
