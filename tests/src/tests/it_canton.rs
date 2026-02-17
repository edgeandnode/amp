//! Integration tests for the Canton Scan extractor.
//!
//! These tests verify that the canton-scan extractor can:
//! 1. Load data from CSV files via the csv_loader module
//! 2. Register manifests correctly via Admin API
//! 3. Validate schemas via the Admin API schema endpoint

use std::path::PathBuf;

use monitoring::logging;
use reqwest::StatusCode;

use crate::testlib::ctx::TestCtxBuilder;

/// Get the path to the Canton data samples directory.
fn canton_data_samples_dir() -> PathBuf {
    // The data samples are in the canton-network-validator sibling directory
    let workspace_root = std::env::var("CARGO_MANIFEST_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("."));

    // Navigate from amp/tests to canton-network-validator/data-samples
    workspace_root
        .parent() // amp
        .unwrap()
        .parent() // canton workspace
        .unwrap()
        .join("canton-network-validator")
        .join("data-samples")
}

/// Test that the canton_scan manifest can be registered.
#[tokio::test]
async fn canton_scan_manifest_registration() {
    logging::init();

    // Create a test context with the canton_scan manifest
    let _test_ctx = TestCtxBuilder::new("canton_scan_manifest_registration")
        .with_dataset_manifest("canton_scan")
        .with_provider_config("canton_csv")
        .build()
        .await
        .expect("Failed to create test environment");

    // If we get here, the manifest was successfully registered
    tracing::info!("Canton scan manifest registered successfully");
}

/// Test that we can validate queries against the Canton dataset schema.
#[tokio::test]
async fn canton_scan_schema_query() {
    logging::init();

    let test_ctx = TestCtxBuilder::new("canton_scan_schema_query")
        .with_dataset_manifest("canton_scan")
        .with_provider_config("canton_csv")
        .build()
        .await
        .expect("Failed to create test environment");

    // Use the Admin API schema endpoint to validate a query
    let client = reqwest::Client::new();
    let admin_api_url = test_ctx.daemon_controller().admin_api_url();
    let schema_api_url = format!("{}/schema", admin_api_url);

    // Query that references the transactions table
    let request_payload = serde_json::json!({
        "tables": {
            "txs": "SELECT offset, update_id, record_time FROM canton.transactions"
        },
        "dependencies": {
            "canton": "_/canton_scan@0.0.0"
        }
    });

    let resp = client
        .post(&schema_api_url)
        .json(&request_payload)
        .send()
        .await
        .expect("Failed to send schema validation request");

    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "Schema validation should succeed for canton_scan transactions query"
    );

    let response: serde_json::Value = resp.json().await.expect("Failed to parse response");
    tracing::info!("Schema validation response: {:?}", response);

    // Verify the response contains schema info for our table
    assert!(
        response.get("schemas").is_some(),
        "Response should contain schemas"
    );
}

// Unit test for CSV loader (doesn't require full test infrastructure)
#[cfg(test)]
mod csv_loader_tests {
    use super::*;
    use canton_scan_datasets::csv_loader;

    #[test]
    fn test_load_sample_transactions() {
        let data_dir = canton_data_samples_dir();
        let transactions_path = data_dir.join("01_transactions.csv");

        if !transactions_path.exists() {
            tracing::warn!(
                "Skipping CSV loader test - data file not found at {:?}",
                transactions_path
            );
            return;
        }

        let transactions = csv_loader::load_transactions(&transactions_path)
            .expect("Failed to load transactions CSV");

        assert!(!transactions.is_empty(), "Should have loaded some transactions");
        tracing::info!("Loaded {} transactions from CSV", transactions.len());

        // Verify first transaction has expected structure
        let first = &transactions[0];
        assert!(!first.update_id.is_empty(), "update_id should not be empty");
        assert!(
            !first.synchronizer_id.is_empty(),
            "synchronizer_id should not be empty"
        );
    }

    #[test]
    fn test_load_sample_mining_rounds() {
        let data_dir = canton_data_samples_dir();
        let mining_rounds_path = data_dir.join("05_mining_rounds.csv");

        if !mining_rounds_path.exists() {
            tracing::warn!(
                "Skipping mining rounds test - data file not found at {:?}",
                mining_rounds_path
            );
            return;
        }

        let rounds = csv_loader::load_mining_rounds(&mining_rounds_path)
            .expect("Failed to load mining rounds CSV");

        assert!(!rounds.is_empty(), "Should have loaded some mining rounds");
        tracing::info!("Loaded {} mining rounds from CSV", rounds.len());

        // Mining rounds should have sequential round numbers
        for (i, round) in rounds.iter().enumerate() {
            assert_eq!(
                round.round_number, i as i64,
                "Round numbers should be sequential starting at 0"
            );
        }
    }
}
