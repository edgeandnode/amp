use arrow_flight::FlightData;
use common::BlockRange;
use monitoring::logging;
use serde::Deserialize;
use tokio::sync::mpsc;

use crate::testlib::{ctx::TestCtxBuilder, helpers as test_helpers};

/// Test batch (non-streaming) JOIN query across two datasets with different networks.
#[tokio::test]
async fn multi_network_batch_join() {
    logging::init();

    // Create test environment with two datasets from different networks
    let test_ctx = TestCtxBuilder::new("multi_network_batch_join")
        .with_dataset_manifests(["eth_rpc", "base_rpc"])
        .with_dataset_snapshots(["eth_rpc", "base_rpc"])
        .build()
        .await
        .expect("Failed to create test environment");

    // Restore both dataset snapshots (indexes files into metadata DB)
    let ampctl = test_ctx.new_ampctl();
    let dataset_store = test_ctx.daemon_controller().dataset_store();
    let data_store = test_ctx.daemon_server().data_store();

    let eth_rpc_ref = "_/eth_rpc@0.0.0".parse().expect("Valid reference");
    let base_rpc_ref = "_/base_rpc@0.0.0".parse().expect("Valid reference");

    test_helpers::restore_dataset_snapshot(&ampctl, dataset_store, data_store, &eth_rpc_ref)
        .await
        .expect("Failed to restore eth_rpc snapshot");

    test_helpers::restore_dataset_snapshot(&ampctl, dataset_store, data_store, &base_rpc_ref)
        .await
        .expect("Failed to restore base_rpc snapshot");

    let mut client = test_ctx
        .new_flight_client()
        .await
        .expect("Failed to connect FlightClient");

    // Execute cross-network JOIN query
    // eth_rpc is on mainnet, base_rpc is on base network
    let query = r#"
        SELECT 
            e.block_num as eth_block,
            b.block_num as base_block
        FROM eth_rpc.blocks e
        CROSS JOIN base_rpc.blocks b
        WHERE e.block_num = 15000000 
          AND b.block_num = 33411770
        LIMIT 1
    "#;

    // Execute query and verify results
    let (results, batch_count) = client
        .run_query(query, None)
        .await
        .expect("Failed to execute query");

    // We may get multiple batches (data + completion), but should have exactly 1 row total
    assert!(
        batch_count > 0,
        "Expected at least one batch, got: {}",
        batch_count
    );

    // Parse results and verify values
    let results_array = results
        .as_array()
        .expect("Results should be an array of rows");

    assert_eq!(results_array.len(), 1, "Expected exactly one result row");

    let row = &results_array[0];
    let eth_block = row["eth_block"].as_u64().expect("eth_block should be u64");
    let base_block = row["base_block"]
        .as_u64()
        .expect("base_block should be u64");

    assert_eq!(eth_block, 15000000, "eth_block should be 15000000");
    assert_eq!(base_block, 33411770, "base_block should be 33411770");

    // Now verify metadata contains ranges for both networks
    let mut metadata_rx = client
        .execute_with_metadata_stream(query)
        .await
        .expect("Failed to execute query with metadata stream");

    let ranges = pull_flight_metadata(&mut metadata_rx).await;

    // We should have ranges for both networks
    assert_eq!(
        ranges.len(),
        2,
        "Should have block ranges for both networks, got: {:?}",
        ranges
    );

    // Verify we have ranges for both networks (order may vary)
    let networks: Vec<&str> = ranges.iter().map(|r| r.network.as_str()).collect();

    assert!(
        networks.contains(&"mainnet"),
        "Should have range for mainnet network, got: {:?}",
        networks
    );
    assert!(
        networks.contains(&"base"),
        "Should have range for base network, got: {:?}",
        networks
    );

    // Verify each range has valid block numbers
    for range in &ranges {
        assert!(
            range.start() > 0,
            "Range for network {} should have start > 0",
            range.network
        );
        assert!(
            range.end() >= range.start(),
            "Range for network {} should have end >= start",
            range.network
        );
    }
}

/// Extract BlockRange metadata from FlightData app_metadata field on batch completion.
///
/// Returns None if the FlightData contains no metadata, parsing fails, or ranges_complete is false.
fn extract_block_ranges(data: &FlightData) -> Option<Vec<BlockRange>> {
    if data.app_metadata.is_empty() {
        return None;
    }

    #[derive(Deserialize)]
    struct Metadata {
        ranges: Vec<BlockRange>,
        ranges_complete: bool,
    }

    let metadata: Metadata = match serde_json::from_slice(&data.app_metadata) {
        Ok(m) => m,
        Err(e) => {
            tracing::warn!(
                "Failed to parse metadata: {}, raw: {}",
                e,
                String::from_utf8_lossy(&data.app_metadata)
            );
            return None;
        }
    };

    tracing::debug!(
        "Extracted metadata: ranges={}, ranges_complete={}",
        metadata.ranges.len(),
        metadata.ranges_complete
    );

    // Only return ranges when ranges_complete is true (final message)
    if metadata.ranges_complete {
        Some(metadata.ranges)
    } else {
        None
    }
}

/// Pull and extract all metadata from a flight stream.
///
/// Drains the receiver and returns all BlockRange metadata found.
async fn pull_flight_metadata(rx: &mut mpsc::UnboundedReceiver<FlightData>) -> Vec<BlockRange> {
    let mut buffer = Vec::new();

    // Wait for the stream to close (all messages received)
    while let Some(data) = rx.recv().await {
        buffer.push(data);
    }

    // For batch queries, we expect ranges in the final message with ranges_complete: true
    buffer
        .into_iter()
        .filter_map(|data| extract_block_ranges(&data))
        .flatten()
        .collect()
}
