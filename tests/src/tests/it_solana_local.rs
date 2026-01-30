//! Integration tests for Solana local extraction via Surfpool.
//!
//! These tests verify that the Amp ETL pipeline can extract Solana blockchain data
//! from a local Surfpool node (backed by LiteSVM). This proves end-to-end Solana
//! extraction without requiring any external network connectivity.

use std::time::Duration;

use datasets_common::reference::Reference;
use monitoring::logging;

use crate::testlib::{ctx::TestCtxBuilder, fixtures::Surfpool, helpers as test_helpers};

#[tokio::test(flavor = "multi_thread")]
async fn solana_local_extraction_produces_data_in_all_tables() {
    logging::init();

    if !Surfpool::is_available() {
        eprintln!(
            "SKIPPED: surfpool binary not found in PATH. Install with: brew install txtx/taps/surfpool (macOS) or from source."
        );
        return;
    }

    //* Given — a test environment with Surfpool and Solana manifest
    let test_ctx = TestCtxBuilder::new("solana_local")
        .with_dataset_manifest("solana_local")
        .with_surfpool()
        .build()
        .await
        .expect("Failed to build test environment");

    let surfpool = test_ctx.surfpool();

    // Fund a test keypair and send SOL transfers to create on-chain activity.
    // Each transfer produces rows in all 4 Solana tables: block_headers,
    // transactions, messages, and instructions.
    let sender = surfpool
        .fund_new_keypair()
        .expect("Failed to fund sender keypair");
    let recipient = solana_sdk::pubkey::Pubkey::new_unique();

    for _ in 0..3 {
        surfpool
            .send_sol_transfer(&sender, &recipient, 100_000)
            .expect("Failed to send SOL transfer");
    }

    // Wait for Surfpool to finalize blocks containing our transactions
    tokio::time::sleep(Duration::from_secs(2)).await;

    let end_slot = surfpool.latest_slot().expect("Failed to get latest slot");
    assert!(end_slot > 0, "Surfpool should have advanced past slot 0");

    //* When — deploy extraction job and wait for completion
    let ampctl = test_ctx.new_ampctl();
    let dataset_ref: Reference = "_/solana_local@0.0.0"
        .parse()
        .expect("valid dataset reference");

    let job_info = test_helpers::deploy_and_wait(
        &ampctl,
        &dataset_ref,
        Some(end_slot),
        Duration::from_secs(60),
    )
    .await
    .expect("Failed to deploy and wait for Solana extraction job");

    assert_eq!(
        job_info.status, "COMPLETED",
        "Solana extraction job should complete successfully, got status: {}",
        job_info.status
    );

    //* Then — query extracted data and verify all 4 tables have rows
    let jsonl_client = test_ctx.new_jsonl_client();

    let block_headers: Vec<serde_json::Value> = jsonl_client
        .query("SELECT COUNT(*) as count FROM solana_local.block_headers")
        .await
        .expect("Failed to query block_headers");
    let block_count = block_headers[0]["count"]
        .as_i64()
        .expect("count should be i64");
    assert!(
        block_count > 0,
        "block_headers table should have rows, got {block_count}"
    );

    let transactions: Vec<serde_json::Value> = jsonl_client
        .query("SELECT COUNT(*) as count FROM solana_local.transactions")
        .await
        .expect("Failed to query transactions");
    let tx_count = transactions[0]["count"]
        .as_i64()
        .expect("count should be i64");
    assert!(
        tx_count > 0,
        "transactions table should have rows, got {tx_count}"
    );

    let messages: Vec<serde_json::Value> = jsonl_client
        .query("SELECT COUNT(*) as count FROM solana_local.messages")
        .await
        .expect("Failed to query messages");
    let msg_count = messages[0]["count"].as_i64().expect("count should be i64");
    assert!(
        msg_count > 0,
        "messages table should have rows, got {msg_count}"
    );

    let instructions: Vec<serde_json::Value> = jsonl_client
        .query("SELECT COUNT(*) as count FROM solana_local.instructions")
        .await
        .expect("Failed to query instructions");
    let instr_count = instructions[0]["count"]
        .as_i64()
        .expect("count should be i64");
    assert!(
        instr_count > 0,
        "instructions table should have rows, got {instr_count}"
    );

    tracing::info!(
        block_count,
        tx_count,
        msg_count,
        instr_count,
        end_slot,
        "Solana local extraction verified: all 4 tables populated"
    );
}
