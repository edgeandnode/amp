//! Integratiohttps://x.com/dok2001/status/1996872325678223609?s=20n tests for eth_call UDF with smart contract interaction.
//!
//! This module tests the `eth_call` UDF by deploying a Counter smart contract to Anvil,
//! incrementing it multiple times, indexing the blockchain data with amp, and then
//! querying the counter value using SQL with the eth_call UDF.

use std::time::Duration;

use alloy::hex;
use monitoring::logging;
use serde::Deserialize;

use crate::testlib::{
    ctx::{TestCtx, TestCtxBuilder},
    fixtures::DatasetPackage,
};

#[tokio::test]
async fn eth_call_reads_counter_value_after_increments() {
    //* Given
    let test = EthCallTestCtx::setup("eth_call_reads_counter_value").await;
    let cli = test.ctx.new_amp_cli();
    let ampctl = test.ctx.new_ampctl();

    // Deploy anvil_rpc dataset (dependency for amp_demo) using ampctl
    ampctl
        .dataset_deploy("_/anvil_rpc@0.0.0", None, Some(1), None)
        .await
        .expect("Failed to deploy anvil_rpc dataset");

    // Build contracts and deploy to Anvil
    let amp_demo_package = DatasetPackage::builder("amp_demo")
        .contracts_dir("contracts")
        .build();
    let artifacts = amp_demo_package
        .build_contracts(["Counter"])
        .await
        .expect("Failed to build contracts");
    let counter_artifact = &artifacts[0];

    let deployment = test
        .ctx
        .anvil()
        .deploy_contract(counter_artifact)
        .await
        .expect("Failed to deploy Counter contract");

    tracing::info!(
        address = %deployment.address,
        tx_hash = %deployment.tx_hash,
        "Counter contract deployed"
    );

    // Mine a block so contract is deployed
    test.mine(1).await;

    // Register and deploy amp_demo dataset
    amp_demo_package
        .register(&cli, "0.0.1")
        .await
        .expect("Failed to register amp_demo dataset");
    amp_demo_package
        .deploy(&cli, Some("edgeandnode/amp_demo@0.0.1"), None)
        .await
        .expect("Failed to deploy amp_demo dataset");

    // Increment counter 3 times
    let increment_selector = counter_artifact
        .function_selector("increment")
        .expect("Failed to get increment selector");

    for i in 1..=3 {
        test.ctx
            .anvil()
            .send_transaction(deployment.address, increment_selector.clone())
            .await
            .expect("Failed to send increment transaction");

        tracing::info!(iteration = i, "Counter incremented");
    }

    // Mine blocks for values to be picked up
    test.mine(3).await;

    let final_block = test
        .ctx
        .anvil()
        .latest_block()
        .await
        .expect("Failed to get latest block")
        .block_num;

    // Wait for dump job to process the new blocks
    tokio::time::sleep(Duration::from_secs(1)).await;

    //* When

    // Query 1: Get counter value using eth_call UDF
    let count_selector = counter_artifact
        .function_selector_hex("count")
        .expect("Failed to get count selector");
    let null_addr_hex = "0000000000000000000000000000000000000000";
    let contract_addr_hex = hex::encode(deployment.address.as_slice());
    let eth_call_query = indoc::formatdoc! {r#"
        WITH call_result AS (
            SELECT "_/anvil_rpc@0.0.0".eth_call(
                arrow_cast(decode('{null_addr_hex}', 'hex'), 'FixedSizeBinary(20)'),
                arrow_cast(decode('{contract_addr_hex}', 'hex'), 'FixedSizeBinary(20)'),
                decode('{count_selector}', 'hex'),
                '{final_block}'
            ) as result
        )
        SELECT evm_decode_type(result['data'], 'uint64') as counter_value
        FROM call_result
    "#};
    let eth_call_results: Vec<EthCallRow> = test.send_flight_query(&eth_call_query).await;

    // Query 2: Get counter value from dataset tables
    let dataset_query = indoc::formatdoc! {r#"
        SELECT CAST(count AS BIGINT) as counter_value
        FROM "edgeandnode/amp_demo@0.0.1".incremented
        WHERE address = arrow_cast(decode('{contract_addr_hex}', 'hex'), 'FixedSizeBinary(20)')
          AND block_num <= {final_block}
        ORDER BY block_num DESC
        LIMIT 1
    "#};
    let dataset_results: Vec<DatasetCounterRow> = test.send_flight_query(&dataset_query).await;

    //* Then

    // Assert eth_call query successful and value is 3
    assert_eq!(
        eth_call_results.len(),
        1,
        "eth_call query should return exactly one row"
    );

    assert_eq!(
        eth_call_results[0].counter_value, 3,
        "Counter value from eth_call should be 3 after 3 increments"
    );

    tracing::info!(
        counter_value = eth_call_results[0].counter_value,
        "Successfully read counter value via eth_call"
    );

    // Assert dataset query successful and value is 3
    assert_eq!(
        dataset_results.len(),
        1,
        "Dataset query should return exactly one row"
    );

    assert_eq!(
        dataset_results[0].counter_value, 3,
        "Counter value from dataset should be 3 (last Incremented event)"
    );

    tracing::info!(
        counter_value = dataset_results[0].counter_value,
        "Successfully read counter value from dataset"
    );
}

/// Test context wrapper providing convenient helpers for eth_call tests.
struct EthCallTestCtx {
    ctx: TestCtx,
}

impl EthCallTestCtx {
    /// Set up a new test context with Anvil and the amp_demo dataset.
    async fn setup(test_name: &str) -> Self {
        logging::init();

        let ctx = TestCtxBuilder::new(test_name)
            .with_anvil_ipc()
            .with_dataset_manifest("anvil_rpc")
            .build()
            .await
            .expect("Failed to create test context");

        Self { ctx }
    }

    /// Mine the specified number of blocks using the Anvil fixture.
    async fn mine(&self, blocks: u64) {
        self.ctx
            .anvil()
            .mine(blocks)
            .await
            .expect("Failed to mine blocks");
    }

    /// Query using Flight client and deserialize results.
    async fn send_flight_query<T>(&self, sql: &str) -> Vec<T>
    where
        T: for<'de> Deserialize<'de> + std::fmt::Debug,
    {
        let mut flight_client = self
            .ctx
            .new_flight_client()
            .await
            .expect("Failed to create flight client");
        let (json_value, _batch_count) = flight_client
            .run_query(sql, None)
            .await
            .expect("Failed to execute query");

        serde_json::from_value(json_value).expect("Failed to deserialize query results")
    }
}

/// Response structure for eth_call query results.
#[derive(Debug, Deserialize)]
struct EthCallRow {
    counter_value: i64,
}

/// Response structure for dataset counter query results.
#[derive(Debug, Deserialize)]
struct DatasetCounterRow {
    counter_value: i64,
}
