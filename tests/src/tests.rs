use common::tracing_helpers;
use pretty_assertions::assert_str_eq;

use crate::test_support::{
    check_blocks, check_provider_file, load_sql_tests, run_query_on_fresh_server, SnapshotContext,
    SqlTestResult,
};
use metadata_db::KEEP_TEMP_DIRS;

#[tokio::test]
async fn evm_rpc_single_dump() {
    let dataset_name = "eth_rpc";
    check_provider_file("rpc_eth_mainnet.toml").await;
    tracing_helpers::register_logger();

    let blessed = SnapshotContext::blessed(&dataset_name).await.unwrap();

    // Check the dataset directly against the Firehose provider with `check_blocks`.
    check_blocks(dataset_name, 15_000_000, 15_000_000)
        .await
        .expect("blessed data differed from provider");

    // Now dump the dataset to a temporary directory and check it again against the blessed files.
    let temp_dump = SnapshotContext::temp_dump(
        &dataset_name,
        vec![],
        15_000_000,
        15_000_000,
        1,
        *KEEP_TEMP_DIRS,
    )
    .await
    .expect("temp dump failed");
    temp_dump.assert_eq(&blessed).await.unwrap();
}

#[tokio::test]
async fn eth_firehose_single_dump() {
    let dataset_name = "eth_firehose";
    check_provider_file("firehose_eth_mainnet.toml").await;
    tracing_helpers::register_logger();

    let blessed = SnapshotContext::blessed(&dataset_name).await.unwrap();

    // Check the dataset directly against the Firehose provider with `check_blocks`.
    check_blocks(dataset_name, 15_000_000, 15_000_000)
        .await
        .expect("blessed data differed from provider");
    // Now dump the dataset to a temporary directory and check it again against the blessed files.
    let temp_dump = SnapshotContext::temp_dump(
        &dataset_name,
        vec![],
        15_000_000,
        15_000_000,
        1,
        *KEEP_TEMP_DIRS,
    )
    .await
    .expect("temp dump failed");
    temp_dump.assert_eq(&blessed).await.unwrap();
}

#[tokio::test]
async fn sql_over_eth_firehose_dump() {
    let dataset_name = "sql_over_eth_firehose";
    tracing_helpers::register_logger();

    let blessed = SnapshotContext::blessed(&dataset_name).await.unwrap();

    // Now dump the dataset to a temporary directory and check blessed files against it.
    let temp_dump = SnapshotContext::temp_dump(
        &dataset_name,
        vec!["eth_firehose"],
        15_000_000,
        15_000_000,
        2,
        *KEEP_TEMP_DIRS,
    )
    .await
    .expect("temp dump failed");
    blessed.assert_eq(&temp_dump).await.unwrap();
}

#[tokio::test]
async fn sql_tests() {
    for test in load_sql_tests().unwrap() {
        let results = run_query_on_fresh_server(&test.query)
            .await
            .map_err(|e| format!("{e:?}"));
        match test.result {
            SqlTestResult::Success {
                results: expected_results,
            } => {
                let expected_results: serde_json::Value = serde_json::from_str(&expected_results)
                    .map_err(|e| {
                        format!(
                            "Failed to parse expected results for test \"{}\": {e:?}",
                            test.name,
                        )
                    })
                    .unwrap();
                let results = results.unwrap();
                assert_str_eq!(
                    results.to_string(),
                    expected_results.to_string(),
                    "SQL test \"{}\" failed: SQL query \"{}\" did not return the expected results, see sql-tests.yaml",
                    test.name, test.query,
                );
            }
            SqlTestResult::Failure { failure } => {
                let failure = failure.trim();
                let results = results.unwrap_err();
                if !results.to_string().contains(&failure) {
                    panic!(
                        "SQL test \"{}\" failed: SQL query \"{}\" did not return the expected error, got \"{}\", expected \"{}\"",
                        test.name, test.query, results, failure,
                    );
                }
            }
        }
    }
}
