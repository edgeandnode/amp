use std::time::Duration;

use common::tracing_helpers;
use metadata_db::KEEP_TEMP_DIRS;

use crate::test_support::{
    assert_sql_test_result, check_blocks, check_provider_file, load_sql_tests,
    run_query_on_fresh_server, DumpTestDatasetCommand, SnapshotContext, StreamingExecutionOptions,
};

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
    for test in load_sql_tests("sql-tests.yaml").unwrap() {
        let results = run_query_on_fresh_server(&test.query, vec![], vec![], None)
            .await
            .map_err(|e| format!("{e:?}"));
        assert_sql_test_result(&test, results);
    }
}

#[tokio::test]
async fn streaming_tests() {
    for test in load_sql_tests("sql-streaming-tests.yaml").unwrap() {
        let results = run_query_on_fresh_server(
            &test.query,
            vec![],
            vec![],
            Some(StreamingExecutionOptions {
                max_duration: Duration::from_secs(10),
                at_least_rows: 1,
            }),
        )
        .await
        .map_err(|e| format!("{e:?}"));

        assert_sql_test_result(&test, results);
    }
}

#[tokio::test]
async fn streaming_tests_with_additional_dumps() {
    let initial_dumps = vec![
        DumpTestDatasetCommand {
            dataset_name: "eth_firehose_stream".to_string(),
            dependencies: vec![],
            start: 15_000_000,
            end: 15_000_001,
            n_jobs: 1,
        },
        DumpTestDatasetCommand {
            dataset_name: "sql_stream_ds".to_string(),
            dependencies: vec![],
            start: 15_000_001,
            end: 15_000_001,
            n_jobs: 1,
        },
    ];
    let dumps_on_running_server = vec![
        DumpTestDatasetCommand {
            dataset_name: "eth_firehose_stream".to_string(),
            dependencies: vec![],
            start: 15_000_002,
            end: 15_000_002,
            n_jobs: 1,
        },
        DumpTestDatasetCommand {
            dataset_name: "eth_firehose_stream".to_string(),
            dependencies: vec![],
            start: 15_000_002,
            end: 15_000_004,
            n_jobs: 1,
        },
        DumpTestDatasetCommand {
            dataset_name: "sql_stream_ds".to_string(),
            dependencies: vec![],
            start: 15_000_002,
            end: 15_000_004,
            n_jobs: 1,
        },
    ];

    for test in load_sql_tests("sql-complex-streaming-tests.yaml").unwrap() {
        let results = run_query_on_fresh_server(
            &test.query,
            initial_dumps.clone(),
            dumps_on_running_server.clone(),
            Some(StreamingExecutionOptions {
                max_duration: Duration::from_secs(10),
                at_least_rows: 4,
            }),
        )
        .await
        .map_err(|e| format!("{e:?}"));

        assert_sql_test_result(&test, results);
    }
}
