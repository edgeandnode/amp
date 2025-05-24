use std::{str::FromStr as _, sync::Arc};

use common::{tracing_helpers, BoxError};
use dump::worker::Worker;
use metadata_db::{workers::WorkerNodeId, KEEP_TEMP_DIRS};
use tokio::sync::broadcast;

use crate::test_support::{
    check_blocks, check_provider_file, load_sql_tests, load_test_config, run_query_on_fresh_server,
    DatasetPackage, SnapshotContext,
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
        test.assert_result_eq(results);
    }
}

#[tokio::test]
async fn streaming_tests() {
    for test in load_sql_tests("sql-streaming-tests.yaml").unwrap() {
        let results = run_query_on_fresh_server(
            &test.query,
            test.initial_dumps.clone(),
            test.dumps_on_running_server.clone(),
            test.streaming_options.as_ref(),
        )
        .await
        .map_err(|e| format!("{e:?}"));

        test.assert_result_eq(results);
    }
}

#[tokio::test]
async fn basic_function() -> Result<(), BoxError> {
    tracing_helpers::register_logger();

    let config = load_test_config(None).await.unwrap();

    let metadata_db = Arc::new(config.metadata_db().await?);
    let (tx, rx) = broadcast::channel(1);
    std::mem::forget(tx);

    let (bound_addrs, server) =
        nozzle::server::run(config.clone(), metadata_db.clone(), false, false, rx).await?;
    tokio::spawn(server);

    let worker = Worker::new(
        config.clone(),
        metadata_db,
        WorkerNodeId::from_str("basic_function").unwrap(),
    );
    tokio::spawn(worker.run());

    // Run `pnpm build` on the dataset.
    let dataset = DatasetPackage::new("basic_function");
    dataset.deploy(bound_addrs).await?;

    for test in load_sql_tests("basic-function.yaml").unwrap() {
        let results = run_query_on_fresh_server(&test.query, vec![], vec![], None)
            .await
            .map_err(|e| format!("{e:?}"));
        test.assert_result_eq(results);
    }

    Ok(())
}
