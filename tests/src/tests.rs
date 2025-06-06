use std::str::FromStr as _;

use common::{query_context::parse_sql, tracing_helpers, BoxError};
use dataset_store::DatasetStore;
use dump::worker::Worker;
use futures::StreamExt;
use metadata_db::workers::WorkerNodeId;
use tokio::sync::broadcast;

use crate::test_support::{
    check_blocks, check_provider_file, load_sql_tests, record_batch_to_json,
    restore_blessed_dataset, run_query_on_fresh_server, DatasetPackage, SnapshotContext, TestEnv,
};

#[tokio::test]
async fn evm_rpc_single_dump() {
    tracing_helpers::register_logger();

    let dataset_name = "eth_rpc";
    check_provider_file("rpc_eth_mainnet.toml").await;

    let test_env = TestEnv::temp().await.unwrap();

    let blessed = SnapshotContext::blessed(&test_env, &dataset_name)
        .await
        .unwrap();

    // Check the dataset directly against the Firehose provider with `check_blocks`.
    check_blocks(&test_env, dataset_name, 15_000_000, 15_000_000)
        .await
        .expect("blessed data differed from provider");

    // Now dump the dataset to a temporary directory and check it again against the blessed files.
    let temp_dump = SnapshotContext::temp_dump(&test_env, &dataset_name, 15_000_000, 15_000_000, 1)
        .await
        .expect("temp dump failed");
    temp_dump.assert_eq(&blessed).await.unwrap();
}

#[tokio::test]
async fn eth_firehose_single_dump() {
    tracing_helpers::register_logger();

    let dataset_name = "eth_firehose";
    check_provider_file("firehose_eth_mainnet.toml").await;

    let test_env = TestEnv::temp().await.unwrap();
    let blessed = SnapshotContext::blessed(&test_env, &dataset_name)
        .await
        .unwrap();

    // Check the dataset directly against the Firehose provider with `check_blocks`.
    check_blocks(&test_env, dataset_name, 15_000_000, 15_000_000)
        .await
        .expect("blessed data differed from provider");
    // Now dump the dataset to a temporary directory and check it again against the blessed files.
    let temp_dump = SnapshotContext::temp_dump(&test_env, &dataset_name, 15_000_000, 15_000_000, 1)
        .await
        .expect("temp dump failed");
    temp_dump.assert_eq(&blessed).await.unwrap();
}

#[tokio::test]
async fn sql_over_eth_firehose_dump() {
    tracing_helpers::register_logger();
    let dataset_name = "sql_over_eth_firehose";

    let test_env = TestEnv::temp().await.unwrap();
    let blessed = SnapshotContext::blessed(&test_env, &dataset_name)
        .await
        .unwrap();

    // Restore dependency
    restore_blessed_dataset("eth_firehose", &test_env.metadata_db)
        .await
        .unwrap();

    // Now dump the dataset to a temporary directory and check blessed files against it.
    let temp_dump = SnapshotContext::temp_dump(&test_env, &dataset_name, 15_000_000, 15_000_000, 2)
        .await
        .expect("temp dump failed");
    temp_dump.assert_eq(&blessed).await.unwrap();
}

#[tokio::test]
async fn sql_tests() {
    tracing_helpers::register_logger();
    let test_env = TestEnv::temp().await.unwrap();

    for test in load_sql_tests("sql-tests.yaml").unwrap() {
        let results =
            run_query_on_fresh_server(&test_env, &test.name, &test.query, vec![], vec![], None)
                .await
                .map_err(|e| format!("{e:?}"));
        test.assert_result_eq(results);
    }
}

#[tokio::test]
async fn streaming_tests() {
    tracing_helpers::register_logger();
    let test_env = TestEnv::temp().await.unwrap();

    for test in load_sql_tests("sql-streaming-tests.yaml").unwrap() {
        let results = run_query_on_fresh_server(
            &test_env,
            &test.name,
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

    let test_env = TestEnv::temp().await.unwrap();
    let (tx, rx) = broadcast::channel(1);
    std::mem::forget(tx);

    let (bound_addrs, server) = nozzle::server::run(
        test_env.config.clone(),
        test_env.metadata_db.clone(),
        false,
        false,
        rx,
    )
    .await?;
    tokio::spawn(server);

    let worker = Worker::new(
        test_env.config.clone(),
        test_env.metadata_db.clone(),
        WorkerNodeId::from_str("basic_function").unwrap(),
    );
    tokio::spawn(worker.run());

    // Run `pnpm build` on the dataset.
    let dataset = DatasetPackage::new("basic_function");
    dataset.pnpm_install().await?;
    dataset.deploy(bound_addrs).await?;

    let dataset_store = DatasetStore::new(test_env.config.clone(), test_env.metadata_db.clone());
    let env = test_env.config.make_query_env()?;
    let ctx = dataset_store
        .ctx_for_sql(&parse_sql("SELECT basic_function.testString()")?, env)
        .await?;
    let result = ctx
        .execute_sql("SELECT basic_function.testString()")
        .await?
        .next()
        .await
        .unwrap()?;
    assert_eq!(
        record_batch_to_json(result),
        "[{\"basic_function.testString()\":\"I'm a function\"}]"
    );

    // TOOD: Fix function calls on flight server.
    // for test in load_sql_tests("basic-function.yaml").unwrap() {
    //     let results = run_query_on_fresh_server(&test.query, vec![], vec![], None)
    //         .await
    //         .map_err(|e| format!("{e:?}"));
    //     test.assert_result_eq(results);
    // }

    Ok(())
}
