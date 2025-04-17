use std::sync::LazyLock;

use crate::{
    temp_metadata_db::test_metadata_db,
    test_support::{check_blocks, check_provider_file, load_test_config, SnapshotContext},
};
use arrow_flight::{
    flight_service_client::FlightServiceClient, sql::client::FlightSqlServiceClient,
};
use common::{
    arrow::{
        array::{
            Array, BinaryArray, FixedSizeBinaryArray, PrimitiveArray, StringArray, StructArray,
        },
        datatypes::Int64Type,
    },
    tracing,
};
use futures::StreamExt;

static KEEP_TEMP_DIRS: LazyLock<bool> = LazyLock::new(|| std::env::var("KEEP_TEMP_DIRS").is_ok());

#[tokio::test]
async fn evm_rpc_single_dump() {
    let dataset_name = "eth_rpc";
    check_provider_file("rpc_eth_mainnet.toml").await;
    tracing::register_logger();

    let metadata_db = test_metadata_db(*KEEP_TEMP_DIRS).await;
    let blessed = SnapshotContext::blessed(&dataset_name).await.unwrap();

    // Check the dataset directly against the RPC provider with `check_blocks`.
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
        Some(metadata_db),
        *KEEP_TEMP_DIRS,
    )
    .await
    .expect("temp dump failed");
    temp_dump
        .assert_eq(&blessed, Some(&*metadata_db))
        .await
        .unwrap();
}

#[tokio::test]
async fn eth_firehose_single_dump() {
    let dataset_name = "eth_firehose";
    check_provider_file("firehose_eth_mainnet.toml").await;
    tracing::register_logger();

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
        None,
        *KEEP_TEMP_DIRS,
    )
    .await
    .expect("temp dump failed");
    temp_dump.assert_eq(&blessed, None).await.unwrap();
}

#[tokio::test]
async fn sql_over_eth_firehose_dump() {
    let dataset_name = "sql_over_eth_firehose";
    tracing::register_logger();

    let blessed = SnapshotContext::blessed(&dataset_name).await.unwrap();

    // Now dump the dataset to a temporary directory and check blessed files against it.
    let temp_dump = SnapshotContext::temp_dump(
        &dataset_name,
        vec!["eth_firehose"],
        15_000_000,
        15_000_000,
        2,
        None,
        *KEEP_TEMP_DIRS,
    )
    .await
    .expect("temp dump failed");
    blessed.assert_eq(&temp_dump, None).await.unwrap();
}

#[tokio::test]
async fn simplest_possible_sql_query() {
    check_provider_file("rpc_eth_mainnet.toml").await;
    tracing::register_logger();

    // Start the nozzle server.
    let config = load_test_config(None).unwrap();
    let (shutdown_tx, shutdown_rx) = tokio::sync::broadcast::channel(1);
    let (bound, server) = nozzle::server::run(config, None, false, shutdown_rx)
        .await
        .unwrap();
    tokio::spawn(async move {
        server.await.unwrap();
    });

    let client = FlightServiceClient::connect(format!("grpc://{}", bound.flight_addr))
        .await
        .unwrap();
    let mut client = FlightSqlServiceClient::new_from_inner(client);

    let mut results = Vec::new();

    // Execute an SQL query and collect the results.
    let mut info = client.execute("SELECT 1".to_string(), None).await.unwrap();
    let mut batches = client
        .do_get(info.endpoint[0].ticket.take().unwrap())
        .await
        .unwrap();
    while let Some(batch) = batches.next().await {
        let batch = batch.unwrap();
        let column = batch.column(0);
        let column = column
            .as_any()
            .downcast_ref::<PrimitiveArray<Int64Type>>()
            .unwrap();
        assert_eq!(column.len(), 1);
        results.push(column.value(0));
    }

    assert_eq!(results, vec![1]);

    shutdown_tx.send(()).unwrap();
}

#[tokio::test]
async fn eth_call_sql_query() {
    check_provider_file("rpc_eth_mainnet.toml").await;
    tracing::register_logger();

    // Start the nozzle server.
    let config = load_test_config(None).unwrap();
    let (shutdown_tx, shutdown_rx) = tokio::sync::broadcast::channel(1);
    let (bound, server) = nozzle::server::run(config, None, false, shutdown_rx)
        .await
        .unwrap();
    tokio::spawn(async move {
        server.await.unwrap();
    });

    let client = FlightServiceClient::connect(format!("grpc://{}", bound.flight_addr))
        .await
        .unwrap();
    let mut client = FlightSqlServiceClient::new_from_inner(client);

    // Execute an SQL query and collect the results.
    let mut data_results: Vec<u8> = Vec::new();
    let mut message_results = String::new();
    let mut info = client
        .execute(
            "
                SELECT eth_rpc.eth_call(from, to, input, CAST(block_num as STRING))
                FROM eth_rpc.transactions
                WHERE tx_index = 2
            "
            .to_string(),
            None,
        )
        .await
        .unwrap();
    let mut batches = client
        .do_get(info.endpoint[0].ticket.take().unwrap())
        .await
        .unwrap();
    while let Some(batch) = batches.next().await {
        let batch = batch.unwrap();
        assert_eq!(batch.num_columns(), 1);
        let column = batch.column(0);
        let column = column.as_any().downcast_ref::<StructArray>().unwrap();
        assert_eq!(column.len(), 1);
        let data = column
            .column(0)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap();
        let message = column
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(data.len(), 1);
        assert_eq!(message.len(), 1);
        data_results.extend(data.value(0));
        message_results.push_str(message.value(0));
    }

    assert_eq!(
        data_results,
        vec![
            8, 195, 121, 160, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 32, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 85, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        ]
    );
    assert_eq!(message_results, "execution reverted: U");

    shutdown_tx.send(()).unwrap();
}

#[tokio::test]
async fn evm_decode_sql_query() {
    check_provider_file("rpc_eth_mainnet.toml").await;
    tracing::register_logger();

    // Start the nozzle server.
    let config = load_test_config(None).unwrap();
    let (shutdown_tx, shutdown_rx) = tokio::sync::broadcast::channel(1);
    let (bound, server) = nozzle::server::run(config, None, false, shutdown_rx)
        .await
        .unwrap();
    tokio::spawn(async move {
        server.await.unwrap();
    });

    let client = FlightServiceClient::connect(format!("grpc://{}", bound.flight_addr))
        .await
        .unwrap();
    let mut client = FlightSqlServiceClient::new_from_inner(client);

    // Execute an SQL query and collect the results.
    let mut from_results: Vec<u8> = Vec::new();
    let mut to_results: Vec<u8> = Vec::new();
    let mut value_results = String::new();
    let mut info = client
        .execute(
            "
                SELECT evm_decode(l.topic1, l.topic2, l.topic3, l.data, 'Transfer(address indexed from, address indexed to, uint256 value)')
                FROM eth_rpc.logs l
                WHERE l.topic0 = evm_topic('Transfer(address indexed from, address indexed to, uint256 value)')
                AND l.topic3 IS NULL
                LIMIT 1
            "
            .to_string(),
            None,
        )
        .await
        .unwrap();
    let mut batches = client
        .do_get(info.endpoint[0].ticket.take().unwrap())
        .await
        .unwrap();
    while let Some(batch) = batches.next().await {
        let batch = batch.unwrap();
        assert_eq!(batch.num_columns(), 1);
        let column = batch.column(0);
        let column = column.as_any().downcast_ref::<StructArray>().unwrap();
        assert_eq!(column.len(), 1);
        let from = column
            .column(0)
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap();
        let to = column
            .column(1)
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap();
        let value = column
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        from_results.extend(from.value(0));
        to_results.extend(to.value(0));
        value_results.push_str(value.value(0));
    }

    assert_eq!(
        from_results,
        vec![
            6, 114, 158, 178, 66, 77, 164, 120, 152, 249, 53, 38, 123, 212, 166, 41, 64, 222, 81,
            5,
        ]
    );
    assert_eq!(
        to_results,
        vec![
            190, 239, 186, 190, 234, 50, 63, 7, 197, 153, 38, 41, 82, 5, 211, 183, 161, 126, 134,
            56,
        ]
    );
    assert_eq!(value_results, "6818627949560085517");

    shutdown_tx.send(()).unwrap();
}

#[tokio::test]
async fn evm_topic_sql_query() {
    check_provider_file("rpc_eth_mainnet.toml").await;
    tracing::register_logger();

    // Start the nozzle server.
    let config = load_test_config(None).unwrap();
    let (shutdown_tx, shutdown_rx) = tokio::sync::broadcast::channel(1);
    let (bound, server) = nozzle::server::run(config, None, false, shutdown_rx)
        .await
        .unwrap();
    tokio::spawn(async move {
        server.await.unwrap();
    });

    let client = FlightServiceClient::connect(format!("grpc://{}", bound.flight_addr))
        .await
        .unwrap();
    let mut client = FlightSqlServiceClient::new_from_inner(client);

    let mut results: Vec<u8> = Vec::new();

    // Execute an SQL query and collect the results.
    let mut info = client
        .execute(
            "SELECT evm_topic('Transfer(address indexed from, address indexed to, uint256 value)')"
                .to_string(),
            None,
        )
        .await
        .unwrap();
    let mut batches = client
        .do_get(info.endpoint[0].ticket.take().unwrap())
        .await
        .unwrap();
    while let Some(batch) = batches.next().await {
        let batch = batch.unwrap();
        let column = batch.column(0);
        let column = column
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap();
        assert_eq!(column.len(), 1);
        results.extend(column.value(0));
    }

    assert_eq!(
        results,
        vec![
            221, 242, 82, 173, 27, 226, 200, 155, 105, 194, 176, 104, 252, 55, 141, 170, 149, 43,
            167, 241, 99, 196, 161, 22, 40, 245, 90, 77, 245, 35, 179, 239
        ]
    );

    shutdown_tx.send(()).unwrap();
}

#[tokio::test]
async fn attestation_hash_sql_query() {
    check_provider_file("rpc_eth_mainnet.toml").await;
    tracing::register_logger();

    // Start the nozzle server.
    let config = load_test_config(None).unwrap();
    let (shutdown_tx, shutdown_rx) = tokio::sync::broadcast::channel(1);
    let (bound, server) = nozzle::server::run(config, None, false, shutdown_rx)
        .await
        .unwrap();
    tokio::spawn(async move {
        server.await.unwrap();
    });

    let client = FlightServiceClient::connect(format!("grpc://{}", bound.flight_addr))
        .await
        .unwrap();
    let mut client = FlightSqlServiceClient::new_from_inner(client);

    let mut results: Vec<u8> = Vec::new();

    // Execute an SQL query and collect the results.
    let mut info = client
        .execute(
            "SELECT attestation_hash(input) FROM eth_rpc.transactions".to_string(),
            None,
        )
        .await
        .unwrap();
    let mut batches = client
        .do_get(info.endpoint[0].ticket.take().unwrap())
        .await
        .unwrap();
    while let Some(batch) = batches.next().await {
        let batch = batch.unwrap();
        let column = batch.column(0);
        let column = column.as_any().downcast_ref::<BinaryArray>().unwrap();
        assert_eq!(column.len(), 1);
        results.extend(column.value(0));
    }

    assert_eq!(
        results,
        vec![140, 57, 239, 34, 141, 226, 46, 189, 238, 68, 151, 25, 53, 26, 118, 101]
    );

    shutdown_tx.send(()).unwrap();
}
