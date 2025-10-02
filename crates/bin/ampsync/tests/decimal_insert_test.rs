use std::sync::Arc;

use ampsync::sync_engine::AmpsyncDbEngine;
use arrow_array::{
    BinaryArray, Decimal128Array, FixedSizeBinaryArray, RecordBatch, TimestampMicrosecondArray,
    UInt64Array,
};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use pgtemp::PgTempDB;

#[tokio::test]
async fn test_anvil_blocks_insert() {
    // Set C locale for pgtemp
    unsafe {
        std::env::set_var("LANG", "C");
    }

    // Create a temporary PostgreSQL database
    let pg_temp = PgTempDB::new();
    let connection_string = pg_temp.connection_uri();

    println!("PostgreSQL temp database created: {}", connection_string);

    // Connect to the temporary database using sqlx for schema setup
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .connect(&connection_string)
        .await
        .expect("Failed to connect to test database");

    // Create the blocks table matching the anvil.blocks schema
    sqlx::query("DROP TABLE IF EXISTS blocks")
        .execute(&pool)
        .await
        .expect("Failed to drop test table");

    sqlx::query(
        "CREATE TABLE blocks (
            timestamp TIMESTAMPTZ NOT NULL,
            mix_hash BYTEA NOT NULL,
            base_fee_per_gas NUMERIC(38, 0) NOT NULL,
            blob_gas_used NUMERIC(20, 0) NOT NULL,
            miner BYTEA NOT NULL,
            block_num NUMERIC(20, 0) NOT NULL,
            parent_hash BYTEA NOT NULL,
            excess_blob_gas NUMERIC(20, 0) NOT NULL,
            state_root BYTEA NOT NULL,
            receipt_root BYTEA NOT NULL,
            withdrawals_root BYTEA NOT NULL,
            hash BYTEA NOT NULL,
            difficulty NUMERIC(38, 0) NOT NULL,
            ommers_hash BYTEA NOT NULL,
            transactions_root BYTEA NOT NULL,
            nonce NUMERIC(20, 0) NOT NULL,
            parent_beacon_root BYTEA NOT NULL,
            extra_data BYTEA NOT NULL,
            gas_used NUMERIC(20, 0) NOT NULL,
            logs_bloom BYTEA NOT NULL,
            gas_limit NUMERIC(20, 0) NOT NULL
        )",
    )
    .execute(&pool)
    .await
    .expect("Failed to create test table");

    // Create test Arrow data matching the anvil.blocks schema
    // Note: Using Microsecond instead of Nanosecond because PostgreSQL only supports microsecond precision
    let schema = Arc::new(Schema::new(vec![
        Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Microsecond, Some("+00:00".into())),
            false,
        ),
        Field::new("mix_hash", DataType::FixedSizeBinary(32), false),
        Field::new("base_fee_per_gas", DataType::Decimal128(38, 0), false),
        Field::new("blob_gas_used", DataType::UInt64, false),
        Field::new("miner", DataType::FixedSizeBinary(20), false),
        Field::new("block_num", DataType::UInt64, false),
        Field::new("parent_hash", DataType::FixedSizeBinary(32), false),
        Field::new("excess_blob_gas", DataType::UInt64, false),
        Field::new("state_root", DataType::FixedSizeBinary(32), false),
        Field::new("receipt_root", DataType::FixedSizeBinary(32), false),
        Field::new("withdrawals_root", DataType::FixedSizeBinary(32), false),
        Field::new("hash", DataType::FixedSizeBinary(32), false),
        Field::new("difficulty", DataType::Decimal128(38, 0), false),
        Field::new("ommers_hash", DataType::FixedSizeBinary(32), false),
        Field::new("transactions_root", DataType::FixedSizeBinary(32), false),
        Field::new("nonce", DataType::UInt64, false),
        Field::new("parent_beacon_root", DataType::FixedSizeBinary(32), false),
        Field::new("extra_data", DataType::Binary, false),
        Field::new("gas_used", DataType::UInt64, false),
        Field::new("logs_bloom", DataType::Binary, false),
        Field::new("gas_limit", DataType::UInt64, false),
    ]));

    // Create test data for a single block
    // Using microsecond precision (PostgreSQL-compatible)
    let timestamp =
        TimestampMicrosecondArray::from(vec![1727914811000000i64]).with_timezone("+00:00");
    let mix_hash = FixedSizeBinaryArray::try_from_iter(vec![vec![0u8; 32]].into_iter()).unwrap();
    let base_fee_per_gas = Decimal128Array::from(vec![1000000000i128])
        .with_precision_and_scale(38, 0)
        .unwrap();
    let blob_gas_used = UInt64Array::from(vec![0u64]);
    let miner = FixedSizeBinaryArray::try_from_iter(vec![vec![0u8; 20]].into_iter()).unwrap();
    let block_num = UInt64Array::from(vec![1u64]);
    let parent_hash = FixedSizeBinaryArray::try_from_iter(vec![vec![0u8; 32]].into_iter()).unwrap();
    let excess_blob_gas = UInt64Array::from(vec![0u64]);
    let state_root = FixedSizeBinaryArray::try_from_iter(vec![vec![0u8; 32]].into_iter()).unwrap();
    let receipt_root =
        FixedSizeBinaryArray::try_from_iter(vec![vec![0u8; 32]].into_iter()).unwrap();
    let withdrawals_root =
        FixedSizeBinaryArray::try_from_iter(vec![vec![0u8; 32]].into_iter()).unwrap();
    let hash = FixedSizeBinaryArray::try_from_iter(vec![vec![1u8; 32]].into_iter()).unwrap();
    let difficulty = Decimal128Array::from(vec![0i128])
        .with_precision_and_scale(38, 0)
        .unwrap();
    let ommers_hash = FixedSizeBinaryArray::try_from_iter(vec![vec![0u8; 32]].into_iter()).unwrap();
    let transactions_root =
        FixedSizeBinaryArray::try_from_iter(vec![vec![0u8; 32]].into_iter()).unwrap();
    let nonce = UInt64Array::from(vec![0u64]);
    let parent_beacon_root =
        FixedSizeBinaryArray::try_from_iter(vec![vec![0u8; 32]].into_iter()).unwrap();
    let extra_data = BinaryArray::from(vec![vec![0u8; 0].as_slice()]);
    let gas_used = UInt64Array::from(vec![21000u64]);
    let logs_bloom = BinaryArray::from(vec![vec![0u8; 256].as_slice()]);
    let gas_limit = UInt64Array::from(vec![30000000u64]);

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(timestamp),
            Arc::new(mix_hash),
            Arc::new(base_fee_per_gas),
            Arc::new(blob_gas_used),
            Arc::new(miner),
            Arc::new(block_num),
            Arc::new(parent_hash),
            Arc::new(excess_blob_gas),
            Arc::new(state_root),
            Arc::new(receipt_root),
            Arc::new(withdrawals_root),
            Arc::new(hash),
            Arc::new(difficulty),
            Arc::new(ommers_hash),
            Arc::new(transactions_root),
            Arc::new(nonce),
            Arc::new(parent_beacon_root),
            Arc::new(extra_data),
            Arc::new(gas_used),
            Arc::new(logs_bloom),
            Arc::new(gas_limit),
        ],
    )
    .expect("Failed to create record batch");

    println!("Created test batch with {} rows", batch.num_rows());

    // Connect using the DbConnPool wrapper
    let db_pool = ampsync::conn::DbConnPool::connect(&connection_string, 1)
        .await
        .expect("Failed to create DbConnPool");

    let db_engine = AmpsyncDbEngine::new(&db_pool);

    // This should use our pgpq encoder
    match db_engine.insert_record_batch("blocks", &batch).await {
        Ok(()) => {
            println!("Successfully inserted batch!");

            // Verify the data was inserted correctly
            let rows: Vec<(String, String, String, String)> = sqlx::query_as(
                "SELECT difficulty::TEXT, base_fee_per_gas::TEXT, block_num::TEXT, gas_used::TEXT FROM blocks ORDER BY block_num",
            )
            .fetch_all(&pool)
            .await
            .expect("Failed to query inserted data");

            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].0, "0"); // difficulty
            assert_eq!(rows[0].1, "1000000000"); // base_fee_per_gas
            assert_eq!(rows[0].2, "1"); // block_num
            assert_eq!(rows[0].3, "21000"); // gas_used

            println!("Data verification passed!");
        }
        Err(e) => {
            panic!("Failed to insert: {}", e);
        }
    }

    // Cleanup
    sqlx::query("DROP TABLE blocks")
        .execute(&pool)
        .await
        .expect("Failed to drop test table");
}
