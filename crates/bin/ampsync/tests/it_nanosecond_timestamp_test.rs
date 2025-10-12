use std::{sync::Arc, time::Duration};

use ampsync::sync_engine::AmpsyncDbEngine;
use arrow_array::{
    BinaryArray, FixedSizeBinaryArray, Int64Array, RecordBatch, TimestampNanosecondArray,
    UInt64Array,
};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use pgtemp::PgTempDB;

#[tokio::test]
async fn test_nanosecond_timestamp_insert() {
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

    // Create a test table with nanosecond timestamp (stored as TIMESTAMPTZ in Postgres)
    sqlx::query("DROP TABLE IF EXISTS timestamp_test")
        .execute(&pool)
        .await
        .expect("Failed to drop test table");

    sqlx::query(
        "CREATE TABLE timestamp_test (
            _id BYTEA NOT NULL,
            _block_num_start BIGINT NOT NULL,
            _block_num_end BIGINT NOT NULL,
            timestamp TIMESTAMPTZ NOT NULL,
            block_num NUMERIC(20, 0) NOT NULL,
            hash BYTEA NOT NULL,
            PRIMARY KEY (_id)
        )",
    )
    .execute(&pool)
    .await
    .expect("Failed to create test table");

    // Create Arrow schema with NANOSECOND precision timestamps
    // This matches the actual anvil schema
    let schema = Arc::new(Schema::new(vec![
        // System columns
        Field::new("_id", DataType::Binary, false),
        Field::new("_block_num_start", DataType::Int64, false),
        Field::new("_block_num_end", DataType::Int64, false),
        // Data columns
        Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Nanosecond, Some("+00:00".into())),
            false,
        ),
        Field::new("block_num", DataType::UInt64, false),
        Field::new("hash", DataType::FixedSizeBinary(32), false),
    ]));

    // Test various nanosecond timestamp edge cases
    // Postgres epoch: 2000-01-01 00:00:00 UTC = 946684800 seconds since Unix epoch
    let pg_epoch_ns = 946_684_800_000_000_000i64;

    let test_cases = vec![
        // Case 1: Exact Postgres epoch (2000-01-01 00:00:00 UTC)
        (1u64, pg_epoch_ns, "Postgres epoch"),
        // Case 2: Just after Postgres epoch with sub-microsecond precision
        // 2000-01-01 00:00:00.000001500 UTC (1.5 microseconds = 1500 nanoseconds)
        (2u64, pg_epoch_ns + 1_500, "Sub-microsecond after epoch"),
        // Case 3: 2020-01-01 00:00:00.000000000 UTC (exact microsecond boundary)
        (3u64, 1_577_836_800_000_000_000, "2020 exact microsecond"),
        // Case 4: 2020-01-01 00:00:00.000000123 UTC (123 nanoseconds)
        (4u64, 1_577_836_800_000_000_123, "2020 with nanoseconds"),
        // Case 5: Recent timestamp with sub-microsecond precision
        // 2024-10-12 12:34:56.789012345 UTC
        (
            5u64,
            1_728_736_496_789_012_345,
            "Recent with sub-microsecond",
        ),
    ];

    println!("Testing {} timestamp cases", test_cases.len());

    for (block_num, timestamp_ns, description) in test_cases {
        println!("\nTest case: {} (block {})", description, block_num);
        println!("  Nanosecond timestamp: {}", timestamp_ns);

        // Calculate expected microsecond value (what Postgres will store)
        // Postgres epoch in microseconds
        let pg_epoch_us = 946_684_800_000_000i64;
        // Convert nanoseconds to microseconds (truncating sub-microsecond precision)
        let expected_us_since_unix = timestamp_ns / 1_000;
        let expected_us_since_pg_epoch = expected_us_since_unix - pg_epoch_us;

        println!(
            "  Expected microseconds since Unix epoch: {}",
            expected_us_since_unix
        );
        println!(
            "  Expected microseconds since Postgres epoch: {}",
            expected_us_since_pg_epoch
        );

        // Create system columns
        let mut id = vec![0u8; 16];
        id[0..8].copy_from_slice(&block_num.to_le_bytes());

        // Create arrays
        let id_array = BinaryArray::from(vec![id.as_slice()]);
        let block_num_start_array = Int64Array::from(vec![block_num as i64]);
        let block_num_end_array = Int64Array::from(vec![block_num as i64]);
        let timestamp_array =
            TimestampNanosecondArray::from(vec![timestamp_ns]).with_timezone("+00:00");
        let block_num_array = UInt64Array::from(vec![block_num]);
        let hash_array =
            FixedSizeBinaryArray::try_from_iter(vec![vec![block_num as u8; 32]].into_iter())
                .unwrap();

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(id_array),
                Arc::new(block_num_start_array),
                Arc::new(block_num_end_array),
                Arc::new(timestamp_array),
                Arc::new(block_num_array),
                Arc::new(hash_array),
            ],
        )
        .expect("Failed to create record batch");

        // Connect using the DbConnPool wrapper
        let db_pool =
            ampsync::conn::DbConnPool::connect(&connection_string, 1, Duration::from_secs(300))
                .await
                .expect("Failed to create DbConnPool");

        let db_engine = AmpsyncDbEngine::new(&db_pool, Duration::from_secs(60));

        // Insert the batch
        match db_engine
            .insert_record_batch("timestamp_test", &batch)
            .await
        {
            Ok(()) => {
                println!("  Successfully inserted batch");

                // Verify the timestamp was stored correctly
                // Query timestamp as microseconds since Unix epoch for verification
                // Note: Must multiply before casting to preserve microsecond precision
                let row: (i64, String) = sqlx::query_as(
                    "SELECT
                        (EXTRACT(EPOCH FROM timestamp) * 1000000)::BIGINT as timestamp_us,
                        block_num::TEXT
                     FROM timestamp_test
                     WHERE block_num = $1",
                )
                .bind(block_num as i64)
                .fetch_one(&pool)
                .await
                .expect("Failed to query inserted data");

                let (stored_us, stored_block_num) = row;
                println!("  Stored microseconds since Unix epoch: {}", stored_us);
                println!("  Stored block_num: {}", stored_block_num);

                // Verify block number
                assert_eq!(
                    stored_block_num,
                    block_num.to_string(),
                    "Block number mismatch for {}",
                    description
                );

                // Verify timestamp (should match our expected microsecond value)
                assert_eq!(
                    stored_us,
                    expected_us_since_unix,
                    "Timestamp mismatch for {}: expected {} us, got {} us (difference: {} ns)",
                    description,
                    expected_us_since_unix,
                    stored_us,
                    (stored_us - expected_us_since_unix) * 1000
                );

                println!("  Timestamp verification passed");

                // Clean up this test case
                sqlx::query("DELETE FROM timestamp_test WHERE block_num = $1")
                    .bind(block_num as i64)
                    .execute(&pool)
                    .await
                    .expect("Failed to delete test row");
            }
            Err(e) => {
                panic!(
                    "Failed to insert {} (block {}): {}",
                    description, block_num, e
                );
            }
        }
    }

    println!("\nAll nanosecond timestamp test cases passed!");

    // Cleanup
    sqlx::query("DROP TABLE timestamp_test")
        .execute(&pool)
        .await
        .expect("Failed to drop test table");
}

#[tokio::test]
async fn test_nanosecond_timestamp_batch_insert() {
    // Set C locale for pgtemp
    unsafe {
        std::env::set_var("LANG", "C");
    }

    // Create a temporary PostgreSQL database
    let pg_temp = PgTempDB::new();
    let connection_string = pg_temp.connection_uri();

    println!("PostgreSQL temp database created: {}", connection_string);

    // Connect to the temporary database
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .connect(&connection_string)
        .await
        .expect("Failed to connect to test database");

    // Create test table
    sqlx::query("DROP TABLE IF EXISTS timestamp_batch_test")
        .execute(&pool)
        .await
        .expect("Failed to drop test table");

    sqlx::query(
        "CREATE TABLE timestamp_batch_test (
            _id BYTEA NOT NULL,
            _block_num_start BIGINT NOT NULL,
            _block_num_end BIGINT NOT NULL,
            timestamp TIMESTAMPTZ NOT NULL,
            block_num NUMERIC(20, 0) NOT NULL,
            PRIMARY KEY (_id)
        )",
    )
    .execute(&pool)
    .await
    .expect("Failed to create test table");

    // Create Arrow schema with NANOSECOND precision
    let schema = Arc::new(Schema::new(vec![
        Field::new("_id", DataType::Binary, false),
        Field::new("_block_num_start", DataType::Int64, false),
        Field::new("_block_num_end", DataType::Int64, false),
        Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Nanosecond, Some("+00:00".into())),
            false,
        ),
        Field::new("block_num", DataType::UInt64, false),
    ]));

    // Create a batch with 100 records, each with sub-microsecond precision
    let num_records = 100;
    let base_timestamp_ns = 1_577_836_800_000_000_000i64; // 2020-01-01 00:00:00 UTC

    let mut ids = Vec::with_capacity(num_records);
    let mut block_num_starts = Vec::with_capacity(num_records);
    let mut block_num_ends = Vec::with_capacity(num_records);
    let mut timestamps = Vec::with_capacity(num_records);
    let mut block_nums = Vec::with_capacity(num_records);

    for i in 0..num_records {
        let mut id = vec![0u8; 16];
        id[0..8].copy_from_slice(&(i as u64).to_le_bytes());
        ids.push(id);

        let block_number = (i + 1) as u64;
        block_num_starts.push(block_number as i64);
        block_num_ends.push(block_number as i64);
        block_nums.push(block_number);

        // Add 12 seconds per block + some nanoseconds to test sub-microsecond precision
        // Each block gets a unique nanosecond offset (0-999 nanoseconds)
        timestamps.push(base_timestamp_ns + (i as i64 * 12_000_000_000) + (i as i64 % 1000));
    }

    let id_array = BinaryArray::from(ids.iter().map(|v| v.as_slice()).collect::<Vec<_>>());
    let block_num_start_array = Int64Array::from(block_num_starts);
    let block_num_end_array = Int64Array::from(block_num_ends);
    let timestamp_array =
        TimestampNanosecondArray::from(timestamps.clone()).with_timezone("+00:00");
    let block_num_array = UInt64Array::from(block_nums);

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(id_array),
            Arc::new(block_num_start_array),
            Arc::new(block_num_end_array),
            Arc::new(timestamp_array),
            Arc::new(block_num_array),
        ],
    )
    .expect("Failed to create record batch");

    println!(
        "Created batch with {} records (nanosecond precision)",
        batch.num_rows()
    );

    // Insert batch
    let db_pool =
        ampsync::conn::DbConnPool::connect(&connection_string, 1, Duration::from_secs(300))
            .await
            .expect("Failed to create DbConnPool");

    let db_engine = AmpsyncDbEngine::new(&db_pool, Duration::from_secs(60));

    match db_engine
        .insert_record_batch("timestamp_batch_test", &batch)
        .await
    {
        Ok(()) => {
            println!("Successfully inserted batch of {} records", num_records);

            // Verify all records were inserted
            let count: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM timestamp_batch_test")
                .fetch_one(&pool)
                .await
                .expect("Failed to count records");

            assert_eq!(
                count.0, num_records as i64,
                "Expected {} records, found {}",
                num_records, count.0
            );

            println!("Verified {} records inserted", count.0);

            // Sample check: verify first, middle, and last record timestamps
            for &block_num in &[1u64, 50, 100] {
                let idx = (block_num - 1) as usize;
                let original_ns = timestamps[idx];
                let expected_us = original_ns / 1_000;

                let (stored_us,): (i64,) = sqlx::query_as(
                    "SELECT (EXTRACT(EPOCH FROM timestamp) * 1000000)::BIGINT
                     FROM timestamp_batch_test
                     WHERE block_num = $1",
                )
                .bind(block_num as i64)
                .fetch_one(&pool)
                .await
                .expect("Failed to query timestamp");

                assert_eq!(
                    stored_us, expected_us,
                    "Block {} timestamp mismatch: expected {} us, got {} us",
                    block_num, expected_us, stored_us
                );
            }

            println!("Spot-checked timestamps for blocks 1, 50, and 100");
        }
        Err(e) => {
            panic!("Failed to insert batch: {}", e);
        }
    }

    // Cleanup
    sqlx::query("DROP TABLE timestamp_batch_test")
        .execute(&pool)
        .await
        .expect("Failed to drop test table");

    println!("Batch insert test passed!");
}
