use std::{ops::RangeInclusive, sync::Arc, time::Duration};

use alloy::primitives::BlockHash;
use amp_client::InvalidationRange;
use arrow_array::{Int64Array, RecordBatch, StringArray, TimestampMicrosecondArray};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use common::metadata::segments::BlockRange;
use datasets_common::manifest::DataType as ManifestDataType;
use datasets_derived::manifest::{ArrowSchema, Field as ManifestField};
use pgtemp::PgTempDB;

use crate::sync_engine::AmpsyncDbEngine;

const DEFAULT_DB_OPERATION_RETRY_DURATION_SECS: Duration = Duration::from_secs(60);
const DEFAULT_DB_MAX_RETRY_DURATION_SECS: Duration = Duration::from_secs(300);

/// Test multi-row batch insert with `_block_num` injection.
///
/// This tests the scenario where:
/// 1. User's SQL query doesn't SELECT block_num
/// 2. System injects `_id`, `_block_num_start`, `_block_num_end` columns
/// 3. Multiple transactions exist in the same block (common scenario)
///
/// Tests that all rows insert successfully with unique `_id` values.
#[tokio::test]
async fn test_multi_row_batch_without_block_num() {
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

    // Connect using the DbConnPool wrapper
    let db_pool =
        crate::conn::DbConnPool::connect(&connection_string, 1, DEFAULT_DB_MAX_RETRY_DURATION_SECS)
            .await
            .expect("Failed to create DbConnPool");

    let db_engine = AmpsyncDbEngine::new(&db_pool, DEFAULT_DB_OPERATION_RETRY_DURATION_SECS);

    // Create Arrow schema WITHOUT block_num column
    // This is what happens when user's query doesn't SELECT block_num
    let manifest_schema = ArrowSchema {
        fields: vec![
            ManifestField {
                name: "timestamp".to_string(),
                type_: ManifestDataType(DataType::Timestamp(
                    TimeUnit::Microsecond,
                    Some("+00:00".into()),
                )),
                nullable: false,
            },
            ManifestField {
                name: "from_addr".to_string(),
                type_: ManifestDataType(DataType::Utf8),
                nullable: false,
            },
            ManifestField {
                name: "to_addr".to_string(),
                type_: ManifestDataType(DataType::Utf8),
                nullable: false,
            },
            ManifestField {
                name: "value".to_string(),
                type_: ManifestDataType(DataType::Int64),
                nullable: false,
            },
        ],
    };

    // Let the engine create the table - it will inject _id, _block_num_start, _block_num_end
    db_engine
        .create_table_from_schema("transfers", &manifest_schema)
        .await
        .expect("Failed to create table");

    // Create RecordBatch schema (for in-memory data)
    let schema = Arc::new(Schema::new(vec![
        Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Microsecond, Some("+00:00".into())),
            false,
        ),
        Field::new("from_addr", DataType::Utf8, false),
        Field::new("to_addr", DataType::Utf8, false),
        Field::new("value", DataType::Int64, false),
    ]));

    // Create realistic test data: 100 transactions across 10 blocks (10 tx per block)
    // This is a VERY common scenario (multiple transactions in same block)
    let num_transactions = 100;
    let base_timestamp = 1727914811000000i64;

    let mut timestamps = Vec::with_capacity(num_transactions);
    let mut from_addrs = Vec::with_capacity(num_transactions);
    let mut to_addrs = Vec::with_capacity(num_transactions);
    let mut values = Vec::with_capacity(num_transactions);

    for i in 0..num_transactions {
        // 10 transactions per block (blocks 1-10)
        let block_num = (i / 10) + 1;
        timestamps.push(base_timestamp + (block_num as i64 * 12_000_000)); // Block timestamp
        from_addrs.push(format!("0x{:040x}", i + 1000)); // Unique addresses
        to_addrs.push(format!("0x{:040x}", i + 2000));
        values.push((i + 1) as i64 * 1000); // Varying values
    }

    let timestamp = TimestampMicrosecondArray::from(timestamps).with_timezone("+00:00");
    let from_addr = StringArray::from(from_addrs);
    let to_addr = StringArray::from(to_addrs);
    let value = Int64Array::from(values);

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(timestamp),
            Arc::new(from_addr),
            Arc::new(to_addr),
            Arc::new(value),
        ],
    )
    .expect("Failed to create record batch");

    println!("Created test batch with {} rows", batch.num_rows());

    // Create block ranges metadata (blocks 1-10)
    let ranges = vec![BlockRange {
        numbers: RangeInclusive::new(1, 10),
        network: "test".to_string(),
        hash: BlockHash::ZERO,
        prev_hash: None,
    }];

    // Inject system metadata columns: _id, _block_num_start, _block_num_end
    let batch_with_metadata = crate::batch_utils::inject_system_metadata(batch, &ranges)
        .expect("Failed to inject metadata columns");

    println!(
        "Injected system metadata columns. Batch now has {} columns",
        batch_with_metadata.num_columns()
    );

    // Try to insert the batch
    // This should now SUCCEED with all 100 rows
    match db_engine
        .insert_record_batch("transfers", &batch_with_metadata)
        .await
    {
        Ok(()) => {
            println!("Successfully inserted batch!");

            // Verify the data was inserted correctly
            let rows: Vec<(i64, i64, String, String, i64)> = sqlx::query_as(
                "SELECT _block_num_start, _block_num_end, from_addr, to_addr, value FROM transfers ORDER BY value",
            )
            .fetch_all(&pool)
            .await
            .expect("Failed to query inserted data");

            println!("Found {} rows in database", rows.len());

            // We should have all 100 rows
            assert_eq!(rows.len(), 100, "Expected 100 transactions");

            // All rows should have the same block range (1-10)
            assert_eq!(rows[0].0, 1); // _block_num_start
            assert_eq!(rows[0].1, 10); // _block_num_end

            // Verify first transaction
            assert_eq!(rows[0].2, format!("0x{:040x}", 1000)); // from_addr
            assert_eq!(rows[0].3, format!("0x{:040x}", 2000)); // to_addr
            assert_eq!(rows[0].4, 1000); // value

            // Verify middle transaction
            assert_eq!(rows[45].0, 1); // _block_num_start
            assert_eq!(rows[45].1, 10); // _block_num_end
            assert_eq!(rows[45].2, format!("0x{:040x}", 1045)); // from_addr
            assert_eq!(rows[45].3, format!("0x{:040x}", 2045)); // to_addr
            assert_eq!(rows[45].4, 46000); // value

            // Verify last transaction
            assert_eq!(rows[99].0, 1); // _block_num_start
            assert_eq!(rows[99].1, 10); // _block_num_end
            assert_eq!(rows[99].2, format!("0x{:040x}", 1099)); // from_addr
            assert_eq!(rows[99].3, format!("0x{:040x}", 2099)); // to_addr
            assert_eq!(rows[99].4, 100000); // value

            println!("Data verification passed!");
        }
        Err(e) => {
            panic!("Insert failed: {}", e);
        }
    }
}

/// Test reorg handling with injected system metadata columns.
///
/// This tests that reorg DELETE operations work correctly with the `_block_num_end` column.
/// Uses conservative reorg: deletes entire batch if any block is invalidated.
#[tokio::test]
async fn test_reorg_with_injected_block_num() {
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

    // Connect using the DbConnPool wrapper
    let db_pool =
        crate::conn::DbConnPool::connect(&connection_string, 1, DEFAULT_DB_MAX_RETRY_DURATION_SECS)
            .await
            .expect("Failed to create DbConnPool");

    let db_engine = AmpsyncDbEngine::new(&db_pool, DEFAULT_DB_OPERATION_RETRY_DURATION_SECS);

    // Create Arrow schema WITHOUT block_num column
    let manifest_schema = ArrowSchema {
        fields: vec![
            ManifestField {
                name: "from_addr".to_string(),
                type_: ManifestDataType(DataType::Utf8),
                nullable: false,
            },
            ManifestField {
                name: "to_addr".to_string(),
                type_: ManifestDataType(DataType::Utf8),
                nullable: false,
            },
            ManifestField {
                name: "value".to_string(),
                type_: ManifestDataType(DataType::Int64),
                nullable: false,
            },
        ],
    };

    // Let the engine create the table - it will inject _id, _block_num_start, _block_num_end
    db_engine
        .create_table_from_schema("transfers", &manifest_schema)
        .await
        .expect("Failed to create table");

    // Create RecordBatch schema (for in-memory data)
    let schema = Arc::new(Schema::new(vec![
        Field::new("from_addr", DataType::Utf8, false),
        Field::new("to_addr", DataType::Utf8, false),
        Field::new("value", DataType::Int64, false),
    ]));

    // Insert transactions for blocks 1-10 (10 tx per block = 100 total)
    let num_transactions = 100;
    let mut from_addrs = Vec::with_capacity(num_transactions);
    let mut to_addrs = Vec::with_capacity(num_transactions);
    let mut values = Vec::with_capacity(num_transactions);

    for i in 0..num_transactions {
        from_addrs.push(format!("0x{:040x}", i + 1000));
        to_addrs.push(format!("0x{:040x}", i + 2000));
        values.push((i + 1) as i64 * 1000);
    }

    let from_addr = StringArray::from(from_addrs);
    let to_addr = StringArray::from(to_addrs);
    let value = Int64Array::from(values);

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(from_addr), Arc::new(to_addr), Arc::new(value)],
    )
    .expect("Failed to create record batch");

    let ranges = vec![BlockRange {
        numbers: RangeInclusive::new(1, 10),
        network: "test".to_string(),
        hash: BlockHash::ZERO,
        prev_hash: None,
    }];
    let batch_with_metadata = crate::batch_utils::inject_system_metadata(batch, &ranges)
        .expect("Failed to inject metadata columns");

    // Insert the batch (should now succeed with all 100 rows)
    db_engine
        .insert_record_batch("transfers", &batch_with_metadata)
        .await
        .expect("Failed to insert initial batch");

    // Verify initial count
    let initial_count: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM transfers")
        .fetch_one(&pool)
        .await
        .expect("Failed to count rows");

    println!("Initial row count: {}", initial_count.0);
    assert_eq!(initial_count.0, 100, "Expected 100 rows initially");

    // Simulate reorg: Delete blocks 8-10
    // Since all rows have _block_num_end = 10, the reorg will conservatively delete ALL 100 rows
    let reorg_ranges = vec![InvalidationRange {
        network: "test".to_string(),
        numbers: RangeInclusive::new(8, 10),
    }];
    db_engine
        .handle_reorg("transfers", &reorg_ranges)
        .await
        .expect("Failed to handle reorg");

    // Verify that all rows were deleted (conservative reorg)
    let after_reorg_count: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM transfers")
        .fetch_one(&pool)
        .await
        .expect("Failed to count rows after reorg");

    println!("After reorg row count: {}", after_reorg_count.0);

    // Conservative reorg: All 100 rows deleted because _block_num_end=10 >= 8
    // This is safe but wasteful - better than not deleting at all
    assert_eq!(
        after_reorg_count.0, 0,
        "Expected 0 rows after conservative reorg (entire batch deleted because _block_num_end >= 8)"
    );

    println!("Reorg verification passed! Conservative delete worked correctly.");
}
