use std::{sync::Arc, time::Duration};

use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use pgtemp::PgTempDB;

use crate::sync_engine::AmpsyncDbEngine;

/// Test that we can insert data into tables with SQL reserved word column names
/// like "to", "from", "select", etc.
#[tokio::test]
async fn test_insert_with_reserved_word_columns() {
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

    // Create a table with reserved word column names
    // These columns need to be quoted in SQL statements
    sqlx::query("DROP TABLE IF EXISTS transfers")
        .execute(&pool)
        .await
        .expect("Failed to drop test table");

    // Create table with system columns that AmpsyncDbEngine expects
    sqlx::query(
        r#"CREATE TABLE transfers (
            _id BYTEA NOT NULL,
            _block_num_start BIGINT NOT NULL,
            _block_num_end BIGINT NOT NULL,
            "from" TEXT NOT NULL,
            "to" TEXT NOT NULL,
            "select" TEXT NOT NULL,
            amount BIGINT NOT NULL,
            block_num BIGINT NOT NULL,
            PRIMARY KEY (_id)
        )"#,
    )
    .execute(&pool)
    .await
    .expect("Failed to create test table");

    // Create Arrow schema matching the table (including system columns)
    let schema = Arc::new(Schema::new(vec![
        Field::new("_id", DataType::Binary, false),
        Field::new("_block_num_start", DataType::Int64, false),
        Field::new("_block_num_end", DataType::Int64, false),
        Field::new("from", DataType::Utf8, false),
        Field::new("to", DataType::Utf8, false),
        Field::new("select", DataType::Utf8, false),
        Field::new("amount", DataType::Int64, false),
        Field::new("block_num", DataType::Int64, false),
    ]));

    // Create test data with realistic batch size (50 transfers across multiple blocks)
    let num_transfers = 50;
    let mut ids = Vec::with_capacity(num_transfers);
    let mut block_num_starts = Vec::with_capacity(num_transfers);
    let mut block_num_ends = Vec::with_capacity(num_transfers);
    let mut from_addrs = Vec::with_capacity(num_transfers);
    let mut to_addrs = Vec::with_capacity(num_transfers);
    let mut select_values = Vec::with_capacity(num_transfers);
    let mut amounts = Vec::with_capacity(num_transfers);
    let mut block_nums = Vec::with_capacity(num_transfers);

    for i in 0..num_transfers {
        // Generate synthetic _id
        let mut id = vec![0u8; 16];
        id[0..8].copy_from_slice(&(i as u64).to_le_bytes());
        ids.push(id);

        block_num_starts.push((i + 1) as i64);
        block_num_ends.push((i + 1) as i64);
        from_addrs.push(format!("0x{:040x}", i + 1000)); // Unique addresses
        to_addrs.push(format!("0x{:040x}", i + 2000));
        select_values.push(format!("transfer{}", i + 1));
        amounts.push((i + 1) as i64 * 100); // Increasing amounts: 100, 200, 300, ...
        block_nums.push((i + 1) as i64); // Blocks 1-50
    }

    use arrow_array::BinaryArray;
    let id_array = BinaryArray::from(ids.iter().map(|v| v.as_slice()).collect::<Vec<_>>());
    let block_num_start_array = Int64Array::from(block_num_starts);
    let block_num_end_array = Int64Array::from(block_num_ends);
    let from_array = StringArray::from(from_addrs);
    let to_array = StringArray::from(to_addrs);
    let select_array = StringArray::from(select_values);
    let amount_array = Int64Array::from(amounts);
    let block_num_array = Int64Array::from(block_nums);

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(id_array),
            Arc::new(block_num_start_array),
            Arc::new(block_num_end_array),
            Arc::new(from_array),
            Arc::new(to_array),
            Arc::new(select_array),
            Arc::new(amount_array),
            Arc::new(block_num_array),
        ],
    )
    .expect("Failed to create record batch");

    println!("Created test batch with {} rows", batch.num_rows());

    // Connect using the DbConnPool wrapper
    let db_pool = crate::conn::DbConnPool::connect(&connection_string, 1, Duration::from_secs(300))
        .await
        .expect("Failed to create DbConnPool");

    let db_engine = AmpsyncDbEngine::new(&db_pool, Duration::from_secs(60));

    // Insert the batch - this should correctly quote the reserved word columns
    match db_engine.insert_record_batch("transfers", &batch).await {
        Ok(()) => {
            println!("Successfully inserted batch with reserved word columns!");

            // Verify the data was inserted correctly
            // Note: We need to quote the reserved words in our SELECT query too
            let rows: Vec<(String, String, String, i64, i64)> = sqlx::query_as(
                r#"SELECT "from", "to", "select", amount, block_num FROM transfers ORDER BY block_num"#,
            )
            .fetch_all(&pool)
            .await
            .expect("Failed to query inserted data");

            assert_eq!(rows.len(), 50, "Expected 50 transfers to be inserted");

            // Verify first transfer (block 1)
            assert_eq!(rows[0].0, format!("0x{:040x}", 1000)); // from
            assert_eq!(rows[0].1, format!("0x{:040x}", 2000)); // to
            assert_eq!(rows[0].2, "transfer1"); // select
            assert_eq!(rows[0].3, 100); // amount
            assert_eq!(rows[0].4, 1); // block_num

            // Verify last transfer (block 50)
            assert_eq!(rows[49].0, format!("0x{:040x}", 1049)); // from
            assert_eq!(rows[49].1, format!("0x{:040x}", 2049)); // to
            assert_eq!(rows[49].2, "transfer50"); // select
            assert_eq!(rows[49].3, 5000); // amount (50 * 100)
            assert_eq!(rows[49].4, 50); // block_num

            // Verify a middle transfer (block 25)
            assert_eq!(rows[24].0, format!("0x{:040x}", 1024)); // from
            assert_eq!(rows[24].1, format!("0x{:040x}", 2024)); // to
            assert_eq!(rows[24].2, "transfer25"); // select
            assert_eq!(rows[24].3, 2500); // amount (25 * 100)
            assert_eq!(rows[24].4, 25); // block_num

            println!("Data verification passed for reserved word columns!");
        }
        Err(e) => {
            panic!("Failed to insert with reserved word columns: {}", e);
        }
    }

    // Test duplicate insert (should handle conflict on _id primary key)
    println!("Testing duplicate insert with conflict handling...");

    // Create duplicate with same _id as first row
    let mut duplicate_id = vec![0u8; 16];
    duplicate_id[0..8].copy_from_slice(&(0u64).to_le_bytes()); // Same ID as first row

    let duplicate_batch = RecordBatch::try_new(
        batch.schema(),
        vec![
            Arc::new(BinaryArray::from(vec![duplicate_id.as_slice()])),
            Arc::new(Int64Array::from(vec![1])),
            Arc::new(Int64Array::from(vec![1])),
            Arc::new(StringArray::from(vec!["0xaaa"])),
            Arc::new(StringArray::from(vec!["0xccc"])),
            Arc::new(StringArray::from(vec!["transfer_duplicate"])),
            Arc::new(Int64Array::from(vec![999])),
            Arc::new(Int64Array::from(vec![1])),
        ],
    )
    .expect("Failed to create duplicate batch");

    db_engine
        .insert_record_batch("transfers", &duplicate_batch)
        .await
        .expect("Failed to insert duplicate batch");

    // Verify still only 50 rows (duplicate was skipped due to _id conflict)
    let count: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM transfers")
        .fetch_one(&pool)
        .await
        .expect("Failed to count rows");

    assert_eq!(
        count.0, 50,
        "Duplicate should have been skipped due to _id conflict"
    );
    println!("Duplicate handling test passed!");

    // Cleanup
    sqlx::query("DROP TABLE transfers")
        .execute(&pool)
        .await
        .expect("Failed to drop test table");
}
