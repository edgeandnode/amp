use std::sync::Arc;

use ampsync::sync_engine::AmpsyncDbEngine;
use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use pgtemp::PgTempDB;

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

    sqlx::query(
        r#"CREATE TABLE transfers (
            "from" TEXT NOT NULL,
            "to" TEXT NOT NULL,
            "select" TEXT NOT NULL,
            amount BIGINT NOT NULL,
            block_num BIGINT NOT NULL PRIMARY KEY
        )"#,
    )
    .execute(&pool)
    .await
    .expect("Failed to create test table");

    // Create Arrow schema matching the table
    let schema = Arc::new(Schema::new(vec![
        Field::new("from", DataType::Utf8, false),
        Field::new("to", DataType::Utf8, false),
        Field::new("select", DataType::Utf8, false),
        Field::new("amount", DataType::Int64, false),
        Field::new("block_num", DataType::Int64, false),
    ]));

    // Create test data
    let from_array = StringArray::from(vec!["0xaaa", "0xbbb"]);
    let to_array = StringArray::from(vec!["0xccc", "0xddd"]);
    let select_array = StringArray::from(vec!["transfer1", "transfer2"]);
    let amount_array = Int64Array::from(vec![100, 200]);
    let block_num_array = Int64Array::from(vec![1, 2]);

    let batch = RecordBatch::try_new(
        schema,
        vec![
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
    let db_pool = ampsync::conn::DbConnPool::connect(&connection_string, 1)
        .await
        .expect("Failed to create DbConnPool");

    let db_engine = AmpsyncDbEngine::new(&db_pool);

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

            assert_eq!(rows.len(), 2);

            // First row
            assert_eq!(rows[0].0, "0xaaa"); // from
            assert_eq!(rows[0].1, "0xccc"); // to
            assert_eq!(rows[0].2, "transfer1"); // select
            assert_eq!(rows[0].3, 100); // amount
            assert_eq!(rows[0].4, 1); // block_num

            // Second row
            assert_eq!(rows[1].0, "0xbbb"); // from
            assert_eq!(rows[1].1, "0xddd"); // to
            assert_eq!(rows[1].2, "transfer2"); // select
            assert_eq!(rows[1].3, 200); // amount
            assert_eq!(rows[1].4, 2); // block_num

            println!("Data verification passed for reserved word columns!");
        }
        Err(e) => {
            panic!("Failed to insert with reserved word columns: {}", e);
        }
    }

    // Test duplicate insert (should handle conflict on block_num primary key)
    println!("Testing duplicate insert with conflict handling...");
    let duplicate_batch = RecordBatch::try_new(
        batch.schema(),
        vec![
            Arc::new(StringArray::from(vec!["0xaaa"])),
            Arc::new(StringArray::from(vec!["0xccc"])),
            Arc::new(StringArray::from(vec!["transfer1"])),
            Arc::new(Int64Array::from(vec![100])),
            Arc::new(Int64Array::from(vec![1])), // Same block_num as first row
        ],
    )
    .expect("Failed to create duplicate batch");

    db_engine
        .insert_record_batch("transfers", &duplicate_batch)
        .await
        .expect("Failed to insert duplicate batch");

    // Verify still only 2 rows (duplicate was skipped)
    let count: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM transfers")
        .fetch_one(&pool)
        .await
        .expect("Failed to count rows");

    assert_eq!(count.0, 2, "Duplicate should have been skipped");
    println!("Duplicate handling test passed!");

    // Cleanup
    sqlx::query("DROP TABLE transfers")
        .execute(&pool)
        .await
        .expect("Failed to drop test table");
}
