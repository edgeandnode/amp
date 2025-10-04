use std::sync::Arc;

use ampsync::{conn::DbConnPool, sync_engine::AmpsyncDbEngine};
use arrow_array::{ArrayRef, RecordBatch, UInt64Array};
use arrow_schema::{DataType, Field, Schema};
use pgtemp::PgTempDB;

/// Helper to create a test database pool
/// Returns (DbConnPool, sqlx::PgPool, PgTempDB) - the PgTempDB must be kept alive
async fn create_test_pool() -> (DbConnPool, sqlx::PgPool, PgTempDB) {
    // Set C locale for pgtemp
    unsafe {
        std::env::set_var("LANG", "C");
    }

    let pg_temp = PgTempDB::new();
    let connection_string = pg_temp.connection_uri();

    let db_pool = DbConnPool::connect(&connection_string, 1)
        .await
        .expect("Failed to create DbConnPool");

    // Also create a raw pool for verification queries
    let raw_pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .connect(&connection_string)
        .await
        .expect("Failed to connect to test database");

    (db_pool, raw_pool, pg_temp)
}

/// Helper to create a test RecordBatch with block_num column
fn create_test_batch(block_nums: Vec<u64>) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::UInt64, false),
        Field::new("block_num", DataType::UInt64, false),
    ]));

    let ids: Vec<u64> = (0..block_nums.len() as u64).collect();
    let id_array: ArrayRef = Arc::new(UInt64Array::from(ids));
    let block_num_array: ArrayRef = Arc::new(UInt64Array::from(block_nums));

    RecordBatch::try_new(schema, vec![id_array, block_num_array])
        .expect("Failed to create test batch")
}

#[tokio::test]
async fn test_init_checkpoint_table() {
    let (db_pool, pool, _pg_temp) = create_test_pool().await;
    let engine = AmpsyncDbEngine::new(&db_pool);

    // Initialize checkpoint table
    engine
        .init_checkpoint_table()
        .await
        .expect("Failed to initialize checkpoint table");

    // Verify table exists
    let table_exists: Option<(bool,)> = sqlx::query_as(
        "SELECT EXISTS (
            SELECT 1 FROM information_schema.tables
            WHERE table_name = '_ampsync_checkpoints'
        )",
    )
    .fetch_optional(&pool)
    .await
    .expect("Failed to check table existence");

    assert_eq!(table_exists, Some((true,)));

    // Verify columns exist
    let columns: Vec<(String,)> = sqlx::query_as(
        "SELECT column_name FROM information_schema.columns
         WHERE table_name = '_ampsync_checkpoints'
         ORDER BY ordinal_position",
    )
    .fetch_all(&pool)
    .await
    .expect("Failed to query columns");

    assert_eq!(columns.len(), 3);
    assert_eq!(columns[0].0, "table_name");
    assert_eq!(columns[1].0, "max_block_num");
    assert_eq!(columns[2].0, "updated_at");

    // Verify PRIMARY KEY on table_name
    let pk_exists: Option<(bool,)> = sqlx::query_as(
        "SELECT EXISTS (
            SELECT 1 FROM pg_constraint c
            JOIN pg_class t ON c.conrelid = t.oid
            JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = ANY(c.conkey)
            WHERE t.relname = '_ampsync_checkpoints'
            AND a.attname = 'table_name'
            AND c.contype = 'p'
        )",
    )
    .fetch_optional(&pool)
    .await
    .expect("Failed to check primary key");

    assert_eq!(pk_exists, Some((true,)));
}

#[tokio::test]
async fn test_get_checkpoint_none() {
    let (db_pool, _pool, _pg_temp) = create_test_pool().await;
    let engine = AmpsyncDbEngine::new(&db_pool);

    engine
        .init_checkpoint_table()
        .await
        .expect("Failed to initialize checkpoint table");

    // Get checkpoint for non-existent table
    let checkpoint = engine
        .get_checkpoint("test_table")
        .await
        .expect("Failed to get checkpoint");

    assert_eq!(checkpoint, None);
}

#[tokio::test]
async fn test_update_and_get_checkpoint() {
    let (db_pool, _pool, _pg_temp) = create_test_pool().await;
    let engine = AmpsyncDbEngine::new(&db_pool);

    engine
        .init_checkpoint_table()
        .await
        .expect("Failed to initialize checkpoint table");

    // Update checkpoint
    engine
        .update_checkpoint("test_table", 1000)
        .await
        .expect("Failed to update checkpoint");

    // Get checkpoint
    let checkpoint = engine
        .get_checkpoint("test_table")
        .await
        .expect("Failed to get checkpoint");

    assert_eq!(checkpoint, Some(1000));
}

#[tokio::test]
async fn test_update_checkpoint_upsert() {
    let (db_pool, _pool, _pg_temp) = create_test_pool().await;
    let engine = AmpsyncDbEngine::new(&db_pool);

    engine
        .init_checkpoint_table()
        .await
        .expect("Failed to initialize checkpoint table");

    // Initial update
    engine
        .update_checkpoint("test_table", 1000)
        .await
        .expect("Failed to update checkpoint");

    // Update again (should UPSERT)
    engine
        .update_checkpoint("test_table", 2000)
        .await
        .expect("Failed to update checkpoint");

    // Get checkpoint - should have latest value
    let checkpoint = engine
        .get_checkpoint("test_table")
        .await
        .expect("Failed to get checkpoint");

    assert_eq!(checkpoint, Some(2000));
}

#[tokio::test]
async fn test_extract_max_block_num() {
    // Test with UInt64 blocks
    let batch = create_test_batch(vec![100, 200, 150, 300, 250]);
    let max_block = AmpsyncDbEngine::extract_max_block_num(&batch);
    assert_eq!(max_block, Some(300));

    // Test with single block
    let batch = create_test_batch(vec![42]);
    let max_block = AmpsyncDbEngine::extract_max_block_num(&batch);
    assert_eq!(max_block, Some(42));

    // Test with empty batch
    let batch = create_test_batch(vec![]);
    let max_block = AmpsyncDbEngine::extract_max_block_num(&batch);
    assert_eq!(max_block, None);
}

#[tokio::test]
async fn test_extract_max_block_num_no_block_column() {
    // Create batch without block_num column
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::UInt64, false),
        Field::new("name", DataType::Utf8, true),
    ]));

    let ids: Vec<u64> = vec![1, 2, 3];
    let id_array: ArrayRef = Arc::new(UInt64Array::from(ids));
    let names: ArrayRef = Arc::new(arrow_array::StringArray::from(vec!["a", "b", "c"]));

    let batch =
        RecordBatch::try_new(schema, vec![id_array, names]).expect("Failed to create test batch");

    let max_block = AmpsyncDbEngine::extract_max_block_num(&batch);
    assert_eq!(max_block, None);
}

#[tokio::test]
async fn test_multiple_tables_separate_checkpoints() {
    let (db_pool, _pool, _pg_temp) = create_test_pool().await;
    let engine = AmpsyncDbEngine::new(&db_pool);

    engine
        .init_checkpoint_table()
        .await
        .expect("Failed to initialize checkpoint table");

    // Update checkpoints for different tables
    engine
        .update_checkpoint("blocks", 5000)
        .await
        .expect("Failed to update blocks checkpoint");

    engine
        .update_checkpoint("transactions", 3000)
        .await
        .expect("Failed to update transactions checkpoint");

    engine
        .update_checkpoint("logs", 7000)
        .await
        .expect("Failed to update logs checkpoint");

    // Verify each table has its own checkpoint
    assert_eq!(engine.get_checkpoint("blocks").await.unwrap(), Some(5000));
    assert_eq!(
        engine.get_checkpoint("transactions").await.unwrap(),
        Some(3000)
    );
    assert_eq!(engine.get_checkpoint("logs").await.unwrap(), Some(7000));
}

#[tokio::test]
async fn test_checkpoint_survives_reconnection() {
    let pg_temp = {
        // Set C locale for pgtemp
        unsafe {
            std::env::set_var("LANG", "C");
        }
        PgTempDB::new()
    };
    let connection_string = pg_temp.connection_uri();

    // First connection - set checkpoint
    {
        let db_pool = DbConnPool::connect(&connection_string, 1)
            .await
            .expect("Failed to create DbConnPool");
        let engine = AmpsyncDbEngine::new(&db_pool);

        engine
            .init_checkpoint_table()
            .await
            .expect("Failed to initialize checkpoint table");

        engine
            .update_checkpoint("test_table", 9999)
            .await
            .expect("Failed to update checkpoint");
    }
    // Connection dropped

    // Second connection - verify checkpoint persisted
    {
        let db_pool = DbConnPool::connect(&connection_string, 1)
            .await
            .expect("Failed to create DbConnPool");
        let engine = AmpsyncDbEngine::new(&db_pool);

        let checkpoint = engine
            .get_checkpoint("test_table")
            .await
            .expect("Failed to get checkpoint");

        assert_eq!(checkpoint, Some(9999));
    }
}
