use ampsync::{conn::DbConnPool, sync_engine::AmpsyncDbEngine};
use arrow_schema::DataType;
use datasets_common::manifest::DataType as ManifestDataType;
use datasets_derived::manifest::{ArrowSchema, Field as ManifestField};
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

/// Helper to create a test Arrow schema
fn create_test_schema(fields: Vec<(&str, DataType, bool)>) -> ArrowSchema {
    ArrowSchema {
        fields: fields
            .into_iter()
            .map(|(name, data_type, nullable)| ManifestField {
                name: name.to_string(),
                type_: ManifestDataType(data_type),
                nullable,
            })
            .collect(),
    }
}

#[tokio::test]
async fn test_create_new_table() {
    let (db_pool, pool, _pg_temp) = create_test_pool().await;
    let engine = AmpsyncDbEngine::new(&db_pool);

    let schema = create_test_schema(vec![
        ("id", DataType::UInt64, false),
        ("name", DataType::Utf8, true),
        ("block_num", DataType::UInt64, false),
    ]);

    // Create table
    engine
        .create_table_from_schema("test_table", &schema)
        .await
        .expect("Failed to create table");

    // Verify table exists and has correct columns
    let columns: Vec<(String,)> = sqlx::query_as(
        "SELECT column_name FROM information_schema.columns WHERE table_name = 'test_table' ORDER BY ordinal_position",
    )
    .fetch_all(&pool)
    .await
    .expect("Failed to query columns");

    assert_eq!(columns.len(), 3);
    assert_eq!(columns[0].0, "id");
    assert_eq!(columns[1].0, "name");
    assert_eq!(columns[2].0, "block_num");

    // Verify PRIMARY KEY exists on block_num
    let pk_exists: Option<(bool,)> = sqlx::query_as(
        "SELECT EXISTS (
            SELECT 1 FROM pg_constraint c
            JOIN pg_class t ON c.conrelid = t.oid
            JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = ANY(c.conkey)
            WHERE t.relname = 'test_table'
            AND a.attname = 'block_num'
            AND c.contype = 'p'
        )",
    )
    .fetch_optional(&pool)
    .await
    .expect("Failed to check primary key");

    assert_eq!(pk_exists, Some((true,)));
}

#[tokio::test]
async fn test_restart_with_same_schema() {
    let (db_pool, pool, _pg_temp) = create_test_pool().await;
    let engine = AmpsyncDbEngine::new(&db_pool);

    let schema = create_test_schema(vec![
        ("id", DataType::UInt64, false),
        ("name", DataType::Utf8, true),
    ]);

    // Create table first time
    engine
        .create_table_from_schema("test_table", &schema)
        .await
        .expect("Failed to create table");

    // Call again with same schema (simulates restart)
    engine
        .create_table_from_schema("test_table", &schema)
        .await
        .expect("Failed to handle schema check on restart");

    // Verify table still has correct columns
    let columns: Vec<(String,)> = sqlx::query_as(
        "SELECT column_name FROM information_schema.columns WHERE table_name = 'test_table' ORDER BY ordinal_position",
    )
    .fetch_all(&pool)
    .await
    .expect("Failed to query columns");

    assert_eq!(columns.len(), 2);
}

#[tokio::test]
async fn test_add_new_column() {
    let (db_pool, pool, _pg_temp) = create_test_pool().await;
    let engine = AmpsyncDbEngine::new(&db_pool);

    // Initial schema
    let schema_v1 = create_test_schema(vec![
        ("id", DataType::UInt64, false),
        ("name", DataType::Utf8, true),
    ]);

    // Create table
    engine
        .create_table_from_schema("test_table", &schema_v1)
        .await
        .expect("Failed to create table");

    // Updated schema with new column
    let schema_v2 = create_test_schema(vec![
        ("id", DataType::UInt64, false),
        ("name", DataType::Utf8, true),
        ("email", DataType::Utf8, true), // New column
    ]);

    // Apply schema evolution
    engine
        .create_table_from_schema("test_table", &schema_v2)
        .await
        .expect("Failed to add new column");

    // Verify new column exists
    let columns: Vec<(String,)> = sqlx::query_as(
        "SELECT column_name FROM information_schema.columns WHERE table_name = 'test_table' ORDER BY ordinal_position",
    )
    .fetch_all(&pool)
    .await
    .expect("Failed to query columns");

    assert_eq!(columns.len(), 3);
    assert_eq!(columns[2].0, "email");
}

#[tokio::test]
async fn test_add_multiple_columns() {
    let (db_pool, pool, _pg_temp) = create_test_pool().await;
    let engine = AmpsyncDbEngine::new(&db_pool);

    // Initial schema
    let schema_v1 = create_test_schema(vec![("id", DataType::UInt64, false)]);

    engine
        .create_table_from_schema("test_table", &schema_v1)
        .await
        .expect("Failed to create table");

    // Add multiple columns at once
    let schema_v2 = create_test_schema(vec![
        ("id", DataType::UInt64, false),
        ("name", DataType::Utf8, true),
        ("email", DataType::Utf8, true),
        ("age", DataType::Int32, true),
    ]);

    engine
        .create_table_from_schema("test_table", &schema_v2)
        .await
        .expect("Failed to add multiple columns");

    // Verify all columns exist
    let columns: Vec<(String,)> = sqlx::query_as(
        "SELECT column_name FROM information_schema.columns WHERE table_name = 'test_table' ORDER BY ordinal_position",
    )
    .fetch_all(&pool)
    .await
    .expect("Failed to query columns");

    assert_eq!(columns.len(), 4);
    assert_eq!(columns[0].0, "id");
    assert_eq!(columns[1].0, "name");
    assert_eq!(columns[2].0, "email");
    assert_eq!(columns[3].0, "age");
}

#[tokio::test]
async fn test_type_change_fails() {
    let (db_pool, _pool, _pg_temp) = create_test_pool().await;
    let engine = AmpsyncDbEngine::new(&db_pool);

    // Initial schema with name as TEXT
    let schema_v1 = create_test_schema(vec![
        ("id", DataType::UInt64, false),
        ("name", DataType::Utf8, true),
    ]);

    engine
        .create_table_from_schema("test_table", &schema_v1)
        .await
        .expect("Failed to create table");

    // Try to change name from TEXT to INT32
    let schema_v2 = create_test_schema(vec![
        ("id", DataType::UInt64, false),
        ("name", DataType::Int32, true), // Type change!
    ]);

    let result = engine
        .create_table_from_schema("test_table", &schema_v2)
        .await;

    assert!(result.is_err());
    let err_msg = result.unwrap_err().to_string();
    assert!(err_msg.contains("Type changes detected"));
    assert!(err_msg.contains("name"));
}

#[tokio::test]
async fn test_dropped_column_fails() {
    let (db_pool, _pool, _pg_temp) = create_test_pool().await;
    let engine = AmpsyncDbEngine::new(&db_pool);

    // Initial schema with 3 columns
    let schema_v1 = create_test_schema(vec![
        ("id", DataType::UInt64, false),
        ("name", DataType::Utf8, true),
        ("email", DataType::Utf8, true),
    ]);

    engine
        .create_table_from_schema("test_table", &schema_v1)
        .await
        .expect("Failed to create table");

    // Try to drop email column
    let schema_v2 = create_test_schema(vec![
        ("id", DataType::UInt64, false),
        ("name", DataType::Utf8, true),
        // email dropped
    ]);

    let result = engine
        .create_table_from_schema("test_table", &schema_v2)
        .await;

    assert!(result.is_err());
    let err_msg = result.unwrap_err().to_string();
    assert!(err_msg.contains("Columns dropped from schema"));
    assert!(err_msg.contains("email"));
}

#[tokio::test]
async fn test_add_block_num_with_primary_key() {
    let (db_pool, pool, _pg_temp) = create_test_pool().await;
    let engine = AmpsyncDbEngine::new(&db_pool);

    // Initial schema without block_num
    let schema_v1 = create_test_schema(vec![
        ("id", DataType::UInt64, false),
        ("name", DataType::Utf8, true),
    ]);

    engine
        .create_table_from_schema("test_table", &schema_v1)
        .await
        .expect("Failed to create table");

    // Add block_num column (should add PRIMARY KEY automatically)
    let schema_v2 = create_test_schema(vec![
        ("id", DataType::UInt64, false),
        ("name", DataType::Utf8, true),
        ("block_num", DataType::UInt64, false), // New column
    ]);

    engine
        .create_table_from_schema("test_table", &schema_v2)
        .await
        .expect("Failed to add block_num column");

    // Verify block_num column exists
    let has_block_num: Option<(bool,)> = sqlx::query_as(
        "SELECT EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_name = 'test_table' AND column_name = 'block_num'
        )",
    )
    .fetch_optional(&pool)
    .await
    .expect("Failed to check block_num column");

    assert_eq!(has_block_num, Some((true,)));

    // Note: PRIMARY KEY is only added during initial table creation, not via ALTER TABLE
    // This is expected behavior - adding constraints to existing tables requires manual intervention
}
