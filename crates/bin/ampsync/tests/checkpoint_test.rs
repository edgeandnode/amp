use std::collections::BTreeMap;

use ampsync::{conn::DbConnPool, sync_engine::AmpsyncDbEngine};
use common::metadata::segments::ResumeWatermark;
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

/// Helper to create a test watermark for a single network
fn create_test_watermark(network: &str, block_num: u64) -> ResumeWatermark {
    let mut map: BTreeMap<String, (u64, [u8; 32])> = BTreeMap::new();
    // Use a simple deterministic hash for testing
    let hash: [u8; 32] = {
        let mut h = [0u8; 32];
        h[0..8].copy_from_slice(&block_num.to_le_bytes());
        h
    };
    map.insert(network.to_string(), (block_num, hash));
    ResumeWatermark::from(map)
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

    // Verify columns exist (new hybrid checkpoint schema)
    let columns: Vec<(String,)> = sqlx::query_as(
        "SELECT column_name FROM information_schema.columns
         WHERE table_name = '_ampsync_checkpoints'
         ORDER BY ordinal_position",
    )
    .fetch_all(&pool)
    .await
    .expect("Failed to query columns");

    assert_eq!(columns.len(), 8);
    assert_eq!(columns[0].0, "table_name");
    assert_eq!(columns[1].0, "network");
    assert_eq!(columns[2].0, "incremental_block_num");
    assert_eq!(columns[3].0, "incremental_updated_at");
    assert_eq!(columns[4].0, "watermark_block_num");
    assert_eq!(columns[5].0, "watermark_block_hash");
    assert_eq!(columns[6].0, "watermark_updated_at");
    assert_eq!(columns[7].0, "updated_at");

    // Verify PRIMARY KEY on (table_name, network)
    let pk_columns: Vec<(String,)> = sqlx::query_as(
        "SELECT a.attname
         FROM pg_constraint c
         JOIN pg_class t ON c.conrelid = t.oid
         JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = ANY(c.conkey)
         WHERE t.relname = '_ampsync_checkpoints'
         AND c.contype = 'p'
         ORDER BY array_position(c.conkey, a.attnum)",
    )
    .fetch_all(&pool)
    .await
    .expect("Failed to query primary key columns");

    assert_eq!(pk_columns.len(), 2);
    assert_eq!(pk_columns[0].0, "table_name");
    assert_eq!(pk_columns[1].0, "network");
}

#[tokio::test]
async fn test_load_watermark_none() {
    let (db_pool, _pool, _pg_temp) = create_test_pool().await;
    let engine = AmpsyncDbEngine::new(&db_pool);

    engine
        .init_checkpoint_table()
        .await
        .expect("Failed to initialize checkpoint table");

    // Load watermark for non-existent table
    let watermark = engine
        .load_watermark("test_table")
        .await
        .expect("Failed to load watermark");

    assert_eq!(watermark, None);
}

#[tokio::test]
async fn test_save_and_load_watermark() {
    let (db_pool, _pool, _pg_temp) = create_test_pool().await;
    let engine = AmpsyncDbEngine::new(&db_pool);

    engine
        .init_checkpoint_table()
        .await
        .expect("Failed to initialize checkpoint table");

    // Save watermark
    let watermark = create_test_watermark("mainnet", 1000);
    engine
        .save_watermark("test_table", &watermark)
        .await
        .expect("Failed to save watermark");

    // Load watermark
    let loaded = engine
        .load_watermark("test_table")
        .await
        .expect("Failed to load watermark");

    assert_eq!(loaded, Some(watermark));
}

#[tokio::test]
async fn test_save_watermark_upsert() {
    let (db_pool, _pool, _pg_temp) = create_test_pool().await;
    let engine = AmpsyncDbEngine::new(&db_pool);

    engine
        .init_checkpoint_table()
        .await
        .expect("Failed to initialize checkpoint table");

    // Initial save
    let watermark1 = create_test_watermark("mainnet", 1000);
    engine
        .save_watermark("test_table", &watermark1)
        .await
        .expect("Failed to save watermark");

    // Save again (should UPSERT)
    let watermark2 = create_test_watermark("mainnet", 2000);
    engine
        .save_watermark("test_table", &watermark2)
        .await
        .expect("Failed to save watermark");

    // Load watermark - should have latest value
    let loaded = engine
        .load_watermark("test_table")
        .await
        .expect("Failed to load watermark");

    assert_eq!(loaded, Some(watermark2));
}

#[tokio::test]
async fn test_multiple_tables_separate_watermarks() {
    let (db_pool, _pool, _pg_temp) = create_test_pool().await;
    let engine = AmpsyncDbEngine::new(&db_pool);

    engine
        .init_checkpoint_table()
        .await
        .expect("Failed to initialize checkpoint table");

    // Save watermarks for different tables
    let watermark1 = create_test_watermark("mainnet", 5000);
    engine
        .save_watermark("blocks", &watermark1)
        .await
        .expect("Failed to save blocks watermark");

    let watermark2 = create_test_watermark("mainnet", 3000);
    engine
        .save_watermark("transactions", &watermark2)
        .await
        .expect("Failed to save transactions watermark");

    let watermark3 = create_test_watermark("mainnet", 7000);
    engine
        .save_watermark("logs", &watermark3)
        .await
        .expect("Failed to save logs watermark");

    // Verify each table has its own watermark
    assert_eq!(
        engine.load_watermark("blocks").await.unwrap(),
        Some(watermark1)
    );
    assert_eq!(
        engine.load_watermark("transactions").await.unwrap(),
        Some(watermark2)
    );
    assert_eq!(
        engine.load_watermark("logs").await.unwrap(),
        Some(watermark3)
    );
}

#[tokio::test]
async fn test_watermark_survives_reconnection() {
    let pg_temp = {
        // Set C locale for pgtemp
        unsafe {
            std::env::set_var("LANG", "C");
        }
        PgTempDB::new()
    };
    let connection_string = pg_temp.connection_uri();

    let expected_watermark = create_test_watermark("mainnet", 9999);

    // First connection - save watermark
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
            .save_watermark("test_table", &expected_watermark)
            .await
            .expect("Failed to save watermark");
    }
    // Connection dropped

    // Second connection - verify watermark persisted
    {
        let db_pool = DbConnPool::connect(&connection_string, 1)
            .await
            .expect("Failed to create DbConnPool");
        let engine = AmpsyncDbEngine::new(&db_pool);

        let loaded_watermark = engine
            .load_watermark("test_table")
            .await
            .expect("Failed to load watermark");

        assert_eq!(loaded_watermark, Some(expected_watermark));
    }
}

#[tokio::test]
async fn test_incremental_checkpoint_between_watermarks() {
    use ampsync::sync_engine::ResumePoint;

    let (db_pool, _pool, _pg_temp) = create_test_pool().await;
    let engine = AmpsyncDbEngine::new(&db_pool);

    engine
        .init_checkpoint_table()
        .await
        .expect("Failed to initialize checkpoint table");

    // Simulate batches arriving before first watermark
    engine
        .update_incremental_checkpoint("test_table", "mainnet", 100)
        .await
        .expect("Failed to update incremental checkpoint");

    engine
        .update_incremental_checkpoint("test_table", "mainnet", 200)
        .await
        .expect("Failed to update incremental checkpoint");

    engine
        .update_incremental_checkpoint("test_table", "mainnet", 300)
        .await
        .expect("Failed to update incremental checkpoint");

    // Load resume point - should get incremental checkpoint
    let resume_point = engine
        .load_resume_point("test_table")
        .await
        .expect("Failed to load resume point");

    match resume_point {
        ResumePoint::Incremental {
            network,
            max_block_num,
        } => {
            assert_eq!(network, "mainnet");
            assert_eq!(max_block_num, 300);
        }
        _ => panic!("Expected incremental resume point"),
    }

    // Now save a watermark
    let watermark = create_test_watermark("mainnet", 500);
    engine
        .save_watermark("test_table", &watermark)
        .await
        .expect("Failed to save watermark");

    // Load resume point - should now get watermark (preferred over incremental)
    let resume_point = engine
        .load_resume_point("test_table")
        .await
        .expect("Failed to load resume point");

    match resume_point {
        ResumePoint::Watermark(w) => {
            assert_eq!(w, watermark);
        }
        _ => panic!("Expected watermark resume point"),
    }
}

#[tokio::test]
async fn test_multi_network_watermark() {
    use std::collections::BTreeMap;

    let (db_pool, _pool, _pg_temp) = create_test_pool().await;
    let engine = AmpsyncDbEngine::new(&db_pool);

    engine
        .init_checkpoint_table()
        .await
        .expect("Failed to initialize checkpoint table");

    // Create multi-network watermark
    let mut map: BTreeMap<String, (u64, [u8; 32])> = BTreeMap::new();

    let hash1: [u8; 32] = {
        let mut h = [0u8; 32];
        h[0..8].copy_from_slice(&1000u64.to_le_bytes());
        h
    };
    map.insert("mainnet".to_string(), (1000, hash1));

    let hash2: [u8; 32] = {
        let mut h = [0u8; 32];
        h[0..8].copy_from_slice(&2000u64.to_le_bytes());
        h
    };
    map.insert("sepolia".to_string(), (2000, hash2));

    let watermark = ResumeWatermark::from(map);

    // Save multi-network watermark
    engine
        .save_watermark("test_table", &watermark)
        .await
        .expect("Failed to save watermark");

    // Load watermark
    let loaded = engine
        .load_watermark("test_table")
        .await
        .expect("Failed to load watermark");

    assert_eq!(loaded, Some(watermark));
}

#[tokio::test]
async fn test_batch_with_empty_ranges() {
    let (db_pool, _pool, _pg_temp) = create_test_pool().await;
    let engine = AmpsyncDbEngine::new(&db_pool);

    engine
        .init_checkpoint_table()
        .await
        .expect("Failed to initialize checkpoint table");

    // Test with empty ranges vector
    let empty_ranges: Vec<common::metadata::segments::BlockRange> = vec![];
    let result = AmpsyncDbEngine::extract_max_block_from_ranges(&empty_ranges);

    assert_eq!(result, None);

    // Verify updating incremental checkpoint with None doesn't panic
    // (It just won't update anything)
}
