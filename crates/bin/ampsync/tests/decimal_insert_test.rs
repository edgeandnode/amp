use std::sync::Arc;

use ampsync::sync_engine::AmpsyncDbEngine;
use arrow_array::{
    BinaryArray, Decimal128Array, FixedSizeBinaryArray, Int64Array, RecordBatch,
    TimestampMicrosecondArray, UInt64Array,
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
            _id BYTEA NOT NULL,
            _block_num_start BIGINT NOT NULL,
            _block_num_end BIGINT NOT NULL,
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
            gas_limit NUMERIC(20, 0) NOT NULL,
            PRIMARY KEY (_id)
        )",
    )
    .execute(&pool)
    .await
    .expect("Failed to create test table");

    // Create test Arrow data matching the anvil.blocks schema
    // Note: Using Microsecond instead of Nanosecond because PostgreSQL only supports microsecond precision
    let schema = Arc::new(Schema::new(vec![
        // System columns (always first three columns)
        Field::new("_id", DataType::Binary, false), // Variable-length binary (xxh3_128 = 16 bytes)
        Field::new("_block_num_start", DataType::Int64, false), // BIGINT = signed Int64
        Field::new("_block_num_end", DataType::Int64, false), // BIGINT = signed Int64
        // Data columns
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

    // Create test data for multiple blocks (realistic batch size)
    // Simulating 100 blocks to test batch insert performance and correctness
    let num_blocks = 100;
    let base_timestamp = 1727914811000000i64;

    // System column vectors
    let mut ids = Vec::with_capacity(num_blocks);
    let mut block_num_starts = Vec::with_capacity(num_blocks);
    let mut block_num_ends = Vec::with_capacity(num_blocks);

    // Data column vectors
    let mut timestamps = Vec::with_capacity(num_blocks);
    let mut mix_hashes = Vec::with_capacity(num_blocks);
    let mut base_fees = Vec::with_capacity(num_blocks);
    let mut blob_gas_useds = Vec::with_capacity(num_blocks);
    let mut miners = Vec::with_capacity(num_blocks);
    let mut block_nums = Vec::with_capacity(num_blocks);
    let mut parent_hashes = Vec::with_capacity(num_blocks);
    let mut excess_blob_gases = Vec::with_capacity(num_blocks);
    let mut state_roots = Vec::with_capacity(num_blocks);
    let mut receipt_roots = Vec::with_capacity(num_blocks);
    let mut withdrawals_roots = Vec::with_capacity(num_blocks);
    let mut hashes = Vec::with_capacity(num_blocks);
    let mut difficulties = Vec::with_capacity(num_blocks);
    let mut ommers_hashes = Vec::with_capacity(num_blocks);
    let mut transactions_roots = Vec::with_capacity(num_blocks);
    let mut nonces = Vec::with_capacity(num_blocks);
    let mut parent_beacon_roots = Vec::with_capacity(num_blocks);
    let mut extra_datas = Vec::with_capacity(num_blocks);
    let mut gas_useds = Vec::with_capacity(num_blocks);
    let mut logs_blooms = Vec::with_capacity(num_blocks);
    let mut gas_limits = Vec::with_capacity(num_blocks);

    for i in 0..num_blocks {
        // Generate system columns
        // _id: Create a synthetic 16-byte deterministic ID based on block number
        let mut id = vec![0u8; 16];
        id[0..8].copy_from_slice(&(i as u64).to_le_bytes()); // Use block index as unique ID
        ids.push(id);

        // _block_num_start and _block_num_end: Use actual block numbers
        let block_number = (i + 1) as u64; // Blocks 1-100
        block_num_starts.push(block_number);
        block_num_ends.push(block_number);

        // Generate data columns
        timestamps.push(base_timestamp + (i as i64 * 12_000_000)); // 12 second block time
        mix_hashes.push(vec![0u8; 32]);
        base_fees.push(1000000000i128 + (i as i128 * 1000)); // Increasing base fee
        blob_gas_useds.push(i as u64);
        miners.push(vec![0u8; 20]);
        block_nums.push((i + 1) as u64); // Blocks 1-100
        parent_hashes.push(vec![0u8; 32]);
        excess_blob_gases.push(0u64);
        state_roots.push(vec![0u8; 32]);
        receipt_roots.push(vec![0u8; 32]);
        withdrawals_roots.push(vec![0u8; 32]);
        hashes.push(vec![(i + 1) as u8; 32]); // Unique hash per block
        difficulties.push(0i128);
        ommers_hashes.push(vec![0u8; 32]);
        transactions_roots.push(vec![0u8; 32]);
        nonces.push(i as u64);
        parent_beacon_roots.push(vec![0u8; 32]);
        extra_datas.push(vec![0u8; 0]);
        gas_useds.push(21000u64 + (i as u64 * 1000)); // Varying gas usage
        logs_blooms.push(vec![0u8; 256]);
        gas_limits.push(30000000u64);
    }

    // Create system column arrays
    let id_array = BinaryArray::from(ids.iter().map(|v| v.as_slice()).collect::<Vec<_>>());
    let block_num_start_array = Int64Array::from(
        block_num_starts
            .iter()
            .map(|&x| x as i64)
            .collect::<Vec<_>>(),
    );
    let block_num_end_array =
        Int64Array::from(block_num_ends.iter().map(|&x| x as i64).collect::<Vec<_>>());

    // Create data column arrays
    let timestamp = TimestampMicrosecondArray::from(timestamps).with_timezone("+00:00");
    let mix_hash = FixedSizeBinaryArray::try_from_iter(mix_hashes.into_iter()).unwrap();
    let base_fee_per_gas = Decimal128Array::from(base_fees)
        .with_precision_and_scale(38, 0)
        .unwrap();
    let blob_gas_used = UInt64Array::from(blob_gas_useds);
    let miner = FixedSizeBinaryArray::try_from_iter(miners.into_iter()).unwrap();
    let block_num = UInt64Array::from(block_nums);
    let parent_hash = FixedSizeBinaryArray::try_from_iter(parent_hashes.into_iter()).unwrap();
    let excess_blob_gas = UInt64Array::from(excess_blob_gases);
    let state_root = FixedSizeBinaryArray::try_from_iter(state_roots.into_iter()).unwrap();
    let receipt_root = FixedSizeBinaryArray::try_from_iter(receipt_roots.into_iter()).unwrap();
    let withdrawals_root =
        FixedSizeBinaryArray::try_from_iter(withdrawals_roots.into_iter()).unwrap();
    let hash = FixedSizeBinaryArray::try_from_iter(hashes.into_iter()).unwrap();
    let difficulty = Decimal128Array::from(difficulties)
        .with_precision_and_scale(38, 0)
        .unwrap();
    let ommers_hash = FixedSizeBinaryArray::try_from_iter(ommers_hashes.into_iter()).unwrap();
    let transactions_root =
        FixedSizeBinaryArray::try_from_iter(transactions_roots.into_iter()).unwrap();
    let nonce = UInt64Array::from(nonces);
    let parent_beacon_root =
        FixedSizeBinaryArray::try_from_iter(parent_beacon_roots.into_iter()).unwrap();
    let extra_data =
        BinaryArray::from(extra_datas.iter().map(|v| v.as_slice()).collect::<Vec<_>>());
    let gas_used = UInt64Array::from(gas_useds);
    let logs_bloom =
        BinaryArray::from(logs_blooms.iter().map(|v| v.as_slice()).collect::<Vec<_>>());
    let gas_limit = UInt64Array::from(gas_limits);

    let batch = RecordBatch::try_new(
        schema,
        vec![
            // System columns (must match schema order)
            Arc::new(id_array),
            Arc::new(block_num_start_array),
            Arc::new(block_num_end_array),
            // Data columns
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

    // This should use our arrow_to_pg encoder
    match db_engine.insert_record_batch("blocks", &batch).await {
        Ok(()) => {
            println!("Successfully inserted batch!");

            // Verify the data was inserted correctly
            // Note: ORDER BY block_num (numeric) not block_num::TEXT (lexicographic)
            let rows: Vec<(String, String, String, String)> = sqlx::query_as(
                "SELECT difficulty::TEXT, base_fee_per_gas::TEXT, block_num::TEXT, gas_used::TEXT FROM blocks ORDER BY block_num::BIGINT",
            )
            .fetch_all(&pool)
            .await
            .expect("Failed to query inserted data");

            assert_eq!(rows.len(), 100, "Expected 100 blocks to be inserted");

            // Verify first block (block 1, i=0)
            assert_eq!(rows[0].0, "0"); // difficulty
            assert_eq!(rows[0].1, "1000000000"); // base_fee_per_gas (1000000000 + 0 * 1000)
            assert_eq!(rows[0].2, "1"); // block_num
            assert_eq!(rows[0].3, "21000"); // gas_used (21000 + 0 * 1000)

            // Verify last block (block 100, i=99)
            // Note: Each block i has: base_fee = 1000000000 + i*1000, gas_used = 21000 + i*1000
            let last_block_num: i64 = rows[99].2.parse().unwrap();
            let expected_base_fee = 1000000000 + ((last_block_num - 1) * 1000);
            let expected_gas = 21000 + ((last_block_num - 1) * 1000);
            assert_eq!(rows[99].0, "0"); // difficulty
            assert_eq!(rows[99].1, expected_base_fee.to_string()); // base_fee_per_gas
            assert_eq!(last_block_num, 100); // block_num
            assert_eq!(rows[99].3, expected_gas.to_string()); // gas_used

            // Verify a middle block (block 50, i=49)
            let mid_block_num: i64 = rows[49].2.parse().unwrap();
            let expected_mid_base_fee = 1000000000 + ((mid_block_num - 1) * 1000);
            let expected_mid_gas = 21000 + ((mid_block_num - 1) * 1000);
            assert_eq!(rows[49].0, "0"); // difficulty
            assert_eq!(rows[49].1, expected_mid_base_fee.to_string()); // base_fee_per_gas
            assert_eq!(mid_block_num, 50); // block_num
            assert_eq!(rows[49].3, expected_mid_gas.to_string()); // gas_used

            println!("Data verification passed for all 100 blocks!");
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
