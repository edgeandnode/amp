use std::sync::Arc;

use arrow_array::{
    Array, BinaryArray, BooleanArray, Date32Array, Decimal128Array, FixedSizeBinaryArray,
    Float32Array, Float64Array, Int8Array, Int16Array, Int32Array, Int64Array, LargeBinaryArray,
    LargeStringArray, RecordBatch, StringArray, TimestampMicrosecondArray,
    TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray, UInt8Array,
    UInt16Array, UInt32Array, UInt64Array, builder::PrimitiveBuilder,
    types::TimestampMicrosecondType,
};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use common::{BoxError, metadata::segments::BlockRange};
use xxhash_rust::xxh3::xxh3_128;

/// Maximum number of rows allowed in a single batch to prevent OOM.
const MAX_BATCH_ROWS: usize = 50_000;

/// Maximum batch size in bytes (100MB) to prevent OOM.
const MAX_BATCH_BYTES: usize = 100 * 1024 * 1024;

/// Serialize an Arrow column value at a specific row index to bytes for deterministic hashing.
///
/// This function provides a portable, deterministic serialization of Arrow values that:
/// - Works consistently across platforms and Rust versions
/// - Handles all Arrow data types properly
/// - Uses little-endian byte order for numeric types
/// - Includes length prefixes for variable-length data
///
/// # Arguments
/// * `column` - The Arrow array column
/// * `row_idx` - The row index to serialize
/// * `buf` - The buffer to write serialized bytes to
fn serialize_arrow_value(column: &dyn Array, row_idx: usize, buf: &mut Vec<u8>) {
    // Handle NULL values with a special marker
    if column.is_null(row_idx) {
        buf.push(0x00); // NULL marker
        return;
    }

    buf.push(0x01); // NOT NULL marker

    // Serialize based on Arrow data type
    match column.data_type() {
        // Integer types - use little-endian bytes
        DataType::Int8 => {
            let arr = column.as_any().downcast_ref::<Int8Array>().unwrap();
            buf.push(arr.value(row_idx) as u8);
        }
        DataType::Int16 => {
            let arr = column.as_any().downcast_ref::<Int16Array>().unwrap();
            buf.extend_from_slice(&arr.value(row_idx).to_le_bytes());
        }
        DataType::Int32 => {
            let arr = column.as_any().downcast_ref::<Int32Array>().unwrap();
            buf.extend_from_slice(&arr.value(row_idx).to_le_bytes());
        }
        DataType::Int64 => {
            let arr = column.as_any().downcast_ref::<Int64Array>().unwrap();
            buf.extend_from_slice(&arr.value(row_idx).to_le_bytes());
        }
        DataType::UInt8 => {
            let arr = column.as_any().downcast_ref::<UInt8Array>().unwrap();
            buf.push(arr.value(row_idx));
        }
        DataType::UInt16 => {
            let arr = column.as_any().downcast_ref::<UInt16Array>().unwrap();
            buf.extend_from_slice(&arr.value(row_idx).to_le_bytes());
        }
        DataType::UInt32 => {
            let arr = column.as_any().downcast_ref::<UInt32Array>().unwrap();
            buf.extend_from_slice(&arr.value(row_idx).to_le_bytes());
        }
        DataType::UInt64 => {
            let arr = column.as_any().downcast_ref::<UInt64Array>().unwrap();
            buf.extend_from_slice(&arr.value(row_idx).to_le_bytes());
        }

        // Float types - use to_le_bytes for bit-exact representation
        DataType::Float32 => {
            let arr = column.as_any().downcast_ref::<Float32Array>().unwrap();
            buf.extend_from_slice(&arr.value(row_idx).to_le_bytes());
        }
        DataType::Float64 => {
            let arr = column.as_any().downcast_ref::<Float64Array>().unwrap();
            buf.extend_from_slice(&arr.value(row_idx).to_le_bytes());
        }

        // Boolean - single byte
        DataType::Boolean => {
            let arr = column.as_any().downcast_ref::<BooleanArray>().unwrap();
            buf.push(if arr.value(row_idx) { 1 } else { 0 });
        }

        // String types - length prefix + UTF-8 bytes
        DataType::Utf8 => {
            let arr = column.as_any().downcast_ref::<StringArray>().unwrap();
            let val = arr.value(row_idx);
            buf.extend_from_slice(&(val.len() as u32).to_le_bytes());
            buf.extend_from_slice(val.as_bytes());
        }
        DataType::LargeUtf8 => {
            let arr = column.as_any().downcast_ref::<LargeStringArray>().unwrap();
            let val = arr.value(row_idx);
            buf.extend_from_slice(&(val.len() as u64).to_le_bytes());
            buf.extend_from_slice(val.as_bytes());
        }

        // Binary types - length prefix + raw bytes
        DataType::Binary => {
            let arr = column.as_any().downcast_ref::<BinaryArray>().unwrap();
            let val = arr.value(row_idx);
            buf.extend_from_slice(&(val.len() as u32).to_le_bytes());
            buf.extend_from_slice(val);
        }
        DataType::LargeBinary => {
            let arr = column.as_any().downcast_ref::<LargeBinaryArray>().unwrap();
            let val = arr.value(row_idx);
            buf.extend_from_slice(&(val.len() as u64).to_le_bytes());
            buf.extend_from_slice(val);
        }
        DataType::FixedSizeBinary(_) => {
            let arr = column
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .unwrap();
            let val = arr.value(row_idx);
            buf.extend_from_slice(val);
        }

        // Date/Time types - serialize as i32/i64
        DataType::Date32 => {
            let arr = column.as_any().downcast_ref::<Date32Array>().unwrap();
            buf.extend_from_slice(&arr.value(row_idx).to_le_bytes());
        }
        DataType::Timestamp(TimeUnit::Second, _) => {
            let arr = column
                .as_any()
                .downcast_ref::<TimestampSecondArray>()
                .unwrap();
            buf.extend_from_slice(&arr.value(row_idx).to_le_bytes());
        }
        DataType::Timestamp(TimeUnit::Millisecond, _) => {
            let arr = column
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .unwrap();
            buf.extend_from_slice(&arr.value(row_idx).to_le_bytes());
        }
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            let arr = column
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .unwrap();
            buf.extend_from_slice(&arr.value(row_idx).to_le_bytes());
        }
        DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            let arr = column
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .unwrap();
            buf.extend_from_slice(&arr.value(row_idx).to_le_bytes());
        }

        // Decimal type - serialize as i128
        DataType::Decimal128(_, _) => {
            let arr = column.as_any().downcast_ref::<Decimal128Array>().unwrap();
            buf.extend_from_slice(&arr.value(row_idx).to_le_bytes());
        }

        // Unsupported types - fail loudly
        other => {
            panic!(
                "Unsupported Arrow type for hashing: {:?}. \
                Please add support for this type in serialize_arrow_value()",
                other
            );
        }
    }
}

/// Convert nanosecond timestamps to microseconds in a RecordBatch for PostgreSQL compatibility.
///
/// PostgreSQL only supports microsecond precision, but Nozzle may return nanosecond timestamps.
/// This function converts any Timestamp(Nanosecond) columns to Timestamp(Microsecond) by
/// dividing the values by 1000.
///
/// Takes ownership of the batch to avoid cloning when no conversion is needed.
pub fn convert_nanosecond_timestamps(batch: RecordBatch) -> Result<RecordBatch, BoxError> {
    let schema = batch.schema();
    let mut needs_conversion = false;

    // Check if any fields need conversion
    for field in schema.fields() {
        if matches!(
            field.data_type(),
            DataType::Timestamp(TimeUnit::Nanosecond, _)
        ) {
            needs_conversion = true;
            break;
        }
    }

    // If no conversion needed, return the original batch without cloning
    if !needs_conversion {
        return Ok(batch);
    }

    // Build new schema and convert columns
    let mut new_fields = Vec::new();
    let mut new_columns = Vec::new();

    for (i, field) in schema.fields().iter().enumerate() {
        match field.data_type() {
            DataType::Timestamp(TimeUnit::Nanosecond, tz) => {
                // Convert nanoseconds to microseconds
                let ns_array = batch
                    .column(i)
                    .as_any()
                    .downcast_ref::<TimestampNanosecondArray>()
                    .ok_or("Failed to downcast to TimestampNanosecondArray")?;

                // Build the microsecond array directly without intermediate Vec allocation
                // This is much more efficient for large batches (avoids 16MB+ allocations)
                let mut builder =
                    PrimitiveBuilder::<TimestampMicrosecondType>::with_capacity(ns_array.len());
                for row in 0..ns_array.len() {
                    if ns_array.is_null(row) {
                        builder.append_null();
                    } else {
                        builder.append_value(ns_array.value(row) / 1000);
                    }
                }
                let us_array = builder.finish().with_timezone_opt(tz.clone());

                // Create new field with microsecond precision
                new_fields.push(Arc::new(Field::new(
                    field.name(),
                    DataType::Timestamp(TimeUnit::Microsecond, tz.clone()),
                    field.is_nullable(),
                )));

                new_columns.push(Arc::new(us_array) as _);
            }
            _ => {
                // Keep the field and column as-is
                new_fields.push(Arc::new((**field).clone()));
                new_columns.push(batch.column(i).clone());
            }
        }
    }

    // Create new schema and record batch
    let new_schema = Arc::new(Schema::new(new_fields));
    let new_batch = RecordBatch::try_new(new_schema, new_columns)?;

    Ok(new_batch)
}

/// Inject system metadata columns into RecordBatch.
///
/// Three system columns are ALWAYS injected:
/// - `_id` (BYTEA): Deterministic hash of (row_content + block_range + row_index) for PRIMARY KEY
/// - `_block_num_start` (BIGINT): First block in the batch range
/// - `_block_num_end` (BIGINT): Last block in the batch range
///
/// These columns enable:
/// - Deduplication on reconnect (deterministic `_id`)
/// - Reorg handling (delete WHERE `_block_num_end` >= reorg_block)
/// - Checkpointing (tracking progress via block ranges)
///
/// If the user's query includes `block_num`, it's preserved as a separate column.
/// An index is created on `block_num` for query performance (in DDL generation).
///
/// Takes ownership of the batch.
///
/// # Batch Size Limits
/// Rejects batches that exceed safety limits to prevent OOM:
/// - Max rows: 50,000
/// - Max size: 100MB (approximate)
pub fn inject_system_metadata(
    batch: RecordBatch,
    ranges: &[BlockRange],
) -> Result<RecordBatch, BoxError> {
    // Extract block range from metadata
    let block_num_start = ranges
        .iter()
        .map(|r| r.start())
        .min()
        .ok_or("No block ranges provided for metadata injection")?;

    let block_num_end = ranges
        .iter()
        .map(|r| r.end())
        .max()
        .ok_or("No block ranges provided for metadata injection")?;

    let num_rows = batch.num_rows();

    // Validate batch size to prevent OOM
    if num_rows > MAX_BATCH_ROWS {
        return Err(format!(
            "Batch size exceeds maximum allowed rows: {} > {}. \
            Please configure smaller batch sizes in your data source.",
            num_rows, MAX_BATCH_ROWS
        )
        .into());
    }

    let estimated_bytes = batch.get_array_memory_size();
    if estimated_bytes > MAX_BATCH_BYTES {
        return Err(format!(
            "Batch size exceeds maximum allowed bytes: {} > {} ({}MB > {}MB). \
            Please configure smaller batch sizes in your data source.",
            estimated_bytes,
            MAX_BATCH_BYTES,
            estimated_bytes / (1024 * 1024),
            MAX_BATCH_BYTES / (1024 * 1024)
        )
        .into());
    }

    let schema = batch.schema();

    // Create _id array: hash(row_content + block_range + row_index)
    // Pre-allocate with capacity and reuse buffer for performance
    let mut row_hashes = Vec::with_capacity(num_rows);

    // Reusable buffer for hash input - avoids allocation on every row
    // Start with 1KB which covers most row sizes
    let mut hasher_input = Vec::with_capacity(1024);

    for row_idx in 0..num_rows {
        hasher_input.clear(); // Reuse buffer

        // Serialize all column values for this row using deterministic Arrow serialization
        for col_idx in 0..batch.num_columns() {
            let column = batch.column(col_idx);
            serialize_arrow_value(column.as_ref(), row_idx, &mut hasher_input);
        }

        // Add block range to hash input
        hasher_input.extend_from_slice(&block_num_start.to_le_bytes());
        hasher_input.extend_from_slice(&block_num_end.to_le_bytes());

        // Add row index to hash input (ensures uniqueness within batch)
        hasher_input.extend_from_slice(&(row_idx as u64).to_le_bytes());

        // Compute xxh3 128-bit hash (16 bytes)
        let hash = xxh3_128(&hasher_input);
        row_hashes.push(hash.to_le_bytes().to_vec());
    }

    // Create arrays for system columns
    let id_array = BinaryArray::from(row_hashes.iter().map(|h| h.as_slice()).collect::<Vec<_>>());
    let block_start_array = Int64Array::from(vec![block_num_start as i64; num_rows]);
    let block_end_array = Int64Array::from(vec![block_num_end as i64; num_rows]);

    // Build new schema with system columns as first three columns
    let mut new_fields = vec![
        Arc::new(Field::new("_id", DataType::Binary, false)),
        Arc::new(Field::new("_block_num_start", DataType::Int64, false)),
        Arc::new(Field::new("_block_num_end", DataType::Int64, false)),
    ];
    new_fields.extend(schema.fields().iter().cloned());

    // Build new columns with system columns first
    let mut new_columns: Vec<Arc<dyn Array>> = vec![
        Arc::new(id_array),
        Arc::new(block_start_array),
        Arc::new(block_end_array),
    ];
    for i in 0..batch.num_columns() {
        new_columns.push(batch.column(i).clone());
    }

    // Create new schema and record batch
    let new_schema = Arc::new(Schema::new(new_fields));
    let new_batch = RecordBatch::try_new(new_schema, new_columns)?;

    Ok(new_batch)
}
