//! Transaction metadata injection for Arrow RecordBatches.
//!
//! This module provides utilities to add transaction metadata columns
//! (`_tx_id` and `_row_index`) to Arrow RecordBatches before insertion
//! into PostgreSQL.
//!
//! # Transaction Metadata
//!
//! - `_tx_id`: Transaction ID from amp-client's TransactionalStream
//! - `_row_index`: Row index within the transaction (0-based)
//!
//! Together these form a composite primary key that ensures:
//! - Idempotent inserts (duplicate inserts are rejected by PK constraint)
//! - Crash safety (uncommitted data can be identified and deleted)
//! - Proper ordering within transactions
//!
//! # PostgreSQL COPY Protocol
//!
//! The module also provides conversion to PostgreSQL COPY binary format
//! for efficient bulk insertion using the `arrow-to-postgres` crate.

use std::sync::Arc;

use arrow_array::{Array, Int32Array, Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use arrow_to_postgres::ArrowToPostgresBinaryEncoder;
use bytes::BytesMut;

/// Errors that occur when adding transaction metadata to RecordBatch
#[derive(Debug, thiserror::Error)]
pub enum AddTransactionMetadataError {
    /// Failed to create RecordBatch with transaction metadata
    #[error("Failed to create RecordBatch with transaction metadata columns")]
    CreateRecordBatch(#[source] arrow_schema::ArrowError),
}

/// Errors that occur when converting RecordBatch to PostgreSQL COPY format
#[derive(Debug, thiserror::Error)]
pub enum ToPostgresCopyError {
    /// Failed to create PostgreSQL encoder for Arrow schema
    #[error("Failed to create PostgreSQL encoder for Arrow schema")]
    CreateEncoder(#[source] arrow_to_postgres::error::Error),

    /// Failed to encode RecordBatch
    #[error("Failed to encode RecordBatch to PostgreSQL COPY format")]
    EncodeBatch(#[source] arrow_to_postgres::error::Error),
}

/// Add transaction metadata columns to a RecordBatch.
///
/// This function prepends `_tx_id` and `_row_index` columns to the batch,
/// creating a new RecordBatch with an extended schema. The metadata columns
/// are added as the first two columns to match the PostgreSQL table schema.
///
/// # Arguments
///
/// - `batch`: Original RecordBatch from the stream
/// - `tx_id`: Transaction ID from TransactionEvent::Data
///
/// # Returns
///
/// A new RecordBatch with metadata columns prepended.
/// Schema: `(_tx_id: Int64, _row_index: Int32, ...original columns)`
///
/// # Errors
///
/// Returns an error if:
/// - Schema creation fails
/// - RecordBatch construction fails
pub fn add_transaction_metadata(
    batch: RecordBatch,
    tx_id: u64,
) -> Result<RecordBatch, AddTransactionMetadataError> {
    let num_rows = batch.num_rows();

    // _tx_id: same for all rows in batch
    let tx_id_array = Arc::new(Int64Array::from(vec![tx_id as i64; num_rows]));

    // _row_index: 0, 1, 2, ..., num_rows-1
    let row_indices: Vec<i32> = (0..num_rows as i32).collect();
    let row_index_array = Arc::new(Int32Array::from(row_indices));

    // Build new schema: (_tx_id, _row_index, user_cols...)
    let mut fields = vec![
        Arc::new(Field::new("_tx_id", DataType::Int64, false)),
        Arc::new(Field::new("_row_index", DataType::Int32, false)),
    ];
    fields.extend(batch.schema().fields().iter().cloned());

    let new_schema = Arc::new(Schema::new(fields));

    // Build new columns
    let mut columns = vec![
        tx_id_array as Arc<dyn Array>,
        row_index_array as Arc<dyn Array>,
    ];
    columns.extend(batch.columns().iter().cloned());

    RecordBatch::try_new(new_schema, columns)
        .map_err(AddTransactionMetadataError::CreateRecordBatch)
}

/// Convert RecordBatch to PostgreSQL COPY binary format.
///
/// This function uses the `arrow-to-postgres` crate to efficiently encode
/// an Arrow RecordBatch into PostgreSQL COPY binary format for bulk insertion.
///
/// # Arguments
///
/// - `batch`: RecordBatch to convert (should include metadata columns)
///
/// # Returns
///
/// A `BytesMut` buffer containing the encoded data in PostgreSQL COPY format.
///
/// # Errors
///
/// Returns an error if:
/// - Encoder initialization fails (schema incompatible with PostgreSQL)
/// - Batch encoding fails (data type conversion errors)
///
/// # Performance
///
/// The COPY protocol is significantly faster than individual INSERT statements,
/// especially for large batches. Typical performance improvements are 10-100x
/// depending on batch size and network latency.
pub fn to_postgres_copy(batch: RecordBatch) -> Result<BytesMut, ToPostgresCopyError> {
    let encoder = ArrowToPostgresBinaryEncoder::try_new(batch.schema().as_ref())
        .map_err(ToPostgresCopyError::CreateEncoder)?;
    let (buffer, _finished) = encoder
        .encode_batch(&batch)
        .map_err(ToPostgresCopyError::EncodeBatch)?;
    Ok(buffer)
}

#[cfg(test)]
mod tests {
    use arrow_array::StringArray;

    use super::*;

    #[test]
    fn test_add_transaction_metadata() {
        // Create test batch
        let schema = Arc::new(Schema::new(vec![Field::new("name", DataType::Utf8, false)]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(StringArray::from(vec!["Alice", "Bob"]))],
        )
        .unwrap();

        // Add metadata
        let result = add_transaction_metadata(batch, 42).unwrap();

        // Verify schema
        assert_eq!(result.num_columns(), 3);
        assert_eq!(result.schema().field(0).name(), "_tx_id");
        assert_eq!(result.schema().field(1).name(), "_row_index");
        assert_eq!(result.schema().field(2).name(), "name");

        // Verify data
        assert_eq!(result.num_rows(), 2);
        let tx_ids = result
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(tx_ids.value(0), 42);
        assert_eq!(tx_ids.value(1), 42);

        let row_indices = result
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(row_indices.value(0), 0);
        assert_eq!(row_indices.value(1), 1);
    }
}
