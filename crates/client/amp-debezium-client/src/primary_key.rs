//! Primary key extraction from Arrow RecordBatch.
//!
//! This module provides functionality to extract composite primary keys from Arrow
//! RecordBatch rows and hash them into a stable RecordKey for deduplication and
//! state management.

use common::arrow::{
    array::{
        Array, BinaryArray, BooleanArray, Date32Array, FixedSizeBinaryArray, Float32Array,
        Float64Array, Int8Array, Int16Array, Int32Array, Int64Array, LargeBinaryArray,
        LargeStringArray, RecordBatch, StringArray, TimestampMicrosecondArray,
        TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray, UInt8Array,
        UInt16Array, UInt32Array, UInt64Array,
    },
    datatypes::{DataType, TimeUnit},
};
use xxhash_rust::xxh3::xxh3_128;

use crate::{
    error::{Error, Result},
    types::RecordKey,
};

/// Extractor for composite primary keys from Arrow RecordBatch rows.
///
/// Configures which columns to use as primary keys and provides methods to
/// extract and hash them into a stable RecordKey.
#[derive(Debug, Clone)]
pub struct PrimaryKeyExtractor {
    /// Names of columns that form the primary key
    key_columns: Vec<String>,
}

impl PrimaryKeyExtractor {
    /// Create a new primary key extractor.
    ///
    /// # Arguments
    /// * `key_columns` - Names of columns to use as primary key (e.g., ["block_num", "log_index"])
    ///
    /// # Example
    /// ```
    /// use amp_debezium::PrimaryKeyExtractor;
    ///
    /// let extractor =
    ///     PrimaryKeyExtractor::new(vec!["block_num".to_string(), "log_index".to_string()]);
    /// ```
    pub fn new(key_columns: Vec<String>) -> Self {
        Self { key_columns }
    }

    /// Extract and hash the primary key for a specific row in a RecordBatch.
    ///
    /// # Arguments
    /// * `batch` - The Arrow RecordBatch containing the data
    /// * `row_idx` - The row index to extract the key from
    ///
    /// # Returns
    /// A hashed RecordKey that uniquely identifies this row
    ///
    /// # Errors
    /// Returns an error if:
    /// - A key column is not found in the batch
    /// - A key column has an unsupported data type
    pub fn extract_key(&self, batch: &RecordBatch, row_idx: usize) -> Result<RecordKey> {
        let mut buf = Vec::with_capacity(128);

        // Serialize each key column value into the buffer
        for col_name in &self.key_columns {
            let column = batch
                .column_by_name(col_name)
                .ok_or_else(|| Error::PrimaryKey(format!("Column '{}' not found", col_name)))?;

            serialize_arrow_value(column.as_ref(), row_idx, &mut buf)?;
        }

        // Hash the serialized bytes to create a stable key
        let hash = xxh3_128(&buf);
        Ok(RecordKey::new(hash))
    }

    /// Get the list of primary key column names.
    pub fn key_columns(&self) -> &[String] {
        &self.key_columns
    }
}

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
///
/// # Errors
/// Returns an error if the Arrow data type is not supported for hashing
fn serialize_arrow_value(column: &dyn Array, row_idx: usize, buf: &mut Vec<u8>) -> Result<()> {
    // Handle NULL values with a special marker
    if column.is_null(row_idx) {
        buf.push(0x00); // NULL marker
        return Ok(());
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

        // Unsupported types
        other => {
            return Err(Error::PrimaryKey(format!(
                "Unsupported primary key column type: {:?}",
                other
            )));
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use common::arrow::{
        array::{Int64Array, RecordBatch, StringArray},
        datatypes::{DataType, Field, Schema},
    };

    use super::*;

    #[test]
    fn extract_single_column_key() {
        //* Given
        let schema = Arc::new(Schema::new(vec![Field::new(
            "block_num",
            DataType::Int64,
            false,
        )]));
        let array = Int64Array::from(vec![100, 200, 300]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(array)])
            .expect("create batch should succeed");

        let extractor = PrimaryKeyExtractor::new(vec!["block_num".to_string()]);

        //* When
        let key0 = extractor
            .extract_key(&batch, 0)
            .expect("extract key should succeed");
        let key1 = extractor
            .extract_key(&batch, 1)
            .expect("extract key should succeed");
        let key2 = extractor
            .extract_key(&batch, 2)
            .expect("extract key should succeed");

        //* Then
        assert_ne!(key0, key1);
        assert_ne!(key1, key2);
        assert_ne!(key0, key2);
    }

    #[test]
    fn extract_composite_key() {
        //* Given
        let schema = Arc::new(Schema::new(vec![
            Field::new("block_num", DataType::Int64, false),
            Field::new("log_index", DataType::Int64, false),
        ]));
        let block_nums = Int64Array::from(vec![100, 100, 200]);
        let log_indices = Int64Array::from(vec![0, 1, 0]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(block_nums), Arc::new(log_indices)])
            .expect("create batch should succeed");

        let extractor =
            PrimaryKeyExtractor::new(vec!["block_num".to_string(), "log_index".to_string()]);

        //* When
        let key0 = extractor
            .extract_key(&batch, 0)
            .expect("extract key should succeed");
        let key1 = extractor
            .extract_key(&batch, 1)
            .expect("extract key should succeed");
        let key2 = extractor
            .extract_key(&batch, 2)
            .expect("extract key should succeed");

        //* Then
        // All keys should be different
        assert_ne!(key0, key1);
        assert_ne!(key1, key2);
        assert_ne!(key0, key2);
    }

    #[test]
    fn extract_key_with_string_column() {
        //* Given
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        let ids = Int64Array::from(vec![1, 2, 3]);
        let names = StringArray::from(vec!["alice", "bob", "charlie"]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(ids), Arc::new(names)]).unwrap();

        let extractor = PrimaryKeyExtractor::new(vec!["id".to_string(), "name".to_string()]);

        //* When
        let key0 = extractor.extract_key(&batch, 0).unwrap();
        let key1 = extractor.extract_key(&batch, 1).unwrap();

        //* Then
        assert_ne!(key0, key1);
    }

    #[test]
    fn extract_key_missing_column() {
        //* Given
        let schema = Arc::new(Schema::new(vec![Field::new(
            "block_num",
            DataType::Int64,
            false,
        )]));
        let array = Int64Array::from(vec![100]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(array)]).unwrap();

        let extractor = PrimaryKeyExtractor::new(vec!["missing_column".to_string()]);

        //* When
        let result = extractor.extract_key(&batch, 0);

        //* Then
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Column 'missing_column' not found")
        );
    }

    #[test]
    fn same_values_produce_same_key() {
        //* Given
        let schema = Arc::new(Schema::new(vec![Field::new(
            "block_num",
            DataType::Int64,
            false,
        )]));
        let array1 = Int64Array::from(vec![100, 200]);
        let batch1 = RecordBatch::try_new(schema.clone(), vec![Arc::new(array1)]).unwrap();

        let array2 = Int64Array::from(vec![100, 200]);
        let batch2 = RecordBatch::try_new(schema, vec![Arc::new(array2)]).unwrap();

        let extractor = PrimaryKeyExtractor::new(vec!["block_num".to_string()]);

        //* When
        let key1_0 = extractor.extract_key(&batch1, 0).unwrap();
        let key2_0 = extractor.extract_key(&batch2, 0).unwrap();

        //* Then
        assert_eq!(key1_0, key2_0);
    }
}
