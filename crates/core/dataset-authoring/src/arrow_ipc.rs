//! Arrow schema serialization in IPC file format.
//!
//! This module handles serialization and deserialization of Arrow schemas to IPC files.
//! The IPC (Inter-Process Communication) file format is Arrow's native binary format,
//! providing efficient, lossless schema storage that preserves all Arrow type information.
//!
//! # Schema File Format
//!
//! Schema files are stored as `tables/<table>.ipc` and contain an Arrow IPC file
//! with schema metadata only (no record batches). This format:
//!
//! - Is Arrow's native binary format
//! - Preserves all Arrow type information losslessly
//! - Is compact and efficient
//! - Is compatible with Arrow tooling across languages
//!
//! # Example
//!
//! ```ignore
//! use dataset_authoring::arrow_ipc::{write_ipc_schema, read_ipc_schema};
//! use datafusion::arrow::datatypes::{Schema, Field, DataType};
//! use std::sync::Arc;
//!
//! // Create a schema
//! let schema = Arc::new(Schema::new(vec![
//!     Field::new("id", DataType::UInt64, false),
//!     Field::new("name", DataType::Utf8, true),
//! ]));
//!
//! // Write schema to file
//! write_ipc_schema(&schema, "tables/transfers.ipc")?;
//!
//! // Read schema from file
//! let loaded_schema = read_ipc_schema("tables/transfers.ipc")?;
//! ```

use std::{
    fs::File,
    io::{self, BufReader, BufWriter},
    path::Path,
};

use datafusion::arrow::{
    datatypes::SchemaRef,
    ipc::{reader::FileReader, writer::FileWriter},
};

/// Errors that can occur during IPC schema file operations.
#[derive(Debug, thiserror::Error)]
pub enum IpcSchemaError {
    /// Failed to read or write the schema file.
    #[error("I/O error accessing IPC schema file")]
    Io(#[from] io::Error),

    /// Failed to create the IPC file writer.
    #[error("failed to create IPC file writer")]
    WriterCreation(#[source] datafusion::arrow::error::ArrowError),

    /// Failed to finish writing the IPC file.
    #[error("failed to finish writing IPC file")]
    WriterFinish(#[source] datafusion::arrow::error::ArrowError),

    /// Failed to create the IPC file reader.
    #[error("failed to create IPC file reader")]
    ReaderCreation(#[source] datafusion::arrow::error::ArrowError),
}

/// Writes an Arrow schema to an IPC file.
///
/// The schema is written as an Arrow IPC file with no record batches. This preserves
/// all Arrow type information losslessly in a compact binary format.
///
/// # Arguments
///
/// * `schema` - The Arrow schema to write.
/// * `path` - The path to write the IPC file to.
///
/// # Errors
///
/// Returns an error if the file cannot be created or the schema cannot be written.
pub fn write_ipc_schema(schema: &SchemaRef, path: impl AsRef<Path>) -> Result<(), IpcSchemaError> {
    let file = File::create(path.as_ref())?;
    let writer = BufWriter::new(file);

    let mut ipc_writer =
        FileWriter::try_new(writer, schema).map_err(IpcSchemaError::WriterCreation)?;

    // Write schema only - no record batches
    ipc_writer.finish().map_err(IpcSchemaError::WriterFinish)?;

    Ok(())
}

/// Reads an Arrow schema from an IPC file.
///
/// # Arguments
///
/// * `path` - The path to read the IPC file from.
///
/// # Returns
///
/// The Arrow schema contained in the IPC file.
///
/// # Errors
///
/// Returns an error if the file cannot be read or the IPC data is invalid.
pub fn read_ipc_schema(path: impl AsRef<Path>) -> Result<SchemaRef, IpcSchemaError> {
    let file = File::open(path.as_ref())?;
    let reader = BufReader::new(file);

    let ipc_reader = FileReader::try_new(reader, None).map_err(IpcSchemaError::ReaderCreation)?;

    Ok(ipc_reader.schema())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit};

    use super::*;

    /// Creates a simple test schema with basic types.
    fn simple_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::UInt64, false),
            Field::new("name", DataType::Utf8, true),
        ]))
    }

    /// Creates a schema with complex types.
    fn complex_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("block_num", DataType::UInt64, false),
            Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Nanosecond, Some("+00:00".into())),
                false,
            ),
            Field::new("hash", DataType::FixedSizeBinary(32), false),
            Field::new("value", DataType::Decimal128(38, 0), true),
        ]))
    }

    /// Creates a schema with nested types.
    fn nested_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::UInt64, false),
            Field::new(
                "tags",
                DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
                true,
            ),
            Field::new(
                "metadata",
                DataType::Struct(
                    vec![
                        Field::new("key", DataType::Utf8, false),
                        Field::new("value", DataType::Utf8, true),
                    ]
                    .into(),
                ),
                true,
            ),
        ]))
    }

    mod roundtrip_tests {
        use super::*;

        #[test]
        fn roundtrips_simple_schema() {
            //* Given
            let original = simple_schema();
            let temp_dir = std::env::temp_dir();
            let file_path = temp_dir.join("test_simple_schema.ipc");

            //* When
            write_ipc_schema(&original, &file_path).expect("write should succeed");
            let restored = read_ipc_schema(&file_path).expect("read should succeed");

            //* Then
            assert_eq!(original.fields().len(), restored.fields().len());
            for (orig, rest) in original.fields().iter().zip(restored.fields().iter()) {
                assert_eq!(orig.name(), rest.name());
                assert_eq!(orig.data_type(), rest.data_type());
                assert_eq!(orig.is_nullable(), rest.is_nullable());
            }

            // Cleanup
            let _ = std::fs::remove_file(&file_path);
        }

        #[test]
        fn roundtrips_complex_schema() {
            //* Given
            let original = complex_schema();
            let temp_dir = std::env::temp_dir();
            let file_path = temp_dir.join("test_complex_schema.ipc");

            //* When
            write_ipc_schema(&original, &file_path).expect("write should succeed");
            let restored = read_ipc_schema(&file_path).expect("read should succeed");

            //* Then
            assert_eq!(original.fields().len(), restored.fields().len());
            for (orig, rest) in original.fields().iter().zip(restored.fields().iter()) {
                assert_eq!(orig.name(), rest.name(), "field names should match");
                assert_eq!(
                    orig.data_type(),
                    rest.data_type(),
                    "types should match for field {}",
                    orig.name()
                );
                assert_eq!(
                    orig.is_nullable(),
                    rest.is_nullable(),
                    "nullability should match"
                );
            }

            // Cleanup
            let _ = std::fs::remove_file(&file_path);
        }

        #[test]
        fn roundtrips_nested_schema() {
            //* Given
            let original = nested_schema();
            let temp_dir = std::env::temp_dir();
            let file_path = temp_dir.join("test_nested_schema.ipc");

            //* When
            write_ipc_schema(&original, &file_path).expect("write should succeed");
            let restored = read_ipc_schema(&file_path).expect("read should succeed");

            //* Then
            assert_eq!(original.fields().len(), restored.fields().len());
            for (orig, rest) in original.fields().iter().zip(restored.fields().iter()) {
                assert_eq!(orig.name(), rest.name(), "field names should match");
                assert_eq!(
                    orig.data_type(),
                    rest.data_type(),
                    "types should match for field {}",
                    orig.name()
                );
            }

            // Cleanup
            let _ = std::fs::remove_file(&file_path);
        }

        #[test]
        fn roundtrips_empty_schema() {
            //* Given
            let original = Arc::new(Schema::empty());
            let temp_dir = std::env::temp_dir();
            let file_path = temp_dir.join("test_empty_schema.ipc");

            //* When
            write_ipc_schema(&original, &file_path).expect("write should succeed");
            let restored = read_ipc_schema(&file_path).expect("read should succeed");

            //* Then
            assert!(restored.fields().is_empty());

            // Cleanup
            let _ = std::fs::remove_file(&file_path);
        }

        #[test]
        fn roundtrips_schema_with_metadata() {
            //* Given
            let schema = Schema::new(vec![Field::new("id", DataType::UInt64, false)]);
            let mut metadata = std::collections::HashMap::new();
            metadata.insert("key1".to_string(), "value1".to_string());
            metadata.insert("key2".to_string(), "value2".to_string());
            let original = Arc::new(schema.with_metadata(metadata));

            let temp_dir = std::env::temp_dir();
            let file_path = temp_dir.join("test_schema_with_metadata.ipc");

            //* When
            write_ipc_schema(&original, &file_path).expect("write should succeed");
            let restored = read_ipc_schema(&file_path).expect("read should succeed");

            //* Then
            assert_eq!(original.metadata(), restored.metadata());

            // Cleanup
            let _ = std::fs::remove_file(&file_path);
        }
    }

    mod error_tests {
        use super::*;

        #[test]
        fn returns_error_for_nonexistent_file() {
            //* Given
            let path = "/nonexistent/path/to/schema.ipc";

            //* When
            let result = read_ipc_schema(path);

            //* Then
            assert!(result.is_err());
            assert!(matches!(result.unwrap_err(), IpcSchemaError::Io(_)));
        }

        #[test]
        fn returns_error_for_invalid_ipc_file() {
            //* Given
            let temp_dir = std::env::temp_dir();
            let file_path = temp_dir.join("invalid_schema.ipc");
            std::fs::write(&file_path, b"not a valid IPC file").expect("write should succeed");

            //* When
            let result = read_ipc_schema(&file_path);

            //* Then
            assert!(result.is_err());
            assert!(matches!(
                result.unwrap_err(),
                IpcSchemaError::ReaderCreation(_)
            ));

            // Cleanup
            let _ = std::fs::remove_file(&file_path);
        }

        #[test]
        fn returns_error_for_unwritable_path() {
            //* Given
            let schema = simple_schema();
            let path = "/nonexistent/directory/schema.ipc";

            //* When
            let result = write_ipc_schema(&schema, path);

            //* Then
            assert!(result.is_err());
            assert!(matches!(result.unwrap_err(), IpcSchemaError::Io(_)));
        }
    }

    mod file_format_tests {
        use super::*;

        #[test]
        fn writes_valid_ipc_file() {
            //* Given
            let schema = simple_schema();
            let temp_dir = std::env::temp_dir();
            let file_path = temp_dir.join("test_valid_ipc.ipc");

            //* When
            write_ipc_schema(&schema, &file_path).expect("write should succeed");
            let contents = std::fs::read(&file_path).expect("read should succeed");

            //* Then
            // IPC files start with "ARROW1" magic bytes
            assert!(contents.len() >= 6, "file should have magic bytes");
            assert_eq!(&contents[0..6], b"ARROW1", "should have IPC magic bytes");

            // Cleanup
            let _ = std::fs::remove_file(&file_path);
        }

        #[test]
        fn file_contains_no_record_batches() {
            //* Given
            let schema = simple_schema();
            let temp_dir = std::env::temp_dir();
            let file_path = temp_dir.join("test_no_batches.ipc");

            //* When
            write_ipc_schema(&schema, &file_path).expect("write should succeed");

            let file = File::open(&file_path).expect("open should succeed");
            let reader = FileReader::try_new(BufReader::new(file), None)
                .expect("reader creation should succeed");

            //* Then
            assert_eq!(reader.num_batches(), 0, "should have no record batches");

            // Cleanup
            let _ = std::fs::remove_file(&file_path);
        }
    }

    mod type_coverage_tests {
        use super::*;

        #[test]
        fn roundtrips_all_primitive_types() {
            //* Given
            let original = Arc::new(Schema::new(vec![
                Field::new("bool", DataType::Boolean, false),
                Field::new("int8", DataType::Int8, false),
                Field::new("int16", DataType::Int16, false),
                Field::new("int32", DataType::Int32, false),
                Field::new("int64", DataType::Int64, false),
                Field::new("uint8", DataType::UInt8, false),
                Field::new("uint16", DataType::UInt16, false),
                Field::new("uint32", DataType::UInt32, false),
                Field::new("uint64", DataType::UInt64, false),
                Field::new("float16", DataType::Float16, false),
                Field::new("float32", DataType::Float32, false),
                Field::new("float64", DataType::Float64, false),
                Field::new("utf8", DataType::Utf8, true),
                Field::new("large_utf8", DataType::LargeUtf8, true),
                Field::new("binary", DataType::Binary, true),
                Field::new("large_binary", DataType::LargeBinary, true),
            ]));

            let temp_dir = std::env::temp_dir();
            let file_path = temp_dir.join("test_primitive_types.ipc");

            //* When
            write_ipc_schema(&original, &file_path).expect("write should succeed");
            let restored = read_ipc_schema(&file_path).expect("read should succeed");

            //* Then
            assert_eq!(original.fields().len(), restored.fields().len());
            for (orig, rest) in original.fields().iter().zip(restored.fields().iter()) {
                assert_eq!(
                    orig.data_type(),
                    rest.data_type(),
                    "type mismatch for field {}",
                    orig.name()
                );
            }

            // Cleanup
            let _ = std::fs::remove_file(&file_path);
        }

        #[test]
        fn roundtrips_temporal_types() {
            //* Given
            let original = Arc::new(Schema::new(vec![
                Field::new("date32", DataType::Date32, false),
                Field::new("date64", DataType::Date64, false),
                Field::new("time32_sec", DataType::Time32(TimeUnit::Second), false),
                Field::new(
                    "time32_milli",
                    DataType::Time32(TimeUnit::Millisecond),
                    false,
                ),
                Field::new(
                    "time64_micro",
                    DataType::Time64(TimeUnit::Microsecond),
                    false,
                ),
                Field::new("time64_nano", DataType::Time64(TimeUnit::Nanosecond), false),
                Field::new(
                    "timestamp_utc",
                    DataType::Timestamp(TimeUnit::Nanosecond, Some("+00:00".into())),
                    false,
                ),
                Field::new(
                    "timestamp_none",
                    DataType::Timestamp(TimeUnit::Microsecond, None),
                    false,
                ),
                Field::new("duration_sec", DataType::Duration(TimeUnit::Second), false),
                Field::new(
                    "interval_month",
                    DataType::Interval(datafusion::arrow::datatypes::IntervalUnit::YearMonth),
                    false,
                ),
            ]));

            let temp_dir = std::env::temp_dir();
            let file_path = temp_dir.join("test_temporal_types.ipc");

            //* When
            write_ipc_schema(&original, &file_path).expect("write should succeed");
            let restored = read_ipc_schema(&file_path).expect("read should succeed");

            //* Then
            assert_eq!(original.fields().len(), restored.fields().len());
            for (orig, rest) in original.fields().iter().zip(restored.fields().iter()) {
                assert_eq!(
                    orig.data_type(),
                    rest.data_type(),
                    "type mismatch for field {}",
                    orig.name()
                );
            }

            // Cleanup
            let _ = std::fs::remove_file(&file_path);
        }
    }
}
