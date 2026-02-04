//! Arrow schema serialization in JSON format.
//!
//! This module handles serialization and deserialization of Arrow schemas to JSON files.
//! The format used is compatible with the existing Amp manifest schema format, which
//! represents Arrow types using DataFusion's serde implementation.
//!
//! # Schema File Format
//!
//! Schema files are stored as `sql/<table>.schema.json` and contain:
//!
//! ```json
//! {
//!   "fields": [
//!     { "name": "column_name", "type": "UInt64", "nullable": false },
//!     { "name": "other_col", "type": { "Timestamp": ["Nanosecond", "+00:00"] }, "nullable": true }
//!   ]
//! }
//! ```
//!
//! This format:
//! - Uses DataFusion's built-in serde for Arrow `DataType`
//! - Is compatible with the legacy manifest bridge
//! - Supports all Arrow types used by Amp datasets
//!
//! # Example
//!
//! ```ignore
//! use dataset_authoring::arrow_json::{write_schema_file, read_schema_file};
//! use datasets_common::manifest::TableSchema;
//!
//! // Write schema to file
//! write_schema_file(&schema, "sql/transfers.schema.json")?;
//!
//! // Read schema from file
//! let loaded_schema = read_schema_file("sql/transfers.schema.json")?;
//! ```

use std::{
    io::{self, BufReader, BufWriter},
    path::Path,
};

use datasets_common::manifest::ArrowSchema;

/// Errors that can occur during schema file operations.
#[derive(Debug, thiserror::Error)]
pub enum SchemaFileError {
    /// Failed to read or write the schema file.
    #[error("I/O error accessing schema file")]
    Io(#[from] io::Error),

    /// Failed to serialize schema to JSON.
    #[error("failed to serialize schema to JSON")]
    Serialize(#[source] serde_json::Error),

    /// Failed to deserialize schema from JSON.
    #[error("failed to deserialize schema from JSON")]
    Deserialize(#[source] serde_json::Error),
}

/// Writes an Arrow schema to a JSON file.
///
/// The schema is written using the Amp manifest format, which is compatible
/// with DataFusion's serde implementation for Arrow types.
///
/// # Arguments
///
/// * `schema` - The Arrow schema to write.
/// * `path` - The path to write the schema file to.
///
/// # Errors
///
/// Returns an error if the file cannot be written or the schema cannot be serialized.
pub fn write_schema_file(
    schema: &ArrowSchema,
    path: impl AsRef<Path>,
) -> Result<(), SchemaFileError> {
    let file = std::fs::File::create(path.as_ref())?;
    let writer = BufWriter::new(file);

    serde_json::to_writer_pretty(writer, schema).map_err(SchemaFileError::Serialize)?;

    Ok(())
}

/// Reads an Arrow schema from a JSON file.
///
/// # Arguments
///
/// * `path` - The path to read the schema file from.
///
/// # Returns
///
/// The deserialized Arrow schema.
///
/// # Errors
///
/// Returns an error if the file cannot be read or the JSON is invalid.
pub fn read_schema_file(path: impl AsRef<Path>) -> Result<ArrowSchema, SchemaFileError> {
    let file = std::fs::File::open(path.as_ref())?;
    let reader = BufReader::new(file);

    serde_json::from_reader(reader).map_err(SchemaFileError::Deserialize)
}

/// Serializes an Arrow schema to a JSON string.
///
/// This is useful for generating schema JSON for embedding in manifests
/// or for testing purposes.
///
/// # Arguments
///
/// * `schema` - The Arrow schema to serialize.
///
/// # Returns
///
/// The JSON representation of the schema.
pub fn schema_to_json(schema: &ArrowSchema) -> Result<String, SchemaFileError> {
    serde_json::to_string_pretty(schema).map_err(SchemaFileError::Serialize)
}

/// Deserializes an Arrow schema from a JSON string.
///
/// # Arguments
///
/// * `json` - The JSON string to parse.
///
/// # Returns
///
/// The deserialized Arrow schema.
pub fn schema_from_json(json: &str) -> Result<ArrowSchema, SchemaFileError> {
    serde_json::from_str(json).map_err(SchemaFileError::Deserialize)
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::datatypes::DataType;
    use datasets_common::manifest::Field;

    use super::*;

    /// Creates a simple test schema with basic types.
    fn simple_schema() -> ArrowSchema {
        ArrowSchema {
            fields: vec![
                Field {
                    name: "id".to_string(),
                    type_: DataType::UInt64.into(),
                    nullable: false,
                },
                Field {
                    name: "name".to_string(),
                    type_: DataType::Utf8.into(),
                    nullable: true,
                },
            ],
        }
    }

    /// Creates a schema with complex types.
    fn complex_schema() -> ArrowSchema {
        ArrowSchema {
            fields: vec![
                Field {
                    name: "block_num".to_string(),
                    type_: DataType::UInt64.into(),
                    nullable: false,
                },
                Field {
                    name: "timestamp".to_string(),
                    type_: DataType::Timestamp(
                        datafusion::arrow::datatypes::TimeUnit::Nanosecond,
                        Some("+00:00".into()),
                    )
                    .into(),
                    nullable: false,
                },
                Field {
                    name: "hash".to_string(),
                    type_: DataType::FixedSizeBinary(32).into(),
                    nullable: false,
                },
                Field {
                    name: "value".to_string(),
                    type_: DataType::Decimal128(38, 0).into(),
                    nullable: true,
                },
            ],
        }
    }

    mod serialization_tests {
        use super::*;

        #[test]
        fn serializes_simple_schema() {
            //* Given
            let schema = simple_schema();

            //* When
            let json = schema_to_json(&schema).expect("serialization should succeed");

            //* Then
            assert!(
                json.contains(r#""name": "id""#),
                "should contain field name"
            );
            assert!(json.contains(r#""type": "UInt64""#), "should contain type");
            assert!(
                json.contains(r#""nullable": false"#),
                "should contain nullable"
            );
        }

        #[test]
        fn serializes_complex_types() {
            //* Given
            let schema = complex_schema();

            //* When
            let json = schema_to_json(&schema).expect("serialization should succeed");

            //* Then
            // Check Timestamp type is serialized correctly
            assert!(
                json.contains("Timestamp"),
                "should contain Timestamp type name"
            );
            assert!(json.contains("Nanosecond"), "should contain time unit");

            // Check FixedSizeBinary
            assert!(
                json.contains("FixedSizeBinary"),
                "should contain FixedSizeBinary"
            );
            assert!(json.contains("32"), "should contain binary size");

            // Check Decimal128
            assert!(json.contains("Decimal128"), "should contain Decimal128");
            assert!(json.contains("38"), "should contain precision");
        }
    }

    mod roundtrip_tests {
        use super::*;

        #[test]
        fn roundtrips_simple_schema() {
            //* Given
            let original = simple_schema();

            //* When
            let json = schema_to_json(&original).expect("serialization should succeed");
            let restored = schema_from_json(&json).expect("deserialization should succeed");

            //* Then
            assert_eq!(original.fields.len(), restored.fields.len());
            for (orig, rest) in original.fields.iter().zip(restored.fields.iter()) {
                assert_eq!(orig.name, rest.name);
                assert_eq!(orig.nullable, rest.nullable);
                assert_eq!(*orig.type_.as_arrow(), *rest.type_.as_arrow());
            }
        }

        #[test]
        fn roundtrips_complex_schema() {
            //* Given
            let original = complex_schema();

            //* When
            let json = schema_to_json(&original).expect("serialization should succeed");
            let restored = schema_from_json(&json).expect("deserialization should succeed");

            //* Then
            assert_eq!(original.fields.len(), restored.fields.len());
            for (orig, rest) in original.fields.iter().zip(restored.fields.iter()) {
                assert_eq!(orig.name, rest.name, "field names should match");
                assert_eq!(orig.nullable, rest.nullable, "nullability should match");
                assert_eq!(
                    *orig.type_.as_arrow(),
                    *rest.type_.as_arrow(),
                    "types should match for field {}",
                    orig.name
                );
            }
        }

        #[test]
        fn roundtrips_empty_schema() {
            //* Given
            let original = ArrowSchema { fields: vec![] };

            //* When
            let json = schema_to_json(&original).expect("serialization should succeed");
            let restored = schema_from_json(&json).expect("deserialization should succeed");

            //* Then
            assert!(restored.fields.is_empty());
        }
    }

    mod file_io_tests {
        use std::io::Write;

        use super::*;

        #[test]
        fn writes_and_reads_schema_file() {
            //* Given
            let schema = simple_schema();
            let temp_dir = std::env::temp_dir();
            let file_path = temp_dir.join("test_schema.json");

            //* When
            write_schema_file(&schema, &file_path).expect("write should succeed");
            let loaded = read_schema_file(&file_path).expect("read should succeed");

            //* Then
            assert_eq!(schema.fields.len(), loaded.fields.len());
            for (orig, rest) in schema.fields.iter().zip(loaded.fields.iter()) {
                assert_eq!(orig.name, rest.name);
                assert_eq!(orig.nullable, rest.nullable);
                assert_eq!(*orig.type_.as_arrow(), *rest.type_.as_arrow());
            }

            // Cleanup
            let _ = std::fs::remove_file(&file_path);
        }

        #[test]
        fn returns_error_for_nonexistent_file() {
            //* Given
            let path = "/nonexistent/path/to/schema.json";

            //* When
            let result = read_schema_file(path);

            //* Then
            assert!(result.is_err());
            assert!(matches!(result.unwrap_err(), SchemaFileError::Io(_)));
        }

        #[test]
        fn returns_error_for_invalid_json() {
            //* Given
            let temp_dir = std::env::temp_dir();
            let file_path = temp_dir.join("invalid_schema.json");
            let mut file = std::fs::File::create(&file_path).expect("file creation should succeed");
            file.write_all(b"{ invalid json }")
                .expect("write should succeed");

            //* When
            let result = read_schema_file(&file_path);

            //* Then
            assert!(result.is_err());
            assert!(matches!(
                result.unwrap_err(),
                SchemaFileError::Deserialize(_)
            ));

            // Cleanup
            let _ = std::fs::remove_file(&file_path);
        }

        #[test]
        fn produces_pretty_formatted_output() {
            //* Given
            let schema = simple_schema();
            let temp_dir = std::env::temp_dir();
            let file_path = temp_dir.join("pretty_schema.json");

            //* When
            write_schema_file(&schema, &file_path).expect("write should succeed");
            let content = std::fs::read_to_string(&file_path).expect("read should succeed");

            //* Then
            // Pretty printed JSON should have newlines and indentation
            assert!(content.contains('\n'), "should have newlines");
            assert!(content.contains("  "), "should have indentation");

            // Cleanup
            let _ = std::fs::remove_file(&file_path);
        }
    }

    mod known_schemas_tests {
        use datafusion::arrow::datatypes::{
            DataType as ArrowDataType, Field as ArrowField, Schema,
        };

        use super::*;

        /// Tests that our format matches what's expected for integration with
        /// existing manifests.
        #[test]
        fn matches_expected_manifest_format() {
            //* Given
            let schema = ArrowSchema {
                fields: vec![
                    Field {
                        name: "_block_num".to_string(),
                        type_: ArrowDataType::UInt64.into(),
                        nullable: false,
                    },
                    Field {
                        name: "tx_hash".to_string(),
                        type_: ArrowDataType::FixedSizeBinary(32).into(),
                        nullable: false,
                    },
                ],
            };

            //* When
            let json = schema_to_json(&schema).expect("serialization should succeed");

            //* Then
            // Verify the format matches expected manifest JSON structure
            let parsed: serde_json::Value =
                serde_json::from_str(&json).expect("should be valid JSON");

            assert!(parsed.get("fields").is_some(), "should have fields array");

            let fields = parsed["fields"].as_array().expect("fields should be array");
            assert_eq!(fields.len(), 2);

            // First field
            assert_eq!(fields[0]["name"], "_block_num");
            assert_eq!(fields[0]["type"], "UInt64");
            assert_eq!(fields[0]["nullable"], false);

            // Second field with complex type
            assert_eq!(fields[1]["name"], "tx_hash");
            assert!(
                fields[1]["type"].is_object(),
                "complex type should be object"
            );
            assert_eq!(fields[1]["type"]["FixedSizeBinary"], 32);
        }

        #[test]
        fn converts_from_datafusion_schema() {
            //* Given
            let df_schema = Schema::new(vec![
                ArrowField::new("id", ArrowDataType::Int64, false),
                ArrowField::new("value", ArrowDataType::Float64, true),
            ]);

            //* When
            let arrow_schema = ArrowSchema {
                fields: df_schema
                    .fields()
                    .iter()
                    .map(|f| Field {
                        name: f.name().clone(),
                        type_: f.data_type().clone().into(),
                        nullable: f.is_nullable(),
                    })
                    .collect(),
            };
            let json = schema_to_json(&arrow_schema).expect("serialization should succeed");
            let restored = schema_from_json(&json).expect("deserialization should succeed");

            //* Then
            assert_eq!(restored.fields.len(), 2);
            assert_eq!(restored.fields[0].name, "id");
            assert_eq!(*restored.fields[0].type_.as_arrow(), ArrowDataType::Int64);
            assert!(!restored.fields[0].nullable);

            assert_eq!(restored.fields[1].name, "value");
            assert_eq!(*restored.fields[1].type_.as_arrow(), ArrowDataType::Float64);
            assert!(restored.fields[1].nullable);
        }
    }
}
