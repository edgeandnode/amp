use arrow_schema::DataType;

use crate::pg_schema_mapper::PostgresType;

/// Errors that occur during Arrow to PostgreSQL conversion operations
///
/// This error type is used when converting Apache Arrow RecordBatches to PostgreSQL's
/// binary COPY format. Errors can occur during schema mapping, type encoding, or data validation.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Type mismatch between expected and actual Arrow column types
    ///
    /// This occurs when the Arrow schema does not match the expected PostgreSQL table schema.
    /// The converter expects specific Arrow data types for each column based on the PostgreSQL
    /// column types, and this error is raised when the actual Arrow type differs.
    ///
    /// Common causes:
    /// - Schema evolution where column types changed
    /// - Incorrect schema specification during conversion setup
    /// - Data source producing different types than expected
    #[error("Type mismatch for column {field}: expected {expected} but got {actual:?}")]
    ColumnTypeMismatch {
        field: String,
        expected: String,
        actual: DataType,
    },

    /// Arrow data type is not supported for PostgreSQL conversion
    ///
    /// This occurs when encountering an Arrow data type that cannot be converted to
    /// PostgreSQL's binary format. Not all Arrow types have corresponding PostgreSQL
    /// representations or binary encodings.
    ///
    /// Common causes:
    /// - Using complex nested types not supported by PostgreSQL
    /// - Custom Arrow extension types without conversion logic
    /// - Arrow types with no PostgreSQL equivalent (e.g., certain interval types)
    #[error("Arrow type {tp} for field {field} is not supported (detail: {msg})")]
    TypeNotSupported {
        field: String,
        tp: DataType,
        msg: String,
    },

    /// Field data exceeds PostgreSQL binary format size limits
    ///
    /// This occurs when a field's binary representation exceeds the maximum size allowed
    /// by PostgreSQL's COPY binary format (typically 1GB - 1 byte for a single field).
    ///
    /// Common causes:
    /// - Extremely large string or binary values
    /// - Large JSON documents
    /// - Oversized array fields
    ///
    /// PostgreSQL's binary format uses a 32-bit signed integer for field lengths,
    /// limiting individual field sizes to 2^31 - 1 bytes (approximately 2GB).
    #[error("field {field} exceeds the maximum allowed size for binary copy ({size} bytes)")]
    FieldTooLarge { field: String, size: usize },

    /// Error during message encoding to PostgreSQL binary format
    ///
    /// This occurs when the binary encoding process fails, typically due to format
    /// constraints or data that cannot be properly serialized.
    ///
    /// Common causes:
    /// - Integer overflow when encoding field lengths (PostgreSQL binary format uses 32-bit integers)
    /// - Invalid data representations that cannot be encoded
    /// - Buffer allocation failures during encoding
    #[error("error encoding message: {reason}")]
    Encode {
        reason: String,
    },

    /// PostgreSQL type encoding not supported for this Arrow type
    ///
    /// This occurs when the converter attempts to use an encoding strategy that is
    /// incompatible with the Arrow data type. Different PostgreSQL types support
    /// different encoding formats (text vs binary).
    ///
    /// Common causes:
    /// - Attempting binary encoding for types that only support text encoding
    /// - Mismatched encoder configuration for the data type
    /// - Unsupported type conversion combination
    #[error("Type {tp:?} for {field} not supported; supported types are {allowed:?}")]
    EncodingNotSupported {
        field: String,
        tp: PostgresType,
        allowed: Vec<PostgresType>,
    },

    /// Encoder does not support the Arrow field type
    ///
    /// This occurs when a specific encoder (e.g., BinaryEncoder, TextEncoder) is selected
    /// but does not have logic to handle the Arrow data type of the field.
    ///
    /// Common causes:
    /// - Using wrong encoder type for a field
    /// - Missing encoder implementation for a data type
    /// - Encoder configuration mismatch with schema
    #[error("Encoder {encoder:?} does not support field type {tp:?} for field {field:?}")]
    FieldTypeNotSupported {
        encoder: String,
        tp: DataType,
        field: String,
    },

    /// No encoder configured for a required field
    ///
    /// This occurs when the converter encounters a field in the Arrow schema that
    /// has no corresponding encoder configured. Every field in the Arrow schema must
    /// have an encoder to produce PostgreSQL binary output.
    ///
    /// Common causes:
    /// - Incomplete encoder configuration
    /// - New fields added to schema without updating encoder setup
    /// - Field name mismatch between schema and encoder configuration
    #[error("Missing encoder for field {field}")]
    EncoderMissing { field: String },

    /// Encoder configuration includes fields not present in Arrow schema
    ///
    /// This occurs when the encoder is configured with field names that do not exist
    /// in the actual Arrow RecordBatch schema. This indicates a configuration mismatch.
    ///
    /// Common causes:
    /// - Outdated encoder configuration referencing old field names
    /// - Typos in field names in encoder configuration
    /// - Schema changes without updating encoder setup
    #[error("No fields match supplied encoder fields: {fields:?}")]
    UnknownFields { fields: Vec<String> },

    /// Number of columns in data does not match expected schema
    ///
    /// This occurs when the Arrow RecordBatch has a different number of columns than
    /// the PostgreSQL table schema expects. The encoder requires an exact match.
    ///
    /// Common causes:
    /// - Schema evolution where columns were added or removed
    /// - Incorrect schema specification
    /// - Data source producing different column counts
    #[error("Column count mismatch: expected {expected} columns but got {actual}")]
    ColumnCountMismatch { expected: usize, actual: usize },

    /// JSON field contains invalid JSON data
    ///
    /// This occurs when a field designated as JSON type contains data that cannot
    /// be parsed or validated as valid JSON.
    ///
    /// Common causes:
    /// - Malformed JSON strings (syntax errors, unclosed brackets, etc.)
    /// - Binary data incorrectly marked as JSON type
    /// - Encoding issues in JSON strings
    #[error("Invalid JSON in field {field}: {reason}")]
    InvalidJson { field: String, reason: String },
}

// Note: InvalidState error variant removed because the type-state pattern
// makes invalid state transitions impossible at compile time

impl Error {
    pub(crate) fn field_too_large(field: &str, size: usize) -> Error {
        Error::FieldTooLarge {
            field: field.to_string(),
            size,
        }
    }

    pub(crate) fn type_unsupported(field: &str, tp: &DataType, msg: &str) -> Error {
        Error::TypeNotSupported {
            field: field.to_string(),
            tp: tp.clone(),
            msg: msg.to_string(),
        }
    }

    pub(crate) fn unsupported_encoding(
        field: &str,
        tp: &PostgresType,
        allowed: &[PostgresType],
    ) -> Error {
        Error::EncodingNotSupported {
            field: field.to_string(),
            tp: tp.clone(),
            allowed: allowed.to_owned(),
        }
    }

    pub(crate) fn mismatched_column_type(field: &str, expected: &str, actual: &DataType) -> Error {
        Error::ColumnTypeMismatch {
            field: field.to_string(),
            expected: expected.to_string(),
            actual: actual.clone(),
        }
    }

    pub(crate) fn column_count_mismatch(expected: usize, actual: usize) -> Error {
        Error::ColumnCountMismatch { expected, actual }
    }

    pub(crate) fn invalid_json(field: &str, reason: &str) -> Error {
        Error::InvalidJson {
            field: field.to_string(),
            reason: reason.to_string(),
        }
    }
}
