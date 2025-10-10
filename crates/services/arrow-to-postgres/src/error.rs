use arrow_schema::DataType;
use thiserror::Error;

use crate::pg_schema_mapper::PostgresType;

#[derive(Debug, Error)]
pub enum Error {
    #[error("Type mismatch for column {field}: expected {expected} but got {actual:?}")]
    ColumnTypeMismatch {
        field: String,
        expected: String,
        actual: DataType,
    },
    #[error("Arrow type {tp} for field {field} is not supported (detail: {msg})")]
    TypeNotSupported {
        field: String,
        tp: DataType,
        msg: String,
    },
    #[error("field {field} exceeds the maximum allowed size for binary copy ({size} bytes)")]
    FieldTooLarge { field: String, size: usize },
    #[error("error encoding message: {reason}")]
    Encode {
        // E.g. because Postgres' binary format only supports fields up to 32bits
        reason: String,
    },
    #[error("Type {tp:?} for {field} not supported; supported types are {allowed:?}")]
    EncodingNotSupported {
        field: String,
        tp: PostgresType,
        allowed: Vec<PostgresType>,
    },
    #[error("Encoder {encoder:?} does not support field type {tp:?} for field {field:?}")]
    FieldTypeNotSupported {
        encoder: String,
        tp: DataType,
        field: String,
    },
    #[error("Missing encoder for field {field}")]
    EncoderMissing { field: String },
    #[error("No fields match supplied encoder fields: {fields:?}")]
    UnknownFields { fields: Vec<String> },
    #[error("Column count mismatch: expected {expected} columns but got {actual}")]
    ColumnCountMismatch { expected: usize, actual: usize },
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
