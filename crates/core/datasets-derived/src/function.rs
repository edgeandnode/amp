//! User-defined function types for logical catalog.
//!
//! This module provides function representations for the logical catalog,
//! which include the function name field (unlike manifest types where the name is the map key).

use std::sync::Arc;

use datafusion::arrow::datatypes::DataType;

/// User-defined function specification for logical catalog.
///
/// This type includes the function name and is used in the logical catalog
/// representation after manifest deserialization.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Function {
    /// Function name
    pub name: String,

    // TODO: Support SQL type names, see https://datafusion.apache.org/user-guide/sql/data_types.html
    /// Arrow data types for function input parameters
    pub input_types: Vec<DataType>,
    /// Arrow data type for function return value
    pub output_type: DataType,
    /// Function implementation source code and metadata
    pub source: FunctionSource,
}

/// Source code and metadata for a user-defined function in logical catalog.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct FunctionSource {
    /// Function implementation source code
    pub source: Arc<str>,
    /// Filename where the function is defined
    pub filename: String,
}
