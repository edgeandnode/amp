//! Dataset manifest and schema types.
//!
//! This module provides the core types for dataset manifests, schemas, and data types
//! used across different dataset definition formats.

use std::{collections::HashMap, sync::Arc};

use datafusion::{
    arrow::datatypes::{
        DataType as ArrowDataType, Field as ArrowField, Fields, Schema as DFSchema, SchemaRef,
    },
    common::DFSchemaRef,
};

/// Common metadata fields required by all dataset definitions.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Manifest {
    /// Dataset kind. See specific dataset definitions for supported values.
    ///
    /// Common values include: `manifest`, `evm-rpc`, `firehose`.
    pub kind: String,

    /// Network name, e.g., `mainnet`, `sepolia`
    ///
    /// Raw datasets' specific
    pub network: Option<String>,
}

/// A serializable representation of a collection of Arrow schemas without metadata.
///
/// This structure maps table names to their field definitions, providing a way to serialize
/// and deserialize Arrow schemas while filtering out the special `SPECIAL_BLOCK_NUM` field.
///
/// Structure: table name -> field name -> data type
#[derive(Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct Schema(HashMap<String, HashMap<String, DataType>>);

impl Schema {
    /// Create a new empty Schema.
    pub fn new() -> Self {
        Self(HashMap::new())
    }

    /// Insert a table schema into this Schema.
    pub fn insert_table(&mut self, table_name: String, fields: HashMap<String, DataType>) {
        self.0.insert(table_name, fields);
    }

    /// Get the inner HashMap.
    pub fn into_inner(self) -> HashMap<String, HashMap<String, DataType>> {
        self.0
    }

    /// Get a reference to the inner HashMap.
    pub fn as_inner(&self) -> &HashMap<String, HashMap<String, DataType>> {
        &self.0
    }
}

impl Default for Schema {
    fn default() -> Self {
        Self::new()
    }
}

/// Apache Arrow data _new-type_ wrapper with JSON schema support.
///
/// This wrapper provides serialization and JSON schema generation capabilities
/// for Apache Arrow data types used in dataset table definitions. Arrow data types
/// define the structure and format of columnar data.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(transparent)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[cfg_attr(feature = "schemars", schemars(transparent))]
#[cfg_attr(
    feature = "schemars",
    schemars(description = "Arrow data type, e.g. `Int32`, `Utf8`, etc.")
)]
pub struct DataType(#[cfg_attr(feature = "schemars", schemars(with = "String"))] pub ArrowDataType);

impl DataType {
    /// Returns a reference to the inner Arrow [`DataType`](ArrowDataType).
    pub fn as_arrow(&self) -> &ArrowDataType {
        &self.0
    }

    /// Consumes the [`DataType`] and returns the inner Arrow [`DataType`](ArrowDataType).
    pub fn into_arrow(self) -> ArrowDataType {
        self.0
    }
}

impl std::ops::Deref for DataType {
    type Target = ArrowDataType;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl From<ArrowDataType> for DataType {
    fn from(value: ArrowDataType) -> Self {
        Self(value)
    }
}

/// Schema definition for a table.
///
/// Wraps an Arrow schema with field definitions including names, types, and nullability.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct TableSchema {
    /// Arrow schema definition
    pub arrow: ArrowSchema,
}

impl From<DFSchemaRef> for TableSchema {
    fn from(arrow: DFSchemaRef) -> Self {
        Self {
            arrow: ArrowSchema {
                fields: arrow
                    .fields()
                    .iter()
                    .map(|f| Field {
                        name: f.name().clone(),
                        type_: f.data_type().clone().into(),
                        nullable: f.is_nullable(),
                    })
                    .collect(),
            },
        }
    }
}

/// Arrow schema representation for serialization.
///
/// Contains an ordered list of fields that define the structure of a table.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct ArrowSchema {
    /// Ordered list of fields in the schema
    pub fields: Vec<Field>,
}

impl ArrowSchema {
    /// Convert to DataFusion SchemaRef
    pub fn into_schema_ref(self) -> SchemaRef {
        let fields = self
            .fields
            .into_iter()
            .map(|f| ArrowField::new(f.name, f.type_.0, f.nullable));

        Arc::new(DFSchema::new(Fields::from_iter(fields)))
    }
}

// Implement From to allow .into() calls
impl From<ArrowSchema> for SchemaRef {
    fn from(schema: ArrowSchema) -> Self {
        schema.into_schema_ref()
    }
}

/// Arrow field definition with name, type, and nullability.
///
/// Represents a single column in a table schema.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct Field {
    /// Field name
    pub name: String,
    /// Arrow data type of the field
    #[serde(rename = "type")]
    pub type_: DataType,
    /// Whether the field can contain null values
    pub nullable: bool,
}

/// User-defined function specification.
///
/// Defines a custom function with input/output types and implementation source.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct Function {
    // TODO: Support SQL type names, see https://datafusion.apache.org/user-guide/sql/data_types.html
    /// Arrow data types for function input parameters
    pub input_types: Vec<DataType>,
    /// Arrow data type for function return value
    pub output_type: DataType,
    /// Function implementation source code and metadata
    pub source: FunctionSource,
}

/// Source code and metadata for a user-defined function.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct FunctionSource {
    /// Function implementation source code
    pub source: Arc<str>,
    /// Filename where the function is defined
    pub filename: String,
}
