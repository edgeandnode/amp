//! Derived dataset
//!
//! This module provides derived datasets that transform and combine data from existing datasets using SQL queries.
//! Derived datasets replace the legacy SQL dataset format, providing versioned, dependency-aware dataset
//! definitions with explicit schemas and functions.

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use datafusion::{
    arrow::datatypes::{Field as ArrowField, Fields, Schema, SchemaRef},
    common::DFSchemaRef,
};
use datasets_common::{manifest::DataType, name::Name, reference::Reference, version::Version};

use crate::dataset_kind::DerivedDatasetKind;

/// Complete manifest definition for a derived dataset.
///
/// A manifest defines a derived dataset with explicit dependencies, tables, and functions.
/// Derived datasets transform and combine data from existing datasets using SQL queries.
/// This is the replacement for the legacy SQL dataset format, providing better
/// versioning, dependency management, and schema validation.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct Manifest {
    /// Dataset name
    pub name: Name,
    /// Dataset version, e.g., `1.0.0`
    pub version: Version,
    /// Dataset kind, must be `manifest`
    pub kind: DerivedDatasetKind,

    /// External dataset dependencies with version requirements
    #[serde(default)]
    pub dependencies: BTreeMap<String, Reference>,
    /// Table definitions mapped by table name
    #[serde(default)]
    pub tables: BTreeMap<String, Table>,
    /// User-defined function definitions mapped by function name
    #[serde(default)]
    pub functions: BTreeMap<String, Function>,
}

/// Table definition within a derived dataset.
///
/// Defines a table with its input source, schema, and network.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct Table {
    /// Table input source (currently only Views are supported)
    pub input: TableInput,
    /// Arrow schema definition for the table
    pub schema: TableSchema,
    /// Network this table belongs to
    pub network: String,
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

/// Input source for a table definition.
///
/// Currently only SQL views are supported as table inputs.
// TODO: Add support for other input types with proper tagging
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[serde(untagged)]
pub enum TableInput {
    /// SQL view as table input
    View(View),
}

/// SQL view definition for table input.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct View {
    /// SQL query defining the view
    pub sql: String,
}

/// Schema definition for a table.
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

impl From<ArrowSchema> for SchemaRef {
    fn from(schema: ArrowSchema) -> Self {
        let fields = schema
            .fields
            .into_iter()
            .map(|f| ArrowField::new(f.name, f.type_.0, f.nullable));

        Arc::new(Schema::new(Fields::from_iter(fields)))
    }
}

/// Arrow schema representation for serialization.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct ArrowSchema {
    /// Ordered list of fields in the schema
    pub fields: Vec<Field>,
}

/// Arrow field definition with name, type, and nullability.
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

/// Errors that occur during derived dataset dependency validation
///
/// This validation ensures that all datasets referenced in SQL queries
/// are properly declared in the manifest's dependencies list.
#[derive(Debug, Clone, thiserror::Error)]
pub enum DependencyValidationError {
    /// SQL query is syntactically invalid or structurally malformed
    ///
    /// This occurs when:
    /// - SQL syntax is incorrect
    /// - Multiple SQL statements are provided (only single statements allowed)
    /// - Table references cannot be resolved from the query
    /// - DataFusion parser/resolver encounters an error
    #[error("invalid SQL query: {0}")]
    InvalidSql(String),

    /// SQL query references datasets not declared in the dependencies list
    ///
    /// This occurs when:
    /// - A SQL query references a schema (dataset) via `schema.table`
    /// - That schema name is not found in the manifest's `dependencies` map
    ///
    /// The error contains a list of all undeclared dataset names that need to be added
    /// to the manifest's dependencies section.
    #[error("undeclared dependencies of SQL query: {0:?}")]
    Missing(Vec<String>),
}

impl Manifest {
    pub fn validate_dependencies(&self) -> Result<(), DependencyValidationError> {
        let mut sql_deps: BTreeSet<String> = Default::default();
        for table in self.tables.values() {
            let sql = match &table.input {
                TableInput::View(view) => &view.sql,
            };
            let statements = datafusion::sql::parser::DFParser::parse_sql(sql)
                .map_err(|err| DependencyValidationError::InvalidSql(err.to_string()))?;
            let statement = match statements {
                _ if statements.len() == 1 => &statements[0],
                _ => {
                    return Err(DependencyValidationError::InvalidSql(format!(
                        "a single SQL statement is expected, found {}",
                        statements.len()
                    )));
                }
            };
            let (references, _) =
                datafusion::sql::resolve::resolve_table_references(statement, true)
                    .map_err(|err| DependencyValidationError::InvalidSql(err.to_string()))?;
            for reference in references.iter().filter_map(|r| r.schema()) {
                sql_deps.insert(reference.to_string());
            }
        }

        let declared_deps: BTreeSet<String> = self
            .dependencies
            .values()
            .map(|dep| dep.name().to_string())
            .collect();
        let missing_deps: Vec<String> = sql_deps.difference(&declared_deps).cloned().collect();
        if !missing_deps.is_empty() {
            return Err(DependencyValidationError::Missing(missing_deps));
        }

        Ok(())
    }
}
