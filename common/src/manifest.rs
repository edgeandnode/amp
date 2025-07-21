//! Manifest for a dataset.
//!
//! Note: This replaces the "sql dataset".

use std::{collections::BTreeMap, sync::Arc};

use datafusion::{
    arrow::datatypes::{DataType, Field as ArrowField, Fields, Schema, SchemaRef},
    common::DFSchemaRef,
};
use semver::{Version, VersionReq};
use serde::{Deserialize, Serialize};

use crate::{Dataset, JsonSchema};

pub const DATASET_KIND: &str = "manifest";
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct Manifest {
    /// Dataset kind, must be `manifest`.
    pub kind: String,
    /// Dataset name.
    pub name: String,
    /// Network name, e.g., `mainnet`.
    pub network: String,
    pub version: Version,

    #[serde(default)]
    pub dependencies: BTreeMap<String, Dependency>,

    #[serde(default)]
    pub tables: BTreeMap<String, Table>,

    #[serde(default)]
    pub functions: BTreeMap<String, Function>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct Dependency {
    pub owner: String,
    pub name: String,
    pub version: VersionReq,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct Table {
    /// Table input, which can be a View.
    pub input: TableInput,
    /// Table schema, which is an Arrow Schema.
    pub schema: TableSchema,
    pub network: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct Function {
    // TODO: Support SQL type names, see https://datafusion.apache.org/user-guide/sql/data_types.html
    pub input_types: Vec<DataType>,
    pub output_type: DataType,
    pub source: FunctionSource,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct FunctionSource {
    pub source: Arc<str>,
    pub filename: String,
}

// TODO: Tagging
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(untagged)]
pub enum TableInput {
    View(View),
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct View {
    /// SQL query defining the view.
    pub sql: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct TableSchema {
    /// Arrow schema of the table.
    pub arrow: ArrowSchema,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct ArrowSchema {
    /// List of Fields in the schema.
    pub fields: Vec<Field>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct Field {
    /// Field name.
    pub name: String,
    /// Data type.
    #[serde(rename = "type")]
    pub type_: DataType,
    /// Boolean indicating whether the field is nullable.
    pub nullable: bool,
}

impl Manifest {
    pub fn tables(&self) -> Vec<crate::Table> {
        self.tables
            .iter()
            .map(|(name, table)| {
                crate::Table::new(
                    name.clone(),
                    table.schema.arrow.clone().into(),
                    table.network.clone(),
                )
            })
            .collect()
    }
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
                        type_: f.data_type().clone(),
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
            .map(|f| ArrowField::new(f.name, f.type_, f.nullable));

        Arc::new(Schema::new(Fields::from_iter(fields)))
    }
}

impl From<Manifest> for Dataset {
    fn from(manifest: Manifest) -> Self {
        use crate::catalog::logical::{
            Function as LogicalFunction, FunctionSource as LogicalFunctionSource,
        };

        // Convert manifest tables into logical Tables
        let tables = manifest.tables();

        Dataset {
            kind: DATASET_KIND.to_string(),
            network: manifest.network,
            name: manifest.name,
            tables,
            functions: manifest
                .functions
                .into_iter()
                .map(|(name, f)| LogicalFunction {
                    name,
                    input_types: f.input_types,
                    output_type: f.output_type,
                    source: LogicalFunctionSource {
                        source: f.source.source,
                        filename: f.source.filename,
                    },
                })
                .collect(),
        }
    }
}
