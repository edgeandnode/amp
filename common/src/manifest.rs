//! Manifest for a dataset.
//!
//! Note: This replaces the "sql dataset".

use std::{collections::BTreeMap, sync::Arc};

use datafusion::{
    arrow::datatypes::{DataType, Field as ArrowField, Fields, Schema, SchemaRef},
    common::DFSchemaRef,
};
use serde::{Deserialize, Serialize};

use crate::Dataset;

pub const DATASET_KIND: &str = "manifest";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Manifest {
    pub name: String,
    pub network: String,
    pub version: semver::Version,

    #[serde(default)]
    pub dependencies: BTreeMap<String, Dependency>,

    #[serde(default)]
    pub tables: BTreeMap<String, Table>,

    #[serde(default)]
    pub functions: BTreeMap<String, Function>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Dependency {
    pub owner: String,
    pub name: String,
    pub version: semver::VersionReq,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Table {
    pub input: TableInput,
    pub schema: TableSchema,
    pub network: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Function {
    // TODO: Support SQL type names, see https://datafusion.apache.org/user-guide/sql/data_types.html
    pub input_types: Vec<DataType>,
    pub output_type: DataType,
    pub source: FunctionSource,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FunctionSource {
    pub source: Arc<str>,
    pub filename: String,
}

// TODO: Tagging
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum TableInput {
    View(View),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct View {
    pub sql: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableSchema {
    pub arrow: ArrowSchema,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArrowSchema {
    pub fields: Vec<Field>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Field {
    pub name: String,
    #[serde(rename = "type")]
    pub type_: DataType,
    pub nullable: bool,
}

impl Manifest {
    pub fn tables(&self) -> Vec<crate::Table> {
        self.tables
            .iter()
            .map(|(name, table)| crate::Table {
                name: name.clone(),
                schema: table.schema.arrow.clone().into(),
                network: table.network.clone(),
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
