//! Manifest for a dataset.
//!
//! Note: This replaces the "sql dataset".

use std::collections::BTreeMap;
use std::sync::Arc;

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
    pub version: semver::Version,
    pub dependencies: BTreeMap<String, Dependency>,
    pub tables: BTreeMap<String, Table>,
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
        // Convert manifest tables into logical Tables
        let tables = manifest
            .tables
            .into_iter()
            .map(|(name, table)| crate::Table {
                name,
                schema: table.schema.arrow.into(),
                network: None, // Network is not part of the manifest yet
            })
            .collect();

        Dataset {
            kind: DATASET_KIND.to_string(),
            name: manifest.name,
            tables,
        }
    }
}
