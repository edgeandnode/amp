//! Manifest for a dataset.
//!
//! Note: This replaces the "sql dataset".

use std::collections::BTreeMap;

use datafusion::common::DFSchemaRef;
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize)]
pub struct DatasetManifest {
    pub name: String,
    pub version: String,
    pub dependencies: BTreeMap<String, Dependency>,
    pub tables: BTreeMap<String, Table>,
}

#[derive(Debug, Deserialize)]
pub struct Dependency {
    pub owner: String,
    pub name: String,
    pub version: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Table {
    pub input: TableInput,
    pub schema: TableSchema,
}

// TODO: Tagging
#[derive(Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum TableInput {
    View(View),
}

#[derive(Debug, Serialize, Deserialize)]
pub struct View {
    pub sql: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TableSchema {
    pub arrow: ArrowSchema,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ArrowSchema {
    pub fields: Vec<Field>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Field {
    pub name: String,
    #[serde(rename = "type")]
    pub type_: String,
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
                        name: f.name().to_string(),
                        type_: f.data_type().to_string(),
                        nullable: f.is_nullable(),
                    })
                    .collect(),
            },
        }
    }
}
