//! Manifest for a dataset.
//!
//! Note: This replaces the "sql dataset".

use std::{collections::BTreeMap, sync::Arc};

use datafusion::{
    arrow::datatypes::{Field as ArrowField, Fields, Schema, SchemaRef},
    common::DFSchemaRef,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::{DataTypeJsonSchema, Dataset};

pub const DATASET_KIND: &str = "manifest";

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum ManifestKind {
    Manifest,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct Manifest {
    /// Dataset name.
    pub name: String,
    /// Network name, e.g., `mainnet`.
    pub network: String,
    /// Semver version of the dataset, e.g. `1.0.0`.
    pub version: Version,
    pub kind: ManifestKind,

    #[serde(default)]
    pub dependencies: BTreeMap<String, Dependency>,

    #[serde(default)]
    pub tables: BTreeMap<String, Table>,

    #[serde(default)]
    pub functions: BTreeMap<String, Function>,
}

/// Wrapper to implement [`JsonSchema`] for [`semver::Version`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Version(pub semver::Version);

impl JsonSchema for Version {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        "Version".into()
    }

    fn json_schema(generator: &mut schemars::SchemaGenerator) -> schemars::Schema {
        String::json_schema(generator)
    }
}

/// Wrapper to implement [`JsonSchema`] for [`semver::VersionReq`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VersionReq(pub semver::VersionReq);

impl JsonSchema for VersionReq {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        "VersionReq".into()
    }

    fn json_schema(generator: &mut schemars::SchemaGenerator) -> schemars::Schema {
        String::json_schema(generator)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct Dependency {
    pub owner: String,
    pub name: String,
    /// Semver version requirement for the dependency, e.g. `^1.0.0` or `>=1.0.0 <2.0.0`.
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
    pub input_types: Vec<DataTypeJsonSchema>,
    pub output_type: DataTypeJsonSchema,
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
    pub type_: DataTypeJsonSchema,
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
                        type_: DataTypeJsonSchema(f.data_type().clone()),
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
            version: Some(manifest.version),
            tables,
            functions: manifest
                .functions
                .into_iter()
                .map(|(name, f)| LogicalFunction {
                    name,
                    input_types: f.input_types.into_iter().map(|dt| dt.0).collect(),
                    output_type: f.output_type.0,
                    source: LogicalFunctionSource {
                        source: f.source.source,
                        filename: f.source.filename,
                    },
                })
                .collect(),
        }
    }
}
