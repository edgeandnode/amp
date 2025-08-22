//! Manifest for a dataset.
//!
//! Note: This replaces the "sql dataset".

use std::{
    collections::{BTreeMap, BTreeSet},
    str::FromStr,
    sync::Arc,
};

use datafusion::{
    arrow::datatypes::{Field as ArrowField, Fields, Schema, SchemaRef},
    common::DFSchemaRef,
    sql::{parser, resolve::resolve_table_references},
};
use metadata_db::registry::Registry;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::{
    BoxError, DataTypeJsonSchema, Dataset, Table as LogicalTable,
    query_context::{self, parse_sql},
    utils::dfs,
};

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

impl std::fmt::Display for Version {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::str::FromStr for Version {
    type Err = semver::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let version = semver::Version::from_str(s)?;
        Ok(Version(version))
    }
}

impl Version {
    /// Convert the Semver version to a string with underscores.
    ///
    /// Example: SemverVersion(1, 0, 0) -> String("1_0_0").
    pub fn to_underscore_version(&self) -> String {
        self.0.to_string().replace('.', "_")
    }

    /// Convert a Semver version string to a string with underscores.
    ///
    /// Example: String("1.0.0") -> String("1_0_0").
    pub fn version_identifier(version: &str) -> Result<String, BoxError> {
        let version = Version::from_str(version)?;
        Ok(version.to_underscore_version())
    }

    pub fn from_version_identifier(v_identifier: &str) -> Result<Self, BoxError> {
        let version = Version::from_str(&v_identifier.replace("_", "."))?;
        Ok(version)
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
    pub fn tables(&self) -> Vec<LogicalTable> {
        self.tables
            .iter()
            .map(|(name, table)| {
                LogicalTable::new(
                    name.clone(),
                    table.schema.arrow.clone().into(),
                    table.network.clone(),
                )
            })
            .collect()
    }

    pub fn extract_registry_info(&self) -> Registry {
        let owner = self.dependencies.first_key_value().unwrap().1.owner.clone();
        let dataset = self.name.clone();
        let version = self.version.0.to_string();
        let filename = self.to_filename();
        let manifest_path = object_store::path::Path::from(filename.clone()).to_string();
        Registry {
            owner,
            dataset,
            version,
            manifest: manifest_path,
        }
    }

    pub fn to_identifier(&self) -> String {
        let version_str = self.version.to_underscore_version();
        format!("{}__{}", self.name, version_str)
    }

    pub fn to_filename(&self) -> String {
        format!("{}.json", self.to_identifier())
    }

    pub fn queries(&self) -> Result<BTreeMap<String, parser::Statement>, query_context::Error> {
        let mut queries = BTreeMap::new();
        for (table_name, table) in &self.tables {
            let TableInput::View(query) = &table.input;
            let query = parse_sql(&query.sql)?;
            queries.insert(table_name.clone(), query);
        }
        Ok(queries)
    }

    pub fn sort_tables(&self) -> Result<Vec<LogicalTable>, BoxError> {
        let unsorted_tables = self.tables();
        let queries = self.queries()?;
        let sorted_tables = sort_tables_by_dependencies(&self.name, unsorted_tables, &queries)?;
        Ok(sorted_tables)
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

pub fn dataset(manifest: Manifest) -> Result<Dataset, BoxError> {
    use crate::catalog::logical::{
        Function as LogicalFunction, FunctionSource as LogicalFunctionSource,
    };

    // Convert manifest tables into logical Tables
    let tables = manifest.sort_tables()?;

    Ok(Dataset {
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
    })
}

pub fn sort_tables_by_dependencies(
    dataset_name: &str,
    tables: Vec<LogicalTable>,
    queries: &BTreeMap<String, parser::Statement>,
) -> Result<Vec<LogicalTable>, BoxError> {
    // Map of table name -> Table
    let table_map: BTreeMap<String, LogicalTable> = tables
        .into_iter()
        .map(|t| (t.name().to_string(), t))
        .collect();

    // Dependency map: table -> [tables it depends on]
    let mut deps: BTreeMap<String, Vec<String>> = BTreeMap::new();

    // Initialize empty deps with all tables
    for table_name in table_map.keys() {
        deps.insert(table_name.clone(), Vec::new());
    }

    for (table_name, query) in queries {
        let (table_refs, _) = resolve_table_references(query, true)?;

        // Filter to only include dependencies within the same dataset
        let mut table_deps: Vec<String> = vec![];
        for table_ref in table_refs {
            match (table_ref.schema(), table_ref.table()) {
                (Some(schema), table) if schema == dataset_name => {
                    // Reference to a table in the same dataset
                    if table != table_name && table_map.contains_key(table) {
                        table_deps.push(table.to_string());
                    }
                }
                (None, _) => {
                    // Unqualified reference
                }
                _ => {
                    // Reference to external dataset, ignore
                }
            }
        }

        // Update the existing entry with dependencies
        if let Some(existing_deps) = deps.get_mut(table_name) {
            *existing_deps = table_deps;
        }
    }

    let sorted_names = table_dependency_sort(deps)?;

    let mut sorted_tables = Vec::new();
    for name in sorted_names {
        if let Some(table) = table_map.get(&name) {
            sorted_tables.push(table.clone());
        }
    }

    Ok(sorted_tables)
}

/// Topological sort for table dependencies
/// Returns tables in order such that each table comes after all tables it depends on
fn table_dependency_sort(deps: BTreeMap<String, Vec<String>>) -> Result<Vec<String>, BoxError> {
    let nodes: BTreeSet<&String> = deps.keys().collect();
    let mut ordered: Vec<String> = Vec::new();
    let mut visited: BTreeSet<&String> = BTreeSet::new();
    let mut visiting: BTreeSet<&String> = BTreeSet::new();

    for node in nodes {
        if !visited.contains(node) {
            dfs(node, &deps, &mut ordered, &mut visited, &mut visiting)?;
        }
    }

    Ok(ordered)
}
