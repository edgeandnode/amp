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
    sql::{parser, resolve::resolve_table_references},
};
use datasets_common::{
    manifest::{DataType, VersionReq},
    name::Name,
    version::Version,
};
use metadata_db::registry::Registry;

use crate::{
    BoxError, Dataset, Table as LogicalTable,
    catalog::logical::{Function as LogicalFunction, FunctionSource as LogicalFunctionSource},
    query_context::{self, parse_sql},
    utils::dfs,
};

/// Dataset kind constant for derived datasets.
pub const DATASET_KIND: &str = "manifest";

/// Complete manifest definition for a derived dataset.
///
/// A manifest defines a derived dataset with explicit dependencies, tables, and functions.
/// Derived datasets transform and combine data from existing datasets using SQL queries.
/// This is the replacement for the legacy SQL dataset format, providing better
/// versioning, dependency management, and schema validation.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct Manifest {
    /// Dataset name. Must be unique within the network
    pub name: Name,
    /// Network name, e.g., `mainnet`, `sepolia`
    pub network: String,
    /// Semver version of the dataset, e.g. `1.0.0`
    pub version: Version,
    /// Manifest kind, always [`ManifestKind::Manifest`]
    pub kind: ManifestKind,
    /// External dataset dependencies with version requirements
    #[serde(default)]
    pub dependencies: BTreeMap<String, Dependency>,
    /// Table definitions mapped by table name
    #[serde(default)]
    pub tables: BTreeMap<String, Table>,
    /// User-defined function definitions mapped by function name
    #[serde(default)]
    pub functions: BTreeMap<String, Function>,
}

impl Manifest {
    /// Convert manifest tables to logical table representations.
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

    /// Extract registry metadata from the manifest for publication.
    pub fn extract_registry_info(&self) -> Registry {
        let owner = self.dependencies.first_key_value().unwrap().1.owner.clone();
        let dataset = self.name.to_string();
        let version = self.version.to_string();
        let filename = self.to_filename();
        let manifest_path = object_store::path::Path::from(filename.clone()).to_string();
        Registry {
            owner,
            dataset,
            version,
            manifest: manifest_path,
        }
    }

    /// Generate a unique identifier for this manifest including version.
    pub fn to_identifier(&self) -> String {
        let version_str = self.version.to_underscore_version();
        format!("{}__{}", self.name, version_str)
    }

    /// Generate the filename for this manifest.
    pub fn to_filename(&self) -> String {
        format!("{}.json", self.to_identifier())
    }

    /// Extract all SQL queries from table views.
    pub fn queries(&self) -> Result<BTreeMap<String, parser::Statement>, query_context::Error> {
        let mut queries = BTreeMap::new();
        for (table_name, table) in &self.tables {
            let TableInput::View(query) = &table.input;
            let query = parse_sql(&query.sql)?;
            queries.insert(table_name.clone(), query);
        }
        Ok(queries)
    }

    /// Sort tables by their dependencies for proper execution order.
    pub fn sort_tables(&self) -> Result<Vec<LogicalTable>, BoxError> {
        let unsorted_tables = self.tables();
        let queries = self.queries()?;
        let sorted_tables = sort_tables_by_dependencies(&self.name, unsorted_tables, &queries)?;
        Ok(sorted_tables)
    }
}

/// Kind discriminator for derived datasets.
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[serde(rename_all = "lowercase")]
pub enum ManifestKind {
    /// Standard manifest dataset type
    #[default]
    Manifest,
}

/// External dataset dependency specification.
///
/// Defines a dependency on another dataset with version constraints.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct Dependency {
    /// Owner/organization of the dependency dataset
    pub owner: String,
    /// Name of the dependency dataset
    pub name: String,
    /// Semver version requirement for the dependency, e.g. `^1.0.0` or `>=1.0.0 <2.0.0`
    pub version: VersionReq,
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

/// Convert a derived dataset manifest into a logical dataset representation.
///
/// This function transforms a derived dataset manifest with its tables, functions, and metadata
/// into the internal `Dataset` structure used by the query engine.
pub fn dataset(manifest: Manifest) -> Result<Dataset, BoxError> {
    // Convert manifest tables into logical Tables
    let tables = manifest.sort_tables()?;

    Ok(Dataset {
        kind: DATASET_KIND.to_string(),
        network: manifest.network,
        name: manifest.name.to_string(),
        version: Some(manifest.version),
        start_block: None,
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

/// Sort tables by their SQL dependencies using topological ordering.
///
/// Analyzes table queries to determine dependencies and returns tables in dependency order.
/// Tables with no dependencies come first, followed by tables that depend on them.
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

/// Topological sort for table dependencies.
///
/// Uses depth-first search to order tables such that each table comes after
/// all tables it depends on. Detects circular dependencies.
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
