//! Authoring manifest generation.
//!
//! Generates the canonical `manifest.json` with file references and content hashes.
//! This is distinct from the runtime manifest in `datasets-derived` which uses
//! inline SQL and schemas.
//!
//! # Manifest Structure
//!
//! The authoring manifest uses file references instead of inline content:
//!
//! ```json
//! {
//!   "namespace": "my_namespace",
//!   "name": "my_dataset",
//!   "kind": "derived",
//!   "dependencies": {
//!     "eth": {
//!       "hash": "abc123..."
//!     }
//!   },
//!   "tables": {
//!     "transfers": {
//!       "sql": { "path": "tables/transfers.sql", "hash": "..." },
//!       "ipc": { "path": "tables/transfers.ipc", "hash": "..." },
//!       "network": "mainnet"
//!     }
//!   },
//!   "functions": {
//!     "decode": {
//!       "inputTypes": ["Binary"],
//!       "outputType": "Utf8",
//!       "source": { "path": "functions/decode.js", "hash": "..." }
//!     }
//!   }
//! }
//! ```
//!
//! For raw tables (no SQL definition), the `sql` field is omitted.
//!
//! # Identity Hash
//!
//! The manifest identity is computed as `sha256(<canonical JSON bytes>)` using
//! RFC 8785 (JCS) canonicalization via `serde_json_canonicalizer`.

use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
};

use datasets_common::{
    hash::Hash, manifest::DataType, name::Name, namespace::Namespace, network_id::NetworkId,
    table_name::TableName,
};
use datasets_derived::{deps::DepAlias, func_name::FuncName};
use serde::{Deserialize, Serialize};

use crate::{
    canonical::{CanonicalizeError, canonicalize, hash_canonical},
    config::AmpYaml,
    files::{FileRef, FileRefError},
    resolver::DependencyGraph,
};

/// Errors that occur during manifest generation.
#[derive(Debug, thiserror::Error)]
pub enum ManifestError {
    /// Failed to create file reference for SQL file.
    ///
    /// This occurs when the SQL file cannot be read or hashed.
    #[error("failed to create file reference for SQL file '{table_name}'")]
    SqlFileRef {
        /// The table name associated with the SQL file.
        table_name: TableName,
        /// The underlying file reference error.
        #[source]
        source: FileRefError,
    },

    /// Failed to create file reference for IPC schema file.
    ///
    /// This occurs when the IPC schema file cannot be read or hashed.
    #[error("failed to create file reference for IPC schema file '{table_name}'")]
    IpcFileRef {
        /// The table name associated with the IPC schema file.
        table_name: TableName,
        /// The underlying file reference error.
        #[source]
        source: FileRefError,
    },

    /// Failed to create file reference for function source file.
    ///
    /// This occurs when the function source file cannot be read or hashed.
    #[error("failed to create file reference for function '{func_name}'")]
    FunctionFileRef {
        /// The function name.
        func_name: FuncName,
        /// The underlying file reference error.
        #[source]
        source: FileRefError,
    },

    /// Missing dependency in resolved graph.
    ///
    /// This occurs when a dependency alias from amp.yaml is not found
    /// in the resolved dependency graph.
    #[error("dependency '{alias}' not found in resolved dependency graph")]
    MissingDependency {
        /// The missing dependency alias.
        alias: DepAlias,
    },

    /// Failed to infer network for table.
    ///
    /// This occurs when a table has no dependencies to infer network from,
    /// or when dependencies have conflicting networks.
    #[error("cannot infer network for table '{table_name}': {reason}")]
    NetworkInference {
        /// The table name.
        table_name: TableName,
        /// Reason for the failure.
        reason: String,
    },

    /// Failed to parse data type for function.
    ///
    /// This occurs when a function's input or output type string
    /// cannot be parsed as a valid Arrow data type.
    #[error("invalid data type '{type_str}' for function '{func_name}'")]
    InvalidDataType {
        /// The function name.
        func_name: FuncName,
        /// The invalid type string.
        type_str: String,
        /// The underlying parse error.
        #[source]
        source: serde_json::Error,
    },

    /// Failed to canonicalize manifest for hashing.
    ///
    /// This occurs when the manifest cannot be converted to canonical JSON.
    #[error("failed to canonicalize manifest")]
    Canonicalize(#[source] CanonicalizeError),

    /// Failed to serialize manifest to JSON.
    ///
    /// This occurs when the manifest struct cannot be converted to JSON.
    #[error("failed to serialize manifest to JSON")]
    Serialize(#[source] serde_json::Error),
}

/// Dependency information in the authoring manifest.
///
/// Contains the resolved hash and the original reference information
/// needed for legacy manifest conversion.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DepInfo {
    /// The content hash of the resolved dependency manifest.
    pub hash: Hash,
    /// The original namespace of the dependency.
    pub namespace: Namespace,
    /// The original name of the dependency.
    pub name: Name,
}

/// Table definition in the authoring manifest.
///
/// References SQL and IPC schema files by path and content hash.
/// For derived tables, `sql` contains the CTAS query. For raw tables,
/// `sql` is `None` since raw tables have no SQL definition.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TableDef {
    /// Reference to the SQL file containing the CTAS statement.
    /// `None` for raw tables which have no SQL definition.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sql: Option<FileRef>,
    /// Reference to the Arrow IPC schema file.
    pub ipc: FileRef,
    /// Network this table belongs to (inferred from dependencies).
    pub network: NetworkId,
}

/// Function definition in the authoring manifest.
///
/// Contains the function signature and source file reference.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AuthoringFunctionDef {
    /// Arrow data types for function input parameters.
    pub input_types: Vec<DataType>,
    /// Arrow data type for function return value.
    pub output_type: DataType,
    /// Reference to the function source file.
    pub source: FileRef,
}

/// The authoring manifest for a derived dataset.
///
/// This manifest uses file references instead of inline content,
/// which is the format used during the authoring workflow. It is
/// converted to the runtime manifest format (with inline content)
/// via the legacy bridge for deployment.
///
/// # Identity
///
/// The manifest identity is computed as `sha256(<canonical JSON bytes>)`
/// using JCS canonicalization. This hash uniquely identifies the manifest
/// content and is used for content-addressable storage.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuthoringManifest {
    /// Dataset namespace.
    pub namespace: Namespace,
    /// Dataset name.
    pub name: Name,
    /// Dataset kind (always "derived" for authoring).
    pub kind: String,
    /// Resolved dependencies with their content hashes.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub dependencies: BTreeMap<DepAlias, DepInfo>,
    /// Table definitions with file references.
    pub tables: BTreeMap<TableName, TableDef>,
    /// Function definitions with file references.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub functions: BTreeMap<FuncName, AuthoringFunctionDef>,
}

impl AuthoringManifest {
    /// Computes the canonical JSON representation of the manifest.
    ///
    /// Uses RFC 8785 (JCS) canonicalization via `serde_json_canonicalizer` to
    /// produce deterministic JSON output with sorted keys and no whitespace.
    ///
    /// # Errors
    ///
    /// Returns [`ManifestError::Serialize`] if the manifest cannot be serialized.
    /// Returns [`ManifestError::Canonicalize`] if canonicalization fails.
    pub fn canonical_json(&self) -> Result<String, ManifestError> {
        let value = serde_json::to_value(self).map_err(ManifestError::Serialize)?;
        canonicalize(&value).map_err(ManifestError::Canonicalize)
    }

    /// Computes the identity hash of the manifest.
    ///
    /// The identity is `sha256(<canonical JSON bytes>)`.
    ///
    /// # Errors
    ///
    /// Returns [`ManifestError::Serialize`] if the manifest cannot be serialized.
    /// Returns [`ManifestError::Canonicalize`] if canonicalization fails.
    pub fn identity(&self) -> Result<Hash, ManifestError> {
        let value = serde_json::to_value(self).map_err(ManifestError::Serialize)?;
        hash_canonical(&value).map_err(ManifestError::Canonicalize)
    }

    /// Serializes the manifest to pretty-printed JSON.
    ///
    /// This is useful for writing the manifest to a file for human inspection.
    /// For identity computation, use [`canonical_json`](Self::canonical_json) instead.
    ///
    /// # Errors
    ///
    /// Returns [`ManifestError::Serialize`] if serialization fails.
    pub fn to_json_pretty(&self) -> Result<String, ManifestError> {
        serde_json::to_string_pretty(self).map_err(ManifestError::Serialize)
    }
}

/// Builder for constructing an [`AuthoringManifest`] from authoring inputs.
///
/// This builder takes:
/// - The parsed `amp.yaml` configuration
/// - The resolved dependency graph
/// - The base path for file reference normalization
/// - The directory containing rendered SQL and schema files
/// - The discovered tables (from model discovery)
/// - A network resolver for inferring table networks
///
/// # Example
///
/// ```ignore
/// use dataset_authoring::manifest::ManifestBuilder;
///
/// let manifest = ManifestBuilder::new(&config, &dep_graph, project_dir, &sql_dir, &tables)
///     .with_network_resolver(|table, deps| infer_network(table, deps))
///     .build()?;
/// ```
pub struct ManifestBuilder<'a, F> {
    config: &'a AmpYaml,
    dep_graph: &'a DependencyGraph,
    base_path: &'a Path,
    sql_dir: &'a Path,
    /// Discovered tables (table name -> SQL file path relative to base_path).
    /// This replaces the old `config.tables` field.
    tables: &'a BTreeMap<TableName, PathBuf>,
    network_resolver: F,
}

impl<'a> ManifestBuilder<'a, fn(&TableName, &DependencyGraph) -> Result<NetworkId, ManifestError>> {
    /// Creates a new manifest builder with the default network resolver.
    ///
    /// The default network resolver requires all dependencies to have the
    /// same network and uses that network for all tables.
    ///
    /// # Arguments
    ///
    /// * `config` - The parsed amp.yaml configuration
    /// * `dep_graph` - The resolved dependency graph
    /// * `base_path` - Base path for file reference normalization
    /// * `sql_dir` - Directory containing rendered SQL and schema files
    /// * `tables` - Discovered tables (table name -> SQL file path relative to base_path)
    pub fn new(
        config: &'a AmpYaml,
        dep_graph: &'a DependencyGraph,
        base_path: &'a Path,
        sql_dir: &'a Path,
        tables: &'a BTreeMap<TableName, PathBuf>,
    ) -> Self {
        Self {
            config,
            dep_graph,
            base_path,
            sql_dir,
            tables,
            network_resolver: default_network_resolver,
        }
    }
}

impl<'a, F> ManifestBuilder<'a, F>
where
    F: Fn(&TableName, &DependencyGraph) -> Result<NetworkId, ManifestError>,
{
    /// Sets a custom network resolver.
    ///
    /// The resolver is called for each table to determine its network.
    pub fn with_network_resolver<G>(self, resolver: G) -> ManifestBuilder<'a, G>
    where
        G: Fn(&TableName, &DependencyGraph) -> Result<NetworkId, ManifestError>,
    {
        ManifestBuilder {
            config: self.config,
            dep_graph: self.dep_graph,
            base_path: self.base_path,
            sql_dir: self.sql_dir,
            tables: self.tables,
            network_resolver: resolver,
        }
    }

    /// Builds the authoring manifest.
    ///
    /// This method:
    /// 1. Maps dependencies from amp.yaml to resolved hashes
    /// 2. Creates file references for SQL and schema files
    /// 3. Creates file references for function source files
    /// 4. Infers networks for tables using the network resolver
    ///
    /// # Errors
    ///
    /// Returns [`ManifestError`] if any file references cannot be created
    /// or if network inference fails.
    pub fn build(self) -> Result<AuthoringManifest, ManifestError> {
        // Build dependencies map
        let dependencies = self.build_dependencies()?;

        // Build tables map
        let tables = self.build_tables()?;

        // Build functions map
        let functions = self.build_functions()?;

        Ok(AuthoringManifest {
            namespace: self.config.namespace.clone(),
            name: self.config.name.clone(),
            kind: "derived".to_string(),
            dependencies,
            tables,
            functions,
        })
    }

    fn build_dependencies(&self) -> Result<BTreeMap<DepAlias, DepInfo>, ManifestError> {
        let mut dependencies = BTreeMap::new();

        for (alias, dep_ref) in &self.config.dependencies {
            let hash = self.dep_graph.direct.get(alias).ok_or_else(|| {
                ManifestError::MissingDependency {
                    alias: alias.clone(),
                }
            })?;

            let fqn = dep_ref.fqn();
            dependencies.insert(
                alias.clone(),
                DepInfo {
                    hash: hash.clone(),
                    namespace: fqn.namespace().clone(),
                    name: fqn.name().clone(),
                },
            );
        }

        Ok(dependencies)
    }

    fn build_tables(&self) -> Result<BTreeMap<TableName, TableDef>, ManifestError> {
        let mut tables = BTreeMap::new();

        for table_name in self.tables.keys() {
            // SQL file path: sql/<table_name>.sql (rendered output)
            let sql_file_path = self.sql_dir.join(format!("{}.sql", table_name));
            let sql_ref = FileRef::from_file(&sql_file_path, self.base_path).map_err(|err| {
                ManifestError::SqlFileRef {
                    table_name: table_name.clone(),
                    source: err,
                }
            })?;

            // IPC schema file path: sql/<table_name>.schema.json
            // Note: Path will change to tables/<table_name>.ipc in Phase 3
            let ipc_file_path = self.sql_dir.join(format!("{}.schema.json", table_name));
            let ipc_ref = FileRef::from_file(&ipc_file_path, self.base_path).map_err(|err| {
                ManifestError::IpcFileRef {
                    table_name: table_name.clone(),
                    source: err,
                }
            })?;

            // Infer network using the resolver
            let network = (self.network_resolver)(table_name, self.dep_graph)?;

            tables.insert(
                table_name.clone(),
                TableDef {
                    sql: Some(sql_ref),
                    ipc: ipc_ref,
                    network,
                },
            );
        }

        Ok(tables)
    }

    fn build_functions(&self) -> Result<BTreeMap<FuncName, AuthoringFunctionDef>, ManifestError> {
        let mut functions = BTreeMap::new();

        let Some(config_functions) = &self.config.functions else {
            return Ok(functions);
        };

        for (func_name, func_def) in config_functions {
            // Use canonical output path: functions/<name>.js
            // build.rs copies input files to this canonical location
            let canonical_path = format!("functions/{}.js", func_name);
            let source_path = self.base_path.join(&canonical_path);
            let source_ref = FileRef::from_file(&source_path, self.base_path).map_err(|err| {
                ManifestError::FunctionFileRef {
                    func_name: func_name.clone(),
                    source: err,
                }
            })?;

            let input_types = parse_data_types(&func_def.input_types, func_name)?;
            let output_type = parse_data_type(&func_def.output_type, func_name)?;

            functions.insert(
                func_name.clone(),
                AuthoringFunctionDef {
                    input_types,
                    output_type,
                    source: source_ref,
                },
            );
        }

        Ok(functions)
    }
}

/// Parses a list of data type strings into Arrow data types.
fn parse_data_types(
    type_strs: &[String],
    func_name: &FuncName,
) -> Result<Vec<DataType>, ManifestError> {
    type_strs
        .iter()
        .map(|s| parse_data_type(s, func_name))
        .collect()
}

/// Parses a data type string into an Arrow data type.
///
/// Uses JSON parsing with the DataType wrapper from datasets-common.
fn parse_data_type(type_str: &str, func_name: &FuncName) -> Result<DataType, ManifestError> {
    // DataType serializes as a string (transparent), so we can parse it directly
    serde_json::from_value(serde_json::Value::String(type_str.to_string())).map_err(|err| {
        ManifestError::InvalidDataType {
            func_name: func_name.clone(),
            type_str: type_str.to_string(),
            source: err,
        }
    })
}

/// Default network resolver that infers network from dependencies.
///
/// This resolver:
/// 1. Collects all unique networks from dependencies
/// 2. If all dependencies have the same network, uses that network
/// 3. If there are no dependencies, returns an error
/// 4. If dependencies have different networks, returns an error
pub(crate) fn default_network_resolver(
    table_name: &TableName,
    dep_graph: &DependencyGraph,
) -> Result<NetworkId, ManifestError> {
    if dep_graph.is_empty() {
        return Err(ManifestError::NetworkInference {
            table_name: table_name.clone(),
            reason: "no dependencies to infer network from".to_string(),
        });
    }

    // Collect all unique networks from dependencies
    let mut networks: Vec<NetworkId> = Vec::new();
    for node in dep_graph.nodes.values() {
        for table in node.resolved.manifest.tables.values() {
            if !networks.contains(&table.network) {
                networks.push(table.network.clone());
            }
        }
    }

    match networks.len() {
        0 => Err(ManifestError::NetworkInference {
            table_name: table_name.clone(),
            reason: "dependencies have no tables with network information".to_string(),
        }),
        1 => Ok(networks.into_iter().next().expect("checked len == 1")),
        _ => Err(ManifestError::NetworkInference {
            table_name: table_name.clone(),
            reason: format!(
                "dependencies have conflicting networks: {}",
                networks
                    .iter()
                    .map(|n| n.to_string())
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
        }),
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, fs};

    use datasets_common::dataset_kind_str::DatasetKindStr;
    use tempfile::TempDir;

    use super::*;
    use crate::{
        dependency_manifest::{DependencyManifest, DependencyTable},
        resolver::{DependencyNode, ResolvedDependency},
    };

    // ============================================================
    // AuthoringManifest tests
    // ============================================================

    fn create_test_manifest() -> AuthoringManifest {
        AuthoringManifest {
            namespace: "test_ns".parse().expect("valid namespace"),
            name: "test_dataset".parse().expect("valid name"),
            kind: "derived".to_string(),
            dependencies: BTreeMap::new(),
            tables: BTreeMap::new(),
            functions: BTreeMap::new(),
        }
    }

    #[test]
    fn authoring_manifest_serializes_to_json() {
        //* Given
        let manifest = create_test_manifest();

        //* When
        let result = manifest.to_json_pretty();

        //* Then
        let json = result.expect("serialization should succeed");
        assert!(json.contains("\"namespace\": \"test_ns\""));
        assert!(json.contains("\"name\": \"test_dataset\""));
        assert!(json.contains("\"kind\": \"derived\""));
    }

    #[test]
    fn authoring_manifest_canonical_json_is_deterministic() {
        //* Given
        let manifest = create_test_manifest();

        //* When
        let json1 = manifest.canonical_json();
        let json2 = manifest.canonical_json();

        //* Then
        let j1 = json1.expect("should succeed");
        let j2 = json2.expect("should succeed");
        assert_eq!(j1, j2, "canonical JSON should be deterministic");
    }

    #[test]
    fn authoring_manifest_canonical_json_has_no_whitespace() {
        //* Given
        let manifest = create_test_manifest();

        //* When
        let result = manifest.canonical_json();

        //* Then
        let json = result.expect("should succeed");
        assert!(!json.contains('\n'), "should have no newlines");
        // Note: spaces may exist in string values, but not between tokens
        assert!(!json.contains(": "), "should have no space after colons");
    }

    #[test]
    fn authoring_manifest_identity_is_deterministic() {
        //* Given
        let manifest = create_test_manifest();

        //* When
        let hash1 = manifest.identity();
        let hash2 = manifest.identity();

        //* Then
        let h1 = hash1.expect("should succeed");
        let h2 = hash2.expect("should succeed");
        assert_eq!(h1, h2, "identity hash should be deterministic");
    }

    #[test]
    fn authoring_manifest_identity_differs_for_different_content() {
        //* Given
        let manifest1 = create_test_manifest();
        let mut manifest2 = create_test_manifest();
        manifest2.name = "other_dataset".parse().expect("valid name");

        //* When
        let hash1 = manifest1.identity();
        let hash2 = manifest2.identity();

        //* Then
        let h1 = hash1.expect("should succeed");
        let h2 = hash2.expect("should succeed");
        assert_ne!(
            h1, h2,
            "different content should produce different identity"
        );
    }

    #[test]
    fn authoring_manifest_roundtrip_serialization() {
        //* Given
        let original = create_test_manifest();

        //* When
        let json = serde_json::to_string(&original).expect("serialize should succeed");
        let deserialized: AuthoringManifest =
            serde_json::from_str(&json).expect("deserialize should succeed");

        //* Then
        assert_eq!(deserialized, original);
    }

    #[test]
    fn authoring_manifest_skips_empty_optional_fields() {
        //* Given
        let manifest = AuthoringManifest {
            namespace: "test_ns".parse().expect("valid namespace"),
            name: "test_dataset".parse().expect("valid name"),
            kind: "derived".to_string(),
            dependencies: BTreeMap::new(),
            tables: BTreeMap::new(),
            functions: BTreeMap::new(),
        };

        //* When
        let result = manifest.to_json_pretty();

        //* Then
        let json = result.expect("serialization should succeed");
        assert!(
            !json.contains("dependencies"),
            "should skip empty dependencies"
        );
        assert!(!json.contains("functions"), "should skip empty functions");
    }

    // ============================================================
    // DepInfo tests
    // ============================================================

    #[test]
    fn dep_info_serializes_correctly() {
        //* Given
        let dep_info = DepInfo {
            hash: "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9"
                .parse()
                .expect("valid hash"),
            namespace: "test_ns".parse().expect("valid namespace"),
            name: "test_dep".parse().expect("valid name"),
        };

        //* When
        let json = serde_json::to_string(&dep_info);

        //* Then
        let json_str = json.expect("serialization should succeed");
        assert!(
            json_str.contains("b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9")
        );
        assert!(json_str.contains("test_ns"));
        assert!(json_str.contains("test_dep"));
    }

    // ============================================================
    // TableDef tests
    // ============================================================

    #[test]
    fn table_def_serializes_correctly() {
        //* Given
        let table_def = TableDef {
            sql: Some(FileRef::new(
                "tables/transfers.sql".to_string(),
                "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                    .parse()
                    .expect("valid hash"),
            )),
            ipc: FileRef::new(
                "tables/transfers.ipc".to_string(),
                "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
                    .parse()
                    .expect("valid hash"),
            ),
            network: "mainnet".parse().expect("valid network"),
        };

        //* When
        let json = serde_json::to_string_pretty(&table_def);

        //* Then
        let json_str = json.expect("serialization should succeed");
        assert!(json_str.contains("tables/transfers.sql"));
        assert!(json_str.contains("tables/transfers.ipc"));
        assert!(json_str.contains("mainnet"));
    }

    #[test]
    fn table_def_without_sql_omits_sql_field() {
        //* Given - a raw table with no SQL definition
        let table_def = TableDef {
            sql: None,
            ipc: FileRef::new(
                "tables/raw_events.ipc".to_string(),
                "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
                    .parse()
                    .expect("valid hash"),
            ),
            network: "mainnet".parse().expect("valid network"),
        };

        //* When
        let json = serde_json::to_string_pretty(&table_def);

        //* Then
        let json_str = json.expect("serialization should succeed");
        assert!(
            !json_str.contains("\"sql\""),
            "sql field should be omitted when None"
        );
        assert!(json_str.contains("tables/raw_events.ipc"));
        assert!(json_str.contains("mainnet"));
    }

    #[test]
    fn table_def_roundtrip_with_optional_sql() {
        //* Given - a table with SQL
        let table_def_with_sql = TableDef {
            sql: Some(FileRef::new(
                "tables/transfers.sql".to_string(),
                "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                    .parse()
                    .expect("valid hash"),
            )),
            ipc: FileRef::new(
                "tables/transfers.ipc".to_string(),
                "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
                    .parse()
                    .expect("valid hash"),
            ),
            network: "mainnet".parse().expect("valid network"),
        };

        //* When - roundtrip serialize/deserialize
        let json = serde_json::to_string(&table_def_with_sql).expect("serialize should succeed");
        let deserialized: TableDef =
            serde_json::from_str(&json).expect("deserialize should succeed");

        //* Then
        assert_eq!(deserialized, table_def_with_sql);

        //* Given - a table without SQL (raw table)
        let table_def_without_sql = TableDef {
            sql: None,
            ipc: FileRef::new(
                "tables/raw_events.ipc".to_string(),
                "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
                    .parse()
                    .expect("valid hash"),
            ),
            network: "mainnet".parse().expect("valid network"),
        };

        //* When
        let json = serde_json::to_string(&table_def_without_sql).expect("serialize should succeed");
        let deserialized: TableDef =
            serde_json::from_str(&json).expect("deserialize should succeed");

        //* Then
        assert_eq!(deserialized, table_def_without_sql);
    }

    // ============================================================
    // ManifestBuilder tests
    // ============================================================

    fn create_test_config() -> AmpYaml {
        let yaml = r#"
amp: 1.0.0
namespace: test_ns
name: test_dataset
version: 1.0.0
models: models
"#;
        AmpYaml::from_yaml(yaml).expect("valid config")
    }

    fn create_test_tables() -> BTreeMap<TableName, PathBuf> {
        let mut tables = BTreeMap::new();
        tables.insert(
            "transfers".parse().expect("valid table name"),
            PathBuf::from("models/transfers.sql"),
        );
        tables
    }

    fn create_test_dep_graph_with_network(network: &str) -> DependencyGraph {
        let mut graph = DependencyGraph::new();
        let hash: Hash = "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9"
            .parse()
            .expect("valid hash");

        let mut tables = BTreeMap::new();
        tables.insert(
            "source_table".parse().expect("valid table name"),
            DependencyTable {
                schema: datasets_common::manifest::TableSchema {
                    arrow: datasets_common::manifest::ArrowSchema { fields: vec![] },
                },
                network: network.parse().expect("valid network"),
            },
        );

        let manifest = DependencyManifest {
            kind: DatasetKindStr::new("manifest".to_string()),
            dependencies: BTreeMap::new(),
            tables,
            functions: BTreeMap::new(),
        };

        graph.nodes.insert(
            hash.clone(),
            DependencyNode {
                resolved: ResolvedDependency {
                    hash: hash.clone(),
                    manifest,
                    namespace: "test".parse().expect("valid namespace"),
                    name: "dependency".parse().expect("valid name"),
                },
                deps: vec![],
            },
        );

        graph
    }

    #[test]
    fn manifest_builder_builds_minimal_manifest() {
        //* Given
        let dir = TempDir::new().expect("should create temp dir");
        let sql_dir = dir.path().join("sql");
        fs::create_dir(&sql_dir).expect("should create sql dir");

        // Create SQL and schema files
        fs::write(sql_dir.join("transfers.sql"), "SELECT 1").expect("should write sql");
        fs::write(
            sql_dir.join("transfers.schema.json"),
            r#"{"arrow":{"fields":[]}}"#,
        )
        .expect("should write schema");

        let config = create_test_config();
        let tables = create_test_tables();
        let dep_graph = create_test_dep_graph_with_network("mainnet");

        //* When
        let result =
            ManifestBuilder::new(&config, &dep_graph, dir.path(), &sql_dir, &tables).build();

        //* Then
        let manifest = result.expect("build should succeed");
        assert_eq!(manifest.namespace, "test_ns");
        assert_eq!(manifest.name, "test_dataset");
        assert_eq!(manifest.kind, "derived");
        assert_eq!(manifest.tables.len(), 1);
        assert!(manifest.tables.contains_key(&"transfers".parse().unwrap()));
    }

    #[test]
    fn manifest_builder_infers_network_from_deps() {
        //* Given
        let dir = TempDir::new().expect("should create temp dir");
        let sql_dir = dir.path().join("sql");
        fs::create_dir(&sql_dir).expect("should create sql dir");

        fs::write(sql_dir.join("transfers.sql"), "SELECT 1").expect("should write sql");
        fs::write(
            sql_dir.join("transfers.schema.json"),
            r#"{"arrow":{"fields":[]}}"#,
        )
        .expect("should write schema");

        let config = create_test_config();
        let tables = create_test_tables();
        let dep_graph = create_test_dep_graph_with_network("mainnet");

        //* When
        let result =
            ManifestBuilder::new(&config, &dep_graph, dir.path(), &sql_dir, &tables).build();

        //* Then
        let manifest = result.expect("build should succeed");
        let table = manifest
            .tables
            .get(&"transfers".parse().unwrap())
            .expect("should have table");
        assert_eq!(table.network.to_string(), "mainnet");
    }

    #[test]
    fn manifest_builder_fails_on_missing_sql_file() {
        //* Given
        let dir = TempDir::new().expect("should create temp dir");
        let sql_dir = dir.path().join("sql");
        fs::create_dir(&sql_dir).expect("should create sql dir");
        // Don't create the SQL file

        let config = create_test_config();
        let tables = create_test_tables();
        let dep_graph = create_test_dep_graph_with_network("mainnet");

        //* When
        let result =
            ManifestBuilder::new(&config, &dep_graph, dir.path(), &sql_dir, &tables).build();

        //* Then
        assert!(result.is_err());
        let err = result.expect_err("should fail");
        assert!(
            matches!(err, ManifestError::SqlFileRef { .. }),
            "should be SqlFileRef error, got: {:?}",
            err
        );
    }

    #[test]
    fn manifest_builder_fails_on_missing_ipc_file() {
        //* Given
        let dir = TempDir::new().expect("should create temp dir");
        let sql_dir = dir.path().join("sql");
        fs::create_dir(&sql_dir).expect("should create sql dir");

        // Create SQL but not IPC schema
        fs::write(sql_dir.join("transfers.sql"), "SELECT 1").expect("should write sql");

        let config = create_test_config();
        let tables = create_test_tables();
        let dep_graph = create_test_dep_graph_with_network("mainnet");

        //* When
        let result =
            ManifestBuilder::new(&config, &dep_graph, dir.path(), &sql_dir, &tables).build();

        //* Then
        assert!(result.is_err());
        let err = result.expect_err("should fail");
        assert!(
            matches!(err, ManifestError::IpcFileRef { .. }),
            "should be IpcFileRef error, got: {:?}",
            err
        );
    }

    #[test]
    fn manifest_builder_with_custom_network_resolver() {
        //* Given
        let dir = TempDir::new().expect("should create temp dir");
        let sql_dir = dir.path().join("sql");
        fs::create_dir(&sql_dir).expect("should create sql dir");

        fs::write(sql_dir.join("transfers.sql"), "SELECT 1").expect("should write sql");
        fs::write(
            sql_dir.join("transfers.schema.json"),
            r#"{"arrow":{"fields":[]}}"#,
        )
        .expect("should write schema");

        let config = create_test_config();
        let tables = create_test_tables();
        let dep_graph = DependencyGraph::new(); // Empty graph

        // Custom resolver that always returns "testnet"
        let custom_resolver =
            |_table: &TableName, _graph: &DependencyGraph| -> Result<NetworkId, ManifestError> {
                Ok("testnet".parse().expect("valid network"))
            };

        //* When
        let result = ManifestBuilder::new(&config, &dep_graph, dir.path(), &sql_dir, &tables)
            .with_network_resolver(custom_resolver)
            .build();

        //* Then
        let manifest = result.expect("build should succeed");
        let table = manifest
            .tables
            .get(&"transfers".parse().unwrap())
            .expect("should have table");
        assert_eq!(table.network.to_string(), "testnet");
    }

    // ============================================================
    // Network inference tests
    // ============================================================

    #[test]
    fn default_network_resolver_fails_on_empty_graph() {
        //* Given
        let dep_graph = DependencyGraph::new();
        let table_name: TableName = "test".parse().expect("valid");

        //* When
        let result = default_network_resolver(&table_name, &dep_graph);

        //* Then
        assert!(result.is_err());
        let err = result.expect_err("should fail");
        assert!(
            matches!(err, ManifestError::NetworkInference { .. }),
            "should be NetworkInference error, got: {:?}",
            err
        );
    }

    #[test]
    fn default_network_resolver_succeeds_with_single_network() {
        //* Given
        let dep_graph = create_test_dep_graph_with_network("mainnet");
        let table_name: TableName = "test".parse().expect("valid");

        //* When
        let result = default_network_resolver(&table_name, &dep_graph);

        //* Then
        let network = result.expect("should succeed");
        assert_eq!(network.to_string(), "mainnet");
    }

    // ============================================================
    // Function path canonicalization tests
    // ============================================================

    #[test]
    fn manifest_builder_uses_canonical_function_path() {
        //* Given
        // Create a config with a non-canonical function path (lib/helpers/my_func.js)
        let yaml = r#"
amp: 1.0.0
namespace: test_ns
name: test_dataset
version: 1.0.0
models: models
functions:
  my_func:
    source: lib/helpers/my_func.js
    input_types: ["Binary"]
    output_type: Utf8
"#;
        let config = AmpYaml::from_yaml(yaml).expect("valid config");
        let tables = create_test_tables();

        // Set up directory with canonical output structure
        let dir = TempDir::new().expect("should create temp dir");
        let sql_dir = dir.path().join("sql");
        let functions_dir = dir.path().join("functions");
        fs::create_dir_all(&sql_dir).expect("should create sql dir");
        fs::create_dir_all(&functions_dir).expect("should create functions dir");

        // Create required files at canonical locations
        fs::write(sql_dir.join("transfers.sql"), "SELECT 1").expect("should write sql");
        fs::write(
            sql_dir.join("transfers.schema.json"),
            r#"{"arrow":{"fields":[]}}"#,
        )
        .expect("should write schema");

        // Function file at CANONICAL path (build.rs copies to this location)
        fs::write(
            functions_dir.join("my_func.js"),
            "function my_func(input) { return input; }",
        )
        .expect("should write function");

        let dep_graph = create_test_dep_graph_with_network("mainnet");

        //* When
        let result =
            ManifestBuilder::new(&config, &dep_graph, dir.path(), &sql_dir, &tables).build();

        //* Then
        let manifest = result.expect("build should succeed");
        let func = manifest
            .functions
            .get(&"my_func".parse().unwrap())
            .expect("should have function");

        // Verify the manifest uses the canonical path, not the input path
        assert_eq!(
            func.source.path, "functions/my_func.js",
            "function path should be canonical (functions/<name>.js), not input path"
        );
    }
}
