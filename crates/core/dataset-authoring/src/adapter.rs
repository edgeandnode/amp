//! Legacy manifest adapter.
//!
//! Converts legacy inline manifest format (from admin API) to the canonical
//! package format used by dataset authoring.
//!
//! # Overview
//!
//! The admin API returns manifests in legacy inline format where SQL, schemas,
//! and function sources are embedded directly in the JSON. The canonical package
//! format uses file references instead:
//!
//! - `tables/<table>.sql` contains the SQL string
//! - `tables/<table>.ipc` contains the Arrow IPC binary schema
//! - `functions/<name>.js` contains the function source code
//! - `manifest.json` contains file references with content hashes
//!
//! This module provides the [`LegacyAdapter`] to convert between formats, enabling
//! the cache to store the richer canonical package structure.
//!
//! # Example
//!
//! ```ignore
//! use dataset_authoring::adapter::LegacyAdapter;
//!
//! let legacy_json: serde_json::Value = fetch_from_admin_api().await?;
//! let adapter = LegacyAdapter::new(&cache_dir);
//! let manifest = adapter.adapt_legacy_manifest(legacy_json)?;
//! ```

use std::{fs, io, path::Path, sync::Arc};

use datasets_common::manifest::TableSchema;
use datasets_derived::manifest::{Manifest as LegacyManifest, TableInput};

use crate::{arrow_ipc, files::FileRef};

/// Errors that occur during legacy manifest adaptation.
#[derive(Debug, thiserror::Error)]
pub enum AdapterError {
    /// Failed to parse legacy manifest JSON.
    ///
    /// This occurs when the admin API returns JSON that cannot be parsed
    /// as a valid legacy manifest structure.
    #[error("failed to parse legacy manifest JSON")]
    ParseLegacyManifest(#[source] serde_json::Error),

    /// Failed to create tables directory.
    ///
    /// This occurs when the adapter cannot create the `tables/` directory
    /// in the target cache location.
    #[error("failed to create tables directory at '{path}'")]
    CreateTablesDir {
        path: String,
        #[source]
        source: io::Error,
    },

    /// Failed to create functions directory.
    ///
    /// This occurs when the adapter cannot create the `functions/` directory
    /// in the target cache location.
    #[error("failed to create functions directory at '{path}'")]
    CreateFunctionsDir {
        path: String,
        #[source]
        source: io::Error,
    },

    /// Failed to write SQL file for a table.
    ///
    /// This occurs when the adapter cannot write the extracted SQL content
    /// to the `tables/<table>.sql` file.
    #[error("failed to write SQL file for table '{table_name}' at '{path}'")]
    WriteSqlFile {
        table_name: String,
        path: String,
        #[source]
        source: io::Error,
    },

    /// Failed to write IPC schema file for a table.
    ///
    /// This occurs when the adapter cannot write the converted Arrow IPC
    /// schema to the `tables/<table>.ipc` file.
    #[error("failed to write IPC schema file for table '{table_name}' at '{path}'")]
    WriteIpcSchemaFile {
        table_name: String,
        path: String,
        #[source]
        source: arrow_ipc::IpcSchemaError,
    },

    /// Failed to write function source file.
    ///
    /// This occurs when the adapter cannot write the extracted function
    /// source code to the `functions/<name>.js` file.
    #[error("failed to write function source file for function '{func_name}' at '{path}'")]
    WriteFunctionFile {
        func_name: String,
        path: String,
        #[source]
        source: io::Error,
    },
}

/// Adapter for converting legacy inline manifests to canonical package format.
///
/// The adapter extracts inline content from legacy manifests (as received from
/// the admin API) and writes them as separate files in the canonical package
/// structure expected by the cache.
///
/// # Usage
///
/// ```ignore
/// let adapter = LegacyAdapter::new("/path/to/cache/hash");
/// let dep_manifest = adapter.adapt_legacy_manifest(legacy_json)?;
/// ```
pub struct LegacyAdapter<'a> {
    /// Target directory for the canonical package.
    target_dir: &'a Path,
}

impl<'a> LegacyAdapter<'a> {
    /// Creates a new legacy adapter with the specified target directory.
    ///
    /// The target directory is where the canonical package structure will be
    /// written (e.g., `~/.amp/registry/<hash>/`).
    pub fn new(target_dir: &'a Path) -> Self {
        Self { target_dir }
    }

    /// Adapts a legacy manifest JSON to canonical package format.
    ///
    /// This method:
    /// 1. Parses the legacy manifest JSON
    /// 2. Extracts inline SQL and writes to `tables/<table>.sql`
    /// 3. Converts inline schemas and writes to `tables/<table>.ipc`
    /// 4. Extracts function sources and writes to `functions/<name>.js`
    /// 5. Returns a [`DependencyManifest`](crate::dependency_manifest::DependencyManifest)
    ///    for use in dependency resolution
    ///
    /// # Errors
    ///
    /// Returns [`AdapterError`] if:
    /// - The JSON cannot be parsed as a legacy manifest
    /// - Required directories cannot be created
    /// - Files cannot be written
    pub fn adapt_legacy_manifest(
        &self,
        legacy_json: &serde_json::Value,
    ) -> Result<AdaptedPackage, AdapterError> {
        // Parse the legacy manifest
        let legacy: LegacyManifest = serde_json::from_value(legacy_json.clone())
            .map_err(AdapterError::ParseLegacyManifest)?;

        // Extract and write tables
        let tables = self.extract_tables(&legacy)?;

        // Extract and write functions
        let functions = self.extract_functions(&legacy)?;

        Ok(AdaptedPackage {
            kind: legacy.kind.to_string(),
            dependencies: legacy.dependencies,
            tables,
            functions,
        })
    }

    /// Extracts tables from legacy manifest and writes files.
    fn extract_tables(
        &self,
        legacy: &LegacyManifest,
    ) -> Result<
        std::collections::BTreeMap<datasets_common::table_name::TableName, AdaptedTable>,
        AdapterError,
    > {
        use std::collections::BTreeMap;

        if legacy.tables.is_empty() {
            return Ok(BTreeMap::new());
        }

        // Create tables directory
        let tables_dir = self.target_dir.join("tables");
        fs::create_dir_all(&tables_dir).map_err(|err| AdapterError::CreateTablesDir {
            path: tables_dir.display().to_string(),
            source: err,
        })?;

        let mut tables = BTreeMap::new();

        for (table_name, table) in &legacy.tables {
            // Extract SQL from View input
            let sql_content = match &table.input {
                TableInput::View(view) => view.sql.as_str(),
            };

            // Write SQL file
            let sql_path = tables_dir.join(format!("{table_name}.sql"));
            fs::write(&sql_path, sql_content).map_err(|err| AdapterError::WriteSqlFile {
                table_name: table_name.to_string(),
                path: sql_path.display().to_string(),
                source: err,
            })?;

            // Convert inline schema to Arrow SchemaRef and write IPC file
            let schema_ref: datafusion::arrow::datatypes::SchemaRef =
                table.schema.arrow.clone().into_schema_ref();
            let ipc_path = tables_dir.join(format!("{table_name}.ipc"));
            arrow_ipc::write_ipc_schema(&schema_ref, &ipc_path).map_err(|err| {
                AdapterError::WriteIpcSchemaFile {
                    table_name: table_name.to_string(),
                    path: ipc_path.display().to_string(),
                    source: err,
                }
            })?;

            // Create file references with content hashes
            let sql_ref = FileRef::from_file(&sql_path, self.target_dir)
                .expect("SQL file should exist after writing");
            let ipc_ref = FileRef::from_file(&ipc_path, self.target_dir)
                .expect("IPC file should exist after writing");

            tables.insert(
                table_name.clone(),
                AdaptedTable {
                    sql: Some(sql_ref),
                    ipc: ipc_ref,
                    network: table.network.clone(),
                    // Store the original schema for DependencyManifest compatibility
                    schema: table.schema.clone(),
                },
            );
        }

        Ok(tables)
    }

    /// Extracts functions from legacy manifest and writes files.
    fn extract_functions(
        &self,
        legacy: &LegacyManifest,
    ) -> Result<
        std::collections::BTreeMap<datasets_derived::func_name::FuncName, AdaptedFunction>,
        AdapterError,
    > {
        use std::collections::BTreeMap;

        if legacy.functions.is_empty() {
            return Ok(BTreeMap::new());
        }

        // Create functions directory
        let functions_dir = self.target_dir.join("functions");
        fs::create_dir_all(&functions_dir).map_err(|err| AdapterError::CreateFunctionsDir {
            path: functions_dir.display().to_string(),
            source: err,
        })?;

        let mut functions = BTreeMap::new();

        for (func_name, func) in &legacy.functions {
            // Write function source file
            let func_path = functions_dir.join(&func.source.filename);
            fs::write(&func_path, func.source.source.as_ref()).map_err(|err| {
                AdapterError::WriteFunctionFile {
                    func_name: func_name.to_string(),
                    path: func_path.display().to_string(),
                    source: err,
                }
            })?;

            // Create file reference with content hash
            let source_ref = FileRef::from_file(&func_path, self.target_dir)
                .expect("Function file should exist after writing");

            functions.insert(
                func_name.clone(),
                AdaptedFunction {
                    input_types: func.input_types.clone(),
                    output_type: func.output_type.clone(),
                    source: source_ref,
                },
            );
        }

        Ok(functions)
    }
}

/// Result of adapting a legacy manifest to canonical package format.
///
/// Contains both file references (for the package structure) and the original
/// schema data (for dependency resolution compatibility).
#[derive(Debug, Clone)]
pub struct AdaptedPackage {
    /// Dataset kind (e.g., "manifest").
    pub kind: String,
    /// Dependencies from the legacy manifest.
    pub dependencies: std::collections::BTreeMap<
        datasets_derived::deps::DepAlias,
        datasets_derived::deps::DepReference,
    >,
    /// Adapted table definitions with file references.
    pub tables: std::collections::BTreeMap<datasets_common::table_name::TableName, AdaptedTable>,
    /// Adapted function definitions with file references.
    pub functions:
        std::collections::BTreeMap<datasets_derived::func_name::FuncName, AdaptedFunction>,
}

impl AdaptedPackage {
    /// Converts to a [`DependencyManifest`](crate::dependency_manifest::DependencyManifest)
    /// for use in dependency resolution.
    ///
    /// This method creates a `DependencyManifest` from the adapted package,
    /// using the original schema data for compatibility with existing resolution logic.
    pub fn into_dependency_manifest(self) -> crate::dependency_manifest::DependencyManifest {
        use datasets_common::dataset_kind_str::DatasetKindStr;

        use crate::dependency_manifest::{DependencyManifest, DependencyTable};

        DependencyManifest {
            kind: DatasetKindStr::new(self.kind),
            dependencies: self.dependencies,
            tables: self
                .tables
                .into_iter()
                .map(|(name, table)| {
                    (
                        name,
                        DependencyTable {
                            schema: table.schema,
                            network: table.network,
                        },
                    )
                })
                .collect(),
            functions: self
                .functions
                .into_iter()
                .map(|(name, func)| {
                    (
                        name,
                        datasets_common::manifest::Function {
                            input_types: func.input_types,
                            output_type: func.output_type,
                            source: datasets_common::manifest::FunctionSource {
                                source: Arc::from(""),
                                filename: func.source.path.clone(),
                            },
                        },
                    )
                })
                .collect(),
        }
    }
}

/// Adapted table definition with file references.
#[derive(Debug, Clone)]
pub struct AdaptedTable {
    /// Reference to the SQL file (derived tables only).
    pub sql: Option<FileRef>,
    /// Reference to the IPC schema file.
    pub ipc: FileRef,
    /// Network for the table.
    pub network: datasets_common::network_id::NetworkId,
    /// Original schema for DependencyManifest compatibility.
    pub schema: TableSchema,
}

/// Adapted function definition with file reference.
#[derive(Debug, Clone)]
pub struct AdaptedFunction {
    /// Function input types.
    pub input_types: Vec<datasets_common::manifest::DataType>,
    /// Function output type.
    pub output_type: datasets_common::manifest::DataType,
    /// Reference to the function source file.
    pub source: FileRef,
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::datatypes::DataType as ArrowDataType;
    use tempfile::TempDir;

    use super::*;

    // ============================================================
    // Helper functions
    // ============================================================

    fn create_legacy_manifest_json() -> serde_json::Value {
        serde_json::json!({
            "kind": "manifest",
            "dependencies": {},
            "tables": {
                "transfers": {
                    "input": {
                        "sql": "CREATE TABLE transfers AS SELECT * FROM source.transactions"
                    },
                    "schema": {
                        "arrow": {
                            "fields": [
                                {
                                    "name": "id",
                                    "type": "UInt64",
                                    "nullable": false
                                },
                                {
                                    "name": "from_address",
                                    "type": "Utf8",
                                    "nullable": true
                                }
                            ]
                        }
                    },
                    "network": "mainnet"
                }
            },
            "functions": {}
        })
    }

    fn create_legacy_manifest_with_function() -> serde_json::Value {
        serde_json::json!({
            "kind": "manifest",
            "dependencies": {},
            "tables": {},
            "functions": {
                "decode": {
                    "inputTypes": ["Binary"],
                    "outputType": "Utf8",
                    "source": {
                        "source": "function decode(input) { return input.toString(); }",
                        "filename": "decode.js"
                    }
                }
            }
        })
    }

    fn create_legacy_manifest_with_dependencies() -> serde_json::Value {
        serde_json::json!({
            "kind": "manifest",
            "dependencies": {
                "eth": "dep_ns/eth_mainnet@b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9"
            },
            "tables": {},
            "functions": {}
        })
    }

    // ============================================================
    // LegacyAdapter::new tests
    // ============================================================

    #[test]
    fn legacy_adapter_new_creates_instance() {
        //* Given
        let dir = TempDir::new().expect("should create temp dir");

        //* When
        let adapter = LegacyAdapter::new(dir.path());

        //* Then
        assert_eq!(adapter.target_dir, dir.path());
    }

    // ============================================================
    // LegacyAdapter::adapt_legacy_manifest tests - minimal manifest
    // ============================================================

    #[test]
    fn adapt_minimal_manifest_succeeds() {
        //* Given
        let dir = TempDir::new().expect("should create temp dir");
        let json = serde_json::json!({
            "kind": "manifest",
            "dependencies": {},
            "tables": {},
            "functions": {}
        });
        let adapter = LegacyAdapter::new(dir.path());

        //* When
        let result = adapter.adapt_legacy_manifest(&json);

        //* Then
        let package = result.expect("adaptation should succeed");
        assert_eq!(package.kind, "manifest");
        assert!(package.tables.is_empty());
        assert!(package.functions.is_empty());
    }

    // ============================================================
    // LegacyAdapter::adapt_legacy_manifest tests - tables
    // ============================================================

    #[test]
    fn adapt_manifest_extracts_sql_to_file() {
        //* Given
        let dir = TempDir::new().expect("should create temp dir");
        let json = create_legacy_manifest_json();
        let adapter = LegacyAdapter::new(dir.path());

        //* When
        let result = adapter.adapt_legacy_manifest(&json);

        //* Then
        let package = result.expect("adaptation should succeed");
        assert_eq!(package.tables.len(), 1);

        // Verify SQL file was written
        let sql_path = dir.path().join("tables/transfers.sql");
        assert!(sql_path.exists(), "SQL file should exist");

        let sql_content = fs::read_to_string(&sql_path).expect("should read SQL file");
        assert!(
            sql_content.contains("CREATE TABLE transfers"),
            "SQL file should contain the query"
        );
    }

    #[test]
    fn adapt_manifest_extracts_schema_to_ipc() {
        //* Given
        let dir = TempDir::new().expect("should create temp dir");
        let json = create_legacy_manifest_json();
        let adapter = LegacyAdapter::new(dir.path());

        //* When
        let result = adapter.adapt_legacy_manifest(&json);

        //* Then
        let _package = result.expect("adaptation should succeed");

        // Verify IPC schema file was written
        let ipc_path = dir.path().join("tables/transfers.ipc");
        assert!(ipc_path.exists(), "IPC schema file should exist");

        // Verify the schema can be read back
        let schema_ref =
            arrow_ipc::read_ipc_schema(&ipc_path).expect("should read IPC schema file");
        assert_eq!(schema_ref.fields().len(), 2);
        assert_eq!(schema_ref.field(0).name(), "id");
        assert_eq!(schema_ref.field(1).name(), "from_address");
    }

    #[test]
    fn adapt_manifest_creates_file_refs_with_hashes() {
        //* Given
        let dir = TempDir::new().expect("should create temp dir");
        let json = create_legacy_manifest_json();
        let adapter = LegacyAdapter::new(dir.path());

        //* When
        let result = adapter.adapt_legacy_manifest(&json);

        //* Then
        let package = result.expect("adaptation should succeed");
        let table = package
            .tables
            .get(&"transfers".parse().unwrap())
            .expect("should have table");

        // Verify file references
        assert!(table.sql.is_some(), "should have SQL file ref");
        let sql_ref = table.sql.as_ref().unwrap();
        assert_eq!(sql_ref.path, "tables/transfers.sql");
        assert!(!sql_ref.hash.as_str().is_empty(), "should have hash");

        assert_eq!(table.ipc.path, "tables/transfers.ipc");
        assert!(!table.ipc.hash.as_str().is_empty(), "should have hash");
    }

    #[test]
    fn adapt_manifest_preserves_network() {
        //* Given
        let dir = TempDir::new().expect("should create temp dir");
        let json = create_legacy_manifest_json();
        let adapter = LegacyAdapter::new(dir.path());

        //* When
        let result = adapter.adapt_legacy_manifest(&json);

        //* Then
        let package = result.expect("adaptation should succeed");
        let table = package
            .tables
            .get(&"transfers".parse().unwrap())
            .expect("should have table");

        assert_eq!(table.network.to_string(), "mainnet");
    }

    // ============================================================
    // LegacyAdapter::adapt_legacy_manifest tests - functions
    // ============================================================

    #[test]
    fn adapt_manifest_extracts_function_source() {
        //* Given
        let dir = TempDir::new().expect("should create temp dir");
        let json = create_legacy_manifest_with_function();
        let adapter = LegacyAdapter::new(dir.path());

        //* When
        let result = adapter.adapt_legacy_manifest(&json);

        //* Then
        let package = result.expect("adaptation should succeed");
        assert_eq!(package.functions.len(), 1);

        // Verify function file was written
        let func_path = dir.path().join("functions/decode.js");
        assert!(func_path.exists(), "function file should exist");

        let func_content = fs::read_to_string(&func_path).expect("should read function file");
        assert!(
            func_content.contains("function decode"),
            "function file should contain the source"
        );
    }

    #[test]
    fn adapt_manifest_preserves_function_types() {
        //* Given
        let dir = TempDir::new().expect("should create temp dir");
        let json = create_legacy_manifest_with_function();
        let adapter = LegacyAdapter::new(dir.path());

        //* When
        let result = adapter.adapt_legacy_manifest(&json);

        //* Then
        let package = result.expect("adaptation should succeed");
        let func = package
            .functions
            .get(&"decode".parse().unwrap())
            .expect("should have function");

        assert_eq!(func.input_types.len(), 1);
        assert_eq!(*func.input_types[0].as_arrow(), ArrowDataType::Binary);
        assert_eq!(*func.output_type.as_arrow(), ArrowDataType::Utf8);
    }

    // ============================================================
    // LegacyAdapter::adapt_legacy_manifest tests - dependencies
    // ============================================================

    #[test]
    fn adapt_manifest_preserves_dependencies() {
        //* Given
        let dir = TempDir::new().expect("should create temp dir");
        let json = create_legacy_manifest_with_dependencies();
        let adapter = LegacyAdapter::new(dir.path());

        //* When
        let result = adapter.adapt_legacy_manifest(&json);

        //* Then
        let package = result.expect("adaptation should succeed");
        assert_eq!(package.dependencies.len(), 1);
        assert!(package.dependencies.contains_key(&"eth".parse().unwrap()));
    }

    // ============================================================
    // AdaptedPackage::into_dependency_manifest tests
    // ============================================================

    #[test]
    fn into_dependency_manifest_converts_correctly() {
        //* Given
        let dir = TempDir::new().expect("should create temp dir");
        let json = create_legacy_manifest_json();
        let adapter = LegacyAdapter::new(dir.path());
        let package = adapter
            .adapt_legacy_manifest(&json)
            .expect("adaptation should succeed");

        //* When
        let dep_manifest = package.into_dependency_manifest();

        //* Then
        assert_eq!(dep_manifest.kind.as_str(), "manifest");
        assert_eq!(dep_manifest.tables.len(), 1);

        let table = dep_manifest
            .tables
            .get(&"transfers".parse().unwrap())
            .expect("should have table");
        assert_eq!(table.schema.arrow.fields.len(), 2);
        assert_eq!(table.network.to_string(), "mainnet");
    }

    // ============================================================
    // Error handling tests
    // ============================================================

    #[test]
    fn adapt_manifest_fails_on_invalid_json() {
        //* Given
        let dir = TempDir::new().expect("should create temp dir");
        let json = serde_json::json!({
            "invalid": "structure"
        });
        let adapter = LegacyAdapter::new(dir.path());

        //* When
        let result = adapter.adapt_legacy_manifest(&json);

        //* Then
        assert!(result.is_err(), "should fail on invalid JSON");
        let err = result.expect_err("should have error");
        assert!(
            matches!(err, AdapterError::ParseLegacyManifest(_)),
            "should be ParseLegacyManifest error"
        );
    }

    // ============================================================
    // Round-trip tests (adapter -> bridge -> compare)
    // ============================================================

    #[test]
    fn round_trip_table_content_preserved() {
        //* Given
        let dir = TempDir::new().expect("should create temp dir");
        let original_sql = "CREATE TABLE transfers AS SELECT * FROM source.transactions";
        let json = serde_json::json!({
            "kind": "manifest",
            "dependencies": {},
            "tables": {
                "transfers": {
                    "input": {
                        "sql": original_sql
                    },
                    "schema": {
                        "arrow": {
                            "fields": [
                                {
                                    "name": "id",
                                    "type": "UInt64",
                                    "nullable": false
                                }
                            ]
                        }
                    },
                    "network": "mainnet"
                }
            },
            "functions": {}
        });

        //* When - adapt from legacy
        let adapter = LegacyAdapter::new(dir.path());
        let _package = adapter
            .adapt_legacy_manifest(&json)
            .expect("adaptation should succeed");

        //* Then - verify SQL content is preserved
        let sql_path = dir.path().join("tables/transfers.sql");
        let sql_content = fs::read_to_string(&sql_path).expect("should read SQL file");
        assert_eq!(
            sql_content, original_sql,
            "SQL content should be preserved exactly"
        );

        // Verify schema is preserved
        let ipc_path = dir.path().join("tables/transfers.ipc");
        let schema_ref = arrow_ipc::read_ipc_schema(&ipc_path).expect("should read IPC schema");
        assert_eq!(schema_ref.fields().len(), 1);
        assert_eq!(schema_ref.field(0).name(), "id");
        assert!(!schema_ref.field(0).is_nullable());
    }
}
