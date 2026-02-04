//! Legacy manifest bridge.
//!
//! Converts the new file-reference based authoring manifest to the legacy inline
//! manifest format expected by existing Amp instances.
//!
//! # Overview
//!
//! The authoring workflow uses file references in manifests:
//! - SQL files at `tables/<table>.sql` (derived tables only)
//! - IPC schema files at `tables/<table>.ipc`
//! - Function source files at `functions/<name>.js`
//!
//! The legacy runtime format requires inline content:
//! - `tables.<name>.input.view.sql` contains the SQL string
//! - `tables.<name>.schema` contains the Arrow schema
//! - `functions.<name>.source` contains `{ source: <code>, filename: <name> }`
//!
//! This module provides the [`LegacyBridge`] type to convert between formats.
//!
//! Note: Raw tables (no SQL definition) are not supported by the legacy bridge.
//!
//! # Example
//!
//! ```ignore
//! use dataset_authoring::bridge::LegacyBridge;
//!
//! let bridge = LegacyBridge::new(project_dir);
//! let legacy_manifest = bridge.convert(&authoring_manifest)?;
//! let json = serde_json::to_string_pretty(&legacy_manifest)?;
//! ```

use std::{collections::BTreeMap, fs, io, path::Path, sync::Arc};

use datasets_common::manifest::{FunctionSource, TableSchema};
use datasets_derived::{
    DerivedDatasetKind,
    deps::{DepReference, HashOrVersion},
    func_name::FuncName,
    manifest::{Function, Manifest, Table, TableInput, View},
};

use crate::{arrow_ipc, manifest::AuthoringManifest};

/// Errors that occur during legacy bridge conversion.
#[derive(Debug, thiserror::Error)]
pub enum BridgeError {
    /// Failed to read SQL file for a table.
    ///
    /// This occurs when the SQL file referenced in the manifest cannot be read
    /// from disk. The file should exist at the path specified in the manifest's
    /// table definition.
    #[error("failed to read SQL file for table '{table_name}' at path '{path}'")]
    ReadSqlFile {
        /// The table name.
        table_name: String,
        /// The path to the SQL file.
        path: String,
        /// The underlying I/O error.
        #[source]
        source: io::Error,
    },

    /// Failed to read IPC schema file for a table.
    ///
    /// This occurs when the IPC schema file referenced in the manifest cannot
    /// be read from disk. The file should exist at the path specified in the
    /// manifest's table definition.
    #[error("failed to read IPC schema file for table '{table_name}' at path '{path}'")]
    ReadIpcSchemaFile {
        /// The table name.
        table_name: String,
        /// The path to the IPC schema file.
        path: String,
        /// The underlying IPC schema error.
        #[source]
        source: arrow_ipc::IpcSchemaError,
    },

    /// Failed to read function source file.
    ///
    /// This occurs when the function source file referenced in the manifest
    /// cannot be read from disk. The file should exist at the path specified
    /// in the manifest's function definition.
    #[error("failed to read function source file for function '{func_name}' at path '{path}'")]
    ReadFunctionFile {
        /// The function name.
        func_name: FuncName,
        /// The path to the function source file.
        path: String,
        /// The underlying I/O error.
        #[source]
        source: io::Error,
    },

    /// Missing SQL file reference for derived table.
    ///
    /// This occurs when a derived table is missing a SQL file reference.
    /// Raw tables (which have no SQL) cannot be converted to the legacy format.
    #[error(
        "table '{table_name}' has no SQL definition (raw tables not supported in legacy bridge)"
    )]
    MissingSql {
        /// The table name.
        table_name: String,
    },

    /// SQL content is invalid.
    ///
    /// This occurs when the SQL content read from the file cannot be parsed
    /// as a valid SQL string for the legacy manifest.
    #[error("invalid SQL content for table '{table_name}'")]
    InvalidSql {
        /// The table name.
        table_name: String,
        /// The underlying parse error.
        #[source]
        source: datasets_derived::sql_str::SqlStrError,
    },
}

/// Bridge for converting authoring manifests to legacy runtime format.
///
/// The legacy bridge reads file content from disk and inlines it into the
/// runtime manifest structure expected by existing Amp instances.
///
/// # Usage
///
/// ```ignore
/// let bridge = LegacyBridge::new("/path/to/project");
/// let legacy = bridge.convert(&authoring_manifest)?;
/// ```
pub struct LegacyBridge<'a> {
    /// Base path for resolving file references.
    base_path: &'a Path,
}

impl<'a> LegacyBridge<'a> {
    /// Creates a new legacy bridge with the specified base path.
    ///
    /// The base path is used to resolve relative file paths in the authoring
    /// manifest (e.g., `tables/transfers.sql` becomes `<base_path>/tables/transfers.sql`).
    pub fn new(base_path: &'a Path) -> Self {
        Self { base_path }
    }

    /// Converts an authoring manifest to the legacy runtime format.
    ///
    /// This method:
    /// 1. Reads SQL files and inlines them into table definitions
    /// 2. Reads schema files and converts them to `TableSchema`
    /// 3. Reads function source files and inlines them
    ///
    /// # Errors
    ///
    /// Returns [`BridgeError`] if any file cannot be read or parsed.
    pub fn convert(&self, manifest: &AuthoringManifest) -> Result<Manifest, BridgeError> {
        let tables = self.convert_tables(manifest)?;
        let functions = self.convert_functions(manifest)?;

        Ok(Manifest {
            kind: DerivedDatasetKind,
            start_block: None,
            dependencies: manifest
                .dependencies
                .iter()
                .map(|(alias, info)| {
                    // Convert DepInfo to DepReference format (namespace/name@hash)
                    let reference = DepReference::new(
                        info.namespace.clone(),
                        info.name.clone(),
                        HashOrVersion::Hash(info.hash.clone()),
                    );
                    (alias.clone(), reference)
                })
                .collect(),
            tables,
            functions,
        })
    }

    /// Converts authoring table definitions to legacy format.
    fn convert_tables(
        &self,
        manifest: &AuthoringManifest,
    ) -> Result<BTreeMap<datasets_common::table_name::TableName, Table>, BridgeError> {
        let mut tables = BTreeMap::new();

        for (table_name, table_def) in &manifest.tables {
            // Read SQL file (required for derived tables)
            let sql_ref = table_def
                .sql
                .as_ref()
                .ok_or_else(|| BridgeError::MissingSql {
                    table_name: table_name.to_string(),
                })?;
            let sql_path = self.base_path.join(&sql_ref.path);
            let sql_content =
                fs::read_to_string(&sql_path).map_err(|err| BridgeError::ReadSqlFile {
                    table_name: table_name.to_string(),
                    path: sql_ref.path.clone(),
                    source: err,
                })?;

            // Parse SQL content
            let sql_str = sql_content.parse().map_err(|err| BridgeError::InvalidSql {
                table_name: table_name.to_string(),
                source: err,
            })?;

            // Read IPC schema file and convert to legacy ArrowSchema format
            let ipc_path = self.base_path.join(&table_def.ipc.path);
            let schema_ref = arrow_ipc::read_ipc_schema(&ipc_path).map_err(|err| {
                BridgeError::ReadIpcSchemaFile {
                    table_name: table_name.to_string(),
                    path: table_def.ipc.path.clone(),
                    source: err,
                }
            })?;
            let arrow_schema = datasets_common::manifest::ArrowSchema::from(&schema_ref);

            let table = Table {
                input: TableInput::View(View { sql: sql_str }),
                schema: TableSchema {
                    arrow: arrow_schema,
                },
                network: table_def.network.clone(),
            };

            tables.insert(table_name.clone(), table);
        }

        Ok(tables)
    }

    /// Converts authoring function definitions to legacy format.
    fn convert_functions(
        &self,
        manifest: &AuthoringManifest,
    ) -> Result<BTreeMap<FuncName, Function>, BridgeError> {
        let mut functions = BTreeMap::new();

        for (func_name, func_def) in &manifest.functions {
            // Read function source file
            let source_path = self.base_path.join(&func_def.source.path);
            let source_content =
                fs::read_to_string(&source_path).map_err(|err| BridgeError::ReadFunctionFile {
                    func_name: func_name.clone(),
                    path: func_def.source.path.clone(),
                    source: err,
                })?;

            // Extract filename from path for the legacy format
            let filename = Path::new(&func_def.source.path)
                .file_name()
                .and_then(|s| s.to_str())
                .unwrap_or(&func_def.source.path)
                .to_string();

            let function = Function {
                input_types: func_def.input_types.clone(),
                output_type: func_def.output_type.clone(),
                source: FunctionSource {
                    source: Arc::from(source_content.as_str()),
                    filename,
                },
            };

            functions.insert(func_name.clone(), function);
        }

        Ok(functions)
    }

    /// Converts an authoring manifest to a JSON string in legacy format.
    ///
    /// This is a convenience method that converts the manifest and serializes
    /// it to pretty-printed JSON suitable for upload to the legacy admin API.
    ///
    /// # Errors
    ///
    /// Returns [`BridgeError`] if conversion fails, or serialization error
    /// wrapped in the result if JSON serialization fails.
    pub fn to_json(&self, manifest: &AuthoringManifest) -> Result<String, ToJsonError> {
        let legacy = self.convert(manifest).map_err(ToJsonError::Bridge)?;
        serde_json::to_string_pretty(&legacy).map_err(ToJsonError::Serialize)
    }
}

/// Errors that occur during JSON conversion.
#[derive(Debug, thiserror::Error)]
pub enum ToJsonError {
    /// Failed to convert manifest to legacy format.
    #[error("failed to convert manifest to legacy format")]
    Bridge(#[source] BridgeError),

    /// Failed to serialize legacy manifest to JSON.
    #[error("failed to serialize legacy manifest to JSON")]
    Serialize(#[source] serde_json::Error),
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, sync::Arc};

    use datafusion::arrow::datatypes::{DataType as ArrowDataType, Field as ArrowField, Schema};
    use datasets_common::{hash::Hash, manifest::DataType};
    use tempfile::TempDir;

    use super::*;
    use crate::{
        arrow_ipc::write_ipc_schema,
        files::FileRef,
        manifest::{AuthoringFunctionDef, AuthoringManifest, DepInfo, TableDef},
    };

    // ============================================================
    // Helper functions
    // ============================================================

    fn create_test_hash() -> Hash {
        "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9"
            .parse()
            .expect("valid hash")
    }

    fn create_minimal_manifest() -> AuthoringManifest {
        AuthoringManifest {
            namespace: "test_ns".parse().expect("valid namespace"),
            name: "test_dataset".parse().expect("valid name"),
            kind: "derived".to_string(),
            dependencies: BTreeMap::new(),
            tables: BTreeMap::new(),
            functions: BTreeMap::new(),
        }
    }

    fn setup_test_files(dir: &TempDir) {
        let tables_dir = dir.path().join("tables");
        fs::create_dir_all(&tables_dir).expect("should create tables dir");

        // Create SQL file
        fs::write(
            tables_dir.join("transfers.sql"),
            "CREATE TABLE transfers AS SELECT * FROM source.transactions",
        )
        .expect("should write sql");

        // Create IPC schema file
        let schema = Arc::new(Schema::new(vec![ArrowField::new(
            "id",
            ArrowDataType::UInt64,
            false,
        )]));
        write_ipc_schema(&schema, tables_dir.join("transfers.ipc"))
            .expect("should write ipc schema");
    }

    fn setup_function_files(dir: &TempDir) {
        let functions_dir = dir.path().join("functions");
        fs::create_dir_all(&functions_dir).expect("should create functions dir");

        fs::write(
            functions_dir.join("decode.js"),
            "function decode(input) { return input.toString(); }",
        )
        .expect("should write function");
    }

    // ============================================================
    // LegacyBridge::new tests
    // ============================================================

    #[test]
    fn legacy_bridge_new_creates_instance() {
        //* Given
        let dir = TempDir::new().expect("should create temp dir");

        //* When
        let bridge = LegacyBridge::new(dir.path());

        //* Then
        assert_eq!(bridge.base_path, dir.path());
    }

    // ============================================================
    // LegacyBridge::convert tests - minimal manifest
    // ============================================================

    #[test]
    fn convert_minimal_manifest_succeeds() {
        //* Given
        let dir = TempDir::new().expect("should create temp dir");
        let manifest = create_minimal_manifest();
        let bridge = LegacyBridge::new(dir.path());

        //* When
        let result = bridge.convert(&manifest);

        //* Then
        let legacy = result.expect("conversion should succeed");
        assert!(legacy.tables.is_empty());
        assert!(legacy.functions.is_empty());
    }

    // ============================================================
    // LegacyBridge::convert tests - tables
    // ============================================================

    #[test]
    fn convert_manifest_with_table_inlines_sql() {
        //* Given
        let dir = TempDir::new().expect("should create temp dir");
        setup_test_files(&dir);

        let mut manifest = create_minimal_manifest();
        manifest.tables.insert(
            "transfers".parse().expect("valid table name"),
            TableDef {
                sql: Some(FileRef::new(
                    "tables/transfers.sql".to_string(),
                    create_test_hash(),
                )),
                ipc: FileRef::new("tables/transfers.ipc".to_string(), create_test_hash()),
                network: "mainnet".parse().expect("valid network"),
            },
        );

        let bridge = LegacyBridge::new(dir.path());

        //* When
        let result = bridge.convert(&manifest);

        //* Then
        let legacy = result.expect("conversion should succeed");
        assert_eq!(legacy.tables.len(), 1);

        let table = legacy
            .tables
            .get(&"transfers".parse().unwrap())
            .expect("should have table");

        match &table.input {
            TableInput::View(view) => {
                assert!(view.sql.as_str().contains("CREATE TABLE transfers"));
            }
        }
    }

    #[test]
    fn convert_manifest_with_table_reads_schema() {
        //* Given
        let dir = TempDir::new().expect("should create temp dir");
        setup_test_files(&dir);

        let mut manifest = create_minimal_manifest();
        manifest.tables.insert(
            "transfers".parse().expect("valid table name"),
            TableDef {
                sql: Some(FileRef::new(
                    "tables/transfers.sql".to_string(),
                    create_test_hash(),
                )),
                ipc: FileRef::new("tables/transfers.ipc".to_string(), create_test_hash()),
                network: "mainnet".parse().expect("valid network"),
            },
        );

        let bridge = LegacyBridge::new(dir.path());

        //* When
        let result = bridge.convert(&manifest);

        //* Then
        let legacy = result.expect("conversion should succeed");
        let table = legacy
            .tables
            .get(&"transfers".parse().unwrap())
            .expect("should have table");

        assert_eq!(table.schema.arrow.fields.len(), 1);
        assert_eq!(table.schema.arrow.fields[0].name, "id");
    }

    #[test]
    fn convert_manifest_preserves_network() {
        //* Given
        let dir = TempDir::new().expect("should create temp dir");
        setup_test_files(&dir);

        let mut manifest = create_minimal_manifest();
        manifest.tables.insert(
            "transfers".parse().expect("valid table name"),
            TableDef {
                sql: Some(FileRef::new(
                    "tables/transfers.sql".to_string(),
                    create_test_hash(),
                )),
                ipc: FileRef::new("tables/transfers.ipc".to_string(), create_test_hash()),
                network: "arbitrum-one".parse().expect("valid network"),
            },
        );

        let bridge = LegacyBridge::new(dir.path());

        //* When
        let result = bridge.convert(&manifest);

        //* Then
        let legacy = result.expect("conversion should succeed");
        let table = legacy
            .tables
            .get(&"transfers".parse().unwrap())
            .expect("should have table");

        assert_eq!(table.network.to_string(), "arbitrum-one");
    }

    #[test]
    fn convert_manifest_fails_on_missing_sql_file() {
        //* Given
        let dir = TempDir::new().expect("should create temp dir");
        // Don't create the sql file

        let mut manifest = create_minimal_manifest();
        manifest.tables.insert(
            "transfers".parse().expect("valid table name"),
            TableDef {
                sql: Some(FileRef::new(
                    "tables/transfers.sql".to_string(),
                    create_test_hash(),
                )),
                ipc: FileRef::new("tables/transfers.ipc".to_string(), create_test_hash()),
                network: "mainnet".parse().expect("valid network"),
            },
        );

        let bridge = LegacyBridge::new(dir.path());

        //* When
        let result = bridge.convert(&manifest);

        //* Then
        assert!(result.is_err());
        let err = result.expect_err("should fail");
        assert!(
            matches!(err, BridgeError::ReadSqlFile { .. }),
            "should be ReadSqlFile error, got: {:?}",
            err
        );
    }

    #[test]
    fn convert_manifest_fails_on_missing_schema_file() {
        //* Given
        let dir = TempDir::new().expect("should create temp dir");
        let tables_dir = dir.path().join("tables");
        fs::create_dir_all(&tables_dir).expect("should create tables dir");
        fs::write(
            tables_dir.join("transfers.sql"),
            "CREATE TABLE transfers AS SELECT 1",
        )
        .expect("should write sql");
        // Don't create the IPC schema file

        let mut manifest = create_minimal_manifest();
        manifest.tables.insert(
            "transfers".parse().expect("valid table name"),
            TableDef {
                sql: Some(FileRef::new(
                    "tables/transfers.sql".to_string(),
                    create_test_hash(),
                )),
                ipc: FileRef::new("tables/transfers.ipc".to_string(), create_test_hash()),
                network: "mainnet".parse().expect("valid network"),
            },
        );

        let bridge = LegacyBridge::new(dir.path());

        //* When
        let result = bridge.convert(&manifest);

        //* Then
        assert!(result.is_err());
        let err = result.expect_err("should fail");
        assert!(
            matches!(err, BridgeError::ReadIpcSchemaFile { .. }),
            "should be ReadIpcSchemaFile error, got: {:?}",
            err
        );
    }

    // ============================================================
    // LegacyBridge::convert tests - functions
    // ============================================================

    #[test]
    fn convert_manifest_with_function_inlines_source() {
        //* Given
        let dir = TempDir::new().expect("should create temp dir");
        setup_function_files(&dir);

        let mut manifest = create_minimal_manifest();
        manifest.functions.insert(
            "decode".parse().expect("valid func name"),
            AuthoringFunctionDef {
                input_types: vec![DataType(datafusion::arrow::datatypes::DataType::Binary)],
                output_type: DataType(datafusion::arrow::datatypes::DataType::Utf8),
                source: FileRef::new("functions/decode.js".to_string(), create_test_hash()),
            },
        );

        let bridge = LegacyBridge::new(dir.path());

        //* When
        let result = bridge.convert(&manifest);

        //* Then
        let legacy = result.expect("conversion should succeed");
        assert_eq!(legacy.functions.len(), 1);

        let func = legacy
            .functions
            .get(&"decode".parse().unwrap())
            .expect("should have function");

        assert!(func.source.source.contains("function decode"));
        assert_eq!(func.source.filename, "decode.js");
    }

    #[test]
    fn convert_manifest_preserves_function_types() {
        //* Given
        let dir = TempDir::new().expect("should create temp dir");
        setup_function_files(&dir);

        let mut manifest = create_minimal_manifest();
        manifest.functions.insert(
            "decode".parse().expect("valid func name"),
            AuthoringFunctionDef {
                input_types: vec![
                    DataType(datafusion::arrow::datatypes::DataType::Binary),
                    DataType(datafusion::arrow::datatypes::DataType::Int64),
                ],
                output_type: DataType(datafusion::arrow::datatypes::DataType::Utf8),
                source: FileRef::new("functions/decode.js".to_string(), create_test_hash()),
            },
        );

        let bridge = LegacyBridge::new(dir.path());

        //* When
        let result = bridge.convert(&manifest);

        //* Then
        let legacy = result.expect("conversion should succeed");
        let func = legacy
            .functions
            .get(&"decode".parse().unwrap())
            .expect("should have function");

        assert_eq!(func.input_types.len(), 2);
        assert_eq!(
            *func.input_types[0].as_arrow(),
            datafusion::arrow::datatypes::DataType::Binary
        );
        assert_eq!(
            *func.input_types[1].as_arrow(),
            datafusion::arrow::datatypes::DataType::Int64
        );
        assert_eq!(
            *func.output_type.as_arrow(),
            datafusion::arrow::datatypes::DataType::Utf8
        );
    }

    #[test]
    fn convert_manifest_fails_on_missing_function_file() {
        //* Given
        let dir = TempDir::new().expect("should create temp dir");
        // Don't create the function file

        let mut manifest = create_minimal_manifest();
        manifest.functions.insert(
            "decode".parse().expect("valid func name"),
            AuthoringFunctionDef {
                input_types: vec![],
                output_type: DataType(datafusion::arrow::datatypes::DataType::Utf8),
                source: FileRef::new("functions/decode.js".to_string(), create_test_hash()),
            },
        );

        let bridge = LegacyBridge::new(dir.path());

        //* When
        let result = bridge.convert(&manifest);

        //* Then
        assert!(result.is_err());
        let err = result.expect_err("should fail");
        assert!(
            matches!(err, BridgeError::ReadFunctionFile { .. }),
            "should be ReadFunctionFile error, got: {:?}",
            err
        );
    }

    // ============================================================
    // LegacyBridge::to_json tests
    // ============================================================

    #[test]
    fn to_json_produces_valid_json() {
        //* Given
        let dir = TempDir::new().expect("should create temp dir");
        setup_test_files(&dir);

        let mut manifest = create_minimal_manifest();
        manifest.tables.insert(
            "transfers".parse().expect("valid table name"),
            TableDef {
                sql: Some(FileRef::new(
                    "tables/transfers.sql".to_string(),
                    create_test_hash(),
                )),
                ipc: FileRef::new("tables/transfers.ipc".to_string(), create_test_hash()),
                network: "mainnet".parse().expect("valid network"),
            },
        );

        let bridge = LegacyBridge::new(dir.path());

        //* When
        let result = bridge.to_json(&manifest);

        //* Then
        let json = result.expect("to_json should succeed");

        // Verify it's valid JSON by parsing it
        let parsed: serde_json::Value = serde_json::from_str(&json).expect("should be valid JSON");

        // Verify structure
        assert_eq!(parsed["kind"], "manifest");
        assert!(parsed["tables"]["transfers"].is_object());
        assert!(parsed["tables"]["transfers"]["input"]["sql"].is_string());
    }

    #[test]
    fn to_json_includes_inline_sql() {
        //* Given
        let dir = TempDir::new().expect("should create temp dir");
        setup_test_files(&dir);

        let mut manifest = create_minimal_manifest();
        manifest.tables.insert(
            "transfers".parse().expect("valid table name"),
            TableDef {
                sql: Some(FileRef::new(
                    "tables/transfers.sql".to_string(),
                    create_test_hash(),
                )),
                ipc: FileRef::new("tables/transfers.ipc".to_string(), create_test_hash()),
                network: "mainnet".parse().expect("valid network"),
            },
        );

        let bridge = LegacyBridge::new(dir.path());

        //* When
        let result = bridge.to_json(&manifest);

        //* Then
        let json = result.expect("to_json should succeed");
        assert!(
            json.contains("CREATE TABLE transfers"),
            "JSON should contain inline SQL"
        );
    }

    // ============================================================
    // Dependencies conversion tests
    // ============================================================

    #[test]
    fn convert_manifest_with_dependencies() {
        //* Given
        let dir = TempDir::new().expect("should create temp dir");

        let mut manifest = create_minimal_manifest();
        manifest.dependencies.insert(
            "eth".parse().expect("valid alias"),
            DepInfo {
                hash: create_test_hash(),
                namespace: "dep_ns".parse().expect("valid namespace"),
                name: "eth_mainnet".parse().expect("valid name"),
            },
        );

        let bridge = LegacyBridge::new(dir.path());

        //* When
        let result = bridge.convert(&manifest);

        //* Then
        let legacy = result.expect("conversion should succeed");
        assert_eq!(legacy.dependencies.len(), 1);
        assert!(legacy.dependencies.contains_key(&"eth".parse().unwrap()));

        // Verify the DepReference has the correct format
        let dep_ref = legacy.dependencies.get(&"eth".parse().unwrap()).unwrap();
        assert_eq!(dep_ref.fqn().namespace().to_string(), "dep_ns");
        assert_eq!(dep_ref.fqn().name().to_string(), "eth_mainnet");
    }

    // ============================================================
    // derived.spec.json schema compliance tests
    // ============================================================

    #[test]
    fn legacy_output_has_required_kind_field() {
        //* Given
        // As per derived.spec.json: "kind" is required and must be "manifest"
        let dir = TempDir::new().expect("should create temp dir");
        let manifest = create_minimal_manifest();
        let bridge = LegacyBridge::new(dir.path());

        //* When
        let result = bridge.to_json(&manifest);

        //* Then
        let json = result.expect("to_json should succeed");
        let parsed: serde_json::Value = serde_json::from_str(&json).expect("valid JSON");
        assert_eq!(
            parsed["kind"], "manifest",
            "legacy output must have kind='manifest' per derived.spec.json"
        );
    }

    #[test]
    fn legacy_output_table_has_required_fields() {
        //* Given
        // As per derived.spec.json Table definition:
        // - "input" (required): TableInput containing View with "sql"
        // - "schema" (required): TableSchema containing "arrow" with "fields"
        // - "network" (required): NetworkId string
        let dir = TempDir::new().expect("should create temp dir");
        setup_test_files(&dir);

        let mut manifest = create_minimal_manifest();
        manifest.tables.insert(
            "transfers".parse().expect("valid table name"),
            TableDef {
                sql: Some(FileRef::new(
                    "tables/transfers.sql".to_string(),
                    create_test_hash(),
                )),
                ipc: FileRef::new("tables/transfers.ipc".to_string(), create_test_hash()),
                network: "mainnet".parse().expect("valid network"),
            },
        );

        let bridge = LegacyBridge::new(dir.path());

        //* When
        let result = bridge.to_json(&manifest);

        //* Then
        let json = result.expect("to_json should succeed");
        let parsed: serde_json::Value = serde_json::from_str(&json).expect("valid JSON");

        let table = &parsed["tables"]["transfers"];

        // Validate Table required fields per spec
        assert!(
            table["input"].is_object(),
            "table must have 'input' object per derived.spec.json"
        );
        assert!(
            table["input"]["sql"].is_string(),
            "table.input must have 'sql' string (View format) per derived.spec.json"
        );
        assert!(
            table["schema"].is_object(),
            "table must have 'schema' object per derived.spec.json"
        );
        assert!(
            table["schema"]["arrow"].is_object(),
            "table.schema must have 'arrow' object per derived.spec.json"
        );
        assert!(
            table["schema"]["arrow"]["fields"].is_array(),
            "table.schema.arrow must have 'fields' array per derived.spec.json"
        );
        assert!(
            table["network"].is_string(),
            "table must have 'network' string per derived.spec.json"
        );
    }

    #[test]
    fn legacy_output_schema_field_has_required_properties() {
        //* Given
        // As per derived.spec.json Field definition:
        // - "name" (required): string
        // - "type" (required): DataType string
        // - "nullable" (required): boolean
        let dir = TempDir::new().expect("should create temp dir");
        setup_test_files(&dir);

        let mut manifest = create_minimal_manifest();
        manifest.tables.insert(
            "transfers".parse().expect("valid table name"),
            TableDef {
                sql: Some(FileRef::new(
                    "tables/transfers.sql".to_string(),
                    create_test_hash(),
                )),
                ipc: FileRef::new("tables/transfers.ipc".to_string(), create_test_hash()),
                network: "mainnet".parse().expect("valid network"),
            },
        );

        let bridge = LegacyBridge::new(dir.path());

        //* When
        let result = bridge.to_json(&manifest);

        //* Then
        let json = result.expect("to_json should succeed");
        let parsed: serde_json::Value = serde_json::from_str(&json).expect("valid JSON");

        let fields = &parsed["tables"]["transfers"]["schema"]["arrow"]["fields"];
        assert!(fields.is_array(), "fields must be an array");

        for field in fields.as_array().expect("fields is array") {
            assert!(
                field["name"].is_string(),
                "field must have 'name' string per derived.spec.json"
            );
            assert!(
                field["type"].is_string(),
                "field must have 'type' string per derived.spec.json"
            );
            assert!(
                field["nullable"].is_boolean(),
                "field must have 'nullable' boolean per derived.spec.json"
            );
        }
    }

    #[test]
    fn legacy_output_function_has_required_fields() {
        //* Given
        // As per derived.spec.json Function definition:
        // - "inputTypes" (required): array of DataType strings
        // - "outputType" (required): DataType string
        // - "source" (required): FunctionSource with "source" and "filename"
        let dir = TempDir::new().expect("should create temp dir");
        setup_function_files(&dir);

        let mut manifest = create_minimal_manifest();
        manifest.functions.insert(
            "decode".parse().expect("valid func name"),
            AuthoringFunctionDef {
                input_types: vec![DataType(datafusion::arrow::datatypes::DataType::Binary)],
                output_type: DataType(datafusion::arrow::datatypes::DataType::Utf8),
                source: FileRef::new("functions/decode.js".to_string(), create_test_hash()),
            },
        );

        let bridge = LegacyBridge::new(dir.path());

        //* When
        let result = bridge.to_json(&manifest);

        //* Then
        let json = result.expect("to_json should succeed");
        let parsed: serde_json::Value = serde_json::from_str(&json).expect("valid JSON");

        let func = &parsed["functions"]["decode"];

        // Validate Function required fields per spec
        assert!(
            func["inputTypes"].is_array(),
            "function must have 'inputTypes' array per derived.spec.json"
        );
        assert!(
            func["outputType"].is_string(),
            "function must have 'outputType' string per derived.spec.json"
        );
        assert!(
            func["source"].is_object(),
            "function must have 'source' object per derived.spec.json"
        );
        assert!(
            func["source"]["source"].is_string(),
            "function.source must have 'source' string per derived.spec.json"
        );
        assert!(
            func["source"]["filename"].is_string(),
            "function.source must have 'filename' string per derived.spec.json"
        );
    }

    #[test]
    fn legacy_output_dependency_has_correct_format() {
        //* Given
        // As per derived.spec.json DepReference:
        // Format: `namespace/name@version` or `namespace/name@hash`
        // Pattern: ^[a-z0-9_]+/[a-z_][a-z0-9_]*@.+$
        let dir = TempDir::new().expect("should create temp dir");

        let mut manifest = create_minimal_manifest();
        manifest.dependencies.insert(
            "eth".parse().expect("valid alias"),
            DepInfo {
                hash: create_test_hash(),
                namespace: "dep_ns".parse().expect("valid namespace"),
                name: "eth_mainnet".parse().expect("valid name"),
            },
        );

        let bridge = LegacyBridge::new(dir.path());

        //* When
        let result = bridge.to_json(&manifest);

        //* Then
        let json = result.expect("to_json should succeed");
        let parsed: serde_json::Value = serde_json::from_str(&json).expect("valid JSON");

        let dep_value = &parsed["dependencies"]["eth"];
        assert!(
            dep_value.is_string(),
            "dependency must be a string (DepReference format)"
        );

        let dep_str = dep_value.as_str().expect("dependency is string");
        // Validate DepReference pattern: namespace/name@hash_or_version
        let pattern = regex::Regex::new(r"^[a-z0-9_]+/[a-z_][a-z0-9_]*@.+$").expect("valid regex");
        assert!(
            pattern.is_match(dep_str),
            "dependency reference '{}' must match pattern '^[a-z0-9_]+/[a-z_][a-z0-9_]*@.+$' per derived.spec.json",
            dep_str
        );
    }

    #[test]
    fn legacy_output_complete_manifest_structure() {
        //* Given
        // Complete manifest with tables, functions, and dependencies
        let dir = TempDir::new().expect("should create temp dir");
        setup_test_files(&dir);
        setup_function_files(&dir);

        let mut manifest = create_minimal_manifest();
        manifest.tables.insert(
            "transfers".parse().expect("valid table name"),
            TableDef {
                sql: Some(FileRef::new(
                    "tables/transfers.sql".to_string(),
                    create_test_hash(),
                )),
                ipc: FileRef::new("tables/transfers.ipc".to_string(), create_test_hash()),
                network: "mainnet".parse().expect("valid network"),
            },
        );
        manifest.functions.insert(
            "decode".parse().expect("valid func name"),
            AuthoringFunctionDef {
                input_types: vec![DataType(datafusion::arrow::datatypes::DataType::Binary)],
                output_type: DataType(datafusion::arrow::datatypes::DataType::Utf8),
                source: FileRef::new("functions/decode.js".to_string(), create_test_hash()),
            },
        );
        manifest.dependencies.insert(
            "eth".parse().expect("valid alias"),
            DepInfo {
                hash: create_test_hash(),
                namespace: "dep_ns".parse().expect("valid namespace"),
                name: "eth_mainnet".parse().expect("valid name"),
            },
        );

        let bridge = LegacyBridge::new(dir.path());

        //* When
        let result = bridge.to_json(&manifest);

        //* Then
        let json = result.expect("to_json should succeed");
        let parsed: serde_json::Value = serde_json::from_str(&json).expect("valid JSON");

        // Verify top-level structure per derived.spec.json
        assert_eq!(parsed["kind"], "manifest", "kind must be 'manifest'");
        assert!(
            parsed["tables"].is_object(),
            "tables must be present as object"
        );
        assert!(
            parsed["functions"].is_object(),
            "functions must be present as object"
        );
        assert!(
            parsed["dependencies"].is_object(),
            "dependencies must be present as object"
        );

        assert!(
            parsed["start_block"].is_null(),
            "start_block should be null"
        );
    }
}
