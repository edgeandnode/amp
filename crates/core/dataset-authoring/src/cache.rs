//! Global dependency cache.
//!
//! Provides caching for resolved manifests at `~/.amp/registry/<hash>/`.
//!
//! The cache stores dependency packages locally to avoid repeated network
//! fetches during builds. Each package is stored in a directory named by
//! its content hash, containing:
//!
//! - `manifest.json` - Canonical manifest with file references
//! - `tables/<table>.sql` - SQL files for derived tables
//! - `tables/<table>.ipc` - Arrow IPC schema files
//! - `functions/<name>.js` - Function source files

use std::path::{Path, PathBuf};

use datafusion::arrow::datatypes::SchemaRef;
use datasets_common::{hash::Hash, table_name::TableName};

use crate::{arrow_ipc, dependency_manifest::DependencyManifest};

/// Default cache location relative to home directory.
const CACHE_DIR: &str = ".amp/registry";

/// Global cache for resolved dependency manifests.
///
/// Stores manifests at `~/.amp/registry/<hash>/manifest.json`.
/// The cache is content-addressed by manifest hash for deduplication.
#[derive(Debug, Clone)]
pub struct Cache {
    /// Root directory for the cache.
    root: PathBuf,
}

/// Errors that occur during cache operations.
#[derive(Debug, thiserror::Error)]
pub enum CacheError {
    /// Failed to determine home directory
    ///
    /// This occurs when the system cannot determine the user's home directory,
    /// typically due to missing environment variables or unusual system configuration.
    #[error("failed to determine home directory")]
    NoHomeDir,

    /// Failed to create cache directory
    ///
    /// This occurs when the cache directory cannot be created, typically due to
    /// permission issues or disk space constraints.
    #[error("failed to create cache directory '{path}'")]
    CreateDir {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },

    /// Failed to read manifest from cache
    ///
    /// This occurs when reading an existing cached manifest fails, typically due to
    /// file permissions or concurrent modification.
    #[error("failed to read cached manifest for hash '{hash}'")]
    ReadManifest {
        hash: Hash,
        #[source]
        source: std::io::Error,
    },

    /// Failed to parse cached manifest JSON
    ///
    /// This occurs when the cached manifest file exists but contains invalid JSON
    /// or does not match the expected manifest schema.
    #[error("failed to parse cached manifest for hash '{hash}'")]
    ParseManifest {
        hash: Hash,
        #[source]
        source: serde_json::Error,
    },

    /// Failed to write manifest to cache
    ///
    /// This occurs when writing a manifest to the cache fails, typically due to
    /// disk space constraints or permission issues.
    #[error("failed to write manifest to cache for hash '{hash}'")]
    WriteManifest {
        hash: Hash,
        #[source]
        source: std::io::Error,
    },

    /// Failed to serialize manifest to JSON
    ///
    /// This occurs when converting the manifest to JSON fails, which indicates
    /// an internal error in the manifest structure.
    #[error("failed to serialize manifest for hash '{hash}'")]
    SerializeManifest {
        hash: Hash,
        #[source]
        source: serde_json::Error,
    },

    /// Failed to read SQL file from cached package.
    #[error("failed to read SQL file for table '{table_name}'")]
    ReadSqlFile {
        table_name: TableName,
        #[source]
        source: std::io::Error,
    },

    /// Failed to read IPC schema file from cached package.
    #[error("failed to read IPC schema file for table '{table_name}'")]
    ReadIpcSchema {
        table_name: TableName,
        #[source]
        source: arrow_ipc::IpcSchemaError,
    },

    /// Failed to read function source file from cached package.
    #[error("failed to read function source file '{filename}'")]
    ReadFunctionFile {
        filename: String,
        #[source]
        source: std::io::Error,
    },
}

impl Cache {
    /// Creates a new cache with the default location.
    ///
    /// The default location is `~/.amp/registry/`.
    ///
    /// # Errors
    ///
    /// Returns [`CacheError::NoHomeDir`] if the home directory cannot be determined.
    pub fn new() -> Result<Self, CacheError> {
        let home = dirs::home_dir().ok_or(CacheError::NoHomeDir)?;
        Ok(Self {
            root: home.join(CACHE_DIR),
        })
    }

    /// Creates a cache at a custom location.
    ///
    /// Useful for testing or when a non-standard cache location is required.
    pub fn with_root(root: PathBuf) -> Self {
        Self { root }
    }

    /// Returns the root directory of the cache.
    pub fn root(&self) -> &PathBuf {
        &self.root
    }

    /// Returns the directory path for a given manifest hash.
    ///
    /// The path is `<root>/<hash>/` where hash is the full 64-character hex string.
    pub fn dir_path(&self, hash: &Hash) -> PathBuf {
        self.root.join(hash.as_str())
    }

    /// Returns the path to a manifest file for a given hash.
    ///
    /// The path is `<root>/<hash>/manifest.json`.
    pub fn manifest_path(&self, hash: &Hash) -> PathBuf {
        self.dir_path(hash).join("manifest.json")
    }

    /// Checks if a manifest exists in the cache.
    pub fn contains(&self, hash: &Hash) -> bool {
        self.manifest_path(hash).exists()
    }

    /// Gets a manifest from the cache if it exists.
    ///
    /// # Errors
    ///
    /// Returns an error if the manifest exists but cannot be read or parsed.
    pub fn get(&self, hash: &Hash) -> Result<Option<DependencyManifest>, CacheError> {
        let path = self.manifest_path(hash);
        if !path.exists() {
            return Ok(None);
        }

        let contents = std::fs::read_to_string(&path).map_err(|err| CacheError::ReadManifest {
            hash: hash.clone(),
            source: err,
        })?;

        let manifest =
            serde_json::from_str(&contents).map_err(|err| CacheError::ParseManifest {
                hash: hash.clone(),
                source: err,
            })?;

        Ok(Some(manifest))
    }

    /// Stores a manifest in the cache.
    ///
    /// Creates the cache directory if it doesn't exist.
    ///
    /// # Errors
    ///
    /// Returns an error if the directory cannot be created or the manifest
    /// cannot be serialized or written.
    pub fn put(&self, hash: &Hash, manifest: &DependencyManifest) -> Result<(), CacheError> {
        let dir_path = self.dir_path(hash);

        // Create directory if needed
        if !dir_path.exists() {
            std::fs::create_dir_all(&dir_path).map_err(|err| CacheError::CreateDir {
                path: dir_path.clone(),
                source: err,
            })?;
        }

        // Serialize manifest
        let contents = serde_json::to_string_pretty(manifest).map_err(|err| {
            CacheError::SerializeManifest {
                hash: hash.clone(),
                source: err,
            }
        })?;

        // Write to file
        let manifest_path = self.manifest_path(hash);
        std::fs::write(&manifest_path, contents).map_err(|err| CacheError::WriteManifest {
            hash: hash.clone(),
            source: err,
        })?;

        Ok(())
    }

    /// Gets a cached package by hash, returning access to all package files.
    ///
    /// Unlike [`get`](Self::get) which returns only the manifest, this method
    /// returns a [`CachedPackage`] that provides access to the full package
    /// structure including SQL files, IPC schemas, and function sources.
    ///
    /// # Errors
    ///
    /// Returns an error if the package exists but cannot be read or parsed.
    pub fn get_package(&self, hash: &Hash) -> Result<Option<CachedPackage>, CacheError> {
        let dir_path = self.dir_path(hash);
        let manifest_path = self.manifest_path(hash);

        if !manifest_path.exists() {
            return Ok(None);
        }

        let contents =
            std::fs::read_to_string(&manifest_path).map_err(|err| CacheError::ReadManifest {
                hash: hash.clone(),
                source: err,
            })?;

        let manifest =
            serde_json::from_str(&contents).map_err(|err| CacheError::ParseManifest {
                hash: hash.clone(),
                source: err,
            })?;

        Ok(Some(CachedPackage {
            root: dir_path,
            manifest,
        }))
    }
}

/// A cached package providing access to all package files.
///
/// This represents a complete package stored in the cache, including:
/// - The dependency manifest (parsed from `manifest.json`)
/// - SQL files in `tables/<table>.sql`
/// - Arrow IPC schema files in `tables/<table>.ipc`
/// - Function sources in `functions/<name>.js`
///
/// Use this when you need to access the actual file contents beyond just
/// the manifest metadata.
#[derive(Debug, Clone)]
pub struct CachedPackage {
    /// Root directory of the cached package.
    root: PathBuf,
    /// Parsed manifest from manifest.json.
    manifest: DependencyManifest,
}

impl CachedPackage {
    /// Returns the root directory of the cached package.
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Returns the dependency manifest.
    pub fn manifest(&self) -> &DependencyManifest {
        &self.manifest
    }

    /// Consumes the cached package and returns the manifest.
    pub fn into_manifest(self) -> DependencyManifest {
        self.manifest
    }

    /// Returns the path to the tables directory.
    pub fn tables_dir(&self) -> PathBuf {
        self.root.join("tables")
    }

    /// Returns the path to the functions directory.
    pub fn functions_dir(&self) -> PathBuf {
        self.root.join("functions")
    }

    /// Returns the path to a table's SQL file.
    pub fn sql_path(&self, table_name: &TableName) -> PathBuf {
        self.tables_dir().join(format!("{table_name}.sql"))
    }

    /// Returns the path to a table's IPC schema file.
    pub fn ipc_path(&self, table_name: &TableName) -> PathBuf {
        self.tables_dir().join(format!("{table_name}.ipc"))
    }

    /// Returns the path to a function's source file.
    pub fn function_path(&self, filename: &str) -> PathBuf {
        self.functions_dir().join(filename)
    }

    /// Reads the SQL content for a table.
    ///
    /// # Errors
    ///
    /// Returns [`CacheError::ReadSqlFile`] if the file cannot be read.
    pub fn read_sql(&self, table_name: &TableName) -> Result<String, CacheError> {
        let path = self.sql_path(table_name);
        std::fs::read_to_string(&path).map_err(|err| CacheError::ReadSqlFile {
            table_name: table_name.clone(),
            source: err,
        })
    }

    /// Reads the Arrow schema from a table's IPC file.
    ///
    /// # Errors
    ///
    /// Returns [`CacheError::ReadIpcSchema`] if the file cannot be read or parsed.
    pub fn read_schema(&self, table_name: &TableName) -> Result<SchemaRef, CacheError> {
        let path = self.ipc_path(table_name);
        arrow_ipc::read_ipc_schema(&path).map_err(|err| CacheError::ReadIpcSchema {
            table_name: table_name.clone(),
            source: err,
        })
    }

    /// Reads the source content for a function.
    ///
    /// # Errors
    ///
    /// Returns [`CacheError::ReadFunctionFile`] if the file cannot be read.
    pub fn read_function(&self, filename: &str) -> Result<String, CacheError> {
        let path = self.function_path(filename);
        std::fs::read_to_string(&path).map_err(|err| CacheError::ReadFunctionFile {
            filename: filename.to_string(),
            source: err,
        })
    }

    /// Checks if a table has an IPC schema file.
    pub fn has_ipc_schema(&self, table_name: &TableName) -> bool {
        self.ipc_path(table_name).exists()
    }

    /// Checks if a table has a SQL file.
    pub fn has_sql(&self, table_name: &TableName) -> bool {
        self.sql_path(table_name).exists()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use datasets_common::dataset_kind_str::DatasetKindStr;

    use super::*;

    fn create_test_manifest() -> DependencyManifest {
        DependencyManifest {
            kind: DatasetKindStr::new("manifest".to_string()),
            dependencies: BTreeMap::new(),
            tables: BTreeMap::new(),
            functions: BTreeMap::new(),
        }
    }

    #[test]
    fn cache_with_root_creates_cache_at_custom_path() {
        //* Given
        let root = PathBuf::from("/tmp/test-cache");

        //* When
        let cache = Cache::with_root(root.clone());

        //* Then
        assert_eq!(cache.root(), &root);
    }

    #[test]
    fn dir_path_returns_hash_directory() {
        //* Given
        let cache = Cache::with_root(PathBuf::from("/cache"));
        let hash: Hash = "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9"
            .parse()
            .expect("valid hash");

        //* When
        let path = cache.dir_path(&hash);

        //* Then
        assert_eq!(
            path,
            PathBuf::from(
                "/cache/b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9"
            )
        );
    }

    #[test]
    fn manifest_path_returns_manifest_file_path() {
        //* Given
        let cache = Cache::with_root(PathBuf::from("/cache"));
        let hash: Hash = "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9"
            .parse()
            .expect("valid hash");

        //* When
        let path = cache.manifest_path(&hash);

        //* Then
        assert_eq!(
            path,
            PathBuf::from(
                "/cache/b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9/manifest.json"
            )
        );
    }

    #[test]
    fn contains_returns_false_for_nonexistent_hash() {
        //* Given
        let temp_dir = tempfile::tempdir().expect("should create temp dir");
        let cache = Cache::with_root(temp_dir.path().to_path_buf());
        let hash: Hash = "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9"
            .parse()
            .expect("valid hash");

        //* When
        let result = cache.contains(&hash);

        //* Then
        assert!(!result, "cache should not contain nonexistent hash");
    }

    #[test]
    fn get_returns_none_for_nonexistent_hash() {
        //* Given
        let temp_dir = tempfile::tempdir().expect("should create temp dir");
        let cache = Cache::with_root(temp_dir.path().to_path_buf());
        let hash: Hash = "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9"
            .parse()
            .expect("valid hash");

        //* When
        let result = cache.get(&hash);

        //* Then
        assert!(result.is_ok(), "get should succeed");
        assert!(
            result.expect("should be ok").is_none(),
            "should return None for nonexistent hash"
        );
    }

    #[test]
    fn put_and_get_roundtrip_succeeds() {
        //* Given
        let temp_dir = tempfile::tempdir().expect("should create temp dir");
        let cache = Cache::with_root(temp_dir.path().to_path_buf());
        let hash: Hash = "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9"
            .parse()
            .expect("valid hash");
        let manifest = create_test_manifest();

        //* When
        let put_result = cache.put(&hash, &manifest);

        //* Then
        assert!(put_result.is_ok(), "put should succeed");
        assert!(cache.contains(&hash), "cache should contain hash after put");

        let get_result = cache.get(&hash);
        assert!(get_result.is_ok(), "get should succeed");
        let retrieved = get_result
            .expect("should be ok")
            .expect("should have manifest");

        // Verify manifest fields match
        assert_eq!(retrieved.kind.as_str(), "manifest");
        assert!(retrieved.dependencies.is_empty());
        assert!(retrieved.tables.is_empty());
    }

    #[test]
    fn put_creates_directory_if_needed() {
        //* Given
        let temp_dir = tempfile::tempdir().expect("should create temp dir");
        let cache = Cache::with_root(temp_dir.path().to_path_buf());
        let hash: Hash = "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9"
            .parse()
            .expect("valid hash");
        let manifest = create_test_manifest();

        // Verify directory doesn't exist
        assert!(!cache.dir_path(&hash).exists());

        //* When
        let result = cache.put(&hash, &manifest);

        //* Then
        assert!(result.is_ok(), "put should succeed");
        assert!(
            cache.dir_path(&hash).exists(),
            "directory should be created"
        );
        assert!(
            cache.manifest_path(&hash).exists(),
            "manifest file should exist"
        );
    }

    #[test]
    fn get_with_corrupt_json_returns_parse_error() {
        //* Given
        let temp_dir = tempfile::tempdir().expect("should create temp dir");
        let cache = Cache::with_root(temp_dir.path().to_path_buf());
        let hash: Hash = "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9"
            .parse()
            .expect("valid hash");

        // Create directory and write invalid JSON
        let dir_path = cache.dir_path(&hash);
        std::fs::create_dir_all(&dir_path).expect("should create dir");
        std::fs::write(cache.manifest_path(&hash), "not valid json").expect("should write");

        //* When
        let result = cache.get(&hash);

        //* Then
        assert!(result.is_err(), "get should fail with corrupt JSON");
        let err = result.expect_err("should have error");
        assert!(
            matches!(err, CacheError::ParseManifest { .. }),
            "should be ParseManifest error"
        );
    }

    // ============================================================
    // CachedPackage tests
    // ============================================================

    #[test]
    fn get_package_returns_none_for_nonexistent_hash() {
        //* Given
        let temp_dir = tempfile::tempdir().expect("should create temp dir");
        let cache = Cache::with_root(temp_dir.path().to_path_buf());
        let hash: Hash = "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9"
            .parse()
            .expect("valid hash");

        //* When
        let result = cache.get_package(&hash);

        //* Then
        assert!(result.is_ok(), "get_package should succeed");
        assert!(
            result.expect("should be ok").is_none(),
            "should return None for nonexistent hash"
        );
    }

    #[test]
    fn get_package_returns_cached_package() {
        //* Given
        let temp_dir = tempfile::tempdir().expect("should create temp dir");
        let cache = Cache::with_root(temp_dir.path().to_path_buf());
        let hash: Hash = "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9"
            .parse()
            .expect("valid hash");
        let manifest = create_test_manifest();
        cache.put(&hash, &manifest).expect("put should succeed");

        //* When
        let result = cache.get_package(&hash);

        //* Then
        assert!(result.is_ok(), "get_package should succeed");
        let package = result.expect("should be ok").expect("should have package");
        assert_eq!(package.manifest().kind.as_str(), "manifest");
        assert_eq!(package.root(), cache.dir_path(&hash));
    }

    #[test]
    fn cached_package_path_methods_return_correct_paths() {
        //* Given
        let temp_dir = tempfile::tempdir().expect("should create temp dir");
        let cache = Cache::with_root(temp_dir.path().to_path_buf());
        let hash: Hash = "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9"
            .parse()
            .expect("valid hash");
        let manifest = create_test_manifest();
        cache.put(&hash, &manifest).expect("put should succeed");

        let package = cache
            .get_package(&hash)
            .expect("should succeed")
            .expect("should have package");

        //* When/Then
        let table_name: TableName = "transfers".parse().expect("valid table name");
        assert!(
            package
                .sql_path(&table_name)
                .ends_with("tables/transfers.sql")
        );
        assert!(
            package
                .ipc_path(&table_name)
                .ends_with("tables/transfers.ipc")
        );
        assert!(
            package
                .function_path("decode.js")
                .ends_with("functions/decode.js")
        );
    }

    #[test]
    fn cached_package_read_sql_succeeds() {
        //* Given
        let temp_dir = tempfile::tempdir().expect("should create temp dir");
        let cache = Cache::with_root(temp_dir.path().to_path_buf());
        let hash: Hash = "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9"
            .parse()
            .expect("valid hash");
        let manifest = create_test_manifest();
        cache.put(&hash, &manifest).expect("put should succeed");

        // Create SQL file in cache
        let tables_dir = cache.dir_path(&hash).join("tables");
        std::fs::create_dir_all(&tables_dir).expect("should create dir");
        std::fs::write(tables_dir.join("test_table.sql"), "SELECT * FROM source")
            .expect("should write");

        let package = cache
            .get_package(&hash)
            .expect("should succeed")
            .expect("should have package");

        //* When
        let table_name: TableName = "test_table".parse().expect("valid table name");
        let result = package.read_sql(&table_name);

        //* Then
        assert!(result.is_ok(), "read_sql should succeed");
        assert_eq!(result.expect("should have content"), "SELECT * FROM source");
    }

    #[test]
    fn cached_package_read_schema_succeeds() {
        use std::sync::Arc;

        use datafusion::arrow::datatypes::{DataType, Field, Schema};

        //* Given
        let temp_dir = tempfile::tempdir().expect("should create temp dir");
        let cache = Cache::with_root(temp_dir.path().to_path_buf());
        let hash: Hash = "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9"
            .parse()
            .expect("valid hash");
        let manifest = create_test_manifest();
        cache.put(&hash, &manifest).expect("put should succeed");

        // Create IPC schema file in cache
        let tables_dir = cache.dir_path(&hash).join("tables");
        std::fs::create_dir_all(&tables_dir).expect("should create dir");

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::UInt64, false),
            Field::new("name", DataType::Utf8, true),
        ]));
        arrow_ipc::write_ipc_schema(&schema, tables_dir.join("test_table.ipc"))
            .expect("should write");

        let package = cache
            .get_package(&hash)
            .expect("should succeed")
            .expect("should have package");

        //* When
        let table_name: TableName = "test_table".parse().expect("valid table name");
        let result = package.read_schema(&table_name);

        //* Then
        assert!(result.is_ok(), "read_schema should succeed");
        let read_schema = result.expect("should have schema");
        assert_eq!(read_schema.fields().len(), 2);
        assert_eq!(read_schema.field(0).name(), "id");
        assert_eq!(read_schema.field(1).name(), "name");
    }

    #[test]
    fn cached_package_read_function_succeeds() {
        //* Given
        let temp_dir = tempfile::tempdir().expect("should create temp dir");
        let cache = Cache::with_root(temp_dir.path().to_path_buf());
        let hash: Hash = "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9"
            .parse()
            .expect("valid hash");
        let manifest = create_test_manifest();
        cache.put(&hash, &manifest).expect("put should succeed");

        // Create function file in cache
        let functions_dir = cache.dir_path(&hash).join("functions");
        std::fs::create_dir_all(&functions_dir).expect("should create dir");
        std::fs::write(functions_dir.join("decode.js"), "function decode() {}")
            .expect("should write");

        let package = cache
            .get_package(&hash)
            .expect("should succeed")
            .expect("should have package");

        //* When
        let result = package.read_function("decode.js");

        //* Then
        assert!(result.is_ok(), "read_function should succeed");
        assert_eq!(result.expect("should have content"), "function decode() {}");
    }

    #[test]
    fn cached_package_has_methods_work() {
        //* Given
        let temp_dir = tempfile::tempdir().expect("should create temp dir");
        let cache = Cache::with_root(temp_dir.path().to_path_buf());
        let hash: Hash = "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9"
            .parse()
            .expect("valid hash");
        let manifest = create_test_manifest();
        cache.put(&hash, &manifest).expect("put should succeed");

        // Create only SQL file
        let tables_dir = cache.dir_path(&hash).join("tables");
        std::fs::create_dir_all(&tables_dir).expect("should create dir");
        std::fs::write(tables_dir.join("test_table.sql"), "SELECT 1").expect("should write");

        let package = cache
            .get_package(&hash)
            .expect("should succeed")
            .expect("should have package");

        //* When/Then
        let table_name: TableName = "test_table".parse().expect("valid table name");
        let missing_table: TableName = "missing".parse().expect("valid table name");

        assert!(package.has_sql(&table_name), "should have SQL");
        assert!(!package.has_ipc_schema(&table_name), "should not have IPC");
        assert!(!package.has_sql(&missing_table), "should not have SQL");
    }

    #[test]
    fn cached_package_into_manifest_returns_manifest() {
        //* Given
        let temp_dir = tempfile::tempdir().expect("should create temp dir");
        let cache = Cache::with_root(temp_dir.path().to_path_buf());
        let hash: Hash = "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9"
            .parse()
            .expect("valid hash");
        let manifest = create_test_manifest();
        cache.put(&hash, &manifest).expect("put should succeed");

        let package = cache
            .get_package(&hash)
            .expect("should succeed")
            .expect("should have package");

        //* When
        let manifest = package.into_manifest();

        //* Then
        assert_eq!(manifest.kind.as_str(), "manifest");
    }
}
