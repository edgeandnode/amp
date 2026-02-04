//! Global dependency cache.
//!
//! Provides caching for resolved manifests at `~/.amp/registry/<hash>/`.
//!
//! The cache stores dependency manifests locally to avoid repeated network
//! fetches during builds. Each manifest is stored in a directory named by
//! its content hash, containing `manifest.json`.

use std::path::PathBuf;

use datasets_common::hash::Hash;

use crate::dependency_manifest::DependencyManifest;

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
}
