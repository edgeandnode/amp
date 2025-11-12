use std::sync::Arc;

use datasets_common::hash::Hash;
use object_store::{ObjectStore, PutPayload, path::Path as ObjectStorePath};

/// Manages dataset manifest configurations with object store operations
///
/// ## Manifest Storage Format
///
/// This store uses hash-based storage:
/// - **Storage**: Manifests are stored as `{hash}` (no file extension)
/// - **Content Addressing**: Filename is the SHA-256 hash of the manifest JSON content
/// - **Deduplication**: Identical manifests share the same file
/// - **Retrieval**: Use [`get`](Self::get) with the manifest hash
/// - **Creation**: Use [`store`](Self::store) to write new manifests
///
/// ## Object Store Agnostic Design
///
/// The store logic is agnostic to rate-limiting and location prefixing. API users must provide
/// the appropriate object store implementation to the struct constructor:
///
/// - **Rate Limiting**: Use `object_store::limit::LimitStore` to wrap the underlying store
/// - **Path Prefixing**: Use `object_store::prefix::PrefixStore` or equivalent for path isolation
/// - **Composition**: These can be combined as needed (e.g., `LimitStore<PrefixStore<LocalFileSystem>>`)
#[derive(Debug, Clone)]
pub struct DatasetManifestsStore<S: ObjectStore = Arc<dyn ObjectStore>> {
    store: S,
}

impl<S> DatasetManifestsStore<S>
where
    S: ObjectStore + Clone,
{
    /// Create a new [`DatasetManifestsStore`] instance with the given underlying store.
    pub fn new(store: S) -> Self {
        Self { store }
    }

    /// Store a new dataset manifest in the underlying object store
    ///
    /// Idempotent (content-addressable storage). Input must be canonical JSON.
    /// Returns the storage path.
    pub async fn store(
        &self,
        hash: &Hash,
        content: impl Into<PutPayload>,
    ) -> Result<ManifestPath, StoreError> {
        // Derive path from content hash
        let path = ManifestPath(hash.to_string());

        // Store the manifest content in the object store
        let store_path: ObjectStorePath = path.clone().into();
        self.store
            .put(&store_path, content.into())
            .await
            .map_err(StoreError)?;

        tracing::debug!(
            manifest_hash = %hash,
            path = %path,
            "Manifest written to object store"
        );

        Ok(path)
    }

    /// Get a dataset manifest from the object store by path
    ///
    /// Returns `None` if file doesn't exist. Caller must resolve hash to path via
    /// `metadata_db::manifests::get_path()` before calling.
    pub async fn get(&self, path: ManifestPath) -> Result<Option<ManifestContent>, GetError> {
        let path: ObjectStorePath = path.into();

        let get_result = match self.store.get(&path).await {
            Ok(res) => res,
            Err(object_store::Error::NotFound { .. }) => return Ok(None),
            Err(err) => return Err(GetError::ObjectStoreGet(err)),
        };

        // Read all bytes from the object
        let bytes = get_result
            .bytes()
            .await
            .map_err(GetError::ObjectStoreReadBytes)?;

        // Convert bytes to UTF-8 string
        let content = String::from_utf8(bytes.to_vec())
            .map(ManifestContent)
            .map_err(GetError::Utf8Error)?;

        tracing::debug!("Manifest retrieved from object store");

        Ok(Some(content))
    }

    /// Delete a manifest file from the object store
    ///
    /// Idempotent (tolerates missing files).
    pub async fn delete(&self, path: ManifestPath) -> Result<(), DeleteError> {
        let path: ObjectStorePath = path.into();

        match self.store.delete(&path).await {
            Ok(_) => {
                tracing::debug!(
                    path = %path,
                    "Manifest deleted from object store"
                );
                Ok(())
            }
            Err(object_store::Error::NotFound { .. }) => {
                tracing::debug!(
                    path = %path,
                    "Manifest not found in object store (already deleted or never existed)"
                );
                Ok(())
            }
            Err(err) => Err(DeleteError(err)),
        }
    }
}

/// Error when storing manifest in object store
#[derive(Debug, thiserror::Error)]
#[error("Failed to put manifest in object store: {0}")]
pub struct StoreError(#[source] pub object_store::Error);

/// Errors specific to manifest retrieval operations
#[derive(Debug, thiserror::Error)]
pub enum GetError {
    /// Failed to get manifest object from object store
    #[error("Failed to get manifest object from object store: {0}")]
    ObjectStoreGet(#[source] object_store::Error),

    /// Failed to read manifest bytes from object store
    #[error("Failed to read manifest bytes from object store: {0}")]
    ObjectStoreReadBytes(#[source] object_store::Error),

    /// Failed to decode manifest content as UTF-8
    #[error("Failed to decode manifest content as UTF-8: {0}")]
    Utf8Error(#[source] std::string::FromUtf8Error),
}

/// Errors that can occur during manifest deletion
#[derive(Debug, thiserror::Error)]
#[error("Failed to delete manifest from object store")]
pub struct DeleteError(#[source] pub object_store::Error);

/// New-type wrapper for manifest file path in object store
///
/// This type represents the path where a manifest is stored in the object store.
/// The path is typically the content hash of the manifest (content-addressable storage).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManifestPath(String);

impl ManifestPath {
    /// Consume and return the inner String
    pub fn into_inner(self) -> String {
        self.0
    }

    /// Get a reference to the inner str
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::ops::Deref for ManifestPath {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl AsRef<str> for ManifestPath {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for ManifestPath {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl From<metadata_db::ManifestPathOwned> for ManifestPath {
    fn from(value: metadata_db::ManifestPathOwned) -> Self {
        // Convert to string - Database values are trusted to uphold invariants
        ManifestPath(value.into_inner())
    }
}

impl From<ManifestPath> for metadata_db::ManifestPathOwned {
    fn from(value: ManifestPath) -> Self {
        // SAFETY: ManifestPath is created from validated content hash in store() method,
        // ensuring invariants are upheld.
        metadata_db::ManifestPath::from_owned_unchecked(value.0)
    }
}

impl<'a> From<&'a ManifestPath> for metadata_db::ManifestPath<'a> {
    fn from(value: &'a ManifestPath) -> Self {
        // SAFETY: ManifestPath is created from validated content hash in store() method,
        // ensuring invariants are upheld.
        metadata_db::ManifestPath::from_ref_unchecked(&value.0)
    }
}

impl From<ManifestPath> for ObjectStorePath {
    fn from(value: ManifestPath) -> Self {
        value.0.into()
    }
}

/// New-type wrapper for dataset manifest content
pub struct ManifestContent(String);

impl ManifestContent {
    /// Get the raw JSON string content of the manifest, consuming self
    pub fn into_json_string(self) -> String {
        self.0
    }

    pub fn try_into_manifest<T>(&self) -> Result<T, ManifestParseError>
    where
        T: serde::de::DeserializeOwned,
    {
        serde_json::from_str(&self.0).map_err(ManifestParseError)
    }
}

impl AsRef<[u8]> for ManifestContent {
    fn as_ref(&self) -> &[u8] {
        self.0.as_bytes()
    }
}

/// JSON parsing error when deserializing manifest content
#[derive(Debug, thiserror::Error)]
#[error("JSON parsing error: {0}")]
pub struct ManifestParseError(#[source] pub serde_json::Error);
