use std::sync::Arc;

use backon::Retryable as _;
use datasets_common::hash::Hash;
use metadata_db::MetadataDb;
use object_store::{ObjectStore, path::Path as ObjectStorePath};

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
///
/// ## No Caching Strategy
///
/// Unlike provider's store, this implementation does not use caching:
///
/// - **Direct Access**: All operations directly access the underlying store
/// - **Fresh Data**: Each call returns the most up-to-date information
/// - **Simplicity**: No cache invalidation or consistency concerns
///
/// **Note**: External changes to the underlying store are immediately visible.
#[derive(Debug, Clone)]
pub struct DatasetManifestsStore<S: ObjectStore = Arc<dyn ObjectStore>> {
    metadata_db: MetadataDb,
    store: S,
}

impl<S> DatasetManifestsStore<S>
where
    S: ObjectStore + Clone,
{
    /// Create a new [`DatasetManifestsStore`] instance with the given underlying store.
    pub fn new(metadata_db: MetadataDb, store: S) -> Self {
        Self { metadata_db, store }
    }

    /// Store a new dataset manifest in the underlying store and register it in the metadata database
    ///
    /// This operation is idempotent using object-store-first ordering:
    /// 1. Write manifest to object store (idempotent - content-addressable)
    /// 2. Register manifest in metadata database (idempotent with ON CONFLICT DO NOTHING)
    ///
    /// The manifest is stored with hash-based filename: `{hash}` (no extension).
    /// Input must be canonical JSON string (already validated and serialized).
    pub async fn store(&self, hash: &Hash, manifest_str: String) -> Result<(), StoreError> {
        // Write to object store first (idempotent operation)
        let manifest_path = ObjectStorePath::from(hash.to_string());
        self.store
            .put(&manifest_path, manifest_str.into())
            .await
            .map_err(StoreError::ObjectStorePut)?;

        tracing::debug!(
            manifest_hash = %hash,
            "Manifest written to object store, registering in database"
        );

        // Register in database (idempotent with ON CONFLICT DO NOTHING)
        register_manifest_with_retry(&self.metadata_db, hash)
            .await
            .map_err(StoreError::MetadataDbRegister)?;

        tracing::debug!(
            manifest_hash = %hash,
            "Manifest registered in database"
        );

        Ok(())
    }

    /// Get a dataset manifest by hash from the object store
    ///
    /// Returns the manifest content if found, or None if not registered in the metadata database.
    /// This method first checks the metadata database for the manifest path, and only fetches
    /// from the object store if the manifest is registered.
    ///
    /// ## Steps performed:
    /// 1. Query metadata database for manifest path by hash (with retry on connection issues)
    /// 2. If not registered (None): return Ok(None)
    /// 3. If registered (Some(path)): fetch manifest content from object store using the path
    pub async fn get(&self, hash: &Hash) -> Result<Option<ManifestContent>, GetError> {
        // Get manifest path from metadata database (with retry)
        let Some(path) = get_manifest_path_with_retry(&self.metadata_db, hash)
            .await
            .map_err(GetError::MetadataDbQueryPath)?
        else {
            return Ok(None);
        };

        // Fetch manifest content from object store using the path from metadata DB
        let manifest_path = ObjectStorePath::from(path);
        let get_result = match self.store.get(&manifest_path).await {
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
        let content = String::from_utf8(bytes.to_vec()).map_err(GetError::Utf8Error)?;

        Ok(Some(ManifestContent(content)))
    }
}

/// Error when storing manifest in object store and metadata database
#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    /// Failed to put manifest in object store
    #[error("Failed to put manifest in object store")]
    ObjectStorePut(#[source] object_store::Error),

    /// Failed to register manifest in metadata database
    #[error("Failed to register manifest in metadata database")]
    MetadataDbRegister(#[source] metadata_db::Error),
}

/// Errors specific to manifest retrieval operations
#[derive(Debug, thiserror::Error)]
pub enum GetError {
    /// Failed to query manifest path from metadata database
    #[error("Failed to query manifest path from metadata database")]
    MetadataDbQueryPath(#[source] metadata_db::Error),

    /// Failed to get manifest object from object store
    #[error("Failed to get manifest object from object store")]
    ObjectStoreGet(#[source] object_store::Error),

    /// Failed to read manifest bytes from object store
    #[error("Failed to read manifest bytes from object store")]
    ObjectStoreReadBytes(#[source] object_store::Error),

    /// Failed to decode manifest content as UTF-8
    #[error("Failed to decode manifest content as UTF-8")]
    Utf8Error(#[source] std::string::FromUtf8Error),
}

/// Newtype wrapper for dataset manifest content (JSON format only)
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

/// Register manifest file in metadata database with retry on connection errors
///
/// This operation is idempotent (`ON CONFLICT DO NOTHING`).
async fn register_manifest_with_retry(
    metadata_db: &MetadataDb,
    hash: &Hash,
) -> Result<(), metadata_db::Error> {
    let path = hash.to_string();
    (|| metadata_db.register_manifest(hash, path.as_str()))
        .retry(retry_policy())
        .when(metadata_db::Error::is_connection_error)
        .notify(|err, dur| {
            tracing::warn!(
                error = %err,
                manifest_hash = %hash,
                "Database connection issue during manifest registration. Retrying in {:.1}s",
                dur.as_secs_f32()
            );
        })
        .await
}

/// Get manifest file path by hash with retry on connection errors
async fn get_manifest_path_with_retry(
    metadata_db: &MetadataDb,
    hash: &Hash,
) -> Result<Option<String>, metadata_db::Error> {
    (|| metadata_db.get_manifest_path(hash))
        .retry(retry_policy())
        .when(metadata_db::Error::is_connection_error)
        .notify(|err, dur| {
            tracing::warn!(
                error = %err,
                manifest_hash = %hash,
                "Database connection issue during path lookup. Retrying in {:.1}s",
                dur.as_secs_f32()
            );
        })
        .await
}

/// A retry policy for metadata DB operations in the manifest store.
///
/// Uses exponential backoff with jitter to prevent thundering herd problems when
/// multiple clients retry simultaneously after a transient failure (e.g., database
/// connection timeout, temporary network issue).
///
/// ## Retry Configuration
/// - **Jitter**: Enabled (randomizes delay to spread out retry attempts)
/// - **Factor**: 2 (exponential backoff multiplier)
/// - **Min Delay**: 100ms (initial retry delay)
/// - **Max Delay**: 2s (maximum retry delay cap)
/// - **Max Attempts**: 5 (total of 5 retry attempts before giving up)
///
/// ## Example Retry Sequence (with jitter)
/// Without jitter, retries would occur at: 100ms, 200ms, 400ms, 800ms, 1600ms (capped at 2s)
/// With jitter, each delay is randomized within a range around these values, preventing
/// synchronized retry storms from multiple clients.
///
/// ## Thundering Herd Prevention
/// Jitter is critical when multiple instances retry database operations after a shared
/// failure (e.g., database restart). Without jitter, all instances would retry at the
/// exact same moments, potentially overwhelming the recovering database. Jitter spreads
/// these retries over time, allowing gradual recovery.
#[inline]
fn retry_policy() -> backon::ExponentialBuilder {
    backon::ExponentialBuilder::default()
        .with_jitter() // Add jitter to prevent thundering herd
        .with_min_delay(std::time::Duration::from_millis(100))
        .with_max_delay(std::time::Duration::from_secs(2))
        .with_max_times(5)
}
