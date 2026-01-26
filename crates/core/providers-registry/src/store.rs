use std::{collections::BTreeMap, sync::Arc};

use futures::{Stream, StreamExt as _, TryStreamExt as _, stream};
use monitoring::logging;
use object_store::ObjectStore;
use parking_lot::RwLock;

use crate::config::ProviderConfig;

/// Store for provider configuration files with integrated caching.
///
/// Wraps an ObjectStore to provide provider-specific storage semantics.
/// This follows the same pattern as `DatasetManifestsStore` for consistency.
///
/// ## Caching Mechanism
///
/// This store implements a lazy-loaded, write-through caching strategy:
///
/// - **Lazy Loading**: The cache is populated on the first read operation when empty
/// - **Write-Through**: Both `store()` and `delete()` operations update both the
///   underlying store and the in-memory cache synchronously
/// - **Thread-Safe**: Uses `Arc<RwLock<BTreeMap>>` for safe concurrent access
/// - **No Expiration**: The cache never expires or invalidates
///
/// **Note**: External changes to the underlying store will not be reflected in the cache
/// until the process is restarted, as there is no automatic cache invalidation mechanism.
#[derive(Debug, Clone)]
pub struct ProviderConfigsStore<S: ObjectStore = Arc<dyn ObjectStore>> {
    object_store: S,
    cache: Arc<RwLock<BTreeMap<String, ProviderConfig>>>,
}

impl<S> ProviderConfigsStore<S>
where
    S: ObjectStore + Clone,
{
    /// Creates a new store backed by the given object store. Cache starts empty.
    pub fn new(store: S) -> Self {
        Self {
            object_store: store,
            cache: Default::default(),
        }
    }

    /// Get all cached provider configurations.
    ///
    /// Returns a read guard that dereferences to the cached `BTreeMap<String, ProviderConfig>`.
    /// This provides efficient access to all provider configurations without cloning.
    ///
    /// # Deadlock Warning
    ///
    /// The returned guard holds a read lock on the internal cache. Holding this guard
    /// for extended periods can cause deadlocks with operations that require write access
    /// (such as `store` and `delete`). Extract the needed data immediately and drop
    /// the guard as soon as possible.
    #[must_use]
    pub async fn get_all(
        &self,
    ) -> impl std::ops::Deref<Target = BTreeMap<String, ProviderConfig>> + '_ {
        // Load and populate cache if empty
        if self.cache.read().is_empty() {
            self.load_into_cache().await;
        }

        // Return the read guard for the cached provider configurations
        self.cache.read()
    }

    /// Gets a provider configuration by name. Returns None if not found.
    pub async fn get(&self, name: &str) -> Option<ProviderConfig> {
        // Load and populate cache if empty
        if self.cache.read().is_empty() {
            self.load_into_cache().await;
        }

        self.cache.read().get(name).cloned()
    }

    /// Store a provider configuration file.
    ///
    /// Serializes the provider to TOML, writes to `{name}.toml` in the underlying store,
    /// and updates the cache.
    pub async fn store(
        &self,
        name: &str,
        provider: ProviderConfig,
    ) -> Result<(), ConfigStoreError> {
        let path: object_store::path::Path = format!("{}.toml", name).into();

        // Serialize provider to TOML
        let content = toml::to_string(&provider)
            .map_err(ConfigStoreError::SerializeError)?
            .into_bytes();

        // Write to store
        self.object_store
            .put(&path, content.into())
            .await
            .map_err(ConfigStoreError::ObjectStoreError)?;

        // Update cache after successful store
        self.cache.write().insert(name.to_string(), provider);

        Ok(())
    }

    /// Delete a provider configuration file from both the store and cache.
    ///
    /// This operation is idempotent: deleting a non-existent file succeeds silently.
    ///
    /// # Cache Management
    /// - If file not found: removes stale cache entry and returns `Ok(())`
    /// - If other store errors: preserves cache (file may still exist) and propagates error
    /// - If deletion succeeds: removes from both store and cache
    pub async fn delete(&self, name: &str) -> Result<(), ConfigDeleteError> {
        let path: object_store::path::Path = format!("{}.toml", name).into();

        match self.object_store.delete(&path).await {
            Ok(()) => {}
            Err(object_store::Error::NotFound { .. }) => {} // Idempotent: not found is success
            Err(err) => return Err(ConfigDeleteError(err)),
        }

        // Remove from cache if present (safe even if not in cache)
        self.cache.write().remove(name);

        Ok(())
    }

    /// Load provider configurations from the store and initialize the cache.
    ///
    /// Overwrites existing cache contents. Invalid configuration files are logged and skipped.
    pub async fn load_into_cache(&self) {
        let stream = match self.fetch_from_store().await {
            Ok(stream) => stream,
            Err(err) => {
                tracing::error!(
                    error = %err, error_source = logging::error_source(&err),
                    "Failed to enumerate provider configurations from store, leaving cache empty"
                );

                *self.cache.write() = Default::default();
                return;
            }
        };

        let mut provider_map = BTreeMap::new();
        let mut failed_count = 0;
        let mut total_count = 0;

        let mut stream = std::pin::pin!(stream);
        while let Some(result) = stream.next().await {
            total_count += 1;
            match result {
                Ok((name, provider)) => {
                    provider_map.insert(name.clone(), provider);
                    tracing::debug!(name = %name, "Successfully loaded provider configuration");
                }
                Err(err) => {
                    failed_count += 1;
                    tracing::warn!(
                        file = %err.path(),
                        error = %err, error_source = logging::error_source(&err),
                        "Failed to load and parse provider configuration file, skipping"
                    );
                }
            }
        }

        if failed_count > 0 {
            tracing::warn!(
                invalid_files = failed_count,
                total_files = total_count,
                "Provider configuration loading completed with some failures"
            );
        } else {
            tracing::info!(
                total_files = total_count,
                "Successfully loaded all provider configuration files"
            );
        }

        *self.cache.write() = provider_map;
    }

    /// Fetch all [`ProviderConfig`]s from the underlying store and parse them.
    ///
    /// Filters for non-empty `.toml` files with valid filenames, then loads and parses each file.
    /// Returns a stream of provider configuration results that can fail individually.
    /// Store enumeration failure returns an error immediately.
    async fn fetch_from_store(
        &self,
    ) -> Result<impl Stream<Item = Result<(String, ProviderConfig), LoadFileError>>, ListError>
    {
        let files: Vec<_> = self
            .object_store
            .list(None) // None = no prefix filter, list all files
            .try_filter_map(|meta| async move {
                // Select valid provider configuration files and extract their names for processing
                let is_empty = meta.size == 0;
                let is_toml_file = meta.location.as_ref().ends_with(".toml");

                if is_empty || !is_toml_file {
                    return Ok(None);
                }

                // Use the full relative path (with prefix stripped by PrefixedStore)
                // instead of just the filename to handle subdirectories correctly
                let path_str = meta.location.as_ref();
                let Some(name) = path_str.strip_suffix(".toml") else {
                    return Ok(None);
                };

                Ok(Some((name.to_string(), meta.location)))
            })
            .try_collect()
            .await
            .map_err(ListError)?;

        let store = self.object_store.clone();
        let stream = stream::iter(files).then(move |(name, path)| {
            let store = store.clone();
            async move {
                let provider = load_and_parse_file(&store, &path).await?;
                Ok((name, provider))
            }
        });

        Ok(stream)
    }
}

/// Error that can occur when storing a provider configuration.
#[derive(Debug, thiserror::Error)]
pub enum ConfigStoreError {
    /// Failed to serialize provider configuration to TOML.
    #[error("Failed to serialize provider configuration to TOML")]
    SerializeError(#[source] toml::ser::Error),

    /// Failed to write to object store.
    #[error("Failed to write provider configuration to object store")]
    ObjectStoreError(#[source] object_store::Error),
}

/// Error that can occur when deleting a provider configuration.
#[derive(Debug, thiserror::Error)]
#[error("Failed to delete provider configuration from object store")]
pub struct ConfigDeleteError(#[source] pub object_store::Error);

/// Load and parse a single provider configuration file.
///
/// Loads a provider configuration file from storage, parses it as TOML,
/// and deserializes it into a [`ProviderConfig`] struct.
///
/// Returns detailed error information suitable for structured logging and diagnostics.
async fn load_and_parse_file(
    store: &impl ObjectStore,
    location: &object_store::path::Path,
) -> Result<ProviderConfig, LoadFileError> {
    let path = location.to_string();

    // Fetch file contents
    let get_res = store
        .get(location)
        .await
        .map_err(|source| LoadFileError::StoreFetchFailed {
            path: path.clone(),
            source,
        })?;

    let bytes = get_res
        .bytes()
        .await
        .map_err(|source| LoadFileError::StoreReadFailed {
            path: path.clone(),
            source,
        })?;

    // Parse as UTF-8
    let content =
        String::from_utf8(bytes.to_vec()).map_err(|source| LoadFileError::InvalidUtf8 {
            path: path.clone(),
            source,
        })?;

    // Parse TOML and deserialize directly to ProviderConfig
    let mut provider = toml::from_str::<ProviderConfig>(&content).map_err(|source| {
        LoadFileError::TomlParseError {
            path: path.clone(),
            source,
        }
    })?;

    // Auto-populate name field from file path if not present in TOML
    if provider.name.is_empty() {
        provider.name = path.strip_suffix(".toml").unwrap_or(&path).to_string();
    }

    Ok(provider)
}

/// Error that can occur when loading provider configurations from the store into the cache.
#[derive(Debug, thiserror::Error)]
#[error("Failed to enumerate provider configuration files from store")]
struct ListError(#[source] object_store::Error);

/// Error types when loading and parsing individual provider configuration files.
/// These are used for structured logging and error handling during graceful degradation.
#[derive(Debug, thiserror::Error)]
enum LoadFileError {
    /// Failed to fetch file from the underlying store
    #[error("Failed to fetch file from the object store: {source}")]
    StoreFetchFailed {
        path: String,
        source: object_store::Error,
    },

    /// Failed to read bytes from the fetched file
    #[error("Failed to read file bytes from the object store: {source}")]
    StoreReadFailed {
        path: String,
        source: object_store::Error,
    },

    /// File contains invalid UTF-8 content
    #[error("Invalid UTF-8 content: {source}")]
    InvalidUtf8 {
        path: String,
        source: std::string::FromUtf8Error,
    },

    /// TOML parsing failed
    #[error("Invalid TOML file format: {source}")]
    TomlParseError {
        path: String,
        source: toml::de::Error,
    },
}

impl LoadFileError {
    /// Returns the file path associated with this error.
    fn path(&self) -> &str {
        match self {
            LoadFileError::StoreFetchFailed { path, .. }
            | LoadFileError::StoreReadFailed { path, .. }
            | LoadFileError::InvalidUtf8 { path, .. }
            | LoadFileError::TomlParseError { path, .. } => path,
        }
    }
}
