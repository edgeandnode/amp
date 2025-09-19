//! Provider configuration management for dataset stores.
//!
//! This module provides functionality to manage external data source provider configurations
//! (such as EVM RPC endpoints, Firehose endpoints, etc.) that datasets connect to.
//! Provider configurations are defined as TOML files.
//!
//! ## Provider Configuration Structure
//!
//! All provider configurations must define at least:
//! - `kind`: The type of provider (e.g., "evm-rpc", "firehose", "substreams")
//! - `network`: The blockchain network (e.g., "mainnet", "goerli", "polygon")
//!
//! Additional fields depend on the provider type.

use std::{collections::BTreeMap, ops::Deref, sync::Arc};

use common::BoxError;
use futures::{Stream, StreamExt as _, TryStreamExt as _, stream};
use object_store::ObjectStore;
use parking_lot::RwLock;

use crate::dataset_kind::DatasetKind;

/// Provider configuration with required and provider-specific fields.
///
/// This struct captures the required fields (`kind` and `network`) that must be present
/// in all provider configurations, while using serde's `flatten` attribute to collect
/// all additional provider-specific configuration fields in the `rest` field.
#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct ProviderConfig {
    /// The type of provider (e.g., "evm-rpc", "firehose", "substreams")
    pub kind: DatasetKind,
    /// The blockchain network (e.g., "mainnet", "goerli", "polygon")
    pub network: String,
    /// All other provider-specific configuration fields
    #[serde(flatten)]
    pub rest: toml::Table,
}

/// Manages provider configurations and caching
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
/// ## Caching Mechanism
///
/// This store implements a lazy-loaded, write-through caching strategy:
///
/// - **Lazy Loading**: The cache is populated on the first read operation when empty
/// - **Write-Through**: Both `register()` and `delete()` operations update both the
///   underlying store and the in-memory cache synchronously
/// - **Thread-Safe**: Uses `Arc<RwLock<BTreeMap>>` for safe concurrent access
/// - **No Expiration**: The cache never expires or invalidates
///
/// **Note**: External changes to the underlying store will not be reflected in the cache
/// until the process is restarted, as there is no automatic cache invalidation mechanism.
#[derive(Debug, Clone)]
pub struct ProvidersConfigStore<S: ObjectStore = Arc<dyn ObjectStore>> {
    store: S,
    cache: Arc<RwLock<BTreeMap<String, ProviderConfig>>>,
}

impl<S> ProvidersConfigStore<S>
where
    S: ObjectStore + Clone,
{
    /// Create a new [`ProvidersConfigStore`] instance with the given underlying store.
    pub fn new(store: S) -> Self {
        Self {
            store,
            cache: Default::default(),
        }
    }

    /// Load provider configurations from disk and initialize the cache.
    ///
    /// Overwrites existing cache contents. Invalid configuration files are logged and skipped.
    pub async fn load_into_cache(&self) {
        let stream = match self.fetch_from_store().await {
            Ok(stream) => stream,
            Err(err) => {
                tracing::error!(
                    error = %err,
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
                        error = %err,
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

    /// Get all provider configurations, using cache if available
    ///
    /// Returns a read guard that dereferences to the cached `BTreeMap<String, ProviderConfig>`.
    /// This provides efficient access to all provider configurations without cloning.
    ///
    /// # Deadlock Warning
    ///
    /// The returned guard holds a read lock on the internal cache. Holding this guard
    /// for extended periods can cause deadlocks with operations that require write access
    /// (such as `register` and `delete`). Extract the needed data immediately and drop
    /// the guard as soon as possible.
    #[must_use]
    pub async fn get_all(&self) -> impl Deref<Target = BTreeMap<String, ProviderConfig>> + '_ {
        // Load and populate cache if empty
        if self.cache.read().is_empty() {
            self.load_into_cache().await;
        }

        // Return the read guard for the cached provider configurations
        self.cache.read()
    }

    /// Get a provider configuration by `name`, using cache if available
    pub async fn get_by_name(&self, name: &str) -> Option<ProviderConfig> {
        // Load and populate cache if empty
        if self.cache.read().is_empty() {
            self.load_into_cache().await;
        }

        self.cache.read().get(name).cloned()
    }

    /// Register a new provider configuration in both cache and store
    ///
    /// If a provider configuration with the same name already exists, returns a conflict error.
    pub async fn register(
        &self,
        name: &str,
        provider: ProviderConfig,
    ) -> Result<(), RegisterError> {
        // Load and populate cache if empty
        if self.cache.read().is_empty() {
            self.load_into_cache().await;
        }

        // Serialize and write to store first for durability
        let content = toml::to_string(&provider)?.into_bytes();
        let path = format!("{}.toml", name).into();
        self.store.put(&path, content.into()).await?;

        // Check if provider configuration already exists in cache
        if self.cache.read().contains_key(name) {
            return Err(RegisterError::Conflict {
                name: name.to_string(),
            });
        }

        // Add to cache
        self.cache.write().insert(name.to_string(), provider);

        Ok(())
    }

    /// Delete a provider configuration by name from both the store and cache
    ///
    /// This operation fails if the provider configuration file does not exist in the store, ensuring
    /// consistent delete behavior across different object store implementations.
    ///
    /// # Cache Management
    /// - If file not found: removes stale cache entry and returns `DeleteError::NotFound`
    /// - If other store errors: preserves cache (file may still exist) and propagates error
    /// - If deletion succeeds: removes from both store and cache
    pub async fn delete(&self, name: &str) -> Result<(), DeleteError> {
        let path = format!("{}.toml", name).into();

        // Fail fast if file doesn't exist
        if let Err(err) = self.store.head(&path).await.map_err(Into::into) {
            match err {
                DeleteError::NotFound { .. } => {
                    // Remove from cache if present (safe even if not in cache)
                    self.cache.write().remove(name);

                    // Propagate the not found error
                    return Err(err);
                }
                // Propagate other store errors (without modifying cache)
                _ => return Err(err),
            }
        }

        // File exists, proceed with deletion
        self.store.delete(&path).await?;

        // Remove from cache if present (safe even if not in cache)
        self.cache.write().remove(name);

        Ok(())
    }

    /// Fetch all [`ProviderConfig`]s from the underlying store and parse them.
    ///
    /// Filters for non-empty `.toml` files with valid filenames, then loads and parses each file.
    /// Returns a stream of provider configuration results that can fail individually.
    /// Store enumeration failure returns an error immediately.
    async fn fetch_from_store(
        &self,
    ) -> Result<impl Stream<Item = Result<(String, ProviderConfig), LoadFileError>>, LoadError>
    {
        let providers: Vec<_> = self
            .store
            .list(None)
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
            .map_err(|source| LoadError { source })?;

        let stream = stream::iter(providers).then(move |(name, path)| {
            let store = self.store.clone();
            async move {
                // Load and parse provider configuration file
                match load_and_parse_file(&store, &path).await {
                    Ok(provider) => Ok((name, provider)),
                    Err(err) => Err(err),
                }
            }
        });

        Ok(stream)
    }
}

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
    let provider = toml::from_str::<ProviderConfig>(&content).map_err(|source| {
        LoadFileError::TomlParseError {
            path: path.clone(),
            source,
        }
    })?;

    Ok(provider)
}

/// Errors that can occur when fetching provider configurations.
#[derive(Debug, thiserror::Error)]
pub enum FetchError {
    /// Failed to fetch provider configuration from the underlying store.
    ///
    /// This wraps errors from the storage layer when attempting to read provider
    /// configuration files, such as file not found, permission errors, or network
    /// issues for remote stores.
    #[error("failed to fetch provider configuration: {0}")]
    StoreFetchFailed(#[from] BoxError),

    /// Failed to parse provider configuration as valid TOML.
    ///
    /// This occurs when a provider configuration file exists but contains invalid
    /// TOML syntax, missing required fields, or field values that cannot be
    /// deserialized into the expected types.
    #[error("TOML parse error: {0}")]
    TomlParseError(#[from] toml::de::Error),

    /// Provider configuration file contains invalid UTF-8.
    ///
    /// This occurs when a provider configuration file exists but contains binary data
    /// or text in a non-UTF-8 encoding, preventing it from being parsed as a text file.
    #[error("provider configuration file is not valid UTF-8 at {location}: {source}")]
    InvalidUtf8 {
        /// The location (file path) of the invalid UTF-8 file
        location: String,
        /// The underlying UTF-8 decoding error
        source: std::string::FromUtf8Error,
    },
}

/// Error that can occur when registering a provider configuration.
#[derive(Debug, thiserror::Error)]
pub enum RegisterError {
    /// Failed to fetch existing provider configurations from the store.
    #[error("failed to fetch existing provider configurations from store: {0}")]
    FetchError(#[from] FetchError),
    /// Provider configuration already exists with the same name.
    #[error("provider configuration '{name}' already exists")]
    Conflict { name: String },
    /// Failed to serialize provider configuration to TOML.
    #[error("failed to serialize provider configuration to TOML: {0}")]
    SerializeError(#[from] toml::ser::Error),
    /// Failed to write provider configuration to store.
    #[error("failed to write provider configuration to store: {0}")]
    StoreError(#[from] object_store::Error),
}

/// Error that can occur when deleting a provider configuration.
#[derive(Debug, thiserror::Error)]
pub enum DeleteError {
    /// The provider configuration file was not found in the store.
    #[error("provider configuration not found: {name}")]
    NotFound { name: String },

    /// An error occurred while accessing the store.
    #[error("store error: {0}")]
    StoreError(object_store::Error),
}

impl From<object_store::Error> for DeleteError {
    fn from(error: object_store::Error) -> Self {
        match error {
            object_store::Error::NotFound { path, .. } => {
                // Extract provider configuration name from path by removing .toml extension
                let name = path.strip_suffix(".toml").unwrap_or(&path).to_string();
                DeleteError::NotFound { name }
            }
            other => DeleteError::StoreError(other),
        }
    }
}

/// Error that can occur when loading provider configurations from the store into the cache.
#[derive(Debug, thiserror::Error)]
#[error("Failed to enumerate provider configuration files from store")]
pub struct LoadError {
    pub source: object_store::Error,
}

/// Error types when loading and parsing individual provider configuration files.
/// These are used for structured logging and error handling during graceful degradation.
#[derive(Debug, thiserror::Error)]
pub enum LoadFileError {
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
    /// Get the file path associated with this error
    pub fn path(&self) -> &str {
        match self {
            LoadFileError::StoreFetchFailed { path, .. }
            | LoadFileError::StoreReadFailed { path, .. }
            | LoadFileError::InvalidUtf8 { path, .. }
            | LoadFileError::TomlParseError { path, .. } => path,
        }
    }
}

#[cfg(test)]
mod tests {
    mod cache;
    mod crud;
}
