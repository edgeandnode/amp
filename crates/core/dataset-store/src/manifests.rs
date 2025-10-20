use std::sync::Arc;

use common::store::ObjectStoreExt as _;
use datasets_common::{name::Name, namespace::Namespace, version::Version};
use futures::TryStreamExt as _;
use metadata_db::MetadataDb;
use object_store::{ObjectStore, path::Path as ObjectStorePath};

/// Manages dataset manifest configurations combining ObjectStore and MetadataDb operations
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
/// - **MetadataDb Integration**: Operations coordinate between ObjectStore and MetadataDb
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
    /// Create a new [`DatasetManifestsStore`] instance with the given metadata database and underlying store.
    pub fn new(metadata_db: MetadataDb, store: S) -> Self {
        Self { metadata_db, store }
    }

    /// Get the latest version for a given dataset name
    ///
    /// Returns the latest version if found, or None if no dataset with the given name exists.
    pub async fn get_latest_version(
        &self,
        name: &Name,
    ) -> Result<Option<Version>, GetLatestVersionError> {
        // TODO: Pass the actual namespace instead of using a placeholder
        let namespace = "_"
            .parse::<Namespace>()
            .expect("'_' should be a valid namespace");
        let dataset = self
            .metadata_db
            .get_dataset_latest_version_with_details(&namespace, name)
            .await
            .map_err(GetLatestVersionError)?;

        Ok(dataset.map(|details| details.version.into()))
    }

    /// Get all dataset manifests from the underlying store
    ///
    /// Returns a set of all available manifests with their name, version, and path information.
    /// This operation directly queries the underlying store without caching.
    pub async fn list(&self) -> Vec<(Name, Version)> {
        // Get all datasets from metadata database
        let datasets = match self
            .metadata_db
            .stream_all_datasets()
            .try_collect::<Vec<_>>()
            .await
        {
            Ok(datasets) => datasets,
            Err(err) => {
                tracing::error!(error = ?err, "Failed to get datasets from metadata database");
                return Vec::new();
            }
        };

        // Convert to Vec of (Name, Version), filtering out invalid entries
        datasets
            .into_iter()
            .map(|dataset| {
                let name = dataset.name.parse::<Name>().unwrap_or_else(|_| {
                    unreachable!(
                        "Datasets from the metadata DB MUST have valid names, got: {}",
                        dataset.name
                    )
                });
                let version = dataset.version.into();
                (name, version)
            })
            .collect()
    }

    /// Get a specific dataset manifest by name and version
    ///
    /// Returns the manifest path and content if found, or None if not found.
    /// This operation directly queries the underlying store without caching.
    pub async fn get(
        &self,
        name: &Name,
        version: impl Into<Option<&Version>>,
    ) -> Result<Option<ManifestContent>, GetError> {
        // TODO: Pass the actual namespace instead of using a placeholder
        let namespace = "_"
            .parse::<Namespace>()
            .expect("'_' should be a valid namespace");
        let res = match version.into() {
            None => self
                .metadata_db
                .get_dataset_latest_version_with_details(&namespace, name)
                .await
                .map_err(GetError::MetadataDbError)?,
            Some(version) => self
                .metadata_db
                .get_dataset_with_details(&namespace, name, version)
                .await
                .map_err(GetError::MetadataDbError)?,
        };

        let Some(dataset) = res else {
            return Ok(None);
        };

        let manifest_file_path = ManifestPath::try_from(dataset.manifest_path)
            .map_err(GetError::UnsupportedManifestFormat)?;
        fetch_manifest_content(&self.store, manifest_file_path)
            .await
            .map_err(GetError::ObjectStoreFetch)
    }

    /// Store a new dataset manifest in the underlying store
    ///
    /// Returns the path where the manifest was stored.
    /// Note: This function only stores the manifest file, it does not register
    /// the dataset in the metadata database. The caller is responsible for that.
    pub async fn store(
        &self,
        name: &Name,
        version: &Version,
        manifest_str: String,
    ) -> Result<ObjectStorePath, StoreError> {
        // Store manifest in underlying store
        let manifest_path = ObjectStorePath::from(format!(
            "{}__{}.json",
            name,
            version.to_underscore_version()
        ));

        self.store
            .put(&manifest_path, manifest_str.into())
            .await
            .map_err(StoreError)?;

        Ok(manifest_path)
    }
}

/// Error when storing manifest in object store
#[derive(Debug, thiserror::Error)]
#[error("Failed to store manifest in object store: {0}")]
pub struct StoreError(#[source] pub object_store::Error);

/// Errors specific to manifest retrieval operations
#[derive(Debug, thiserror::Error)]
pub enum GetError {
    /// Failed to query metadata database
    #[error("Failed to query metadata database: {0}")]
    MetadataDbError(metadata_db::Error),

    /// Unsupported manifest file format
    #[error("Unsupported manifest file format: {}", .0.format)]
    UnsupportedManifestFormat(#[source] UnsupportedManifestFormat),

    /// Failed to fetch manifest from object store
    #[error("Failed to fetch manifest from object store: {0}")]
    ObjectStoreFetch(common::store::StoreError),
}

/// Failed to query metadata database for latest version
#[derive(Debug, thiserror::Error)]
#[error("Failed to query metadata database for latest version: {0}")]
pub struct GetLatestVersionError(#[source] pub metadata_db::Error);

/// Fetches manifest content from the object store
///
/// Retrieves the content of a manifest file (JSON format) from the provided
/// object store. The function gracefully handles missing files by returning `None`.
async fn fetch_manifest_content<S>(
    store: &S,
    path: ManifestPath,
) -> Result<Option<ManifestContent>, common::store::StoreError>
where
    S: ObjectStore,
{
    match store.get_string(path.into_inner()).await {
        Ok(content) => Ok(Some(ManifestContent(content))),
        Err(err) if err.is_not_found() => Ok(None),
        Err(err) => Err(err),
    }
}

/// Newtype wrapper for dataset manifest paths (must be .json files)
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct ManifestPath(ObjectStorePath);

impl ManifestPath {
    /// Get the inner ObjectStorePath
    pub fn into_inner(self) -> ObjectStorePath {
        self.0
    }
}

impl AsRef<ObjectStorePath> for ManifestPath {
    fn as_ref(&self) -> &ObjectStorePath {
        &self.0
    }
}

impl From<ManifestPath> for ObjectStorePath {
    fn from(path: ManifestPath) -> Self {
        path.0
    }
}

impl TryFrom<String> for ManifestPath {
    type Error = UnsupportedManifestFormat;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        if !value.ends_with(".json") {
            return Err(UnsupportedManifestFormat { format: value });
        }
        Ok(ManifestPath(ObjectStorePath::from(value)))
    }
}

impl TryFrom<ObjectStorePath> for ManifestPath {
    type Error = UnsupportedManifestFormat;

    fn try_from(value: ObjectStorePath) -> Result<Self, Self::Error> {
        if !value.as_ref().ends_with(".json") {
            return Err(UnsupportedManifestFormat {
                format: value.to_string(),
            });
        }
        Ok(ManifestPath(value))
    }
}

impl std::fmt::Display for ManifestPath {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

/// Error for unsupported manifest file format
#[derive(Debug, thiserror::Error)]
#[error("unsupported manifest file format: {format}")]
pub struct UnsupportedManifestFormat {
    pub format: String,
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
